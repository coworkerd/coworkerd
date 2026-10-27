# Architecture overview

This page is the map of Coworkerd for anyone who wants to know how it works before reading the code or the other design documents: what one cluster is, which components it has and how they talk, the path a call takes from the ingress to a tenant's isolate, and where the system's boundaries are. Each topic has a document of its own; this page says what the topic is and where to read on.

## 1. One cluster, one platform

A Coworkerd cluster runs all the Workers of one platform. The platform is the cluster's only administrator; its users are **tenants**, every Worker belongs to one tenant and is named `tenant/Worker`, and a tenant's credential acts only on its own tenant. Worker code follows the Cloudflare Workers programming model and runs in workerd, Cloudflare's open-source runtime, with a series of thirteen patches, all under `src/workerd/server/`.

Coworkerd adds tenant isolation and resource limits, quotas and metering, placement across nodes, and a declarative API. In 0.5, stateless Workers run alongside SQLite Durable Objects and D1 databases. Objects have one owner per shard; transaction frames are confirmed by peers (or flushed locally with no peer), uploaded as log segments, and replayed when ownership changes. Accounts, billing and user interfaces belong to the platform on top.

## 2. Components and channels

```text
  the platform's systems, cowctl                       clients
        |  /v1 over HTTP, port 8940                     |  HTTP, port 8930 (the ingress)
        v                                               v
  coworkerd-apiserver  -- state, logs, trial loads -->  node: coworkerd  <-- peer sessions, port 8934 -->  other nodes
        |                 (peer port, management token)    |  (CONNECT signed with the cluster key, when configured)
        |                 and its own addresses            |
        |  writes tenants, credentials,                    |  reads the deployment state, tenants, members, code;
        |  deployments and code                            |  writes membership, usage, shard ownership and object history
        v                                                  v
       object storage (S3 API), one bucket: desired state and uploaded durable history

  inside a node: coworkerd <-- Unix sockets in run/ and inherited descriptors --> its workerd processes
```

| Component | Role |
| --- | --- |
| `coworkerd` | The node daemon: the ingress, placement and forwarding to peers, the pool of workerd processes and their limits, membership, usage counting, the management endpoints |
| `coworkerd-apiserver` | The control plane: the `/v1` API, tenants and credentials, validation and trial loads of new versions, writing the desired state |
| `coworkerd-launcher` | Starts each workerd process, where the host allows it, in namespaces of its own, without capabilities and under a system call filter |
| `workerd` | Upstream workerd with the patch series, plus the runtime bridge `runtime/runtime.js` that loads tenant code into isolates. The channel between a node and its workerd processes is workerd's own event interface: `WorkerdBootstrap.startEvent(cf)` for a request and `EventDispatcher.jsRpcSession()` for a method call on a Binding, which the node implements on the same socket and on the peer session |
| `cowctl` | The command line: converts wrangler projects and talks only to the control plane |
| Object storage | External, S3-compatible: membership, desired and observed state, code, shard ownership and uploaded object history; the latest confirmed tail can still be on nodes |

[Components and boundaries](components.md) describes each of them, how they are deployed and configured, the trust between them and the repository's layout.

## 3. The path of one call

1. **Ingress.** A request reaches any node's ingress. The node selects the route -- on a hostname registered to a tenant only that tenant's routes count -- its routes on that host and its paths under the default host `*` --, and on any other hostname the routes on that host (only the operations tool writes such routes) and those under `*`; among them the longest path prefix that covers the path at a segment boundary (`/acme` covers `/acme` and `/acme/...`, never `/acme-corp/...`), at the same prefix a route on the hostname before one under `*` -- and so the Worker; static assets of the Worker's version are answered there, without workerd.
2. **One deployment state per call tree.** From the same revision of its deployment state as the route, the node takes the **version lock table** of the Worker's closure: every Worker the call tree can reach, each with its version. Every call of the tree, on every node, executes by that table.
3. **The call lease.** The root gets a deadline, the lease time of 30 s from its arrival, and a budget of 8 subrequests, which Service Binding and stateless-to-object calls draw on; outgoing `fetch()` has the egress service's limits instead. A child call is granted at most 1 of them for its own calls, and its parent is charged for the call and the grant at once, so a tree can never spend more than its root was given: with these defaults a root makes up to four Service Binding calls and each of them one more, while calls to the `ASSETS` Binding draw nothing. A WebSocket connection is one root whose messages are its events, and each message the Worker receives gives it a root's lease again (the deadline the lease time from then, the budget the root's 8): the budget is conserved per message, the message's Service Binding calls and theirs sharing its 8.
4. **Placement across nodes.** The node chooses where the call executes: itself, when the version ran there within the last 30 s and it would admit the call -- except that, when it is not the Worker's home, the home goes first with probability 0.05 --; otherwise the Worker's **home** node, the first by rendezvous hashing of the Worker's name over the membership table, then itself, then the others by rank; candidates that failed recently go last, and at most eight attempts are made. A forwarded call carries its lock table, its share of the budget and its remaining time; the receiving node executes or refuses it and never forwards it again.
5. **Admission and the process.** The executing node admits the call -- at its concurrency limits it refuses new trees before nested calls, while its memory band refuses forwarded roots and nested calls, in yellow only those for versions not warm there, and never a request at the node whose ingress received it; it never queues -- and chooses a workerd process by rendezvous hashing: of the Worker over the ordinary processes, or of the tenant over the quarantine processes when the tenant is a suspect.
6. **The load check.** Before a version's first call in a process generation, and again once its last check there is more than 30 s old, the node loads it in that process without running a request; a version that does not load is refused before it executes (`LOAD_FAILED`).
7. **Execution.** The call reaches workerd over the process's Unix socket. The runtime bridge runs the version in an isolate of its own, with `env` holding the version's variables and secrets and the call's Bindings.
8. **Nested calls.** A Service Binding call leaves the isolate through a stub the platform created for its one target and comes back to the node over a Unix socket; the node places it like any call, in the same tree, with the same lock table, the same deadline and a share of the budget. A **method call** on such a Binding, or on a Durable Object stub, is the same call by another event: workerd's `jsRpcSession()` instead of a request. The node answers it with the target's own entry point, a capability it proxies to the caller -- through the peer session when the target runs elsewhere -- so the arguments are serialized in the calling isolate and decoded in the target's and the node reads neither. Everything the node reads of such a call is the envelope the runtime's shim put in the name of the session's one method, because workerd carries no `cf` on a method call. A capability the target returned belongs to the call tree: it is released with the tree's lease. Where a subcall executes is a policy point with the caller's node as its hint (`placement.subcalls`, section 4).
9. **Results.** A call goes to another candidate only when it certainly did not execute: an explicit refusal before execution, or a connection that was never established. Once a call was sent, a timeout or a broken connection is `RESULT_UNCERTAIN`, because neither proves that nothing ran.

## 4. Placement

Stateless placement is decided per call. An operator's static entry in `placements` can pin Binding calls; ingress requests for that Worker then execute where they arrive. Otherwise a Worker's home is a locality preference. `placement.subcalls: "home"` (the default) uses the ordinary warm/home policy for stateless Service Binding calls; `"local"` tries the caller's node first and falls back to that policy if it refuses. Both `fetch` and method calls follow this policy. Durable Object calls instead follow their shard's owner. [Cluster state](cluster-state.md), sections 7 and 11, describes both paths.

## 5. Isolation

Tenants share workerd processes: the V8 isolate is the boundary between tenants, and the process is the unit of enforcement. CPU defaults to 200 ms per event and memory to 128 MiB per version isolate. Ordinary and quarantine processes run stateless calls; durable processes (`d0` by default) run objects. Idle Workers and object trees are evicted; processes rotate on a binary change, a manual request or a memory threshold. Age and request-count rotation are off by default and never rotate durable processes. A durable rotation drains and exits before its replacement starts, avoiding two writers of the same SQLite files. [Process isolation](process-isolation.md) describes limits, facets, logs and the sandbox.

## 6. Control plane and state

The control plane `coworkerd-apiserver` serves `/v1`: the administrator creates tenants and sets their quotas, hostnames and credentials; tenants upload versions, which exist only once a node has trial-loaded them (unless they hold only static assets), activate them, set routes and secrets, and read their logs and usage. It writes the desired state to the object store; the nodes read it every few seconds and write their own membership and usage, plus shared Durable Object ownership and data prefixes, as the store's identities allow: each node has one of its own, which compose's `secrets` step generates and an operator on bare metal configures in the store. It keeps no state of its own -- it holds a mirror of the desired state, read from the store -- and no node calls it: the cluster serves calls while it is down; changes wait. [Control plane API](control-plane-api.md) describes the API and [cluster state](cluster-state.md) the object layout.

## 7. Egress and authentication

Tenant code has no network of its own: its outgoing `fetch()` goes to the node's egress service, which refuses the cluster's own addresses and metadata addresses whatever a rule says and private networks unless the administrator allows them, resolves every destination itself and connects only to the addresses it checked, applies the tenant's rules and limits, and meters the bytes. Every node has an object storage identity of its own, which writes its own membership and usage and the shared Durable Object prefixes; secrets' values are sealed with the cluster's secrets key before they are stored; peer sessions are signed with a cluster key and the management endpoints require a token unless the operator explicitly allows operation without one. [Egress and authentication](egress-and-auth.md) has the details and what is not done yet.

## 8. Compatibility

`cowctl deploy` builds a wrangler project with wrangler itself and turns the result into a package of modules and a version definition. Configuration the cluster cannot honour is refused -- at conversion and listed, or for a few values at upload. Cloudflare's platform settings, empty values and, behind a build tool's redirect, wrangler's own path and name fields and fields left at wrangler's defaults are ignored with a notice; without one, source maps are dropped and `send_metrics` is turned off. [Compatibility](compatibility.md) explains what runs, what is refused and why, and where behaviour differs from Cloudflare's.

## 9. Boundaries

0.5 runs HTTP and inbound WebSocket Workers, Service Bindings by `fetch` and JS RPC, SQLite Durable Objects (including hibernation and alarms), and D1's basic database API. Service RPC reaches a `WorkerEntrypoint`; Durable Object RPC reaches the object's public methods. workerd carries arguments and return values as capabilities; nodes validate the trusted call envelope and proxy the capabilities without decoding application arguments. Capabilities expire with their call tree's lease.

A Durable Object's environment has its version's variables, secrets, Durable Object bindings and D1 bindings, but no Service Bindings. An object's call to another object starts its own tree, bounded by eight object hops. The confirmation and recovery protocol reuses trusted same-boot files or rebuilds from uploaded history and surviving peer tails (see [cluster state](cluster-state.md), section 12); simultaneous loss of the owner and all confirming peers can lose history not yet uploaded. Alarms use the database as truth and object-store hints for discovery; missing hints delay discovery. Connections close when a process or node exits, so clients must reconnect.

KV, R2, Queues, Workflows, scheduled triggers, Tail Workers and Cloudflare's managed services are outside this release. The [compatibility document](compatibility.md) gives the checked differences, and the [Durable Object guide](guide/durable-objects.md) shows the programming model. The platform provides its own gateway, tenant accounts and billing.

## 10. Public references

- [workerd](https://github.com/cloudflare/workerd), and its [warning that workerd is not a hardened sandbox](https://github.com/cloudflare/workerd#warning-workerd-is-not-a-hardened-sandbox)
- [Workers versions and deployments](https://developers.cloudflare.com/workers/versions-and-deployments/), [Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/), [Service Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/), [Durable Objects](https://developers.cloudflare.com/durable-objects/), [D1](https://developers.cloudflare.com/d1/), [Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
- [Dynamic Worker Loader](https://developers.cloudflare.com/dynamic-workers/api-reference/)
- [Eliminating cold starts 2: shard and conquer](https://blog.cloudflare.com/eliminating-cold-starts-2-shard-and-conquer/)

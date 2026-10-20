# Architecture overview

This page is the map of Coworkerd for anyone who wants to know how it works before reading the code or the other design documents: what one cluster is, which components it has and how they talk, the path a call takes from the ingress to a tenant's isolate, and where the system's boundaries are. Each topic has a document of its own; this page says what the topic is and where to read on.

## 1. One cluster, one platform

A Coworkerd cluster runs all the Workers of one platform. The platform is the cluster's only administrator; its users are **tenants**, every Worker belongs to one tenant and is named `tenant/Worker`, and a tenant's credential acts only on its own tenant. Worker code follows the Cloudflare Workers programming model and runs in workerd, Cloudflare's open-source runtime, with a series of nine patches, all under `src/workerd/server/`.

Coworkerd adds what a shared, multi-tenant cluster needs: isolation and limits between tenants, per-tenant quotas and metering, placement of every call across the nodes, and a declarative API with a thin command line. Accounts, billing, user interfaces and stateful services are left to the platform on top.

## 2. Components and channels

```text
  the platform's systems, cowctl                       clients
        |  /v1 over HTTP, port 8940                     |  HTTP, port 8930 (the ingress)
        v                                               v
  coworkerd-apiserver  -- state, logs, trial loads -->  node: coworkerd  <-- peer sessions, port 8934 -->  other nodes
        |                 (peer port, management token)    |  (CONNECT signed with the cluster key, when configured)
        |  writes tenants, credentials, deployments         |  reads the deployment state, tenants, members;
        v                                                   v  writes its membership, suspects, usage
       etcd, through its HTTP gateway  <--------------------+
       object storage (S3 API): packages, modules, assets, definitions -- written by the control plane, read by the nodes

  inside a node: coworkerd <-- Unix sockets in run/ and inherited descriptors --> its workerd processes
```

| Component | Role |
| --- | --- |
| `coworkerd` | The node daemon: the ingress, placement and forwarding to peers, the pool of workerd processes and their limits, membership, usage counting, the management endpoints |
| `coworkerd-apiserver` | The control plane: the `/v1` API, tenants and credentials, validation and trial loads of new versions, writing the desired state |
| `coworkerd-launcher` | Starts each workerd process, where the host allows it, in namespaces of its own, without capabilities and under a system call filter |
| `workerd` | Upstream workerd with the patch series, plus the runtime bridge `runtime/runtime.js` that loads tenant code into isolates |
| `cowctl` | The command line: converts wrangler projects and talks only to the control plane |
| etcd, object storage | External: membership and the desired and observed state; the code |

[Components and boundaries](components.md) describes each of them, how they are deployed and configured, the trust between them and the repository's layout.

## 3. The path of one call

1. **Ingress.** A request reaches any node's ingress. The node selects the route -- the longest path prefix the path starts with, the exact host before the default host `*` at each length -- and so the Worker; static assets of the Worker's version are answered there, without workerd.
2. **One deployment state per call tree.** From the same revision of its deployment state as the route, the node takes the **version lock table** of the Worker's closure: every Worker the call tree can reach, each with its version. Every call of the tree, on every node, executes by that table.
3. **The call lease.** The root gets a deadline, the lease time of 30 s from its arrival, and a budget of 8 subrequests, which only Service Binding calls draw on; outgoing `fetch()` has the egress service's limits instead. A child call is granted at most 1 of them for its own calls, and its parent is charged for the call and the grant at once, so a tree can never spend more than its root was given: with these defaults a root makes up to four Service Binding calls and each of them one more, while calls to the `ASSETS` Binding draw nothing.
4. **Placement across nodes.** The node chooses where the call executes: itself, when the version ran there within the last 30 s and it would admit the call -- except that, when it is not the Worker's home, the home goes first with probability 0.05 --; otherwise the Worker's **home** node, the first by rendezvous hashing of the Worker's name over the membership table, then itself, then the others by rank; candidates that failed recently go last, and at most eight attempts are made. A forwarded call carries its lock table, its share of the budget and its remaining time; the receiving node executes or refuses it and never forwards it again.
5. **Admission and the process.** The executing node admits the call -- at its concurrency limits it refuses new trees before nested calls, while its memory band refuses forwarded and nested calls first, and it never queues -- and chooses a workerd process by rendezvous hashing: of the Worker over the ordinary processes, or of the tenant over the quarantine processes when the tenant is a suspect.
6. **The load check.** Before a version's first call in a process generation, and again once its last check there is more than 30 s old, the node loads it in that process without running a request; a version that does not load is refused before it executes (`LOAD_FAILED`).
7. **Execution.** The call reaches workerd over the process's Unix socket. The runtime bridge runs the version in an isolate of its own, with `env` holding the version's variables and secrets and the call's Bindings.
8. **Nested calls.** A Service Binding call leaves the isolate through a stub the platform created for its one target and comes back to the node over a Unix socket; the node places it like any call, in the same tree, with the same lock table, the same deadline and a share of the budget.
9. **Results.** A call goes to another candidate only when it certainly did not execute: an explicit refusal before execution, or a connection that was never established. Once a call was sent, a timeout or a broken connection is `RESULT_UNCERTAIN`, because neither proves that nothing ran.

## 4. Placement

Placement is decided per call; no Worker is assigned to a machine. An operator's static entry in `placements` can pin a Worker's Binding calls to one node; its ingress requests then execute where they arrive. The home node is a locality preference that keeps a Worker's isolates on few machines, not an owner its calls have to pass through or wait for. [Cluster state](cluster-state.md) describes the membership table, the deployment state, how code reaches the nodes, and placement in full.

## 5. Isolation

Tenants share workerd processes: the V8 isolate is the boundary between them, and the process is the unit of enforcement. Each request has a CPU limit and each isolate a memory limit (200 ms and 128 MiB by default, adjustable per tenant and per Worker); a watchdog kills a process that stops answering; a tenant whose code a limit could not stop becomes a suspect and runs only in quarantine processes for a while; idle isolates are evicted, and processes are replaced by new generations every hour. Where the host allows it, every workerd runs under the launcher in namespaces of its own, without capabilities and under a system call filter, which by default reports and does not block. [Process isolation and resource governance](process-isolation.md) describes all of it and where isolation ends.

## 6. Control plane and state

The control plane `coworkerd-apiserver` serves `/v1`: the administrator creates tenants and sets their quotas, hostnames and credentials; tenants upload versions, which exist only once a node has trial-loaded them (unless they hold only static assets), activate them, set routes and secrets, and read their logs and usage. It writes the desired state in etcd; the nodes read it and write only their own keys, which etcd's roles enforce once authentication is on. It keeps no state of its own, and no node calls it: the cluster serves calls while it is down; changes and the API's answers wait. [Control plane API](control-plane-api.md) describes the API and [cluster state](cluster-state.md) the etcd layout.

## 7. Egress and authentication

Tenant code has no network of its own: its outgoing `fetch()` goes to the node's egress service, which refuses the cluster's own addresses and metadata addresses whatever a rule says and private networks unless the administrator allows them, resolves every destination itself and connects only to the addresses it checked, applies the tenant's rules and limits, and meters the bytes. Peer sessions are signed with a cluster key, every component has an etcd user of its own, nodes need only a read-only object storage identity, and the management endpoints take a token -- each of these when it is configured. [Egress and authentication](egress-and-auth.md) has the details and what is not done yet.

## 8. Compatibility

`cowctl deploy` builds a wrangler project with wrangler itself and turns the result into a package of modules and a version definition. Configuration the cluster cannot honour is refused -- at conversion and listed, or for a few values at upload. Cloudflare's platform settings, empty values and, behind a build tool's redirect, wrangler's own path and name fields and fields left at wrangler's defaults are ignored with a notice; without one, source maps are dropped and `send_metrics` is turned off. [Compatibility](compatibility.md) explains what runs, what is refused and why, and where behaviour differs from Cloudflare's.

## 9. Boundaries

- **One region.** A cluster lives in one region or an equivalent low-latency network; there is no routing or failover across regions.
- **Stateless Workers only.** Coworkerd schedules stateless calls; stateful bindings are refused at conversion, and stateful services are the platform's, reached as external services. State with ownership, such as Durable Objects, would need guarantees this design does not have.
- **Isolates share processes.** Several tenants run in one workerd process, as on Cloudflare's platform, and an isolate is not as strong a boundary as a process; the per-isolate limits, the quarantine processes, process rotation and the sandbox are what this design adds to it.
- **Trust between nodes.** Nodes trust each other through a shared signing key, when one is configured; there is no mTLS and no procedure for a node to join, and secret values are stored unencrypted in etcd.
- **Not done.** Accounts, billing and user interfaces; stateful bindings; scheduled triggers, JS RPC, Tail Workers and email handlers; routing across regions.

## 10. Public references

- [workerd](https://github.com/cloudflare/workerd), and its [warning that workerd is not a hardened sandbox](https://github.com/cloudflare/workerd#warning-workerd-is-not-a-hardened-sandbox)
- [Workers versions and deployments](https://developers.cloudflare.com/workers/versions-and-deployments/), [Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/), [Service Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/)
- [Dynamic Worker Loader](https://developers.cloudflare.com/dynamic-workers/api-reference/)
- [Eliminating cold starts 2: shard and conquer](https://blog.cloudflare.com/eliminating-cold-starts-2-shard-and-conquer/)

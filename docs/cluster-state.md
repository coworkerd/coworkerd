# Cluster state

This document is for operators and contributors who need to know how the nodes of a cluster agree: what is kept in etcd and how it is laid out, who writes which keys, how nodes join and leave, how a deployment reaches every node and why a call tree always sees one deployment state, how code reaches the nodes, how a node decides where a call executes, and how a node leaves.

## 1. What is kept where

| Information | Where | Why |
| --- | --- | --- |
| Membership | etcd, one key per node on a lease | Joining and leaving need one authority, and a lease ends by itself when its holder dies |
| Tenants, credentials, routes, active versions, secrets | etcd, written only by the control plane | Deploying is a write that needs one write point and atomic transactions |
| Version definitions | etcd, immutable, identified by their hash | Small, and arriving with the pointers under one revision means a node that has a pointer has its definition |
| Code packages, modules, static assets | Object storage, addressed by content hash | Large and immutable: any copy is right, and a copy is checked against its hash |
| Suspects, usage | etcd, each node under its own prefix | Written by the node that made them; every node adopts the suspects, while only the control plane sums the usage -- a node reads back just its own current hour when it starts |
| Whether a peer can be reached now, load, which isolates are warm | Each node, in memory | Soft and fast-changing; under a partial partition a single cluster-wide answer would be wrong |

**Why etcd.** The cluster needs leases that expire by themselves, changes pushed from a known revision without gaps, and atomic conditional writes over several keys; etcd has all three natively and is one small binary. Nodes and the control plane reach it through its HTTP/JSON gateway with the HTTP client they already have, without gRPC.

**Rules of use.**

1. **etcd is not on the request path.** Every node keeps a local copy of what it watches and reads only that copy while serving calls.
2. **Static stability.** When etcd cannot be reached, nodes keep serving with the last state they know; joining, deploying and sharing new suspect marks wait for it.
3. **No high-frequency soft state in etcd.** Reachability, load and warm sets never go there; what does -- membership and suspects -- changes rarely and lives on leases.

An inaccurate membership view can only make placement worse, never make calls wrong: calls are rerouted only when they certainly did not execute (section 7), and only stateless calls are scheduled. State with ownership, such as Durable Objects, would break this premise and need stronger guarantees.

## 2. The etcd layout and who writes it

Everything lives under `/coworkerd/v2/`:

| Key | Value | Written by |
| --- | --- | --- |
| `tenants/<tenant>` | The tenant: state (`active`, `deleting`), quotas, hostnames, default limits | The control plane |
| `credentials/<keyId>` | A tenant credential: the SHA-256 of its secret part, its scope, expiry, revocation | The control plane |
| `deploy/<tenant>/definitions/<worker>/<version>` | A version definition, never overwritten; deleting a version that is not active removes it | The control plane |
| `deploy/<tenant>/active/<worker>` | The active version: `versionId` and `definitionHash`, and the Worker's own limits when it has them | The control plane |
| `deploy/routes/<host><pathPrefix>` | A route: host, path prefix, tenant, Worker -- one key for each host and prefix, the owner in the value | The control plane |
| `deploy/<tenant>/secrets/<worker>/<name>` | A secret's value and when it was set | The control plane |
| `deploy/<tenant>/binders/<target>/<binder>` | The reverse index of Service Bindings, which the write protocol compares | The control plane |
| `deploy/generation` | A counter written with every deployment, for information only | The control plane |
| `nodes/<nodeId>` | Membership: the advertised address, the epoch, the start time, `active` or `draining`; on the node's lease | That node |
| `nodes/<nodeId>/suspects/<tenant>` | A suspect tenant and when its suspicion ends, on a lease of its own | That node |
| `nodes/<nodeId>/usage/<hour>/<tenant>` | One tenant's usage on this node in one hour | That node |

**Desired and observed state.** Everything under `tenants/`, `credentials/` and `deploy/` is desired state: what the platform asked for, written only through the control plane. Everything under `nodes/` is observed state: what each node reports about itself. etcd's own roles make the split hard rather than a convention:

| Role | Permissions under `/coworkerd/v2/` | Held by |
| --- | --- | --- |
| `coworkerd-apiserver` | `tenants/`, `credentials/`, `deploy/` read-write; `nodes/` read-only | The control plane's user, which its operations tool uses too |
| `coworkerd-node` | `deploy/`, `tenants/`, `nodes/` read-only; `credentials/` not readable | Every node's user |
| `coworkerd-node-<nodeId>` | The key `nodes/<nodeId>` and the prefix `nodes/<nodeId>/` read-write | That node's user, `coworkerd-node-<nodeId>` |

Once `coworkerd-apiserver tool etcd-auth` has turned authentication on and every component logs in with its own credentials file, a node therefore cannot change what is deployed, cannot read credential hashes, and cannot write another node's membership, suspects or usage; without a credentials file a component's requests carry no identity, and an etcd without authentication enforces no role. etcd's permissions are unions of key ranges without exceptions, so the control plane's range is three read-write prefixes plus a read-only one, and a node's own range is two entries, so that `node-1` cannot write `node-10`'s keys. [Egress and authentication](egress-and-auth.md) describes how the users authenticate.

## 3. Membership

- **Joining.** At startup a node takes an etcd lease (`membership.ttlSeconds`, default 15 s) and creates its key `nodes/<nodeId>` with a conditional create. If the key exists and someone else keeps renewing it, another process holds the identity and startup fails, so a second process cannot start under a `nodeId` in use. This does not cover a running node cut off from etcd past its lease: if another process registered the same `nodeId` meanwhile, the first one's registration fails when etcd is back, and it keeps serving from its last state and retrying, so both serve until one is stopped.
- **Restarting right after a crash.** A restarted process finds its previous key still on the old lease. It watches that lease against a time bound -- the first remaining TTL it reads, plus a second because etcd reports whole seconds and two more for expiry processing: if the lease is renewed or outlives the bound, startup fails; if it expires, the key was the process's own previous life, and it registers with a new epoch.
- **Renewing.** The lease is renewed every third of its TTL. Once the node's own renewal deadline has passed by its local clock, that epoch is never revived, even if etcd still has the lease: a node cannot tell "etcd is down" from "only I cannot reach etcd", and in the second case the others already consider it gone. The node then registers again and moves its key to the new lease in one conditional transaction, so the key never disappears in between.
- **The epoch** is the lease's id and goes into every call id (`<nodeId>.<epoch>-invocation-<n>`), so ids stay unique although the sequence restarts with the process.
- **Watching.** Each node watches `nodes/` from the revision it read and keeps a local membership table; when etcd has compacted the history it needs, it reads everything again and continues, never staying on an old view.
- **Leaving.** A graceful exit revokes the lease, and the key disappears at once -- measured, within about 50 ms; after a crash the key disappears when the lease expires.
- **Without etcd.** The node keeps its last membership table and goes on serving and placing calls with it. Its own lease may expire meanwhile; the others then stop placing calls on it until it registers again with a new epoch.

## 4. Deployment state

A deployment publishes the immutable parts first -- the package and a copy of the definition in object storage, then the trial load, then the definition key in etcd -- and only then flips the pointers: the active version and the routes. Each node applies changes in revision order, never going back to an older one, and a change takes effect on a node all at once.

**A version is identified by its definition's hash.** `versionId` is a label for people; a version's identity is `definitionHash`, the SHA-256 of the definition's canonical encoding: Worker, version, compatibility date, main module and package hash, the compatibility flags and variables, the Bindings, and the assets settings and declared secret names when there are any. A label cannot point to another definition while its definition key exists: publishing other bytes under it is refused. Deleting a version removes the key, and a later upload may use the label again for another definition; a node that knows the label only under the old hash refuses calls locked to the new one (`VERSION_UNAVAILABLE`, inconsistent definition) until it has applied the new key. workerd loads a version under a key that includes its definition hash, so two definitions never share a loaded isolate.

**Validity is per Worker.** For the state at one revision, every node computes the same set of valid Workers -- the active pointer names an existing, well-formed definition with a matching hash, every secret the definition declares has a value, every Worker it reaches through Bindings is valid and has Worker code, not only static assets, and that closure has at most 64 Workers -- and the effective routes, those whose key and value agree and whose Worker is valid. An invalid Worker is not served (`WORKER_INVALID`, not executed) and its reasons, names only, are listed in the state endpoint; the other Workers deploy and serve as usual. The result depends only on the keys at that revision, so a new node and an old one never disagree.

**Routes.** A request goes to the route whose prefix is the longest one its path starts with; at each length the exact host is tried before the default host `*`. The prefix is compared as a string, not by path segments. So a longer route under the default host wins over a shorter route on a request's own hostname: a tenant's route `*` with `/beta/` also receives the requests for `/beta/...` on every other tenant's hostname, with their cookies and credentials, and a route `/acme` also takes paths such as `/acme-corp/...` that no longer route matches. A matching route that is broken answers `ROUTE_INVALID` and never falls back to a shorter prefix, so one bad write cannot make a request run in another Worker; no match is `404 ROUTE_NOT_FOUND`.

**Applying.** A node watches `deploy/` and applies each watch response as one batch of complete revisions: it decodes only the changed keys and re-evaluates only the changed Workers and those that bind to them. A node does not start on an empty deployment prefix, so that one connected to the wrong etcd does not serve as if nothing were deployed. Definitions seen once are kept locally, so call trees locked to an old version can finish after its key is deleted.

## 5. One call tree sees one deployment state

A Service Binding follows its target's active version, as on Cloudflare: deploying B changes what its callers reach without redeploying them. On top of that, **one call tree sees only one deployment state**: requests that started before a deployment use the old versions everywhere, requests that start after it use the new ones, and no request mixes them -- when A and B change their interface together, this is a matter of correctness.

- When a root call starts, the node takes the **version lock table** of the entry Worker's reachable closure from its current state: every Worker the tree might call, each with its version and definition hash at that moment. The closures are computed when the state is evaluated, so a root call only looks them up.
- The lock table travels with the call to every node, and **downstream always executes by the lock table, never by its own active pointers**, so a node whose view lags behind or runs ahead still serves the tree consistently.
- A node that does not know a locked definition fetches its copy from object storage by hash and checks it; a node that knows the label under another hash refuses (`VERSION_UNAVAILABLE`, inconsistent definition), as does one that cannot get the definition or the code. A call whose own version fields disagree with the lock table it carries is refused as `VERSION_PIN_MISMATCH`. None of them executes.
- A closure may hold at most 64 Workers; the control plane refuses a deployment that would exceed it in the state it read, and nodes check it as well: two concurrent deployments of different Workers can together push a common caller past the limit, and every node then refuses that caller as invalid until a deployment brings its closure back within the limit.

Resolving versions lazily, the first time each Worker is called, would not work: the lock would be recorded along one chain of calls, and a sibling branch calling the same Worker after a flip would see another version. Placement can be lazy because it needs no consistency across the tree; versions do. Each lock costs about 192 bytes on the wire, and the added latency is below what measurements resolve.

## 6. Code distribution

**Object storage** holds content by hash: `packages/<hash>` (package manifests), `modules/<sha256>` (modules and static assets alike), `definitions/<definitionHash>` (copies of definitions, for nodes whose view lags) and `bundles/<hash>` (the single-module code of the operations tool's manifests). Objects are only written, read, checked and deleted by key -- the store is never listed -- with signed path-style requests over plain HTTP.

**The node's cache** lives in `storage.cacheDirectory` (by default `cache/` in the stack directory):

1. Only verified content enters it: a fetched object is written to a temporary file, checked against its hash and renamed. **Unverified bytes are never executed**; the bytes handed to workerd are checked again.
2. Of the cached content only an index lives in memory, besides up to 128 parsed package manifests; least recently used content is evicted. `storage.cacheMaxBytes` (64 MiB) bounds each of up to three caches -- single-module code, package manifests, modules and assets -- and an object larger than it is never cached, so it cannot be served on that node.
3. A miss is fetched once however many requests wait for it; each waiter is bounded by its call's deadline, and the fetch goes on for later requests.
4. After a deployment takes effect, the node prefetches the code of the versions it serves that it does not have, retrying with a backoff, so it is usually ready before the first call. Asset bytes are fetched on their first request.
5. A failed fetch, a missing object or a wrong hash refuses the call before it executes (`VERSION_UNAVAILABLE`, or `ASSET_UNAVAILABLE` for an asset); content already cached keeps serving.
6. At startup the cache checks every file it holds and drops bad ones.

When object storage is down, cached versions run as usual, uncached ones are refused explicitly, and uploads fail before they change etcd; a route change through `/v1` does not need object storage, while activating a published version that has code, or rolling back to one, trial-loads it again and needs object storage unless the trial runs on a node that has the code cached.

## 7. Placement

**The home node.** Each Worker's candidates are ranked by rendezvous hashing: the score of a node is the first 8 bytes of SHA-256 over the Worker's full name, a zero byte and the node's id, and the highest score is the Worker's home. Every node computes the same ranking from the same membership table without coordination, and a node leaving or joining moves only the Workers that rank it first, about 1/N of them. The home is a locality preference -- it keeps a Worker's isolates on few machines and saves cold starts -- not an owner calls must pass through.

**The candidates** are the members of the deciding node's own membership table, except members that announced they are draining; a draining node ranks itself last. Reachability is not filtered in advance: a candidate that fails is avoided afterwards.

**The decision**, for an ingress request and a Binding call alike, on the node X that received it or runs the caller:

1. If the version ran on X within `placement.idleMs` (30 s) and X would admit it, X goes first -- except that, when X is not the home, the home goes first anyway with probability `placement.homeProbability` (0.05), so that copies spread by load drift back once traffic falls.
2. Otherwise the order is the home, then X, then the others by rank. Executing on X after the home refused is Cloudflare's choice too: the overflow stays where it arrived, at no extra hop, and the Worker is warm there for the next request.
3. Candidates that refused or could not be reached recently move behind the others for a short avoidance period, at most `placement.avoidMaxMs` (2 s), without being removed.
4. A refusal before execution, or a connection that was never established, moves on to the next candidate; a load failure of the version itself (other than a package the node could not get), a load check that could not finish (`RESULT_UNCERTAIN`) and anything that happens after the call was sent end the decision.
5. Each attempt uses one of `initialPlacementHops` (8). When every attempt failed, the call ends with `503 PLACEMENT_EXHAUSTED`, not executed, listing each candidate's refusal -- or with `503 LOAD_FAILED` when every attempt failed in its load check because the code package itself could not be loaded; the codes of other refusals reach the client only inside that list. A candidate that cannot fetch or verify the code refuses before that, with `VERSION_UNAVAILABLE`.

With `ingressPlacement` off (it is on by default), and for a Worker with a static entry in `placements`, ingress requests execute where they arrive. A static entry also sends the Worker's Binding calls to its one node, without other candidates; it is an operations tool.

**Admission refuses, it never queues.** A node refuses new tree roots once `placement.rootLimit` (128) calls are in flight and nested calls only at `placement.nestedLimit` (384) (`503 OVERLOADED`): under overload new trees are held off first, while trees already admitted can finish. The memory band ([process isolation](process-isolation.md)) adds to it: in yellow a node refuses forwarded roots and nested calls for versions cold on it, in red all of them (`LOAD_BAND_REJECTED`); a request executing where it arrived is never refused by the band. A refusal carries the reason, the node's band and load and a suggested avoidance time.

**Forwarding.** A forwarded call carries its Worker and version, the lock table, the call tree's identity, its share of the budget and the remaining time -- a duration, so clocks need not agree -- but never secrets or request bytes. The receiver records a forwarded request as the tree's root, executes or refuses it, and never forwards it again. The request body flows only after the receiver accepted the call, so a refused call goes to the next candidate as it is. A receiver that restarted with a new epoch refuses calls addressed to its previous process; a process that only renewed its registration accepts every epoch it has held.

**Why only certain non-execution reroutes.** A timeout or a broken connection cannot prove that a call did not run, and rerouting it could run it twice. So the sender waits at most `peerTimeoutMs` (2 s) for the receiver's answer, and once the call was sent, a timeout or a lost connection is `RESULT_UNCERTAIN`. A load failure is the version's own and would fail everywhere, so it ends the decision too -- except a package the node could not fetch, which another node may have.

## 8. The control plane's write protocol

The control plane writes deployments per Worker, so that deployments of different Workers do not conflict and one Worker's mistake affects only itself and the Workers that bind it:

- **Every change** reads one tenant's snapshot -- its keys, all routes and the generation -- at one revision, and validates the state after the write with the same evaluation the nodes use: the Workers of the change must be valid and no other valid Worker may become invalid -- in the state it read: the write compares the keys of the change, not those of the Workers that bind it (section 5 has the consequence).
- **upsert** (one Worker, or, from the operations tool, a few that must flip together): publish the immutable parts, then one transaction that compares each touched pointer and route key with the revision read (new keys must not exist) and the creation of each declared secret and of each Binding target outside the change, and writes the pointers, routes, the reverse index and the generation. A failed comparison rereads, revalidates and retries, at most 8 times, after a jittered pause of 5 to 160 ms.
- **remove** deletes a Worker's active pointer and routes and keeps its definitions; it checks that no Worker started binding it after the read.
- **apply** writes one tenant's whole state the same way, for the operations tool, but does not retry a failed comparison unless it is asked to.
- A route that belongs to another Worker is refused rather than taken over. Every `/v1` write names one Worker, so through the API a route moves in two writes -- off one Worker, then onto the other -- and no Worker has it in between; the operations tool moves it in one upsert that names both.
- A transaction holds at most etcd's 128 operations; a write that would need more is refused (with one new route each and neither Bindings nor secrets, 63 Workers fit), except the operations tool's `apply` for a tenant with no Workers yet, which is then written as several upserts, Binding targets first: a Binding cycle that alone does not fit is still refused, and a step that meets a concurrent write stops the apply with the earlier steps written.

## 9. Draining and rolling upgrades

A node that receives SIGTERM or SIGINT, or `POST /__coworkerd/drain`:

1. **announces** it: at once its readiness endpoint answers 503 and new root calls and calls from other nodes get `503 SERVER_DRAINING`, not executed; its membership value is rewritten as `draining`, so the other nodes stop placing calls on it. It applies no further deployments.
2. **drains**: it waits until its ingress requests, the calls admitted on it and its processes' work -- `waitUntil` included -- reach zero, for at most `drainTimeoutMs` (30 s); what is still running then is killed and ends as `RESULT_UNCERTAIN`.
3. **leaves**: it closes its workerd processes, writes its last usage, revokes its lease, closes its listeners and exits with 0.

A second signal kills the processes at once and exits with 2. `drainTimeoutMs` `0` turns the drain off: the node exits at once, without the announcement and without writing its last usage. Upgrading a cluster is draining one node at a time, replacing it, and waiting until it is registered and ready; measured with one node leaving under load, no request failed.

# Cluster state

This document is for operators and contributors who need to know how the nodes of a cluster agree: what is kept in object storage and how it is laid out, who writes which objects, how nodes join and leave, how a deployment reaches every node and why a call tree always sees one deployment state, how code reaches the nodes, how a node decides where a call executes, how a node leaves, and which node owns a Durable Object.

## 1. What is kept where

The bucket holds the cluster's desired state, code, membership, shard ownership and uploaded Durable Object history. Nodes cache verified code and recovered SQLite files, keep logs locally, and buffer recent tenant logs and usage in memory. A confirmed object write may still exist only in the owner's append log and its peers' memory before upload; with an empty selected peer list the node calls a local disk flush (see section 12 for current limitations). The bucket alone therefore need not contain the latest confirmed tail. A control plane holds a mirror of desired state and no private authority. Per-node policy, such as egress rules, remains in configuration.

| Information | Where | Why |
| --- | --- | --- |
| Membership | `members/<nodeId>/`: each epoch's registration, created only if absent, its heartbeat, overwritten every 5 s, and a drain marker | A registration that only one writer can create gives every process an epoch of its own, and a heartbeat that stops ages out by the store's own clock |
| Tenants, credentials, routes, active versions, secrets | `cluster/` and `tenants/`: the tenant index, one manifest per tenant and the credentials index, written only by the control plane and the operations tool | One object per tenant makes every change of a tenant a single write that commits whole or not at all |
| Version definitions | `definitions/<definitionHash>`, immutable | Identified by their hash, so any copy is right and a node checks the one it reads |
| Code packages, modules, static assets | `packages/`, `modules/` and `bundles/`, addressed by content hash | Large and immutable: any copy is right, and a copy is checked against its hash |
| Suspects | `members/<nodeId>/<epoch>.suspects`, one object per node | Written by the node that identified them, beside its heartbeat, so that the other nodes find them in the listing they make anyway |
| Durable Object shards' owners | `do/<namespace>/shards/<shard>/`: each epoch of the shard's ownership, created only if absent, a release marker, and the owner hint (section 11) | One owner per shard: whoever created its latest epoch, which no other writer can create too |
| Usage | `nodes/<nodeId>/usage/<hour>`, one object per node and hour | Written by the node that counted it, below a prefix only that node may write; only the control plane sums it |
| Whether a peer can be reached now, load, which isolates are warm | Each node, in memory | Soft and fast-changing; under a partial partition a single cluster-wide answer would be wrong |

**What the store must do.** The protocol uses S3 operations and requires these four properties from the store:

1. **Create-if-absent that holds under concurrency**: of several writers creating one key with `If-None-Match: *`, exactly one succeeds and the others are told it exists (412, or 409 for a conditional write already in flight). Epoch registrations, every revision of the desired state, the definitions and the epochs of a Durable Object shard's ownership are created this way.
2. **Strong read-after-write**: a read after a write returns that write.
3. **Listing with `LastModified`**: a listing names each object with the time the store last wrote it, and its answer carries the store's `Date`; liveness is judged from these two, both of the store's clock.
4. **Overwrite**: small objects are overwritten in place -- the hints, the heartbeats and drain markers, each node's suspects object and its hourly usage objects; nothing that decides who wins a write depends on an overwrite.

Conditional overwrites by ETag, appends and versioning are not used. The storage experiments found that concurrent creates of one key can all succeed on SeaweedFS 3.97, while exactly one succeeded on 4.48. On the tested 3.97 store only one writer of the desired state may run at a time (the membership reads its registrations back and does not rely on the property). The compose files pin SeaweedFS 4.48, which queues concurrent conditional writers of one key behind each other, about 1 s per waiting writer. `coworkerd check storage` measures create-if-absent under concurrency and reports it as `conditionalPutAtomic`, and every node measures it again at startup; a store where it does not hold is reported and warned of, not refused, but serves no Durable Object (section 11).

**The client.** Nodes and the control plane reach the store with signed path-style requests over plain HTTP: writes signed in the `Authorization` header, reads presigned in the query (signing only the host, with an unsigned payload). A request gets `storage.connectTimeoutMs` (2 s) to connect, `storage.readTimeoutMs` (5 s) for its answer and between two reads or writes of a body, and `storage.createTimeoutMs` (30 s) for the answer to a create-if-absent. A GET, HEAD, listing page or DELETE that fails before its whole answer arrives -- a timeout, or a connection that broke -- is sent once more on a new connection; an overwrite is sent once, and an answer from the store, of any status, is never retried. A create whose answer was lost is decided by reading the key back: the same bytes there mean the create took effect, other bytes mean another writer's, and only a missing key is created once more -- a revision object and the cluster's marker name their writer (its process's instance and a random value of the write), an epoch's registration its process's instance and the time, and a definition is the same bytes whoever writes it.

**Rules of use.**

1. **Stateless calls use cached state.** Every node keeps what it reads -- the members, the deployment state, the tenants -- in memory and its code in a verified cache, and serves calls from those; code, an asset's bytes or a locked definition it does not have yet is fetched while the call waits.
2. **Static stability.** When the store cannot be reached, nodes keep serving with the last state they read: stateless calls, at the ingress and from other nodes, are served even once a node's own epoch has expired. Joining, deploying and sharing new suspects wait for the store. Object ownership must remain live: a node whose own epoch expires stops owning shards; recovery and uploading also need the store (sections 11–12).
3. **No high-frequency soft state in the store.** A node writes a heartbeat every 5 s, lists the members every 10 s and reads one hint of the desired state every 5 s; reachability, load and warm sets never go there.

Stateless placement tolerates an inaccurate membership view because rerouting requires proof that a call did not execute. Durable Objects need the stronger ownership fence of section 11; a placement preference cannot establish ownership.

## 2. The layout and who writes it

| Key | Content | Written by |
| --- | --- | --- |
| `cluster/format/1` | The cluster's marker, created only if absent: a bucket without it is not a cluster's | The operations tool's `init` (compose's `init` runs it) and nothing else: every other writer refuses a bucket without it |
| `cluster/tenants/<rev>`, `cluster/tenants/current` | The tenant index -- each tenant's status, hostnames and the revision of its current manifest -- and its hint | The control plane and the operations tool |
| `cluster/credentials/<rev>`, `cluster/credentials/current` | The credentials index -- each tenant credential's scope, the SHA-256 of its secret part, expiry and revocation -- and its hint | The control plane (the operations tool's `init` writes its first, empty revision) |
| `tenants/<tenant>/manifest/<rev>`, `tenants/<tenant>/current` | The tenant's manifest -- its record (state, quotas, hostnames, default limits, deletion progress), its generation, and for each Worker the active version with its definition hash and limits, the versions, the routes, sealed secrets, class namespaces and tenant databases -- and its hint | The control plane, and the operations tool (its default tenant, or the tenant its `--tenant` names) |
| `definitions/<definitionHash>` | A version definition, created only if absent; an existing one must hold the same bytes | The control plane and the operations tool |
| `cluster/cleanup/<due>-<random>` | The objects a deletion's cleanup left for later -- younger than `control.cleanupGraceMs`, or whose references could not be read -- and when they are due (`<due>`: Unix milliseconds, 20 digits) | The control plane and the operations tool; the control plane's sweep removes each once due (section 8) |
| `packages/<hash>`, `modules/<sha256>`, `bundles/<hash>` | Code, addressed by content hash (section 6) | The control plane and the operations tool |
| `members/<nodeId>/epochs/<epoch>` | An epoch's registration, created only if absent | That node |
| `members/<nodeId>/<epoch>` | The epoch's heartbeat: the node's id, epoch, advertised address, start time, state (`active` or `draining`), the process's instance and the time written | That node |
| `members/<nodeId>/<epoch>.draining` | The epoch's drain marker | That node |
| `members/<nodeId>/<epoch>.suspects` | The suspects this node identified (section 9) | That node |
| `members/<nodeId>/probe/` | The node's startup measurement of create-if-absent, removed afterwards | That node |
| `do/<namespace>/shards/<shard>/epochs/<epoch>` | A Durable Object shard's ownership, created only if absent: the node and its membership epoch, when, why, the epoch before, the writer's instance and a random value (section 11) | The node that takes the shard |
| `do/<namespace>/shards/<shard>/epochs/<epoch>.released` | That epoch's release marker: its owner let the shard go | The owner, when it drains |
| `do/<namespace>/shards/<shard>/owner` | The hint: the latest epoch, its node and whether it was released | The owner, and any node that listed the shard's epochs |
| `do/<namespace>/shards/<shard>/alarms/<object>` | The hint of an object's next alarm (its time and which of its databases holds it), so the shard's next owner knows which objects to wake; the object's own database decides (section 12) | The shard's owner, whenever one of its objects' alarms changes |
| `nodes/<node>/wal/<node epoch>/<segment>` | Immutable segments of that node's confirmed append log | That node |
| `do/<namespace>/objects/<object>/manifest/<rev>` | Immutable revisions naming the snapshot and ordered log segments needed to rebuild the object; no `current` hint is used | The shard's owner |
| `do/<namespace>/objects/<object>/snapshot/<sequence>/` | Database parts and the index of their hashes, including the facet tree | The shard's owner |
| `nodes/<node>/durable-usage` | What that node's Durable Objects of each tenant hold, as it last published them (the `durableObjectBytes` quota, section 12) | The node itself |
| `nodes/<nodeId>/usage/<hour>` | Every tenant's usage on this node in one UTC hour (section 9) | That node |
| `check/` | `coworkerd check storage`'s probe keys, created and removed; its signed probe object stays as `bundles/<SHA-256 of its bytes>`, and with a node's identity (`COWORKERD_READ_ACCESS_KEY` and `COWORKERD_READ_SECRET_KEY`, and `COWORKERD_READ_NODE_ID` naming the node, as compose's `init` gives them) it also writes and removes a key below `members/<nodeId>/probe/` and `nodes/<nodeId>/` | The check, with the deploy identity (compose's `init` runs it) |

A revision `<rev>` and an epoch in a key are decimal numbers 20 digits wide, so that key order is numeric order. A sequence of revisions is created only if absent, `rev + 1` after the last; its hint `current` holds `{"rev":n}` and only says which revision is current. A reader always reads the revision a hint names, and lists the sequence for its largest revision when the hint is missing or behind what the reader already knows. Each revision object, the cluster's marker included, names its writer: the writing process's instance, the time and a random value of that write. The control plane keeps the latest 20 revisions of each sequence and removes older ones.

**Desired and observed state.** Everything under `cluster/` and `tenants/`, and the definitions, is desired state: what the platform asked for, written by the control plane or the operations tool. Everything under `members/` and `nodes/` is observed state: what each node reports about itself. The store's identities make the split hard rather than a convention. Each node has an identity of its own, which compose's `secrets` step generates with this policy:

| Identity | May | For |
| --- | --- | --- |
| `node-<nodeId>` | Read `cluster/format/`, `cluster/tenants/`, `tenants/`, `definitions/`, `packages/`, `modules/`, `bundles/`, `members/` and `do/` and `nodes/`; list `members/`, `cluster/tenants/`, `tenants/` and `do/` and `nodes/`; read, write and list `members/<nodeId>/` and `nodes/<nodeId>/`; write `do/*/shards/*` and `do/*/objects/*` (ownership, alarm hints, manifests and snapshots) | That node |
| `deploy` | Everything | The control plane and its operations tool |

A node therefore cannot change what is deployed, cannot read the credentials index, and cannot write another node's membership, suspects or usage; the Durable Object shards' objects are every node's to write, since any node may take a shard over, and ownership rests on create-if-absent and data publication on epoch fencing; the policy does not protect Durable Object data against a compromised node. Every prefix in the policy ends with `/`, so that `node-1` cannot write below `node-10`. On bare metal the store's identities are configured by hand to the same effect; `coworkerd check storage` reports whether a node's identity is held to it ([installing and running](guide/install.md), section 4). [Egress and authentication](egress-and-auth.md) describes the identities and how requests are signed.

## 3. Membership

- **Registering.** At startup a node lists its own prefix `members/<nodeId>/`. When the id is in use -- one of its heartbeats, or its newest registration, is less than `membership.ttlSeconds` (15 s) old by the listing's `Date` -- it waits, listing again every 0.5 s, until it is free; if the id is written again meanwhile, or is still in use after the TTL plus `membership.livenessGraceMs` plus 2 s, another process holds it, and startup fails with `membership nodeId conflict`. Then it creates `epochs/<n>` only if absent, `n` one more than the largest epoch the id has had, waits `membership.createSettleMs` (1 s) and reads the registration back: one that holds another process's instance is lost, and the node decides again. The read-back keeps two processes from sharing an epoch even on a store whose create-if-absent does not hold under concurrency. Startup goes on once the first heartbeat is written and the node has listed the members.
- **The heartbeat.** The node overwrites `members/<nodeId>/<epoch>` on a fixed cadence of `membership.heartbeatIntervalMs` (5 s): the n-th heartbeat of an epoch goes out n intervals after its first, whether the one before has finished or not -- at startup from the first heartbeat on, while the node still removes its old epochs' objects and lists the members. Every request of the membership is bounded by that interval, its connection's 2 s included; a heartbeat that runs out of it is given up and counted as a failure, so at most two heartbeats are in flight and one that hangs never holds up the next. One given up on may still reach the store after a later one, and the object then holds the older bytes: nothing changes, since liveness reads only `LastModified` and the bytes name the same process.
- **Liveness.** Every `membership.listIntervalMs` (10 s) each node lists `members/` and judges every heartbeat by the store's clock alone: one whose `LastModified` is more than the TTL plus the grace (10 s) behind the `Date` of the page that listed it is out, and a node is its newest live epoch. A new node and epoch is read once, for its address. A node that joins is seen within a listing interval -- measured, 6.6 s -- and one that crashed drops out of the others' tables 25 to 35 s after its last heartbeat.
- **The identity.** In every listing cycle a node also reads its own heartbeat back. When it holds another process's instance, or a newer epoch of its id appears that it did not register, another process holds the id: the node writes no more membership or usage objects (the new process goes on with the hour's usage from the object it read at its start), leaves its epoch's objects to the new process, which removes them as an old epoch's, drains (section 10) and exits with 3, so that its supervisor starts it again under a new epoch.
- **Expiry.** When a node has written no heartbeat for the TTL, by its own monotonic clock since the last heartbeat it sent that the store took, its epoch is expired: the other nodes drop it once its heartbeat is the TTL plus the grace old by the store's clock, so it always considers itself gone first. The TTL is three intervals, so after two failed heartbeats in a row the third goes out at the deadline itself; a heartbeat that went out, by the cadence, within the TTL and is still in flight when the TTL has passed is waited for: taken, the epoch lives on; failed, or out of its interval, the epoch expires at that moment. A node thus survives two failed heartbeats in a row, and a third expires it one interval past the TTL -- 20 s after the last heartbeat taken, still before the others drop it, at the TTL plus the grace (25 s) by the store's clock. That is the design's margin, not a fault: the TTL is what bounds how long a lost node goes unnoticed. An expired node writes nothing more under that epoch, gives up every Durable Object shard it holds (section 11), and places its own calls on itself only when no other member takes them: the home among the other members first, then the others, then itself, as a draining node does (section 7); and it keeps serving every stateless call addressed to any epoch it has held, at its ingress and from other nodes. An epoch is never renewed: once the store answers, the node registers a new one at its next heartbeat -- waiting, as at startup, until its id is no longer in use, where a heartbeat of its own that reaches the store late, its instance in the bytes, only restarts the wait -- and the new one joins the epochs it holds -- except a draining node, which registers no new epoch and so cannot write its drain marker either. A node cannot tell "the store is down" from "only I cannot reach the store"; in the second case the others drop it once its heartbeat is the TTL plus the grace old.
- **Recovering.** A node that could not list the store for longer than the TTL does not take the staleness that built up meanwhile at face value -- an outage of the whole cluster stopped every heartbeat. From its first successful listing after such an outage until that listing's `Date` plus the TTL plus the grace, it keeps the members it had whose heartbeat objects are still listed, taking a member's newer epoch as it appears; a member whose heartbeat object is gone is dropped at once, as usual. The state endpoint's `membership.recoveries`, `recoveredUntilUnixMs` and `keptMembers` show it. So a store outage longer than the TTL fails no call between nodes. The control plane's `/v1/nodes` judges every listing as it comes.
- **The epoch** is a positive integer, 1 for an id's first registration, and goes into every call id (`<nodeId>.<epoch>-invocation-<n>`), so ids stay unique although the sequence restarts with the process. Once a new epoch holds the id, the node removes its older epochs' heartbeats, drain markers and suspects objects -- reading the suspects and successfully writing them under the new epoch before removing the old object; failed reads or writes retain it, section 9 -- and keeps the newest two registrations.
- **The clock.** Liveness uses only the store's clock, and a node's own expiry only its monotonic clock; but the suspects' expiry and the usage hours use the wall clock. At startup a node compares its real clock with the `Date` of the store's answer to its first listing; since a `Date` names whole seconds and is stamped between request and answer, it computes the range the difference lies in, and refuses to start, naming the difference and the store's endpoint, only when the whole range is beyond `membership.clockSkewToleranceMs` (2 s); a store whose answers carry no `Date` stops the start too, since the clock cannot be checked. While it runs, it checks again every minute and only warns.
- **Leaving.** A graceful exit removes the epoch's heartbeat and drain marker, and removes its suspects object only when no unexpired suspicions remain; otherwise it leaves that object for the next epoch to recover, and the others drop the node at their next listing; after a crash they drop it once its heartbeat is the TTL plus the grace old. A heartbeat the node gave up on shortly before it exited may still reach the store after the removal and leave the heartbeat object behind: the others then drop the node once that object is the TTL plus the grace old, as after a crash.
- **Without the store.** A node keeps its last membership table and goes on serving and placing calls with it. Its epoch may expire meanwhile; the others then stop placing calls on it until it registers again with a new epoch.

A node also measures, in the background at startup, whether the store's create-if-absent holds under concurrency -- 4 writers on one key in each of 5 rounds, below its own `members/<nodeId>/probe/` -- and reports it in a `[membership] {"event":"conditional-put",...}` line and the state endpoint's `storage.conditionalPutAtomic`. Membership does not rely on it; more than one control plane on a bucket does (section 8).

### Host first-touch diagnostics

The authenticated state endpoint includes `host`. This is local observed state; it is not written to membership,
sent to peers or used for admission. `hostCheck.intervalMs` defaults to 60000 and `hostCheck.sampleMiB` to 16.
An interval of 0 disables the check. Each enabled round execs an independent helper, maps fresh anonymous memory,
touches every page and releases it. There is no idle helper or retained sample memory. Yellow/red memory bands
skip a round; a round still running at the next interval is also skipped. A helper failure or 30 s timeout does not
stop service, and a later interval tries again. The helper dies with the node, including a node killed by SIGKILL.

| `host` field | Meaning (since this node started) |
| --- | --- |
| `enabled`, `available` | Configured on; whether the last completed attempt produced a valid measurement (`false` before the first result or after failure) |
| `intervalMs`, `sampleMiB` | Effective configuration |
| `rounds`, `skippedRounds`, `errors` | Completed measurements, skipped intervals and failed attempts; a skip does not increase `touchedMiB` |
| `touchedMiB` | Total measured MiB in completed rounds |
| `chunksAtLeast20ms`, `chunksAtLeast100ms` | Counts of 1 MiB chunks at or above each wall-time threshold |
| `maxChunkWallMs`, `maxChunkSystemMs` | Largest chunk's wall time and the system CPU of that same chunk |
| `lastStallAtUnixMs` | End of the most recent chunk at least 20 ms, in real Unix milliseconds; 0 before any stall |
| `pid` | Current helper pid, or 0 between rounds |
| `lastResult`, `lastError` | Last successful measurement, or `null` before one; most recent failure, cleared by success |
| `balloon` | Startup detection: status, aggregate `freePageReporting`, and each device's complete `featureBits` and bit-5 result. Unknown reads are `null`, distinct from `false` |

A result includes requested/touched MiB, chunk/page sizes, counts at 5/20/100 ms, `stalledMsTotal` (sum of chunks
at least 5 ms), whole-round wall/user/system times, start/end/stall timestamps, and `largest` (at most eight chunks
with index, wall/user/system times and `observedAtUnixMs`). `coworkerd check host [MiB]` prints the same measurement
with current balloon detection, default 256 MiB; exit 1 means at least one chunk took 20 ms, 2 means a check error.
The [installation guide](guide/install.md#10-troubleshooting) explains interpretation and operator remedies.

`[host-check]` has `event: "startup"` for the first outcome (including skipped, unavailable or disabled), its real
`observedAtUnixMs`, and the cumulative `host` object containing that round's result. Startup warns when free page
reporting is negotiated or the first sample has a chunk at least 20 ms. Later only such slow rounds produce a line,
with `event: "result"`; successful quiet rounds and errors remain visible in state without recurring log traffic.

The helper stays in the node's cgroup. Its OOM kill must not count as a workerd's: `processes.kernelOomKills` counts
new cgroup OOM increments during a helper exposure window under `other` and `hostCheckUnattributed`, rather than
allowing a workerd SIGKILL to claim them. The window includes the last counter read after helper reaping. If helper
and workerd deaths overlap, this conservatively leaves the pid-less increment unattributed; a workerd's own V8 OOM
report and culprit are still read normally. Request limits and their accounting do not change.

`[process-memory-crash]` carries `inFlight` just like `[process-kill]`: all calls still held by the process, each with
`workerId`, `versionId`, `invocationId`, `treeId` (strings), `startedAtMonotonicMs`, `startedAtUnixMs` (numbers), and
`dispatched` (boolean); no calls gives `[]`. The same offending tenant can lead to a watchdog kill or a process's own
V8 memory crash, so both kinds of line can point to the same culprit.

## 4. Deployment state

A deployment publishes the immutable parts first -- the package's objects and `definitions/<definitionHash>`, then the trial load -- and only then writes the tenant's next manifest, which flips the pointers: the active versions and the routes; the manifest's hint and the tenant index follow. Each node applies the deployment state in revision order, never going back to an older one, and a change takes effect on a node all at once.

**A version is identified by its definition's hash.** `versionId` is a label for people; a version's identity is `definitionHash`, the SHA-256 of the definition's canonical encoding: Worker, version, compatibility date, main module and package hash, the compatibility flags and variables, the Bindings, Durable Object / database binding mappings, declared / deleted / renamed Durable Object classes, and the assets settings and declared secret names when there are any. A label cannot point to another definition while the manifest names it: publishing other bytes under it is refused. Deleting a version, or the tenant -- created again under its name --, removes it from the manifest, and a later upload may use the label again for another definition. A node forgets a label once the revision it applies no longer names it (the definitions it read stay, by hash); until then it refuses calls locked to another definition under that label (`VERSION_UNAVAILABLE`, inconsistent definition), and a trial load that meets such a label, for a revision the node has not applied yet, reads the desired state first. workerd loads a version under a key that includes its definition hash, so two definitions never share a loaded isolate.

**Validity is per Worker.** For the state at one revision, every node that could read the definitions computes the same set of valid Workers -- the active pointer names a well-formed definition with a matching hash, every secret the definition declares has a value that opens with the cluster's secrets key, every Worker it reaches through Bindings is valid and has Worker code, not only static assets, and that closure has at most 64 Workers -- and the effective routes, those whose Worker is valid. An invalid Worker is not served (`WORKER_INVALID`, not executed) and its reasons, names only, are listed in the state endpoint; the other Workers deploy and serve as usual. The result depends only on the state at that revision and the definitions the node could read: one that is missing or does not verify leaves its Worker invalid on that node until a later poll reads it, while one the store fails to give (an error, not a missing object) holds the whole revision back once the node has started: it keeps serving the revision it applied and reads again after 200 ms, doubling up to the poll interval, until it has them (`pendingDefinitions` in the state endpoint).

**Routes.** The request's hostname (compared in lower case, without a trailing dot) decides whose routes are tried: when it is registered to a tenant, only that tenant's routes -- those on that hostname and its paths under the default host `*` -- and when it belongs to no tenant, the routes on that hostname (only the operations tool writes such routes) and those under `*`. Among those, the longest prefix wins, compared by whole path segments: `/acme` matches `/acme` and `/acme/...`, never `/acme-corp/...`; at the same prefix a route on the hostname comes before one under `*`. The control plane writes a tenant's path `/<tenant>` under `*` as `/<tenant>/`, so the bare path `/<tenant>` matches none of the routes it wrote; the operations tool writes a route as given. Nothing matching is `404 ROUTE_NOT_FOUND`; a tenant being deleted, or one without a record -- the operations tool's `default` tenant excepted, so a tenant the tool wrote with `--tenant` that the API never created too --, is not served (`503 TENANT_UNAVAILABLE`). There is no separate route index: the routes are in each tenant's manifest, and a node derives the route table from all of them. The control plane refuses a write that would let two tenants name one route; should a node still find one, the first tenant by name serves it and the state endpoint counts it (`routeConflicts`).

**Applying.** Every `desiredState.pollIntervalMs` (5 s) each node reads one object, the tenant index's hint `cluster/tenants/current`; when it names the revision the node applied, that is all. Otherwise the node reads the index, then only the manifests whose revision the index changed, up to 8 at once, then by hash the definitions they name that it has not verified yet, up to 8 at once, each checked against its hash: one that is missing or does not verify stays out -- the Worker that names it is invalid -- and is read again at a later poll, not more often than every 2 s; one the store fails to give holds the revision back (above). It opens each secret's value with the cluster's secrets key, evaluates the result and applies the changes as one batch at the index's revision. A hint behind the revision the node applied, or a missing one, makes it list the index for its largest revision instead. In the recorded three-node run, deployment reached the nodes 3.8 to 4.7 s after upload. The poll interval is not an upper bound: reads take time and storage failures can defer applying a revision. Until then a node serves the state before it: a request right after a `/v1` deployment may still get the previous version, or `404 ROUTE_NOT_FOUND` for a new route. Each node reports the revision it applied (`storage.appliedTenantsRevision` in the state endpoint), and `cowctl deploy` waits until every node that is not draining reports it. A node does not start on a bucket without `cluster/format/1`, so that one connected to the wrong bucket does not serve as if nothing were deployed, and it is ready only after its first complete read. Definitions seen once are kept locally, so call trees locked to an old version can finish after it is deleted.

## 5. One call tree sees one deployment state

A Service Binding follows its target's active version: deploying B changes what its callers reach without redeploying them. On top of that, **one call tree sees only one deployment state**: requests that started before a deployment use the old versions everywhere, requests that start after it use the new ones, and no request mixes them -- when A and B change their interface together, this is a matter of correctness. Secret values are not part of it: a call gets the values its isolate loaded with on the node that executes it -- what that node's view held when the isolate loaded --, so after a secret changes, one tree can see the old value in one isolate and the new one in another.

- When a root call starts, the node takes the **version lock table** of the entry Worker's reachable closure from its current state: every Worker the tree might call, each with its version and definition hash at that moment. The closures are computed when the state is evaluated, so a root call only looks them up.
- The lock table travels with the call to every node, and **downstream always executes by the lock table, never by its own active pointers**, so a node whose view lags behind or runs ahead -- by up to a poll interval, section 4 -- still serves the tree consistently.
- A node that does not know a locked definition reads it from `definitions/<definitionHash>` and checks its hash; a node that knows the label under another hash refuses (`VERSION_UNAVAILABLE`, inconsistent definition), as does one that cannot get the definition or the code. A call whose own version fields disagree with the lock table it carries is refused as `VERSION_PIN_MISMATCH`. None of them executes.
- A closure may hold at most 64 Workers; the control plane refuses a deployment that would exceed it, and nodes check it as well.

Resolving versions lazily, the first time each Worker is called, would not work: the lock would be recorded along one chain of calls, and a sibling branch calling the same Worker after a flip would see another version. Placement can be lazy because it needs no consistency across the tree; versions do. Each lock costs about 192 bytes on the wire, and the added latency is below what measurements resolve.

## 6. Code distribution

**Object storage** holds code by hash: `packages/<hash>` (package manifests), `modules/<sha256>` (modules and static assets alike) and `bundles/<hash>` (the single-module code of the operations tool's manifests), beside the definitions `definitions/<definitionHash>`. Code is only written, read, checked and deleted by key -- its prefixes are never listed.

**The node's cache** lives in `storage.cacheDirectory` (by default `cache/` in the stack directory):

1. Only verified content enters it: a fetched object is written to a temporary file, checked against its hash and renamed. **Unverified bytes are never executed**; the bytes handed to workerd are checked again.
2. Of the cached content only an index lives in memory, besides up to 128 parsed package manifests; least recently used content is evicted. `storage.cacheMaxBytes` (64 MiB) bounds each of up to three caches -- single-module code, package manifests, modules and assets -- and an object larger than it is never cached, so it cannot be served on that node.
3. A miss is fetched once however many requests wait for it; each waiter is bounded by its call's deadline, and the fetch goes on for later requests.
4. After a deployment takes effect, the node prefetches the code of the versions it serves that it does not have, retrying with a backoff, so it is usually ready before the first call. Asset bytes are fetched on their first request.
5. A failed fetch, a missing object or a wrong hash refuses the call before it executes (`VERSION_UNAVAILABLE`, or `ASSET_UNAVAILABLE` for an asset); content already cached keeps serving.
6. At startup the cache checks every file it holds and drops bad ones.

When object storage is down, the deployment state a node has and its cached versions run as usual, uncached ones are refused explicitly, and every change of the desired state fails with `503 STORAGE_UNAVAILABLE` before it is written.

## 7. Placement

**The home node.** Each Worker's candidates are ranked by rendezvous hashing: the score of a node is the first 8 bytes of SHA-256 over the Worker's full name, a zero byte and the node's id, and the highest score is the Worker's home. Every node computes the same ranking from the same membership table without coordination, and a node leaving or joining moves only the Workers that rank it first, about 1/N of them. The home is a locality preference -- it keeps a Worker's isolates on few machines and saves cold starts -- not an owner calls must pass through.

**The candidates** are the members of the deciding node's own membership table, except members that announced they are draining; a draining node, and one whose own epoch expired, ranks itself last -- the home among the other members, the others, then itself -- and never goes first for a version it ran recently. Reachability is not filtered in advance: a candidate that fails is avoided afterwards.

**The decision**, for an ingress request and a Binding call alike, on the node X that received it or runs the caller:

Stateless Service Binding subcalls first try the caller's node by default (`placement.subcalls: "local"`), falling back to the order below on refusal; `"home"` disables this first step. Static `placements` entries are unaffected.

1. If the version ran on X within `placement.idleMs` (30 s) and X would admit it, X goes first -- except that, when X is not the home, the home goes first anyway with probability `placement.homeProbability` (0.05), so that copies spread by load drift back once traffic falls.
2. Otherwise the order is the home, then X, then the others by rank. Executing on X after the home refused keeps the call local: the overflow stays where it arrived, at no extra hop, and the Worker is warm there for the next request.
3. Candidates that refused or could not be reached recently move behind the others for a short avoidance period, at most `placement.avoidMaxMs` (2 s), without being removed.
4. A refusal before execution, or a connection that was never established, moves on to the next candidate; a load failure of the version itself (other than a package the node could not get), a load check that could not finish (`RESULT_UNCERTAIN`), the call's own lease running out (`410 LEASE_EXPIRED`: no candidate is avoided for it) and anything that happens after the call was sent end the decision.
5. Each attempt uses one of `initialPlacementHops` (8). When every attempt failed, the call ends with `503 PLACEMENT_EXHAUSTED`, not executed, listing each candidate's refusal -- or with `503 LOAD_FAILED` when every attempt failed in its load check because the code package itself could not be loaded; the codes of other refusals reach the client only inside that list. A candidate that cannot fetch or verify the code refuses before that, with `VERSION_UNAVAILABLE`.

With `ingressPlacement` off (it is on by default), and for a Worker with a static entry in `placements`, ingress requests execute where they arrive. A static entry also sends the Worker's Binding calls to its one node, without other candidates; it is an operations tool.

**Admission limits apply before any wait.** A node refuses new tree roots once `placement.rootLimit` (128) calls are in flight and nested calls only at `placement.nestedLimit` (384) (`503 OVERLOADED`): under overload new trees are held off first, while trees already admitted can finish. The memory band ([process isolation](process-isolation.md)) adds to it: in yellow a node refuses forwarded roots and nested calls for versions cold on it, in red all of them (`LOAD_BAND_REJECTED`); a request executing where it arrived is never refused by the band. A refusal carries the reason, the node's band and load and a suggested avoidance time.

A live paused process is the one exception to immediate refusal. `processPool.pausedAdmissionWaitMs` defaults to 1000 ms (0 disables it) and is shorter than `stuckTimeoutMs`. Waiting occupies the same root or nested admission slot as execution and ends at the earlier of that budget and the call's lease. Admission is checked again after asynchronous package fetching and load checks, immediately before dispatch. These checks reuse the same slot and share one cumulative paused-wait budget. Other ready processes and candidate nodes are tried immediately; only when every candidate explicitly refuses because its eligible processes are paused may one candidate wait. Dynamic placement waits on the local node when it is among the candidates; static placement to a remote node waits on that remote node with serial retries. Pause hints do not trigger avoidance: requests arriving during a pause still try that node first, so `reroutes` can be higher than in 0.5.4. The existing refusal category carries the pause hint, without a new wire field. Exit, kill, disconnection, retirement or a full process does not queue a request for a restart. Expiration retains the existing refusal codes; a lease expires through `LEASE_EXPIRED`. A peer retry is permitted only after an explicit pre-execution refusal; an accepted or uncertain attempt ends retries. The wait budget only decides whether to start another peer retry; an open already sent awaits its normal `peerTimeoutMs`, even if the wait budget expires meanwhile. The wait loop and the receiver enforce the lease. While waiting for a remote node, the sender rechecks its lease and concurrency slot; memory-band admission applies at the executing node.

The state endpoint reports `placement.pausedAdmission`, with `ordinary`, `quarantine` and `durable` counters. Each has `count`, `active`, `maxActive`, outcomes `resumed`, `expired`, `killed`, `lease`, `unavailable`, `canceled`, and `totalWaitMs` / `maxWaitMs`. `[admission-wait]` begin/end rows name the class, id, outcome and wait duration. `[refusal-count]` counts final refusals; a provisional pause hint does not emit that row.

**Forwarding.** A forwarded call carries its Worker and version, the lock table, the call tree's identity, its share of the budget and the remaining time -- a duration, so clocks need not agree -- but never secrets or request bytes. The receiver records a forwarded request as the tree's root, executes or refuses it, and never forwards it again. The request body flows only after the receiver accepted the call, so a refused call goes to the next candidate as it is. A call is addressed to the epoch the sender's table names: a process accepts every epoch it has held, expired ones included, while a process that started again holds only its new epoch and refuses calls addressed to its previous life.

**Why only certain non-execution reroutes.** A timeout or a broken connection cannot prove that a call did not run, and rerouting it could run it twice. So the sender waits at most `peerTimeoutMs` (2 s) for the receiver's answer, and once the call was sent, a timeout or a lost connection is `RESULT_UNCERTAIN`. A load failure is the version's own and would fail everywhere, so it ends the decision too -- except a package the node could not fetch, which another node may have.

## 8. The control plane's write protocol

The control plane and the operations tool, which uses the same protocol, are the only writers of the desired state; the control plane holds all of it in memory: at startup it reads the cluster's marker, the tenant index, every manifest the index names and the credentials index, completes what a writer left half done (below), and it refuses to start on a bucket without the marker. Every write is one aggregate's next revision -- a tenant's manifest, the tenant index or the credentials index -- computed from that mirror:

- **A change** computes the tenant's next manifest from its current one and validates it with the evaluation the nodes use: the Workers of the change must be valid and no other valid Worker may become invalid. A change that alters nothing writes nothing.
- **The write** creates the manifest's next revision only if absent. Created is committed. When another writer created that revision first, the control plane reads the tenant again from the store, computes the change again and retries -- at most 9 attempts in all for a deployment, an activation, a change of routes, a Worker's removal or a secret, and 8 for the other writes -- after a jittered pause of 5 to 160 ms; then it gives up with `409 CONFLICT`, nothing written. The operations tool's `apply` does not retry unless it is asked to.
- **After the commit** it writes the manifest's hint -- then looks for a later revision another writer may have created meanwhile and points the hint at the latest -- removes the revision 20 writes back, and writes the tenant index's next revision, which names the new manifest, merged with other writers' revisions, at most 16 attempts. The write is complete, and answered as done, only once the hint and the index name it: a failure of either is the write's (`503 STORAGE_UNAVAILABLE`), the committed revision stays, and the next write, the control plane's next poll or any control plane's next start completes it. A request that repeats a change already in the latest manifest completes it the same way.
- **In one process** the writes of one aggregate wait for each other, so a single control plane never conflicts with itself, and requests on different tenants write their manifests without waiting for each other and queue only for the tenant index's next revision; only writers in different processes -- control planes and the operations tool -- meet as revisions created first -- on SeaweedFS 4.48 each waiting writer of one key takes about 1 s more. That correctness holds only where create-if-absent holds under concurrency: on a store that reports `conditionalPutAtomic: false`, run one writer at a time: one control plane, and no write of the operations tool beside it.
- **Another instance's writes** reach the mirror within `control.pollIntervalMs` (5 s): the control plane reads the two indexes' hints that often and reads what changed; a conflict reads the tenant at once.
- **Across tenants**, the control plane checks before a write, against its mirror, that a hostname belongs to one tenant only and that no route of another tenant is taken over; the hostnames once more in the tenant index's turn, where every tenant's entry meets: an index revision never gives one hostname to two tenants, and a tenant's revision that would is refused (`409 CONFLICT`) and the tenant goes back to the revision the index names.
- **The repair.** At every poll and at its start, the control plane completes what a writer -- itself, another control plane, the operations tool -- left half done: an index whose hint lags its latest revision gets its hint again, and one listing of `tenants/` finds every tenant whose hint lags its latest manifest (rewritten) and every manifest newer than the index names (indexed); each tenant's hint is read only when its ETag changed.
- **Deletions' objects** go only once nothing references them and they are older than `control.cleanupGraceMs` (1 hour) by the store's clock; younger ones wait in `cluster/cleanup/` for the control plane's sweep, every `cleanupGraceMs` but at least every minute. An upload that finds an object present writes it again when it is older than half that grace, and checks the objects it found once its manifest is committed, writing a missing one again, so an upload and a cleanup that meet do not leave a definition without its objects.
- **Size.** A tenant's desired-state manifest is one write, so there is no limit on how many Workers a change touches: the operations tool's `apply` of 200 Workers is one manifest of 63 KB, written in about a second.

An interrupted request leaves at most immutable objects -- a package's objects, a definition --, a committed manifest the index does not name yet, or an index revision -- of the tenants or of the credentials -- its hint does not name yet, which the next poll of a control plane, or the next start of one, completes; never a half-written manifest; an interrupted deletion leaves the tenant partly deleted ([control plane API](control-plane-api.md), section 6, describes its steps). [Control plane API](control-plane-api.md) describes what each `/v1` write does, a tenant's deletion included.

## 9. Usage and suspects

**Usage.** Every node counts, per tenant and UTC hour, requests, CPU milliseconds, egress bytes, log lines and isolate time. Every `usage.flushIntervalMs` (60 s) it writes each hour whose counts changed as one object, `nodes/<nodeId>/usage/<hour>`, holding every tenant of that hour; an hour without a change is not written. A draining node writes its last counts before it leaves (and leaves even when that write fails), and a node that restarts within the hour reads that hour's object back first and continues from it, so while the store takes the writes a stopped node loses at most one interval; counts whose writes failed stay in memory until a later write takes them, and a node that cannot read its hour's object at its start reads it again before each later write and writes that hour only once it has read it, adding its own counts to the stored ones. Once an hour each node lists its own prefix and removes the objects older than `usage.retentionHours` (744); the control plane does the same for every node at its start and once a day, so that the objects of nodes that left go too. The control plane sums usage when asked ([control plane API](control-plane-api.md), section 9).

**Suspects.** A node that identified a tenant as a culprit ([process isolation](process-isolation.md), section 7) applies the mark at once and writes the suspicions it identified, every tenant with its Worker, the call and its expiry, as one object beside its heartbeat, `members/<nodeId>/<epoch>.suspects`: at once when they change, again with the same bytes every 60 s while there are any -- which refreshes its `LastModified` and keeps its ETag -- and never while its epoch is expired. It removes the object when none is left; graceful exit preserves any unexpired suspicions with their original expiry times. The other nodes find the object in their membership listing and read it only when its ETag changed, so a node's suspects cost each reader one GET per change and nothing in between; each adopts every suspicion until its own expiry, which every node judges by its clock, and never lifts one because the object disappeared. A node that started again reads its previous epoch's suspects, writes the recovered unexpired entries into its new epoch's object with unchanged expiry times, and only then removes the old object. A failed read, invalid content or failed write leaves the old object in place for a later attempt. The other nodes adopt a mark within one listing interval -- measured, 6 s.

Only unexpired suspicions are stored or rewritten. By default (`suspectProbation: true`, with quarantine processes), an expired mark remains **locally** in probation until an invocation dispatched into this node's quarantine process after the deadline finishes its runtime context, including background work, without harming the process. An isolate limit termination or a tenant's 5xx counts when the process survives. Each node decides independently; no probation state or release message is shared. Reoffending during probation doubles the previous duration, capped by `suspectMaxMs`, and publishes a new active mark. A restart loses local probation; expired entries are not restored, so the tenant goes straight to ordinary processes on that node. With `suspectProbation: false` or `quarantineProcesses: 0`, expiry retains its original behavior.

`state.processes.suspects.entries` includes active and probation tenants; with probation enabled, `state` is `suspected` or `probation`. Its `configuration.probation: true` is present when probation is effective (omitted for the legacy disabled mode), and `objectSuspects` counts only synchronized, unexpired entries. The effective startup configuration also reports the configured `suspects.probation` flag. The `[suspect]` events mean:

| Event | Meaning |
| --- | --- |
| `recognized` | This node identified a new offense and set a new deadline and count |
| `synchronized` | This node wrote its active suspicion to the object store |
| `shared` | This node adopted another node's unexpired mark |
| `restored` | This node recovered its own unexpired mark from an earlier epoch |
| `probation` | The deadline passed; this node still routes the tenant to quarantine |
| `released` | A qualifying invocation completed in a surviving quarantine process; this node released the tenant. `completion` identifies its Worker, invocation, process and generation |
| `expired` | With probation disabled or no quarantine processes, the deadline passed and the local mark was removed, as before |

Expiry removes an entry from the storage object even while local probation continues; it does not mean the tenant was released. No `probation` or `released` events are emitted with the feature disabled.

## 10. Draining and rolling upgrades

A node that receives SIGTERM or SIGINT, or `POST /__coworkerd/drain`:

1. **announces** it: at once its readiness endpoint answers 503 and new root calls and calls from other nodes get `503 SERVER_DRAINING`, not executed -- requests to the Durable Objects of its shards `503 DO_OWNER_DRAINING`, and it releases those shards as soon as their objects' requests in flight are done (section 11); it creates its drain marker `members/<nodeId>/<epoch>.draining` and writes `draining` into its heartbeat, so the other nodes stop placing calls on it at their next listing. It applies no further deployments. Its WebSocket connections are closed with `1001` (going away) at once -- those another node's ingress forwarded to it through that ingress, which passes the code on -- and the drain record counts them.
2. **drains**: it waits until its ingress requests, the calls admitted on it and its processes' work -- `waitUntil` included -- reach zero, for at most `drainTimeoutMs` (30 s); what is still running then is killed and ends as `RESULT_UNCERTAIN`.
3. **leaves**: it closes its workerd processes, writes its last usage, removes its heartbeat and drain marker, retains its suspects object while it contains unexpired suspicions, closes its listeners and exits with 0 -- or with 3 when the drain began because another process took its identity (section 3).

A second signal kills the processes at once and exits with 2. `drainTimeoutMs` `0` turns the drain off: the node exits at once, without the announcement and without writing its last usage. Upgrading a cluster is draining one node at a time, replacing it, and waiting until it is registered and ready; measured with one node leaving under load, no request failed.

## 11. Durable Object shards

A Durable Object namespace has a fixed number of shards -- 1024, set when the control plane makes the namespace and fixed for its life -- and an object falls into the shard its id's first 8 bytes, read as a big-endian number, give modulo the shards: every node computes the same. A shard has one owner in the cluster at a time, the node that runs its objects in its durable process with their SQLite databases on its disk. Those databases are a cache: the object's data is the log of its transactions, which its owner has peers confirm and puts on the object store, so a node that takes a shard over rebuilds its objects from there (section 12).

- **Epochs.** Owning a shard is having created its latest epoch, `do/<namespace>/shards/<shard>/epochs/<n>` (`n` 20 digits wide), created only if absent: a node lists the shard's epochs and creates the largest plus one, and holds the shard once its create succeeded; another writer's create of the same epoch got there first, and the node reads who did. An epoch names the node, its membership epoch, when it was taken, why (`first`, `timeout`, `released`, `restart`) and the epoch before, and the writer's process instance with a random value of the write: a create whose answer was lost is decided by reading the epoch back (section 1, the client) -- its own bytes mean the node holds the shard. A create waits up to `storage.createTimeoutMs` (30 s) for its answer, a read `storage.readTimeoutMs` (5 s). Then the node writes the hint `do/<namespace>/shards/<shard>/owner` -- the epoch, the node and its membership epoch --, which only says which epoch is the latest: a reader reads that epoch, and lists the shard's epochs when the hint is missing, names an epoch that does not exist, or names an owner that turns out not to hold the shard; a node that listed the epochs writes the hint again. A shard is taken when a request to one of its objects first comes, not when its namespace is made.
- **Who takes a shard, and when**, each node deciding on its own member table (section 3):
  - a shard without an epoch is taken by its **default owner**: the first, in the rendezvous order of `<namespace>/<shard>` (section 7's rule), of the members that run durable processes and are not draining. Another node that gets a request for it sends the request to the default owner, which takes the shard; only when the default owner refuses or cannot be reached does that node wait 200 to 500 ms, read the shard again and take it itself;
  - a shard whose owner is live -- its membership epoch is in the member table -- is not taken: its requests go to the owner;
  - a shard whose owner's membership epoch has expired -- its heartbeat is the TTL plus the grace old by the store's clock, the recovery anchor included, or a newer epoch of the node replaced it -- or whose owner released it is taken over by the default owner among the other members; each other member waits its rank in that order times 1 s, reads the shard again and takes it only if nobody did, so that few nodes ever compete for one epoch (on SeaweedFS 4.48 each waiting writer of one key waits about 1 s more). A node that cannot reach an owner its member table still counts live lists the owner's own heartbeats and judges it by the store's clock at that moment;
  - a shard whose epoch names this node under an earlier membership epoch -- the node started again, or registered again after its own expiry -- is taken back at once with a new epoch; its objects recover through section 12's path, reusing trusted files and their confirmed WAL positions when available.
- **Requests and their check.** Node-originated requests (HTTP SQL and alarms) follow the same forwarding path with no calling object or call tree; the trusted peer accepts them only with zero hops and a database or alarm target. A node that does not own an object's shard sends the request to the owner through the peer session -- a WebSocket upgrade too, and the hop count of the Durable Object calls that led to it (at most 8) --, naming the shard and the epoch it expects the owner to hold. The owner runs it only when it holds the shard at that epoch, and refuses it otherwise, `409 DO_SHARD_EPOCH_MISMATCH`, before anything ran. A refusal, or a connection that could not be made, sends the caller's node to the store once more -- it lists the shard's epochs -- and the request to the owner it finds, once; a request whose answer was lost after it was sent is `RESULT_UNCERTAIN`. Each node keeps a cache of the owners it found -- the owner, its membership epoch, the shard's epoch, where it was found and when a request last confirmed it --, and drops an entry when its owner refuses with a mismatch, when the owner's membership epoch expires, when the shard turns out released, and when no request confirmed it for 60 s.
- **Expiry fences a node off.** A node whose own membership epoch expires (section 3) gives up every shard it holds at once, writing nothing: from then on it answers requests to objects `503 DO_OWNER_EXPIRED`, tells their WebSocket connections to go away (`1001`), lets the requests in flight run for up to a second and starts its durable processes again, empty -- every object's facet closed. It keeps serving stateless calls. It expires at most 20 s after the last heartbeat the store took, while the other nodes take its shards over only a TTL plus the grace (25 s) after that heartbeat by the store's clock, so a shard is never served by two nodes at once. Back with a new membership epoch, it does not take back the shards others took, and a request that names its old epoch of such a shard is refused with `DO_SHARD_EPOCH_MISMATCH`.
- **Draining releases.** A draining node refuses new requests to its objects, `503 DO_OWNER_DRAINING`, and closes their WebSocket connections with `1001` at once; when the requests in flight on its durable processes are done (within the drain period) it closes those processes, then writes a release marker `do/<namespace>/shards/<shard>/epochs/<n>.released` beside each epoch it holds, and the hint saying released: the next owner takes the shard at once, while the rest of the drain goes on. The drain record counts the shards and the time (`durableObjects`). A node that leaves without releasing -- killed, or its markers could not be written -- loses its shards a TTL plus the grace after its last heartbeat. A node whose identity another process took writes no marker.
- **The store and the nodes.** On a store where a node measured create-if-absent not to hold under concurrency (`conditionalPutAtomic: false`, section 1) the node takes no shard and answers every request to an object `503 DO_UNSUPPORTED_STORE`. While the store cannot be reached a node keeps serving the shards it holds until its membership expires; a request whose shard's owner it does not know fails with `503 STORAGE_UNAVAILABLE` once the store's timeouts ran out. A node with `durableObjects.processes` `0` starts no durable process, takes no shard and is no default owner -- its heartbeat says so -- and sends its Workers' requests to objects to their owners.

The state endpoint's `durableShards` shows the shards a node holds by namespace, its takeovers by reason, its releases, its refusals by code, its forwarding and its owner cache; each take, release and refusal is a `[do-shard]` line, and each request to an object a `[do]` line with its shard and epoch, on the node that ran it and on the node that sent it.

## 12. Durable Object durability

An object's data does not stay on the node that owns its shard. Every transaction the object
commits is a few WAL frames; the durable process hands them to the node's replicator, which has K peers hold them
before the object's answer goes out, merges the frames of all its objects into segments on the object store, and keeps
each object's manifest naming what makes it up. A node that takes a shard over rebuilds its objects from that history
and never reads the previous owner's files.

- **The commit path.** The durable process ships the frames of each committed transaction over a descriptor it
  inherited at startup (workerd's patch 0013; nothing is opened by path). The node writes the record -- the node's
  epoch and the shard's, the object, which of its databases, the transaction number, the frames as the database wrote
  them, and a CRC32C over all of it -- to its own append log and sends it to the peers of the object's shard: the first
  `durableObjects.peers` (2) nodes after the shard on the member ring, which hold it in memory and answer with the
  highest sequence they hold. The answer to the write goes out when they have it: no disk and no upload are on that
  path, so it costs a peer round trip. Transactions of the same database are confirmed in commit order; this is not a single order across all facets of an object.
  A peer that cannot be reached is not counted as reachable. Its stream is retried to the same node; a changed membership
  table can select a replacement for later records. A reopened stream first receives the exclusions again as `drop` messages, then the unsealed records except those marked isolated.
  While an unconfirmed or unsealed tail remains, a background loop drives one connection attempt per peer, at most
  once per second; it does not need another business write or reset the write's confirmation deadline. Every stream
  has a local session token, the receiver's address and membership epoch, and the owner's log epoch. Open, append
  and drop continuations from a retired session cannot update its replacement. An open whose sealed prefix advances
  while connecting is discarded and retried with the current `firstSeq=sealedSeq+1`. Requests retain their old sink
  until completion. A local epoch change fails pending writes and retains confirmed, unsealed bytes for the existing
  earlier-epoch uploader, which releases them only after the segment and manifests succeed. DROP exclusions remain
  in the receiver's epoch buffer, so a delayed append cannot restore a record after DROP.
  Current and earlier-epoch seals share one writer through segment allocation, upload and manifest publication.
  If a conditional segment create finds an existing key, the full stored bytes must match before it is treated as
  successful; conflicting bytes fail the upload and retain the pending records.
  With an empty selected list the node calls a local disk flush instead. If the selected list is nonempty but all peers are unreachable, the write waits and can time out. A failed flush logs `log-flush-failed`, isolates the waiting records and answers `STORAGE_UNAVAILABLE` without confirming them. The owner never waits for its own disk otherwise
  (`synchronous=OFF`): this machine's files are a cache.
- **Recoverable record size (0.5.5).** The normal tail page budget is 32 MiB. A larger encoded record
  occupies one page alone. The production peer `TwoPartyClient` / `TwoPartyServer` use Cap'n Proto's default
  `ReaderOptions`: 8,388,608 traversal words (64 MiB) and nesting limit 64; this fixed, flat result needs no deeper
  nesting. The append stream uses 4 MiB messages and bounds each reassembled record by the same effective
  record limit (including its four-byte framing), within `peerBufferBytes` (256 MiB by default). The local
  process channel has UInt32 message lengths, and the local confirmation queue has no separate byte cap.
  The effective encoded record admission limit is **67,104,768 bytes by default**. The governing constraint
  is the existing recovery RPC's 64 MiB reader limit minus 4 KiB of result/framing/alignment headroom; that
  reservation also covers a single record's store-segment envelope. Store segments are checked separately
  against the client's 64 MiB read limit. The page timeout is increased rather than lowering this record limit.
  A smaller `peerBufferBytes` lowers this limit. WAL page headers and record metadata count, so the SQL payload
  limit is lower; CPU, memory and confirmation-time limits can reject a transaction earlier.
  The read-only state field `durableLog.settings.recordLimitBytes` reports the actual node limit. Oversized
  records are refused before entering the append log or peers, answer the existing `STORAGE_UNAVAILABLE`, and
  mark the object for repair from confirmed history. `record-limit-refused` names its encoded size and limit;
  `durableLog.recovery.recordLimitRefusals` counts refusals. This intentionally replaces the old behavior that
  could acknowledge a record too large to retrieve during recovery. Historical records beyond the RPC limit
  are reported as incomplete evidence; they are never truncated or certified as an empty tail.

- **Segments and manifests.** Every `durableObjects.segmentIntervalMs` (30 s) or `durableObjects.segmentBytes` (16 MiB)
  the replicator seals the records its peers hold, in sequence order up to the first one still waiting for them, into
  bounded segments. Normal sealing, earlier-epoch resealing and recovery publication all check before adding
  the next record, including length prefixes, the JSON index and the digest in the 64 MiB store-read ceiling.
  One large record occupies a segment alone; an earlier epoch with a large backlog is split into prefixes and
  each prefix is retained until its manifests succeed. A segment is -- a header, the records, and a SHA-256 of all of it, with an index by object -- and creates it as
  `nodes/<node>/wal/<node epoch>/<segment>`. A segment combines records of the node's objects, sharing the segment upload across them. Manifest writes still occur per touched object. Then each object the segment touched gets the next revision of its manifest,
  `do/<namespace>/objects/<object>/manifest/<rev>`, created only if absent and carrying the writer's identity so a lost
  answer is decided by reading it back: the manifest names the object's latest snapshot and, after it, the segments to
  replay in order. Only once every manifest is written are the peers told to drop what the segment covers and the
  databases told they may checkpoint those frames -- a segment no manifest names would be a segment nobody replays.
  Readers do not keep a `current` hint for an object: a rebuild lists the revisions and reads the largest.
- **Snapshots.** When an object's log since its last snapshot exceeds `durableObjects.snapshotBytes` (64 MiB), or an existing snapshot is more than
  `durableObjects.snapshotIntervalMs` (one day) old, its owner uploads each of its databases after a checkpoint -- the file then holds every transaction of
  the generations before it -- in parts of `durableObjects.snapshotPartBytes` (16 MiB) under
  `do/<namespace>/objects/<object>/snapshot/<sequence>/`, writes the index last, and then the manifest that names it.
  The first snapshot requires the size threshold; elapsed time alone does not create it.
  The segments the snapshot covers leave the manifest; the stored segments and snapshots themselves are retained.
- **Rebuilding an object.** The first request waits while the node reads the latest manifest, verifies and downloads
  its snapshot, and replays its segments. It also collects unsealed records from its own peer buffer, its own log,
  and the owners and possible holders of every source shard epoch after the snapshot cut. Retained member addresses
  can be used even when a lease has expired. Each source is paged to completion; a failed, refused or invalid answer
  is unknown, not an empty history. Records are deduplicated by shard epoch and owner sequence, and replay checks
  WAL generations, checksums and consecutive frames. These checks alone cannot detect every missing older generation.
  The latest manifest is checked again after collection in case the old owner sealed while answering.
  The complete new image is replayed and synchronized under staging names before it replaces any local database,
  WAL, shared-memory file or facet index. A failure before replacement preserves the old files; the object remains
  REBUILDING and cannot serve them while the next request retries recovery.

  The authenticated peer CONNECT response advertises `X-Coworkerd-Tail-Owner: 1`. Only a marked original owner's
  complete answer proves coverage; older nodes answer only from their incoming buffers, so their empty answer
  cannot prove that their own tail is empty. A complete marked answer from each source owner ends collection
  immediately. Without that proof the
  object waits until `membership.ttlSeconds * 1000 + membership.livenessGraceMs` (25 s by default), measured from its
  first rebuild attempt at this ownership epoch; request and storage retries do not restart that clock.
  Lease expiry, a missing member and a failed connection do not prove that a tail never existed.

  A trusted successor on the same machine boot can answer for an old destination node epoch only after reading
  every CRC-framed record through that epoch's log EOF. If all the records for the requested namespace and shard
  have reached their objects' manifests, or have been fenced and classified, it returns an empty complete owner
  answer. Otherwise it refuses without claiming that the epoch is absent, and prioritizes resealing that epoch's
  prefix in segments below the store-read ceiling. A different object in the same shard can delay this answer.
  Original destination epochs are always preserved in queries. During local recovery, a complete old log with
  no startup-unsealed record for this object and source shard epoch also closes that source immediately; later
  resealing cannot turn a positive startup observation into an absence proof.

  A missing, incomplete or untrusted source log cannot provide this proof. One such epoch anywhere in the chain
  can therefore add the default 25 s wait to the first open of every later new object in that shard, including
  after an owner drains permanently or replaces its disk. A successor's new registration alone is insufficient.
  `durableLog.recovery.ownerLogUnavailable` and `ownerLogUnavailable` in the bounded recovery log identify cases where a
  responding successor explicitly reported that its source log was unavailable. Old nodes could acknowledge
  writes after a local log write failure; their logs cannot prove those writes absent. Check for
  `log-write-failed` and adequate log-disk space before stopping an old node for upgrade.

  Before the object serves, adopted tails are published as unchanged records in segments under the new owner's
  `nodes/<id>/wal/<epoch>/` path and a manifest references them. At the deadline the node first logs and counts
  `owner-silent` if only the source owner is missing, or `tail-recovery-bounded` if other possible holders did not
  answer. It then publishes a full snapshot at the new shard epoch, including a valid empty snapshot if no history
  survived. That epoch is the durable cut: older records are never replayed, even for a facet absent from the snapshot;
  records at the same epoch use the facet's sequence watermark. Every later manifest update retains the cut.
  Late records received before that cut are logged and counted, and cannot be merged into the new history.

  A small complete recovery adds only collection and the writes needed to publish adopted tails. If the old owner
  machine is unavailable, the first request after takeover can instead wait the 25 s missing-evidence period, a bounded final
  holder round described below, and snapshot upload; default failure detection adds about another 25 s. A large snapshot takes longer.
  Store read or publication failures return `STORAGE_UNAVAILABLE`; the deadline does not authorize serving an
  unpublished history. Tail collection runs alongside snapshot download and segment replay. Verified tails
  survive publication failures and subsequent attempts until the object publishes successfully. Recovery
  manifests use CAS against the last replayed revision: a newly added segment aborts publication and is replayed
  by the retry, rather than being removed by an image that never contained it.

  The 25 s clock limits retries for absent evidence, not whether to ask a known holder. Every attempt, including
  one beginning after that clock expires, asks every known holder once. Connections keep `peerTimeoutMs`;
  connected probes page to `more=false` within a finite drain round. Because the protocol gives no size before
  a reply, each page reserves the largest legal page at 8 MiB/s aggregate divided among `recoverParallelism`
  slots, plus connection time, and never less than `confirmTimeoutMs`. With defaults this is 34,000 ms per
  page, independent of the remaining 25 s. A probe gets a drain window for one configured peer buffer at the
  same rate (128,000 ms by default), and its final page retains its full budget. For N queued remote probes
  and P node-wide slots, let Q include transfers already active or queued ahead of this round. A conservative
  post-deadline bound is ceil((Q+N)/P) × (25,000 + 128,000 + 34,000) ms with default buffers and timeouts,
  plus local metadata/encoding work. New arrivals cannot overtake this FIFO batch. A probe whose connected
  source still has more data at the drain limit contributes only the verified pages already received; the
  missing source is named in the bounded-cut log. The owner's zero-peer append backlog has no separate
  byte cap and can exceed the configured peer-buffer drain window. A local probe does not consume a network slot. Requests may
  answer `DO_RECOVERING` while that background collection continues. Registered nodes with zero durable
  processes are excluded per membership epoch, including from missing-holder accounting.

  Candidate and cursor handling uses one FIFO transfer-slot queue for the whole node, not one-millisecond
  polling per object. Multiple objects share `recoverParallelism`; a known local owner's complete proof does
  not start an unnecessary peer batch, and retained self registrations share one local query. Incoming records are
  CRC-validated on admission. Incoming-buffer and current-epoch page selection read retained metadata, skip
  records at or below the cursor, and decode/copy the selected page. For a held earlier epoch, each page first
  re-encodes every matching record above its cursor, including CRC work and copies outside the selected page.
  Large tails and multiple recovering objects repeat this work; transient bytes can reach the remaining
  matching tail size. The page transfer limit is not a bound on that temporary allocation or CPU work.
  Metadata uses O(retained record count) memory within the configured byte buffer. Store member
  discovery remains per recovering object, at most once per second. Unless every source already has a complete
  owner proof, even an attempt after the 25 s clock performs one bounded full discovery before its final holder
  round. Previously discovered addresses and unresolved keys survive failed attempts. Each LIST and member
  metadata GET has its own `peerTimeoutMs` budget, including after the missing-evidence clock expires.
  For O objects and M registrations, normal discovery work is O(O × M × recovery rounds). A malformed successor
  registration can be read again for each predecessor, making the conservative full-round bound one LIST plus
  M + M × (M - 1) / 2 metadata GETs, each with that budget. Successfully decoded registrations are reused within
  the round. The retained in-process member history is capped at 4,096 entries;
  eviction leaves `recoveryHistoryIncomplete` set until process restart, so early completion cannot assume that
  all evicted addresses were rediscovered. A failed membership listing can also leave older member entries visible.

  Candidate windows use only object-store LastModified times. A heartbeat older than the source's shard acquisition
  by TTL, grace and one listing interval excludes that member epoch. With only a registration, exclusion also
  needs a verified newer registration for a different instance before that margin. Missing dates, ambiguous
  instances and registration-only members without such a successor remain candidates; they can force a
  `tail-recovery-bounded` cut. Node-reported start times do not establish these windows.
  A chain of S source epochs requires at most S serial immutable authority GETs per attempt, each bounded by
  max(`peerTimeoutMs`, `confirmTimeoutMs`) (5 s by default). Successful reads are cached across attempts in this
  object's current recovery, giving a first-pass bound of S × 5 s plus local work; failed reads may be retried.
  A request waits at most `durableObjects.recoverTimeoutMs` (30 s) before `503 DO_RECOVERING`,
  while the same rebuild continues. The receiving peer returns its dispatcher before this wait; peer-open retains
  its two-second `peerTimeoutMs` bound, and an open known not to have sent an event may be tried elsewhere.
- **Healing a trusted local copy.** On startup, objects recovered from trusted local files are checked before a
  durable process opens them. Risk is determined from the trusted position's `dataEpoch`, the start of this
  node's uninterrupted ownership, the manifest's snapshot cut, and the local marker that a complete recovery
  in this data epoch was already published by 0.5.5. A legacy takeover after that cut requires healing
  even if it never produced a segment or the previous snapshot predates its tail. A data epoch of 1 has no
  earlier owner. Own restarts and own seals do not increase the data epoch and cannot create a new risk after
  a healing cut. For a risky object the node checks the shard's authority and reclaims its own shard at its new membership
  epoch, preserving the data epoch. It validates the known WAL prefix completely, then materializes it into the
  database in bounded chunks and publishes a full snapshot. A request routed through the same restart joins
  that operation. Before reading the manifest, healing lets an existing local manifest write finish and defers
  new background publications for this object until the snapshot settles. Deferred records keep their log
  positions; changes by a remote writer still have to pass the manifest revision check. That object waits behind the healing operation; its
  files are protected from cache eviction. A failed check or publication keeps the local files and reports the
  object and failing step; it can retry after 30 s. A newer owner or data epoch is never repaired from stale files.
  Risk checks run outside the serial snapshot gate, so a large healing snapshot does not block unrelated
  healthy first opens. Requested risky objects take priority over the single background check; the background
  scan does not enqueue work while the gate is busy. Snapshot payload is limited to 64 MiB/s per node.
  `heal-snapshot` records each successful publication; `heal-wal-materialized` records the known frames included.
  The node materializes and synchronizes all live facet databases before synchronizing the object's
  healing-in-progress marker and removing any WAL. Restart can reapply a surviving WAL or continue from
  its synchronized database after WAL removal.
  An empty legacy WAL is eligible when its position's generation matches the last logged FRAMES generation,
  `lastFrame` is zero and `checkpointedSeq` covers that record. New positions retain checkpoint salts and frames.
  A successfully healed ownership run needs no new healing after its own restart or seals. A facet explicitly
  deleted by the latest local log record is omitted; an unexplained missing required file still fails healing.
  `healErrors` counts currently unresolved healing failures; `healFailures` is cumulative, with one failure and
  one object-operation-failed log per operation rather than per waiting request. This publishes a trusted surviving copy; it
  cannot reconstruct pages already absent from that copy. WAL checks do not constitute a SQLite integrity check.
- **A node id has one location.** Run a node id in only one place at a time. When moving it to another
  machine, confirm that the old machine's node processes stopped before reusing it. Missing heartbeats or
  expired registrations do not prove that the old machine has stopped.
- **This machine's files.** The remaining admission, WAL and healing gaps are listed in
  [Known issues in 0.5.5](guide/install.md#known-issues-in-055). They are a cache of the history, with a `durableObjects.diskBytes` budget (half of the
  filesystem the stack is on): the files of the objects nothing has asked for in the longest time, and whose changes
  are all in a segment, go when that is full. An object registered as open on a live durable-process channel is
  skipped when its turn to evict arrives, as is an object with a request in flight or an open connection on a live
  durable-process channel. Dispatching a request refreshes its last-access time, keeping it out of
  the cold candidates for 60 seconds even before the open notification arrives. A dispatched request that never
  opens the object does not prevent eviction after that interval, and a matching close removes the live-open
  registration protection. `durableLog.disk.coldFilesSkippedLive` counts candidates skipped in the latest eviction pass; it
  resets to zero on every pass. While nonzero, `files-drop-live-skipped` logs the count at most once per minute.
  These objects can keep the node over budget until their connection or request ends. An object with only a
  hibernating WebSocket remains protected while that connection's process generation has an open channel,
  even after the instance's close notification removes its registration. `durableLog.objects.activeRequests`
  and `activeConnections` total the requests in flight and open connections on live channels. A closed process
  generation no longer contributes to these totals or protects files; its late completion callbacks only retire
  its remaining counts. A JS RPC session retains its request count until the caller releases its stub.
  Reuse requires the same Linux boot; a clean drain does not grant
  reuse across boots because SQLite uses `synchronous=OFF`. Startup synchronizes `clean:false` before trusting
  or changing object files. Failure gates all DO service and retries without deleting caches. An untrusted
  startup retains the previous boot identity until cleanup and classification finish; an interrupted startup
  cannot make the remaining old files eligible for same-boot reuse. Before handing
  files to workerd, an atomic position update marks an object open with an epoch and generation, without fsync.
  Only the last matching facet close, with logged checkpoint coverage and no pending repair, clears it. A stale
  close cannot clear a newer open. EOF or process closure invalidates every still-open object of that process;
  the next event rebuilds it. A rebuild resets durable processes only if the object has a live open facet or a
  dispatched request whose `FROM_OPEN` has not yet been processed, or a request or connection still held on a
  live channel. Rebuilding an object with a live connection first resets this node's durable processes and closes
  their connections. A closed object's cold rebuild leaves
  otherwise live durable processes running. A request that already passed `openObject` is checked again after its
  durable generation is selected; if the object now needs recovery it is refused rather than sent to unreconstructed
  files.

  This synchronous dispatch check precedes request accounting. An unavailable startup or recovery log refuses
  with 503 `STORAGE_UNAVAILABLE`; lost or fenced ownership, a changed shard epoch, or GONE refuses with
  409 `DO_SHARD_EPOCH_MISMATCH`. Recovery, legacy cache admission, or snapshot healing in progress refuses
  with 503 `DO_RECOVERING`. A different live generation holding the object refuses with 503 `DO_UNAVAILABLE`.
  Each refusal logs `[do-dispatch-refused]` with the object, selected process and generation, status, code and
  reason. A matching live registration repairs a false open marker without changing its generation. A matching
  live dispatch marker preserves its WAL; without a live holder, WAL preparation runs again before dispatch.
  Preparation or marker-write failure refuses with 503 `STORAGE_UNAVAILABLE`. These refusals are returned
  without retrying. The request and connection counts also protect an instance reopening after dispatch:
  between its FROM_CLOSED and subsequent FROM_OPEN, a rebuild still resets a process with a live holder.

  Each node epoch has a versioned `.recovery` sidecar even when empty. REPAIR_BEGIN and exclusions are
  synchronized before replacing files, publishing exclusions/SEAL or appending on the repaired image.
  Unexcluded decodable raw records are possible writes; there is no CONFIRMED sidecar or second healthy-path
  synchronization. An append, short-write or local flush failure freezes that epoch's append/confirmation. Every subsequent
  DO write on the node is refused, even if disk space is freed. Restart the node daemon to begin a new node
  epoch before retrying writes. A sidecar failure
  freezes this process's DO service until restart, preserves `cache.json` and possible unique copies, and prevents
  a clean marker. Required database files and named-facet indexes must be readable; explicitly deleted facets
  are exempt. For opens routed through the node, missing files are never handed to workerd to create an empty
  replacement; the hibernating-socket wake described above does not take that path. A possibly unique legacy
  takeover cache enters named HOLD when admission fails; a 0.5.5 complete recovery instead rebuilds from its
  published history. Store history is read, verified, replayed and synchronized in staging before original non-risk
  cache files are replaced.

  Power-loss startup deletes database caches, indexes and positions but retains raw log and exclusion prefixes.
  A persistent crossed-boot flag prevents these epochs from later proving absence in the same boot. Complete,
  fully released predecessor logs can be replaced by `.closed` EOF/shard summaries only after unresolved repair,
  rebuild, healing and cache-admission state is carried forward durably. Original record encodings are unchanged.
  All files in `do/log/`, plus database/WAL bytes, count in the eviction accounting. Positions, facet indexes,
  snapshot staging and code caches add other disk use. During recovery, the old and staged databases coexist;
  leave room for one additional object-sized image. The active epoch, incomplete prefixes and crossed-boot
  logs are protected; they have no configured byte ceiling and can exceed `diskBytes` after all eligible caches
  are evicted. Filesystem capacity is the ultimate bound and a full disk refuses writes and marker-dependent
  first reads. Monitor free space. Draining attempts one current segment and does not drain every earlier epoch;
  it is no substitute for the bounded upgrade observations in [installation](guide/install.md), section 9.
- **The fence.** Every record carries the shard's epoch. A peer rejects a record older than a nonzero shard epoch it knows. Before the first complete remote original-owner tail page, the owner fences its earlier ownerships: pending records are isolated, objects leave LOCAL, the shard leaves held routing, and those epochs can never append or confirm again in that process. GONE applies the same fence; a local self-repair probe is exempt. A higher manifest also fences older publication. These checks still depend on available authority, rather than every peer instantly knowing every change.
- **The recovery lower bound.** Tail sources start at `L = max(snapshot shard epoch, dataEpoch of the latest manifest's shard-epoch record)`, recalculated after the final manifest reread. A same-node restart can raise the manifest epoch without raising the continuous-ownership start, so the manifest epoch itself is not the bound. This applies to every non-manifest source: local logs, predecessors, peers and owner answers. Earlier records below L are late-tail observations, not permanent isolated pairs. Above L, a complete same-boot local predecessor contributes all unexcluded records matching namespace and object id, regardless of data epoch. Adopted records are published before LOCAL and then retired from earlier logs. Records already present in referenced manifest segments remain valid even when their source predates L.
- **When the store cannot be reached.** Uploads are retried with a growing wait, the peers keep holding the records,
  and the writes go on. A peer holds at most `durableObjects.peerBufferBytes` (256 MiB) for the owners it is a peer of
  and refuses records past it; an owner whose records nobody confirms within `durableObjects.confirmTimeoutMs` (5 s)
  fails that write with `STORAGE_UNAVAILABLE`, isolates the records it could not get confirmed locally, omits them from its segments, asks peers to drop them and records exclusions at the next seal, even when there are no new confirmed records to upload, then rebuilds the object from its history
  before serving it again. Reads of objects whose writes did not fail are answered as usual. Peer-drop delivery is best effort and repeated when a session reopens. Until exclusion metadata reaches a manifest, a tail retained by an unreachable peer may still be seen in recovery; a failed response does not prove rollback.
  Buffers for an owner's old node epoch are not retired merely because a replacement registered: that does not
  prove their records reached storage. Without an old owner's TRIM/DROP they remain until the holder restarts,
  sharing the same 256 MiB aggregate payload budget with current epochs and potentially causing new appends to
  be refused as full. `durableLog.buffer.bytes`, `limitBytes`, `refusals` and `owners` expose the retained bytes
  and source epochs. This bound covers payload, not the per-record and per-stream metadata reported in memory state.
  Old writers can also erase `snapshotShardEpoch` from a later manifest, invalidating a recovery cut; finish
  replacing all writers before resuming traffic under the documented maintenance procedure.
- **Accepted step back.** Peers hold frames in memory. Losing the owner and every holder before upload can lose
  the confirmed tail; a no-peer confirmation relies on the owner's flushed local disk. The normal 30 s segment
  interval does not bound that loss during an upload outage or backlog. Recovery uses every verified tail obtained
  before its deadline, but an unreachable holder may still have more. Bounded recovery explicitly reports that
  uncertainty and commits a snapshot cut before accepting new writes. Returning old records cannot repair a history
  that has already branched. When an old owner is fenced, actual matching records in referenced segments count as
  `confirmed-tail-transferred`; records not accounted for by those segments after a newer snapshot cut count as
  `confirmed-tail-unaccounted`, and the uploaded old segment is retained. Unaccounted is not proof of loss: a normal
  later snapshot also removes old segment references. Unknown evidence remains pending for bounded retries.
  Histories already missing an adopted tail in older versions cannot be reconstructed from absent bytes merely by
  upgrading. In particular, later WAL generations can refer to pages that were never uploaded; surviving original
  holders or a verified backup may be needed. Recovery checks do not promise to repair an already corrupt database.
- **One kind of object.** There is no hot standby tier: the peers buffer what is not on the store yet, and an object is
  rebuilt from the store. One object is at most `durableObjects.objectBytes` (10 GiB (10,737,418,240 bytes)), judged by its
  databases as they last reported their size (at a checkpoint, and when a database is opened or closed); past it a
  **write** is refused with `413 DO_OBJECT_TOO_LARGE` at the moment it would be made durable, and everything that
  reads is served. Transactions whose commit page count does not exceed the current database size are permitted even over the limit, so rows can be deleted. The refused transaction reaches no peer, no segment and no manifest, so it is not part of the
  object's history. The node immediately marks the object for repair; the next request waits for its files to be
  rebuilt from confirmed history before serving, so the refused transaction cannot be read by that request.
- **The tenant's storage.** What all of a tenant's objects hold is bounded by the tenant's `durableObjectBytes` quota
  (10 GiB by default): past it a growing write is refused with `422 QUOTA_EXCEEDED`, while reads and transactions that do not grow their database go on. Every node publishes what
  its own objects of each tenant hold to `nodes/<id>/durable-usage` every `durableObjects.usagePublishMs` (an hour) and
  reads the other members', so enforcement uses a lagging estimate, not an exact distributed byte counter; the objects of a namespace or a database
  that is being deleted stop counting.
- **Alarms.** An object's alarm time is in its own database, so it is replicated and recovered with everything else.
  The shard's owner keeps the schedule and fires the alarm as an event of the object's Worker, and a hint beside the
  shard's epochs (`do/<namespace>/shards/<shard>/alarms/<object>`) names each object's next alarm so that the shard's
  next owner knows which objects to wake; every firing then asks the object's own database, which decides -- a stale hint does not authorize a handler whose database no longer has a due alarm. Delivery can be delayed or repeated across failures; handlers should tolerate retries. The hint carries no correctness: a stale one costs
  one wake, a missing one a delay until the object is next opened.
- **Resident objects.** A durable process targets `durableObjects.residentObjects` (4096) resident object trees (`0` removes the limit), evicting idle trees in least-recently-used order, facets before their root. Active trees are protected, so the count can temporarily exceed the target; an object with hibernatable WebSockets keeps them
  connected while it is evicted and is woken by the next message. The state endpoint reports the resident count per
  process, the limit, the evictions, the wakes and how long they took.

On glibc the daemon fixes the mmap/trim thresholds at 128 KiB with no top padding and trims free pages at most once a minute after one minute without own or peer log appends. `memory.daemon.containers` reports retained entries and available payload byte counts: unsealed records are pruned after seal/manifest publication, peer buffers after owner trim/drop, histories at their fixed caps, and usage/auth state at flush/expiry; request-log links without a live invocation lease expire after ten quiet minutes and counter windows after sixty seconds even when idle, while epoch exclusions and known-object/tenant metadata follow their documented lifecycle rather than a fixed entry cap.

On Linux the daemon disables transparent huge pages to keep sparse allocator pages from becoming resident again; the launcher restores the host default for workerd, whose runtime heap policy is unchanged; when the launcher is unavailable, workerd starts directly and inherits the daemon's THP-disable flag.

The state endpoint's `durableLog` shows the confirmation latency (mean, p50, p99), the bytes not confirmed and not
sealed, the segments and the last upload, the peers and the streams to them, the checkpoints, the rebuilds and their
time, `recovery` counters (bounded, owner-silent, late, transferred and unaccounted records, healing progress and failures), the buffer this node holds for others, and this machine's files; `[do-log]` lines record the sealing, the
uploads, the manifests, the snapshots, the rebuilds, a peer taking another's place, and a confirmation that failed with
the records it isolated. A `[do]` line says how long a request waited for a rebuild (`recoveredMs`).

Before preparing another open, the node checks whether the durable process generation last given the object's
files still has an open channel. While it does, the node skips `wal-cut-back`; if the cache open marker was cleared,
it only advances the open generation and writes that marker again. Once that process generation is gone, normal
WAL truncation resumes.
When a durable process reopens a LOCAL object itself, `FROM_OPEN` writes the open marker again without advancing
the open generation. If that write fails, the node breaks access to the object and marks it for repair.
A failed open or close marker write restores the in-memory open flag to its prior value.
Before rebuilding an image, the node resets durable processes only while an open facet is live or a request
has been dispatched and its `FROM_OPEN` has not yet been processed; a closed object's old dispatch alone
does not trigger a reset.

`durableLog.log.walCutBackFailures` counts WAL read/write failures while preparing an object open.
`wal-cut-back-failed` names the object, storage id, facet, step (`read`, `truncate`, `sync` or `remove`),
operation and errno. A short header read reports errno 0 and `readBytes`. The request returns 503
`STORAGE_UNAVAILABLE` with a WAL read/write reason before changing the open generation or position marker;
it preserves shared memory and does not count or log a successful `wal-cut-back`. If truncation succeeded
but its sync failed, the WAL remains truncated. A later request retries normally. Persistent errors keep
the object unavailable rather than automatically starting repair. Have another node take over its shard,
for example by draining this node; the new owner rebuilds from storage and peers.

`objects.hold` and `openCache` name caches that cannot yet serve or were handed to a durable process.
`localRecovery.writeFailed`, `writeFailures`, `startupReady`, `startupMarkerFailures` and `startupCleanupFailures`
expose local durability gates.
`startupClassification` counts `history`, `fenced`, `adopt` and `readFallback`, its segment/old-manifest reads and time,
the full log-read time, and absolute daemon peak RSS in bytes. Every startup names the object, source epoch,
sequence range and byte count in `restored-startup-classified`. Fenced records remain until their original segment
is uploaded; unproven records default to resealing. Historical lookup reads at most 256 deduplicated batches and
256 older manifest revisions, with at most 64 MiB of receipt cache; unread and over-budget evidence is counted.

`durableLog.recovery.healPending` and `healBusy` describe local healing still to finish; `healSnapshots`,
`healFailures` and `healedWalFrames` count its work. `objectFailures` and `object-operation-failed` name failures at
rebuild, healing or request stages. Request failures retain their original status and code. This counter includes
execution errors and alone does not establish SQLite corruption. During an upgrade from an affected older node,
that older node's own takeovers still have its old recovery behavior; use the maintenance procedure in
[the installation guide](guide/install.md#9-upgrades-and-draining) before resuming Durable Object or D1 traffic.

Known recovery limits in 0.5.5: mixed 0.5.3/0.5.4 peers can acknowledge 32–64 MiB records but cannot return
such a record in their old 32 MiB tail page. During mixed operation, losing the fixed owner while only old
holders remain can therefore lose that tail at a bounded cut. The maintenance procedure replaces every node
before traffic resumes. Old versions could also write segments larger than the store client's 64 MiB read
limit; this fix prevents new oversized segments but cannot retrieve those existing segments or invent missing
pages. The SQL payload is below the effective encoded limit and may hit CPU/memory/confirmation limits first.

# Cluster state

> Which machines are in the cluster, what is deployed, where the code is, where a call should go -- how this information is kept across several Servers.

- **Status**: **membership (section 4), deployment state (section 5), code bundles (section 6) and placement (section 7) are implemented** -- the cluster can run by itself; so are per-Worker incremental deployment (section 10) and graceful exit and rolling upgrades of the server (section 11).
- **Prerequisite**: the cross-machine call contract (invocation leases)

## 1. What this design replaces

Without cluster state, the cooperation between Servers relies entirely on manual configuration: a peer list hard-coded in each machine's configuration, no knowledge of who is still alive until a call hits `PEER_UNAVAILABLE`, the same `workers/manifest.json` and code bundles placed on each machine by hand, and a hard-coded static placement table.

## 2. Layering: only a small part needs an authoritative store

### The four layers of deployment state

| Layer | Contents | Mutable? |
| --- | --- | --- |
| Routes | Domain + path prefix -> workerId | **Mutable** |
| Active version | workerId -> the versionId currently in effect | **Mutable** (deploying changes it) |
| Version definition | versionId -> code hash, entry module, Bindings | **Immutable** (enforced by `Catalog` in `server/main.c++`: changing the content under the same version id is refused) |
| Code bundle | Addressed by content hash | **Immutable** |

### The split that follows

| Information | Where it lives | Reason |
| --- | --- | --- |
| **Membership leases** | The authoritative store | Joining and leaving need someone with the final say |
| **Routes, active versions** | The authoritative store | Deploying is a "write" and needs a single write point and atomicity |
| Version definitions, code bundles | Distributed by hash, cacheable everywhere | Immutable: any copy is correct, no consistency problem |
| Whether a peer can be reached right now | Each machine judges locally | By nature a property of "between me and it"; under a partial network partition a globally unified answer would actually be wrong |
| Load watermark | The target machine itself | It is the only authority on its own capacity |
| Call lease (budget, deadline) | The machine executing it | An existing invariant, needing no cluster coordination |

**What makes this split possible** are two things: only stateless execution (no ownership like "an object may live on only one machine"),
and the call contract (when placement goes wrong, `PEER_UNAVAILABLE` can be rerouted safely and `RESULT_UNCERTAIN` is not retried).
So **an inaccurate membership view can only make placement worse, never make calls wrong**.

> If state with ownership (like Durable Objects) is introduced in the future, this premise no longer holds,
> and membership will need strong consistency with protective mechanisms. This design must then be revisited.

## 3. The authoritative store: etcd, replaceable through an interface

### The choice

The capabilities needed: leases that expire automatically; pushing changes from a given revision without gaps; atomic conditional writes over several keys;
easy to run on one machine and to form a small cluster with; an open-source license.

etcd has all of them natively, is a single small binary, and is Apache 2.0 licensed. Among the other candidates,
ZooKeeper is complete but heavier; Consul has used a non-open-source license since 2023; NATS JetStream KV is not strong at atomic writes over several keys;
PostgreSQL is strongest for large data and complex queries, but weakest exactly at leases and change pushing; building our own Raft carries the highest risk.

**Known weaknesses**: the native interface is gRPC, which is costly to bring into the C++ server -- **prefer etcd's HTTP/JSON gateway**,
using the server's existing KJ HTTP client without a gRPC dependency; the burst of synchronization when a large cluster restarts all at once -- add snapshots or peer synchronization when the scale gets there.

### Rules of use

1. **Not on the request path.** Each Server keeps a local copy and reads only the local copy when handling requests.
2. **Statically stable.** When etcd is unreachable, keep serving with the last known state; only joining new machines and deploying are temporarily impossible.
3. **No high-frequency soft state.** Reachability, load and warm sets never go into etcd.

### Two interfaces, two backends

| Interface | File backend (single machine / development) | etcd backend (cluster) |
| --- | --- | --- |
| `Membership` | The static peer list in the configuration | Lease registration + watch |
| `DesiredState` | `workers/manifest.json` | Keys for routes and active versions + watch |

The two interfaces are separate so that each can change its backend later (for example, when the deployment state grows too large for etcd, only this half is replaced).
**The file backend must always remain usable**: single-machine development and testing depend on no external service.

### Comparison: coordinating only through object storage, as celld does

celld, another open-source project, does not use etcd: its nodes coordinate only through object storage -- conditional writes determine each object's single owner, ownership epochs fence off stale writers,
and each node polls a cluster summary every 5 seconds, with no membership protocol and no consensus service.

**Why it works for celld**:

1. celld's authoritative data (each object's SQLite log) is in object storage anyway. With the ownership records in the same place as the data and protected by the same conditional writes, there can be no split into two truths like
   "the coordination service says A is the owner, but B wrote into the storage" -- **that is the fundamental reason it removes etcd**; one component fewer is a side benefit;
2. what it coordinates is the ownership of single objects, for which single-object conditional writes are just enough;
3. it can accept change propagation in seconds and failover in about 20 seconds.

**Against the five criteria etcd was chosen by**:

| Criterion | etcd | Object storage |
| --- | --- | --- |
| Leases that expire automatically | Native | None; simulated with "timestamp + conditional write", depending on clocks |
| Pushing changes from a given revision without gaps | Native watch, milliseconds | None; only polling, seconds; to have no gaps you must design your own change log |
| Atomic conditional writes over several keys | Native transactions | Only single-object conditional writes; several keys must become "immutable content + one pointer updated with a conditional write" |
| Easy to run on one machine, small clusters | One small binary, but quorum, backups and upgrades must be operated yourself | A managed service in the cloud, the least effort; self-hosted it has to be deployed anyway (the code bundles are already in it) |
| Open source and portable | Apache 2.0 | The S3 API is the de facto standard, but implementations vary in their support for conditional writes and consistency, and each must be verified |

**Judgement: not following it now.**

- Our data plane has no state, so the fundamental reason celld removed etcd does not exist here. The code bundles are already in object storage (section 6); the metadata used for coordination (membership, deployment state, the suspect table)
  is small, needs low-latency pushes and needs atomic writes over several keys -- exactly what etcd is good at. The division "large blobs in object storage, small but important metadata in etcd" is itself right;
- migrating would redo the coordination parts of membership, deployment state and the suspect table, and the guarantees they already provide (zero failures during etcd outages, real network cuts, deployment consistency) would have to be verified again, mainly to gain one operational component fewer,
  which has nothing to do with the core positioning;
- the costliest part is membership: failure detection would go from "pushed when the lease expires" to "expiry + polling interval", and placement would hit unreachable machines a few more times during transitions. Correctness is still kept by "reroute when unreachable",
  but it is a real step back.

**The path left open**: the two interfaces can each change their backend anyway. If an operator really does not want to maintain etcd in the future, first give `DesiredState` an object storage backend -- definitions are stored by hash in object storage already,
so only a deployment pointer updated with conditional writes plus polling would need adding, the smallest cost; `Membership` would be considered last, accepting slower failure detection. Before starting, verify the object storage in use
(SeaweedFS, MinIO, R2, S3) for its actual support of conditional writes and read-after-write consistency -- a SeaweedFS signature defect has already been met (see section 6).

**What can be borrowed now**: epoch fencing (our call ids and target epoch checks are already the same idea); and the principle of "a single source of truth" -- if state is ever done,
the ownership records must live with the data and be protected by the same mechanism.

## 4. Membership

### Registration

- At startup, request an etcd lease (configurable TTL) and write `/coworkerd/members/<serverId>` with a **conditional create**:
  **if the key already exists, startup fails** -- preventing two processes from claiming the same identity (which would break the invariant "the lease lives on the machine executing it" and the uniqueness of ids);
- the value holds the **advertised address** (the address others use to reach me, distinct from the listen address), the **startup epoch** (taken from the lease id), and the start time;
- renew periodically; **revoke the lease on a graceful exit**, leaving at once; after a crash, leave when the TTL expires.

### An immediate restart after a crash

A process restarted right after a crash does not remember its old lease, so its conditional create runs into its own old key. If that were judged a conflict, the process could not start until the old lease expired (the default TTL is 15 seconds),
and under a process supervisor that restarts it automatically it would keep failing for about one TTL.
So when a conditional create runs into an existing key, **watch the other lease against a time bound**: take the first observed remaining TTL plus 1 second (the rounding error) as the bound.

- The lease is still there after that time -> it has certainly been renewed -> a real conflict, startup fails;
- it expires before then -> nobody renews it (it is our own "previous life") -> register normally after it expires, with a new epoch.

A remaining TTL seen to rise (someone is renewing) serves only as an early decision. Watching for a rise alone is not enough: the remaining TTL etcd reports is in whole seconds, and a renewal is not necessarily "visible" --
with a TTL of 3 seconds and a renewal every second, the reading stays at 2 almost all the time, and a real conflict took 22-39 seconds to be found, with no upper bound in theory.
With the time bound, detection takes at most one TTL plus 1 second, regardless of the TTL's value.

A known exception: if etcd happens to restart during the observation, it resets the TTL of every lease, which is misjudged as "someone is renewing" --
the result is a refusal to start, erring on the safe side, and the process supervisor just retries.

### Watching

- Watch the prefix `/coworkerd/members/`, **continuing from the revision already read**, and maintain a local membership table;
- when etcd has compacted its history so that continuing fails, **read everything again and then continue** -- never stay on an old view.

### When etcd is unreachable

- Keep the last known membership table and continue serving;
- our own lease may expire meanwhile -- other machines will consider us gone and stop placing calls here;
- after recovery: if the lease has not expired, keep renewing; **if it has expired, register again and get a new epoch**.

### The epoch goes into call ids

A call id is `<serverId>.<epoch>-invocation-<sequence>`. The sequence lives only in process memory and **repeats after a restart**;
etcd's lease ids are never reused, so with the epoch the id is unique across restarts too.
Under the file backend, the epoch is a random value generated at startup.

### Relation to forwarding

- Peer addresses come from the membership table instead of the static configuration;
- **a target not in the membership table = certainly not sent = `PEER_UNAVAILABLE`** (can be rerouted safely);
- whether it can be reached is judged from the local peer connection state.

### Membership leases: results

Measured with a single-node etcd 3.6.15, reached through the HTTP/JSON gateway, **with no gRPC dependency**;
the single-machine mode keeps the file backend.

| Situation (three runs each, TTL 3 seconds) | Measured |
| --- | --- |
| Normal join | About 1 second; 8 cross-machine calls over the two machines without any static peer configuration |
| Normal exit | Removed in **45-53 ms** |
| Crash | Removed in **3.3-3.45 s**; calls within the window get `PEER_UNAVAILABLE` |
| Name conflict, judged at once (for the time bound, see below) | The later one fails to start within 61-80 ms, and the original member's epoch is not overwritten |
| etcd down as a whole for about 6.7 seconds | **Zero failures before, during, at recovery and after**; both machines get new epochs after recovery |
| etcd down briefly for 2.7 seconds (< TTL) | Renewal resumes, **the epoch is unchanged** |
| etcd unavailable at startup | Does not serve; ready 0.4-0.7 s after recovery; fails explicitly on timeout |
| Only S2 loses etcd | Removed in 3.1-3.4 s; S2 still serves requests reaching it directly; a new epoch after reconnecting |
| History compacted while disconnected | Detected within 0.3-0.6 s and read in full again; the final revision agrees with etcd |
| Ids after restarts | S1 restarted 3 times in a row: **0 duplicates** with the epoch in the id, 18 without it |

**Restarts after a crash and conflicts** (see "An immediate restart after a crash"): a real conflict with a 3-second TTL is decided in about 5.2 seconds (upper bound 6.75 seconds, against 22-39 seconds when only watching for a rise); with a 15-second TTL in about 5.0 seconds (decided early on seeing a renewal).
A margin of 2 seconds; etcd's measured delay in handling expiry was at most 519 ms. Three immediate restarts after a crash all joined normally.
With the default TTL of 15 seconds (three runs each), a restart within about 250 ms after a crash registered successfully after 14.97-15.22 seconds with a new epoch, not serving until then;
a real conflict failed startup after 4.7-5.0 seconds, and the original member's address and epoch were not overwritten.

#### A lease follows its holder's local clock and is never revived once expired

Measurements show that **leases do not expire while etcd is down**: after a restart, etcd restores their remaining time (with lease checkpoints on).
That means after an outage longer than the TTL, the old lease **may still be valid** on etcd's side.

The implementation adopts the rule: **once the local renewal deadline (by our own monotonic clock) has passed, that epoch is never revived**,
even if the old lease still exists on etcd's side. The reason is that a machine cannot tell "etcd is down" from "only I cannot reach etcd" --
in the latter case its lease on etcd's side really has expired, and others may already regard its membership as ended. It can only assume the worst.

So an outage longer than the TTL gets a new epoch -- not because the lease on etcd's side expires (it may not), but because of this rule.

Lease checkpoints are an experimental etcd option. They affect only the speed of detection (without them, an etcd restart gives every lease a full TTL), not correctness.

#### Three details of the implementation

- **Idempotence when the result of a commit is lost**: when a retry finds the key already exists, and the key hangs on our own lease with the same content, it is taken as our own write, not misjudged as a conflict;
- **atomically taking over our own old key**: one conditional transaction moves the key from the old lease to the new one, **with no moment in between at which the key does not exist**
  (deleting and then creating would let other machines briefly see "I disappeared", breaking static stability); a competitor's lease can never pass this comparison;
- **connections are reused by "address + epoch", and the epoch is checked again before sending**: when the target restarts with a new epoch, an old connection is not handed to the new instance;
  found before sending, it reports `PEER_UNAVAILABLE` (certainly not sent).

## 5. Deployment state

- Deployment has two steps: **first publish the immutable parts** (version definitions, code bundles), **then flip the pointers** (routes, active versions);
- when several pointers must change together, use etcd's **atomic multi-key writes**;
- each machine applies changes **monotonically** by etcd revision, so a deployment takes effect atomically on each machine;
- publishing and flipping need a minimal deployment interface (writing etcd). That is the minimal control plane "running on its own" needs, not a complete control plane.

### The starting point

With Bindings that **hard-code the target version** in the version definition (`workerId` + `versionId`), "which versions a tree uses" is determined by the root's definition; once a Binding follows the target's active version, this determinism has to come from something else.

### Version semantics

**A Binding points to the target Worker and follows the target's active version**, broadly the same as Cloudflare's service bindings:
after a new version of B is deployed, the Workers calling B reach the new version without being redeployed. The Binding in a version definition records only `{name, workerId}`.

On top of that comes **consistency within a tree**: **one call tree sees only one deployment state.**
So the deployer's mental model holds -- requests that started before the deployment all use the old, those that start after it all use the new, and no request sees a mix of new and old.
When the interfaces of A and B change together and flip together, this is a correctness requirement, not an optimization.

### Why it cannot be resolved lazily

Resolving lazily -- "the first time a Worker is called, resolve its active version, record it in the lease, and pass it down the tree" --
**only guarantees consistency along one chain of ancestors, not across the whole tree**:

```text
A (root, S1) ─┬─ first calls B ──→ B ── calls C: resolves C = v1 now, recorded in B's lease
              └─ then calls D ───→ D ── calls C: D's lease comes from A and has no C; the deployment has flipped by now, resolves C = v2
```

C's lock is recorded in B's lease, and the sibling branch D cannot see it. For D to see it, it would have to ask the root -- exactly the "asking back to the root" that invocation leases are designed to eliminate.

A placement cache can resolve lazily because **placement does not require consistency across the tree** (the same B landing on different machines in two branches is perfectly fine);
versions do. The two look like the same mechanism but are not.

### The approach: one snapshot of the reachable closure at the root

- When a root call starts, take the entry Worker's **reachable closure** from the machine's current deployment snapshot: walk the Bindings of the active versions once,
  giving all the Workers this tree **might** use, each with its active version at that moment -- this is the **version lock table**;
- the lock table goes into the lease. Child calls on the same machine share the same one (just a reference count); across machines it travels with `RemoteInvocation`;
- **downstream always executes by the lock table, never by its own active pointers.** Even when a downstream machine's deployment view lags behind or runs ahead
  (disconnected from etcd, delayed watches), the tree stays consistent -- static stability and consistency within a tree hold at the same time here;
- the closure depends only on the snapshot, so it is computed in advance for each Worker when the snapshot takes effect, and a root call is just a table lookup;
- each lock: `workerId`, `versionId`, `contentHash`, `definitionHash` (the hash of the version definition itself, see below).

**The closure's size has an upper bound** (64, the same constant shared by the deployment tool and the server, so that the two cannot judge differently); a deployment over the bound is refused by the deployment tool, and the server checks it as well.
The bound's cost has to be measured: every cross-machine call carries the lock table. In reality a Worker has few Bindings, and a closure is usually in the single digits;
cheaper approaches (carrying only the target subtree's closure, caching lock tables by hash) wait until measurements show they are necessary.

Version locks are determined once at the root, and the scope of the lock is the reachable closure.

**Forward compatibility**: for canary releases later (splitting the active version by ratio), the root just picks one version by ratio and writes it into the lock table, and consistency within the tree is kept naturally.

### A version's identity is the hash of its definition

`versionId` is a label for humans. The real identity is **the hash of the version definition** (`definitionHash`: SHA-256 over the definition's canonical bytes;
the definition includes the workerId, versionId, compatibility date, entry module, code hash and the list of Bindings).

- The lock table carries `definitionHash`, and the receiver executes only after checking that its local definition's hash agrees; otherwise `VERSION_UNAVAILABLE` (certainly not executed).
  So even if someone bypasses the deployment tool to change a published definition and two machines each see a different one, they will not each execute their own within one tree;
- the code cache key on the workerd side has to include `definitionHash` (see `runtime/runtime.js`) --
  two definitions with different compatibility dates or entry modules cannot share one loaded copy of the code;
- labels are unique within a Worker; once a label has corresponded to some definition, it can never change (keeping `Catalog`'s immutability check).

### The layout in etcd

```text
/coworkerd/deploy/definitions/<workerId>/<versionId>   version definitions (immutable: created only, never overwritten)
/coworkerd/deploy/active/<workerId>                    {versionId, definitionHash} (mutable pointer)
/coworkerd/deploy/routes                               the whole route table (mutable)
/coworkerd/deploy/generation                           deployment generation (incremented by each deployment, also an optimistic lock)
```

- The active pointer carries `definitionHash`: what is flipped is "the content the deployment tool checked". If a definition is changed between publishing and flipping, the server's check fails;
- **the route table is one key**: route matching is over the whole table anyway (longest prefix), and so is validation (duplicates, pointing to a Worker that does not exist);
- **all deployments are serialized by the generation**: the flip transaction is conditioned on the revision of the generation key and fails if it differs. Deploying is infrequent,
  and serializing buys "the state the deployment tool checked is exactly the state being replaced". Concurrent deployments per Worker wait until there is a real need (see section 10).

**Version definitions are kept in etcd for now.** Section 2's split is "definitions are distributed by hash" -- a definition is small (a few hundred bytes),
but their number grows with deployments, and at scale they should be distributed by hash together with the code bundles (object storage holds only a copy by hash so far, see section 6). The benefit of keeping them in etcd:
definitions and pointers arrive consistently under the same revision -- once a machine has applied a flip, it certainly has the definition flipped to.

### The server's application rules

1. **Apply in batches**: one etcd watch response contains several **complete** revisions (etcd guarantees that the events of one revision are not split across responses);
   after the local mirror is updated, compile a snapshot of the full state once again;
2. **compiling is validating**: definitions are legal and their hashes match; active pointers point to existing definitions with matching hashes; every Binding target of an active version has an active version;
   routes are non-empty, without duplicates, and all point to Workers with active versions; no closure exceeds the bound; labels are immutable;
3. **if invalid, keep serving with the previous valid snapshot**, reporting the refused revision and the reason in the state endpoint. A wrong write can only make "the deployment not take effect",
   never "requests fail";
4. **snapshots are replaced as a whole**: a root call reads a reference to one snapshot, so a deployment takes effect atomically on each machine;
5. **revisions are monotonic**: never apply a revision older than the one already applied;
6. **definitions are kept locally by hash and only ever added**: definitions seen once are immutable and never go stale. Keeping them lets in-flight trees locked to old versions
   still execute after the definitions are deleted on etcd's side (deletion and reclamation come later).

### Code

The deployment state does not decide how code is distributed (that is section 6); code is always checked against `contentHash`. What the deployment state changes is **the timing**:

- with the etcd backend, code is **made available before execution** (read and hash-checked, cached on success), rather than reading every version at once when loading the deployment state;
- missing or a mismatched hash -> `VERSION_UNAVAILABLE` (certainly not executed, reroutable). Missing code **does not block a snapshot from taking effect** --
  whether the code is on this machine is this machine's business, while the deployment state is the cluster's. Code bundles (section 6) turn "missing means refuse" into "missing means fetch";
- the file backend reads and checks everything on reload, and any mismatch fails the whole reload. A convenience for single-machine development and testing.

### The receiver does not know the locked definition

The lock table makes downstream "execute by the root's view", provided that downstream **knows** the locked definition. A lagging machine may not --
for example, a new version was published and flipped while it was disconnected from etcd. It then answers `VERSION_UNAVAILABLE`, the details saying "unknown definition (deployment state lagging)",
certainly not executed. Fetching the definition and the code by hash, from the caller or from object storage, lets it execute instead (section 6 fetches them from object storage).

### The deployment tool

- **Declarative**: its input is the existing manifest format (the same one the file backend reads), representing the complete desired state;
- **two steps**: first publish the definitions (each "create only if absent"; skip if it exists with the same bytes; abort if it exists with different content),
  then flip (the active pointers, the route table and the generation in one transaction, conditioned on the generation);
- **it shares the same compilation and validation code with the server**: what the tool considers legal the server certainly considers legal, and what the tool refuses the server certainly refuses too.
  Only writing etcd directly, bypassing the tool, can produce a state the server refuses, in which case the server keeps its old snapshot;
- it is not responsible for getting the code to each machine (see section 6); the code is in place before the tool flips -- exactly the order "publish the immutable parts first, then flip".

### Deployment state: results

**The core property holds: one call tree sees only one deployment state.** Each run has 32 closed-loop clients, with A, B and C flipped together between `v1` and `v2` in one transaction 20 times;
in the cross-machine topology, S2's application of deployments is artificially delayed by 100 ms, and the ingress alternates between the two machines.

| Topology | Mode | Trees (three runs) | Mixed trees (three runs) | Trees spanning a flip, per run |
| --- | --- | --- | --- | --- |
| Single machine | Control (resolved call by call) | 4,994 / 5,052 / 5,216 | **476 / 480 / 456** | About 620, of which 456-477 went wrong |
| Single machine | Formal (locked at the root) | 4,811 / 4,919 / 4,933 | **0 / 0 / 0** | About 620, all correct |
| Cross-machine | Control | 7,911 / 7,923 / 7,775 | **1,919 / 1,935 / 1,931** | About 620, of which 451-551 went wrong |
| Cross-machine | Formal | 7,072 / 7,102 / 7,122 | **0 / 0 / 0** | About 620, all correct |

The trees that "went wrong" in the control mode are real call failures (a contract header mismatch returning 412), not just inconsistent version records.

**Consistent even when the deployment views disagree.** Flipping to `v2` while S2 was disconnected from etcd: the trees entering at S1 were all `v2`, those entering at S2 all `v1`,
12 + 12 trees in each of three runs, zero failures. The two machines' execution records give direct evidence: S2, whose own active version was still `v1`, executed `b@v2` 12 times,
and S1, already at `v2` itself, executed `c@v1` 12 times -- downstream executes by the lock table, not by its own active pointers.

**Failure paths (three runs each unless noted):**

| Situation | Observation |
| --- | --- |
| The receiver does not know the locked definition | 409 `VERSION_UNAVAILABLE` "unknown definition", B's new version executed 0 times on S2; after reconnecting to etcd the same request succeeds |
| Code missing / hash mismatch | 409 "code unavailable: missing / hash mismatch"; once the file is in place, the next call in the same process (unchanged PID) succeeds |
| Six kinds of invalid writes bypassing the tool (once each) | Both machines refuse the corresponding revision, the six reasons distinguishable, the generation unchanged, all calls succeed meanwhile; the tool refuses all five kinds it can express, with etcd's revision unchanged |
| Starting a new machine while etcd holds an invalid state | It does not serve, and the log states the reason; ready 54 ms after the fix |
| Two deployments at the same time | Exactly one succeeds; the other reports "another deployment in the meantime" and changed no pointer |
| etcd unreachable while deploying (once) | The deployment fails with the revision unchanged; service continues as usual; deploying succeeds after etcd recovers |
| The deployment watch's history compacted | The failure to continue is detected, read in full again, applied up to the latest generation; zero failures during and after |

The example from "Why it cannot be resolved lazily" behaves as designed: under the old root, A first calls B->C, then, after C alone is flipped, calls D->C,
and both branches still see C's old version; under a new root both branches see the new one. Old trees in flight can still complete after etcd deletes the old definitions (the local index only grows),
and when the lock fields are tampered with, both paths (Binding, peer) refuse with `VERSION_PIN_MISMATCH` with zero executions.

**A real network cut longer than the TTL** (3.6-3.8 seconds, with a 3-second TTL; a Server running two watches at once): while disconnected, S2 handled direct requests with the last known deployment (generation 1),
and after reconnecting applied the deployment made meanwhile (generation 2).

**The cost of the lock table**: about 192 bytes per lock (928 bytes for 3 locks, 12,640 bytes for 64). With no artificial waiting (8 closed-loop clients, 512 samples in each of three runs), the single-machine p50 difference is -0.01 ms, indistinguishable;
the cross-machine closure of 3 adds 0.23 ms to p50, just over the variation between runs (0.20 ms); a closure of 64 adds 0.64 ms, within the variation (0.67 ms).
**The lock table's overhead is below this measurement's resolution (about 0.2-0.6 ms).**

**A side finding**: the runtime creates a call capability for **every** Binding on each call (about 30 µs each), whether used or not -- a Worker with 61 extra Bindings it never calls is about 2 ms slower.
Unrelated to the lock table; creating them on demand is a later optimization.

#### Problems found

- Relays also need to know the locked definition (conservative, hardly ever hit under static placement);
- the local definition index only grows (reclamation comes later); the code cache, which only grew as well, is bounded by section 6's local disk cache;
- error details are KJ's raw strings, hard to read.

## 6. Code bundles

### The choice: object storage, the S3 API

| Candidate | Conclusion |
| --- | --- |
| Put the code into etcd too | Not suitable. etcd is designed for small metadata (by default a single write is limited to about 1.5 MB and the total to 2 GB, recommended at most 8 GB), and every machine mirrors all of it |
| No central store, machines passing code to each other | The highest risk: it means building our own distributed storage (replicas, repair, where the originals are when all machines are down). The original of the code must exist independently of the machines running it |
| **Object storage, the S3 API** | Adopted. The standard approach for data that is "large, immutable, fetched by name" |

- **Only "store by name, fetch by name" is used.** The S3 API is the de facto standard, compatible with every cloud and many self-hosted products;
  like etcd it sits behind an interface (whose semantics are "fetch immutable content by hash"), and the specific product can be replaced;
- **An OCI image registry (such as zot)** can also store and fetch by hash, and has a signing ecosystem. Not used now: our version definitions already act as the "manifest",
  OCI's manifest layer would duplicate them, and pushing is more cumbersome. When signing and provenance tracing are needed, or deploying into environments that already have an image registry, add an OCI backend,
  and zot can still sit on the same S3 underneath;
- **SeaweedFS is used for testing** (Apache 2.0, with an S3 API in a single process). Not MinIO: its community edition stopped publishing official images and binaries from October 2025,
  entered maintenance mode in December, and its repository was archived in 2026. Production uses whatever S3-compatible service the operator already has.

### Layout and identity

```text
bundles/<contentHash>          code (currently a version has only one entry module)
definitions/<definitionHash>   copies of version definitions (the same bytes as in etcd)
```

- **The key is the hash**: the content is immutable, any copy is right, its authenticity is known by checking after fetching, and a cache never goes stale;
- **version definitions still go by etcd**, arriving with the pointers under the same revision (section 5's atomic switch depends on this). The copies in object storage are only copies,
  for lagging machines to fetch by hash;
- **definitions are not moved out of etcd**: there is no need now -- definitions are small, and with reclamation of old definitions they can stay in etcd for a long time;
  move them when the scale really gets there.

### Deployment order

Upload the code and the copies of definitions -> publish the definitions in etcd -> flip. When an upload fails, etcd is not changed at all -- this is how code bundles carry out "publish the immutable parts first, then flip the pointers".

### The server: a local disk cache

1. **Cache only content that passed verification**: write to a temporary file first, check the hash, then rename atomically. **Never execute unverified bytes at any time** -- the one inviolable invariant of code distribution;
2. **only the index lives in memory** (hash -> size, last use), without keeping code content resident;
   the check before execution looks only at the index and goes ahead on a hit, without touching the disk;
3. **fetch on a miss**, at most one fetch at a time for the same hash (single-flight), with other requests waiting for the same result;
   the wait is bounded by the call's deadline, and on timeout `VERSION_UNAVAILABLE` (certainly not executed), while the fetch itself continues for later requests;
4. a failed fetch, a missing object, a mismatched content hash -> `VERSION_UNAVAILABLE`, with distinguishable reasons; bad content never enters the cache;
5. **the capacity has a limit**, evicting the least recently used; a single object over the limit is refused with the reason stated;
6. once a deployment takes effect locally, **prefetch** the code of the newly active versions (with a concurrency limit, retrying with backoff on failure), so that normally it is ready before the first request arrives;
7. the cache survives restarts; a file corrupted on disk is found by the check when used, invalidated and fetched again;
8. the channel supplying code to workerd (`SourceService`) also fetches on a cache miss and does not fail because of eviction.

The cache's capacity is a local resource, to be brought under resource governance later.

### Lagging machines fetching definitions

When the machine does not know a definition in the lock table: if it does not know the label either -> fetch the definition's copy from object storage by `definitionHash`, verify it and add it to the local index, then fetch the code and execute;
if it knows the label but the hash differs -> still refuse (inconsistent definitions); if object storage is unreachable too -> refuse (unknown definition).
This fills in section 5's situation where "a lagging machine can only refuse".

### When object storage is unavailable

Cached versions run as usual; uncached versions are refused explicitly; deployments fail with etcd unchanged. The same principle as when etcd is unavailable:
the store is not on the request path (after prefetching), and its problems affect only "new things", never what is running.

### Analyzed and deliberately not done

- **Delaying the application of a snapshot until the local code is ready** ("take effect only once able to serve"). Not done: the deployment tool puts the code into the store before flipping,
  so the server failing to fetch happens only in a local fault "between it and the store"; a fetch before execution waits until the deadline and fails only if it really cannot fetch;
  local faults are handled by placement (section 7; a refusal on the target side can be rerouted). Delaying by snapshot would also force a new machine to fetch all code before a cold start;
- distributing code between machines (a scale optimization for many machines fetching the same bundle at once);
- reclaiming old code bundles; multi-module Workers; encrypted transport.

### Code bundles: results

**Prefetch scheduling**: only uncached versions are prefetched, and a coroutine is started only when the queue is not empty. Otherwise, when the total number of active Workers is less than the prefetch concurrency
(default concurrency 2, so it always happens with only one Worker), the KJ coroutine finishes synchronously, the count drops back, the startup loop rebuilds endlessly, the event loop is fully occupied,
and the Server cannot start.

**Both invariants hold.**

- **Never execute unverified bytes**: the cache accepts only content that went through "write a temporary file -> verify -> rename"; the bytes handed to workerd are verified once more before returning;
  and there is one more check before the runtime compiles;
- **object storage problems affect only new things**: while the store was down completely, all 2,018 calls of the running versions over three runs succeeded;
  deployments meanwhile failed with etcd's revision unchanged; after the store recovered, deploying worked without restarts.

**Failure paths (three runs each unless noted; the number of storage requests per key is counted from a fault injection proxy's logs):**

| Situation | Observation |
| --- | --- |
| Only one machine cut off from the store | The other machine works normally; trees that need it to execute a new version are refused explicitly with zero executions; after reconnecting it succeeds without a restart |
| Object missing / content replaced | Refused with "object missing" and "content hash mismatch" respectively, bad content never entering the cache; succeeds after recovery |
| Transfer cut midway (TCP disconnected after 16 KiB) | This call is refused in about 195 ms; no partial file left in the cache directory; the next one succeeds completely |
| Fetching slower than the deadline (256 KiB/s) | Refused 8-12 ms after the deadline; the fetch continues in the background (about 4.2 seconds) and enters the cache, and later requests fetch nothing |
| 32 requests missing the same code at once | Exactly 1 read of that key on the store side, all 32 succeed; when the fetch fails, all 32 get the same reason, still with 1 read |
| The cache limit (3 MiB) | Disk use exceeded the limit by at most about 2.4 KB (within "the limit plus one object in flight"); after eviction, calling again fetches again; a corrupted cache file is found and invalidated |
| Lagging machines fetching definitions | With S2 disconnected from etcd and itself still at the old version, it fetched the definition and the code from the store and executed successfully; with the store disconnected too, it refused explicitly |
| Prefetch edge cases (four situations) | Only one Worker, concurrency above the number of Workers, everything already cached, only one Worker changed: all normal; 126-130 samples of the state endpoint during prefetching with zero failures, the slowest 61 ms |

**The cost of a cold start**: the first call (needing a fetch) takes about 37-45 ms for 64 KiB / 1 MiB and about 84-105 ms for 8 MiB; calls after that 2-3.5 ms.

**Other notes**: SeaweedFS 3.97, when checking header signatures, wrongly requires write permission for read operations, so reads use query signatures instead, which are standard SigV4 as well (with the source reference);
at startup every file in the cache directory is read and verified, so startup gets slower with a very large cache; reads and writes of staging files happen synchronously on the event loop;
fetched objects are cached whole in memory (bounded by the cache limit). All acceptable at the current scale.
The failure result of a single-flight fetch is shared by all waiters, including requests arriving only after the store has recovered.

## 7. Placement

- **Candidates = members ∩ reachable from here ∩ willing to accept**;
- **the home machine (Home) by rendezvous hashing**: each machine computes it from "workerId + the current member list" on its own;
  the same inputs give the same result, without coordination; when membership changes, only a few Workers change their home;
- **load is guarded by the target side**: in the yellow / red band it refuses before execution (safely reroutable),
  piggybacking its own watermark on the response to reduce later futile attempts;
- the rest: local first, back to the Home with some probability, reservation for nested calls.
  Cost input: a nested call across machines takes about 0.28 ms more than locally.

### The starting point

Without placement, **the target's refusals before execution** (version unavailable, hops exhausted and the like) come back wrapped as "a dispatcher that returns an error" --
so the sender **cannot tell at the protocol level whether the target took the call or refused it, and cannot reroute based on it**; this is the first gap placement fills.

### Decision flow

For one Binding call (target B), on the machine X where the caller is:

1. B has a static override (`placements` in the configuration: a listed Worker goes to the named machine) -> execute by the override, semantics unchanged (an operations tool, also used by tests);
2. sort the candidates by B's rendezvous hash; the first is the Home; X itself is a candidate at its own rank;
3. **B is hot on X, and X admits it** -> execute locally; but with probability `p` go to the Home instead (contraction, see below);
4. otherwise try the candidates one by one in order: **refused or certainly not delivered -> the next one**; **uncertain result -> no rerouting**;
5. the number of attempts is bounded by the lease's placement hops; if all fail -> `PLACEMENT_EXHAUSTED` (certainly not executed), with each candidate's failure reason attached.

Ingress requests take part in placement through the same flow, with one change to step 3 (see "Ingress placement: design" below).

### Rendezvous hashing

`score(server, worker)` = the first 8 bytes of `SHA-256(workerId ‖ 0x00 ‖ serverId)`; the highest score is the Home.
Each machine computes it with its own membership table; the same table gives the same result, without coordination. When a machine leaves, only the Workers that belonged to it
(about 1/N) change their Home; when one joins, about 1/(N+1) of the Workers move to it.

### "Hot" is judged by the server

workerd has no interface for evicting isolates, so the server cannot know whether an isolate is still there.
Placement uses the server's own record: **the last time a version (by `definitionHash`) executed on this machine; beyond the idle period it counts as cold**.
This is an approximation for placement; once real eviction is implemented, the eviction events take precedence, with the interface unchanged.

### Admission: refuse rather than queue

- **Deadlock of nesting within the same pool** only happens when "waiting for a slot". This design's admission **refuses immediately**:
  the refusal happens before execution, certainly not executed, the caller can reroute, and nobody waits for anybody;
- **tree roots are refused before nested calls**: ingress requests are refused when the local in-flight count reaches `rootLimit` (503, certainly not executed),
  while nested calls are refused only at `nestedLimit` (`nestedLimit > rootLimit`). The margin in between is kept for trees already admitted --
  under overload, new trees are held off first, rather than letting trees that have started fail halfway. This is the form "reserving slots for nested calls" takes under refusal-style admission;
- **watermark bands** (the watermark interface): green -> normal; yellow -> no longer admit Workers **cold** on this machine (hot ones admitted as usual); red -> admit no new nested or forwarded calls.
  Here the watermark's only source is a test switch; the real watermark plugs into the same interface later;
- **refusals piggyback the target's own state**: the reason (concurrency / watermark), the band, the in-flight count and limit, a suggested avoidance duration. Within the avoidance duration the caller **moves it to the end of the candidates**
  -- not removing it: when all candidates are in their avoidance period, they are still tried one by one.

### Protocol: refusals must be explicit

The result of `openInvocation` gains a "refusal" branch (code, reason, band, in-flight count and limit, suggested avoidance duration); a refusal returns no dispatcher.
**Rerouting happens only in three situations**: the connection cannot be established, the target refuses explicitly, the target is no longer in the membership table. All three are "certainly not executed".
`RESULT_UNCERTAIN` is never rerouted.

Rerouting happens before the request body flows out: the sender waits for the peer's `openInvocation` answer before handing the dispatcher to workerd anyway,
and at that point not a single byte of the request body has been sent.

### Conservation

A child call's budget is **allocated only once** at the caller; a reroute carries the same budget, lock table and deadline. A refused attempt creates no lease at the target.
Each attempt decrements the hops by one, so neither loops nor unbounded rerouting are possible.

### Contraction: back to the Home with some probability

Even when hot locally, go to the Home with probability `p`. Machines with sparse traffic therefore get through the idle period more easily
and turn cold, after which their calls to B all go to the Home -- **replicas converge onto a few machines without a centralized contraction controller**,
so replicas can contract once traffic drops. The complete "spread -> contract" process is measured below, with `p = 0` as the control.

### No placement cache per tree

A placement cache "resolved lazily + accumulated with the lease" would replace **a remote placement query on every call**
(a remote selector adds about 0.035 ms and cannot make real decisions). Placement is a local computation -- the local membership table, the local heat record,
a few hashes; there is no remote query to save; and placement does not require consistency within a tree (the same B landing on different machines in two branches is fine). Not done.

### Not covered here

- A local fast path inside workerd (nested calls not going through the server): it involves accounting for budgets inside workerd; a separate topic;
- the second level of placement (several workerd processes on one machine);
- the real watermark and real eviction.

### Placement: results

**Only what certainly did not execute is rerouted.** Among 22,850 dynamic placements measured, there were 0 reroutes after "result uncertain".

- **Certainly not delivered -> reroute**: the Home's server process was stopped (while still in the membership table). The first attempt fails to connect and the reroute succeeds; B and C each executed exactly once;
  the next call within the avoidance period skips the Home directly;
- **uncertain result -> no reroute**: the Home admitted a slow B and was then killed; the caller got `RESULT_UNCERTAIN`, and B executed only that once, on the Home.

**Home assignment is consistent**: the candidate order of 1,000 Workers, recomputed from the definition with an independent implementation, exactly matches what the three machines give. When S3 left, exactly the 321 under its name
(32.1%) changed their home, and the rest did not move at all; after S3 came back, all returned.

**Tree roots refused before nested calls, against a control** (1,800 requests per group per run, all three machines overloaded):

| | Trees admitted | Completed | Failed at a nested call | Tree roots held off (zero executions) |
| --- | --- | --- | --- | --- |
| Control (the two limits equal) | 719-736 | **105-118 (about 15%)** | 601-631 | 1,064-1,081 |
| Formal | 532-539 | **All completed** | **0** | 1,261-1,268 |

The control group admitted more trees but completed only about a seventh -- refusing indiscriminately under overload amounts to doing half the work already started and throwing it away.

**Spreading and contraction, against a control** (the Home is S2; first the Home is put in red for 20 seconds so that B spreads to S1 and S3, then each of the three machines calls B once a second, observed for 120 seconds):

| | B's executions on non-Home machines during contraction | The last non-Home execution |
| --- | --- | --- |
| p = 0 (control) | 240 / 360 per run, every second of the 120 seconds | At second 119, not converging |
| p = 0.5 (formal) | 10-16 per run | At seconds 12-14, only on the Home afterwards |

Spreading caused by real concurrency saturation (rather than the watermark switch) likewise converged about 5 seconds after the load dropped: replicas contract once traffic drops.

**Other failure paths**: yellow refuses only cold versions and admits hot ones; all three machines red -> `PLACEMENT_EXHAUSTED` with the reasons of the three candidates, zero executions; stopping early when there are fewer hops than candidates;
the target's epoch changed -> explicit refusal and reroute; in-flight counts return to 0 after overload, cancellation and execution failures; budgets stay conserved when the first target refuses and the call executes after the reroute,
and a reroute allocates no second budget.

**Three rules of the implementation**:

1. "Result uncertain" is never reported with the wording of "refused before execution" (that wording would mislead investigations);
2. the target's explicit old-epoch refusal is a refusal, not a connection failure: the caller avoids the target for the duration the other side suggests, without accumulating exponential backoff;
3. the admission pre-check of local heat is read-only and does not count as a refusal.

#### Problems found

1. **Dynamic placement showed an overhead of about 1.5 ms on the call path.** In both modes the execution locations are exactly the same (A on S3, B and C on S2),
   yet the dynamic mode's p50 rises from 7.5 ms to 8.4-9.9 ms, beyond the variation between runs; the placement decision itself is only about 3.5 µs (p50).
   It comes mainly from logging in the test environment: with logs written to a container-local volume, going from a static override to dynamic placement adds about 0.34 ms (see "Problems found" under "Ingress placement: results");
2. ingress requests executed on the machine that received them (addressed by ingress placement, below); the real watermark and a local fast path inside workerd are not part of this placement;
3. **the `destinationEpoch` check was too strict**: after an etcd outage longer than the TTL, the same process registers again with a new epoch,
   and calls carrying the old epoch were refused, breaking membership's static stability guarantee of "zero failures throughout an etcd outage" (10 of 30 repetitions failed).
   The fix: the target accepts any epoch this process has held.

### Ingress placement and local execution after the Home refuses

Two directions, compared with Cloudflare's design
([Eliminating Cold Starts 2: shard and conquer](https://blog.cloudflare.com/eliminating-cold-starts-2-shard-and-conquer/)).

**1. Ingress requests take part in placement -- placement's first priority.** If only Binding calls are placed and an ingress request executes on whichever machine it enters,
placement is half-finished. The first thing placement has to solve is **cold starts of light-traffic applications**: Cloudflare forwards incoming requests to the Worker's home machine;
only about 4% of requests are forwarded this way, yet the eviction rate drops tenfold and the cold start rate falls from 0.1% to 0.01%.

- Ingress requests and Binding calls follow **the same placement rules** (home machine, hot locally, handling after a refusal, see below);
- the invariants of the call contract and of placement continue to hold: only what certainly did not execute is rerouted; streaming and cancellation; tree roots refused before nested calls.

**2. Local execution after the home refuses -- moving closer to Cloudflare.** Cloudflare's original: when the home machine refuses, the machine that received the request cold-starts it locally,
"and does not forward this Worker again until its traffic is low enough for it to be evicted". The decision flow above goes to the second candidate instead, a deviation without a stated reason. So:

1. The home refuses or cannot be reached -> **execute locally** (when local admission allows);
2. after executing locally the Worker becomes hot locally and keeps executing locally, returning to the home once idle (the existing "hot locally" mechanism);
3. the local machine refuses too -> continue down the ranking.

Two premises: 1. **mature idle eviction is a precondition for production** -- the design follows the target state, and eviction belongs to
[process isolation and resource governance](process-isolation.md); 2. with reasonable capacity planning, refusals by the home should be rare, so **the refusal rate must be observable**,
and so must the cost of cold starts when refusals happen.

Why local execution: executing locally saves a hop; overflow traffic disperses naturally by where the requests came from; there is no chain of refusals in which the second and third candidates are overwhelmed one after the other.
No data supports "going to the second candidate" being better.

**Two deviations from Cloudflare:**

| Deviation | Reason | Disposition |
| --- | --- | --- |
| **Keeping "back to the Home with some probability"** (`p`) | Cloudflare gathers replicas by eviction under memory pressure; our "heat expiry" triggers only when completely idle. Measured (see "Placement: results"): with steady traffic of one request per second per machine, `p = 0` does not converge within 120 seconds, `p = 0.5` converges within 12-14 seconds | Kept, with a small default; once real eviction is in place, reassess whether it is still needed |
| **Asking whether it can accept before sending the request body** (explicit refusal) | Cloudflare sends optimistically: the request carries a lazy local execution entry of the sender, and on a refusal the other side returns this entry as it is, so the bytes already sent loop back with zero extra round trips. We wait for an explicit answer first, one more round trip within the data center per cross-machine call; in return, the judgement "certainly not executed" is simple and testable | Recorded as a performance optimization; adopt Cloudflare's lazy entry return when needed |

### Ingress placement: design

**One decision flow, shared by ingress and Binding.** For a target B, on the deciding machine X (ingress request: the machine that received it; Binding call: the machine where the caller is):

1. B has a static override -> effective only for Binding calls, semantics unchanged (see "Static overrides and the switch" below);
2. B is hot on X and X admits it -> execute locally; go to the Home instead with probability `p` (not applicable when X is the Home) -- unchanged;
3. otherwise the order of attempts is **Home -> local X -> the other candidates by rank** (when X is the Home: X -> the others by rank); candidates in their avoidance period move to the end without being removed -- unchanged;
4. refused or certainly not delivered -> the next one; uncertain result -> no reroute -- unchanged;
5. each attempt uses up one hop; all failing -> `PLACEMENT_EXHAUSTED` (certainly not executed; ingress requests get 503) -- unchanged.

The only difference from the decision flow at the start of this section is in step 3: the local machine moves from "at its own rank" to "right after the Home". After the Home refuses, it cold-starts and executes locally, the local machine becomes hot,
later requests stay local, and once idle they return to the Home -- exactly the three points above. When the Home is in its avoidance period, the order naturally puts the local machine first, without touching the Home first.

**Forwarded ingress requests.** One ingress request has only one call tree and one root; forwarding does not change that:

| Item | Where it is produced | When forwarded |
| --- | --- | --- |
| Route (host / path -> Worker) | X | Not routed again; the workerId is carried |
| Version lock table (snapshot at the root) | X, when it receives the request | Carried as it is; the executing machine executes by the lock table, not by its own active pointers (section 5) |
| Call tree identity | X | Carried |
| Deadline | X, when it receives the request | Carried as the remaining duration (independent of clocks); the time spent on refused attempts counts |
| Root budget | X, allocated only once | Carried as a budget; carried as it is when rerouting, never allocating a second one (see "Conservation" above) |
| Call id, lease | The executing machine | -- |

- The executing machine records it as a root (depth 0, kind "ingress") and notes the ingress machine; it only executes or refuses, never forwards again;
- **the executing machine's admission of forwarded roots**: counted as tree roots (`rootLimit`), and subject to the watermark bands (red refuses; yellow refuses versions cold on this machine).
  Roots executed locally on X are only subject to `rootLimit`. Reason: a forwarded root is optional work for the executing machine -- the sender can always execute it itself -- so under overload it should be held off first;
- the request body starts flowing only after the other side accepts; on a refusal not a single byte has been sent, and the request can be handed to the next candidate as it is -- the same rerouting condition as for Binding calls;
- once the other side accepts, streaming requests and responses, client cancellation and failures midway all follow the semantics of the cross-machine call contract; the executing machine becoming unreachable after accepting -> the client gets "result uncertain", without rerouting;
- X only forwards bytes and does not take its own `rootLimit`; the number of "ingress requests being proxied" on X goes into the state endpoint.

**Static overrides and the switch.**

- Static overrides act only on Binding calls, with unchanged semantics; for Workers with static overrides, ingress requests still execute on the machine that received them.
  Static overrides are used only to pin locations, in operations and in tests; not worth another code path for their ingress path;
- a switch `ingressPlacement` (on by default). When off, ingress requests execute on the machine that received them; used for comparisons
  and by tests that presume "ingress executes on the receiving machine".

**The refusal rate and cold starts must be observable.**

- Refusal rate: each machine counts, per target and per refusal code, the attempts and refusals of ingress and Binding separately, as cumulative values and as values over the last 60-second window;
  refusals it gives itself are counted likewise by code and by source; the decision counts gain the kind "local execution after the Home refused";
- cold starts: **the first time a process loads a version** (the Worker Loader calling the code-fetching callback) counts as one cold start, counted per process and per version;
  each execution's record notes whether it was a cold start.

**Without eviction, what can be measured is the cause, not the result.** Cloudflare's gains (a tenfold drop in eviction, the cold start rate falling from 0.1% to 0.01%) come from eviction under memory pressure:
fewer replicas, fewer isolates each machine has to hold, fewer pushed out. Without eviction, once loaded, an isolate stays in the process,
so the measure is how many copies of each light-traffic Worker are loaded in the cluster (≈ the number of cold starts, the number of resident isolates) and the forwarding cost paid for it;
the improvement of the cold start rate itself depends on idle eviction.

**The cost of the default `p` = 0.05.** By the contraction mechanism (see "Contraction" above), a non-Home machine turns cold only after a whole idle period without local execution.
With `p = 0.05` and continuous traffic, that hardly ever happens -- replicas that spread out stay until the traffic on that machine stops. This is the cost "a small default" accepts on purpose
(gathering left to idle eviction); it is measured below.

### Ingress placement: results

**Each Worker is loaded only once in the cluster.** 60 light-traffic Workers, each receiving on average one request every 5 seconds, landing randomly on one of the three machines, for 2 minutes;
both groups used the same fixed sequence of requests, three runs each:

| | Copies loaded per Worker | Total cold starts | Ingress requests forwarded | Ingress requests p50 / p99 |
| --- | --- | --- | --- | --- |
| Ingress placement off (control) | 2.98 (59 with 3 copies, 1 with 2) | 179 | 0 | 5.6-6.1 / 16.3-19.9 ms |
| Ingress placement on | **1** | **60** | 66.7% | 6.7-6.8 / 15.3-17.1 ms |

All 1,469 × 6 requests succeeded, each executed exactly once; refused attempts: 0. With ingress placement on, p50 is about 0.8 ms higher (forwarded requests p50 about 7.1 ms, locally executed ones about 6.1 ms),
while p99 is actually lower -- cold starts dropped from 179 to 60, and cold-start requests have a p50 of about 13 ms. This is the "cause": the number of copies down to a third; the "result" (eviction and cold start rates) depends on idle eviction.

**The Home refuses or cannot be reached -> execute locally** (three situations: red band, concurrency saturation, the Home's server process stopped; ingress and Binding three times each): all executed exactly once on the caller's machine,
with zero executions on the Home; a local cold start takes 16-59 ms, hot executions after that 1.3-16 ms; after the local machine has been idle for more than 3 seconds, the next request goes back to the Home.

**Forwarded root calls**: an 8 MiB request body echoed back byte for byte; the first chunk of a chunked response arrives in 36-39 ms (the whole takes about 0.7 seconds, proving it is not buffered and sent at once); after the client disconnects midway,
both machines' in-flight and proxy counts return to zero within 73-76 ms; a failure midway shows as a failure; time spent on refused attempts counts against the deadline (the forwarding itself 0.23-0.37 ms);
budgets stay conserved under forwarding and rerouting; the tree identity is produced on the receiving machine; when the two machines' active pointers differ, the version locked by the receiving machine executes; when the definition cannot be fetched, it refuses explicitly and executes locally;
killing the executing machine after it accepts -> `RESULT_UNCERTAIN`, with no execution elsewhere. The executing machine admits forwarded roots as tree roots, subject to the watermark bands.

**Forwarding racing against a kill**: with the kill of the executing machine timed uniformly at random between 129 ms before the request is sent and 1 ms after, 11 of 30 were killed before the other side took the request and succeeded after rerouting,
and 19 got uncertain results; each executed exactly once.

**Spreading and contraction** (ingress-driven, a 3-second idle period, one request per second on each of the three machines, observed for 120 seconds):

| `p` | Result |
| --- | --- |
| 0 (control, three runs) | Never gathered: the non-Home machines executed every second (240 times each) |
| 0.5 (formal, three runs) | Gathered after 4.0, 4.0 and 11.0 seconds |
| 0.05 (the product default, one run) | **Did not gather**: 227 of 240 possible non-Home executions happened |
| 0.5, spreading caused by real concurrency saturation (one run) | Gathered after 8.0 seconds |

The result for `p = 0.05` is exactly the cost the design predicted and took on deliberately: as long as traffic does not stop, replicas that spread out stay, and gathering is left to idle eviction.

**Tree roots are still refused before nested calls with ingress placement on** (1,800 slow root requests per group per run, all three machines overloaded): the control group admitted 816 trees and completed only 67 (8%);
the formal group admitted 595-600 in three runs and **completed all of them**; each of the 1,200-1,205 tree roots held off was tried 3 times (the Home, the local machine, one other), with zero executions.

**etcd outage with ingress placement on, 30 times**: 7,071 requests with zero failures, 66.8% of the roots forwarded across machines -- exactly the path the too-strict `destinationEpoch` check had broken (see "Placement: results").

#### Problems found

1. **Local execution through the placement flow was about 2 ms slower than the direct path, mainly because of logging in the test environment.** With a fixture without waiting, ingress entering at the Home and executing locally through the placement flow had a p50 of 4.7-5.6 ms;
   with ingress placement off, taking the direct path, it was 3.0-3.1 ms, consistent over three runs; the placement decision plus the local attempt themselves take only 0.1-0.3 ms.
   The server and workerd flush every log line to disk immediately, and the test environment's log directory was mounted to the host through
   Docker Desktop's file sharing; local execution through the placement flow writes 5 more lines per request than the direct path, which under load also causes queueing on the event loop. With logs written to a container-local volume:

   | | Logs written to a host directory (p50) | Logs written to a container-local volume (p50) |
   | --- | --- | --- |
   | Ingress placement off (direct local execution) | 3.6-3.7 ms | **1.6-1.7 ms** |
   | Through the placement flow, executed locally | 5.1-5.2 ms (+1.49) | 1.7-1.9 ms (**+0.18**, variation between runs 0.17) |
   | Forwarded once | 6.6-6.7 ms | 2.4-2.5 ms (+0.62 over local execution) |
   | Binding: static override -> dynamic placement | +0.71 ms | **+0.34 ms** (variation between runs 0.31) |

   The remaining roughly 0.18 ms and 0.34 ms are spread over the necessary allocations, admission, identity checks and asynchronous waits. The dynamic placement overhead (see "Placement: results") likewise comes mainly from logging.
   **The latencies measured in the test environment with logs written to a host directory -- those in sections 4 to 7 unless stated otherwise -- are inflated by about 2 ms by logging** (relative comparisons within one measurement still hold).
   **A risk that must be resolved before production**: the server writes logs synchronously on its event loop, so as soon as the log's receiver slows down, all requests on that machine are held up.

## 8. Failure paths

Every component has to handle its failure paths, not only its success paths:

| Component | Failure paths |
| --- | --- |
| Membership | Crash (leaving when the TTL expires), graceful exit, name conflicts, etcd unreachable, only one machine losing etcd, the watch's history compacted |
| Deployment state | Call trees in flight during a deployment, atomicity of flipping several pointers, machines with inconsistent deployment views, a receiver not knowing the locked definition, invalid writes bypassing the tool, concurrent deployments, deployment behaviour when etcd is unreachable |
| Code bundles | Failed fetches, mismatched hashes, the source machine disappearing in the middle of a fetch |
| Placement | A stale membership view, the target refusing, all candidates unavailable |

## 9. Scale

- Membership: using etcd leases as node heartbeats is a mature practice (Kubernetes does it), no pressure up to thousands of machines;
- deployment state: the authoritative store holds only pointers, a manageable amount at a million Workers (version definitions are kept in etcd for now, to be moved out before the scale gets there, see sections 5 and 6); the burst of synchronization when **the whole cluster restarts at once** needs snapshots or peer synchronization later;
- code bundles and version definitions are distributed by hash, scaling horizontally by nature.

## 10. Per-Worker incremental deployment

### Why per Worker

Section 5's deployment state is "declarative, the whole state": the deployment tool reads in the complete desired state, and one transaction flips all active pointers, the whole route table and the generation.
Enough for a single deployer, unusable for a multi-tenant control plane:

1. **Deploying one Worker requires submitting all Workers**: Workers not listed in the manifest are deleted. To deploy one tenant's Worker, a control plane would first have to read the state of all tenants, merge, and write the whole back;
2. **the transaction's size grows with the total**: the flip transaction writes once per active pointer, and etcd allows at most 128 operations per transaction by default -- beyond a hundred or so Workers, deploying is impossible;
3. **globally serial**: all deployments use the generation as an optimistic lock, so deployments of different tenants conflict with each other, and a conflict is a failure;
4. **the route table is one key**: each deployment rewrites the whole table, whose size is bounded by etcd's limit on a single value;
5. **one invalid place freezes everything**: the server compiles the complete state once, and any illegal part makes it refuse the whole revision and keep the old snapshot. One tenant's wrong write makes every tenant's later deployments not take effect until someone fixes it.

### Goals

- Deploying, updating or removing one Worker (or a small group of Workers that must flip together) reads and writes only the keys related to them, with the amount written proportional to the change, not growing with the total;
- deployments of different Workers can run concurrently without conflicting; concurrent deployments of the same Worker have exactly one outcome take effect, never half and half;
- one Worker's invalid state affects only itself and the Workers bound to it;
- section 5's core properties are unchanged: one call tree sees only one deployment state (the closure locked at the root); a group of Workers flips together atomically; definitions are immutable and identified by hash; the immutable parts are published first, then the pointers flipped.

### The layout in etcd

```text
/coworkerd/deploy/definitions/<workerId>/<versionId>   version definitions (unchanged: created only, never overwritten)
/coworkerd/deploy/active/<workerId>                    {versionId, definitionHash} (unchanged)
/coworkerd/deploy/routes/<route key>                   {host, pathPrefix, workerId} (new: one key per route)
```

- The route key is derived deterministically from (host, pathPrefix): one (host, pathPrefix) has only one key, so "duplicate routes" are structurally impossible; the server checks that key and value agree;
- the whole-table `routes` key is removed; **there is no global serialization point any more**. The generation is informational only and the condition of no write.

### Operations

| Operation | Semantics |
| --- | --- |
| upsert (add or update) | Takes the definitions and routes of one or a few Workers (a partial manifest). These Workers' active pointers and routes flip together in one transaction; Workers outside the manifest are not affected |
| remove | Deletes the Worker's active pointer and all its routes; the definitions are kept (immutable history, reclamation later). Refused while valid Workers are still bound to it |
| apply (the whole state) | The declarative deployment of section 5, for development and tests: computes the difference with the current state and writes it the same way; Workers outside the manifest are deleted |

### Concurrency

- Each write is conditioned only on the keys this change touches: these Workers' active pointers and their route keys (new routes require the key not to exist), each equal to the revision read;
- two concurrent deployments of the same Worker: the later commit's condition fails, and it rereads, revalidates and retries a bounded number of times;
- route ownership: refused when the route key already belongs to another Worker; nobody can take away someone else's route in passing. To move a route from one Worker to another, put both in the same upsert;
- when removal runs concurrently with "adding a Worker bound to it", no dangling binding may be left behind.

### The server: deterministic, per-Worker validity

No longer "compile the whole, refuse the whole revision if any place is invalid". Each server computes, for the state under one revision:

- **valid Workers**: the active pointer points to an existing definition with a matching hash, the definition is legal, every Binding target is a valid Worker, and the closure does not exceed the bound;
- **effective routes**: routes pointing to valid Workers;
- invalid Workers are not served (certainly not executed), with their reasons listed one by one in the state endpoint; the other Workers are served and deployed as usual.

This result **depends only on the state under that revision**, regardless of when the server started or what it applied before: all servers see the same valid set under the same revision, and a newly joined machine cannot diverge from the old ones.

**The cost**: wrongly writing one Worker's pointer makes that Worker (and the Workers bound to it) unavailable, instead of, as in section 5, "nothing takes effect, keep serving the old snapshot".
A deliberate trade-off: the old approach let one tenant's wrong write freeze every tenant's deployments; isolated per Worker, only the Worker written wrongly is broken.
The normal flow never writes invalid state -- the deployment tool (and the control plane later) validates before writing; only wrong writes bypassing the tool get here.

**What stays the same**: the closure locked at the root, lock tables passed across machines, downstream executing by the lock table, definitions identified by hash, `VERSION_UNAVAILABLE` when the receiver does not know a definition, publish before flipping, code fetched on demand.

### Known limits

- To validate "is any other Worker still bound to me" and "will my change push an upstream Worker's closure over the bound", the deployment tool still reads all active state once. At scale, a control plane maintaining a reverse index will save that;
- definitions only grow; reclamation later.

### The write protocol for the control plane

The main points (keys, values, comparison conditions, order, retries, the per-transaction limit):

| Key | Value | Who writes it |
| --- | --- | --- |
| `definitions/<workerId>/<versionId>` | The canonical version definition, including `definitionHash` | Created only, never overwritten |
| `active/<workerId>` | `{versionId, definitionHash}` | upsert / remove / apply |
| `routes/<host><pathPrefix>` | `{host, pathPrefix, workerId}`; the key is derived uniquely from (host, pathPrefix), and the server requires key and value to agree | As above |
| `binders/<target>/<binder>` | The binder's current pointer; the tool's reverse Binding index, the guard against concurrent removal, not part of validity | Every write of the binder |
| `generation` | A decimal string, informational only, **the condition of no write** | Written along with each write |

- **upsert**: one consistent read (revision R) -> validate "the state at R + this write" with the same evaluation code as the server (the Workers of this batch are valid, other Workers valid before are still valid, route ownership)
  -> publish the immutable parts first (object storage, definition keys) -> one flip transaction: each Worker's pointer in this batch and each changed route key compared "revision equals the one read" (new keys compared "does not exist"),
  each Binding target outside the batch compared "creation revision unchanged" (the target was not deleted or recreated; its own version updates do not count as conflicts); writing the pointers, the route keys, the `binders/` index and `generation`.
  On a failed comparison, reread, revalidate and rewrite, at most 8 times, with a backoff of 5-160 ms and jitter;
- **remove**: compare the pointer, the route keys it owns, and a range comparison that `binders/X/` has had no writes since the read (no Worker started binding X after the read); delete the pointer, the routes and the index, keeping the definitions;
- **apply** (the whole manifest): compute the difference against R and write the same keys, with Workers outside the manifest written as removals; no retries by default. An initial deployment from an empty state that does not fit into one transaction is split into several steps in Binding dependency order;
- **the per-transaction limit**: the number of operations = max(comparisons, writes) <= etcd's `--max-txn-ops` (128 by default). With one new route and no Bindings, one upsert holds at most 63 Workers; the tool checks before writing and refuses;
- **no dangling**: upserting W compares the creation revision of target X and rewrites `binders/X/W`; removing X range-compares `binders/X/`. Whichever commits first, the later one's comparison is bound to fail, and after rereading it revalidates against the new state and is refused.

**Known limits**: two different Workers upserted concurrently may each pass validation while together pushing a common upstream closure over 64 -- the result is deterministic (all servers judge that upstream invalid and refuse explicitly; deploying again restores it);
a control plane that wants to rule this out deploys serially within one connected component of Bindings; when someone bypasses the tool to rewrite a published definition and also changes the pointer's hash to match, the server judges validity by the rewritten content (the result is determined only by the state, and trees in flight still execute by the old locked hash).

### Per-Worker incremental deployment: results

| Property | Result |
| --- | --- |
| Consistency within a tree (the mixed-tree measurement of section 5, a, b and c upserted together and flipped 20 times; single machine and cross-machine 3 runs each) | **36,571 trees, 0 mixed**; the control group resolving call by call had 2,721 mixed trees over 2 runs |
| Per-Worker operations | Upserting one of 5,000 Workers leaves the other keys' revisions unchanged; the transaction's operation count is the same for N = 10 / 100 / 1,000 / 5,000; after remove the definitions remain and trees in flight complete normally; the tool refuses 6 situations, each with its reason, etcd unchanged |
| Concurrency | 20 different Workers upserted at once all succeed with 0 retries; the same Worker raced 30 times, the final state exactly one side (14 / 16); removal raced against adding a binding 30 times (removal first 24, binding first 6), 0 dangling bindings |
| Isolation | Worker W written wrongly bypassing the tool: W and the Workers bound to it answer an explicit 503 `WORKER_INVALID` with zero executions, other Workers are served as usual, later deployments of unrelated Workers take effect as usual, and redeploying W restores it. **Control**: the same bytes written into a server with section 5's whole-state validity make later deployments of unrelated Workers not take effect (frozen as a whole). Another 7 kinds of writes bypassing the tool each affect only the Worker or route written wrongly |
| Servers agree | After 8 changes (including invalid writes and moving a route), a newly started machine: under the same revision, the digests of the valid set and the effective routes are the same as on the two long-running machines, and equal to the digest the tool evaluates independently from etcd |
| Scale (N = 5,000, all sharing one code bundle) | Changing a single Worker: a server applies it in **p99 0.134 ms**, from commit until all three have applied it p99 7 ms; the tool takes p50 160 ms (it still reads all state once to validate upstream closures and binders, growing with N, a known limit); 100 sampled real calls all succeed; one upsert holds at most 63 Workers, and the tool refuses explicitly beyond that |
| Incremental application equals full evaluation | 5,000 rounds of random batches: all outputs of incremental application byte-identical with full evaluation, and route selection the same as a brute-force longest-prefix reference |

**Implementation points**: `DeploymentState` keeps all keys under one revision, decodes only the changed keys, recomputes only the changed Workers and the upstream ones found along the reverse Binding edges, and maintains an order-independent set digest each for the valid set and the effective routes;
the watch now "delivers only the changed keys", and the full read applies from the empty state with the same code. When ingress hits an invalid route or an invalid Worker, it answers an explicit 503 (not executed), and no match answers 404; an invalid route never falls back to a shorter prefix,
so writing one route wrongly cannot make a request execute in another Worker.

**Startup and `apply`**:

1. Invalid Workers do not prevent the server from starting (under section 5's whole-state validity, it did not open the port): isolated per Worker, one bad pointer should not keep a whole machine from serving; it refuses to start only when the deployment prefix has no key at all;
2. the whole-state `apply` keeps the semantics "a conflict is a failure, no retry", for single-machine development and tests; the control plane uses upsert / remove.

## 11. Graceful exit and rolling upgrades of the server

**Why**: process rotation replaces workerd generation by generation (see [process isolation and resource governance](process-isolation.md)); the server itself (`coworkerd-server`) needs a way of its own to be replaced. Without a graceful exit, SIGTERM makes it exit at once: call trees in flight fail, and the other machines learn it is gone only when its lease expires (TTL 15 seconds).
Rolling upgrades of a cluster's machines must not lose requests.

**The approach**:

| Step | What |
| --- | --- |
| 1. Announce the exit | The member key's value gains the state `draining` (renewal continues as before). When the other members see it in their watch, placement no longer puts new call trees or child calls on it; its own ingress answers **new root calls** with an explicit 503 `SERVER_DRAINING` (not executed, reroutable), and `/__coworkerd/ready` turns not ready at the same time -- the front load balancer takes traffic away on it |
| 2. Drain | Wait until the call trees in flight on the machine reach zero (including background work of `waitUntil`); child calls forwarded from other machines that arrive after the announcement are likewise refused explicitly and rerouted by the other side (the explicit refusal of section 7). The deadline is 30 seconds by default; what remains at the deadline is handled like calls on a killed process (`RESULT_UNCERTAIN`, counted separately) |
| 3. Close the processes | Close each slot's workerd with process rotation's drain and close (starting no new generation) |
| 4. Leave | Revoke the lease (the member key disappears at once), exit code 0 |

A restart registers as before and gets a new epoch. **A rolling upgrade = for each machine in turn: announce the exit -> wait for it to exit -> replace the binary -> start -> wait until it has registered and is ready -> the next machine**. The deployment tool provides `drain --server URL [--wait]`.

**What has to hold**: not one request fails because of the upgrade during the whole process (except drain timeouts, counted separately); a call tree still sees only one deployment state (the lock table is unaffected by membership changes, section 5);
other machines meet a machine that is exiting only with "explicit refusal, then reroute", never with "result uncertain"; epochs are monotonic and call ids never repeat.

**Not covered**: upgrading etcd itself; the front load balancer (out of scope; only the readiness probe's semantics are provided); automatic rollback when an upgrade fails (swap the binary back by hand and roll again).

### Graceful exit and rolling upgrades: results

**As implemented**, the exit has five steps -- announce (the machine immediately refuses new root calls with 503 `SERVER_DRAINING`, the readiness probe turns 503, and the member value changes to `draining` asynchronously) -> drain (in-flight counts of three kinds, ingress, local execution and each generation's request contexts, reach zero; the deadline is 30 seconds by default) -> close the processes (as in process rotation, starting no new generation) -> revoke the lease -> close the ingress (first close the listener, keep answering for 100 ms, then close idle connections), exiting with 0. A second signal kills immediately, with exit code 2. Other Servers remove it from the placement candidates when they see `draining`; calls arriving within the window are caught by explicit refusals and rerouted by the other side.

| Situation | Observation (three runs each) |
| --- | --- |
| One machine exits (60 seconds of load, SIGTERM to s2 at second 20) | About 24,000 requests with 0 failures and 0 `RESULT_UNCERTAIN`; the other two see `draining` within 3-5 ms, draining 0-17 ms, revoking 13-38 ms, the process exiting at 118-143 ms; s2 disappears from the membership table immediately (without waiting for the TTL) |
| Trees across machines | After the announcement, the first child call is explicitly refused by s2 (95-101 ms) and rerouted to execute on s1; the whole tree succeeds and still sees only one deployment state (`v2`, deployed while the tree was running, appears only in new trees) |
| Rolling upgrade of three machines | Rolling back and forth between two server builds that can both drain: 6 passes, all with zero failures; each machine 33-116 ms from drain to no longer accepting connections and 473-750 ms until the new process is ready, with at least 2 machines in the membership table and deployments in between taking effect as usual |
| Draining and the deadline | A stream completes in the old process (3.7 seconds); a stuck one is killed at the deadline (5 seconds), and only it gets `RESULT_UNCERTAIN`; `waitUntil` finishes before closing; two SIGTERMs exit with 2 within 8-9 ms |
| Abnormal paths | A deployment during draining takes effect only on the other two and is caught up after the restart; SIGINT likewise; with etcd unreachable it drains and exits as usual, the other machines remove it when its TTL expires, and the restarted process waits for its "previous life" to expire before getting a new epoch; call ids never repeat |

A hang was recorded once during development (`PeerForwarder`'s membership table overwritten through a dangling pointer); it has not recurred, and is to be rechecked in an environment with ASAN.

**Configuration and notes**:

1. `drainTimeoutMs` is in the configuration file; `drainTimeoutMs=0` turns draining off (the server exits at once);
2. the drain criterion depends on the `completed` lines the limit patch writes, so a context dispatched but never executing JS makes the drain wait for the full deadline (it only happens when the caller vanishes suddenly, without affecting correctness) -- the remedy is for workerd to report the end of requests whether or not CPU was used;
3. deployment notes: `drainTimeoutMs` has to be larger than the longest request and `waitUntil`; the orchestrator's kill grace period has to be larger than the deadline plus the grace period for closing processes (5 seconds by default).

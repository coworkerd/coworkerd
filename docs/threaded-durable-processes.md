# Threaded Durable Object processes

Design for [issue #16](https://github.com/coworkerd/coworkerd/issues/16).
The source and baseline use main at `dfc8803731a33feaaf8796313a0053d403f5ba8a`.
The node continues to own placement and durable history. The threaded child hosts object events and native database
operations. Billing and separation by trust level are outside this design.

## Current execution and the proposed boundary

`ProcessPool` in [node/process-pool.c++](../node/process-pool.c++) maps an object key to a durable slot, explicitly
selects threaded execution only for non-durable slots, and gives the old durable child a generation-scoped log FD
and a writable object-data directory. The node routes a DO binding, SQL request or scheduled alarm through
`DurableShards` and `DurableLog::openObject`. The durable dispatch in [node/main.c++](../node/main.c++) checks
ownership, the object image and the selected generation's log channel again immediately before dispatch.
[node/root-objects.c++](../node/root-objects.c++) prepares the root definition. The trusted
[runtime](../runtime/runtime.js) loads it through `ROOTS`, builds the class wrapper and uses the root actor
registry for the actual call. A tenant Worker version shares an isolate across its objects.
`SqlDatabase` in that same runtime implements the SQL side of the D1 binding and the HTTP API using one shared
runtime isolate. Durable processes share the upstream event loop even across different isolates.

The current kernel already has an idle path below the 4,096 limit. Upstream `ActorContainer::inactive`
arms a 10 s shutdown for an evictable namespace. Patch 0020 registers shipped root namespaces with
`evictable: true` and adapts their retirement. Separately, its `ResidencyCoordinator` runs the process-wide
LRU pass introduced by patch 0014. The proposal replaces the old idle path with explicit blocker deadlines,
home-thread release and node-owned sockets. [Pinned upstream actor implementation](https://github.com/cloudflare/workerd/blob/3d3093c8f01b9b53f4d36d20a8e4ea0a39787992/src/workerd/server/server/actor.rs#L1480-L1513) ·
[Shipped root loader](../patches/workerd/0020-root-loader.patch).

The new child reuses threaded process supervision, private ingress and node binding channels from
[patch 0042](../patches/workerd/0042-threaded-server-s1a.patch), the per-isolate V8 groups from
[patch 0043](../patches/workerd/0043-v8-sandbox-per-isolate.patch) and the interrupt and reservation work in
[patch 0044](../patches/workerd/0044-s1e-node-cost-and-long-operations.patch).
The ordinary rule of one program's isolate per process and the selection of a call thread for each invocation
do not give a resident actor a stable home. Introduce a durable role and a separate dispatcher inside the
threaded child. The node chooses the same slot and generation as today. The child chooses a home thread and
isolate for an object when the object first becomes resident. It chooses a database thread when a database
first opens. Events and completion messages go to the selected home. Use namespace and object IDs, data and shard
epochs, and the admitted open identity to reject stale home references. Actor, isolate-bound values, SQLite
connection, cursors and callbacks never travel across threads.

The main thread of the child accepts and demultiplexes work. Home threads run their own event loops. An object home
holds exactly one isolate copy and the object trees placed in it. Copies of different Worker definitions never
share a home, so a synchronous event can delay only objects of its own Worker that live in the same copy.
A home does no blocking disk work that is not part of the event it is running: checkpoints and the file reads
of a cold open or a rebuild run on disk workers, or, where one has to stay on the home, the step that puts it
there measures how long it stalls the other objects of the copy and states the bound.
Global thread, memory and descriptor reservations bound the pool. Budget the database, WAL and
shared-memory descriptors per open and leave headroom for checkpoint and recovery work, the log channel
and control traffic. Refuse new work before it consumes that headroom. A native
database leases its own thread as long as its connection is resident. Other databases and actors use
different threads. Close an idle connection before returning its thread to the pool. Object workers sharing a home
have fair mailboxes and bounded synchronous execution. Each Worker definition may have multiple independent
isolate copies in the child. Each copy has a home thread of its own, and a woken tree is placed with a copy
that has safe capacity, or with a bounded new copy on a new home. A copy has safe capacity while it is within
its memory budget and its home is not saturated: when the home has been busy above a threshold over the last
seconds, the next woken tree of that Worker goes to another copy. A tree and its facets stay together.
Choose placement before constructing the actor, and reserve thread, address space, heap and native charges
before publishing it. Count initializing and closing trees against the backstop until their reservations
are released. Include native databases in the process backstop, as database objects count today. Preserve the
version pin and actor input/output gates, including supported
interleaving across awaits. The mailbox bounds waiting events, rather than turning every event into a
run-to-completion lock.

An isolate-wide V8 interrupt needs a stricter contract for object-local CPU reset than the ordinary path
does. Consume CPU only while that event executes (including its synchronous SQLite work). Once an interrupt
identifies the offending event, unwind its JavaScript and SQLite work, cancel its actor IO, then reset the
actor on its home and clear the termination state before re-entering the isolate. Roll back uncommitted
transactions and leave confirmed history intact. Serialize that sequence
with all isolate entry. Preserve unrelated objects' in-memory state. Use the existing storage gate for
committed writes and preserve their history. Prove reset isolation with two live objects in one isolate,
including a sibling with a suspended event. Report `CPU_LIMIT_EXCEEDED` for the event that triggered the reset. Treat recoverable resource exhaustion as a bounded refusal before allocation. An unavoidable isolate-fatal error is a separate process containment path, not the intended
outcome of tenant CPU or memory pressure.

Choose the 30 s default and resolve the existing Worker `limits.cpuMs` and tenant policy before
admitting the event. The threaded durable ceiling is 300,000 ms. A higher per-Worker setting
still needs room under the tenant's `maxWorkerCpuMs`, which defaults to 30,000 ms.
[Worker limits](control-plane-api.md) · [Tenant defaults](../common/tenancy.h).

## Node and child protocol

Each durable slot declares its backend and version during private readiness. The node switches only new
generations, with the old leader fully drained before it authorizes a new child to open the same image.
A generation becomes ready only after successful negotiation. Keep the launcher, generation identity, lifecycle
and parent-death channels, authenticated peer protocol, and node-owned desired state. Extend the private
protocol by role and feature negotiation within a node-child pair. The old child uses one persistent
Cap'n Proto bootstrap connection per generation. Ordinary threaded execution uses a per-call Unix connection, a raw admission acknowledgement, then Cap'n Proto
`EventDispatcher`. Use that transport shape for durable calls with the object-specific admission described
below. Node-to-node and persisted encodings stay at their current versions.
[Current connection paths](../node/process-pool.c++).

- **Open and invoke.** The node chooses ownership and verifies the image with the existing checks. It sends
  authenticated private metadata identifying node/slot generation, namespace, object and class, data and shard
  epochs, pinned version and bundle identity, invocation identity and lease, and event kind. Send the routing
  identity in a bounded private preface before bootstrapping the call on its home loop, and check it against
  the full descriptor before entry. Fetch uses HTTP over the per-call dispatcher. RPC and WebSocket upgrades
  retain their capabilities and streams beyond dispatch acknowledgement, with a membrane owning the connection
  for as long as they are live. A routing receipt acknowledges transport only. Send explicit application
  refusal when the generation, target or mailbox cannot be admitted, and signal acceptance only once the
  home has reserved the event. Treat an ambiguous disconnect conservatively as today. The node retries only
  explicit pre-execution refusals allowed by the existing peer contract. Preserve streaming, flow control,
  caller abort, error/status mapping, versioned binding calls and lease expiry. Acquire object queue credit
  before accepting unbounded input or waiting for image reconstruction. A credit covers the pending open
  and the eventual mailbox entry, expires with the call, and transfers once to the admitted event. The
  existing dispatch recheck governs exposing files after a queued open. Reuse the node's
  `prepareRootDispatch` plan and selection and the `RootStorageRef` naming rules. The new dispatcher
  constructs the class and its binding wrappers on the home event loop. Cancellation releases the credit
  even if the shared rebuild continues. Socket wakes and alarms use the same admission boundary.
- **Storage.** Keep the generation-scoped FD, WAL framing and confirmation boundary. A home thread publishes
  immutable frames via the child's log-channel owner, which serializes wire writes and routes correlated
  replies to the correct home. Drop locks that guard another home's state before waiting. Keep cancellable
  byte-bounded queues, per-database commit ordering, SQLite checkpoint safety and node-side disk sequencing.
  Correlate existing `HELLO`, frames, opens, closes, checkpoint reports, ACK, seal and break messages
  with generation and object-open identity. Use the negotiated node-child revision for any extra private
  correlation fields. Size the byte reservation to admit any single record within the node's existing
  record limit. A lost acknowledgement, generation change or fenced shard invalidates the appropriate
  open. Hold the write's reply or RPC result behind the existing confirmation gate.
- **Close and drain.** First stop admission for the affected open or generation. Flush committed work through
  the existing gate, close facets before their root, then report the close and release the home reservations.
  A generation may be drained or killed on a real fault. EOF invalidates live opens and lets the node run
  existing recovery. For a targeted reconstruction, wait for positively confirmed close or process death
  before swapping files. Stale completions cannot clear a newer registration.
- **Wake.** A user event, alarm or nonautomatic WebSocket event reaches the owner through the same open and
  epoch checks on a cold image as a fresh dispatch. If an object is already resident, post to its home.
  Serialize concurrent open, wake, close and move for one object. Treat an accepted call whose result was
  lost as uncertain after transport or ownership failure.

Expose the resident root count, database homes, worker threads, idle retirements by reason, queue
saturation, actor resets, isolate resets and socket wake counts per live generation in the node state.
The node can report a stale generation for fault diagnosis. Use the current PID and generation in
acceptance measurements. Record SQL execution and confirmation timestamps separately so the isolation
test can prove it sampled during the statement.

## Durability and ownership contract

[The durable log protocol](cluster-state.md#12-durable-object-durability) remains authoritative. The child writes
SQLite through the existing WAL and output-gate discipline, while the node controls the raw log, peer
replication, fencing, immutable segments and manifests, snapshots, recovery and disk eviction.
Reuse `DurableLogShipper`'s frame extraction, acknowledgement and checkpoint ordering from
[patch 0013](../patches/workerd/0013-durable-object-log.patch) without using its actor-loop singleton from
multiple threads. Keep precisely which peers were selected, their positive record acknowledgements, the local
flush and empty-selected-peer fallback, and the current confirmation deadline. With selected peers, the
same positive acknowledgements of the individual record are required, including existing reachability
rules. An empty selection requires a successful local flush. An unreachable nonempty selection cannot
use that fallback. Simultaneous loss of the owner and the acknowledging peers can still lose an
unpublished tail.
A failed response can have an uncertain outcome. Confirm, trim, drop, replay, repair and exclusion still
operate on the same records. Keep sequence and epoch checks at the node and preserve the peer fence floor,
recoverability of old manifests and tails, and current quarantine of failed or uncertain writes.

## Decisions against the current source

Cloudflare references were checked on 2026-11-22. Cloudflare describes hibernation after about 10 seconds,
non-hibernating eviction after 70–140 seconds of inactivity, and retention for up to 15 minutes **per pending
operation**, from when it began. It documents protocol ping/pong and matching application auto-responses
without a wake. Its SQLite-backed DO CPU default is 30 seconds, configurable to five minutes, with an
incoming HTTP request or WebSocket message refilling the available CPU. The D1 limit is 30 seconds for
an individual query and for the entire batch. The alarm docs specify one handler per object at a time,
at-least-once delivery and up to six retries starting at two seconds. Its architecture guidance says that
objects of one class can share an isolate, in which one thread executes JavaScript at a time, so that
synchronous work in one object can delay the others whatever the CPU limit.
[Lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/) ·
[WebSockets](https://developers.cloudflare.com/durable-objects/best-practices/websockets/#automatic-pingpong-handling) ·
[Auto-response](https://developers.cloudflare.com/durable-objects/api/state/#setwebsocketautoresponse) ·
[DO limits](https://developers.cloudflare.com/durable-objects/platform/limits/) ·
[D1 limits](https://developers.cloudflare.com/d1/platform/limits/) ·
[Alarms](https://developers.cloudflare.com/durable-objects/api/alarms/#alarm) ·
[Shared isolates](https://developers.cloudflare.com/durable-objects/best-practices/architecture-guidance/#shared-isolates).

| #16 decision | Where it lands and what it replaces |
| --- | --- |
| 1. Home threads | The durable role in the threaded server replaces the upstream actor-loop execution of the definitions in `runtime.js` and the old single-loop use of its SQLite shipper. Actor creation, use and disposal stay on one home. Node WAL append, peer coordination and history stay node-owned. Blocking file and SQLite work runs on the relevant home or existing disk workers, and control coordination multiplexes completions. |
| 2. Native databases | The durable child routes the existing database identity to a native C++ SQLite handler on a database home. It replaces `SqlDatabase` and its runtime root, using workerd's `util/sqlite` and shipped storage hooks. Keep node/API forwarding and the D1 binding's API and envelope. |
| 3. Idle lifecycle | Replace the shipped actor residency path in [patch 0014](../patches/workerd/0014-durable-object-hibernation-alarms.patch) as amended by [patch 0020](../patches/workerd/0020-root-loader.patch). The latter coordinates residency across namespaces, orders idle trees by last access and can exceed the configured target while active trees are protected. The child enforces timed retirement and reserves root slots against the process-wide 4,096 backstop before allocation. The node owns parked hibernatable sockets. |
| 4. Multiple isolates per Worker | Replace reuse of one isolate for the installed Worker definition with an affinity-aware set of independent copies. Key by the full pinned definition, binding and limit identity. Give each copy a V8 group, a home and a bounded placement budget. A cold placement never enters an isolate on the wrong thread. |
| 5. Event and SQL limits | Replace the durable 200 ms default from [configuration](../common/config.h) and the SQL-specific accounting and interrupts of [patch 0021](../patches/workerd/0021-sql-execution-limit.patch). Preserve the event boundaries from [patch 0012](../patches/workerd/0012-event-cpu-top-up.patch), including delivered messages, RPC entry and alarms. Use an event-scoped interrupt and reset in place of the old [CPU watchdog](../patches/workerd/0002-cpu-limit.patch). Charge actor SQL to its event's CPU, including compilation and row steps. Give native D1 a 30 s wall-time deadline shared by a whole batch. Account for SQL and result encoding under that deadline, with queueing and replication tracked separately. |
| 6. Isolate memory | Replace the version-wide reset under memory pressure in [patch 0005](../patches/workerd/0005-memory-limit.patch). Evict idle trees from the pressured isolate first and sample accounting again. If still over the placement threshold, mark that isolate closed to new trees. Reset only on continuing pressure at the hard threshold and after stopping admission. Wait for safe close where possible, keeping other isolates live. |
| 7. Queue per object | `ServerCore::admission` currently checks tenant ingress/nested quotas for durable dispatch. The separate process-local tenant-execution quota already applies only to non-durable selection. Replace the durable path's tenant concurrency admission with a bounded mailbox per object, and count limits in events and queued bytes before reading unbounded bodies. An overflow receives a pre-execution `503 OVERLOADED`. Expiring or canceled entries release their reservations. Node, process, disk, storage quota and ordinary ingress bounds still protect shared resources. |
| 8. Alarms | Keep `DurableLog`'s node scheduler, durable metadata and hint discovery. Move the handler to its object's mailbox and use a per-object firing token across awaits so a reschedule cannot start a second handler alongside the first. Remove `alarmFireConcurrency` as a dispatch cap. Keep fair bounded pumping and rebuild reservations. Treat object mailbox saturation as deferral, without closing the whole destination's gate or consuming a handler retry. |
| 9. Shared-loop controls | Remove the legacy durable 200 ms budget, `sqlCpuLimitMs`, `sqlWallLimitMs`, shared-loop `stuckTimeoutMs` kill, `pausedAdmissionWaitMs`, and process-wide GC credit and clock for the migrated path. `ProcessPool::refresh` already skips stalled-probe kill for threaded children. Audit the other startup/request deadline and restart-backoff uses of that setting before deleting it. Retain real exit, containment and resource accounting. The GC credit and clock are in [patch 0011](../patches/workerd/0011-gc-pauses.patch) and the old idle collector is in [patch 0018](../patches/workerd/0018-idle-collector-lifecycle-gc.patch). |
| 10. Storage and ownership | `DurableShards`, `DurableLog`, log/segment/manifest encodings and the confirmed-write contract above stay in force. Track the child backend independently of node and shard epochs. |

### Homes: one per isolate copy, a change to decision 1

#16 started from a home thread for every resident object, with the isolate belonging to no thread and entered
through its lock as on the ordinary path. This design gives a home to each isolate copy instead, and the
objects placed in the copy share it. The reasons:

- V8 runs one thread in an isolate at a time, so objects in one copy take turns in JavaScript under either
  arrangement, and their synchronous storage calls with it. In both, the copy is the unit inside which one
  object's work can hold up another's. Cloudflare documents the same for its platform.
- A thread per object makes the thread count follow the count of resident objects. A parked thread of the
  ordinary path measured about 97.5 KiB of resident memory, which is roughly 390 MiB for 4,096 resident
  objects, besides the descriptors of as many event loops. A thread per copy does not grow with the objects.
- With one thread per copy no lock moves between threads at every event, a wake needs no thread, and the
  reset of one object is serialized with every other entry to its isolate by construction.
- What a thread per object would add is that an object's work outside JavaScript does not hold up its
  neighbours in the copy. The rule on blocking disk work in "Current execution and the proposed boundary"
  covers that.

A deployment with few nodes places many objects of one Worker in the same copy, more than is usual on
Cloudflare. Two things therefore go with this choice: another copy is used when a copy's home is busy, not
only when the copy is full, and the guide tells authors that objects of one class may share an isolate and
that synchronous work should be short.

### Platform Durable Object definitions (proposal for #82)

The threaded durable host will load the product's own Durable Object classes from the
`coworkerd:platform` definition. Its entry module exports the classes. The node daemon will
carry an immutable ES module package built with the product. The private source service gives
the durable host the package for the admitted invocation, using the existing module manifest
and hash checks. The host will compile the package on a durable home with the same compiler
and exported-class checks as a Worker's package. `runtime.js` continues to provide the process
entrypoint and its runtime services. Adding a class consists of exporting it from the platform
entry module, with its module included in the package when it lives in a separate file.

The definition pin is the product release version and a SHA-256 of the canonical definition.
The digest covers the definition name, entry module, ordered module manifest and content
hashes, and the compatibility date and flags. The digest distinguishes development builds
with the same version label. The source service matches the complete pin and process generation
to the admitted invocation before returning the immutable package. A platform package comes
from the daemon that admitted it.

A platform namespace records its tenant and a class reference consisting of the definition name
and case-sensitive Durable Object export name. The namespace still has its own stable ID and
shard count. The node resolves the reference through trusted namespace ownership. A new pin
leaves the namespace, object IDs and storage identities in place. Only the platform can assign
a platform class reference. Tenant deployment data can declare classes of its own Worker. A
call needs authority for the exact namespace it targets, including a match with the namespace
owner's tenant. A test-only node executable accepts fixture definitions and namespace mappings
from its startup fixture. The released executable has no fixture injection path. Test Workers
call the installed objects through existing Durable Object bindings.

Placement and loaded-copy reuse use a definition key with separate fields for definition kind,
tenant ID, definition name and pin, and the effective bindings and limits of the copy. The node
gets the tenant from the validated namespace owner. The selected namespace and export identify
an object and its class within the copy. This permits compatible objects of one tenant to share
a copy. The loader verifies the namespace selection, the actor export and the copy identity
before admitting the event to JavaScript. Platform objects have an empty `env` and run under
the usual tenant Durable Object event limits, using the node's defaults where the tenant has
no override.

The forwarding node sends the class reference. The shard owner selects its own build's pin
when it admits the event, so either direction of forwarding works in a rolling upgrade. A queued
or running event and its retained work use the pin and copy they acquired. The package stays fixed
for the lifetime of the node daemon. The daemon alone holds each child's parent-liveness write
end, and the threaded child exits on EOF. The sandbox launcher also sets a parent-death signal.
Replacing the daemon therefore uses ordinary process shutdown and shard fencing. The next owner
can reopen the same storage and fire later alarms under its own definition. The node's durable
log, confirmation, takeover, alarm and idle-release paths remain the common path for the object.

### Databases: original native execution proposal

The comparison in [#79](https://github.com/coworkerd/coworkerd/issues/79#issuecomment-7155649275)
selected the Durable Object path for databases. The original proposal below records the reasons
for evaluating native execution.

A database could instead stay a Durable Object of the JavaScript `SqlDatabase` class with an isolate copy, and
so a home, of its own. That would reuse steps 1 to 3, and Cloudflare describes D1 as built on SQLite in Durable
Objects. Native execution is kept. The reasons first given, the resident memory of an isolate and a smaller
shared lock, are weak: a minimal isolate measured about 2 MiB (about 2.8 MiB with the V8 sandbox), small next to
the 64 MiB of SQLite memory a database home reserves, and a database runs one statement at a time either way.
The reasons that decide it:

- **Isolates are what a process runs short of.** In the default build all isolates of a process share one
  4 GiB pointer cage; with the V8 sandbox each reserves about 136 GiB of address space ([CHANGELOG](../CHANGELOG.md)).
  Measured on x86_64 Linux, a process held about 1,400 minimal isolates in the default build and about 650
  with the sandbox. A database with an isolate of its own takes one of them from the objects of its process;
  a native database takes none. Threads are not the limit: about 97.5 KiB each, one per working database in
  both arrangements.
- **No V8 on the data path.** Stopping a statement works the same way in both, through SQLite's progress
  callback and the execution budget in `util/sqlite` ([patch 0021](../patches/workerd/0021-sql-execution-limit.patch)),
  because `ctx.storage.sql` runs SQLite synchronously on the isolate's thread. An isolate per database adds
  V8 work that native execution does not have: `JSON.parse` of the request, rows held as JavaScript values and
  `JSON.stringify` of the answer, on the isolate's heap and under V8's interrupts and heap limit. Native
  execution meters a database's memory in one place, SQLite's metered memory, and fails with SQLite's codes.
- **A deadline of its own.** A D1 query and a batch have a 30 s wall deadline; a Durable Object event has a
  CPU budget that each request refills. An isolate per database would need a wall deadline the object path
  does not have.
- **The native code is mostly our own.** Step 4's patches, 0110 and following, put nearly all of their code
  in files of their own (`coworkerd-native-sql.*` and the threaded server); a few dozen changed lines are in
  upstream's `util/sqlite`. Upstream changed `util/sqlite` about four times a month in the half year before this review,
  and those changes reach the JavaScript class too, through `ctx.storage.sql`. The lasting cost is a second
  implementation of the D1 contract while the JavaScript class exists; step 4's comparison tests cover it.

Not measured yet: how long a native database takes to open, against a new isolate. The baseline's cold reopen
(about 70 ms p50 against 15 ms with the isolate warm) restarted the whole durable process, so it is not the
cost of creating one isolate.

**Fallback.** Databases use `SqlDatabase` in the runtime isolate on the upstream backend today.
[#82](https://github.com/coworkerd/coworkerd/issues/82) provides the shared platform definition and
loader for threaded homes. [#81](https://github.com/coworkerd/coworkerd/issues/81) will move
`SqlDatabase` to that definition, give each open database its own copy, and enforce the 30 s
wall deadline on the object path.

### Lifecycle and alarms

Use idle time since the last delivered event finished, or since its last hibernation blocker cleared if later.
An active event, response/RPC stream, timer, outbound connection, pending IO or `waitUntil` work prevents
hibernation, as does a standard `ws.accept()` socket. Re-evaluate blockers at the home before retirement, with
a monotonic clock. A hibernatable tree with no pending work closes approximately 10 s after becoming eligible.
Each retirement releases the object's home reservation. A home whose copy has no resident tree left keeps
the copy warm for the idle time of ordinary Workers (`idleEvictMs`), so that a wake soon after a release does
not pay for a cold isolate. After that time, or earlier under memory pressure, retire the copy and return the
thread to the pool. If a tree has hibernatable sockets, atomically
transfer its transport state before closing the actor. An otherwise idle ineligible tree gets a jittered deadline within 70–140 s. A pending operation can protect it for at most
15 min from its start. Cancel expired work before final retirement and close non-hibernatable sockets when
evicting. Active request wall limits and their invocation leases are separate. An event racing the retirement
either saves the old actor before close begins or joins the next open, once.

Use the node's WebSocket registry and transport loop for parked connections. The handoff carries the
owner/shard epoch, object and attachment identity, ordered frame position, tags, serialized attachments and
the auto-response pair/timestamp. Node-side ping and exact-match auto-response update transport state without
scheduling or refreshing an actor. For an application frame or a close/error that requires a handler, create
a bounded wake entry and pass the transport state to the new actor through the node's normal recovery gate.
Keep frame fragmentation, close handshakes, backpressure, cancellation and attachment encoding consistent
with the current surface. An object lacking enough capacity to wake cannot leave an unbounded buffered socket.
The existing hibernation manager can re-enter the old actor from inside workerd without a fresh node
`openObject` check. Remove that shortcut, establish a live node-transport reference independent of the child's
log FD, and make its close/fence path explicit. A connection is scoped to its living node and owner epoch.
After node death or ownership loss the client reconnects and storage recovers through the usual path.
[Current open and connection accounting](cluster-state.md#12-durable-object-durability).

Preserve one scheduled alarm stored per object as its source of truth. Compare the scheduled timestamp and
firing token before entry and before clearing, and commit the clear through the same storage gate when the
schedule was neither changed nor deleted by the handler. Stale hints and platform/rebuild refusals never
authorize handler execution. Target six retries after an initial failed handler with nominal delays 2, 4, 8,
16, 32 and 64 s. Preserve jitter around those delays. Key the retry
series to the schedule. Stop retrying that schedule after the sixth failed retry and conditionally clear it
through confirmed storage, keeping an alarm set by newer work. Treat loss of reply as uncertain and retain
at-least-once behavior on owner change.

### Database compatibility and deadlines

Keep the D1 binding adapter in `runtime.js` and the public HTTP SQL route in
[apiserver/main.c++](../apiserver/main.c++). Only the privileged owner prepares a native DB open, resolving its
existing namespace, object ID, data epoch, facet and seed/restore data. Use the same SQLite settings and
authorizer policy as the existing storage API. Keep result shape and conversions (null, integer/number,
text and blobs), columns/rows modes, meta counters and JSON escaping. Keep the current 7,340,032-byte result
budget, including encoding and metadata, and its error. Keep `/query` single/batch and ingest transaction
boundaries, including rollback when encoding or result size fails. Preserve the successful prefix of
`/execute`/D1 `exec()` if a later statement fails, and the response's existing failure envelope. Keep
SQL error codes, import, export and seed/restore paths on the same identity and history. The deadline
covers prepare, step, encoding and transactional work and cancels via SQLite progress/interrupt. Finalize
the statement and roll back a transactional batch before reusing the connection. Confirm completed writes
under the unchanged replication deadline before responding. Preserve the HTTP `503 SQL_LIMIT_EXCEEDED`
and binding `D1_ERROR` mapping for a timed-out query, including existing precedence for
`STORAGE_UNAVAILABLE` and `RESULT_UNCERTAIN` when completion is uncertain.
Preserve local filesystem bounds and declare
any native operation without a proven interruption path a release blocker for that execution path.
[Existing database API and error contract](guide/databases.md).

### Decisions taken in review

The four points below were open when the design was written. Each ends with what the review of the design
decided.

- **Retry exhaustion.** The issue says six retries are already enforced. In `DurableLog::fireAlarm`'s
  `FAILED` branch, `countedRetries` saturates at six and `backoff` continues to grow. The JS wrapper's
  exception branch reports `failed` and leaves the alarm set. The node passes `retries`, which continues
  incrementing, as the handler's `retryCount`. A permanently throwing shipped alarm may
  therefore run again after the sixth retry on one healthy owner. Settle whether the new cap applies per
  stable owner or must survive takeover. An in-memory firing token or retry counter cannot enforce a
  cluster-wide cap after a crash. A durable count, if required, needs an explicitly reviewed SQLite metadata
  representation, schedule identity, rollback compatibility and a definition for ambiguous in-flight failures.
  [Node scheduler](../node/durable-log.c++) · [Alarm wrapper](../runtime/runtime.js).

  **Decided.** The cap counts on the owner that fires the alarm, in memory. After the sixth failed retry of a
  schedule that owner clears it through confirmed storage, as "Lifecycle and alarms" describes. A takeover
  starts the count again, and alarms stay at-least-once. There is no durable counter and no new metadata.
  Deferrals and refusals by the platform do not count as retries.
- **CPU time and leases.** The node defaults its invocation-tree lease to 30 seconds. The ordinary threaded
  path already clamps its descriptor CPU to five minutes, but inheriting a 30 s call-tree lease can prevent
  the caller from observing even a 30 s D1 timeout or using a 5 min DO budget. The control plane SQL
  forwarder already times out after 15 seconds and returns `RESULT_UNCERTAIN` after a connected request
  loses its reply. Carry the new deadline through that path and the peer data plane. Confirm whether
  the product needs a 30 s public response ceiling in addition to the SQL execution ceiling. Agree on the
  durable-event admission and transport deadline class, including nested calls and alarms, before enabling
  higher configurable CPU. Preserve authenticated tree grants, monotonic deadline enforcement and the shorter bound on unanswered peer-open
  attempts. CPU refills apply to delivered events and do not renew an expired caller's authority.
  [Lease and dispatch](../node/main.c++) · [Control plane](../apiserver/main.c++).

  **Decided.** This design sets no lease rules of its own: how long a call tree's lease lasts is settled in
  [#18](https://github.com/coworkerd/coworkerd/issues/18). Step 4 raises the control plane's SQL forwarding
  timeout to the query deadline plus the confirmation deadline, and there is no separate ceiling on the
  public response. A per-Worker CPU limit above 30 s for object events is accepted once #18 is in.
- **Pressure and host sizing.** Set pool size, per-object queue depth and bytes, placement thresholds,
  reservation headroom and the compatible treatment of `residentObjects: 0`. Current zero disables the
  residency pass. The new process needs a real backstop even in that case. Review the extent of isolation
  between objects sharing a home. A synchronous object event can occupy that home for up to its budget.
  Separate database execution homes provide the required cross-database behavior. A guarantee for every
  pair of actor events would need an additional scheduling or placement rule.
  An isolate reset under hard memory pressure can reset the remaining objects of that isolate. Prove
  ordinary tenant allocation reaches the pressure policy before V8 fatal OOM, and settle which fatal
  runtime failures must still trigger the generation containment path.
  [Residency coordinator](../patches/workerd/0020-root-loader.patch) ·
  [Threaded allocations](../patches/workerd/0044-s1e-node-cost-and-long-operations.patch).

  **Decided.** An object home holds one isolate copy, as "Current execution and the proposed boundary" says,
  so an event can delay only objects of its own Worker in the same copy. The final gates check this across
  Workers. "Homes: one per isolate copy" gives the reasons and what goes with the choice. Pool size, queue
  depth and bytes per object, placement thresholds, the busy threshold and headroom are configuration,
  with defaults that step 1's pull request proposes from measurement. Nothing is limited by a count per
  tenant. `residentObjects: 0` means no count target; the reservations still refuse before threads,
  descriptors or memory run out. Hard memory pressure may reset an isolate copy together with the objects
  left in it, after the two earlier stages. A V8 fatal error stays a containment event for the process, and
  the durable role writes the culprit record of [#17](https://github.com/coworkerd/coworkerd/issues/17).
- **Recovery close and socket scope.** Today the node can reset durable processes to recover one object
  with a live open, in-flight call or socket. The new targeted revocation needs an acknowledged close and
  a positive proof that no SQLite handle can still access the old files. Keep generation death as the
  fallback if that proof is missing. Decide whether to ship targeted close first or keep that fallback
  as the initial implementation. A stale socket may be closed at a fence, since the transport belongs to
  the old owner. Existing recovery may discard cached files and rebuild from history even if the caller
  reuses a node ID. [Dispatch and recovery rules](cluster-state.md#12-durable-object-durability).

  **Decided.** Step 2 implements the acknowledged close: the home sends it after its SQLite connection is
  closed, and a targeted reconstruction waits for that acknowledgement. The death of the generation stays as
  the fallback when the acknowledgement does not arrive in time. A parked socket is closed at a fence.

## Migration and follow-up issues

Each heading below is the title and scope of one follow-up issue. Keep the old backend selectable through
the compatibility and rollback tests. Build the bare object host first, attach durability before exercising
it on live images, and attach alarms and sockets after the durable close and wake contract exists. Native
D1 shares that contract and can be developed alongside step 3 after step 2. This is the dependency order for
merging. It leaves functional coverage for the old backend until the final switch.

### 1. Host Durable Object trees on threaded homes

**Scope and deletion.** Implement durable role selection behind an opt-in used only with test storage.
Add home routing, bounded reservations, multi-isolate placement by memory and by how busy a home is, mailbox
and timed idle release for trees with no sockets. Replace per-call thread selection and one-isolate lookup in this role. Replace the durable
tenant-admission wait and the 200 ms, shared-loop GC and watchdog design in the new path. Keep GC and V8
observability, accounting and interruptibility. Land no deletions from the shipping backend at this step.

**Unit and kernel gate.** Assert actor creation, execution and destruction IDs all match the home, even
after a cross-thread cancellation. Fill one isolate and prove the next copy has a distinct ID and its own
home. Saturate one copy's home and prove the next woken tree of that Worker goes to another copy.
Force queue overflow and expiry. Assert that only waiting calls for that object get its overload and
that release returns reservations exactly once. Check input/output gates and a nested RPC during an await.
Use two actors sharing one isolate to distinguish a CPU reset from an isolate reset. Use an injected CPU clock to test refills, lower
and higher configured CPU limits, native SQLite CPU charging and legitimate long execution. Force idle-first
memory eviction, closed placement and hard reset as separate transitions. With a fake clock, exercise idle
and re-entry races. Force thread, fd and memory reservation failures, including a V8 group creation
refusal, and check that the child remains ready.

**Cluster gate.** None at this step. A single node/child integration fixture tests readiness, cancellation
and resource counters with disposable files. The opt-in cannot write production durable state yet.

### 2. Connect threaded homes to the durable log and recovery

**Depends on step 1 and [#29](https://github.com/coworkerd/coworkerd/issues/29).** #29 can proceed in parallel
with this design and the test-storage host. Merge its crash-during-acknowledgements regression before this
step changes the production DO or recovery path. The current takeover case kills the owner after a single
acknowledged write and therefore leaves a race the new test must cover.

**Scope and deletion.** Generalize the kernel shipper's singleton transport to generation-scoped log
multiplexing with per-home state. Keep frame contents, record order requirements and all peer formats.
Implement generation negotiation, reopen and drain. Keep checkpoints and the file reads of a cold open or a
rebuild off the homes, or measure what stays on a home and state its bound. Retire direct access from a threaded actor to
the old actor-loop shipper. Retain the old implementation while the upstream backend still runs. Keep
the fallback reset until the node can prove targeted close safely.

**Unit and component gate.** Exercise two homes sending interleaved frames with delayed and out-of-order
acknowledgements, byte backpressure, cancel/EOF, a stale reply after reopen and a stale close after checkpoint.
Hold one home's peer ACK and show its reply is still pending, including RPC output, while an unrelated
read runs on another home. Test no selected peers versus
unreachable selected peers, interrupted checkpoint, requested close during commit and generation loss.
Replay unchanged log/segment/manifest fixtures and rerun the existing channel and fault tests. A
deliberately early reply must fail the held-ACK test.

**Cluster gate.** Run the existing takeover test and #29 on the threaded data path with peer ACKs withheld
and with owner loss during active writes. Fence an old owner and verify its later writes cannot be
confirmed. Verify that a confirmed object reopens after child rotation, channel loss, and clean node drain
and restart. Exercise old/new backend peers in both ownership directions, using a mixed deployment only
with the current on-disk and peer protocols. Test both clean close and unresponsive old generation as
rollover cases.

### 3. Move hibernation, alarms and class migration onto the durable host

**Depends on step 2.** The alarm cap is the one decided under "Decisions taken in review".

**Scope and deletion.** Implement the timed lifecycle for sockets and other blockers, hand sockets to the
node for parking, and route wakes and alarms through validated opens. Retire child-owned parked socket
wake and the workerd-local scheduling hooks for threaded hosted objects. Remove the fixed scheduler cap
from the threaded path and retain bounded preparation and fair pumping. For a version change, class
rename or deletion, use existing namespace identity and migration checks, then fence the old binding or
actor as required by the current contract. Drain old pins and facets before reopening a migrated object on
a compatible home under the new definition. Do not move SQLite files because a class was renamed. Keep
namespace deletion and named/anonymous facet semantics.

**Unit and component gate.** Use a fake monotonic clock for 10 s idle, 70–140 s non-hibernating idleness, pending
operations with overlapping 15 min windows and close/wake races. Assert ordered socket handoff and the
correct epoch, close/error handling, constructor count, attachments/tags and auto-response timestamps.
Assert automatic frames allocate no actor or mailbox slot. Test single-alarm execution, failed-then-retried
schedule, exhaustion, a superseding schedule while an alarm runs, and deferral with no retry consumed.
Prove independently runnable alarms can exceed two on one node. Assert that class rename preserves
namespace identity and deletion rejects an old-generation open.

**Cluster gate.** Test idle WebSocket wake with ping and auto-response on either side of an actor being
dropped. Test the node losing ownership with a parked connection. Move the shard with a due alarm and
verify a stale hint cannot fire after the schedule changed. Exercise class rename, version pin, and drain
with both sockets and due alarms. Verify unconfirmed alarm clears never remove the next owner's authority
to fire, and that a confirmed clear survives takeover.

### 4. Execute D1-style databases on native SQLite homes

**Depends on step 2.** Use the same release and reservation machinery as DOs, and land with a backend fixture
that can execute colocated DO probes.

**Scope and deletion.** Implement the native SQL service on the durable role and stop constructing its
`SqlDatabase` actor on that path. Keep the old SQL actor only as long as rollback can select the old backend.
Delete the 200/800 ms SQL accounting from the migrated execution path. Implement the 30 s query and batch
deadline with the existing SQL limit error family. Preserve authorizer scope, transaction and storage
semantics, limit codes and precedence.

**Unit and component gate.** Compare native responses against the JS implementation for malformed input,
bindings, types, result formats and size boundaries, and counter/meta behavior. Prove rollback on query
batch failure, timeout during a write, result-size/serialization error and client cancellation. Verify
sequential execute retains its confirmed prefix. Cover duplicate keys, interrupt races, authorizer
restrictions and retry after interruption on a clean connection. Force read, batch, ingest and output-gate
failures and compare their HTTP status and D1 error envelope.

**Cluster gate.** Run existing `sql-atomicity`, HTTP and binding parity, CLI migration/import/export, restore
and an owner-kill test on native DBs. Demonstrate the final 30-second neighbor gate in the same child with
two tenants. Test timeout and retry through the public HTTP path and a Worker binding. Check timeout of an
entire batch of short statements as well as timeout of one long statement.

### 5. Make threaded durable execution the default and remove the upstream route

**Depends on steps 3 and 4.**

**Scope and deletion.** Enable the new backend by durable generation on a single drained node first.
Confirm readiness, load and fault behavior, then roll node by node. Validate drain back to the old backend
using the same public images and histories while that fallback still exists. After acceptance, delete
the upstream durable spawn branch, generated Cap'n Proto and runtime control routes used solely by it,
its actor-side socket manager integration, legacy SQL class, legacy residency coordinator, old schedulable
actor hooks and old backend selection switch. Remove the retired configuration, environment variables,
counters and documentation for durable CPU, SQL, pause waiting, process-wide GC clock and budget, and
`alarmFireConcurrency`. Split live readiness, startup and restart backoff settings from the obsolete
`stuckTimeoutMs` before removing its remaining role as a shared-loop watchdog. Keep reusable Worker API
shims, real process supervision, the public API, the ordinary threaded path, and launcher functionality
that remains in use. Audit the workerd patch series and build/publish the matching verified kernels and
allowlists through the existing maintainer workflow. The guide says that objects of one class may share an
isolate and that synchronous work should be short.

**Before the JavaScript SQL class is deleted.** All of these hold; otherwise the fallback in "Databases:
native execution, not an isolate per database" is built instead:
- step 4's comparison of native and JavaScript answers passes on both Linux architectures, and the database
  guide names every intended difference;
- every path the database guide lists without a proven interruption bound has a bound and a test;
- every update of the upstream base after step 4 carried the native patches with no change to the D1 contract;
- the open time and the memory of a native database are measured and recorded in the guide.

**Unit and component gate.** Audit every deleted config, metric and dispatch branch. Old documented config
files need an intentional, tested compatibility behavior for retired fields. Verify routing all event
kinds, ordinary admission, parent death, wake and new generation accounting. Run kernel and product
unit suites. Preserve equivalent negative controls wherever the old backend's fixture was the only test.

**Cluster gate.** Run every final gate below and the existing public integration suite. Record source,
kernel and runtime provenance, use the released launcher modes, and exercise both clean and unclean
upgrades with process and node failures. Test downgrade via confirmed close or process death while the
fallback is still available. Maintenance completes the switch only after those gates pass.

## Final acceptance gates

All latency measurements use monotonic clocks and report nearest-rank p50/p99, sample counts, failures
and process generation. Record engine and CPU configuration, test load and prewarming. Use the public
cluster fixture, and keep placement evidence for every same-process assertion.

| Gate from #16 | Pass criteria |
| --- | --- |
| Long SQL isolation | Sustain a statement until the 30 s limit on one DB. In the same durable PID/generation, sample queries against a different same-tenant and a different cross-tenant DB, and requests to a DO. Run equal, warmed quiet and busy workloads in repeated 30 s windows. Use at least 1,000 successful samples per probe class in each condition, with fixed offered rate and actual overlap confirmed by the server. Record all misses and errors, and fail if the busy run adds failures. Each class's busy p99 is at most 5 ms above its quiet p99. Also verify that the statement terminates and the batch rolls back on deadline. |
| Idle resources and recoverability | Call 5,000 distinct objects once each with a distinct confirmed value. Observe retirement, then compare threads, RSS and descriptors of the same durable PID against its warmed idle baseline. Take several idle pre/post samples with the same background conditions. Report absolute values and differences. Threads and descriptors return to baseline, RSS returns within the measured idle variation, and repeated cohorts leave no accumulating footprint. If there is excess RSS, the gate remains open. Call every ID again, assert a new constructor instance and exact saved value, and record cold p50/p99. |
| Hibernatable WebSocket | Observe the actor become nonresident after idle and verify a real protocol ping/pong and the configured application auto-response on the same connection. An external constructor counter and home-residency state remain unchanged through both messages. Only then send a message requiring tenant code and assert one reconstruction, correct attachment/state, and the expected reply. Record wake p50/p99. |
| Capacity exhaustion | Load a durable process to its first refusal using separate cases for mailbox, active trees, database threads, memory and descriptor reservation. Require explicit overload/refusal and a stable PID and generation. An already admitted object on another home must still answer. After release, capacity and a previously refused object's ability to run return. Repeat with CPU reset and memory-pressure paths. |
| Existing replication and ownership faults | All existing `durable-*` CTest cases, ownership/placement component cases and the public takeover case pass with the new backend exercised where applicable. Keep negative controls for lease/fence, denied and full peers, partial/torn and corrupt tails, recovery equivalence, segment/manifest failures and uncertain writes. Run #29 with an early-answer negative control and then on the correct host. Include mixed-backend ownership, restart/EOF and recovery/file replacement under live work, plus no-peer local flush, and verify the same values and response codes. |
| A CPU-bound object (added in review) | In one durable PID/generation, an object runs a synchronous loop until its CPU limit. Requests to objects of another Worker of the same tenant and of another tenant are sampled meanwhile, at a fixed offered rate, with at least 1,000 successful samples per class and overlap confirmed by the server. Each class's busy p99 is at most 5 ms above its quiet p99. The event ends with `CPU_LIMIT_EXCEEDED`, that object is reset, and another object in the same isolate copy still holds its in-memory state. Record also what an object of the same Worker in the same copy sees meanwhile: it waits for the event, as the guide says. |
| Wake latency (added in review) | Measured on one machine in one run, before the old backend is removed, with the Worker's isolate copy warm: reopening a released object and waking a hibernated object with an application message have a p50 and a p99 at most 5 ms above the old backend's. The [baseline](threaded-durable-baseline.md) recorded 14.877/19.088 ms and 6.148/7.967 ms. |

See [the current-main baseline and replay instructions](threaded-durable-baseline.md) for the comparison workload.

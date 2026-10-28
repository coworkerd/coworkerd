# Changelog

## 0.5.1

- Fix Durable Object recovery after eviction and reopening: a repeated header for the same WAL keeps the replay
  position and checksum chain, including histories written by 0.5.0. New WAL generations still start at frame one;
  header validation, frame continuity, salts and checksums remain enforced.

## 0.5.0

0.5 adds stateful Workers: SQLite Durable Objects and D1 databases alongside stateless Workers, with inbound WebSockets, hibernation, alarms and JS RPC. Shard ownership lives in conditional object-store epochs. Committed transaction frames are confirmed by reachable peers (or flushed locally with no peer), uploaded as node log segments with per-object manifests and snapshots, and replayed after takeover. Confirming peers buffer in memory: simultaneous loss of an owner and all confirming peers can lose the unuploaded tail, which can exceed the normal 30 s segment interval during an outage.

The 0.4.x upgrade procedure retains the bucket, node identities and volumes; bare-metal and in-place upgrade execution are part of release validation. The object layout adds DO history and ownership objects; finish upgrading every component and node's storage policy before using the new features. `/v1` keeps its existing fields and adds namespace / database endpoints, quotas and inspection fields. New errors include `DO_UNAVAILABLE`, `DO_OWNER_EXPIRED`, `DO_UNSUPPORTED_STORE`, `DO_RECOVERING`, `DO_NAMESPACE_UNAVAILABLE`, `DO_HOP_LIMIT`, `DO_OBJECT_TOO_LARGE` and `WEBSOCKET_LIMIT`; durable calls can also return `STORAGE_UNAVAILABLE` and `QUOTA_EXCEEDED`. Clients should follow the [error guide](docs/guide/errors.md) before retrying a write.

- **Durable Objects:** migrations declare SQLite classes; namespace IDs stay stable across versions and renames. Fetch and public method calls route to the shard owner. Durable processes have an object-tree resident target, no age / request-count rotation, per-event CPU and logs, and facet-aware replication and recovery. Object and tenant byte limits are enforced at durable confirmation using reported database sizes; transactions that do not grow their database remain permitted, including deletions.
- **D1:** tenant-scoped databases use a platform SQL Worker, with bindings, the basic JavaScript API, HTTP SQL and `cowctl d1 create/list/delete/execute`. Bindings also work in Workers with no declared DO class. `batch` is one transaction and rejects on any statement failure; `exec` keeps earlier successful lines and stops at its first failure. Database names include hyphens and leading digits. HTTP SQL preserves node statuses and bodies, honors both result formats and does not replay uncertain requests; Sessions, replicas, import/export and point-in-time restore are absent.
- **WebSockets and alarms:** ingress relays frames across processes and peers; hibernation retains connections across idle object eviction. Process / node exit closes them. Alarm times are replicated database state, while object-store hints discover due objects after takeover; retries and discovery delays remain possible.
- **JS RPC:** Service Bindings to `WorkerEntrypoint` classes and Durable Object stubs expose methods through workerd's capability protocol. Nodes validate envelopes and proxy capabilities without decoding arguments; capabilities expire with the call tree's lease. `placement.subcalls` chooses the normal home policy or caller-first stateless placement.
- **Operations and documentation:** configurable durable process count (including zero on forwarding-only nodes), peer buffers, confirmation / recovery / upload / snapshot / disk limits, quotas and diagnostic state. Graceful restart retains unexpired culprit suspicions until the new epoch has persisted them, without extending their expiry; failed recovery keeps the old object. New [Durable Object](docs/guide/durable-objects.md) and [database](docs/guide/databases.md) guides and the `durable-counter` example accompany the updated architecture and installation guides.

Correctness fixes before release: startup preserves the current epoch's append-log file when discarding an untrusted cache, and restores its WAL frame positions on same-boot restart. Local-flush failures return `STORAGE_UNAVAILABLE` without confirmation. Isolation exclusions are published at the next seal even without new confirmed records, and sent again when a peer reconnects. Durable fetch reasons redact known secrets, paths and stack lines within 400 UTF-8 bytes; method exceptions retain RPC transport. Cross-node HTTP SQL and node-originated alarms pass peer validation without a calling object, and dispatch refusals return promptly with their original status and body. Durable usage polling retains node ids across asynchronous reads so membership refresh cannot invalidate its startup iterator.

The changes of 0.5.0 in detail, grouped by development milestone. Early-stage limitations below are historical; the release behavior is described by the completed milestones and fixes above. Measurements are the recorded development runs, not new release-wide guarantees:

JS RPC: a Worker's methods, and a Durable Object's, can now be called over a Binding. `env.SVC.method(args)` reaches
another Worker of the tenant and `stub.method(args)` reaches an object, with Cloudflare's own types -- structured
clone, `RpcTarget`, `RpcStub`, streams and functions -- and the exception a method throws reaching the caller. It is
workerd's own mechanism throughout: the node implements `EventDispatcher.jsRpcSession()` on the channel its runtime
already uses and on the peer session, and answers a call with the target's own entry point as a capability, which it
proxies to the caller. A tenant's arguments are serialized in the calling isolate and decoded in the target's; the node
reads neither, here or on the way between nodes.

- **A method call is a call of the tree, like a `fetch` through the same Binding**: it is placed the same way (so the
  target may run on another node), draws on the budget of subrequests -- eight root slots, two consumed per direct child (four calls; the fifth
  is refused), with one granted slot per child --, and
  carries the deadline; over the budget it throws `SUBREQUEST_BUDGET_EXHAUSTED` and past the deadline `LEASE_EXPIRED`,
  as the HTTP answer of a `fetch` would say. The platform's code starts the error's message; the runtime also sets `.code` when possible. Getters and
  returned capabilities can retain only the message prefix; an application message with the same prefix can look like a platform code. Calls on a capability a method returned are part of the same call and draw nothing more.
- **A capability belongs to the call tree that made it**: an `RpcTarget`, a stub, a stream or a function that crosses a
  Binding works wherever the caller holds it -- through this node and, when the target is elsewhere, the peer session --
  and is released with the tree's lease (`LEASE_EXPIRED` afterwards; a disconnection when the node that held it went
  away). One call's arguments or return value are at most 32 MiB serialized, workerd's own limit.
- **Durable Objects**: a stub has the class's public methods as well as `fetch`. The call goes to the shard's owner with
  the shard's epoch and its fencing, as a request does, and is an event of the object there: its CPU is topped up, its
  lines are the event's, and an object's method call on an object starts a call tree of its own under the same 8-hop
  limit. For Service Bindings, a class that is not a `WorkerEntrypoint`, and a plain object entry point, have `fetch` only.
- **Where a stateless subcall executes is now a policy point**, `placement.subcalls`: `home` (the default, unchanged)
  places it like any call, so it goes to the Worker's home; `local` tries the calling node first, then falls back to other eligible nodes. Both work for `fetch` and for method calls. Which should be the default waits on a measurement of the two
  under load.
- **Observation**: the state endpoint's `jsRpc` counts the sessions, the method calls, the capabilities proxied and held
  and the failures by code, and the node writes one `[rpc]` line per method call of a Binding (the target, the entry
  point, the call tree, how long the target's capability took).

Inbound WebSocket connections for stateless Workers. A Worker that answers an upgrade with `101` and a WebSocket now gets
its connection: the node that received the upgrade relays text and binary messages and close frames, with their code and
reason, both ways -- also when the Worker runs on another node, through the signed peer session, which the connection
keeps for its life. Outgoing WebSockets (a Worker's `fetch()` upgrade, through the egress service) do not change.

- **A connection is one request, and its messages are the request's events**: each message the Worker receives starts
  with the request's CPU topped up -- workerd's patch 0012; upstream does that for Durable Objects only -- and renews
  the call tree's lease as a new root's (the deadline to the lease time from then, the budget of subrequests to the
  root's), so a connection's handlers are limited and leased message by message however long it lasts: a message's
  Service Binding calls and theirs share the root's budget (four calls by default), as a request's do. A message over
  the CPU or memory limit closes the connection with `1011` and `CPU_LIMIT_EXCEEDED` or `MEMORY_LIMIT_EXCEEDED`, the
  codes an HTTP answer carries, and the isolate is replaced as for a request. A message is at most 32 MiB (workerd's
  default); there is no idle timeout and no compression.
- **Admission and a limit per tenant**: an upgrade's admission slot is given back at its `101` -- between two messages a
  connection is not executing --, and each node holds at most `ingress.webSocketsPerTenant` (1000) open connections of
  one tenant: an upgrade beyond them gets `429 WEBSOCKET_LIMIT` before the Worker runs, and other requests are not
  affected. The state endpoint's `webSockets` counts the open connections per tenant, and each relay of a connection
  writes a `[websocket]` line when it opens and one when it closes (tenant, Worker, which side closed, the code, the
  cause).
- **Draining and process rotation close connections with `1001`**: a draining node closes its connections at once --
  those it runs for another node's ingress through that ingress -- and its drain record counts them; a process
  generation being replaced closes its connections at the rotation's switch, and the rotation's record counts them. A
  client whose ingress lost the node its connection ran on sees an abnormal closure (`1006`).
- **Usage and logs**: a connection counts as one request; its CPU is metered while its messages run (workerd reports a
  lasting request's CPU at most once a second), not only when it closes; the lines its handlers write reach the tenant's
  logs as each message runs (log lines below).
- **Configuration**: `ingress.webSocketsPerTenant` (1000; `0`: no limit).

The first Durable Object milestone (v59), on one node: A wrangler project's `durable_objects` and `migrations`
convert, versions carry their Bindings and classes, and the node that runs an object keeps the object's SQLite database
on its disk. At that stage, cluster ownership and object-store persistence were not yet present; the later milestones below
added them before release.

- **Conversion and versions**: `durable_objects.bindings` (`name`, `class_name`; a `script_name` only when it names the
  Worker itself) and `migrations` in their order -- `new_sqlite_classes` and `new_classes` both declare a SQLite class,
  `renamed_classes`, `deleted_classes`; a class of another Worker and `transferred_classes` are refused. A version
  definition gains `durableObjects` and `classes`; the control plane checks that each Binding's class is declared and its
  name taken by nothing else in `env`, and the trial load that each declared class is exported, extending
  `DurableObject` or with `fetch` (`LOAD_FAILED`, `ENTRYPOINT_MISSING`, naming the class).
- **Namespaces**: the control plane makes a class's namespace id (32 random bytes) at the first upload that declares the
  class and keeps it for that name in later versions; `renamed_classes` moves it to the new name and `deleted_classes`
  marks it `deleting`, after which no version declares or binds the class again (its data is kept). `GET
  .../workers/{w}` lists the namespaces, and a tenant has at most `durableObjectNamespaces` of them (16; `422
  QUOTA_EXCEEDED`).
- **The durable process group**: `d0`, ... (`durableObjects.processes`, 1 by default), of class `durable`, never replaced
  for age or requests -- only when the workerd file changes, by hand, or at the resident threshold, and then one at a
  time: the old generation drains and its objects' requests in flight complete, and new requests wait up to 5 s for the
  next generation (`503 DO_UNAVAILABLE` beyond). Its runtime adds the `Host` class, a SQLite-backed Durable Object
  namespace stored in the node's `<stack>/do/` directory, which the launcher binds writable into the sandbox.
- **Objects**: a namespace Binding has `idFromName` (the SHA-256 of the namespace id, `name` and the name),
  `newUniqueId`, `idFromString`, `get` and `getByName`, and a stub's `fetch` -- WebSocket upgrades included -- reaches the
  node, which runs the request on the object's durable process: the object's `Host` runs the Worker's class as a facet,
  from the version's isolate under the same loader key as its stateless calls, with a SQLite database of its own
  (`ctx.storage.sql`, the key-value methods, transactions). A class's `env` holds the version's variables and secrets and
  the Worker's Durable Object Bindings. At this first milestone RPC methods, alarms and hibernation were absent;
  later milestones added them. Service Bindings in a class remain unsupported. After a version is activated, an object's next request runs the new class on the same data.
- **Failures and limits**: each request to an object and each message of a connection to it runs with the request's
  CPU limit, the memory limit is the version's isolate's. An exception the class throws leaves the object as it is --
  the same instance takes the next request -- and the caller's `fetch()` answers `500 EXECUTION_FAILED` with its
  message; a request over a limit aborts the object -- `503` with `CPU_LIMIT_EXCEEDED` or `MEMORY_LIMIT_EXCEEDED` -- and
  the next request constructs it again with its data (the limit restarts every object of the version on that node). A
  stateless Worker's call to an object is a Binding call of its tree; an object's calls to objects start trees of their
  own, at most 8 in a row (`508 DO_HOP_LIMIT`). Each request writes a `[do]` line (the class, the first 12 digits of the
  namespace and object ids, the process, the outcome) and counts in usage as a request.
- **Measured** on one node of a Mac: an object's first request 57 ms (the version's isolate loaded in the durable process,
  the class constructed), then 2 ms (the median of 19), and 11 ms with its data after `d0` was killed and started again;
  10,000 messages to one object on one connection in 5.4 s, 5.0 s of CPU together and none ended; the request after a
  CPU overrun 7 ms, the object constructed again with its data, and after an exception 2 ms, on the same instance; eight
  calls in a row between objects 72 ms, a ninth refused; a manual rotation of `d0` let a request in flight on an object
  finish (the drain took 1.7 s) and started the next generation 33 ms after the old one exited, while a request that
  came meanwhile waited 1.8 s for it; an object's 21 message lines were in the tenant's logs 8 ms after the last, its
  connections open.

The cluster-ownership milestone (v60): one owner per shard. A namespace's objects fall into its shards (1024, an object's by
the first 8 bytes of its id), and each shard has one owner at a time -- the node that created the shard's latest epoch,
`do/<namespace>/shards/<shard>/epochs/<n>`, an object of the store created only if absent -- which runs the shard's
objects; the other nodes send it their requests through the peer session, WebSocket upgrades included. At that milestone the objects' data
still stayed on the owner's disk, so takeover started empty. The persistence milestone below superseded that behavior.

- **Taking a shard**, when a request to one of its objects first comes: a shard without an epoch goes to its default
  owner -- the first, in the rendezvous order of `<namespace>/<shard>`, of the members that run durable processes and
  are not draining --, to which the other nodes send the request; only when it refuses or cannot be reached does another
  node take it, after 200 to 500 ms and a fresh read. A shard whose owner's membership expired -- a TTL plus the grace
  after its last heartbeat, by the store's clock -- or that its owner released is taken over by the default owner among
  the others, the others waiting their rank times 1 s; a node started again under its id takes its own shards back at
  once, with their data. A create whose answer was lost is read back: the writer's instance and a random value in its
  bytes say whose it is. The owner hint `do/<namespace>/shards/<shard>/owner` names the latest epoch; a missing or wrong
  hint costs a listing of the shard's epochs and is written again.
- **The check**: a request names the shard's epoch its node expects, and the owner refuses any other, `409
  DO_SHARD_EPOCH_MISMATCH`; the caller's node then reads the shard again and sends the request once more. A refusal or a
  connection not made is retried that once; an answer lost after the request was sent is `RESULT_UNCERTAIN`. A node
  caches the owners it found until a request is refused, the owner's membership expires, the shard turns out released, or
  60 s pass without a request confirming it.
- **Expiry and drain**: a node whose own membership expires gives its shards up at once -- `503 DO_OWNER_EXPIRED`, its
  objects' WebSocket connections closed with `1001`, its durable processes started again empty -- before any other node
  may take them over, and keeps serving stateless calls; back with a new membership epoch it does not take back what
  others took. A draining node answers `503 DO_OWNER_DRAINING`, closes its objects' connections with `1001`, lets their
  requests in flight finish, closes its durable processes and writes a release marker beside each shard's epoch: the next
  owner takes the shard at once, while the rest of the drain goes on, and the drain record counts the shards.
- **Stores and nodes**: a node takes no shard on a store whose create-if-absent does not hold under concurrency (`503
  DO_UNSUPPORTED_STORE`); without the store it serves the shards it holds until its membership expires, and a request
  whose owner it cannot find fails with `503 STORAGE_UNAVAILABLE`. A node with `durableObjects.processes` `0` runs no
  durable process, takes no shard and is no default owner; its heartbeat announces its durable processes. A node's object
  store identity may now read and list `do/` and write `do/*/shards/*` (compose's `secrets` step).
- **Observed**: the state endpoint's `durableShards` (the shards held by namespace, takeovers by reason, releases,
  refusals by code, the forwarding, the owner cache), a `[do-shard]` line for each take, release and refusal, and each
  `[do]` line names the object's shard and epoch, on the node that ran it and on the node that sent it.
- **Measured** on three nodes of a Mac, with a TTL of 15 s and a grace of 10 s: an object's first request through a node
  other than its shard's default owner 79 ms, the shard taken on the way, then 2 ms whether the node that received a
  request ran it or sent it on (the medians); 60 requests at once through the three nodes counted 2 to 61 on the one
  object. With its owner killed, a shard was taken over 26.0 s after the owner's last heartbeat by the store's clock,
  25.4 s after the kill, the requests meanwhile refused each within 36 ms; cut from the store, an owner gave its shard up
  14.9 s after the cut, and the shard was taken over 26.6 s after its last heartbeat. A drain released its shard 1.24 s
  after it began, once the request in flight had finished, and the next owner took the shard 33 ms after the release and
  served it 60 ms after. A node started again under its id took its shard back 15 ms after it was ready, the object's
  data kept. Without the store, a request to a shard no node held failed after 10.0 s (`STORAGE_UNAVAILABLE`); with its
  owner just killed, a request failed in 23 ms, sent twice (`PEER_UNAVAILABLE`).

Durable Objects keep their data when a shard moves. An object's transactions, not this machine's files, are its state:
the durable process hands the WAL frames of each committed transaction to the node's replicator over a descriptor it
inherited at startup (workerd's patch 0013), the replicator has `durableObjects.peers` (2) other nodes hold them before
the object's answer goes out, merges the frames of all its objects into segments on the object store, and keeps each
object's manifest naming what makes it up -- so a node that takes a shard over rebuilds its objects from that history,
and never reads the previous owner's files. Recovery depends on uploaded history, trusted surviving files or a surviving confirmed peer tail; the failure
boundaries below also apply to writes answered `200`.

- **The commit path costs a peer round trip, not a disk.** The peers of an object's shard are the first entries after it
  on the member ring; they hold the records in memory and answer with the highest sequence they hold, and an object's
  transactions are confirmed in order, so its answers come out in the order it committed them. The owner no longer waits
  for its own disk (`synchronous=OFF`), and nothing on that path uploads anything. Measured on three nodes of one
  machine: commit to confirmation p50 0.12 ms, p99 0.53 ms, and 2 ms for the write a client sees. A peer that cannot be
  reached is retried under the same node id; membership changes can select new peers for later records. A reopened
  stream receives exclusions before the remaining unsealed records. An empty selected peer list (a single-node
  cluster) requires a successful local disk flush; failure returns `STORAGE_UNAVAILABLE` and is logged.
- **Segments, manifests and snapshots.** Every `durableObjects.segmentIntervalMs` (30 s) or `durableObjects.segmentBytes`
  (16 MiB) the records the peers hold are sealed into one segment of the node's log, `nodes/<node>/wal/<epoch>/<segment>`
  -- one for all of the node's objects, so the store sees a node's writes, not an object's -- and then each object the
  segment touched gets the next revision of its manifest, `do/<namespace>/objects/<object>/manifest/<rev>`, created only
  if absent. Only once the manifests name the segment are the peers told to drop what it covers and the databases told
  they may checkpoint those frames. An object whose log passes `durableObjects.snapshotBytes` (64 MiB), or whose existing snapshot is a day old,
  has its whole database uploaded after a checkpoint, in parts, and the segments it covers leave the manifest. The first snapshot needs the size threshold; time alone does not trigger it.
- **Rebuilding.** The first request for an object this node does not have at the shard's data epoch waits while the
  manifest is read, the snapshot's parts are downloaded (at most four at once), the segments are replayed in order and
  the peers are asked for what they still hold of the previous owner's log. Records are merged, deduplicated and
  ordered by shard epoch and owner sequence. WAL generation, checksum and consecutive-frame checks reject frame
  gaps; there is no separate proof that every acknowledged transaction was recovered. Past `durableObjects.recoverTimeoutMs` (30 s) a request
  waiting for a rebuild gets `503 DO_RECOVERING`. Measured: an owner SIGKILLed with four writers on one object, 2,694
  writes answered `200`, another node serving the object 23.7 s later (the membership's TTL and grace) from two segments
  and 785 records of the peer's tail, replayed in 0.6 s -- **every one of the 2,694 there, no torn transaction**; of the
  eight writes in flight at the kill, none turned out committed in that run. A 1 GiB object was rebuilt in 19.5 s from a
  snapshot of 50 parts and 33 segments.
- **This machine's files are a cache.** They are bounded by `durableObjects.diskBytes` (half of the filesystem), and the
  files of cold objects whose changes are all in a segment go first. Beside them the node keeps the machine's boot id
  and a marker a clean exit writes: started again within the same boot, or after a clean exit, it keeps them -- a
  durable process that restarts opens the files as they are, with each WAL cut back to the last frame its log names,
  and confirmed positions beside the files let a daemon restart reuse them too (completed in v62, fixed before release).
  A machine that restarted without a clean exit discards the cache and rebuilds from available history. Native macOS
  has no readable boot id, so an unclean process exit also discards its local durable files.
- **Failure.** A write no peer confirms within `durableObjects.confirmTimeoutMs` (5 s) -- the peers are unreachable, or
  what they buffer for this node (`durableObjects.peerBufferBytes`, 256 MiB) is full because the store has been out of
  reach -- fails with `503 STORAGE_UNAVAILABLE`; the node then isolates the records nobody confirmed (they are never
  confirmed, never put in a segment, and the peers are told to drop them; the next seal persists their exclusions even without
  new confirmed records, and reconnect repeats the drops) and rebuilds the object from its history
  before serving it again. **The result of such a write is uncertain**, as is a write in flight when a node dies, so the
  errors guide now asks for an idempotency key. While the store cannot be reached, uploads are retried with a growing
  wait and the writes go on.
- **The fence.** Every record carries the shard's epoch. A peer rejects epochs older than the nonzero epoch it knows;
  unknown and higher epochs pass that comparison. A manifest writer gives up when the previous revision names a
  newer epoch. These checks depend on each participant's knowledge; they do not prove every stale writer is refused.
- **One kind of object.** There is no hot standby tier (the namespace record's `tier` field is gone): the peers buffer
  what is not on the store yet. One object is at most `durableObjects.objectBytes` (10 GiB (10,737,418,240 bytes)), judged by
  reported database sizes at checkpoint, open and close; past it a growing transaction answers `413 DO_OBJECT_TOO_LARGE`.
  Transactions whose final commit page count does not grow the database remain permitted. `ctx.facets.clone()` is
  refused, because a copy of a database reaches no log.
- **Accepted step back**: the peers do not wait for their disks (a `fdatasync` there would cost tens of milliseconds
  instead of about one), so simultaneous loss of an owner and all confirming peers can lose the unuploaded tail. The normal segment
  interval is 30 s; upload failures or backlogs can extend that tail. A surviving peer, uploaded history or trusted
  local files can recover after a failure. With no peers, loss of the only disk before upload loses its tail.
- **Configuration** (`durableObjects`): `peers` (2), `confirmTimeoutMs` (5000), `recoverTimeoutMs` (30000),
  `segmentIntervalMs` (30000), `segmentBytes` (16 MiB), `snapshotBytes` (64 MiB), `snapshotIntervalMs` (a day),
  `snapshotPartBytes` (16 MiB), `diskBytes` (half the filesystem), `peerBufferBytes` (256 MiB), `objectBytes` (10 GiB),
  `checkpointBytes` (4 MiB), `idleCheckpointMs` (10000), `uploadRetryMs` (500) and `uploadRetryMaxMs` (30000),
  `recoverParallelism` (4). A node's object store identity now reads and lists every node's prefix (`nodes/*`, where the
  log's segments are) and writes `do/*/objects/*`; `coworkerd secrets`, compose and the install guide follow.
- **Observed**: the state endpoint's `durableLog` (the confirmation latency, what is not confirmed and not sealed, the
  segments and uploads, the peers and their streams, the checkpoints, the rebuilds and their time, the buffer this node
  holds for others, this machine's files) and `[do-log]` lines for sealing, uploads, manifests, snapshots, rebuilds, peer reconnection or membership-driven selection and a confirmation that failed with the records it isolated; a `[do]` line says how long a
  request waited for a rebuild.

Durable Objects a team can use: hibernatable connections, a resident limit, alarms, a database (D1), the namespace and
database endpoints of `/v1`, `cowctl do` and `cowctl d1`, and the storage quotas. There is still one kind of object --
the peers buffer, the segments go to the object store, an object rebuilds from them, and an object is at most 10 GB, as
on Cloudflare.

- **Hibernatable WebSockets.** A class's `ctx.acceptWebSocket(ws, tags)` with `webSocketMessage`, `webSocketClose` and
  `webSocketError` handlers, `ctx.getWebSockets()`, `ctx.getTags()` and `ctx.setWebSocketAutoResponse()` all work: an
  object with none of its connections sending anything may be evicted while they stay connected -- at the entry and at
  the owner the relay is unchanged, so the client notices nothing -- and the next message wakes the object with them.
  Each message is still an event of its Worker: its CPU is topped up, its lines are kept and it counts in the usage.
- **A resident limit.** A durable process targets `durableObjects.residentObjects` (4096) resident object trees; past
  it idle trees go in least-recently-used order, facets before their root. Active trees are protected and can
  temporarily exceed the target. An
  evicted object's hibernatable connections hibernate rather than close. The state endpoint's `durableLog.resident`
  reports, per process, how many are resident, the limit, how many were evicted and woken, and how long the wakes took.
- **Alarms.** `ctx.storage.setAlarm`, `getAlarm` and `deleteAlarm` and the `alarm(alarmInfo)` handler. The time lives in
  the object's own database, so it is replicated and recovered with everything else; the owner node schedules the firing
  and fires it as an event of the object's Worker (the version's class, admission, the CPU topped up, a lease, a `[do]`
  line). A hint beside the shard's epochs (`do/<ns>/shards/<s>/alarms/<id>`) names each object's next alarm, so the node
  that takes the shard over discovers due objects. The object's own database decides whether the handler runs;
  delivery can be delayed or repeated across failures, and a missing hint can delay it until the object opens again. A handler that throws is retried with Cloudflare's backoff (two
  seconds, doubling, a quarter of jitter, six counted tries and then up to about seventeen minutes), and
  `alarmInfo.retryCount` says which try it is; a successful handler clears the alarm in its own transaction, which its
  peers confirm before the answer, so handlers must tolerate retries around failures. A class without an `alarm()` handler has its alarm
  dropped rather than retried for ever. Upstream workerd refuses `setAlarm()` on a facet, and a Coworkerd object runs
  the tenant's class as one; patch 0014 lets the hook report to the node instead.
- **A database (D1's shape).** `POST /v1/tenants/{t}/databases/{d}` makes one: a Durable Object namespace of its own
  with one shard and one object, whose class is the platform's own SQL Worker in the static runtime -- run through the
  Host as a facet, like a tenant's class, with the same replication, rebuild and 10 GiB limit. A `d1_databases` Binding
  in wrangler's configuration gives `env.DB` the D1 API over D1's own wire protocol: `prepare().bind().run()`, `all()`,
  `first(column?)`, `raw({columnNames})`, `batch()` and `exec()`. A batch is one transaction and rejects on any statement failure; `exec` keeps earlier
  successful lines and stops at its first failure. Results include D1's `meta` (`changes`, `last_row_id`,
  `rows_read`, `rows_written`, `changed_db`, `size_after`) and its errors (`D1_ERROR:`, `D1_EXEC_ERROR:`,
  `D1_TYPE_ERROR:`, `D1_COLUMN_NOTFOUND:`). The same protocol is the HTTP SQL interface,
  `POST /v1/tenants/{t}/databases/{d}/query`, with the tenant's own credential, `ARRAY_OF_OBJECTS` or `ROWS_AND_COLUMNS` results, preserved platform status codes,
  and retries only before execution is possible. A lost reply after connecting is `RESULT_UNCERTAIN`. No migration tool, no import or export,
  no restore to a point in time, no read replicas and no Sessions API (`withSession()` is refused).
- **`/v1`**: `GET .../workers/{w}/namespaces` and `.../namespaces/{class}` (the shards, the owner of each shard an epoch
  was taken for, the objects and the bytes, summed from the nodes and marked `approximate` with the time it was made),
  `DELETE .../namespaces/{class}` (the semantics of a version's `deleted_classes`: no new request, the data stays), and
  `POST`, `GET`, `DELETE /v1/tenants/{t}/databases[/{d}]`.
- **`cowctl`**: `do list`, `do inspect <class>`, and `d1 create|list|execute|delete`. Creation waits up to 15 s for non-draining nodes to report the revision (warning if still behind), then
  polls `SELECT 1` until one serving path succeeds; `--no-wait` skips both waits.
- **Quotas.** Each tenant's Durable Objects hold at most `durableObjectBytes` (10 GiB by default): past it a growing write is
  refused with `422 QUOTA_EXCEEDED`; reads and transactions that do not grow the database go on. What a tenant holds is what the nodes publish of their own objects
  (`nodes/<id>/durable-usage`, every `durableObjects.usagePublishMs`, an hour by default), so enforcement uses a lagging estimate, not an exact distributed counter; failed reads and publications
  can delay it beyond that interval. A database or a class counts against `durableObjectNamespaces` (16) alike, and the
  objects of a deleted one stop counting.
- **An object past its own limit refuses growing transactions.** `413 DO_OBJECT_TOO_LARGE` is answered at the moment a write
  would be made durable, not before the request is dispatched: reads and transactions that do not grow the database remain possible and the object is not
  reset (0.5's fourth part refused the object as a whole). A refused write is not durable -- no peer
  holds it and no manifest names it -- but on the node that refused it, it stays readable from that machine's file
  until the object is next opened, and the rebuild that follows drops it.
- **The same boot reuses this machine's files.** The position each database is confirmed at is written beside it, so a
  restart of the daemon or of a durable process within the same boot serves the object's first request from the files
  instead of replaying its whole history. An unclean machine restart still throws the files away.
- **Configuration** (`durableObjects`): `residentObjects` (4096; `0`: no limit) and `usagePublishMs` (3600000). Quota:
  `durableObjectBytes` (10737418240).
- **Measured** on three nodes of a Mac: 100 hibernatable connections on one object, which was evicted for a resident
  limit of two with 0 connections closed, and the first message answered in 5 ms and reached all 99 others; 24 objects
  against a limit of three, 46 evictions and 52 wakes, every wake under a millisecond; an alarm whose handler threw
  retried 2,012 ms later with `retryCount` 1, and one set for 12 s fired exactly once, 144 ms after its time, on the
  node that took the shard over when its owner was drained; a database's three rows still there after its owner was
  drained; a write past a 1 MiB object limit refused after 128 rows, with the read right after it still answered.

Fixed: a node no longer drops members on a listing that does not hold its own heartbeat. The first second of a stopped
object store answered a listing of `members/` with `200` and an empty list, and two nodes dropped each other on it
(seen in 0.5's development). A node that wrote its own heartbeat within the TTL now treats such a listing as a failed
one -- nobody is dropped by it, the member table stays as it was, and a `[membership] incomplete-listing` line and the
state's `incompleteListings` count it. The recovery anchor is unchanged.

Log lines arrive as they are written: a Worker's `console.*` lines reach the tenant's logs while its request runs --
each message of a WebSocket connection's as the message runs -- instead of when the request ends. The tail workerd gives
each Worker is a streaming one, which its Worker Loader allows only when the runtime runs with workerd's `experimental`
compatibility flag; tenants' own flags still come from the non-experimental list, so what their code can do does not
change. workerd's cap of 256 KB a request does not apply to a streaming tail, so the node keeps it per event -- a
request, or one message of a connection, including each separate event on an object --
and puts one warning line in place of an event's lines past it (`tenantLogPosts.eventsOverBudget` in the state).

Fixed: with the launcher, every workerd generation's start logged `[process-pool] task failed: epoll_ctl(... EPOLL_CTL_DEL
...): Bad file descriptor` (since 0.3.0): the launcher's status socket was closed before its observer left the event
loop. The observer goes first now: on Linux with the launcher, 0.4.1's node logged the line at each of six generation
starts, this one at none of eight.

## 0.4.1

A full garbage collection is no longer taken for a CPU overrun, and processes are no longer replaced every hour. V8
collects an isolate on the thread that runs its requests, and a long-lived isolate's full collection -- mostly the
destruction of the wrappers of objects it let go, in one atomic pause -- was charged to whichever request it ran in: on
a busy node it terminated well-behaved requests with `CPU_LIMIT_EXCEEDED`. With a tree of Workers at 15 requests a
second per root, four processes and no rotation, 0.4.0 terminated 50 requests in an hour on an amd64 machine, each right
after a full collection; 0.4.1, beside it on the same machine, none. A node also no longer expires, and takes a new
epoch, because one store write of its heartbeat was answered late: the heartbeats keep their cadence, and a node survives
two failed ones in a row. The object store's layout and `/v1` do not change; the node's configuration gains `gc`, and two
of rotation's defaults change.

- **Collections are left out of the CPU limit, within a budget**: the thread CPU of a full collection that runs during a
  request -- also while it is under way -- is subtracted from the request's CPU as far as its Worker's budget goes, a
  token bucket of `gc.budgetMs` (2,000 ms) refilled at `gc.refillMsPerSecond` (100 ms a second), one per Worker in each
  process, which the Worker's next isolate takes over; beyond it, collections are charged as before. A tenant that
  allocates on purpose so gets at most a tenth of a core of uncharged collections (87 ms a second measured over 45 s of
  back-to-back collections), and a request that only runs JavaScript is terminated at the limit as before. The rows of
  completed requests carry the CPU left out (`gcExcludedNs`).
- **Long pauses are named**: a collection that pauses its process for `gc.longPauseMs` (100 ms) or more writes a
  `gc-pause` row -- the isolate, the pause, the request it ran in or none, the kind of collection, the CPU left out and
  the budget left -- to `logs/workerd.log` after `[cpu-limit] ` and to its node, whose state endpoint keeps the latest
  rows (`processes.recentGcPauses`) and counts collections, long pauses and the longest per process
  (`processes.entries[].gc`).
- **Idle isolates are collected between requests**: a Worker's isolate idle for `gc.idleCollectAfterMs` (50 ms) whose
  old generation grew `gc.idleCollectMinGrowthMiB` (4 MiB) since its last full collection is collected outside any
  request, never while its process has work waiting; and every isolate starts with an 8 MiB old generation
  (`gc.initialOldSpaceMiB`, V8's `--initial-old-space-size`), so its first full collection comes while it holds little.
  Measured on an arm64 machine, every first full collection of the tree's isolates was such an idle one and paused its
  process less than 21 ms.
- **The runtime's heap is bounded**: the runtime bridge's isolate, which every call passes through, gets a heap of
  `gc.runtimeHeapMiB` (128 MiB) instead of one V8 sizes from the machine (four times the most it was measured to hold
  after a full collection, 7.7 MiB under load, and at least 128 MiB). Its own full collections, outside any request, are
  now the longest pauses left, and are named like the others (isolate `runtime`).
- **Rotation on demand**: `rotation.maxAgeMs` defaults to `0`, so a process is no longer replaced every hour, and
  `rotation.residentBytes` left out of the configuration is 70% of the node's memory (its container's `memory.max`, else
  `memoryBudgetBytes`, else the machine's; the state endpoint names which): a generation is replaced when its workerd
  file changes, by hand, or at that threshold. With the collections above a process's memory stays level -- in the same
  hour without rotation the node's processes together rose to 891 MiB in the first 40 minutes and to 894 MiB by the end
  (0.4.0's, beside them, to 3,205 MiB) -- and an hourly rotation only rebuilt every isolate, its cold loads and its
  first, largest collection, and cut the requests longer than the drain. Set `rotation.maxAgeMs` to keep replacing
  processes by age.
- **An actor's CPU limit is topped up with each event** (`topUpActor()`, workerd's own contract): the requests to one
  Durable Object shared one CPU limit for the object's whole life. Coworkerd runs no Durable Objects before 0.5; this is
  one of their prerequisites.
- **The system call allowlists** are generated anew for the patched workerd: the same calls as 0.4.0's on both
  architectures (arm64: 59 at startup, 43 in the steady state; amd64: 63 and 42), only the binary they name is new.
- **One slow heartbeat no longer expires a node**: the membership's heartbeats go out on a fixed cadence, one every
  `membership.heartbeatIntervalMs` from the epoch's first -- at startup while the node still lists the members --,
  whether the one before has finished or not, each still bounded by the interval. In 0.4.0 a heartbeat waited an
  interval after the previous one had finished, so one that hung to its bound sent the next exactly at the TTL after the
  last one the store took: a single store write answered late (7 s, say) expired the node and gave it a new epoch,
  although no stateless call failed. Now one failed heartbeat leaves a full interval to spare; after two in a row the
  third goes out at the deadline, and a node waits for its answer before it expires -- a third failure expires it one
  interval past the TTL, still before the other nodes drop it (the cluster's state page, section 3). A node registering a
  new epoch after its own expired no longer takes a heartbeat of its own that lands late, while it waits for its id, for
  another process's, which drained the node and ended it with exit code 3. The TTL, the interval and the grace keep
  their defaults.

## 0.4.0

The object store becomes the cluster's only state, and etcd leaves Coworkerd: membership, the desired state (tenants,
credentials, deployments, secrets), the suspects and the usage are all in one bucket of an S3-compatible store, beside
the code that was there already. The control plane keeps nothing of its own, and a node only its Workers' recent log
lines and the usage counts it has not written yet (a minute's worth while the store takes them), so a stateless node and
a bucket make a cluster. **This release does
not upgrade a 0.3.x cluster in place**: nothing is migrated -- build the cluster anew and deploy again (the install
guide, section 9, "Upgrading from 0.3.x"); a configuration that names `etcdEndpoint`, `etcdCredentialsFile` or an
`etcd` backend is refused, saying why.

- **Membership on the object store**: every node registers an epoch it creates only if absent (and reads back), writes
  a heartbeat every 5 s and lists the members every 10 s; liveness is judged by the store's own clock (`LastModified`
  against the answer's `Date`), and a node that cannot write its heartbeat for the TTL considers its epoch expired. A
  node joined the others' tables within 6.6 s, and one that was killed dropped out of them 28 to 33 s later. A node
  refuses to start when its clock is more than 2 s from the store's, and a node whose identity another process takes
  drains and exits with 3.
- **Store outages fail no stateless call whose code and static assets its node holds**: a node whose epoch expired
  keeps serving the stateless calls addressed to any epoch it held, and a node that could not list the store for
  longer than the TTL keeps its members, whose heartbeats are still there, until the TTL and the grace have passed
  after it recovers. Through a 40 s outage of one node's store connection, 3,330 requests at the ingress succeeded.
- **The desired state on the object store**: one immutable manifest per tenant, the tenant index and the credentials
  index, each change the next revision created only if absent; the control plane holds a mirror of all of it and
  writes it -- as does the operations tool, with the same protocol -- and several control planes can share a bucket.
  Nodes read the index's hint every 5 s and apply what changed: a deployment reached three nodes in 3.8 to 4.7 s (in
  0.3, etcd pushed it within milliseconds), and ten concurrent deployments to one tenant through two control planes
  all committed in 305 ms, none lost. A tenant's whole state is one write: there is no limit of 128 operations any
  more. A change is answered as done only once it is visible -- its revision, the hint and the tenant index naming it;
  a failure after the revision is `503 STORAGE_UNAVAILABLE` -- and the control plane's poll and start complete what a
  writer left half done. The hostnames of different tenants are checked once more in the tenant index's own turn.
- **Secrets sealed at rest**: values are sealed with AES-256-GCM under the cluster's secrets key (`secrets.keyFile`),
  which compose's `secrets` step generates for the control plane and every node; the store never holds a value in the
  clear.
- **An object storage identity for every node**: each node's identity writes only its own `members/<nodeId>/` and
  `nodes/<nodeId>/` and cannot read the credentials index; the control plane and its tool keep the deploy identity.
  `docker compose run --rm secrets add-node <nodeId>` issues a node's identity and its secrets directory to a running
  cluster, and the node is then started with that directory (compose defines `node-1` to `node-3`; the install guide
  gives a file for a fourth): the store took the new identity 1.3 s later without a restart, and the fourth node was a
  member 5.6 s after `add-node`.
- **Suspects and usage on the object store**: a node's suspects are one object beside its heartbeat, read by the other
  nodes in their membership listing when it changed -- adopted within one listing cycle (10 s by default; 6.0 s
  measured), one GET per change and none in between -- and its usage one object per hour
  (`nodes/<nodeId>/usage/<hour>`), summed by the control plane.
- **SeaweedFS 4.48** in the compose files (pinned by digest): before 4.29 its create-if-absent does not hold under
  concurrency. Reads are presigned the standard way (only the host signed), which 4.48 requires -- the 0.3.1 binaries
  cannot read from it, so the pin and this release's binaries go together.
- **Checks**: `coworkerd check storage` reports the contract the cluster relies on (create-if-absent, listings with
  `Date` and `LastModified`, the clock), `conditionalPutAtomic` (create-if-absent under concurrency), whether the bucket
  holds the cluster's marker, and with `COWORKERD_READ_NODE_ID` a node identity's prefix policy;
  `coworkerd check --config` checks the management token, the desired state (the secrets key, the marker) and the
  membership (a listing, the clock, a write below the node's own prefix).
- **The cluster's marker**: `coworkerd-apiserver tool init` writes `cluster/format/1` and empty indexes into a new
  bucket (compose's `init` runs it); nodes and the control plane refuse to start on a bucket without it, and the tool's
  other commands refuse to write to one.
- **`/v1`**: `STORAGE_UNAVAILABLE` (503) answers a change while the store cannot be reached, and the usage endpoints;
  `ETCD_UNAVAILABLE` is not answered any more. The status answer gains `storage` (the revisions of the tenant index and
  the credentials index, and the store client's counters), `/v1/nodes` gains `backend` and `lastHeartbeatAgeMs` per
  node, and `etcdRevision` and the administrator view's `apiserver.etcdEndpoint` stay, always `null`. `cowctl status`
  shows the two revisions.
- **compose**: the services are `secrets`, `storage`, `init`, `node-1` to `node-3`, `apiserver` and `tool`; there is no
  etcd service, volume or credential.
- **The object store client**: separate connect (2 s), read (5 s) and create-if-absent (30 s) timeouts, a second try on
  a new connection for idempotent requests that failed, and a create whose answer was lost decided by reading the key
  back.
- **Deletions**: a tenant's deletion answers `503 DELETION_INCOMPLETE` until nothing of the tenant remains, and goes on
  when run again; deleting a version removes its objects first and leaves the version in place (503) when one cannot
  be removed. A deletion removes an object only once it is older than `control.cleanupGraceMs` (1 hour) by the store's
  clock and leaves younger ones to a sweep of the control plane, so an upload that meets a cleanup keeps its objects.
- **A management token is required**: a node refuses to start without `management.token` (or
  `COWORKERD_MANAGEMENT_TOKEN`) unless its configuration says `management.allowWithoutToken: true` (development); compose
  always sets one. 0.3 started without one and only warned.
- **`cowctl` waits for the nodes**: `cowctl deploy`, `rollback` and `secret put` return once every node that is not
  draining reports the change (`storage.appliedTenantsRevision`, new in `/v1/nodes` and the status answer), at most
  15 s; `--no-wait` returns at once. `cowctl` needs Node.js 22.5 or newer.
- **`secrets remove-node`** withdraws a node's object storage identity within about 2 s, without a restart of the store:
  `s3.json` keeps only the deploy identity, and every node's identity is in the store's filer.
- **Answers**: `SECRET_TOO_LARGE` is retired -- a secret value over 5 KiB is `422 SECRET_REFUSED` whatever its size --;
  a package of exactly `control.maxUploadBytes` or `packageBytes` is accepted; an upload without activation is not
  trial-loaded (its activation is) and lists no routes (`400 INVALID_REQUEST`); a store failure is 503 and a write other
  writers kept winning 409 by the failure's kind, never by its reason's words; the ingress's own refusals carry `error`
  like every call refusal; a call whose lease runs out during placement ends with `410 LEASE_EXPIRED`.
  `request.cf.serverId` stays a deprecated alias of `nodeId`, and is removed in 1.0.
- **Supported versions**: 0.4.x only; 0.3.x is no longer fixed (SECURITY.md).

**Still open**: there is no mTLS between nodes; the system call allowlist still defaults to `observe`; the stream of
`cowctl logs --follow` is filled by asking the nodes every 500 ms; some reasons the nodes and the control plane give
are still in Chinese; and an isolate already loaded keeps a secret's old value until it is replaced: an idle one after
5 minutes, one in use at the latest when its process is rotated, every hour by default.

## 0.3.1

Isolation and consistency fixes found while the design documents were checked against the code, in the tenant
boundaries and in the writes below a tenant. Upgrading is replacing the binaries and the image; nothing in etcd or
object storage changes. Two changes to be aware of before you upgrade: a path route under the default host `*` now
serves only the tenant that owns the request's hostname, and a path prefix matches whole path segments; and a node
refuses to start when its configuration names a CPU or memory limit of `0`.

- **A tenant's routes stay its own**: a request whose hostname belongs to a tenant is matched against that tenant's
  routes only -- its hostname routes first, then its paths under the default host -- so no other tenant's route can
  take a request to it. A path prefix matches at path-segment boundaries (`/acme` no longer takes `/acme-corp/...`),
  the control plane writes a tenant's `/<tenant>` under `*` as `/<tenant>/`, and a hostname is compared in lower case
  without a trailing dot.
- **A Worker's log lines are its own**: they reach the node over the tail channel workerd gives each loaded Worker
  under that Worker's own identity, so nothing tenant code can read or write decides whose lines they are. (Before, a
  per-process marker was part of every tenant isolate.)
- **With `egress.mode` `off`**, a tenant's `fetch()` is refused by the policy point (`EGRESS_DENIED: egress-off`)
  instead of leaving the isolate without an outbound, so a throw after `passThroughOnException` fails that request
  only, rather than ending the workerd process the tenants of that node share.
- **Writes below a tenant and its deletion**: every write compares the tenant's record as it read it, a pointer flip
  compares the version definition it names, and a tenant's deletion removes its routes and every key below its prefix
  in one conditional transaction -- a write that began before the deletion can no longer leave a route, a pointer or a
  Worker behind, and a node serves no tenant whose record is gone.
- **An etcd password change** reaches a running component: every authentication reads the credentials file again when
  it changed, and one that fails reads it again whatever its timestamp, so a watch that is the first to meet the new
  password recovers without a restart.
- **Egress policy**: `allowInternal` opens internal destinations only; a tenant's rules also apply to a URL that names
  an IP address (by the addresses a name resolves to), and an IPv6 form carrying an IPv4 address is judged by that
  address -- the local-use NAT64 prefix at every position RFC 6052 allows; the metadata class covers the other clouds'
  IPv6 addresses and the IPv6 documentation range is no longer public; the `[egress]` log line's `host` field holds a
  host name only; and every control plane announces its own addresses to every node, which refuse them like the rest
  of the cluster's own (`egress.clusterAddresses` remains for what nobody announces).
- **Packages**: a manifest's `packagePath` must be a relative path inside the package, and the asset quotas are read
  from the package file it names.
- **`cowctl`**: a project's local value files (`.env`, `.dev.vars` and their variants, at any depth) are never
  published as static assets.
- **workerd's environment**: a workerd process is given the variables it needs and nothing else of the node's own
  environment -- the management token and object storage keys stay with the node.
- **A limit of `0`** is refused in a node's configuration, in a tenant's default limits and in a Worker's own (it
  turned culprit attribution, and with it the quarantine processes, off). `COWORKERD_CPU_LIMIT_MS` and
  `COWORKERD_MEMORY_LIMIT_MIB` still turn a limit off deliberately, and the node says so at startup.
- **compose**: the object store answers on its S3 interface only; its master, volume and filer servers listen on the
  container's loopback, and the S3 gateway's own gRPC interface requires a client certificate nobody holds.
- **Membership**: a member value a node cannot read is skipped and logged, and the other members -- and a node's
  startup -- are unaffected.

## 0.3.0

The first release, for Linux `amd64` and `arm64`, each as a package and a container image. One cluster serves one
platform, which runs its tenants' Workers on it: create tenants, issue credentials, deploy, call, read logs and usage;
nodes and backends authenticate each other; every workerd runs in a least-privilege environment with a system call
allowlist.

**Components** (`bin/`): `coworkerd` (the node daemon; `coworkerd check` is its self-check), `coworkerd-apiserver` (the
control plane, the only writer of desired state), `coworkerd-launcher` (starts each workerd in a least-privilege
environment), `cowctl` (the command line; needs Node.js 22 or newer for the bundled wrangler), `workerd` (upstream `v1.20261012.1` with this
project's patch series, see [coworkerd/workerd](https://github.com/coworkerd/workerd)), and the system call allowlists
of this workerd build (startup and steady-state tables).

**Capabilities**

- **Isolation and placement**: a pool of workerd processes per node with a watchdog, per-isolate CPU and memory
  limits, culprit identification and quarantine processes; any node is an ingress, calls (including `env.B` Service
  Bindings) are placed on a Worker's home node by rendezvous hashing and rerouted only when they certainly did not
  execute; one call tree always sees one deployment state.
- **Tenant → Worker model** and the **`/v1` API**: the administrator's credential creates tenants, sets quotas and
  hostnames and issues tenant credentials; tenants deploy, roll back, set secrets and routes, read logs and usage with
  their own credential. `/v1` changes only by adding fields and endpoints, until 1.0.
- **`cowctl`**: developer commands (`init`, `deploy`, `versions`, `rollback`, `remove`, `secret`, `status`, `logs`,
  `usage`) and administrator commands (`tenant`, `credential`, `node`).
- **Per-tenant usage**: hourly requests, CPU milliseconds, egress bytes and log lines; a draining node flushes its
  last minute.
- **Egress policy**: tenant `fetch()` leaves through a policy point on the node, which refuses loopback, private,
  cloud-metadata and cluster addresses by default, checks the resolved address and pins the resolution; per-tenant
  allow and deny lists, concurrency and byte limits, byte metering.
- **Authentication**: peer calls between nodes are signed with a cluster key (two-key rotation); etcd requires client
  authentication, with separate users and roles for the apiserver and each node (nodes cannot read credentials or
  write desired state); nodes hold read-only object-storage credentials; the management endpoint takes a token.
- **Sandbox**: each workerd runs under the launcher in its own mount, network, IPC, UTS and user namespaces as an
  unprivileged user, and exits with its parent; the launcher installs the system call allowlist generated by the build
  (default `observe`: calls outside the table are reported and allowed, visible in the state endpoint and the logs);
  after readiness workerd installs the narrower steady-state table itself (default `off`) and makes itself
  non-dumpable. Prerequisites are in the install guide, section 4.
- **Per-Worker CPU and memory limits** taken from the Worker's configuration, bounded by the tenant's quota;
  `request.cf.nodeId` (`request.cf.serverId` carries the same value through 0.3.x).
- **Node side**: trial loads run in the quarantine processes (two by default, sharded by tenant); workerd processes are
  rotated hourly by default.

**Getting started**: unpack and read `docs/quickstart.md` (about ten minutes); installation, configuration and
operations are in `docs/install.md`. Requires Docker Engine 28 or newer and Node.js 22 or newer; the sandbox requires
Linux with unprivileged user namespaces (Ubuntu 23.10 and later: see the install guide, section 4).

**Known limitations** (planned for 0.4): nodes trust each other through a shared signing key -- no mTLS and no
node-join flow yet; Worker secrets are stored unencrypted in etcd (access is authenticated and role-restricted); the
control plane is a single process; the allowlist defaults to `observe`, and switching to `enforce` should follow a
period of observation under real traffic; `cowctl logs --follow` polls; some error reasons the node returns are in
Chinese, with English reasons and codes planned.

# Changelog

## Unreleased

- **Diagnostic and profiling renames.** Update environment settings to use the new names. The old names are removed.

  | Old environment variable | New environment variable |
  | --- | --- |
  | `COWORKERD_R47_TRACE` | `COWORKERD_EVENT_LOOP_TRACE` |
  | `V45_BIN` | `COWORKERD_PROFILE_BIN_DIR` |
  | `V45_STACK` | `COWORKERD_PROFILE_STACK_DIR` |
  | `V45_SERVER_LOG` | `COWORKERD_PROFILE_SERVER_LOG` |
  | `V45_MACHINE` | `COWORKERD_PROFILE_MACHINE` |

  The trace header moved from `node/r47-trace.h` to `node/event-loop-trace.h` and its namespace is now
  `coworkerd::eventLoopTrace`. New syscall profile records omit the unused `round` field. When no program
  is specified, the trace parser matches `/opt/coworkerd/bin/profile-target/workerd` in the trace.
  The signed storage check writes its updated probe payload under a new `bundles/<sha256>` key.

- **Behavior change.** Inbound WebSocket upgrades share one ceiling per ingress node, sized within the daemon's hard descriptor limit. A full node returns `503 WEBSOCKET_NODE_CAPACITY` before dispatch. Remove the old per-tenant setting before upgrading. The [installation guide](docs/guide/install.md#inbound-websocket-capacity) describes sizing and configuration.
- **Behavior change:** long built-in operations of a Worker -- `JSON.parse` of a very large text, `split`, `replaceAll`, a global `replace` with a function, a global `match`, `sort` and `toSorted` of an array-like object or a string, structured clones of large strings -- now take the termination of the call's CPU or memory limit while they run, or throw a `RangeError` past V8's largest array, instead of ending the process (V8 patch `0056`). A Durable Object call whose secret values the node's dictionary cannot take is answered `503 SECRET_UNAVAILABLE` wherever this is found (before, `DO_UNAVAILABLE` when the call's descriptor found it). Platform log cleaning matches all secret values in one pass, and a multi-line secret value split across two reads with another process's output between them is redacted. A Worker log line longer than 4,096 bytes is marked `truncated` and counted again on ordinary processes. An ordinary process at its resident limit evicts its least recently used idle Worker for a new one instead of refusing about four loads in ten while it wrote its lifecycle report. The node's main thread makes about 83 system calls and 48 clock reads per request instead of 692 and 282 (it no longer polls between events, takes termination signals through its event loop, and keeps `memory.events` open); its CPU per request on the threaded path is still about 1.1 ms, several times what the path before the threaded server took, which a later version addresses.
- A V8 sandbox per isolate is available as an optional build configuration of the workerd kernel (`--config=coworkerd-sandbox`, kernel patch `0043`, V8 patches `0055` and `0057`). **It is off by default**: the binaries of a release are built without it and nothing below applies to them. In a kernel built with it, every Worker's isolate in an ordinary process has an 8 GiB sandbox of its own (4 GiB for its objects, 4 GiB for its buffers, of which 2 GiB are a buffer pool), fenced by V8's guard regions, instead of sharing one 4 GiB pointer cage with the other isolates of its process: four Workers of one process that each allocate close to 1 GiB at the same moment no longer end the process (in the default build they still do). One isolate then reserves about 136 GiB of address space, so a process holds fewer Workers. The node measures at start how many sandboxes a process of this build can reserve on this machine (`coworkerd-threaded --sandbox-facts`), lowers the per-process resident limit to 90% of that, keeps the configured number of processes, and says so in a `[server] {"event":"v8-sandbox",...}` line, a warning and `processes.configuration.v8Sandbox` in its state (`residentLimit`, `residentCapacity`, `lowered`); a probe that could reserve no sandbox stops the node from starting. A process that cannot reserve one more sandbox refuses that load as retryable (`503 OVERLOADED`) and lowers its own resident limit to what it holds; a process whose low 4 GiB of address space are taken exits with status 78 and is not restarted. New setting `sandbox.protectionKeys` (`auto`, `off`, `required`; `COWORKERD_SANDBOX_PROTECTION_KEYS`): this version only detects memory protection keys and reports them, `auto` and `off` run with guard regions, and `required` refuses to start. On the amd64 test machine, compared with the same kernel built without the sandbox: about 650 minimal Workers per process instead of about 1,400 (in the default build the shared cage's address budget fills before the nominal limit of 1,840), 0.74 MiB more resident memory per isolate (2.84 instead of 2.08 MiB), cold loads about 11% slower (19.7 instead of 17.7 ms), a byte-heavy request about 6% slower, and plain requests not measurably different. In both builds, a Worker load that an ordinary process refuses for lack of process capacity is now always retryable and is no longer remembered as that Worker's failure. See "The V8 sandbox and memory protection keys" in the [installation guide](docs/guide/install.md).
- The object store client keeps its connections open and reuses them (HTTP/1.1 keep-alive) instead of opening a new connection with `Connection: close` for every request. A kept connection is only the raw stream, and each request reads it through an HTTP client of its own: a connection is reused only when nothing arrived on it while it was idle, for at most 2 s for reads and deletes and 1 s for writes (well below the idle timeouts of MinIO and SeaweedFS), and it is closed after any error, timeout or cancellation, after an answer with `Connection: close`, and after a non-2xx answer to a request with a body. Which requests are sent again is unchanged: a read or a delete once more, on a new connection, and a write never. The membership heartbeat has a connection lane of its own, so it neither queues behind other requests nor picks up a connection they left; at the default heartbeat interval of 5 s each heartbeat still opens a new connection. Before, a node that recovered many Durable Objects at full speed opened one connection per request until the machine ran out of ephemeral ports in `TIME_WAIT`; new connections to the store then failed or were reset and membership leases expired. On the amd64 test machine, after the owner of 2,500 objects was killed, all objects were readable again after 57.8 s instead of 107.7 s, no lease expired (2 to 3 times per node before), the `TIME_WAIT` peak towards the store fell from 28,223 to 154, and the node that took the objects over opened 31 connections for 22,666 requests; with the default deadlines no lease expired either. The node state reports the kept connections (`connections` and `heartbeatConnections` under `durableLog.storeClient` and `membership.storeClient`: in use, waiting, created, reused, and closed by reason; `durableLog.storeProcess` has the totals of every client of the process) and the heartbeat PUT latency (`membership.heartbeatPut`); see "Kept-alive connections" in [cluster state](docs/cluster-state.md).
- **Behavior change:** ordinary Worker processes now run on coworkerd's own multi-threaded server instead of upstream workerd's server and the JavaScript dispatcher (patch `0042`). Each call runs on a thread of its own and each Worker program has one isolate per process, so a call that computes or waits no longer holds up the other Workers of its process. Durable Object processes are unchanged. For Workers this means: work passed to `ctx.waitUntil()` gets at most 30 s once the response has been sent (a streamed body to its end) or the client went away, and a 35 s backstop cancels whatever is left; a Worker that goes over its 128 MiB memory limit is answered `503 MEMORY_LIMIT_EXCEEDED` and its isolate is replaced while the process and its other Workers continue, also when one allocation of up to the largest JavaScript object takes it past the limit; a platform failure before a call runs (for example, the process could not get the call's plan) is answered as not executed and retryable (`PROCESS_UNAVAILABLE`), and placement tries another node, instead of being counted as the tenant's load failure. Secret values are redacted from node logs through a dictionary per node: a value enters it before any process can print it and leaves 60 s after nothing can print it any more; one tenant may use at most a quarter of it and the node at most 16 MiB (65,536 values), and a call whose secrets do not fit is answered `503 SECRET_UNAVAILABLE` (not executed) and goes to another node when it can. Log lines that wait more than 50 s for a stuck log destination are dropped and counted instead of written late. A Durable Object process in which a tenant's replaced secret values pass 512 KiB is rotated, at most once per slot every 10 minutes; a tenant that causes this twice within an hour has its Durable Object calls on that node refused instead. Two limits remain until 0.6.0: four Workers of one process that each allocate close to 1 GiB at the same moment still end that process (the isolates of a process share one 4 GiB pointer cage until each gets a sandbox of its own), and a long built-in that does not check for termination can take an isolate past its raised limit and end the process (the ones found so far no longer do; see the entry on long built-in operations at the top of this list).
- A node that restarts with the disk of the node it was before now reseals its earlier epoch and publishes it without holding up its event loop: it picks the records in small slices on the loop and encodes, hashes, uploads and syncs the segments on worker threads (the segments are byte for byte what it wrote before), and it writes the manifests of 16 objects of a segment at a time instead of one after another, still once per object and segment and in order. A request for one object's log tail is now answered for that object as soon as none of that object's records is left unpublished, instead of waiting until the whole shard is published; requests that name no object, and a proof for the whole shard, are answered and fenced as before. On the amd64 test machine, after the owner of 2,500 objects was killed and restarted with its disk, the first 100 objects were readable again after 30.5 s instead of 123.9 s and all of them after 107.3 s instead of 167.0 s, and the restarted node's republication took about 11 s instead of 95 s with no pause of its event loop of 20 ms or more. The node state reports the republication under `durableLog` (`restoredObjectAnswers`, `earlierSegmentBuilds`, `earlierManifests`, `earlierPublishing`).
- When a peer refuses an owner's append because the shard has a newer owner, the owner now reads that newer epoch from the object store and, when it is there, drops the shard at once: the writes it was waiting to confirm fail with `503 STORAGE_UNAVAILABLE` and later requests for the shard's objects go to the new owner. Before, it kept the shard and each such write waited `durableObjects.confirmTimeoutMs` (5 s) before failing; on the amd64 test machine a write in flight when a paused owner resumed failed after 0.02 s instead of 4.4 s. Other refusals (for example a full peer) do not drop a shard. A peer also takes again, without refusing it, a record it already holds byte for byte when an owner sends it a second time after reconnecting, so an owner that dropped one shard is not held back on its other shards; and an owner no longer sends again the records of a shard it has dropped.
- **Fix (behavior change):** an owner could confirm a Durable Object write with one copy fewer than configured. A peer answers each append with the highest sequence it has taken on that stream, and the owner read that as "taken everything up to here": when a peer refused a record (for example because its buffer was full) and then took a later record on the same stream, the owner counted the refused record as held by the peer and confirmed it, so the write existed only on the owner until it was sealed. This was also in 0.5.6. An owner now counts only the records a peer actually took (a record sent in several parts counts once its last part is taken); a record refused because the peer was full is sent again after a short, randomized back-off, up to five times, and a write that no peer has taken by `durableObjects.confirmTimeoutMs` fails with `503 STORAGE_UNAVAILABLE` instead of being confirmed. The protection covers a cluster once every node runs this version.
- A peer now remembers the newest shard epoch that a new owner asked it about, after checking that this epoch exists in the object store, and refuses later appends from older owners of that shard; it still keeps and answers the records it already holds. An owner whose clock stood still while it was paused (for example a frozen virtual machine), and that therefore still believed its lease held, can no longer get such a write confirmed by that peer. The check costs one object-store read per shard and new epoch, shared by concurrent requests; when the store cannot be read the peer answers as before and tries again in the background. The floor is kept in memory only: a restarted peer, a peer the new owner did not ask, or an older peer does not enforce it.
- Rebuilding Durable Objects after a shard changes owner downloads and decodes each segment of the object store once per node, on a worker thread, and shares the decoded records between the objects that need them. Before, every object's rebuild decoded each of its segments in full on the node's event loop: when a restarted owner republished its earlier epoch as a few segments holding all 2,500 objects of a shard, each rebuild held the loop for about 3.3 s and leases expired. On the amd64 test machine the 2,500 objects of a killed node were all readable again 164 s after the kill (before, 1,081 after 2,445 s), the node that took them over paused its event loop for at most 32 ms, and no lease expired in that test. The node keeps at most 384 MiB of decoded records between rebuilds (segments being loaded are not counted) and drops a segment no rebuild has read for 30 s; at most 16 segments load at once, and decoded segments are freed on a background thread. Which records a rebuild reads, their order, the exclusion evidence and the errors are unchanged. A rebuilt object's log line now also gives where the rebuild's time went (slot wait, manifest, snapshot, segments, local history, tail wait, replay, publication) and what its segments cost; the node state reports the cache under `durableLog.objects.segmentCache`. A restarted owner still pauses its own event loop for up to about 0.9 s while it republishes its earlier epoch; a later version moves that work off the loop.
- **Behavior change:** a node confirms a Durable Object write only while its own membership lease still holds. It reads the lease clock (`CLOCK_BOOTTIME` on Linux) after the local flush and right before each confirmation; a node that was paused, or whose flush outlasted its lease, now answers the pending writes with `503 STORAGE_UNAVAILABLE` and drops its shards, instead of confirming a write that the shard's next owner may not have. Before cutting a lost owner's tail, the new owner also re-reads that owner's heartbeat and does not cut while the heartbeat still looks live, cannot be read, or names another node or epoch. A heartbeat due exactly at the lease deadline still keeps the lease, so a node survives two failed heartbeats in a row as before. **Nodes now refuse to start** when `membership.livenessGraceMs` is not greater than `membership.heartbeatIntervalMs + 1150` (for example a grace equal to the heartbeat interval); the message names the smallest valid grace, and the defaults (heartbeat 5,000 ms, grace 10,000 ms) are fine. The control plane does not check this. Before upgrading, raise the grace on every node and the control plane to the same value and restart them; the protection covers a cluster once every node runs this version.
- Rebuilding a Durable Object after its shard changes owner fetches only that object's records from the other nodes: the node names the object in each log-tail request, and a node of this version answers with that object's positions after the cursor instead of pages of the whole shard. On the amd64 test machine the first node to take over the 2,500 objects of a killed node read 1,536 records from the old holder's epoch instead of 2,040,000 (all but 816 of them for other objects). Nodes of both versions work together: a request from an older node is answered with shard pages as before, and the requester still keeps only the object's records when an older node answers. When two nodes hold different contents for one position, the tail is now refused only if the position belongs to the requested object or another object's record claims one of its positions; a conflict elsewhere in the shard is found when that other object is requested. A node's first start also yields to its event loop between small batches while it reads its WAL, cleans its object cache and groups the records of earlier epochs: in that test the restarted holder's single 5.4 s stall became pauses of at most 43 ms. Recovering the objects of a lost node is not faster yet: each rebuild in that test still held the node's event loop for about 3.3 s, and a later version works on that.
- When a shard changes owner, retiring each object's old confirmed tail no longer decodes every record of the node's log: the node finds that object's records through per-object indexes, including the records of its earlier epochs read at startup. With 2,500 objects and 60,000 unsealed records on the amd64 test machine, one retirement held the node's event loop for at most 1.2 ms (22.5 ms before). Which records are retired, and what the node counts and logs, is unchanged.
- Per-tenant ingress limits can borrow spare node capacity: with `placement.tenantRootLimit` or `placement.tenantNestedLimit` set, `placement.tenantBorrowBelowPercent` lets a tenant go beyond its share while the node's calls in flight (roots and nested together) are below that percentage of the node-wide limit; at or above it, new calls beyond a tenant's share get the existing retryable `503 OVERLOADED` tenant refusal, and calls already admitted are not interrupted. `0` (the default) keeps the limits hard, and the limits themselves stay `0` (off) by default. On the amd64 test machine with a node-wide limit of 128, 16/16 with 88% let a single tenant run 112 overlapping root calls (16 with the hard limits), kept another tenant's requests from being refused while one tenant ran 5-second SQL statements (119 refusals in 1,200 without limits), and left two normal tenants' p99 within 0.3 ms. `placement.tenantAdmission` in the node state shows `borrowed` and `busyRejections` per tenant and call type.
- Native ordinary processes retain per-request CPU limits (30 seconds by default, configurable per Worker up to 5 minutes), memory admission and total process safety limits. The experimental per-tenant thread share is disabled by default; an omitted setting creates no tenant execution counter or wait queue. CPU-share fairness remains deferred for capacity qualification. Admission no longer scans the process heat table. The separate optional tenant ingress limits remain `0` by default.
- In 0.6.0, node diagnostic logs redact known Worker secret values as `[redacted]`, including top-level and request exceptions printed by workerd itself, the native ordinary server and the Rust durable server, and the node's own output. Both raw and JSON-escaped values are covered, including late output from an older generation after a secret changes. This fixes a regression after 0.5.6 in unreleased builds. The scope here is node diagnostics; tenant log API visibility is being checked separately.
- On amd64, the steady system-call allowlist always includes CPU-affinity and time calls that profiling may miss when vDSO handles them. Before 0.6.0, a host whose clock uses system calls (such as an hpet clock source) could lose a workerd process in `enforce` mode. The default `observe` mode allowed those calls.
- The native ordinary backend reports the actual requested method in legacy Worker RPC failures (for example, `echo`). Its direct load and admission path replaces the ordinary JavaScript preparation channel; the former preparation-only HTTP 409 `LOAD_FAILED` and HTTP 503 `CODE_UNAVAILABLE` responses no longer have producers.
- The node no longer stalls when many Durable Objects are active. Rebuilding one object used to decode every record of the node's unsealed log and keep the ones of that object (about 210 ms with 5,000 active objects on the amd64 test machine); the node now keeps the unsealed records of each object apart and decodes only those. Messages from a durable process are now handled in slices of at most 16 messages or about 0.5 ms, so a burst of hundreds of close messages (a durable process expires the containers of idle objects every 70 s) no longer holds the node's event loop for hundreds of milliseconds. With 5,000 active objects (2,500 on each of two nodes) the hot-request p99 was 0.3 ms below an empty node and the longest pause of the node's event loop was 7.6 ms (20.2 ms before); when that expiry fell inside the measurement, the longest pause was 16.7 ms (144 ms before). What a rebuild reads, and in which order, is unchanged.
- Add per-tenant ingress limits on a node: `placement.tenantRootLimit` and `placement.tenantNestedLimit` cap the root and nested calls one tenant may have in flight on a node, inside the node-wide `rootLimit` and `nestedLimit`. Both are `0` (off) by default. A call over its tenant's limit is answered `503 OVERLOADED` with the reason `tenant ingress concurrency limit reached` and is retryable; other tenants on the node are still admitted, and other nodes do not start avoiding this node because of it. Without these limits one tenant's slow requests could fill the node-wide limit: in a test where one tenant's Durable Object ran a 5-second SQL statement, another tenant's stateless requests on the same node were refused 121 times in 1,200; with both limits at 16, 0 times. `placement.tenantAdmission` in the node state lists the limits and, for tenants with calls in flight or refusals, their counts.
- The node's event loop no longer does work that grows with the number of Durable Objects. Each second it used to read the size of every object's database and WAL file (56–75 ms with 5,000 objects with pending alarms on the amd64 test machine), and reading the node state built the whole reply at once (34–52 ms of CPU for a reply of about 0.9 MB); while either ran, every request through the node waited. File sizes are now kept from write, checkpoint and deletion events and checked against the disk in small batches, idle checkpoints are found through an index of due times, sealing the log copies records in slices and encodes and hashes the segment on a background thread, and the state reply is built in segments. With 5,000 objects with far-future alarms the hot-request p99 rose by 2 ms over an empty node (86 ms before), and the longest pause of the node's event loop fell from 212 ms to 17–23 ms. Which cold files are deleted, and in which order, is unchanged.
- Node state: `durableLog.disk.usedBytes` is now an estimate kept from events and corrected against the disk about every 60 s (and when the disk target is reached); before it was measured every second. The sections of one state reply are no longer read at the same instant, and `memory.daemon` now includes the node's background threads. No field was added, removed or moved.
- A node that stops now waits for a seal already in progress and then seals the remaining confirmed records until it is done or its drain deadline passes; before, a seal in progress at that moment made the drain skip sealing.
- **Behavior change:** stateless Workers are loaded through the kernel's own registry, the way Durable Objects and databases already are. The trial load at deploy, HTTP requests and JS RPC all take this path, and nothing calls the Worker Loader any more (its code is removed in a later version). The kernel now carries 37 patches (`0027` to `0035` and `0038` to `0041` are new). Answers, limits, logs, egress rules and usage figures were compared field by field with the previous path (34 cases) and are the same except for the changes listed below. On the amd64 test machine a hot request costs 3.7% more over HTTP and 2.5% more over JS RPC than before (medians of three paired runs of 1,000 requests each), and with 500 resident Workers a process uses about 2.34 MiB per Worker (2.27 MiB before).
- **Behavior change:** after a declared secret, variable or binding of a Worker changes, the next new request to its stateless Worker runs with the new environment, even when the version is the same. Work already running -- request bodies, response streams, RPC capabilities, `waitUntil` and WebSockets -- keeps the environment it started with until it finishes. Changing a secret the version does not declare, or an unrelated database, reloads nothing. Before, a loaded Worker kept the old values until it was evicted. See the [quickstart](docs/guide/quickstart.md).
- A new version of a bound Worker no longer compiles a second instance of the calling Worker: the caller has one instance per version and environment, and call trees pinned to the old and to the new target version share it.
- The first load of a Worker is queued and bounded. `COWORKERD_STATIC_HEAP_MIB` (128 by default) now also bounds preparation: large packages share 21/32 of it (84 MiB, at most 12 at a time) and four small packages (about 126 KiB of v2 code each) have a lane of their own; a request waits up to 25 s for its preparation. A package that can never fit the large lane is refused with non-retryable `409 LOAD_FAILED` (category `limit`), at deploy and at requests: with the default heap that is more than the maximum 10 MiB v2 package with a 1 MiB descriptor, or v1 source whose JSON-escaped form is several times its size. Concurrent first requests to one Worker fetch and install its code once: 512 of them all succeeded, and the longest pause of the process was 0.79 s (1.21 s before).
- A request whose Worker could not be prepared before anything ran is answered `503 CODE_UNAVAILABLE` with `retryable: true` and is confirmed as not executed: it is not counted as a call and no CPU is charged for it. The call is not sent to another node by itself.
- A tenant's count of resident instances now includes its Durable Object instances (the platform's database instances are not counted); the durable processes' figures in the node state can be up to 1 s old.
- When a durable process is rotated or the node exits, objects that have already answered keep their background work (`waitUntil`) until it ends or the drain deadline passes; before, the wait ended with the last request. Twenty rotations right after an answer, each with 5 s of background work, took 5.1 to 5.2 s and lost none.
- **Behavior change:** background alarms fire within a budget. A node dispatches at most `durableObjects.alarmFireConcurrency` background alarms at a time (2 by default); before, it dispatched due alarms without a limit of its own. Tenants take turns and the alarms of one tenant fire in the order they were due, so a tenant with thousands of due alarms no longer holds back a tenant with a few. A dispatch that is refused for lack of capacity (a paused or unavailable durable process, a full node) is not counted as a failed attempt and does not enter the handler's retry backoff: one alarm probes that destination every 200 ms and firing resumes when it has room. The default is deliberately low because background alarms and user requests share the durable process's event loop; see the guide before raising it.
- Alarms of objects that have to be rebuilt first no longer hold a firing slot while they wait: at most 256 such alarms are preparing or waiting for a rebuild, and the rest keep their place in the schedule, in turn between tenants. The rebuild slots (`durableObjects.rebuildConcurrency`) are shared between user requests and background alarms: with three or more slots, alarms get two of them when both sides are waiting and each side uses what the other leaves idle; with one or two slots the two take turns. A user request for an object that is queued for an alarm's rebuild moves it to the user queue.
- A node takes over more than 16,384 marked shards of a stopped member: when its candidate table is full the scan of the markers pauses and later resumes where it stopped. Two surviving nodes took over 20,000 marked shards in 179 s.
- A node removes its own markers for shards it no longer holds (left behind when a shard moved to another live node), at most 16 checks a minute.
- Alarms of a Durable Object class that is disabled, or whose namespace a node does not know yet, are kept and resume when the class is enabled (3.2 s in the test); only a deleted namespace drops them. Before, they were dropped from the node's memory until the object was next opened.
- The Durable Objects guide gives the measured time until alarms fire after a node is lost, and what makes it longer. With the defaults each object of the lost node waits 25 s for its first rebuild and holds one of the four rebuild slots per node while it waits, so two surviving nodes recover about 0.32 objects per second: 24 objects had a median of 75 s from the loss to the alarm, and 1,000 objects are estimated at about 53 minutes. This is a limit of the current recovery design, not of this change.
- **Node storage permissions change:** a node's object storage account now also needs `List` and `Write` on `alarm-wake/by-owner/` (see the table and the example in the install guide). Compose adds both by itself when the stack is restarted with this version. Where you manage the store's policies yourself, add them for every node account; `coworkerd check storage` with `COWORKERD_READ_NODE_ID` reports three new items for this prefix. A node without them keeps working as before: it only cannot write the markers described next, says so in its state (`markerAccessDenied`), tries again every 30 seconds and picks up the permission within a minute of it being granted.
- Alarm hints are published reliably. The small object a node writes for each Durable Object with a pending alarm, so that the shard's next owner knows about it, is now retried until it is written or the alarm has changed again; before, a failed write was only counted. The node also keeps one empty marker per shard that has pending alarms, under `alarm-wake/by-owner/<node>/`, so that a later version can find a stopped node's alarms without a request; nothing takes shards over in the background yet. Acknowledging `setAlarm()` does not wait for any of this. When the store refuses requests across the board the node backs off to one probe every 5 seconds, separately for hints and markers, and resumes at once when a probe succeeds. The node state reports the queue (`publicationPending`, `hintPublicationBackoff`, `markerPublicationBackoff`, `invalidAlarmHints`, `markerWrites`, `markerDeletes`). A hint whose content cannot be read is skipped and counted instead of stopping the shard's other alarms from being taken over.
- **Behavior change:** alarms of a stopped node's Durable Objects fire without waiting for a request. Every surviving node with durable processes looks for the markers of members that are gone -- at each membership change and every `durableObjects.alarmDiscoveryIntervalMs` (60 s) -- and the shard's next owner in the usual order takes each marked shard through the ordinary ownership path, at most `durableObjects.alarmTakeoverConcurrency` (4) at a time on a node. A live owner is never taken from, a shard without pending alarms is not touched, and an object is opened only when its alarm is due. Measured with two surviving nodes: 10,000 marked shards had their new owners in 102 s, each new owner's own marker was written within 0.1 s of its takeover (p99), and no request failed meanwhile. It needs the node storage permissions on `alarm-wake/by-owner/` above; without them the state endpoint reports alarm discovery as `disabled-access-denied` and alarms fire after the shard's next request, as in 0.5.x. A shard without pending alarms still gets its new owner when a request reaches it.
- Preparation for loading stateless Workers through the kernel's own registry, the way Durable Objects and databases already are (patches `0023` to `0026`): every workerd process now carries the private registry, and the stateless processes accept only code without Durable Object namespaces. Nothing is switched yet: stateless Workers are still loaded as before, and requests, limits and usage figures are unchanged. The node state gains `lifecycleLabels`, which counts how many resident instances report their tenant, Worker, version and load generation and how many of those agree with the instance's name. The system call tables were sampled again on the new kernel and are unchanged apart from the kernel hash they name.
- Deleting a tenant or many Durable Object classes and databases at once now cleans the durable processes in batches of 64 namespaces, at most four batches per process at a time, and a process that is slow to finish no longer holds back the others. On the amd64 test machine a tenant with 5,000 namespaces was cleaned out of two processes 8.9 s after the deletion, and the node's event loop was never held longer than 13 ms. Each process reports `rootMaintenanceCompleted` and `rootMaintenancePending` in the node state. The kernel finds the objects of a retired version through an index instead of scanning every object (the new patch `0022-root-registry-scale`).
- A tenant index that went backwards (a bucket restored from an older copy) is still not applied, and the node now reports it: `watchConnected` is `false` and `watchLastError` names the revision read and the revision applied until the index is ahead again. Before, only a log line said so and the state looked healthy.
- The reason an RPC caller receives when a Durable Object cannot be prepared is cleaned the same way as the HTTP one: values of declared secrets are replaced and file paths removed.
- **Behavior change:** SQL has an execution limit. One event of a database or of a Durable Object (a request, an alarm, a WebSocket message) may spend 200 ms of CPU inside SQLite in total (`processPool.sqlCpuLimitMs`), with 800 ms of time inside SQL as a fallback (`processPool.sqlWallLimitMs`); both are node settings between 1 and 60000. The statement that goes over fails with `SQL_LIMIT_EXCEEDED` and later SQL in the same event is refused; the next event starts with a new budget. For a database the request answers `503 SQL_LIMIT_EXCEEDED` (through `env.DB`: `D1_ERROR: SQL_LIMIT_EXCEEDED: …`), and a batch, an import piece or a migration file fails as a whole. In a Durable Object's own `ctx.storage.sql`, an interrupted write rolls its transaction back and resets the object, as SQLite and workerd already did for a failed write; an interrupted read fails only that statement, and what a `transactionSync` callback does with the error decides whether the earlier statements commit. Before, a statement that ran for about three seconds made the watchdog kill the whole durable process, and the other tenants' objects in it failed with it. The CPU of many short statements is estimated from a random sample (one native step in 16); a long statement is measured. A single operation that never reaches a SQLite progress callback (one very expensive function call) is not covered, and a tenant's smaller CPU limit still does not stop a statement while it runs; the watchdog remains the last resort. **What this refuses:** a statement that needs more than the budget (an index built on a large table, hundreds of thousands of rows inserted or changed by one statement, a long scan) no longer finishes; statements between about 0.2 and 3 seconds used to complete while the other objects of the process waited. Split such work into smaller statements in separate requests, or raise the two settings on nodes where neighbours may wait, keeping them below `processPool.stuckTimeoutMs`. New kernel patch `0021-sql-execution-limit`.
- Add database time travel. `cowctl d1 time-travel info <database> [--timestamp T]` shows the restore point for a time, and `cowctl d1 time-travel restore <database> (--timestamp T | --bookmark B) --to <new>` restores the database as of that point into a **new** database of the same tenant; the source keeps serving and is not changed. Restore points are the revisions a database has published, so by default about the last 30 seconds of writes are not restorable yet; when clocks of two owners disagree, the order of revisions decides. History does not expire for now. The control plane gains `GET /v1/tenants/{t}/databases/{d}/time-travel/info`, `POST …/time-travel/restore` and `GET /v1/tenants/{t}/database-restores/{operationId}`. A restore carries an operation id chosen by the client and can be submitted again after an interruption or a restart; the result is one database. The new database's internal id is computed by the control plane from the tenant, the source, the restore point and the operation id, never taken from the request. Limits: a node runs one restore at a time; the retained history of the database is at most 128 MiB and 100,000 records per restore (`SEED_RESOURCE_LIMIT` otherwise); a tenant may have 16 restore requests pending (`429 RESTORE_BUSY`); finding the restore point for a time reads at most 128 history records per request, and when that is not enough the answer is `422 HISTORY_READ_LIMIT` and the same request sent again continues where the last one stopped. See the databases guide, section 11.
- Rebuilding a Durable Object or a database from its snapshot no longer holds the node's event loop while the downloaded image is hashed; the hash is computed in slices. Before, the node answered nothing for a time in proportion to the image (about 0.4 s for 270 MB), on every cold rebuild since 0.5.0. The synchronization of the rebuilt files to disk, after the download and after the replay, runs on a helper thread, and the replay itself gives way every 2 ms; rebuilds, restores and requests take turns in order of arrival. Measured during a restore: the node's longest pause went from about 300 ms to 31 ms for a 270 MB database and 64 ms for a 541 MB one.
- Fix: creating a tenant again under the name of a tenant whose deletion had stopped halfway (`DELETION_INCOMPLETE`) no longer brings back the deleted tenant's database records.
- After a Durable Object class, a database or a tenant is deleted, every node ends that namespace's resident objects and closes its hibernated WebSockets within seconds, and unloads its code and environment from the durable processes. Before, they stayed until the process restarted. A namespace that is merely missing from the catalog (an unreadable record, an empty or stale index read) is left alone.
- Tenant code that exceeds its CPU or memory limit while it starts answers `500 EXECUTION_FAILED` like other code that cannot be loaded; the failure is remembered, for a first load and for a reload after the code was unloaded, and a request in that time is answered without preparing the modules or compiling again. The first failure is remembered for 1 second and each further failure of the same code twice as long, up to 30 seconds; one success, or 60 seconds without another failure, starts over at 1 second. A process keeps up to 256 such failures; when that is full the least recently used one leaves, instead of all of them.
- **Behavior change:** Durable Objects and databases now run as root objects of the kernel's own registry (the new patch `0020-root-loader`) instead of facets of a platform Host object. The Host class and its internal binding and namespace are gone. Object files live under `do/coworkerd-do-root-v1-<namespace>/`; the bucket carries the format marker `cluster/format/do-root-1` and each stack directory a local one. **Durable Object and database data written by 0.5.x is not read.** `coworkerd-apiserver tool init` refuses a bucket whose `do/` prefix holds data without the marker (one LIST request, nothing written), and a node refuses to start without a compatible marker; use a new bucket and a new data directory. See [install](docs/guide/install.md).
- **Behavior change:** after a declared secret or a binding of a Worker changes, each of its Durable Objects reopens at its next request, as it does after a version change; that object's requests in flight and its connections end at that point. Before, a loaded object kept the old values until it was evicted. Rotating a secret the version does not declare reopens nothing, and stateless Workers are unchanged. See [Durable Objects](docs/guide/durable-objects.md).
- Only root objects set alarms: `setAlarm()` in a child facet throws workerd's own `Facets currently cannot set alarms.`
- Refuse a Durable Object class named `default` at deploy (`400 INVALID_REQUEST`), and compile the Durable Object form of the Worker during the deploy-time trial load, so that a class which cannot be loaded as an object fails the deploy instead of every later request.
- Answer `500 EXECUTION_FAILED` when a Durable Object's code cannot be loaded, and count it as a failed alarm attempt, retried with backoff, instead of retrying every second without counting. `503 DO_UNAVAILABLE` (retryable) now means only that this object's own preparation failed before its event was delivered. A request whose namespace was deleted while it waited is answered `409 DO_NAMESPACE_UNAVAILABLE`.
- HTTP SQL: a request body over 1 MiB (1,048,576 bytes, now the same limit in the control plane and the node) is answered `413 REQUEST_TOO_LARGE` instead of `500`, as are the other control-plane requests over their 64 KiB limit. A single statement object on `/query` now runs in one transaction like an array request: if a later statement of its `sql` fails, or its result exceeds the size limit, the earlier effects are rolled back. A node no longer truncates an internal answer over 8 MiB silently; such a request answers `502 RESPONSE_TOO_LARGE` and is not sent again.
- Add `cowctl d1 export <database> --output FILE` (`--no-schema`, `--no-data`, `--table`, `--force`). The file follows the layout of `wrangler d1 export`; every value is written by SQLite's `quote()`, so 64-bit integers and BLOBs are exact. Large tables are fetched in pages. An export that spans several requests compares `total_changes()` and the schema at its start and end and fails if they differ (`--allow-writes` overrides); the check does not survive a restart of the database object, so stop writers for a reliable backup. Statements over 100,000 bytes are written and reported, since `cowctl d1 execute --file` cannot import them.
- Limit the complete answer of a database request to 7,340,032 bytes (7 MiB), the same through HTTP SQL and through `env.DB`. A larger result fails with `D1_RESULT_TOO_LARGE` as an ordinary statement error (`success: false`; `env.DB` throws `D1_ERROR: D1_RESULT_TOO_LARGE`), and an array request is rolled back. Until now HTTP SQL answered `503 RESULT_UNCERTAIN` above 8 MiB and `env.DB` had no limit. Fetch large results in pages with `LIMIT`.
- **Fix binary values in databases.** A value bound as an `ArrayBuffer`, a typed-array view or an HTTP byte array is now stored as a BLOB, and a BLOB column reads back as an array of numbers through `env.DB` and through HTTP SQL. Releases 0.5.0 to 0.5.6 answered such a write with success but stored comma-separated number text, and read a BLOB as `{}`. Rows written that way are not converted; the database guide has a query that finds candidates. Arrays that are not bytes and plain objects are refused with `D1_TYPE_ERROR` instead of becoming text.
- Add `cowctl d1 execute <database> --file FILE` to import a SQL file of any size, for example a `wrangler d1 export` of a Cloudflare D1 database. The file is sent in pieces and SQLite itself finds the statement boundaries, through the new `{"ingest": "..."}` body of `POST /v1/tenants/{t}/databases/{d}/query`. Each piece is one transaction; the whole file is not, so import into an empty database. A failed or uncertain piece stops the import and reports the byte offset committed so far.
- Add `cowctl d1 migrations create`, `list` and `apply`: versioned SQL files in a migrations directory, applied in numeric order, each file together with its row in the `d1_migrations` table in one transaction. The directory and table come from `--dir` and `--table`, then from `migrations_dir` and `migrations_table` of the Wrangler configuration, which `cowctl deploy` now accepts along with `migrations_pattern`; the migration commands do not support `migrations_pattern` or subdirectory layouts yet. After a timeout or `503 RESULT_UNCERTAIN` the file is not sent again; the command reads the table to decide.
- Move the kernel to workerd `v1.20261114.1` with 24 patches (the SQLite `WITH` fix of 0.5.6 is now upstream) and update the compatibility table to 308 flags with a maximum table date of 2026-11-21. The system call tables are unchanged apart from the kernel hash they name. workerd now checks its process-wide default CPU limit at startup instead of at the first Worker load.

## 0.5.6 (2026-11-15)

- Validation (run v122 on the released assets, 2026-11-15): both architectures passed 30 package-content checks, eight delivery-feature checks and all 57 quickstart commands, and the two `WITH` statements run on both packaged kernels; on amd64, 31 Compose operations with a fourth node added while running passed; on arm64, concurrent opening of cold objects on three nodes read back every acknowledged write (689 with simultaneous and 40,172 with staggered arrivals) and never cut a WAL while a process held it, objects held by an open instance or by a hibernating WebSocket kept their files over `durableObjects.diskBytes`, 20 objects read back in full through three restart stages, and 120 objects rebuilt at once with at most 4 rebuilds active. The 25 core rows ran once on the node code `daa0eff2`. Each package differs from its 0.5.5 counterpart in 20 of 2,035 entries and `runtime/runtime.js` is byte-identical. No upgrade from an earlier version, full regression or long run was made for this release.

- Remove the cache admission, hold and healing code that can no longer be reached now that local files are not reused after a restart. The node state loses `durableLog.recovery.healPending`, `healBusy`, `healErrors`, `healSnapshots`, `healFailures`, `healedWalFrames`, `durableLog.objects.hold` and `durableLog.storage.positionsReused`.
- Limit how many Durable Objects a node rebuilds at once with `durableObjects.rebuildConcurrency` (4 by default, 1 to 64); further rebuilds wait first-come first-served. `durableLog.objects.activeRebuilds` and `queuedRebuilds` report the state.
- **Behavior change:** after a daemon restart no local Durable Object database file is reused. Every object starts unknown and is rebuilt from its replicated history at its first open, which removes the defects of deciding whether a local copy could be trusted (unconfirmed frames read after a restart, repeated rebuilds, held objects). The first request to each object after a restart reads the object store, and objects of a restarted node answer 503 while the store is unreachable. Data directories of older versions are not supported.
- Refuse a Durable Object request when the selected durable process generation has no open replication channel (`503 STORAGE_UNAVAILABLE`) and queue one automatic rotation of that slot. This covers a process spawned after its channel could not be created, which until now served commits without replicating them, and a channel that ended while the process kept running.
- Write a Durable Object's open marker back to true when its process reopens it without a new dispatch (a request sent before the close, or a message on a hibernating WebSocket), so that a same-boot restart no longer admits that cache as clean. If the marker cannot be written the instance is broken and the object repaired.
- Replay and synchronize a rebuilt Durable Object image in staging before replacing local files, preserving them when recovery fails before replacement begins.
- Look up a Durable Object's open registrations on a process channel by key prefix instead of scanning the whole table; with 8,000 registrations a dispatch check takes about 1/187 of the time. No behavior change.
- Count, per Durable Object and durable process generation, the requests in flight and the live WebSocket connections. An object with either is not evicted at `durableObjects.diskBytes`, which now also protects objects left with hibernating WebSockets only, and a rebuild of such an object resets the durable processes first. `durableLog.objects.activeRequests` and `activeConnections` report the totals.
- Re-check a Durable Object against the selected durable process generation right before a request is dispatched. A request whose object left the local state, lost its shard, or is still held by another generation while it waited is refused with `503 DO_RECOVERING`, `409 DO_SHARD_EPOCH_MISMATCH` or `503 DO_UNAVAILABLE` instead of being sent to a process that would open stale files; when nobody holds the object its WAL is cut back again before the dispatch. Refusals are logged as `[do-dispatch-refused]`.
- Keep the local files of a Durable Object that a live durable process still holds when the node is over `durableObjects.diskBytes`: an object registered as open is skipped, and dispatching a request refreshes its last-access time. `durableLog.disk.coldFilesSkippedLive` and the `files-drop-live-skipped` log line show candidates kept for this reason.
- Stop rejecting `WITH` statements with many repeated CTE references in the workerd SQLite parser.

## 0.5.5 (2026-11-14)

- Validation (run v111 on the released assets, 2026-11-14): both architectures passed 30 package-content checks, eight delivery-feature checks and all 57 quickstart commands; on amd64, 31 Compose operations with a fourth node added while running passed; on arm64, concurrent opening of cold objects on three nodes (simultaneous and staggered arrivals) read back every acknowledged write and never cut a WAL while a process held it. The node code is commit `b8ce5f85` (25 core rows, 52 component checks and the checks of the two fixes made after the first candidate); the 36 short regression cells of the 0.5.5 development rounds ran on the first candidate `864b1ce5`, which was withdrawn before publication. The kernel and both system call tables are byte-identical to 0.5.4. The coordinated upgrades from 0.5.3 and 0.5.4 were exercised on the first candidate only. No full regression or 12-hour run was made for this release.

- Wait for a live paused process within `processPool.pausedAdmissionWaitMs` (1000 ms by default; 0 disables it), reducing the transient single-process admission 503s in 0.5.3 and 0.5.4. Ready processes and other candidate nodes still take calls immediately. See [process admission](docs/cluster-state.md#7-placement).
- Carry the paused-admission wait through remote placement while retaining lease checks and retries only after a known pre-execution refusal. See [placement](docs/guide/install.md#5-configuration-reference).
- Add `coworkerd check host` and host first-touch reporting so operators can distinguish host page-fault stalls from Worker CPU use. See [host checks](docs/cluster-state.md#host-first-touch-diagnostics).
- Keep an expired suspect tenant in probation on its quarantine process until a later complete call succeeds with the process intact. The suspect record reports `suspected` or `probation`; successful probation logs `released`. See [suspects](docs/cluster-state.md#9-usage-and-suspects).
- Repair takeover paths that could omit acknowledged Durable Object tails or later replay new pages on incomplete history and corrupt a database. Recovery collects source epochs, publishes adopted history before serving, preserves snapshot cuts, adds local repair exclusions and open-generation checks, and heals eligible surviving legacy copies. See [Durable Object recovery](docs/cluster-state.md#12-durable-object-durability).
- Preserve an object's WAL while the durable process generation last given its files is alive, including before the node receives its open message and when the same process reopens it.
- Fail this object open when reading or writing its WAL fails, preserving its files and allowing a later request to retry. A successful truncation is not rolled back if the following sync fails.
- **Intentional behavior change:** a single encoded WAL record above 67,104,768 bytes (or the smaller configured peer-buffer limit) is refused with `STORAGE_UNAVAILABLE` before confirmation. Records over the normal 32 MiB page budget can occupy a page alone. SQL payload capacity is lower because WAL and record headers count, and Worker CPU and memory limits still apply. See [errors](docs/guide/errors.md).
- Use the coordinated [0.5.3 / 0.5.4 upgrade procedure](docs/guide/install.md#upgrading-053-or-054-to-055), retaining backups, all node volumes and the same host boot. The kernel and system call tables are unchanged from 0.5.4; an upgrade from 0.5.3 replaces the complete package while stopped.
- Known data-correctness, recovery-availability and resource issues remain. Read every item in [Known issues in 0.5.5](docs/guide/install.md#known-issues-in-055) before a stateful upgrade. Fixes continue in 0.5.6. Slow storage can also stall the node event loop and cause the watchdog to attribute that delay to a durable process; upgrade healing concentrates disk reads and snapshot uploads.

## 0.5.4 (prerelease, superseded by 0.5.5)

Deployments using Durable Objects or D1 should not remain on this prerelease. Use the coordinated 0.5.5
upgrade procedure and its known-issue precautions.

- Update the compatibility table for workerd `v1.20261108.1` to 306 flags with a maximum table date of 2026-11-15.
- Fix the amd64 steady-state seccomp filter buffer overflow present in 0.5.3 with steady enforcement or observation enabled by allocating its exact instruction count and checking the emitted length.
- Port facet alarms and namespace resident-object limits to the Rust server, preserving hibernating WebSockets, wake accounting and the node's refusal-code protocol.
- Port Durable Object WAL shipping to the Rust server, preserving the node protocol, confirmation gate, reset/delete notifications and `cloneFacet()` rejection.
- Define an explicit `internet` network service without TLS options in generated workerd configurations so the Rust server does not load system trust roots, while tenant egress continues through the node proxy.
- Reject simultaneous startup and steady-state `observe` filters because startup owns the sole seccomp notification listener (`EBUSY`); `auto` warns and disables the steady filter, and `launcher` refuses to start.
- Reject v2 definition dates later than the current UTC date in both the control plane and node, while preserving the existing rejection of invalid dates and dates beyond the kernel table limit.
- Pin the release script to the `v1.20261108.1` kernels with 17 patches, add `--check-only` before any Docker operation, and record each binary's GLIBC requirement with a 2.38 packaging limit.
- Update the installation, compatibility and upgrade guides for 0.5.4, document the bare-metal glibc ≥ 2.38 requirement, and clarify descriptor handoff numbers and their measured runtime reuse.

## 0.5.3 (2026-11-06)

- Validation (run b25 on the released amd64 assets, 2026-11-05/06): all 188 run items of the 209-row regression list, the twelve stateful fault scenarios and the 720-minute three-node single-host run passed; the run issued 2,159,999 stateless requests, read back all 42,429 acknowledged Durable Object writes, recorded no kernel OOM kill and no unplanned process exit, and its five good-tenant failures are all attributed to first-offense large allocations by the designed bad tenants. Daemon and durable-process RSS still grew during the window without an attributed cause (recorded, not a gate).

- Keep the memory-band descriptor copy above inherited descriptor numbers: when daemon fd 10 was free, workerd could receive the parent-liveness socket instead of the band and fail with `invalid inherited shared memory` (introduced during unreleased 0.5.3 development). Check every inherited descriptor source before spawn.

- Default `placement.subcalls` to `local` for caller-first stateless Service Binding calls; select `home` for memory-first deployments or large caller fan-out. Report `subcalls` in `configuration.placement`.

- Disable transparent huge pages for the Linux daemon, preserve workerd's host default, and report `anonHugeBytes` and `malloc.policy.thpDisabled` under `memory.daemon`.

- Report daemon resident/allocator memory and retained containers under `memory.daemon`; reap expired request-log and counter windows while idle, bound glibc's temporary-buffer retention, and return free pages after a minute without Durable Object appends.

- Set RPC platform errors' `.code` from an exception marker, including getters and returned capabilities;
  uppercase prefixes in application error messages no longer cause the runtime to add `.code`.
- End workerd when its node dies on macOS as well as Linux, using an inherited parent-liveness pipe.
- Accept a Durable Object peer open before rebuilding its history; wait on the first event and retain the two-second
  bound for an unavailable peer, so a request forwarded during a takeover is no longer answered `RESULT_UNCERTAIN`.
- Deployment and code refusal reasons are English constants (`definition unknown`, `definition inconsistent`,
  `code unavailable: ...`, `assets unavailable: ...`, `not in the membership table`, `another deployment intervened`);
  the earlier Chinese wording is gone.
- Mark size- and quota-refused objects for immediate repair, so the next read or write waits for confirmed history
  and cannot see the refused transaction.
- Size the bundled Compose store from available disk space with 1024 MiB volumes, removing its fixed 20 × 64 MB limit,
  which the 0.5.2 validation load filled after about 22 h 44 min (run b23, 2026-11-01: heartbeat writes then failed and
  every node fenced its Durable Objects until the store was expanded; no acknowledged write was lost).

## 0.5.2

- Bind each dynamic Worker entrypoint to the trusted invocation identity, so RPC completion clears the correct
  process-pool record and CPU progress belongs to the correct call. Preserve real background work and RPC capabilities.
- Reap storage hotload subprocesses, cache only successful identity updates, and preserve the store's actual exit
  status during shutdown. Compose now includes `storage-entrypoint.sh` and a 40-second storage stop budget.
- Package the updated workerd patch series and matching syscall profiles. Validated as the stable release of the 0.5 series on the
  unchanged assets: 645 release checks, the 188 regression cases with the stateful fault scenarios, and a 12-hour
  three-node run on one host in which every one of 42,434 acknowledged writes was recovered complete.

## 0.5.1

- Reconnect Durable Object replication streams while an unconfirmed or unsealed tail remains, without another
  business write. Retries retain the original five-second confirmation window and log identity. Session tokens
  separate receiver changes, replacement streams and local log epochs; late callbacks cannot confirm reused
  sequence numbers or undo isolation. Reopened streams drop exclusions before sending the current unsealed prefix.
- Serialize current and earlier-epoch log segment uploads across node epoch changes. Conditional-create collisions
  count as a successful upload only when read-back bytes match; conflicting content keeps the tail pending and
  cannot be published through a manifest.
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
- Security fixes for 0.3.x ended with this release. See the [security policy](SECURITY.md#supported-versions)
  for current coverage.

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

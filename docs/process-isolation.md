# Process isolation and resource governance

This document is for operators and security reviewers, and for contributors to the node's process pool and to the workerd patches. It describes how a node runs tenant code: the pool of workerd processes and its watchdog, the CPU and memory limits of every isolate, how a culprit is identified and kept in quarantine processes, eviction, the memory watermark and process rotation, the logs, the channels between a node and its processes, the environment the launcher sets up and the system call allowlist -- with what each costs and, in the last section, where isolation ends. The configuration keys and their defaults are in the [installation guide](guide/install.md), section 5.

## 1. The model

Tenants share workerd processes: **the V8 isolate is the boundary between tenants, and the process is the unit of enforcement.** Three facts shape everything below:

1. **A running request cannot always be stopped.** A CPU limit works by asking V8 to terminate the execution, which stops ordinary JavaScript within about a millisecond of the limit; but a long builtin can delay termination: the recorded `replace` case stayed stuck until the process watchdog killed it. The only certain way to stop them is to kill the process that runs them.
2. **Threads share the process.** When its watchdog kills a stuck process, all isolates in that process go with it.
3. **Stock workerd enforces no per-isolate limits**: its limit enforcer is an empty implementation. The limits are the patch series' (`patches/workerd/`).

So each mechanism handles what the one before it cannot:

| What goes wrong | What handles it |
| --- | --- |
| A request runs too long | The CPU limit terminates it inside its isolate; the process lives on |
| It cannot be terminated (a long builtin) | The watchdog kills the process; workerd has already named the culprit |
| An isolate allocates too much | The memory limit terminates it inside its isolate |
| V8 hits a fatal error | workerd writes any available attribution before exiting |
| The node confirms a culprit | The tenant becomes a suspect and its stateless calls run in quarantine processes |
| Idle isolates hold memory | Idle eviction, earlier and faster under the memory watermark |
| A long-lived isolate's garbage collection runs inside a request | Its CPU is not charged to the request, within its Worker's budget; idle isolates are collected between requests |
| A process's memory keeps growing | Process rotation replaces it with a new generation at a memory threshold |
| Tenant code attacks the host | The launcher's namespaces, no capabilities and the system call allowlist |

## 2. The process pool

- A node runs `processPool.count` ordinary workerd processes (default 1) and `quarantineProcesses` quarantine processes (default 2), plus `durableObjects.processes` durable processes (default 1, `d0`; up to 64, or 0 to host no objects). Each is a **slot** with **generations**; its identity in call records and logs is `slot:generation`, with generations counted from 1 at every start of the node.
- The node starts each generation with its workerd runtime settings, and dispatches to it only after it answered a readiness probe; a generation that is not ready within 15 s is killed. A process that exits is restarted after a backoff (`processPool.restartBackoffMs`, 100 ms, doubling up to `restartMaxMs`, 2 s).
- **Choosing a process.** The node uses rendezvous hashing to find the Worker's home among live processes of its class. Ordinary processes are keyed by Worker name and quarantine processes by tenant. It picks the next process when the home is unavailable, draining or at its in-flight limit. By default, that limit is [derived from memory](guide/install.md#waiting-call-capacity). A Worker's isolates thus stay in one process and pay for their memory once, and when a process fails, only the Workers whose home it was move.
- **Failure semantics.** A process found unavailable before dispatch is replaced by another, and nothing has executed; from the moment the call is sent to workerd -- or its version's load check in that process has started -- a process that dies gives it `RESULT_UNCERTAIN` (`LOAD_FAILED` when it named the load check as its culprit), or cuts a response already started, never a retry. When no local process of the Worker's class can take a call, the node refuses it (`PROCESS_UNAVAILABLE`, or `QUARANTINE_UNAVAILABLE` for a suspect tenant), and where placement decides where the call runs, it tries another node.
- The node is ready (`/__coworkerd/ready` answers 200) only while every ordinary slot has a ready generation, so an ordinary slot that restarts takes the whole node out of a load balancer that follows readiness until it is back; it also answers 503 while the node drains. The quarantine and durable slots do not count -- trial loads and suspect tenants run there, and a version that keeps crashing one must not take the node out --; the state endpoint lists unready quarantine slots under `readiness.quarantine`, and durable slots in the process and durable-log state.
- The node kills its processes when it stops; started through the launcher, a process also dies when its node dies.

A durable object's namespace and object id select one durable slot; it is not moved to another local slot or to quarantine when that slot is unavailable. Nodes with `durableObjects.processes: 0` still accept and forward object calls, but cannot acquire shards. Several object trees of one Worker definition can share an isolate copy on a home thread; each object and its facets have their own SQLite files. A facet's storage participates in the root object's replication, byte limits and recovery. `ctx.facets.clone()` is refused because it would bypass the frame log.

## 3. Child supervision

The node checks the generation's identity, role and protocol before marking it ready. With
`processPool.watchdog` enabled, it polls ready and draining generations every
`processPool.probeIntervalMs` (50 ms). `processPool.stuckTimeoutMs` (3 seconds) caps each
probe and is the minimum stable uptime before the restart backoff resets. The state endpoint
reports probe failures and delays separately from process exits.

## 4. CPU limits

Stateless calls have a default of 30,000 ms and a maximum of 300,000 ms per Worker.
A Durable Object event gets up to 30,000 ms, subject to its Worker's or tenant's limit.
The budget accumulates execution CPU across JavaScript entries and charges the event's
SQLite work in a Durable Object. V8 interrupts a call on reaching its limit. A call that
has yet to send response headers receives `503 CPU_LIMIT_EXCEEDED`. A response already
streaming ends early. A durable limit resets the affected tree, which reopens on confirmed
storage on its next event.

## 5. Memory limits

- Each isolate may hold `memoryLimitMiB` (default 128, or its Worker's or tenant's own) of JavaScript heap and external memory together; ArrayBuffers are counted by an allocator of the isolate's own.
- At the limit -- V8's near-heap-limit callback, a total over the limit after a garbage collection, or an ArrayBuffer allocation over the quota -- the running call is terminated and the isolate invalidated: `503 MEMORY_LIMIT_EXCEEDED` before the response started. The extra heap V8 is granted so that the termination can complete is at most 8 MiB above the limit.
- Native processes reserve virtual heap headroom above the tenant soft limit for one maximum-sized V8 object, plus the limit itself and 32 MiB for the pages large objects fill unevenly. Live heap and external memory are checked after collection, including before response headers are committed; an isolate whose heap still reaches that ceiling (V8 places the first object of its young large-object space without a collection) is terminated as over its limit, with its ceiling raised to one maximum object above the heap it uses (which can already be past the ceiling) so that the allocation in progress finishes. The termination takes effect at V8's next interrupt check; until then the ceiling may rise again, only within what is left of the process memory budget and of the shared pointer cage (in a kernel built with a V8 sandbox per isolate, `--config=coworkerd-sandbox`, of the isolate's own cage), after which V8's fatal out-of-memory path follows. `JSON.parse` and `String.prototype.split` check interrupts while they allocate; `String.prototype.replaceAll`, a global `replace` with a replacement function (while it collects the matches) and the structured clone's strings check them too; `Array.prototype.sort` and `toSorted` of an array-like object, a string or a proxy no longer allocate a work array of the whole length up front, and check interrupts while they collect its elements; and a JSON array past V8's largest (134,217,728 elements; 67,108,864 with a reviver), a JSON object past 16,777,216 properties, a split past 134,217,728 parts, a global `replace` or atom `match` with more matches than that and a `sort` or `toSorted` of an array-like object or a string with more elements than that throw a `RangeError` instead of ending the process (workerd patch 0044, V8 patch 0056). What a raised ceiling may still take from the process budget is reserved while its termination is pending, so that two isolates waiting for theirs cannot both take the same remainder; it is returned when the termination takes effect. The process budget reclaims idle Workers before terminating a growing invocation with `PROCESS_MEMORY_PRESSURE`. On a fatal V8 out-of-memory error, workerd writes the OOM and the last allocation's execution identity to the event pipe when available. For ordinary and quarantine processes, the node confirms the tenant only when V8 identifies an isolate heap limit failure or an isolate-owned cage and the isolate's last heap sample reaches its configured limit. Shared-cage exhaustion stays `undetermined` when the evidence only identifies the final allocation. Attributing shared exhaustion would require evidence of how much of the cage each isolate occupied at the time.
- When the kernel kills a process for memory, nobody can tell who caused it, and no tenant is marked. The node recognises such a kill by the `oom_kill` counter in its own cgroup's `memory.events` (cgroup version 2: the cgroup `/proc/self/cgroup` names, as under systemd, or the namespace's root in a container with a cgroup namespace of its own): a workerd process that exits by `SIGKILL` within 2 s of an increment the node saw takes it and counts as `memory-oom`; an increment no such exit takes was another process's of the cgroup and changes no exit's category (`processes.kernelOomKills` counts both). Without the file every such exit counts as `other`. With `memoryLimitMiB` at `0` -- which only `COWORKERD_MEMORY_LIMIT_MIB=0` sets, the configuration file refuses it -- no exit is classified as a memory one.
- Native processes count WebAssembly backing memory together with each isolate's heap and ArrayBuffers. The same accounting applies to Durable Object isolate copies.

Native tenant memory overruns retire the offending isolate; process pressure terminates the growing invocation without condemning its Worker. An unexpected fatal OOM records the executing call before exit so the node can quarantine its tenant.

## 6. Limits per Worker

A Worker can have its own CPU and memory limits, set through `/v1` within its tenant's maxima; without them its tenant's default limits apply, then the node's `cpuLimitMs` and `memoryLimitMiB`. The node passes the limits a Worker names with every call. The native loader includes the limits in the Worker load identity and the object definition fingerprint. Each isolate uses its Worker's limits, and changing the limits loads a new isolate. The state endpoint reports the limits in effect for each Worker whose limits it or its tenant names (`tenants.tenants.<tenant>.workerLimits`, at most 256 per tenant) and the WebAssembly limit per process generation.

## 7. Culprits and quarantine

- **Three sources of suspicion.** A CPU watchdog record identifies a call that exceeded its configured CPU limit and failed to terminate. A memory record can identify an isolate that exhausted its own memory, as described in section 5. In the threaded ordinary and quarantine roles, V8's fatal-error callback records the Worker executing in the current isolate on the thread that raised the error. The callback uses the identity prepared for that invocation and writes to the inherited event pipe before exiting. The node checks the exit status and matches the record to a dispatch in that process generation. Recent exits appear under `processes.recentFatalExits` in node state. An exit without sufficient evidence stays `undetermined`.
- **Scope of a V8 fatal record.** The same thread-based attribution rule applies to embedder API checks and process-wide exhaustion that V8 reports through the fatal-error callback. The whole load carries the load's identity once the kernel has set up the isolate (compiling and evaluating its modules, its environment's JSON values, handler validation); each JavaScript entry of a call carries the call's (including microtasks, timers and the native binding's RPC entry), as does the source JSON check. Startup, logging and dynamic import entries take the enclosing load or call, else the call of the current I/O context when that call runs the same Worker, else the Worker's load identity, since an isolate runs only its own Worker's code (a legacy dynamic import, which continues outside its call's I/O context, is therefore named by the load identity, and stays `undetermined` once the node no longer holds that load's dispatch); the node still requires a dispatch of the named invocation in that process generation. A heap OOM at an isolate's limit is therefore attributable wherever its report scope is. Not covered, so `undetermined`: work on V8's own platform threads (concurrent compilation and garbage collection), which have no current isolate; setting up a new isolate before the kernel installs its handler, which runs no tenant code; and paths that take an isolate's lock without running JavaScript outside a load or a call (tearing down a Worker that loaded, cancelling a hung request). A Worker whose load fails is torn down inside the load's scope, so a fatal error then is named. The node logs bounded V8 location and message text after secret redaction, with the source line when available.
- **Suspects are tenants.** Once any Worker of a tenant is a culprit, all of that tenant's stateless Workers run only in quarantine processes, in the order of the tenant's rendezvous hash over them: normally all of a suspect's calls go to one quarantine process, its shard, and only while that shard cannot take calls do they go to the next one. The tenant's other Workers follow it by design: the protection is for the other tenants.
- **The suspicion period** is `suspectBaseMs` (10 minutes), doubled when the tenant offends again while suspected or awaiting probation, up to `suspectMaxMs` (1 hour). Its deadline is the earliest possible return: with `suspectProbation` (default `true`), the tenant stays in quarantine until an invocation dispatched there **after** the deadline completes its runtime context, including `waitUntil`, without the process being killed or crashing. An isolate's CPU or memory termination counts if the process survives; so does a Worker returning 5xx. Trial loads and calls dispatched before the deadline do not count. Each node releases the tenant independently; passing on one node does not release it on another. Probation is not persisted: restarting a node loses its expired marks and the tenant returns directly to ordinary processes there. `suspectProbation: false` restores release at the deadline. Without quarantine processes this setting has no effect.
- **Shared by the cluster.** The node that identified a culprit applies the mark at once and writes its unexpired marks as one object beside its heartbeat in the object store, `members/<nodeId>/<epoch>.suspects`; every node reads the other members' objects in its membership listing, each only when it changed. At the recorded deadline each node enters local probation ([cluster state](cluster-state.md), section 9); expired marks are omitted from the object and do not cause periodic rewrites. A node without the object store's membership -- the self-check, or the file membership of the command line's test form -- keeps its marks to itself. Without the store, known marks keep their deadlines and new ones apply locally until they can be written.
- **Never an ordinary process, once a node knows the mark.** When no quarantine process of a node can take a suspect's call, the node refuses it (`QUARANTINE_UNAVAILABLE`, not executed), and where placement decides, it tries another node. The node that identified the culprit applies the mark at its next call; the others learn it at their next membership listing, within `membership.listIntervalMs` (10 s) -- measured, 6 s -- and not at all while it cannot be written to the store; calls already running in an ordinary process are not moved.
- **Trial loads run in quarantine processes too**, in the tenant's order, so a new version that crashes a process during its deployment takes no ordinary tenant's requests with it. The load check before a version's first call in a process runs in the process the call will use -- an ordinary one for a tenant that is not a suspect.

`quarantineProcesses` `0` turns quarantine off: suspicions are still recorded but change nothing, and trial loads use the ordinary processes.

## 8. Eviction and the memory watermark

Idle collections (section 4) run in the same loop of workerd's evictor.

**Idle eviction** is workerd's (a patch): a Worker loaded by the Worker Loader is removed once it has no request in flight -- streaming responses and JS RPC included -- no `waitUntil` work, and has been idle for `idleEvictMs` (5 minutes). A Worker whose load failed is removed at once, so a failure is never permanent. The threshold weighs a cold start, a few milliseconds, against the memory of resident Workers; it must be `0` or longer than `placement.idleMs`.

Durable processes also evict idle object trees when the resident target (`durableObjects.residentObjects`, 4096; `0` disables it) is exceeded. Facets leave before the root; active objects are protected, so this is a target rather than a hard instantaneous count. Hibernatable WebSockets stay connected during this eviction and wake their object on the next message. Evicting a version's Worker tears down its idle object trees first.

**The memory watermark** is the node's, on Linux: every 100 ms it reads its cgroup's working set (`memory.current` minus `inactive_file`) against `memory.max`, or against `memoryBudgetBytes` when the container has no limit, and sets a band; with neither, the watermark is off. Red is from 90 %, yellow from 75 %, back to green below 65 %, and red steps down to yellow below 75 %. workerd's evictor reads the band from shared memory.

| Band | Eviction | Placement |
| --- | --- | --- |
| Green | Idle for `idleEvictMs` | Normal |
| Yellow | The least recently used idle Worker first, idle at least 5 s, one at a time | Forwarded and nested calls for versions that have not run on this node within `placement.idleMs` are refused |
| Red | The same, idle at least 1 s | All forwarded and nested calls are refused |

A request executing on the node that received it is never refused by the band. Measured on three containers limited to 2 GiB with a stream of new Workers, in ten-minute runs with the watermark on and off three times each: with it, no call failed, no process was killed for memory; without it, 1,120 calls failed and the kernel killed processes 154 times in all.

## 9. Process rotation

An ordinary or quarantine slot is replaced by a new generation in steps, at most one rotation at a time on a node:

1. start the new generation next to the old one and wait until it is ready;
2. switch dispatch: new calls go to the new generation, calls already dispatched stay where they are; the old generation's WebSocket connections are closed with `1001` (going away) at once, so that their clients reconnect to the new one instead of being cut when the old one exits, and the rotation's record counts them (`webSocketsClosed`);
3. drain the old generation until its calls and request contexts -- `waitUntil` work included -- have ended, for at most `rotation.drainTimeoutMs` (30 s); what is still running then is killed and ends as `RESULT_UNCERTAIN`;
4. close the node's connections to it, send SIGTERM, and SIGKILL after `rotation.termGraceMs` (5 s).

**Triggers**: the content of the workerd file changes (checked every `rotation.binaryCheckMs`, 1 s); by hand (`POST /__coworkerd/rotate`, `coworkerd-apiserver tool rotate`); a generation's resident memory reaches `rotation.residentBytes` -- by default 70% of the node's memory: its container's `memory.max`, else `memoryBudgetBytes`, else the machine's, which the state endpoint names (`processes.rotation.configuration.residentBytesSource`); and, both off by default, a generation ready for `rotation.maxAgeMs` or one that took `rotation.maxRequests` calls. A failed automatic rotation is retried after `rotation.retryMs` (5 s), doubling. Durable processes use a different sequence: stop new dispatch, close connections and drain the old generation, exit it, then start and wait for its replacement. Incoming object calls may wait for that replacement. They rotate on binary changes, manual rotation, the memory threshold and the secret dictionary (below), never on age or request count even when those triggers are configured. Quarantine processes rotate too; replacing the launcher does not trigger a rotation. A request that lasts longer than the drain period can be cut by a rotation.

**The secret dictionary's trigger** (the log writers' dictionary of secret values, [errors](guide/errors.md)): a durable process keeps every secret value it received until its generation is gone. When one tenant's earlier values pinned in a slot's current generation pass 512 KiB, the node rotates that slot (at most once every 10 minutes per slot); the old generation's values leave the dictionary 60 s after it is gone. A tenant that triggers it a second time within an hour rotates nothing: its Durable Object calls with secrets are refused on that node instead (`SECRET_UNAVAILABLE`, its earlier values grow too fast) until the hour has passed, when the next trigger rotates again. A tenant that changes a 1 KiB secret every hour triggers it about every 512 hours. Since every tenant of the slot sees a rotation (its Durable Object calls wait for the new generation, its WebSockets close with `1001`), one tenant can still cause one an hour on its node.

**Why not every hour.** Up to 0.4.0 every process was replaced every hour, because its resident memory kept growing between rotations. The collections of section 4 keep it level -- measured, an hour of the tree of Workers at 15 requests a second per root without rotation on an amd64 machine: the node's processes together rose to 891 MiB in the first 40 minutes and to 894 MiB by the end (0.4.0's, beside them, to 3,205 MiB) -- while a new generation only rebuilt every isolate, with its cold loads and its first, largest collection, and cut the requests longer than the drain. The memory threshold stays as the bound. Rotation is also how workerd is upgraded on a running node; measured, rotating a node's slots under load failed no request.

## 10. Logs

- **The node's own output** goes through a shared-memory ring of 4 MiB to a separate writer process, `coworkerd-log`, which writes it to the node's original standard output and error (the journal, or `docker compose logs`), so a slow receiver never makes the event loop wait. When the ring is full, lines are dropped and counted, and a `[log-dropped]` line reports them; lines already in the ring are written even after the node is killed.
- **workerd's output** goes through pipes to the reader process `coworkerd-wlog`, which keeps a backlog of 4 MiB per process and drops whole lines beyond it, counted and reported; workerd never waits for a slow disk. It writes `logs/workerd.log`, renamed at 256 MiB, with four files kept.
- **Tenants' logs** are kept for `cowctl logs` in the reader's memory, per tenant. A Worker's `console.*` lines leave its isolate over the tail channel workerd gives each loaded Worker, under that Worker's own identity: the runtime's tail entry posts them to the node over a socket only the runtime can reach (`POST /v1/log`), and the node stores them in the tenant's buffer under the Worker and version the channel names. Nothing tenant code can read or write decides whose lines they are: whatever a tenant writes counts as its own. The lines come as they are written -- the tail is a streaming one, which workerd gives a Worker only when the runtime runs with the `experimental` compatibility flag (tenants' own flags stay non-experimental) -- each post naming the invocation they belong to, and the node keeps at most 256 KiB of them an event: a request, or a message of a WebSocket connection (an object's fetch, RPC, alarm and WebSocket-message handlers are attributed as events); past it the event's lines are cut, with one warning line in their place.

## 11. The channels between a node and its processes

workerd listens on no TCP port, has no network beyond what the sandbox leaves it (section 12) and ordinary processes open no file of the node after their first request; durable processes open SQLite and facet files under their assigned durable directory; what it shares with its node is handed over when it starts:

| Descriptor at handoff or path | Carries | Where it goes while running |
| --- | --- | --- |
| 1, 2 | Standard output and error: pipes to `coworkerd-wlog` | Unchanged |
| 3 | The event pipe: limit events, culprits, out-of-memory identities and the end of every request, one `write()` per row | Unchanged |
| 4 | The threaded server's listening Unix socket, `run/<slot>-<generation>.sock` | The server's I/O layer takes ownership under another descriptor number (7 in the measured processes); 4 is reused for an epoll instance |
| 5 | Shared memory with the double-buffered snapshot of resident Workers and their lifecycle | Mapped once, then closed; the number is reused and the mapping remains in `/proc/<pid>/maps` |
| 6 (only with the watermark on) | Shared memory with the memory band, sealed against resizing | Mapped once, then closed; the number is reused and the mapping remains in `/proc/<pid>/maps` |
| 9 (durable processes) | Transaction frames and confirmation / checkpoint commands over an inherited socket to the node's replicator | Unchanged |
| 10 (since 0.5.3) | The parent-liveness channel, an inherited socketpair: workerd exits when its node dies | Unchanged |
| `run/binding.sock`, `run/source.sock` | The threaded server's Bindings, version code and tenant log requests (`POST /v1/log`) | Paths, not inherited descriptor numbers |
| `run/egress.sock` | Tenant code's outgoing requests, to the node's egress policy point | Path, not an inherited descriptor number |

A running process's `/proc/<pid>/fd` does not correspond one-to-one with the handoff column: verify shared memory
by its inode in `maps`, and the listening socket by the inode's `run/<slot>-<generation>.sock` path in `/proc/net/unix`.
The launcher's own descriptors 7 and 8 are close-on-exec and do not reach workerd; the running server can reuse those numbers.

`run/` is readable only by the node's user. The end of every request is written whether or not it used CPU, so draining knows when a generation is idle, and at an exit the node reads the event pipe to its end, so the evidence of a crash is not lost.

## 12. The launcher's environment

While the sandbox is active, the node starts every workerd through `coworkerd-launcher` ([its protocol](../launcher/PROTOCOL.md)), which sets up the following and then executes workerd:

- one `unshare` for all its namespaces: a **user namespace** in which the process is uid and gid 65534, mapped to the node's own user outside, and the mount, network, IPC and UTS namespaces;
- then a **root** that is an empty read-only tmpfs on `<stack>/sandbox` with only what workerd needs bound in: read-only the workerd file (checked to be the very file the node opened), its ELF interpreter and libraries, `/dev/urandom`, the steady-state table when it is in use and, when present, `/sys/devices/system/cpu` and `/etc/localtime`; read-write `/dev/null` and `run/`, plus `do/` for durable processes;
- in the **network** namespace no route and only a loopback device, which is down (a kernel with tunnel drivers loaded adds their inert devices);
- **no capabilities** and `no_new_privs`;
- a **parent-death signal**, so workerd dies with its node;
- the **system call allowlist**, last (section 13).

The node reads `/proc/<pid>` of every process once it is ready and reports the layers it observes, not the ones the launcher claims (`processes.entries[].sandbox.observed.layers`). A launch that fails exits with status 125 and a record naming the failing step; the slot is restarted with its usual backoff and the exit counted as `launch-failed`, not as a workerd crash.

`sandbox.mode` decides what happens when the host cannot provide this: `auto` (the default) uses the launcher when its startup check passes and otherwise starts workerd directly, with a warning; `launcher` refuses to start; `off` never uses it. The host must allow unprivileged user namespaces; Docker's default seccomp and AppArmor profiles do not, so the default compose cluster runs without the sandbox and says so. Setting up the environment adds a few milliseconds to every process start.

## 13. The system call allowlist

- **Generated by the build.** `scripts/syscall-allowlist.py` derives the tables from profiles of this very workerd binary under load -- every call seen during startup goes into the startup table, every call seen afterwards into both -- plus calls always allowed with a recorded reason. Changing the compiler, the build options or the upstream version can change the set, so it is never written by hand. A table names the workerd binaries it belongs to by their SHA-256.
- **Two tables.** The launcher installs the **startup table** as its last step before executing workerd, so it must allow what startup needs, `execve` among it -- a seccomp filter can only ever be narrowed. When it answers its first request, the node's readiness probe, workerd installs the narrower **steady-state table** itself and, in the same step, makes itself non-dumpable, which the launcher's setting cannot achieve because `execve` resets it. Use the counts and binary hashes in the package's allowlist record; they depend on the workerd build.
- **Three modes** for each table: `off`; `observe`, in which a call outside the table is reported and then allowed -- only a call of another ABI (i386 or x32), which workerd itself never makes, still ends the process --; `enforce`, in which such a call kills the process (exit category `syscall-denied`) and its requests in flight fail. The startup table's reports reach the node through the kernel's user notification: they appear in `processes.sandbox.syscalls` -- counted exactly, per call number; of a burst of more than 4096 between two of the node's reads only the details (the thread of each) are dropped, and counted as `detailsDropped` -- and as `[syscall-observed]` lines, at most one per call number every 10 seconds. The steady table's reports are the process's own `[coworkerd-seccomp]` lines in `logs/workerd.log`, one for the first occurrence of each call; the state endpoint does not count them. In `enforce` the steady table is installed on all threads of the process; in `observe` only on the thread that installs it and the threads it starts afterwards. Steady `observe` also requires the startup table to be in `enforce`: each process has only one seccomp notification listener, which startup `observe` holds, so the node rejects steady `observe` under startup `observe` with an `EBUSY` reason (`auto` warns and leaves steady off; `launcher` refuses to start); steady `enforce` is unaffected.
- **Defaults and the 0.6.0 release tables.** Keep `observe` for the startup table (`sandbox.syscalls.mode`) and `off`
  for the steady-state table (`sandbox.syscalls.steady.mode`). Do not use `enforce` with either published table in 0.6.0.
  With both tables set to `enforce`, a child process can be killed before it is ready. For locally generated tables,
  switching to `enforce` should follow a period of `observe` under real traffic without a report.
  See [#163](https://github.com/coworkerd/coworkerd/issues/163).
- **Pairing.** The launcher reads the startup table at every launch and refuses to start bytes it does not name (`launch-failed`). The node checks the pair once, at startup: in `auto`, a table that does not name the running workerd -- or a steady table that does not -- is left off with a warning, and workerd then runs without that filter while the other layers stay; in `launcher` the node refuses to start. The steady table is used only while the startup table is: with `sandbox.syscalls.mode` `off`, or a startup table left off, a steady mode other than `off` cannot be honoured -- `auto` warns and runs without the steady table, `launcher` refuses to start. workerd reads only the call numbers of the steady table, and nothing checks the steady table again after the workerd file is replaced.
- **Upgrading workerd**: install a startup table that names both the old and the new binary first, then replace the binary -- the node rotates its processes -- then install the table for the new binary alone. The steady-state table needs the same care: the node binds the same file into every new generation without checking it against a replaced binary, so in `enforce` an old steady-state table can end the new binary's processes.

Installing the filter costs about 2 ms per process start; request latency shows no measurable difference.

## 14. What it costs

| Mechanism | Measured cost |
| --- | --- |
| CPU and memory limits | About 0.03 ms per request |
| The launcher's environment | 4-9 ms per process start, and about 2 ms more for the system call filter |
| A stuck process | Its neighbours are unavailable for about half a second with several ordinary processes per node, about three seconds with the default single one |
| An evicted Worker | One cold start, about 6 ms, the next time it is called |
| A rotation | The new generation's cold loads, about 7-12 ms per Worker |
| Idle collections | Each pauses its process, outside any request: measured, the first ones under 21 ms on an arm64 machine; on an amd64 machine 53 of 754 in an hour took 100 ms or more, at most 156 ms |
| The collection budget | Up to a tenth of a core per Worker of collections the CPU limit does not charge |
| A quarantine process | About 30-45 MB of resident memory |

## 15. Where isolation ends

- **Between isolates in one process.** Isolates of different tenants share a process, its address space and V8's pointer-compression cage; V8's own sandbox is not part of the patch series. The V8 isolate is the boundary between tenants there, and the limits bound how much CPU and memory one can take from the others, but a flaw in V8 is a flaw between tenants. These limits and process controls do not make an isolate a separate process.
- **Between processes of a node.** All workerd processes of a node map to the node's own user outside, and the launcher creates no PID namespace. Against one process reading another's memory there are three measures: the host's `kernel.yama.ptrace_scope` of at least 1, which the installation guide asks for (nothing checks it); for each process started through the launcher, non-dumpability from its first request -- the node's readiness probe, before it has answered anything; and the system call allowlist, which names neither `ptrace` nor `process_vm_readv` (a call to either is killed in `enforce` and reported in the default `observe`). Without the sandbox only the first is there. All of them also share the node's `run/`, bound read-write: code that escaped V8 could replace the node's callback sockets there or connect to another process's socket. Tenants are not given processes of their own, and suspicion changes stateless routing only; durable objects stay in durable processes.
- **The node's environment.** A workerd process inherits only a few variables of the node's environment -- the locale, the time zone, the search path, the temporary directory, output colouring and switches workerd itself reads for tests -- besides the settings the node gives it; credentials the node was given as environment variables, such as the management token or object storage keys, stay with the node.
- **Between workerd and the host.** With the sandbox active, a workerd process has no network, no capabilities, a view of a few files, read-only except `/dev/null`, `run/` and, for durable processes, `do/`, and -- unless `sandbox.syscalls.mode` is `off` or, in `auto`, the table could not be used for the running workerd -- a filter on its system calls, which in `enforce` ends the process on a call outside the table and in `observe`, the default, reports it; a call of another ABI ends the process in both modes. When the host does not allow the sandbox and `sandbox.mode` is `auto`, workerd runs without any of these layers, with the node's own network: tenant code then still reaches the network only through the egress policy point, because that is how its workerd is configured, not because the process could not.
- **Tenants' logs.** Attribution comes from workerd's per-Worker tail channel, so a tenant can neither write lines kept as another tenant's nor read another tenant's lines. Reading them is guarded by credentials: `/v1` by the tenant's or the administrator's, a node's `/__coworkerd/logs` by the management token -- starting without one requires `management.allowWithoutToken: true`; in that mode anyone who reaches the endpoint can read the lines.
- **The node itself** is not sandboxed: it runs as an ordinary user with network access to object storage and its peers, holds its own object storage identity and the cluster's secrets key, and it is what tenant code would have to break out to reach.

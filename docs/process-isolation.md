# Process isolation and resource governance

This document is for operators and security reviewers, and for contributors to the node's process pool and to the workerd patches. It describes how a node runs tenant code: the pool of workerd processes and its watchdog, the CPU and memory limits of every isolate, how a culprit is identified and kept in quarantine processes, eviction, the memory watermark and process rotation, the logs, the channels between a node and its processes, the environment the launcher sets up and the system call allowlist -- with what each costs and, in the last section, where isolation ends. The configuration keys and their defaults are in the [installation guide](guide/install.md), section 5.

## 1. The model

Tenants share workerd processes, as on Cloudflare's own platform: **the V8 isolate is the boundary between tenants, and the process is the unit of enforcement.** Three facts shape everything below:

1. **A running request cannot always be stopped.** A CPU limit works by asking V8 to terminate the execution, which stops ordinary JavaScript within about a millisecond of the limit; but long builtins such as `replace`, `sort` or `normalize` do not check for termination and can run hundreds of times past the budget. The only certain way to stop them is to kill the process that runs them.
2. **Threads are not a boundary.** More threads only raise the number of tenants an attacker needs in order to stall a process.
3. **Stock workerd enforces no per-isolate limits**: its limit enforcer is an empty implementation. The limits are the patch series' (`patches/workerd/`).

So each mechanism handles what the one before it cannot:

| What goes wrong | What handles it |
| --- | --- |
| A request runs too long | The CPU limit terminates it inside its isolate; the process lives on |
| It cannot be terminated (a long builtin) | The watchdog kills the process; workerd has already named the culprit |
| An isolate allocates too much | The memory limit terminates it inside its isolate |
| A single allocation too large to stop | V8 aborts the process; workerd writes the culprit first |
| A tenant does it again | The tenant becomes a suspect and runs only in quarantine processes for a while |
| Idle isolates hold memory | Idle eviction, earlier and faster under the memory watermark |
| A long-lived isolate's garbage collection runs inside a request | Its CPU is not charged to the request, within its Worker's budget; idle isolates are collected between requests |
| A process's memory keeps growing | Process rotation replaces it with a new generation at a memory threshold |
| Tenant code attacks the host | The launcher's namespaces, no capabilities and the system call allowlist |

## 2. The process pool

- A node runs `processPool.count` ordinary workerd processes (default 1) and `quarantineProcesses` quarantine processes (default 2). Each is a **slot** with **generations**; its identity in call records and logs is `slot:generation`, with generations counted from 1 at every start of the node.
- The node writes each generation's workerd configuration into `processes/`, starts it, and dispatches to it only after it answered a readiness probe; a generation that is not ready within 15 s is killed. A process that exits is restarted after a backoff (`processPool.restartBackoffMs`, 100 ms, doubling up to `restartMaxMs`, 2 s).
- **Choosing a process** repeats placement's rule inside the node: a Worker's **home process** is chosen by rendezvous hashing among the live processes of its class -- ordinary processes by the Worker's full name, quarantine processes by its tenant -- and the next one is taken when it is not ready, paused, draining or at its limit of calls in flight (`processPool.inFlightLimit`, 512). A Worker's isolates thus stay in one process and pay for their memory once, and when a process fails, only the Workers whose home it was move.
- **Failure semantics.** A process found unavailable before dispatch is replaced by another, and nothing has executed; from the moment the call is sent to workerd -- or its version's load check in that process has started -- a process that dies gives it `RESULT_UNCERTAIN` (`LOAD_FAILED` when it named the load check as its culprit), or cuts a response already started, never a retry. When no local process of the Worker's class can take a call, the node refuses it (`PROCESS_UNAVAILABLE`, or `QUARANTINE_UNAVAILABLE` for a suspect tenant), and where placement decides where the call runs, it tries another node.
- The node is ready (`/__coworkerd/ready` answers 200) only while every ordinary slot has a ready generation, so an ordinary slot that restarts takes the whole node out of a load balancer that follows readiness until it is back; it also answers 503 while an ordinary slot is paused for an overdue probe, and while the node drains. The quarantine slots do not count -- trial loads and suspect tenants run there, and a version that keeps crashing one must not take the node out --; the state endpoint shows them under `readiness.quarantine`.
- The node kills its processes when it stops; started through the launcher, a process also dies when its node dies.

## 3. The watchdog

The node probes every process every `processPool.probeIntervalMs` (50 ms); the probe has to pass through the process's event loop to be answered.

- **Probe overdue** (`probeOverdueMs`, 400 ms): dispatching new calls to the process is paused until a probe is answered again. Pausing is safe -- nothing has executed yet -- and keeps a long but legitimate piece of work from being punished: measured, such work was only paused, never killed.
- **Stuck** (`stuckTimeoutMs`, 3 s): the process is killed with SIGKILL and restarted; the kill record lists the calls that were in flight and the culprit, when workerd named one.

A probe that fails at once (a reset connection) does not stop the probing, and timing goes on from the first unanswered probe. Measured with four processes per node, a stuck process makes its neighbours unavailable for about half a second; with a single process the node waits about three seconds. The calls that were in flight in the stuck process, and those dispatched to it in the 400 ms before the pause, end as `RESULT_UNCERTAIN` when it is killed, or are cut if their responses had started.

## 4. CPU limits

- Each request may use `cpuLimitMs` milliseconds of CPU (default 200), or the limit its Worker or tenant names. `cpuLimitMs` must be positive -- a configuration file that sets it to `0` is refused -- and only `COWORKERD_CPU_LIMIT_MS=0` in the environment turns the node's limit off, while a limit a Worker or tenant names still applies. Only tenants' isolates have one, not the runtime bridge.
- What counts is the CPU time of the thread running the request's JavaScript, accumulated over every entry into JavaScript of the request -- after each `await`, and in `waitUntil` work. A module's top-level code is measured against the same budget.
- At the limit, workerd asks V8 to terminate the execution. The caller gets `503 CPU_LIMIT_EXCEEDED` when the response has not started yet, and a cut response otherwise; the call is not retried, because it did run.
- The isolate is invalidated -- a terminated isolate may be inconsistent: requests still in flight in it fail with it, and the next request loads a new one.
- One watchdog thread per workerd process keeps the deadlines: entering JavaScript registers one, leaving removes it. Termination takes effect within about a millisecond of the limit for ordinary code.
- If termination has not taken effect after a 100 ms grace period, workerd writes a **culprit** row naming the call to its event pipe -- from the watchdog thread, not the event loop, which is what is stuck -- and the node's watchdog later kills the process with the culprit already known.

**Garbage collection.** V8 collects an isolate's garbage on the thread that runs its requests, so a collection's CPU time would count against whichever request is running. A minor collection (scavenge) is short and stays charged. A full collection (mark-compact) of a long-lived isolate can take hundreds of milliseconds -- most of it destroying the wrappers of objects the isolate let go, in one atomic pause -- and is not the request's doing:

- **Left out of the limit, within a budget.** The thread CPU of a full collection -- its incremental marking steps and weak callbacks included -- that runs during a request is subtracted from the request's CPU, also from its deadline while the collection is under way, as far as its Worker's budget goes: a token bucket of `gc.budgetMs` (2,000 ms) refilled at `gc.refillMsPerSecond` (100 ms a second), one per Worker in each process, which the Worker's next isolate takes over. What goes beyond the budget is charged as before. A tenant that allocates on purpose thus gets at most a tenth of a core of collections the limit does not see -- measured, 87 ms a second over 45 s of back-to-back collections -- and a request that only runs JavaScript is terminated at the limit as before. A completed request's row in the event pipe carries the CPU left out (`gcExcludedNs`); a `terminated` row carries the CPU charged.
- **Long pauses named.** A collection that pauses its process for `gc.longPauseMs` (100 ms) or more writes a `gc-pause` row -- the isolate, the pause, the request it ran in (Worker, version, invocation) or none, the kind of collection, the CPU left out and what the budget held after it -- to `logs/workerd.log` after `[cpu-limit] ` and to the event pipe; the state endpoint keeps the latest rows (`processes.recentGcPauses`) and counts collections, long pauses and the longest pause per process (`processes.entries[].gc`).
- **Idle collections.** An isolate of a Worker idle for `gc.idleCollectAfterMs` (50 ms) whose old generation has grown `gc.idleCollectMinGrowthMiB` (4 MiB) since its last full collection is collected between its requests, under its own lock, so that its next full collection inside a request has little to do. Not while the process has work waiting: workerd's evictor wakes every 10 ms, a wake-up more than 10 ms late means the event loop was busy, and idle time is counted again from there. Measured with a tree of Workers (3 roots calling 6 children and 3 shared leaves) at 15 requests a second per root on an arm64 machine, every isolate's first full collection was such an idle one and paused its process less than 21 ms.
- **A small first old generation.** Every isolate starts with an old generation of `gc.initialOldSpaceMiB` (8 MiB; V8's `--initial-old-space-size`), so that its first full collection comes while it holds little.
- **The runtime's heap.** The runtime bridge's isolate -- not a tenant's, without a CPU limit -- has a heap of `gc.runtimeHeapMiB` (128 MiB) instead of one V8 sizes from the machine: four times the most it was measured to hold after a full collection (7.7 MiB, with 100 more calls in flight), and at least 128 MiB. Its own full collections, outside any request and never idle, are the longest pauses left: measured on an amd64 machine under the tree of Workers, 510 of its 518 full collections in an hour took 100 ms or more, half of those over 151 ms and the longest 357 ms.

**Why.** Charged to the request it ran in, a long-lived isolate's first full collection terminated well-behaved requests: with a tree of Workers at 15 requests a second per root on a node with four processes and no rotation, on an amd64 machine, 0.4.0 terminated 50 requests in an hour, each right after a full collection; 0.4.1 terminated none.

**Why 200 ms.** A process has one event loop, so the CPU limit is also the bound of how long one request can delay its neighbours: measured, neighbours in the same process wait at most the limit plus about 8 ms. The limits' own cost is about 0.03 ms per request.

## 5. Memory limits

- Each isolate may hold `memoryLimitMiB` (default 128, Cloudflare's published limit, or its Worker's or tenant's own) of JavaScript heap and external memory together; ArrayBuffers are counted by an allocator of the isolate's own.
- At the limit -- V8's near-heap-limit callback, a total over the limit after a garbage collection, or an ArrayBuffer allocation over the quota -- the running call is terminated and the isolate invalidated: `503 MEMORY_LIMIT_EXCEEDED` before the response started. The extra heap V8 is granted so that the termination can complete is at most 8 MiB above the limit.
- A single allocation too large for that makes V8 abort the process with a fatal out-of-memory error. workerd's handler first writes two rows it prepared before the JavaScript ran -- that an out-of-memory happened, and which call was executing -- to the event pipe; the node reads them after the exit and makes the culprit's tenant a suspect.
- When the kernel kills a process for memory, nobody can tell who caused it, and no tenant is marked. The node recognises such a kill by the `oom_kill` counter in its own cgroup's `memory.events` (cgroup version 2: the cgroup `/proc/self/cgroup` names, as under systemd, or the namespace's root in a container with a cgroup namespace of its own): a workerd process that exits by `SIGKILL` within 2 s of an increment the node saw takes it and counts as `memory-oom`; an increment no such exit takes was another process's of the cgroup and changes no exit's category (`processes.kernelOomKills` counts both). Without the file every such exit counts as `other`. With `memoryLimitMiB` at `0` -- which only `COWORKERD_MEMORY_LIMIT_MIB=0` sets, the configuration file refuses it -- no exit is classified as a memory one.
- WebAssembly memory is bounded by V8's process-wide limit instead, set for each generation to the largest memory limit named by any Worker of the deployment, or the node's own if that is larger.

The overruns that can be terminated (ArrayBuffers, a heap that grows) cost only the offending isolate. A single huge allocation costs one process crash the first time a tenant does it; after that the tenant is in quarantine.

## 6. Limits per Worker

A Worker can have its own CPU and memory limits, set through `/v1` within its tenant's maxima; without them its tenant's default limits apply, then the node's `cpuLimitMs` and `memoryLimitMiB`. The node passes the limits a Worker names with every call, and the runtime bridge puts them into the key it loads the Worker under, so each isolate is created under its own Worker's limits even in a shared process, and a change of limits loads a new isolate. The state endpoint reports the limits in effect for each Worker whose limits it or its tenant names (`tenants.tenants.<tenant>.workerLimits`, at most 256 per tenant) and the WebAssembly limit per process generation.

## 7. Culprits and quarantine

- **Two sources of suspicion, both certain**: the CPU culprit workerd named before the watchdog killed a stuck process, and the memory culprit it named before V8 aborted on memory. Nothing else marks anyone -- not a termination inside the isolate, whose harm is already bounded by the limit, and not a guess among the calls in flight. A CPU culprit is checked against the CPU limit in effect for its Worker, so it is found for a Worker that names a limit even when the node's `cpuLimitMs` is `0`; a memory culprit only while the node's `memoryLimitMiB` is not `0`.
- **Suspects are tenants.** Once any Worker of a tenant is a culprit, all of that tenant's Workers run only in quarantine processes, in the order of the tenant's rendezvous hash over them: normally all of a suspect's calls go to one quarantine process, its shard, and only while that shard cannot take calls do they go to the next one. The tenant's other Workers follow it by design: the protection is for the other tenants.
- **The suspicion period** is `suspectBaseMs` (10 minutes), doubled when the tenant offends again within it, up to `suspectMaxMs` (1 hour); then the tenant returns to the ordinary processes.
- **Shared by the cluster.** The node that identified a culprit applies the mark at once and writes its marks as one object beside its heartbeat in the object store, `members/<nodeId>/<epoch>.suspects`; every node reads the other members' objects in its membership listing, each only when it changed, and keeps every mark until the deadline the mark carries ([cluster state](cluster-state.md), section 9); a node without the object store's membership -- the self-check, or the file membership of the command line's test form -- keeps its marks to itself. Without the store, known marks keep their deadlines and new ones apply locally until they can be written.
- **Never an ordinary process, once a node knows the mark.** When no quarantine process of a node can take a suspect's call, the node refuses it (`QUARANTINE_UNAVAILABLE`, not executed), and where placement decides, it tries another node. The node that identified the culprit applies the mark at its next call; the others learn it at their next membership listing, within `membership.listIntervalMs` (10 s) -- measured, 6 s -- and not at all while it cannot be written to the store; calls already running in an ordinary process are not moved.
- **Trial loads run in quarantine processes too**, in the tenant's order, so a new version that crashes a process during its deployment takes no ordinary tenant's requests with it. The load check before a version's first call in a process runs in the process the call will use -- an ordinary one for a tenant that is not a suspect.

`quarantineProcesses` `0` turns quarantine off: suspicions are still recorded but change nothing, and trial loads use the ordinary processes.

## 8. Eviction and the memory watermark

Idle collections (section 4) run in the same loop of workerd's evictor.

**Idle eviction** is workerd's (a patch): a Worker loaded by the Worker Loader is removed once it has no request in flight -- streaming responses and JS RPC included -- no `waitUntil` work, and has been idle for `idleEvictMs` (5 minutes). Isolates are removed one at a time, at least `idleDestroyIntervalMs` (10 ms) after the previous one was destroyed, so a batch does not stall the neighbours; destroying one takes about 0.3 ms. A Worker whose load failed is removed at once, so a failure is never permanent. The threshold weighs a cold start, a few milliseconds, against about 2 MB per resident Worker; it must be `0` or longer than `placement.idleMs`.

**The memory watermark** is the node's, on Linux: every 100 ms it reads its cgroup's working set (`memory.current` minus `inactive_file`, as Kubernetes counts it) against `memory.max`, or against `memoryBudgetBytes` when the container has no limit, and sets a band; with neither, the watermark is off. Red is from 90 %, yellow from 75 %, back to green below 65 %, and red steps down to yellow below 75 %. workerd's evictor reads the band from shared memory.

| Band | Eviction | Placement |
| --- | --- | --- |
| Green | Idle for `idleEvictMs` | Normal |
| Yellow | The least recently used idle Worker first, idle at least 5 s, one at a time | Forwarded and nested calls for versions that have not run on this node within `placement.idleMs` are refused |
| Red | The same, idle at least 1 s | All forwarded and nested calls are refused |

A request executing on the node that received it is never refused by the band. Measured on three containers limited to 2 GiB with a stream of new Workers, in ten-minute runs with the watermark on and off three times each: with it, no call failed, no process was killed for memory, and the working set moved between 65 % and 75 % of the limit; without it, 1,120 calls failed and the kernel killed processes 154 times in all.

## 9. Process rotation

A slot is replaced by a new generation in steps, at most one rotation at a time on a node:

1. start the new generation next to the old one and wait until it is ready;
2. switch dispatch: new calls go to the new generation, calls already dispatched stay where they are; the old generation's WebSocket connections are closed with `1001` (going away) at once, so that their clients reconnect to the new one instead of being cut when the old one exits, and the rotation's record counts them (`webSocketsClosed`);
3. drain the old generation until its calls and request contexts -- `waitUntil` work included -- have ended, for at most `rotation.drainTimeoutMs` (30 s); what is still running then is killed and ends as `RESULT_UNCERTAIN`;
4. close the node's connections to it, send SIGTERM, and SIGKILL after `rotation.termGraceMs` (5 s).

**Triggers**: the content of the workerd file changes (checked every `rotation.binaryCheckMs`, 1 s); by hand (`POST /__coworkerd/rotate`, `coworkerd-apiserver tool rotate`); a generation's resident memory reaches `rotation.residentBytes` -- by default 70% of the node's memory: its container's `memory.max`, else `memoryBudgetBytes`, else the machine's, which the state endpoint names (`processes.rotation.configuration.residentBytesSource`); and, both off by default, a generation ready for `rotation.maxAgeMs` or one that took `rotation.maxRequests` calls. A failed automatic rotation is retried after `rotation.retryMs` (5 s), doubling. Quarantine processes rotate too; replacing the launcher does not trigger a rotation. A request that lasts longer than the drain period can be cut by a rotation.

**Why not every hour.** Up to 0.4.0 every process was replaced every hour, because its resident memory kept growing between rotations. The collections of section 4 keep it level -- measured, an hour of the tree of Workers at 15 requests a second per root without rotation on an amd64 machine: the node's processes together rose to 891 MiB in the first 40 minutes and to 894 MiB by the end (0.4.0's, beside them, to 3,205 MiB) -- while a new generation only rebuilt every isolate, with its cold loads and its first, largest collection, and cut the requests longer than the drain. The memory threshold stays as the bound. Rotation is also how workerd is upgraded on a running node; measured, rotating a node's slots under load failed no request.

## 10. Logs

- **The node's own output** goes through a shared-memory ring of 4 MiB to a separate writer process, `coworkerd-log`, which writes it to the node's original standard output and error (the journal, or `docker compose logs`), so a slow receiver never makes the event loop wait. When the ring is full, lines are dropped and counted, and a `[log-dropped]` line reports them; lines already in the ring are written even after the node is killed.
- **workerd's output** goes through pipes to the reader process `coworkerd-wlog`, which keeps a backlog of 4 MiB per process and drops whole lines beyond it, counted and reported; workerd never waits for a slow disk. It writes `logs/workerd.log`, renamed at 256 MiB, with four files kept.
- **Tenants' logs** are kept for `cowctl logs` in the reader's memory, per tenant. A Worker's `console.*` lines leave its isolate over the tail channel workerd gives each loaded Worker, under that Worker's own identity: the runtime's tail entry posts them to the node over a socket only the runtime can reach (`POST /v1/log`), and the node stores them in the tenant's buffer under the Worker and version the channel names. Nothing tenant code can read or write decides whose lines they are: whatever a tenant writes counts as its own. A call's lines are delivered when the call ends -- a WebSocket connection's when it closes -- (workerd caps one call's trace at 256 KB and says so in the last line).

## 11. The channels between a node and its processes

workerd listens on no TCP port, has no network beyond what the sandbox leaves it (section 12) and opens no file of the node after its first request; what it shares with its node is handed over when it starts:

| Descriptor or path | Carries |
| --- | --- |
| 1, 2 | Standard output and error: pipes to `coworkerd-wlog` |
| 3 | The event pipe: limit events, culprits, out-of-memory identities and the end of every request, one `write()` per row |
| 4 | The process's listening Unix socket, `run/<slot>-<generation>.sock`, for calls and probes |
| 5 | Shared memory with the snapshot of the process's resident Workers, written double-buffered |
| 6 | Shared memory with the memory band, sealed against resizing (only with the watermark on) |
| `run/binding.sock`, `run/source.sock` | The runtime bridge's calls back into the node: Bindings; the code of a version, and the tenants' log lines (`POST /v1/log`) |
| `run/egress.sock` | Tenant code's outgoing requests, to the node's egress policy point |

`run/` is readable only by the node's user. The end of every request is written whether or not it used CPU, so draining knows when a generation is idle, and at an exit the node reads the event pipe to its end, so the evidence of a crash is not lost.

## 12. The launcher's environment

While the sandbox is active, the node starts every workerd through `coworkerd-launcher` ([its protocol](../launcher/PROTOCOL.md)), which sets up the following and then executes workerd:

- one `unshare` for all its namespaces: a **user namespace** in which the process is uid and gid 65534, mapped to the node's own user outside, and the mount, network, IPC and UTS namespaces;
- then a **root** that is an empty read-only tmpfs on `<stack>/sandbox` with only what workerd needs bound in: read-only the workerd file (checked to be the very file the node opened), its ELF interpreter and libraries, `/dev/urandom`, `processes/`, the steady-state table when it is in use and, when present, `/sys/devices/system/cpu` and `/etc/localtime`; read-write `/dev/null` and `run/`;
- in the **network** namespace no route and only a loopback device, which is down (a kernel with tunnel drivers loaded adds their inert devices);
- **no capabilities** and `no_new_privs`;
- a **parent-death signal**, so workerd dies with its node;
- the **system call allowlist**, last (section 13).

The node reads `/proc/<pid>` of every process once it is ready and reports the layers it observes, not the ones the launcher claims (`processes.entries[].sandbox.observed.layers`). A launch that fails exits with status 125 and a record naming the failing step; the slot is restarted with its usual backoff and the exit counted as `launch-failed`, not as a workerd crash.

`sandbox.mode` decides what happens when the host cannot provide this: `auto` (the default) uses the launcher when its startup check passes and otherwise starts workerd directly, with a warning; `launcher` refuses to start; `off` never uses it. The host must allow unprivileged user namespaces; Docker's default seccomp and AppArmor profiles do not, so the default compose cluster runs without the sandbox and says so. Setting up the environment adds a few milliseconds to every process start.

## 13. The system call allowlist

- **Generated by the build.** `scripts/syscall-allowlist.py` derives the tables from profiles of this very workerd binary under load -- every call seen during startup goes into the startup table, every call seen afterwards into both -- plus calls always allowed with a recorded reason. Changing the compiler, the build options or the upstream version can change the set, so it is never written by hand. A table names the workerd binaries it belongs to by their SHA-256.
- **Two tables.** The launcher installs the **startup table** as its last step before executing workerd, so it must allow what startup needs, `execve` among it -- a seccomp filter can only ever be narrowed. When it answers its first request, the node's readiness probe, workerd installs the narrower **steady-state table** itself and, in the same step, makes itself non-dumpable, which the launcher's setting cannot achieve because `execve` resets it. On arm64 the tables have 59 and 43 calls.
- **Three modes** for each table: `off`; `observe`, in which a call outside the table is reported and then allowed -- only a call of another ABI (i386 or x32), which workerd itself never makes, still ends the process --; `enforce`, in which such a call kills the process (exit category `syscall-denied`) and its requests in flight fail. The startup table's reports reach the node through the kernel's user notification: they appear in `processes.sandbox.syscalls` -- counted exactly, per call number; of a burst of more than 4096 between two of the node's reads only the details (the thread of each) are dropped, and counted as `detailsDropped` -- and as `[syscall-observed]` lines, at most one per call number every 10 seconds. The steady table's reports are the process's own `[coworkerd-seccomp]` lines in `logs/workerd.log`, one for the first occurrence of each call; the state endpoint does not count them. In `enforce` the steady table is installed on all threads of the process; in `observe` only on the thread that installs it and the threads it starts afterwards.
- **Defaults**: `observe` for the startup table (`sandbox.syscalls.mode`) and `off` for the steady-state table (`sandbox.syscalls.steady.mode`). A table sampled from windows of activity may miss a call only some load reaches, so switching to `enforce` should follow a period of `observe` under real traffic without a report.
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

- **Between isolates in one process.** Isolates of different tenants share a process, its address space and V8's pointer-compression cage; V8's own sandbox is not part of the patch series. The V8 isolate is the boundary between tenants there, and the limits bound how much CPU and memory one can take from the others, but a flaw in V8 is a flaw between tenants. This is the trade Cloudflare's platform makes too: the measures in this document make attacks expensive and short-lived, they do not make an isolate a process.
- **Between processes of a node.** All workerd processes of a node map to the node's own user outside, and the launcher creates no PID namespace. Against one process reading another's memory there are three measures: the host's `kernel.yama.ptrace_scope` of at least 1, which the installation guide asks for (nothing checks it); for each process started through the launcher, non-dumpability from its first request -- the node's readiness probe, before it has answered anything; and the system call allowlist, which names neither `ptrace` nor `process_vm_readv` (a call to either is killed in `enforce` and reported in the default `observe`). Without the sandbox only the first is there. All of them also share the node's `run/`, bound read-write: code that escaped V8 could replace the node's callback sockets there or connect to another process's socket. Tenants are not given processes of their own, except that suspects are confined to quarantine processes.
- **The node's environment.** A workerd process inherits only a few variables of the node's environment -- the locale, the time zone, the search path, the temporary directory, output colouring and switches workerd itself reads for tests -- besides the settings the node gives it; credentials the node was given as environment variables, such as the management token or object storage keys, stay with the node.
- **Between workerd and the host.** With the sandbox active, a workerd process has no network, no capabilities, a view of a few files, read-only except `/dev/null` and `run/`, and -- unless `sandbox.syscalls.mode` is `off` or, in `auto`, the table could not be used for the running workerd -- a filter on its system calls, which in `enforce` ends the process on a call outside the table and in `observe`, the default, reports it; a call of another ABI ends the process in both modes. When the host does not allow the sandbox and `sandbox.mode` is `auto`, workerd runs without any of these layers, with the node's own network: tenant code then still reaches the network only through the egress policy point, because that is how its workerd is configured, not because the process could not.
- **Tenants' logs.** Attribution comes from workerd's per-Worker tail channel, so a tenant can neither write lines kept as another tenant's nor read another tenant's lines. Reading them is guarded by credentials: `/v1` by the tenant's or the administrator's, a node's `/__coworkerd/logs` by the management token -- without one, it answers any Worker's lines to whoever reaches the node's ingress, and the node only warns at startup.
- **The node itself** is not sandboxed: it runs as an ordinary user with network access to object storage and its peers, holds its own object storage identity and the cluster's secrets key, and it is what tenant code would have to break out to reach.

# Process isolation and resource governance

> This is the design record of process isolation, in the order the mechanisms were built: each section states the product as it was when its mechanism was added, and later sections refine earlier ones. The current configuration keys and defaults are in the [installation guide](guide/install.md).
>
> Inside one Server: how many workerd processes to run, which process a call goes to, and how to limit the damage when something goes wrong.

It builds on the conclusions of the isolation research (section 1) and the lifecycle research (sections 10-11), and on the membership, deployment state, code bundles and placement of [cluster state](cluster-state.md).

## 1. Why process isolation

The isolation research produced three hard conclusions:

1. **Terminating a request does not work for long builtins**. Every CPU and memory limit rests on "being able to terminate in time",
   and that premise does not hold for a whole class of builtins, which cannot be enumerated and patched. **The only certain means of enforcement is killing the process that hosts it**;
2. **Threads are a blast-radius parameter, not a boundary**. N threads only raise the bar for an attack from 1 tenant identity to N;
3. **Stock workerd provides no per-isolate limits** (`NullIsolateLimitEnforcer`). workerd's dynamic loading interface already carries `limits` (`cpuMs`, `subRequests`), but stock workerd's limit enforcer is an empty implementation,
   so the enforcer has to be our own (a workerd patch).

With **one workerd process per Server**, started by the container script and only connected to by the server, which can neither supervise it nor kill it,
any one tenant's long builtin or endless loop stalls execution on the whole Server, and a workerd crash makes the whole Server unavailable
until something outside restarts it. The cluster level can reroute calls to other Servers, but **within one machine the blast radius is the whole machine**.

The isolation research also said what one machine should look like:

| Question | The research's answer |
| --- | --- |
| How many workerd processes a Server runs | Several, **not divided by tenant**: by capacity and fault domain, plus a **quarantine pool** that takes suspicious Workers |
| The unit of enforcement | **The process**. Threads reduce accidental damage; processes are the boundary against attacks |
| The key unmeasured quantity | The unavailability a malicious tenant causes ≈ **detection + killing the process + rebuilding** -- "the only quantity that matters next" |

## 2. Overview

The mechanisms build on each other. The process pool and the watchdog (sections 3-5) start, supervise and when necessary kill several workerd processes, with a second level of placement that chooses a process for each call.
CPU limits (sections 6-7) handle ordinary endless loops, which are most cases, inside the isolate; when termination fails, workerd reports the culprit from the inside.
Quarantine processes (sections 8-9) take the Workers identified as culprits, so that they stop repeatedly dragging down neighbours in the same process.
Resource governance follows: idle eviction (sections 10-11), the memory watermark (sections 13-14), the memory limit of a single tenant (sections 15-16) and process rotation (sections 23-24).
Sections 12 and 17-22 keep logging and the limits' bookkeeping from slowing requests down; sections 25-26 reduce the channels between the server and workerd to descriptors handed over at startup,
and sections 27, 29 and 30 run workerd in a least-privilege environment; section 28 has the findings of a 24-hour soak.

From the limits on, patching workerd is almost unavoidable. The patches follow the usual constraints: thin, without touching V8's sources, `src/workerd/jsg/` or `IoContext`.

## 3. The process pool and the watchdog: design

### The process pool

- **The server starts N workerd processes** (N configurable) and generates a configuration for each: its own ingress port; the two services that call back into the server (Binding, code) are shared;
- it watches each process's exit; after a crash it restarts with backoff; it dispatches to a process only once that is ready;
- **a single product form**: the container starts only the server; there is no mode that connects to an external workerd. With N = 1 the call semantics are those of a single workerd process per Server;
- process identity = number + generation (incremented at each restart), written into call records and logs;
- when the server exits normally, all child processes end with it, leaving no orphans.

### The second level of placement: choosing a process

- The same rules as the first level: **choose each Worker's "home process" among the live processes by rendezvous hashing**; when it is unavailable (not ready, dispatch paused, over the per-process in-flight limit), take the next one in order;
- the reason: keep a Worker's isolates in one process as far as possible (paying for the memory once), and when a process has a problem, move only the Workers that belong to it;
- ingress requests, local Bindings and calls forwarded from peers all choose a process through the second level;
- when the first level judges "hot locally", being hot on any live process counts as hot locally.

### Watchdog: a stuck process has to be found and killed

- Each process is probed periodically: a probe **must pass through that process's event loop** to get an answer;
- two thresholds:
  - **probe overdue** -> **pause** dispatching new calls to it. Switching to another process before dispatch is safe -- nothing has executed yet;
  - **stuck timeout** -> **kill the process** (SIGKILL) -> restart;
- pausing rather than killing avoids killing by mistake: long but legitimate CPU work only stops the process from taking orders for a short while. Without CPU limits (section 6),
  the stuck timeout is the only safety net, and its value needs a stated reason.

Expected effect: neighbours in the same process lose **the calls already dispatched to that process at the moment it got stuck** (result uncertain); new calls after that go to other processes once "probe overdue" is reached.
Their window of unavailability is about "the probe-overdue threshold + a cold start on another process", not the whole stuck period. Section 5 verifies this by measurement.

### Failure semantics

| Situation | Handling |
| --- | --- |
| The process is found unavailable before dispatch | Switch processes (certainly not executed) |
| The process crashes or is killed after dispatch | `RESULT_UNCERTAIN`, no retry |
| All of the local processes are unavailable | Local admission refuses (an explicit refusal), and the first level reroutes to another Server; ingress requests get 503 |

### Recording possible culprits

When a process is killed, record the calls in flight on it at the time (Worker, call tree, start time). The process pool by itself **neither attributes nor acts**, because the server cannot see
which isolate the event loop is executing at that moment: the in-flight set mixes in innocent neighbours queued behind the culprit. Reliable attribution needs information from inside workerd (section 6).

**Known consequence**: a deliberate tenant can repeatedly make its home process get stuck, dragging in its neighbours in the same process every time. Section 5 measures the cost of that "repeatedly"; culprits and quarantine processes (sections 6-9) address it.

### The key numbers to measure

- **Blast radius**: only the Workers belonging to the stuck process are affected, and the Workers on the other processes not at all;
- **unavailability**: for neighbours in the same process, the interval from getting stuck to the next successful service; and the number of calls in flight at the moment it got stuck, hence lost;
- **controls**: N = 1 (all tenants of the machine affected); the watchdog off (neighbours in the same process stuck for the whole stuck period).

## 4. The placement path's overhead

In the placement of [cluster state](cluster-state.md), the static and the dynamic mode execute in the same place, yet the dynamic mode's p50 was about 1.5 ms higher, while the placement decision itself takes only about 3.5 µs.
The path is profiled in segments, to be fixed if it is an implementation problem; the result is item 4 of "Problems found" in section 5.

## 5. The process pool and the watchdog: results

**The blast radius holds: only the Workers belonging to the stuck process are affected.** Each Server has 4 workerd processes, and one tenant makes its home process get stuck;
in every scenario, the Workers belonging to other processes have zero failures.

**Unavailability** (neighbours in the same process: from getting stuck until it is first served successfully):

| Scenario | Unavailability of neighbours in the same process | Calls lost by neighbours in the same process (result uncertain) | Workers on other processes |
| --- | --- | --- | --- |
| **4 processes + watchdog** (3 runs) | **512-514 ms** | 4 each time | Zero failures, maximum latency 7.5-28.8 ms |
| Control: 1 process (3 runs) | 3,234-3,239 ms | 9-10 each time; between the pause and the rebuild another 54-55 requests immediately got 503 `PROCESS_UNAVAILABLE` | None (the machine has only this one process) |
| Control: watchdog off (3 runs) | 9,606-9,663 ms | 0 (all waited until the stuck period ended) | Zero failures |
| Process crash (3 runs) | 149-153 ms | 1 each time (executing at the time of the crash) | Zero failures |
| Endless loop (1 run) | 527 ms | 4 | Zero failures |
| Legitimate long CPU work (3 runs) | 483-498 ms | 0: dispatch was only paused, nothing killed, resumed afterwards | Zero failures |

Timeline (4 processes + watchdog): dispatch paused 422-450 ms after getting stuck (the 400 ms overdue threshold plus the probe interval), the process killed at 3,025-3,057 ms (the 3,000 ms stuck timeout),
the new generation ready at 3,182-3,219 ms (145-151 ms from exit to ready).

**A difference from the design's expectation: what is lost is not only "the calls already dispatched at the moment it got stuck".** From getting stuck to pausing dispatch there is a detection window of about 400 ms, during which newly arriving calls
that belong to the process are dispatched to it as usual, get stuck the same way, and about 3 seconds later receive "result uncertain" when the process is killed. With 4 processes, 4 were lost each time: 1 already executing when it got stuck,
3 dispatched within the window; in the 1-process control, 8-9 were dispatched within the window. The loss ≈ the calls in flight when it got stuck + arrival rate × detection window, and all these calls wait about 3 seconds for their failure.
The precondition for shortening the window is item 2 of "Problems found" below.

**The dispatch commit point is correct**: choosing a process completes before sending, and failing to choose one is handled as an explicit refusal (reroutable);
from the moment `startEvent` is sent the call counts as dispatched, and any later disconnect or process exit is "result uncertain", without retry:

- The process was known to have exited before dispatch (3 runs): explicit refusal, rerouted to another machine, B executed exactly once;
- the process was killed after dispatch (3 runs): `RESULT_UNCERTAIN`, B executed exactly once, not rerouted;
- an unsynchronized race (30 runs): 15 successes, 15 uncertain results, **B executed at most once**.

**Other properties of the process pool**: each process takes 56-173 ms from start to ready; after the server receives SIGTERM, no child processes remain within 52-99 ms;
of 100 synthetic Workers, when one process exits exactly the 28 under its name change their home process and the rest do not move at all, and all return once the process is rebuilt;
the per-process in-flight limit takes effect; when all local processes are unavailable, ingress requests get 503 with zero executions, and nested calls are explicitly refused and rerouted by the first level to another machine.

**Epochs across an etcd outage longer than the TTL** (after it, the same process registers again with a new epoch), 30 repetitions each:

| Epoch check | Failed repetitions | Failed calls |
| --- | --- | --- |
| Accepting only the target's current epoch | 10 / 30 | 32 (concentrated in the recovery phase) |
| Accepting any epoch the same process has held | **0 / 30** | **0** (6,619 requests in all) |

The check is not too permissive: after the same process re-registers with a new epoch, calls carrying the old epoch are accepted as usual (30 / 30); after the target process **restarts**,
calls carrying the old epoch are explicitly refused and rerouted (30 / 30).

**Probes and process identity**: a tenant's own `/__coworkerd/probe` path is not intercepted as a probe; a process's identity comes from the process's own configuration, not from the call description,
so a forged description cannot impersonate another process.

#### Problems found

1. **The cost of repeated stalls**: the same tenant made its home process get stuck every 5 seconds, and it was killed 12 times in 60 seconds; neighbours in the same process were unavailable for 517-548 ms each time,
   6.4 seconds in all (about 11%), with a request success rate of 90.6% (36 of 384 failed). Without attribution and quarantine this can go on forever -- which is what culprits and quarantine processes (sections 6-9) are for;
2. **Healthy processes' probe latency reached 248 ms** (in the etcd outage scenario). An overdue threshold of 150 ms caused mistaken pauses and
   2 failed calls, so the threshold is 400 ms. The margin is only about 150 ms, and the source of the latency was not found here (the server's event loop being busy, or the workerd side).
   Until it is found, the threshold cannot be lowered to shorten the detection window; section 7 traces it to writing logs to a host directory;
3. **No more probes after a probe fails fast**: when a probe does not time out but fails immediately (for example the connection is reset), the next one is never sent, so a healthy process would also be
   paused after 400 ms and killed after 3 seconds. In none of the measurements without an injected stall did a pause or kill occur even once, but it is a latent point of mistaken killing.
   Section 6 fixes it: keep probing after an error, still timing from the first unanswered probe;
4. **The placement path's overhead was not located**: the roughly 1.5 ms of section 4 measured 0.96 ms here with the binary before the process pool and 0.67 ms with it, the latter within the static mode's own variation between measurements
   (1.94 ms); the placement decision itself takes 3-4 µs. Segment-by-segment profiling found no single source; it is left for separate performance work;
5. small items: each process's table of "hot versions" only grows (idle eviction trims it by the heat window, section 10); a process's readiness identity is
   "number:generation", and the generation restarts from 1 when the server restarts -- in a container the server is PID 1, and when it exits all child processes go with it, so no leftover process can impersonate it;
   outside a container this premise does not hold.

## 6. CPU limits and culprits: design

### What has to be solved

With the process pool and the watchdog alone, any tenant's endless loop makes its process stop taking orders and get killed whole after 3 seconds, with neighbours in the same process losing about 4 calls each time, and it can repeat forever
(12 times in 60 seconds, section 5). The isolation research measured: **CPU limits work on ordinary JS loops** (a 50 ms budget terminates at 51-60 ms),
**and do not work on long builtins** (`replace`, `sort`, `normalize` and the like do not check the interrupt flag; the CPU overshoot can reach over two hundred times the budget). So two levels:

| Situation | Handling |
| --- | --- |
| Ordinary JS running too long (most cases) | **CPU limit**: terminate this one request inside the isolate; the process does not die, and neighbours in the same process wait at most one budget longer |
| Long builtins and others that cannot be terminated | The watchdog kills the process as before (section 3); but **it knows who it was before killing** -- workerd reports, from inside the process, the call over the limit at this moment |

### CPU limits

- A workerd patch implements the limit enforcer for dynamically loaded Workers (stock workerd has an empty implementation): it counts the isolate thread's CPU time per request, and asks V8 to terminate the current execution when the limit is exceeded;
- it applies only to tenants (dynamically loaded Workers), not to our own runtime;
- a terminated request: the caller gets an explicit `CPU_LIMIT_EXCEEDED`, **not rerouted** (it really did execute); when the response has already started, the response is aborted;
- **an isolate that has been terminated serves no new requests**; the next request loads it again -- a termination may leave the isolate in an inconsistent state;
- the limit's value is configured by the server and passed as a startup parameter of the workerd process; all tenants in one process share one value (per-Worker customization could correspond to wrangler's `limits.cpu_ms`; see section 30); 0 turns it off;
- **the default is a product decision**: with a single event loop, the CPU budget is also the upper bound of the latency of neighbours in the same process (in the isolation research, a 50 ms budget gave neighbours a maximum latency of 54 ms).
  Section 7 measures the three levels 50, 200 and 1000 ms; **the default is 200 ms**.

### Culprits

- When a request's CPU time exceeds the limit and the termination does not take effect within a grace period, workerd (the patch) writes this request (Worker, version, call id) somewhere the server can read --
  **not through the event loop**, because the event loop is occupied at that moment;
- when the server's watchdog decides to kill the process, it writes this into the kill record as **the definite culprit** (the process pool alone could only list all calls in flight, mixed with innocent neighbours);
- the CPU limit itself only records: per-Worker counts of terminations and of being identified as the culprit, in the state endpoint; **it makes no placement decisions based on them** (quarantine processes do, section 8).

### Two fixes to the watchdog

- When a probe fails immediately, keep probing, still timing from the first unanswered probe (otherwise one sporadic connection error gets a healthy process killed after 3 seconds);
- with logs written to the container's local disk, measure the healthy processes' probe latency again: section 5 measured up to 248 ms in the etcd outage scenario with logs written to a host directory, which slows down the whole event loop.

### Constraints on the patch

The usual constraints hold: a thin patch; no touching V8's sources, `src/workerd/jsg/` or `IoContext`. The patch is `patches/workerd/0002-cpu-limit.patch`; the production workerd
is built from upstream plus the patch series (see the [patch notes](../patches/README.md)).

## 7. CPU limits and culprits: results

**What can be terminated is terminated inside the isolate.** Three limit levels, a finite loop and `while (true)` three times each:

| Limit | CPU used at termination (p50 / max) | Maximum added latency of neighbours in the same process (p50 / max) |
| --- | --- | --- |
| 50 ms | 51.6 / 53.5 ms | 58.7 / 61.9 ms |
| 200 ms | 201.9 / 203.0 ms | 208.0 / 211.9 ms |
| 1000 ms | 1001.6 / 1003.1 ms | 1007.4 / 1009.5 ms |

- The caller gets `CPU_LIMIT_EXCEEDED`, not rerouted; **the process is not killed**; neighbours in the same process have zero failures and other processes are not affected at all; the tenant's next request is executed by a reloaded isolate;
- boundaries: under a 200 ms limit, a legitimate request of about 150 ms completes normally and one of about 250 ms is terminated; the time accumulates across `await`; with the limit off it falls back to the watchdog's "pause, kill the process after 3 seconds";
- these numbers are what the isolation research called "the CPU budget is the upper bound of neighbours' latency": neighbours in the same process wait at most "the limit + about 8 ms" longer. **The default is 200 ms**.

**Repeated episodes** (once every 5 seconds for 60 seconds):

| | Process killed | Success rate of neighbours in the same process |
| --- | --- | --- |
| A terminable endless loop, 200 ms limit | **0 times** | **100%** (293 / 293); 90.6% with the watchdog alone (section 5) |
| A long builtin that cannot be terminated | 12 times | 91.2% (268 / 294), similar to the watchdog alone; **all 12 identified the same Worker** |

**What cannot be terminated: identifying the culprit.** A 128 MiB `replace`: the 200 ms limit's termination does not take effect, and the watchdog kills the process at about 3.0 seconds; in 10 runs, the culprit in the kill record
was each time exactly that tenant's call, and none of the ordinary calls in flight was misidentified; the new generation was ready 0.17-0.19 seconds after the kill. About 10 calls in the same process failed each time (including the one already executing when it got stuck).

**The watchdog fixes.** 30 trials of probes failing immediately (640 failed probes in all): zero pauses, zero mistaken kills. With logs written to the container's local disk, healthy processes' probe latency in the etcd outage scenario was
p50 0.49 ms, p99 7.5 ms, at most 41.5 ms -- **the 248 ms of section 5 was caused by writing logs**, and the 400 ms overdue threshold does not need to change.

**Overhead.** With the limit on (200 ms), p50 is about 0.12 ms higher (beyond the 0.05 ms variation between measurements), and p99 cannot be told apart. The four ecosystem projects of [ecosystem compatibility](ecosystem-compat.md) still answer the same as under `wrangler dev` with the limit on.

**Code fetching closes its connection.** With the same tenant terminated every 5 seconds, from the 9th time on this tenant was permanently unavailable in that process ("connection already closed").
Root cause: the runtime fetches code over an internal HTTP connection, and both the KJ client's idle reuse and the server's idle expiry are 5 seconds, so requests exactly 5 seconds apart hit a race with the connection being closed;
and the failed load stayed in the loader's cache. The fix: the internal code-fetching response carries `Connection: close`. After the fix, 16 cold loads 5 seconds apart all succeeded.

**The patch** changes only three files under `src/workerd/server/` (`server.c++`, `server.h`, `workerd.c++`) and does not touch V8, jsg or `IoContext`.

#### Problems found

1. **A failed load stays in the cache**: this trigger is fixed, but a failed load for any reason makes that Worker permanently unavailable in that process until the process restarts;
   reloading after a limit termination makes it easier to trigger. Idle eviction removes failed load entries (sections 10-11);
2. **The limit enforcer's overhead can go down further**: each entry into JS creates a new monitoring thread, and each request appends one line to an event file at its end; this is where most of the roughly 0.12 ms added to p50 goes.
   Sections 19-22 open the event file once per process and replace the per-entry threads with one resident watchdog thread;
3. In stalls that cannot be terminated, about 10 calls in the same process fail each time, more than for a busy loop with the limit off (about 5); the reason was not analyzed. With quarantine processes (sections 8-9),
   such losses fall only among suspicious Workers.

## 8. Quarantine processes: design

### What has to be solved

With CPU limits, an endless loop that can be terminated harms only itself; but **one that cannot be terminated** (a long builtin) still makes its process get stuck and killed, each time taking about 10 calls in the same process down with it,
and the same tenant can repeat that forever (section 7 measured 12 times in 60 seconds, a success rate of 91.2% for neighbours in the same process). The culprit can already be identified before the kill (section 7);
quarantine uses that to show it out: **an identified Worker from then on executes only in quarantine processes**, so when it gets stuck again it harms only the other suspicious Workers in the quarantine processes.

### Quarantine processes

- Besides the N ordinary processes, each Server has K **quarantine processes** (default K = 1, configurable; K = 0 turns the feature off);
- **ordinary Workers do not enter quarantine processes, and suspicious Workers do not enter ordinary processes during their suspicion period**. The second level of placement (choosing a process) first partitions by "suspicious or not", then uses rendezvous hashing within the partition;
- when the first level judges "hot locally", a suspicious Worker looks only at its heat on quarantine processes;
- when all of the local quarantine processes are unavailable, a suspicious Worker's call is explicitly refused locally (`QUARANTINE_UNAVAILABLE`, certainly not executed, reroutable) -- rerouted to another Server,
  it again enters only the quarantine processes there, **never any ordinary process**.

### Identifying and clearing suspects

- **There is only one source of identification**: the culprit workerd reports from inside when the watchdog kills a process, as described in section 6. When nobody can be identified (no report), nobody is marked -- no guessing among the calls in flight;
- being terminated by the CPU limit does not make a suspect: that harm is already bounded by the limit (neighbours in the same process wait at most one limit longer);
- **marked per Worker, not per version**: switching to another version does not escape the quarantine;
- **the suspicion period**: 10 minutes by default; when identified again within the period, the period doubles (at most 1 hour); when it expires without another offence, the mark is cleared automatically and the Worker returns to ordinary processes;
- calls already executing in ordinary processes are not migrated; the isolates it loaded in ordinary processes are not actively destroyed either (eviction and process rotation are separate mechanisms, sections 10 and 23).

### Shared across the cluster

- Quarantine on the local machine alone is not enough: ingress placement forwards requests to the Home, and after the Home refuses they come back to execute on the machine that received the request -- if the other machines do not know it is a suspect, they will put it into an ordinary process;
- so the suspect state is written into etcd (`/coworkerd/suspects/<workerId>`, the suspicion period implemented with an etcd lease), and every machine watches it; **the machine that identified it applies it locally at once**, without waiting for etcd;
- when etcd is unavailable: known suspects stay in force (statically stable, the same principle as for membership and the deployment state); new identifications take effect locally first and are written once etcd recovers.

### Not included

- A circuit breaker (pausing all of a suspicious Worker's calls when it keeps getting stuck in the quarantine processes); quarantine per tenant rather than per Worker;
- separate resource quotas for the quarantine processes (for example a smaller CPU limit).

## 9. Quarantine processes: results

**Repeated stalls are contained in the quarantine processes** (a long builtin that cannot be terminated, once every 5 seconds for 60 seconds; 4 ordinary processes per machine, plus 1 quarantine process where quarantine is on; three runs each):

| | Processes killed | Success rate of all ordinary tenants after the first time |
| --- | --- | --- |
| **With a quarantine process** | The 1st kill hits an ordinary process, **the following 11 only the quarantine process** | **100%** (3,258 requests over the three runs) |
| Control (without quarantine processes) | All 12 kills hit ordinary processes | 97.1%-98.1% |

In both groups the culprit was correctly identified as the same Worker every time. All 22 executions of that Worker after it was identified were in the quarantine process.

**Every call path keeps "zero executions in ordinary processes during the suspicion period"**: ingress, local Binding, ingress and Binding forwarded from peers, and local execution after the Home refuses -- all five paths were covered,
with 0 executions on ordinary processes. On the machine that identified it, the mark takes effect with the next call (about 36 ms); on the other machines it takes effect within 0.12-0.16 seconds through the etcd watch.

**Other items**:

- Quarantine processes unavailable: the suspicious Worker's calls are explicitly refused (`QUARANTINE_UNAVAILABLE`) and rerouted to execute in another machine's quarantine process; with all three unavailable, zero executions; ordinary tenants have zero failures;
- after the suspicion period expires it returns to ordinary processes (the next call 24 ms after expiry executes in an ordinary process); a repeat offence within the period doubles it;
- etcd outage: ordinary calls have zero failures; a new stall takes effect locally at once; after recovery the remaining time is written and the other machines follow;
- with two suspicious Workers in the quarantine process together, when one gets stuck the other fails once and is unavailable for about 3.4 seconds -- a cost the design accepts on purpose;
- the boundaries of identification: no Worker is marked when no culprit report can be read; the same Worker terminated 8 times by the CPU limit does not become a suspect;
- without suspects, ordinary tenants' p50 / p99 cannot be told apart from those with the feature off; each machine has one more process, about 30-45 MB of resident memory.

**Code**: the partitioning is concentrated in three places of the process pool, choosing a process, availability and heat, and ingress, local Bindings and calls forwarded from peers all go through the same entry for choosing a process; the suspect table reads only the local view,
an identification takes effect locally at once, and an old etcd snapshot does not wipe out a local identification not yet synchronized. A minor caveat: the suspects' expiry times are passed between machines as wall-clock times, so clock skew makes expiry a few seconds early or late,
which is acceptable against a suspicion period counted in minutes.

**With this, the chain of handling "a piece of runaway tenant code" is complete**: what can be terminated is terminated inside the isolate (section 7), what cannot is killed with its process by the watchdog (section 5) and identified (section 7),
and from then on it is locked into the quarantine processes (this section). In the isolation research the same kind of fault stalled neighbours in the same process for 45-48 seconds; with this chain, ordinary tenants have zero failures after the first time.

## 10. Idle eviction: design

**The problem**: workerd's dynamic loader caches isolates by name (`WorkerLoaderNamespace::isolates`), and **there is no removal path at all except abort** --
once a Worker has executed in a process, its isolate stays until the process exits; failed load entries stay too, so the Worker becomes permanently unavailable in that process (section 7 ran into this).
A resident Worker takes about 2 MB of resident memory. On a multi-tenant platform most tenants are idle, and ingress placement and "local execution after the Home refuses" also keep the same Worker resident on several machines;
without eviction, residency only grows. This is also a precondition for "local execution after the Home refuses" going into production.

**The approach**: inside workerd (our patch, changing only `src/workerd/server/`), track each dynamic Worker's activity and remove it from the loader's table once it has been idle long enough.

| Item | Design |
| --- | --- |
| What counts as idle | No requests in flight (including streaming responses not yet finished and JS RPC), no waitUntil tasks, and longer than the eviction threshold since the last request ended. Only workerd itself knows these, so the judgement lives in workerd, not in the server |
| How to evict | Remove it from the loader's table -- the same existing path as invalidation by the CPU limit. Later calls create a new isolate; the old isolate is destroyed once its last reference is released. The removal itself interrupts no work in flight |
| Destruction does not stall neighbours | An isolate is destroyed on the process's event loop (the lifecycle research observed an upper bound of 85-151 ms for a single eviction, including sampling overhead). Batch eviction has to be rate-limited: destroy one at a time, at a configurable interval, so that neighbours in the same process do not wait noticeably, let alone trigger the watchdog |
| Failed loads | A failed load entry is removed immediately and the next call loads again; the answer to the failing call itself is unchanged |
| Threshold | `idleEvictMs`, 5 minutes by default, 0 turns it off. Basis: a cold start has a p50 of 4.4 ms and a p99 of 12.9 ms, and a Worker takes about 2 MB resident -- reloading is cheap, memory is expensive; Workers called at least once in 5 minutes stay resident. The threshold must be larger than the heat window placement uses (`placement.idleMs`, 30 seconds by default), so that a version the server considers "hot" is certainly still resident |
| The server's bookkeeping | Each process's heat table and `lastExecution` are trimmed by the heat window instead of only growing; the state endpoint gives the actual residency workerd reports, the number of evictions, and the number of removals after failed loads |
| Scope | Dynamic Workers (tenant code); the static runtime bridge is not evicted; quarantine processes are included |

**Not included**: eviction by memory watermark (the lifecycle research concluded that the existing seams give no safe red line; section 13 takes the container's working set as the signal), process rotation (section 23), coordination of eviction across processes.

**Open questions for measurement** (answered in section 11): how much resident memory drops after isolates are destroyed on Linux (the lifecycle research measured only on macOS: 46.9% within 30 seconds);
how long destroying a single isolate takes; neighbours' latency in the same process during batch eviction.

## 11. Idle eviction: results

**Residency comes down** (each ordinary process loads 100 Workers, then traffic stops; a 10-second threshold; 12 ordinary processes over three machines; three runs each):

| | Eviction group | Control group (no eviction) |
| --- | --- | --- |
| Resident Workers: after loading -> after traffic stops | 1,200 -> **0** (all destroyed 10.4-10.7 seconds after stopping) | 1,200 -> 1,200 |
| Median resident memory per process: idle / after loading / when all are destroyed / 60 seconds later | 45 / 242 / 203 / **134 MiB** | 45 / 242 / 244 / 250 MiB |
| Calling these 1,200 Workers again after eviction | All succeed; p50 5.8-6.0 ms, p99 12-16 ms (that is, cold starts) | -- |

**The memory drop is the most important measurement** (the lifecycle research had measured only on macOS): on Linux, destroying 100 isolates gives back only about 40 MiB at once,
and about 108 MiB after 60 seconds, 55% of the loading increment (about 197 MiB); **still about 89 MiB above idle**. Eviction stops the growth and lets later arrivals reuse the memory,
but returning the process's memory to the operating system completely still needs process rotation -- consistent with the lifecycle research's judgement, now with data from Linux.

**What is in use is not evicted**: a streaming response, waitUntil (calling a Binding once more at the end) and JS RPC each lasting 3.3 seconds, more than 3 times the threshold (1 second), were still resident in every sample taken every 100 ms,
were evicted about 1 second after they ended, and the Binding call succeeded. A race at the moment of eviction, 30 times: 15 executed by the old isolate and 15 by a new one, zero failures, each executed exactly once.

**A failed load is not permanent**: after a one-time failure to fetch code, the new version's next call succeeds (503, 200, 200, 200 in all three runs); the same injection without idle eviction kept failing afterwards
(503 × 4). A Worker throwing at its module's top level is reloaded on every call and fails explicitly, each time in about 2-5 ms with about 2-4 ms of CPU, and neighbours in the same process all succeed.

**Eviction does not stall neighbours**: destroying an isolate on Linux is cheap -- median about 0.3 ms, p99 at most 0.82 ms, longest 4.2 ms; by default there are 10 ms between two destructions.
When 100 Workers in the same process expire at once, the neighbours' p99 is 9.3-19.4 ms (baseline 6.8-11.3 ms), the longest 44.8 ms occurring before the first destruction; zero failures, zero mistaken kills by the watchdog.
**Batch loading hurts neighbours more**: while these 100 Workers load, the neighbours' p99 is 37-72 ms -- compilation runs on the event loop (see the problems below).
When no eviction happens, ordinary tenants' p50 / p99 cannot be told apart from those without idle eviction.

**Working with the other mechanisms**: invalidation by the CPU limit and eviction share the removal path without handling anything twice; during an object storage or etcd outage, evicted Workers reload from the local code bundle cache and succeed as usual;
Workers in quarantine processes are evicted too; the server's heat table and `lastExecution` drop to zero after the idle period (100 per process and 400 per machine while loaded).

**Code**: the eviction logic is in the files `coworkerd-idle-eviction.{h,c++}`, with only hooks added to upstream files; "idle" is judged from two counts: the complete lifecycle of a request (including streaming and JS RPC),
and the dependencies hanging on the IoContext (waitUntil releases only when it ends). Judging idleness and removal complete synchronously within one event loop turn, so a new request either takes a count first or sees the new entry.
A failed load entry is held until the startup coroutine ends, then removed from the table. With it, the patch series adds +88 / -21 lines to upstream files in total (including build registration), and our own files have 663 lines.

**The default threshold is 5 minutes**: the cost of reloading is one cold start (p50 about 6 ms), and each resident Worker takes about 2 MB.

**Problems found**:

1. **A failed load is reported as "result uncertain"** (`RESULT_UNCERTAIN`, never rerouted). When fetching code fails or a module throws at its top level, the request handler never ran at all,
   so under the call contract this could be an explicit, reroutable refusal. Load failures were later made explicit refusals before execution (`LOAD_FAILED`, see the [error codes](guide/errors.md));
2. **the effect of batch cold loads on neighbours in the same process** (p99 37-72 ms) is larger than that of eviction itself; to be considered together with watermarks and placement;
3. the evictor wakes every 10 ms (even with nothing to do), and every lifecycle event rewrites the complete residency list -- no problem at the measured scale; with thousands resident and frequent turnover it should publish in batches.

## 12. Logging does not hold up the server's event loop

**The approach**: at startup (before entering the event loop) the server sets up a shared memory ring (4 MiB by default) and then forks a separate writer process. A log call on the event loop only copies the whole line into the ring and
publishes a position, without any system call; the writer process writes to the usual `server.log`. When the ring is full, new lines are dropped and counted by source, never blocking; once the backlog drains, a `[log-dropped]` marker line is written.
After the server is killed with SIGKILL, the writer process still writes out the lines already published in the ring before exiting; `server.pid` is still the server itself. The workerd processes' output is not changed here (see sections 17-18).

**A slow receiver** (the receiver throttled to 4 KB per second for 60 seconds, 8 clients requesting continuously; three runs each):

| | Log ring | Synchronous writes |
| --- | --- | --- |
| Requests / success rate | About 23,500 / **100%** | About 450 / 32%-39% |
| p50 / p99 | 3.2-3.9 / 11.5-14.6 ms | About 1,500 ms (timeouts) |
| Longest stretch without any successful response | 81-96 ms | **13-40 seconds** |
| Log lines dropped | About 33,000-34,000 (counted and marked) | 0 (at the price of the whole machine stalling) |

**No logs lost after a kill**: the server was killed with SIGKILL at random moments 30 times, 17 of them with a backlog in the ring (at most about 420,000 bytes) and 13 with it empty. Every one of the 10,383 requests that had received a successful response
has its line in `server.log`, none missing; the 13,696 JSON log lines after the kills are all complete, without half lines. After the server is killed, the writer process drains and exits in 5 ms at the median and 18 ms at most.

**No cost in normal operation**: with a fast receiver, p50 / p99 cannot be told apart from synchronous writes. Zero drops under normal load; each machine's backlog peaks below 30 KB, so 4 MiB leaves over 140 times the margin.
On SIGTERM it drains and then exits (about 0.17-0.19 seconds); after a restart there is exactly one new writer process; 1,000 stress checks of "close right after the last line is written" missed no line.

**Code**: the writer process first confirms that the producer has ended, then takes a snapshot of the queue -- reading "the queue is empty" first and "the server has ended" second could make it exit early, losing the last few lines.
The ring has a single producer and a single consumer, with correct memory ordering for publishing and releasing.

**The 2 ms of host mounts is not in the server.** With the log directory on a host mount, requests take about 2-2.7 ms longer (p50), with the ring as with synchronous writes. This had been attributed to "writing logs synchronously";
it appears to come mainly from **the workerd process** instead: the runtime synchronously writes one execution log line to `workerd.log` for every request (section 18 finds most of it in the limits' state channel). The server can no longer be held up by logs across the whole machine,
but each workerd process can still be slowed down by its own logs, and a tenant's `console.log` can still slow down its neighbours in the same process.

**Problems found**:

1. **The workerd processes' output still blocks** (sections 17-18 make it non-blocking): a tenant flooding logs must not slow its neighbours down;
2. **refusing to start when locking pages fails**: the shared memory ring is locked into memory, and with a lock limit below about 4 MiB the server would not start.
   Section 13 changes this: when locking fails, the server warns and continues (the memory is touched in advance);
3. the writer process polls every 1 ms when idle; if the writer process exits because the receiver failed, all later logs are dropped (the count is visible) until the server restarts, and it is not restarted automatically.

## 13. The memory watermark: design

**The problem**: idle eviction (section 10) looks only at idle time (5 minutes by default). When too many or too large Workers are used within 5 minutes, the node's memory climbs all the way to the limit: the container is killed by the OOM killer,
or a workerd process crashes -- the watchdog restarts the process, but every call in flight in it fails. Section 11 measured that after destruction a process's RSS drops only about 55% (the capacity inside the sandbox is reclaimed; returning RSS to the operating system is another matter), which also shows that waiting until "full" is too late.

**The approach**:

| Item | Design |
| --- | --- |
| Signal | The container's (cgroup v2) working set: `memory.current` minus `inactive_file` in `memory.stat` -- the same measure as Kubernetes, so reclaimable page cache does not count as pressure. The budget is `memory.max`; when the container has no limit, the configured `memoryBudgetBytes`; when neither exists the feature is off |
| Bands | Green / yellow / red, computed by **the server**, the only source of this machine's watermark; with hysteresis (stepping down requires dropping some distance below the entry line) to avoid flapping. Starting values: yellow 75%, red 90%, back to green below 65% |
| Yellow | 1. Plug into the band interface of placement ([cluster state](cluster-state.md), section 7): among forwarded root calls and nested calls, cold versions are refused (reroutable) and hot ones accepted -- new Workers are placed on other machines; 2. each workerd process's evictor does not wait 5 minutes: **the least recently used goes first** (still only idle ones, idle for at least 5 seconds), one at a time, rate-limited |
| Red | The band interface's red: refuse forwarded root calls and nested calls; the evictor lowers the minimum idle time to 1 second |
| Back to green | The eviction threshold returns to `idleEvictMs` (5 minutes) |
| How workerd learns the band | The server publishes the band, and each workerd's evictor reads it. The channel is up to the implementation (for example a small atomically replaced file, or shared memory), without opening a new request channel into workerd |
| Test switch | The existing `/__coworkerd/load-band` stays; the effective band is the higher of the two |
| Locking the log ring | When locking the log ring of section 12 fails, the server does not refuse to start, but warns and continues (the memory is touched in advance); the state endpoint marks it as not locked |

**Not included**: process rotation (the part of memory that does not come back after destruction still depends on it; section 23); a memory limit for a single isolate (against "one request eating a lot of memory"; section 15);
a machine that received an ingress request while itself in red still executes it locally as a last resort (the placement semantics are unchanged).

## 14. The memory watermark: results

**Memory does not climb to the limit** (three containers limited to 2 GiB each; on each machine 4 hot Workers called continuously, 512 cold Workers rotating in, each taking 4 MiB at its module's top level; for 10 minutes;
the same binary on and off three times each, alternating):

| | On | Off (control) |
| --- | --- | --- |
| Calls / failures | 139,488 / **0** (hot 86,151, cold 53,337) | 103,914 / 1,120 (hot 674, cold 446) |
| OOM kills / process exits | **0 / 0** | 154 / 373 |
| Peak working set | About 1,544 MiB (75.4% of the limit), never entering red | Every machine hit 2,048 MiB |

With the feature on, the working set moves back and forth between 65% and 75%: after entering yellow, the least recently used idle Workers are evicted first, within seconds it falls below 65% and back to green, and cold Workers fill it up again --
150-166 band changes per machine in 10 minutes, with only about 1.5% of the time above 75%. Of the evictions, 52,534 were due to the watermark and the rest due to idleness.
**Under this load, process rotation is not needed**: the memory freed by evicting idle Workers can be reused by newly loaded Workers.

**Other items**:

- Placement avoidance: when a machine enters yellow from real memory pressure, among the forwarded root calls and nested calls sent to it, cold versions are refused and rerouted to execute elsewhere exactly once, while hot ones are accepted; the refusal carries the band and the avoidance duration;
  red refuses all forwarded root and nested calls; ingress requests while the machine itself is red still execute locally. After the pressure lifts it returns to green within 104-186 ms, and after returning to green, Workers idle for 1 minute are not evicted;
- the least recently used goes first: with three Workers A, B and C going idle in that order and A used once more, the eviction order is B, C, A;
- while red persists, a streaming response, waitUntil (calling a Binding at the end) and JS RPC of 3.3 seconds each are not evicted while running, only after they end;
- with neither a memory limit nor a budget the feature turns itself off, and latency cannot be told apart from that without the watermark; with it on, each workerd process reads the band about 82 times per second, each time taking about 0.1-0.2 microseconds;
- with a lock limit of only 64 KiB, the server starts as usual with a warning; the results for a slow receiver and random kills (30 times, zero of 9,890 answered requests lost) are the same as in section 12.

**Code**: every 100 ms the server reads the cgroup and computes the band from the working set against the budget (>= 90% red; red steps down to yellow only below 75%; below 65% back to green), writing it into a 4 KB block of shared memory;
workerd's evictor only reads this memory. The evictor now picks, among the idle Workers, the one used least recently, with a minimum idle time of 5 seconds in yellow and 1 second in red.
The workerd patch for the watermark (`0004-memory-watermark.patch`) changes only our own `coworkerd-idle-eviction.c++` (+46 / -11), with zero changes to upstream files.

## 15. The memory limit of a single tenant, and memory culprits: design

**The problem**: workerd by itself sets no memory limit at all for a single isolate (the parameters for creating an isolate are empty defaults). One tenant can allocate hundreds of MB at once:
the JS heap grows to V8's default limit, or ArrayBuffers push the process to the container's limit, and either way the whole workerd process crashes and every call in flight in the same process fails.
And culprits are identified only when the watchdog kills a process -- when a process crashes from memory, nobody can tell who it was, so the quarantine processes do not step in, and this tenant can come back again and again. This is the situation CPU was in before its limit and culprits (section 6).

**The approach**: follow the CPU limit's two-level model -- what can be terminated is terminated inside the isolate, and for what cannot, identify who it was and lock it into the quarantine processes.

| Item | Design |
| --- | --- |
| Limit | Each dynamic Worker's isolate: the JS heap plus external memory such as ArrayBuffers may not exceed the limit together, **128 MiB by default** (the same as Cloudflare's published limit), configurable, 0 turns it off. Set through the parameters the limit enforcer uses to create the isolate: heap constraints, plus a counting ArrayBuffer allocator per isolate -- jsg installs the default allocator only when the caller provides none, so jsg need not change. WebAssembly memory is capped at the same limit with V8's process-wide parameter |
| What can be terminated | Close to the limit (V8's near-heap-limit callback, or an ArrayBuffer allocation over the quota): terminate the call the isolate is executing and invalidate the isolate, answering `MEMORY_LIMIT_EXCEEDED`; the next call creates a new isolate; the invalidation path is shared with the CPU limit. The headroom temporarily granted so that the termination takes effect **has an overall cap** -- in the isolation research each relaxation granted 8 MiB, repeatedly triggered by a long builtin, reaching about 3 times the nominal limit |
| What cannot be terminated | The headroom is used up, or a single allocation exceeds it (in the isolation research, asking for a 256 MiB array at once crashed the process directly): V8 enters its OOM handling. Take over the isolate's OOM handler in `customizeIsolate()`, write "the call executing at this moment" into the state file before crashing (the same file as the CPU culprit, likewise not through the event loop), then hand back to the original handling |
| Culprits and quarantine | After the process exits, the server reads the memory culprit, identifies it as a suspect, and follows the quarantine processes (section 8). The "only one source of suspicion" of section 8 becomes two: the CPU culprit when the watchdog kills a process, and the memory culprit when a process crashes from memory. Being terminated by the memory limit inside the isolate does not make a suspect -- the harm is already bounded by the limit |
| What cannot be identified | When the container runs out of memory, the kernel kills the process directly, with no chance to write the culprit -- recorded as "undetermined", and no Worker is marked. With the per-isolate limit and the memory watermark (section 13) this should be rare; section 16 measures it |

**Not included**: the static runtime bridge is not subject to this limit; per-tenant limits (corresponding to wrangler's `limits`; per-Worker limits are in section 30); process rotation (section 23).

## 16. The memory limit of a single tenant: results

**The approach**: each dynamic Worker's isolate has a limit of 128 MiB (the JS heap plus external memory such as ArrayBuffers), implemented through the parameters and callbacks with which the limit enforcer creates isolates;
Wasm memory is capped at the same limit with V8's process-wide parameter. Patch 0005 is entirely in `src/workerd/server/`, adding only build dependencies and hooks to upstream files (patches 0001-0005 together add +82/-19 lines to `server.c++`).
What can be terminated inside the isolate answers `MEMORY_LIMIT_EXCEEDED` and invalidates the isolate, sharing the invalidation path with the CPU limit; a single large allocation that cannot be terminated triggers V8's fatal OOM,
and before the process exits the culprit's identity is written down, and the server, after checking it, locks it into the quarantine processes (section 8). The headroom temporarily granted so that termination takes effect is at most 8 MiB in total.

**Controlled measurement** (3 Servers, each with 4 ordinary processes plus 1 quarantine process; one request over the limit every 5 seconds within 60 seconds; three runs each):

| Way of exceeding | With the memory limit | Without it |
| --- | --- | --- |
| ArrayBuffer | The request over the limit gets `MEMORY_LIMIT_EXCEEDED`; **0 process exits, 0 neighbour failures** | 41 process crashes, 109 neighbour failures |
| Gradual heap growth | Terminated at the limit each time; 0 process exits, 0 neighbour failures | 2 crashes and 5-6 neighbour failures per run |
| A single large allocation (a large array, a long builtin, Wasm) | The first one crashes its process with a fatal OOM, and neighbours in the same process fail 0-5 times per run, all within half a second of the first time over the limit; culprit identification correct 108/108, after which the 11 further crashes are only in the quarantine process and the ordinary processes have no more failures | The large array and the long builtin happened to be terminated first by the CPU limit (200 ms), without crashes; Wasm has no limit and even 256 MiB succeeds |

**Interpretation**: the two kinds that can be terminated inside the isolate (ArrayBuffer, gradual heap growth) are the main gain -- without the limit they crashed the whole process and took neighbours down with them; with it they harm only themselves.
V8 cannot terminate a single large allocation midway, only end it with a fatal OOM, at the cost of one process crash the first time each offending tenant exceeds the limit, after which the quarantine processes contain it.
Without the limit, the large array and the long builtin only happened to hit the CPU limit first: allocating a little faster (using under 200 ms of CPU), they would climb all the way to the container's limit and be killed by the kernel without anyone being able to tell who it was -- exactly the gap the memory limit closes.

**Other observations**:

- Dynamic import, the startup phase, resizable ArrayBuffers and other supplementary cases: 19/19 identified correctly; kernel OOM kills and OOMs where the identity could not be written (3 in all) remain "unidentified", with zero suspects, wronging nobody;
- CPU and memory causes alternating, 8 times each: whichever comes first takes effect;
- under normal load, latency cannot be told apart from that without the limit (p50 1.41 vs 1.47 ms, p99 about 3.9 vs 4.8 ms, with the confidence intervals of the differences all containing 0);
- the precision is observational: a single array reaches at most about 289 MiB before failing (V8 notices the excess only after allocating);
- real projects and the six module types pass as usual under 128 MiB.

**Problems found**:

1. A single large allocation still crashes a process the first time it exceeds the limit. Avoiding that needs intercepting before the allocation (changing the allocation paths of V8 or jsg, outside `src/workerd/server/`); not done for now;
2. the limit here is the same for all tenants; per-Worker values (corresponding to wrangler's `limits`) are in section 30.

## 17. The workerd processes' output does not block their event loop: design

**The problem**: section 12 makes the server's own logs non-blocking, but not the workerd processes' output. workerd's standard output and standard error are directly the file `logs/workerd.log` (appended to),
and two things are written to it, both synchronously on workerd's event loop thread: the runtime's one execution log line per request, and tenants' `console.log` -- upstream formats them in
`handleLog()` of `src/workerd/io/worker.c++`, then `fprintf` and `fflush`. As soon as the log's receiver slows down (a host mount, a busy disk, a stuck log collector),
the requests of every tenant in this workerd process are held up. Section 12 measured about 2 ms more per request with the log directory on a host mount and attributed it mostly to this (section 18 finds most of it elsewhere).

**The approach**: copy the server's proven model (section 12), without changing workerd.

| Item | Design |
| --- | --- |
| Output | workerd's standard output and standard error become pipes, drained promptly by a reader **not on the server's event loop** and written to the usual `workerd.log` (line content and format unchanged). The pipes never fill up, so workerd's writes never block |
| A slow receiver | When the backlog exceeds the limit, new lines are dropped and counted per process, and a drop marker line is written after draining; workerd is never blocked in turn |
| Lines already written are not lost | When workerd or the server is killed, lines already written into the pipe reach the disk as usual (the same guarantee as in section 12) |

**Not included**: per-tenant log quotas. To drop a log-flooding tenant first, we first need to know which tenant a log line belongs to; a tenant's `console.log` lines carry no identity,
and the attribution has to come either through a mechanism like the Worker Loader's tail, or by changing workerd's `handleLog()` (outside `src/workerd/server/`).
The pipes first ensure "flooding logs does not slow neighbours down by blocking"; the CPU that flooding logs itself takes counts against that tenant's request CPU limit (section 6).

## 18. The workerd processes' output does not block their event loop: results

**The approach**: copying the server's model (section 12), without changing workerd. Before entering the event loop the server forks a reader process `coworkerd-wlog`; each workerd's standard output and standard error each get a pipe,
whose read end is handed to the reader process over a Unix socket. The reader process has two threads: the read thread only drains the pipes, reassembles whole lines per process and puts them into a 4 MiB backlog per process (when full, whole lines are dropped and counted);
the write thread is the only writer of `workerd.log`. So workerd's `write` can never be held up by the receiver. Lines written into the pipe before a kill reach the disk as usual, with no half lines and no interleaving.

**Controlled measurement** (3 Servers, the receiver throttled to 4 KB per second for 60 seconds, 8 clients requesting continuously; three runs each):

| | Pipes, slow receiver | Pipes, fast receiver | Output directly to the file, slow receiver |
| --- | --- | --- | --- |
| Requests / success rate | About 23,200 / **100%** | About 23,100 / 100% | 377-823 / **11%-16%** |
| p50 / p99 (ms) | 4.46-4.49 / 9.8-10.4 | 4.51-4.70 / 9.3-10.0 | Timeouts (1.5 seconds) |
| Longest stretch without a successful response | 67-92 ms | 61-85 ms | **17.6 / 40.6 / 51.2 seconds** |
| Log lines dropped | About 57,000 (counted and marked) | 0 | -- (the processes were repeatedly killed and restarted by the watchdog) |

With the pipes, the slow receiver is worse than the fast receiver in no item (the difference intervals of p99 all contain 0; p50 was even slightly lower twice by 0.15-0.2 ms, within the variation between runs).

**Other results**:

- **A log-flooding tenant does not slow its neighbours down**: with one tenant in the same process logging 1,000 lines per request (about 20,000 lines per second), the neighbours' p50 / p99 cannot be told apart from without the flooding. But once the backlog is full, about 97% of the neighbours' log lines are pushed out
  (dropping goes by arrival order, regardless of tenant) -- the reason for per-tenant log quotas;
- **no logs lost after kills**: workerd killed at random moments 30 times and the server 30 times, about half of each with a backlog at the time of the kill (0.8-1.6 MB queued, tens to over a hundred KB still in the pipes):
  for the 7,905 requests that had received a successful response, the matching execution log and tenant log lines are all in `workerd.log`, none missing, no half lines;
- **under normal load**: p99 cannot be told apart from direct file output; p50 was 0.2-0.3 ms higher in two of three runs, exactly the two runs where the pipes ran first, so it cannot be separated from an order effect; one more hand-over between processes per line is a possible source;
- the lifecycle (watchdog rotation, SIGTERM, SIGKILL, restart in place) connects or drains and exits correctly, leaving nothing behind.

**Most of the roughly 2 ms added by host mounts is not in logging.** Section 12 attributed it to workerd writing logs synchronously. With the output asynchronous, a host mount still takes 1.4-1.8 ms more than a container-local volume (1.9-2.2 ms with direct file output);
with the CPU and memory limits turned off, the increment disappears. The reason is that the limits' state channel goes through files in the log directory: for every request, workerd opens, appends to and closes `logs/<process>-<generation>.cpu.events` once on its event loop,
and the server opens and reads it again for every request. Even on a container-local volume this takes about 0.4 ms (with the limits off, p50 is 0.4 ms lower, about a tenth of the request latency). This is the channel of the CPU limit (section 6); **sections 19-20 move it out of files**.

**Problems found**:

1. after the server is killed, workerd keeps running as an orphan (in production containers the server is PID 1, so not affected); outside a container, a restart in place collides on ports.
   The launcher's parent-death signal (section 27) ends workerd together with the server;
2. a graceful exit has to wait for the reader process to write out its backlog: when the receiver is stuck, the server's exit is held up (the same as the server's writer process, section 12).

## 19. The limits' state channel does not read or write files per request: design

**The problem** (found in section 18): the state channel of the CPU limit (section 6) and the memory limit (section 15) is the file `logs/<process>-<generation>.cpu.events` in the log directory:
at the end of each request workerd does `record("completed")` -- `fopen`, append one line, `fclose` on the event loop; for every request, the server's `select`/`available` call `refresh()`,
which reopens this file for each process, seeks to the last position read, reads the new lines and closes it. So each request costs a few to over twenty file system calls on each side: about 0.4 ms on a container-local volume (about a tenth of the request latency),
about 1.5 ms on a host mount. The channel has only two uses: the server counting terminations, invalidations and reloads, and identifying the culprit after a process is killed -- neither needs opening a file per request.

**The approach**: the channel's semantics and record format are unchanged; only how it is read and written changes.

| Side | Before | After |
| --- | --- | --- |
| workerd | `fopen` / append / `fclose` for each event | The event file is opened once (append mode) when the limit enforcer is created, and each event is one `write()` of a whole line; **no user-space buffering** -- the event must reach the file before the function returns, so it is not lost if the process is killed with SIGKILL right afterwards |
| Server | Each `refresh()` opens, seeks, reads and closes for each process | Each generation of a process opens the event file once and reads incrementally from the position already read; and reads only when needed: the watchdog's inspection tick, a process exit, classifying a failed call, the state endpoint -- not on each request's dispatch path |

**Unchanged**: the `.events` file is still the record from which the culprit is identified after a process is killed (the CPU limit's `culprit` and the memory limit's OOM identity files as before); the `[cpu-limit]` stderr lines as before (reaching the disk through the reader process of section 18);
the counting is unchanged. Expected effect: the host mount's increment returns to the "limits off" level, and p50 on a local volume is about 0.4 ms lower.

## 20. The limits' state channel does not read or write files per request: results

**The approach**: patch 0006 (+24/-5 lines, changing only `coworkerd-cpu-limit.{h,c++}`) -- the event file is opened once in the process, and each event is one unbuffered `write()`; the server (`process-pool.c++`) opens the event file once per generation of a process and reads incrementally,
only at the watchdog's inspection (once a second), a process exit, classifying a failed call and the state endpoint; the request dispatch path no longer touches files. The record format, the identification and the counting are unchanged.

**System calls** (strace, 30 seconds of load, three runs each):

| Per request | After | Before |
| --- | --- | --- |
| workerd on the state file: open / write / close | **0 / 1 / 0** | 1 / 1 / 1 |
| The server on the state file: open / read / close | **0 / 0.02 / 0** | 11.3 / 2.5 / 2.3 |

Before the change the server called `refresh()` 2.25 times per request on average, each time opening the file once for each of 5 processes -- that is where the roughly 2 ms added on host mounts came from.

**Latency**: the p50 increment of a host mount over a local volume dropped from +1.7-2.0 ms before the change to -0.17-+0.31 ms, in the same range as with the limits off (-0.15-+0.18 ms).
**On a local volume p50 did not drop** (the expected 0.3-0.5 ms reduction did not appear): with the change, turning the limits off is still about 0.45 ms faster. The reason is in the findings below.

**Identification and counting unchanged**: the counts and identifications of the 13 watchdog and culprit scenarios of section 7 are the same as before the change, item by item; the three kinds of single large allocation of section 16 were identified correctly 12/12, with one ordinary process crash followed by all 11 taken by the quarantine process, as in section 16;
workerd killed at random 30 times: the event file and the stderr lines of the generation killed are the same word for word and in order (38,253 lines), with one sample hitting the window "killed between two writes" (one more line in stderr, as designed) and no half lines.

**Findings**:

1. **The roughly 0.45 ms the limits still cost on a local volume is not in the file channel but in the watchdog thread**: the CPU limit's design (section 6) creates a new watchdog thread on each entry into JS and reclaims it on exit; strace shows exactly one more `clone` per request with the limits on.
   Sequential requests from one client for 5 seconds: 1,642 completed with the limits on, 2,283 with them off. A resident watchdog thread, with entering and leaving JS only registering, removes it without changing the identification semantics (sections 21-22).
2. **The server also read the code bundle's manifest file and recomputed its hash for every request** (`ensureCode()` -> `BundleCache::source()`, the deployment layer). Bundles with assets already had a per-bundle-hash manifest index;
   bundles without assets use the same index (see "Bundle manifests resident in memory" in [ecosystem compatibility](ecosystem-compat.md)).

## 21. A resident watchdog thread: design

**The problem** (found in section 20): the CPU limit of section 6 creates a new watchdog thread on each entry into JS (`Scope` in `coworkerd-cpu-limit.h`: a `kj::Thread` sleeping until the CPU deadline and terminating the execution when it arrives), and wakes and reclaims it on leaving JS.
strace shows exactly one more `clone` per request with the limits on; on a local volume the limits on are about 0.45 ms slower than off (about a tenth of the request latency); sequential requests from one client for 5 seconds complete 1,642 times with them on and 2,283 times off.
Creating and reclaiming the thread and waking it across threads is this cost.

**The approach**: one resident watchdog thread per workerd process. Entering JS only registers (the isolate, the deadline, the call's identity), and leaving only unregisters; the watchdog waits on a condition variable until the earliest deadline (woken when a registration or unregistration changes the earliest time),
and when the time comes does exactly what the thread per entry does: terminate that isolate's execution, record the event, take the existing invalidation path. Computing the deadline, judging the excess, identifying the culprit and the memory limit's callbacks (section 15) are all unchanged; only "who waits" changes.
workerd executes JS on a single event loop thread, so at most one or two registrations are active at a time (the memory limit's auxiliary entries nest), and the table is always small.

**What has to hold**: termination is no less prompt than with a thread per entry (where the delay from the watchdog waking to terminating execution is one thread wake-up); the watchdog thread ends cleanly when the process exits; the thread is not created when no limit is enabled.

## 22. A resident watchdog thread: results

**The approach**: patch 0007 (+189/-33 lines, changing only `coworkerd-cpu-limit.{h,c++}` and `coworkerd-memory-limit.{h,c++}`). One watchdog thread `limit-watchdog` per workerd process, started when the first enforcer with a limit enabled is created;
entering JS registers and leaving unregisters (the watchdog is woken only when a new entry's deadline is earlier than the time it plans to wake, and unregistering does not wake it); the watchdog waits until the earliest deadline and then does item for item what happened before (CAS the state, `TerminateExecution()`, the 100 ms grace period, the culprit file).
It waits in segments (when more than 1 ms away, it waits half and recomputes) because macOS and the virtual machines on it coalesce long timed waits and wake several milliseconds late. With both limits off, no thread is created; the thread is joined when the process exits.

**System calls** (strace, 30 seconds of load, three runs each): workerd's `clone` per request dropped from 1.000 to **0**; opening and closing the state file are still 0; the measured process has 11 threads (including 1 watchdog), 10 before the change.

**Latency**: with 8 clients, the comparison cannot tell apart differences of 0.1-0.4 ms (p50 differs by 0.15-0.3 ms between runs already); in direction, on and off overlap after the change, and before the change was 0.14-0.61 ms higher. **Sequential requests from one client for 5 seconds** (not traced, three runs each) are more sensitive and consistent in direction every time:
0.813 ms per request before the change, 0.730 ms after it, 0.699 ms with the limits off -- **the limits' cost per request dropped from 0.114 ms to 0.031 ms** (about -73%). The remaining 0.031 ms corresponds to 6 more system calls per request (2 writes of events, 2 reads of the CPU clock, 2 `getpid()`), unrelated to the watchdog; `getpid()` can be cached (section 26 reads it once per process).

**Semantics and promptness unchanged**: the 13 watchdog and culprit scenarios of section 7, run three times, are each time the same as before the change, item by item; termination promptness (CPU used at termination minus the limit) over 18 runs is p50 1.04 ms, max 1.87 ms, versus 1.42 / 2.40 ms before the change -- more prompt with the resident thread; the three kinds of single large allocation of section 16 identified 12/12, with only the first crash in an ordinary process, as before; CPU and memory alternating 8 times each, whichever comes first takes effect; nested entries (dynamic import) identified correctly; no watchdog thread left behind when a process exits.

**Stopping a single workerd**: a workerd managed by the server does not exit by itself on SIGTERM -- it enters draining, but the server keeps holding its connection to it, so the drain never finishes, and about 3 seconds later the watchdog kills it. **To stop a single workerd gracefully, the server has to close its connection to it first** -- which is what process rotation does (section 23).

## 23. Process rotation (graceful generation change): design

**Why**: three things need it. (1) Upgrading the workerd binary (following upstream every four weeks) cannot rely on restarting whole machines; (2) a process's RSS stays at its historical peak and the main cage keeps a small residue, and changing generations is the only way to return a process's memory to the system completely;
(3) without it there is no way to stop a workerd gracefully (section 22).

**The approach**: the process pool rotates slot by slot, one slot at a time, serially across the machine.

| Step | What |
| --- | --- |
| 1. Start the new generation | Start a new generation of workerd next to the same slot (a new binary or the same one), and wait until it is ready |
| 2. Switch dispatch | New calls all go to the new generation; the old generation stops taking new calls, and placement no longer chooses it |
| 3. Drain the old generation | Wait until the old generation's calls in flight reach zero (including background work of `waitUntil`); what has not reached zero after the drain deadline (30 seconds by default) is handled by the rules of section 3: kill the process, and those calls get `RESULT_UNCERTAIN` |
| 4. Finish | The server closes its connection to the old generation, sends SIGTERM to let it exit by itself, SIGKILL after a grace period; records the rotation's duration, the calls in flight at the switch, the number of calls killed, and the new generation's cold loads |

**Triggers**: (a) manual (an operation on the state endpoint or a subcommand of the deployment tool, naming a slot or all); (b) a changed binary -- when the server finds that the hash of the configured workerd executable changed, it rotates slot by slot, which is workerd's rolling upgrade; (c) limits on a generation's age or the number of requests handled (configurable, off by default);
(d) memory -- only the interface, with its threshold off by default (section 24). Quarantine processes rotate too.

**What has to hold**: during a rotation not one request fails because of the rotation (except those hitting the drain deadline, counted separately); a call tree is never half in the old generation and half in the new one (dispatch is switched by when a call starts, and what has been dispatched does not migrate);
the watchdog, the limits and culprit identification are each correct while the old and new generations exist at once; after a rotation the slot's RSS returns close to the idle baseline (this is the practical answer to the RSS problem).

**Not included**: the rolling upgrade of the server itself (it has to work together with members leaving and placement avoidance; see [cluster state](cluster-state.md), section 11); warming up the new generation before switching (a cold load's p50 is about 6 ms; first measure how rotation affects latency, then decide -- see section 24).

## 24. Process rotation: results

**The approach**: the process pool rotates slot by slot -- start the new generation on a spare port and wait until it is ready -> at the next inspection tick, replace the slot's current generation with the new one (choosing a process and dispatching happen in the same synchronous call, so what has been dispatched does not migrate) -> wait until the old generation's calls in flight reach zero (the criterion: the server's in-flight count is 0, and every dispatched call's request context has ended, the latter judged from the `completed` lines the limit patch writes, so background work of `waitUntil` counts too) -> close the server's connection to the old generation, SIGTERM, SIGKILL after a grace period.
Triggers: manual (the state endpoint and a subcommand of the deployment tool), a changed executable (`stat` every second, the hash computed in a background thread), age and request limits (off by default), the memory interface (threshold off by default). The configuration goes through environment variables here (section 26 moves it into the configuration file's schema).

**Results**:

| Scenario | Observation |
| --- | --- |
| Rotating s1's 4 slots one by one under load (60 seconds × 3) | About 23,000 requests, **0 failures, 0 `RESULT_UNCERTAIN`**; the rotated s1 against s2+s3 not rotated in the same run: p50 difference +0.01-0.03 ms, p99 difference +0.1-0.7 ms, intervals all containing 0; each rotation 43-75 ms, the new generation's cold loads of 2 Workers 7.5-12.4 ms each; at most one slot rotating at any moment |
| A changed executable (two workerd builds swapped, three times each) | Each of the three machines noticed within 0.05-0.9 seconds and rotated slot by slot (5 slots done in 243-333 ms), after which every generation used the new binary; about 23,500 requests in the meantime with zero failures |
| Draining and the deadline (a 5-second deadline) | A 5-second stream was delivered completely in the old generation (20,545 bytes identical byte for byte), while new requests for the same slot all went to the new generation; a stuck `await` was killed at the deadline, and only it got `RESULT_UNCERTAIN`; the background work of `waitUntil` finished in the old generation before it was closed |
| **Memory back to the baseline** | One slot loaded 64 Workers up to 282 MiB, and 60 seconds after evicting them all it still held 180 MiB (only 44% returned); **after the rotation the new generation is at 51.8-52.4 MiB, only -0.1 to +1.1 MiB from the idle baseline (51.3-51.9)** -- the practical answer to "process RSS is not returned" |
| Coexisting with the existing mechanisms | The old generation killed by the watchdog while draining, the new generation killed by the watchdog while the old one drains as usual, CPU overruns across the switch, the large allocations of section 16 crashing the old and the new generation, quarantine slots rotating, lifecycle events across three generations complete -- identification and quarantine each correct |
| Triggers | The request limit, the age limit and the memory interface each triggered as expected, 17,000 requests with zero failures |

**Host stalls**: on a test host whose memory was overcommitted, whole-machine stalls of 400-1,000 ms made all three machines' probes overdue at once, and the watchdog paused all processes.

**Decisions and open points**:

1. The rotation configuration goes through environment variables here; section 26 moves it into the configuration file (`ServerConfig.rotation`);
2. the drain criterion depends on the `completed` lines the limit patch writes, so with the limits off it can only wait for the deadline; section 26 writes `completed` lines regardless of the limits;
3. the only cost of rotation is the new generation's cold loads (7.5-12.4 ms per Worker); no warming up at this scale;
4. the memory trigger's threshold stays off: the research found the main cage's residue bounded (section 28 has what that finding does not cover) -- the need for rotation itself is unaffected (rolling upgrades, graceful process stops and returning RSS all depend on it);
5. the state endpoint's `rotate`, like the other management endpoints, is on the ingress listener and needs the same access control (see the management token in the [installation guide](guide/install.md)).

## 25. The channels between the server and workerd are descriptors handed over at startup: design

**Why**: the process sandbox (namespaces + seccomp, sections 27, 29 and 30) requires workerd to have no file system and no network after startup, only the few descriptors the server hands it. Five channels between the server and workerd went through files or local TCP:

| Channel | Before | After |
| --- | --- | --- |
| Server <-> workerd RPC, probes, the runtime calling back into the server | Local TCP ports (`basePort + i` and spare ports; the runtime's external service `SERVER` is also a TCP address) | Unix sockets in the stack's run directory (with the sandbox, only this one directory needs to be mounted in); on the workerd side, its existing `--socket-fd` / `unix:` addresses |
| Limit events (sections 7, 20) and culprit files (`.cpu`, the memory limit's `.memory*`) | Files under `logs/` | One pipe: workerd holds the write end, the server the read end; one `write()` per line (atomic up to PIPE_BUF), and lines left in the pipe after the process is killed are read by the server as usual; the OOM handler uses the same descriptor, opened in advance |
| Lifecycle events and the residency list (idle eviction) | The file `logs/<process>.lifecycle` | Event lines in the same pipe, or a block of memfd shared memory (the approach is free; the server's logic is unchanged) |
| Memory band (the watermark) | A shared memory file | A memfd, handed over at startup |
| Standard output, standard error (section 18) | Pipes | Unchanged |

workerd's configuration file and the runtime bridge module are still read from files (it is enough that they are read before startup); whether they become descriptors is decided with the sandbox design.

**Three smaller changes**: `completed` lines are written whether or not the request used CPU (so the drain criterion of rotation and of the server's graceful exit no longer waits for the deadline); `getpid()` caching (the two calls per request found in section 22); the rotation configuration goes into the configuration file's schema.

**What has to hold**: the earlier results stay the same item by item on the new channels -- limits and identification (sections 7, 16), idle eviction (section 11), the watermark (section 14), system call counts (sections 20, 22), rotation (section 24), the server's exit ([cluster state](cluster-state.md), section 11); evidence is not lost in crashes (after workerd is killed, the culprit and the OOM identity can still be read); in steady state workerd makes no more opens or writes under `logs/`.

## 26. The channels between the server and workerd are descriptors handed over at startup: results

**The approach**: for each workerd process the server sets up the channels before spawning and hands them to the child under fixed numbers, then immediately closes its own copies of the child's ends -- 1 and 2 the output pipes (section 18, unchanged); 3 the write end of the limit event pipe (`COWORKERD_EVENT_FD`): limit events, culprits and the two OOM record lines all go through it, one `write()` per line of at most `PIPE_BUF`, and the OOM handler uses the same descriptor, writing two lines prepared before entering JS; 4 the listening Unix socket `<stack>/run/<slot>-<generation>.sock` (`--socket-fd rpc=4`, probes and RPC; rotation's spare port is replaced by the next generation's socket name); 5 shared memory for lifecycle snapshots (memfd, double-buffered with a sequence number, so a half-written snapshot is never read); 6 shared memory for the memory band (memfd, sealed read-only). The runtime's callbacks now connect to `<stack>/run/binding.sock` and `source.sock`. The workerd patch adds only `0008-fd-channels.patch`, changing only five `coworkerd-*` files; `processPool.basePort` and the two callback TCP addresses on the command line are still parsed, ignored, and noted in the startup log. The culprit file became one `culprit` line, withdrawn by a later `terminated`/`memory-terminated` line of the same call; on the event loop the server attaches a watcher to each generation's read end and moves data into memory as soon as it arrives (without parsing), reads to the end of the pipe after the process exits, and records each generation's line count, bytes, in-order SHA-256 and leftover bytes in the state endpoint's `processes.recentChannelExits`. Also: `completed` lines are written by the request context's destructor, whether or not CPU was used, also with the limits off; `getpid()` is read once per process; the rotation configuration went into `ServerConfig.rotation`, still overridden by `COWORKERD_ROTATION_*`.

**Results** (three Servers, each with 4 ordinary + 1 quarantine processes, production defaults; three runs each, 30 kills):

| Item | After | Before |
| --- | --- | --- |
| System calls on `logs/` by the home workerd under steady load (strace on all threads, about 11,700 requests in 30 seconds) | **0** | 11,872 / 11,889 / 11,912 |
| `open`/`write` on `logs/` per request | 0 / 0 | 0.002 / 1.002 |
| `clone` / `getpid` per request | 0 / **0.0039** | 0 / 2.0064 |
| TCP listeners | None (only the server's 8930 and 8934 remain in the container) | workerd 9000-9004, and 8932, 8933 the server opened for callbacks |
| State files workerd writes in `logs/` | 0 | 12 |
| Random SIGKILL 30 times (16 clients, kill points 150-900 ms) | All 64,729 lines read, read to the end of the pipe, 0 leftover bytes, 0 unparsable, in order and word for word the same as the `[cpu-limit]` lines | -- |
| Watchdog culprits (section 7: a long builtin, 10 kills; a repeated long builtin, 12 kills) | The same as before the change, item by item | -- |
| The OOM identities of the three kinds of single large allocation (section 16) | 12/12 each, 1 ordinary and 11 quarantine, the same as before the change | -- |
| Draining with the limits off (`waitUntil` 1.5 seconds, a 5-second deadline) | **1,492 / 1,487 / 1,491 ms**, `drained` | 5,001 / 5,004 / 5,003 ms, waiting for the deadline |
| Draining after the calling Server is killed with 32 child calls in flight | 1 / 1 / 1 ms | -- |
| The rotation configuration takes effect from the configuration file, overridden by environment variables | All three items consistent | -- |

The remaining `getpid` calls are all on the main thread and appear only under load; they come from V8's `GCTracer::Print()` (each garbage collection formats a summary line into the heap's OOM ring buffer, whether or not `--trace-gc` is on), are not proportional to the number of requests, and are not in our code.

**Other properties**:

1. On the pipe the bare `memory-oom-undetermined` line is always written before the identity line, so only a full pipe or a closed other end could keep the identity line from being written; invalid values of `COWORKERD_EVENT_FD` are refused before any request is served.
2. **The ingress keep-alive race** (unrelated to the channels): the response had been sent completely to the client, but the ingress handling ended only when the back-end RPC returned; if the back-end generation exited at that point, the connection was closed, and the next request the client sent over that reused connection was lost. It was fixed together with making load failures explicit refusals (section 11).
3. The upper bound on event line length relies on `workerId` and `versionId` together not exceeding about 1.7 KB, so the deployment layer has to cap identifiers accordingly.
4. Upstream kj-rs-io on macOS exits when it accepts an already disconnected Unix connection; Linux is not affected, and the server avoids it (the startup probe does not give up the connection before accept).

**What workerd needs after startup** (the input for the process sandbox): only the `<stack>/run/` directory (its listening socket and the two callback sockets) and the inherited descriptors 1-6; the configuration file and the runtime bridge module are still read from `processes/` before startup. There are also two directory descriptors that KJ's disk file system opens at startup, `/` and the working directory, to be considered in the namespace design.

**Sockets left by a previous server**: at startup the server probes them before cleaning up -- a socket that can be connected to belongs to a live Server in this stack directory, and the new instance refuses to start (`start-refused`, changing no file and registering no member); only those refusing connections or absent are deleted, so starting another instance in the same stack directory does not delete the running instance's callback sockets. When only the Server is killed with SIGKILL, workerd does not exit by itself and a restart in place is refused; the launcher's parent-death signal (section 27) solves this.

**The sandbox prototype**: with this input, the same workerd works as usual through a prototype launcher in a least-privilege environment (a read-only root of 11 bind mounts, no network interfaces, uid 65534, zero capabilities, `no_new_privs`, an allowlist of 61 system calls), at a cost too small to measure; in steady state it touches only `/dev/urandom`, `/proc/self/maps`, `/sys/.../cpu/online` and the two callback sockets, with zero access to `logs/` (this section's result holds in that topology too). The product integrates it in four parts: the launcher and mount/network namespaces (with the rotation trigger recognizing the launcher's target program) and user namespaces and capabilities (section 27), the system call allowlist generated with the build (section 29), and an optional steady-state tightening patch (section 30).

## 27. workerd runs through the launcher in a least-privilege environment

**The approach**: the server starts each workerd through the `coworkerd-launcher` built with it (interface: `launcher/PROTOCOL.md`), and the launcher, following the prototype of section 26, runs workerd in new mount, network, IPC, UTS and user namespaces as 65534, with zero capabilities and `no_new_privs`; descriptors 1-6 and the callback sockets in `<stack>/run/` are carried in as they are, with the same paths inside and out, so neither the configuration nor the protocol changed. The configuration `sandbox.mode`: `auto` (the default: on when the prerequisites are met, otherwise without the sandbox and with a warning), `launcher` (refuses to start without them), `off`; the state endpoint's `processes.sandbox` reports the mode, whether it is in effect, the reason, the launcher's hash and a pending-restart hint, and each process's `sandbox.observed.layers` are the layers the server read from `/proc` when it became ready. Also: the binary identity is the signature of the workerd descriptor the server opens itself, not read from `/proc/<pid>/exe` (which removes a race), and a changed launcher file only hints at a pending restart without rotating; the launcher gives workerd a parent-death signal, which solves the orphan workerd of sections 18 and 26. A launcher failure is its own category `launch-failed`, following the backoff, not counted as a workerd crash.

**Results** (a test machine running Ubuntu 22.04, sandbox on; the control on the same machine with the sandbox off): the environment takes effect layer by layer, consistent with `/proc`; 30 minutes of dense load, on and off, about 960,000 requests each with zero failures, and only the configured triggers rotating; replacing workerd rotates each slot exactly once, replacing the launcher does not rotate; after the server is killed, all workerd processes end within 17-22 ms and a restart in place succeeds, while in the control group workerd survives past 3 s and the restart is refused; the bootstrap intervals of request latency contain 0 in all three runs; from start to ready each process takes 4-9 ms more, the launcher's time to set up the environment.

**Decisions**: (1) The prototype's "non-dumpable" layer does not hold for workerd -- the `PR_SET_DUMPABLE 0` the launcher sets is reset by `execve`, and the state endpoint truthfully reports `false`; workerd itself sets it again after readiness (patch 0010, section 30), and the prerequisite of Ubuntu's default `kernel.yama.ptrace_scope=1` is written into the [installation guide](guide/install.md); the approach of "an execute-only copy of the ELF interpreter" is not adopted. (2) In containers Docker's default seccomp and AppArmor profiles do not allow creating namespaces: the default compose file keeps those profiles and `auto` falls back with a warning, while `docker-compose.sandbox.yml` turns the sandbox on for prepared hosts.

The system call allowlist (generated with the build, observing first, then enforcing) is in section 29; the steady-state tightening and non-dumpable inside workerd are in section 30.

## 28. Findings of a 24-hour soak: memory capped by rotation, three kinds of process-level collateral damage

A 24-hour soak with mixed tenants (three Servers, 54 Workers, 50 requests per second, 4,321,771 requests) adds three points to this document's conclusions:

1. **Each workerd's resident memory keeps rising between two rotations** (0.64-0.95 GB/h per machine, peaks of 1.58-1.74 GB against a 2 GiB container limit; one machine spent 70 minutes at the yellow watermark, with 110 `LOAD_BAND_REJECTED`), returning to 0.3-0.4 GB after a rotation, with the peaks and troughs not rising from cycle to cycle. The Server process itself does not grow. The cause was not separated: more isolates, or the heaps of tenants repeatedly growing to the limit and being terminated not being returned to the system -- both are possible. The soak rotated by hand once an hour; with `rotation.maxAgeMs` and `maxRequests` at 0 (off), **there is no such cap**. The research finding "no process rotation needed for memory" is about the main cage's residue being bounded and does not cover this growth under sustained load. The default rotation policy (by age or by resident memory) is therefore decided for 0.3.0: see `rotation.*` in the [installation guide](guide/install.md).
2. **Three kinds of process-level collateral damage**, 18 `RESULT_UNCERTAIN` for good tenants in all (0.0005%): trial loads ran on the target's Home ordinary process, and a 256 MiB allocation at the top level cannot be terminated inside the isolate, so the watchdog ended the whole process, taking the requests in flight in the same process with it (10 times); a single large allocation exceeding the limit for the first time is not yet a suspect and crashes an ordinary process (2 times; see the problems found in section 16); there was one quarantine process per machine, shared by all suspects, so one suspect crashing it took the other suspects' requests in flight with it (6 times). Consequences: trial loads move to the quarantine processes; sharding the quarantine processes by tenant (`quarantineProcesses` greater than 1, by tenant hash) is in 0.3.0's defaults as a cheap mitigation; the root fix for single large allocations remains a research topic.
3. All other invariants hold: bad tenants fail only with explicit error codes; p99 at most 1.49 times the baseline, with no slope hour over hour; the membership table at three machines throughout; all 221 planned events (143 deployments, 48 bad bundles, 24 rotations, 6 drain restarts) reached their expectations, with 0 calls in flight killed; 0 unplanned exits.

## 29. The system call allowlist: generated with the build, observed first, then enforced

**The approach**: the build has a step: `scripts/syscall-allowlist.py` generates the allowlist file from profiles of the workerd of this build (one window each for startup, load, idle, rotation and exit), shipped in the release package, the file stating the workerd hash it belongs to (it may hold several, so that an upgrade first widens the table and then replaces the binary); after setting up the environment and before handing over to workerd, the launcher installs the filter according to `sandbox.syscalls.mode`: `off`, `observe` (calls outside the allowlist go through the kernel's user-space notification to the server, which allows them and logs one line `[syscall-observed]`; the process behaves the same), `enforce` (calls outside the allowlist kill the process, exit category `syscall-denied`, following the backoff). The default is `observe`. An allowlist that does not match the binary is reported through the launcher's startup failure path (section 27). Only the protocol between the server and the launcher changes: the status descriptor is a Unix socket rather than a pipe, so that the notification listener can be handed over with a record, and the launcher takes `--program-sha256`, passed by the server; the protocol between the server and workerd does not change.

**Results** (a test machine running Ubuntu 22.04; the control on the same machine with the filter off): two generations of the startup table are byte-identical (62 entries; compared with the prototype's table of section 26, `sendmsg` and `clock_nanosleep` were added and none removed), and a steady-state table of 40 entries is generated as well (section 30); observe mode under 2 hours of load, 1 224 195 requests with 0 calls outside the allowlist; in enforce mode with one entry removed by hand, 37 terminations, each with a record, the 88 requests at the time all explicitly 503, and the event pipe read to its end; the launcher's setup takes 6.6 against 4.2 ms, that is about 2 ms for installing the filter, with request latency inconsistent in direction across three runs and intervals containing 0; the server's startup check takes about 670 ms more (hashing the 140 MB workerd once).

Rereading and parsing the 9 KB table at every start costs about 2 ms; caching the parse result is left for later. The condition for switching to `enforce` is zero observations in observe mode, over a complete test run and over a soak.
workerd's own patch (section 30) installs the steady-state table and adds non-dumpable.

## 30. workerd tightens its own steady state, becomes non-dumpable, and applies per-Worker limits

**The approach**: patch 0010 changes only `src/workerd/server/`. Before answering its first request (the readiness probe), workerd installs a second table itself -- the steady-state table generated with the build (section 29), narrower than the startup table (43 against 59 on arm64) -- with the mode given by the node (`sandbox.syscalls.steady.mode`, default `off`, while the startup table's default is `observe`); at the same place the process makes itself non-dumpable, closing the gap section 27 recorded, the bit reset by `execve`. In observe mode, calls outside the table are first reported to the node and then allowed; the reporting thread is created before the table is installed and allocates no memory afterwards. Worker-level CPU and memory limits take their values from the Worker configuration; the process-wide WebAssembly memory limit is the largest one declared by the Workers on the machine, effective per generation, and reported per generation in the state endpoint. While draining, the last minute of usage is written after closing the processes and before revoking the lease.

**Results** (Linux arm64 on Docker Desktop's kernel 6.12, which can create namespaces once Docker's default seccomp and AppArmor profiles are lifted -- section 27's "cannot be created" is the conclusion under the default profiles): all six processes carry both filters and are non-dumpable, with the layers the state endpoint reports identical to `/proc` layer by layer; observe mode under 5 minutes of mixed load, 94 960 requests with 0 calls outside the table; enforce mode under the same load with no process terminated, while deliberately removing one call terminates the process, category `syscall-denied`, and the requests at the time are all explicitly 503; identification of culprits and OOM identities is unaffected (both read from the event pipe). Observe mode found that the steady-state table lacked two calls that appear only on lazy creation; they were added to the generator's "always allowed" list (steady-state table 41 -> 43 entries) -- which shows exactly that a table sampled by windows needs an observation period with real traffic, so the steady-state table defaults to `off` in 0.3.0, and switching it to `enforce` waits for a long observe-mode run on real machines. The amd64 workerd has its own two x86_64 tables.

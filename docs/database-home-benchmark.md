# SQLite Durable Objects and native database homes

The benchmark measures two threaded durable paths. Both use a stateless Worker binding on the same
machine. One node owns all sampled databases and confirms writes by flushing its local WAL. The
comparison uses the predeclared thresholds. [#79](https://github.com/coworkerd/coworkerd/issues/79)

## What the throughput measures

The throughput driver sends HTTP from the Mac through Docker's published port and one stateless
Worker to one database on one node. It counts successful replies over that complete route. The
eight-client read cells reached about 1,300 requests/s on both sides. Multiplying those rates by
the separate serial CPU measurements estimates 0.74 of a logical CPU in the Object durable child
and 0.42 in the Native child. Both estimates are below one core. The records leave the shared
limiting stage unresolved among host-to-VM forwarding, the caller Worker and the node. [#85](https://github.com/coworkerd/coworkerd/issues/85)
The [Linux follow-up](database-throughput.md) splits out the CPU of each process and measures eight databases on the same node.

| Operation | Clients | Object completions | Object /s | Native completions | Native /s | Native ÷ Object |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| One-row read | 1 | 4,623 | 462.22 | 7,088 | 708.72 | 1.533× |
| One-row read | 8 | 13,005 | 1,299.99 | 12,666 | 1,266.10 | 0.974× |
| Confirmed insert | 1 | 4,618 | 461.71 | 5,248 | 524.47 | 1.136× |
| Confirmed insert | 8 | 7,183 | 717.89 | 7,415 | 740.74 | 1.032× |

The Object and Native traffic tables contained all 12,844 and 13,706 expected confirmed rows,
respectively, including warm-ups. The measured windows completed with zero request failures.

The original one-client read cell ran immediately after the 1 MiB read latency cell. Its time per
completed request was 2.163 ms on Object versus that side's 1.617 ms mean serial read latency
(34% higher). Native was 1.411 versus 1.349 ms (5% higher). The mean serial latency ratio was
1.199×. The placement of the original 1.533× cell is a potential contributor to the gap.

A separate one-client control ran before the large-read and cold-open workloads in both runs. Over
10 seconds it completed **554.47 reads/s** on Object (5,545 replies) and **747.75
reads/s** on Native (7,478 replies), or **1.349×** Native to Object.

## Builds and paths

| Side | Daemon source | Published ARM64 kernel archive | Archive SHA-256 |
| --- | --- | --- | --- |
| Object | [`3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd`](https://github.com/coworkerd/coworkerd/tree/3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd) | [`kernel-v1.20261114.1-3aa588b669ff-run-45491272877-1`](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-3aa588b669ff-run-45491272877-1) | `99007ee6693a13929a9c5b203da4b35496e42720469bad2e44c38bea51ac6791` |
| Native | [`e88acea9a3630c67388dbccdd994b4d843a953eb`](https://github.com/coworkerd/coworkerd/tree/e88acea9a3630c67388dbccdd994b4d843a953eb) | [`kernel-v1.20261114.1-ee4ced68def3-run-45542574846-1`](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-ee4ced68def3-run-45542574846-1) | `94f1cd543584ef3994e1ea9c8973bf11f1dad44b2631e24b26e64ff1d83bd07a` |

- **Object.** Host HTTP → ordinary Worker → SQLite Durable Object namespace binding fetch → node →
  threaded durable child → class isolate → `ctx.storage.sql` → D1 JSON → ordinary Worker JSON parse →
  host HTTP. Each numbered Worker deploys the same code as its own class and has a distinct Durable
  Object namespace. The fixture takes `SqlDatabase` from the pinned runtime and adapts its internal
  `prepare` calls to the public `exec` API available to a deployed class. It keeps the transaction,
  result envelope, metadata queries and serialization.
- **Native.** Host HTTP → ordinary Worker → product `env.DB` and workerd's D1 wrapper → node → the same
  type of threaded durable child → native SQLite database home → D1 JSON → ordinary Worker's D1 wrapper
  parses the answer → host HTTP. The native checkout includes #59's daemon and kernel changes. Each side
  uses its own published kernel and daemons built from its recorded commit. Runtime JavaScript and
  Docker files are byte-identical across these checkouts.
- **Confirmation.** Both inserts wait for the node's WAL confirmation. The driver verifies a one-row
  insert advances the node's confirmation counter, validates every successful insert and checks the
  final count. Object handles `/execute` by running the INSERT directly. Native wraps each
  `/execute` statement in a SQLite transaction. Both replies wait for WAL confirmation. The sample uses
  Linux `fdatasync` on a Docker named volume. Network replication and peer acknowledgements may change
  write latency and throughput.
- **Baselines.** An empty SQL batch traverses the real binding and returns an empty batch without a user
  statement. The pinned native D1 wrapper decodes the response and throws a `TypeError` when it accesses
  the absent first result. The Worker validates and handles that error. The successful `SELECT NULL AS
  value WHERE 0` control uses both normal query paths, performs no table I/O, and returns no rows.
  Baselines are measured separately. Percentiles include the full observed route.

## Workload

- **Cold open.** 200 serial first queries (`SELECT 1`) on distinct, previously unopened databases with
  prewarmed stateless callers. The Object side verifies a unique module-level copy identifier for each
  new class copy. For the 200 additional opens in warm copies, four groups of 50 use fresh object IDs
  and verify every reply came from its group's previously observed copy. Native databases each have
  their own home. Cold times include shard lookup or acquisition, SQLite open and, on the Object side,
  any class loading and identifier generation.
- **Warm latency and CPU.** 1,000 serial measured requests per operation after 32 untimed calls. The
  fixed row read hits a primary key. The insert uses a new integer key for every request. The large read
  returns one stored text value of 1,044,480 bytes. The JSON envelope needs space below the binding's 1
  MiB body-read ceiling. Latencies run from starting host HTTP to receiving the entire response body.
  This includes the stateless Worker's binding parse. The driver validates the returned value outside
  the latency timer. Percentiles use nearest rank. CPU divides the durable child's aggregate user plus
  system tick delta in `/proc/PID/stat` by the completed request count. This includes background CPU in
  the same process and has tick granularity.
- **Throughput.** One warm database with one outstanding request per client, at 1 and 8 clients. Each
  cell admits work for 10 seconds and counts all validated completions, dividing by the full elapsed
  time including the last requests. The read and insert cells reuse the same warm database.
- **Memory.** Seed 500 databases with one small table and row, rotate the durable child, and reopen the
  requested number from confirmed storage. Take five idle `/proc/PID/smaps_rollup` samples at each count
  and report median RSS and PSS. Verify the durable PID, generation, zero in-flight work and live root
  and home count at the beginning and end of each sampling window. The density criterion uses the 0–500
  RSS slope. The intermediate slopes show variation with occupancy. The benchmark raises the common test
  home-count setting from the default four homes to 520. This reserves the same capacity on both sides
  and leaves a spare place for the native backend's Durable Object admission reserve. Both sides use 128
  ordinary in-flight places and the cluster fixture's 32-event mailbox so eight clients can queue at one
  home. Both sides use a 3 GiB process admission cap. A 16 GiB node planning budget permits that cap
  with the ordinary, quarantine, durable and replacement slots. The Linux VM's physical allocation stays
  at 7.75 GiB. The first attempt used the default 1.55 GiB process budget and stopped during setup
  before opening all 500 objects. It is excluded from the comparison. The default actor and database
  idle retirement remains active.

## Results

### Measurement window

November 25, 2026 (UTC+8). The client ran on an Apple M4 Mac with 10 logical CPUs and 24 GiB RAM. The
same Docker Engine 29.5.3 ARM64 Linux VM (Linux 6.12.76-linuxkit, 10 vCPUs, 7.75 GiB) ran each side.
The Node.js 26.2.0 client used loopback HTTP. The Mac was on AC power. Both sides used the same Compose
sandbox mode and released Linux ARM64 binaries. Both daemon sets were built with Clang 19.1.1 in Ubuntu
24.04. The root storage and WAL lived on the VM's Docker named volumes.

| Order | Side | Setup through final sample | Minimum–maximum guest available RAM | Mean guest CPU busy during sampling |
| ---: | --- | --- | ---: | ---: |
| 1 | Object | 14:55:43–14:58:00 | 3.85–6.24 GiB | 11.4% |
| 2 | Native | 14:58:08–15:00:12 | 4.97–6.13 GiB | 10.6% |

The sides ran serially after the daemon builds. The other build containers were idle at setup and the
15-second process snapshots found no compilers during the recorded windows. The VM retained roughly
1 GiB free swap throughout. The shared host continued its normal desktop workload. Guest CPU and available
memory are phase-boundary observations from `/proc`. The raw reports record each durable child PID and
generation. Memory uses a later generation of the same durable slot.

[Object raw samples](database-home-benchmark/object.jsonl) · [Native raw samples](database-home-benchmark/native.jsonl)

### End-to-end latency in the original pair

| Operation | Samples per side | Object p50 / p99 (ms) | Native p50 / p99 (ms) |
| --- | ---: | ---: | ---: |
| Cold first query, new isolate copy on Object | 200 | 62.483 / 69.950 | 62.551 / 70.518 |
| Cold first query, warm class copy | 200 | 62.345 / 67.405 | N/A |
| Warm one-row read | 1000 | 1.550 / 3.335 | 1.293 / 2.835 |
| Warm confirmed one-row insert | 1000 | 2.026 / 4.961 | 1.748 / 5.096 |
| Warm large read | 1000 | 9.351 / 47.686 | 11.891 / 18.107 |
| Warm no-op query, zero rows | 1000 | 1.553 / 3.073 | 1.319 / 2.699 |
| Warm empty batch, zero SQL statements | 1000 | 1.503 / 3.102 | 1.298 / 2.847 |

The read p50 minus the successful no-op p50 was -0.003 ms on Object and -0.026 ms on Native.
The insert differences were +0.473 and +0.429 ms. These are differences between cell medians.
The thresholds use full end-to-end latency.

### Cold-open follow-up

First access at a new data epoch runs the shared WAL-tail recovery path before dispatching to
the durable child. The node checks the shard epoch through the recovery metadata gate. The gate
allows two outstanding reads, refills at 16 reads/s, and starts with four tokens. Unpaced cold
requests consume that burst and queue for the next token. At steady state one token arrives
every 62.5 ms. The node logs put most of the added time in the tail-wait lap before durable
dispatch. An initial path trace showed 51.8 ms in that lap and 4.2 ms for the following
durable-child open response on one warm-copy request.
[Recovery metadata gate](https://github.com/coworkerd/coworkerd/blob/3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd/node/durable-log.h#L1149-L1153) ·
[Epoch read](https://github.com/coworkerd/coworkerd/blob/3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd/node/durable-log.c%2B%2B#L5577-L5614) ·
[Token refill](https://github.com/coworkerd/coworkerd/blob/3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd/node/recovery-gate.h#L70-L90)

The follow-up kept the same pins, host, VM, Worker paths and 3 GiB process cap. It ran Native
first from 15:18:58 to 15:20:09, then Object from 15:22:55 to 15:24:45 UTC+8.
Both sides repeated the unpaced sweep and used distinct untouched database IDs for the next
sweep. The paced sweep waited 100 ms after each completed query, outside the request timer.
That kept submissions below the metadata refill rate. These remain full host-to-Worker
measurements and include the metadata check and rebuild itself.

| Cold case | E2E n per schedule | Unpaced E2E p50 / p99 (ms) | Paced E2E p50 / p99 (ms) | Unpaced tail wait p50 / p99 (ms) | Paced tail wait p50 / p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Object, new copy | 200 | 62.447 / 70.503 | 26.173 / 87.876 | 44.361 / 49.295 | 0.773 / 3.417 |
| Object, warm copy | 200 | 62.130 / 72.266 | 24.069 / 34.277 | 49.323 / 55.436 | 1.158 / 3.148 |
| Native | 200 | 62.333 / 68.142 | 23.997 / 32.279 | 48.944 / 55.267 | 1.027 / 2.256 |

Each tail-wait cell contains 200 logged rebuild laps. The process monitor saw one active host
compiler during part of the accepted Object paced-new window. That cell had wider tails than the
Native run. In an earlier Object replay, the complete paced-new cell was 26.206 / 38.131 ms
(200 opens), and the monitor sampled no active compiler in that paced-new window. The earlier
run stopped when the collection check found 199 warm-copy rebuild log entries. Both paced-new
cells give a similar p50, while the p99 varies across windows.
[Earlier Object paced-new samples](database-home-benchmark/object-cold-follow-up-preflight.jsonl)

[Object follow-up samples](database-home-benchmark/object-cold-follow-up.jsonl) ·
[Native follow-up samples](database-home-benchmark/native-cold-follow-up.jsonl)

### Durable-child CPU

| Operation | Samples per side | Object CPU ms/query (ticks) | Native CPU ms/query (ticks) |
| --- | ---: | ---: | ---: |
| One-row read | 1000 | 0.57 (57) | 0.33 (33) |
| Large read | 1000 | 2.71 (271) | 5.56 (556) |
| No-op query | 1000 | 0.58 (58) | 0.34 (34) |
| Empty batch | 1000 | 0.53 (53) | 0.32 (32) |

### Idle durable-process memory

| Open databases | RSS / PSS samples per side | Object RSS / PSS (MiB) | Native RSS / PSS (MiB) |
| ---: | ---: | ---: | ---: |
| 0 | 5 | 46.87 / 36.73 | 46.08 / 36.00 |
| 100 | 5 | 331.11 / 308.39 | 73.56 / 61.95 |
| 500 | 5 | 1292.30 / 1269.59 | 162.39 / 150.78 |

| Interval | Object RSS / PSS (MiB per database) | Native RSS / PSS (MiB per database) |
| --- | ---: | ---: |
| 0–100 | 2.842 / 2.717 | 0.275 / 0.260 |
| 100–500 | 2.403 / 2.403 | 0.222 / 0.222 |
| 0–500 | 2.491 / 2.466 | 0.233 / 0.230 |

### Default-budget admission

A separate pair used the VM-derived **1.55 GiB** process budget (1,664,303,104 bytes). It kept the
same 520 home threads and binding paths. The Native run was first from 15:02:59 to 15:03:55 UTC+8,
followed by Object from 15:04:03 to 15:04:59. The same 15-second resource snapshots found no compilers.
Both runs seeded the tables in two batches and rotated the durable child between batches. The final
child reopened the stored rows in under two seconds. This let the probes reach admission pressure
before the ten-second idle retirement window.

| Side | Simultaneous live roots / homes | Validated reads / attempts | What happened at capacity | Default-budget bound or measured reservation per live root | Matched warm RSS slope above |
| --- | ---: | ---: | --- | --- | ---: |
| Object | 372 / 372 | 365 / 388 | 23 reads in the first refused batch returned HTTP 503 with `OVERLOADED: process admission budget is full` | ≤4.267 MiB from total process budget divided by 372 observed roots | 2.491 MiB |
| Native | 519 / 519 | 521 / 521 | The final two opens succeeded. The observed database-home count stayed at 519. | 0.235 MiB measured (224.30 KiB native home and SQLite plus 16 KiB live-root charge) | 0.233 MiB |

The Object side's live root count includes roots whose read was refused after placement. The
365 successful reads establish a lower bound on usable open databases in that run. Dividing
1,664,303,104 bytes by 372 gives the aggregate budget upper bound, including shared charges.
The 2.50 MiB initial isolate charge is the source's 2 MiB heap seed plus 512 KiB fixed native overhead. Source
bytes and observed heap, external allocations and logs update the process charge. The published
Object lifecycle exposes the 16 KiB tree charge and live roots without exposing the full per-isolate
process charge. The Native lifecycle reported 119,203,920 bytes reserved across 519 database homes,
or 229,680 bytes per native home, in addition to the same tree charge. The reported native
idle and pressure retirement counters stayed at zero. The measurements establish the load and
plateau. The mechanism by which slots became available remains unobserved. The source reserves
one of the 520 places for Durable Objects.
[Object isolate accounting](https://github.com/coworkerd/coworkerd/blob/3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd/patches/workerd/0042-threaded-server-s1a.patch#L1898-L1924) ·
[Tree charge](https://github.com/coworkerd/coworkerd/blob/3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd/patches/workerd/0047-durable-test-events.patch#L350-L357) ·
[Native admission](https://github.com/coworkerd/coworkerd/blob/e88acea9a3630c67388dbccdd994b4d843a953eb/patches/workerd/0114-native-sql-spill-and-idle-homes.patch#L805-L836) ·
[Native reservation counter](https://github.com/coworkerd/coworkerd/blob/e88acea9a3630c67388dbccdd994b4d843a953eb/patches/workerd/0114-native-sql-spill-and-idle-homes.patch#L921-L925)

The initial default-budget density attempt reached the same refusal while creating the 344th
database. Its 48-client setup started more requests in parallel and included earlier latency and
throughput phases in the same child. The fresh staged sweep supplies the live count on a quieter
path. [Initial status](https://github.com/coworkerd/coworkerd/issues/79#issuecomment-7155260041)

[Object admission samples](database-home-benchmark/object-default-admission.jsonl) ·
[Native admission samples](database-home-benchmark/native-default-admission.jsonl)

### Predeclared decision criteria

Latency reductions are Object minus Native, with percentage relative to Object.

| Eligible warm cell | Native reduction (ms) | Native reduction (%) | At least 5 ms and 30% |
| --- | ---: | ---: | --- |
| One-row read p50 | +0.257 | +16.6% | No |
| One-row read p99 | +0.501 | +15.0% | No |
| Confirmed insert p50 | +0.278 | +13.7% | No |
| Confirmed insert p99 | -0.135 | -2.7% | No |

- **Latency threshold unmet.** The eligible cells are shown above. The large read is reported separately.
- **Throughput threshold unmet.** The best Native-to-Object ratio was 1.533×. The threshold is 2×.
- **Density threshold unmet.** The Object slope exceeded Native by 2.258 MiB per database.
  The paced Object minus Native cold-new p50 was +2.176 ms. Both 4 MiB and 20 ms are required.

## Idle window for exclusive platform database copies

Release the exclusive isolate copy when its database tree retires, about 10 seconds after the last
request finishes on a quiet database. On the #90 baseline, keeping empty copies for the common
five-minute grace period exhausted the process admission budget during a serial scan of distinct
databases. Releasing a database copy with its tree gives new databases access to that budget.

Reopening SQLite in a retained copy saved 9.50 ms at p50 and 12.80 ms at p99 compared with reopening
after full copy release. Closing all 64 already-empty copies recovered 24.73 MiB of RSS at the first
post-release sample, or 0.39 MiB per database. The retained copy avoids module and isolate setup for
reuse during the grace period. The policy pays the full reopen cost after idle retirement to
prevent empty copies from blocking new opens. These latency results cover one small,
already-confirmed database per isolate and one serial caller. The frequency of revisits to idle
databases in production remains unmeasured.

### Measurement

The benchmark ran on November 26, 2026, from 02:22:01 to 02:25:23 (UTC+8) on an Apple M4 Mac with
10 logical CPUs and 24 GiB RAM, on battery power. The Linux ARM64 Docker Desktop VM had 10 vCPUs
and 7.75 GiB RAM. Docker Engine was 29.5.3 and the Node.js 26.2.0 client used loopback HTTP. The
phase samples showed 5.70–5.89 GiB of guest RAM available and 7.4% aggregate guest CPU busy time.
The two other running containers were idle at preflight. The run reused the daemons and the hosted
ARM64 artifact from [#90](https://github.com/coworkerd/coworkerd/pull/90).

- Product source `de8d51814b1b558447ee4ece6c3f8be0342a6da0`, built for ARM64 Linux with Clang 19
  in Release mode. Only the cluster test executable exposes the threaded database mapping.
- Kernel artifact from [run 45638096483, attempt 1](https://github.com/coworkerd/coworkerd/actions/runs/45638096483/attempts/1).
  Archive SHA-256 `d3ed82446c09aa602387a2f033f2a387fee5da5497598b66f8816b4d5f985de2`.
  The ordered patch series SHA-256 was `2db2b5d643b55d5a493509cc192748c12dbea5c15b6a955f2d4616c49a1fd64d`.
  The patched workerd tree was `f5c0caf5d5023a29e792f4a4fbe649905adcf630`.
- One node and one threaded durable child, with 80 test home threads, a 1.55 GiB child admission
  budget and local WAL confirmation. The driver created 64 databases and confirmed 128 writes
  to make a small SQLite table with one row in each database. It rotated the child once before
  sampling. All rows below use the same durable PID and generation after that rotation.
- Five idle samples from `/proc/PID/smaps_rollup` per row, with medians in MiB. The driver checked
  root and home counts, zero in-flight work, and zero pressure retirements around each window.
  The 1–64 slope excludes the first database's process warm-up. For comparison, #84's stand-in
  measured a 0–500 RSS slope of 2.49 MiB per open database.

| Sample | Resident roots | Loaded homes | RSS (MiB) | PSS (MiB) |
| --- | ---: | ---: | ---: | ---: |
| After child rotation | 0 | 0 | 27.60 | 17.50 |
| First database open | 1 | 1 | 59.75 | 37.15 |
| Open cohort | 32 | 32 | 145.05 | 122.45 |
| Open cohort | 64 | 64 | 232.47 | 209.87 |
| Trees retired, copies retained | 0 | 64 | 230.54 | 207.94 |
| Copies released, first wave | 0 | 0 | 205.80 | 183.11 |
| Copies released, final wave | 0 | 0 | 215.04 | 192.34 |

The marginal cost from 1 to 64 open databases was 2.74 MiB per database for both RSS and PSS.
The 0–64 slopes were 3.20 MiB RSS and 3.01 MiB PSS. The process returned only 0.39 MiB of the
2.74 MiB per database at the first release. Its RSS still reflected most of the high-water footprint
after all copies were gone. Copy release returns the isolate's admission reservation while the
operating system's resident-memory observation can stay elevated.

The timed profile set a 12-second copy grace period to reach both idle states repeatedly. Across
four rounds, it waited first for zero trees with 64 loaded copies and then for zero trees and
zero loaded copies. The benchmark warmed the ordinary calling Workers with a request that did
no SQL before each serial sweep. For each database it timed the first primary-key read and
the immediate repeat, from starting host HTTP to reading the full response past the real D1
binding parse. It checked the saved row on every reply. Each sweep added exactly 64 SQLite home
opens and zero rebuilds. Both conditions include 256 pairs. Percentiles use nearest rank.

| Read path | First read p50 / p99 (ms) | Immediate repeat p50 / p99 (ms) | Paired first-minus-repeat p50 / p99 (ms) |
| --- | ---: | ---: | ---: |
| SQLite reopen in retained copy | 3.68 / 5.95 | 2.92 / 4.71 | +0.75 / +2.55 |
| SQLite reopen after full copy release | 13.17 / 18.75 | 3.27 / 9.57 | +9.86 / +14.45 |

With the shortened timer, all 64 copies were gone 22.53–22.71 seconds after the last response.
The time includes tree retirement, the copy grace period and the polling interval. The first
requests to new data epochs in #84 also ran through shard-tail recovery, which is a separate
path from these reopens of confirmed local data in the same generation.

A separate probe left the #90 timer at its 300,000 ms default. It observed all 64 trees retired and
all 64 copies loaded around 10.56 seconds after the last read. In the subsequent wave it still
observed loaded copies 310.49 seconds after the last read and saw them all gone at 310.71 seconds.
The home starts the copy timer when the last tree closes.

[Raw idle-window samples](database-home-benchmark/platform-database-idle-window.jsonl) ·
[Raw default-timer samples](database-home-benchmark/platform-database-idle-default.jsonl)

### Admission with the default budget

The admission probe created 1,000 distinct database namespaces and waited for the owner to
apply their catalog revision. It sent `SELECT 1` once to each database through the public
SQL route, with one query in flight and no client retries. It kept the 1.55 GiB child admission
budget and the default idle timers and used the same 520-home test setting as #84. The driver
recorded the durable PID and generation, sampled RSS and checked the home and root counts
every 25 queries and at the first refusal. The paced run spaced starts by at least 50 ms so
earlier roots could retire before the active cohort reached the previously observed ceiling.

| #90 baseline | Duration | Successful queries | Peak sampled roots | Final roots / homes | Peak sampled RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Serial, unpaced | 15.44 s | 405 / 1,000 | 396 | 210 / 406 | 1,060.44 MiB |
| Serial, 50 ms between starts | 50.88 s | 405 / 1,000 | 202 | 0 / 405 | 1,011.27 MiB |

Both runs first refused query 406. Each of the remaining 595 returned HTTP 400 on this
route with `OVERLOADED: process admission budget is full` in the reason. In the paced run
there were 201 live roots and 405 occupied homes at the first refusal. At the end all
405 successful queries' trees had retired and their copies were still loaded. No pressure
retirements or isolate resets were recorded. RSS stayed below the nominal admission budget
while copy reservations exhausted it. The home picker preferred unused homes and could
reuse an empty home once no unused home remained. The retained copies consumed the
admission budget before reaching that reuse path.

The policy returns an exclusive database copy's reservation on the same home when the
last tree closes, after queued and active reservations clear. Copies therefore give way
on tree retirement. In this serial scan, earlier requests become eligible first.

[Raw unpaced admission samples](database-home-benchmark/platform-database-admission-unpaced.jsonl) ·
[Raw paced admission samples](database-home-benchmark/platform-database-admission-paced.jsonl)

In a diagnostic on the same #90 kernel, a 2 ms common copy grace period let all 1,000 paced
queries succeed with peak RSS of 592.27 MiB. The diagnostic elapsed time was 50.878 s.
The paced baseline took 50.881 s and recorded 595 refusals. Both round to 50.88 s.
[Raw short-timer diagnostic](database-home-benchmark/platform-database-admission-short-timer.jsonl)

### Verification with patch 0174

On November 26, 2026, from 05:57:31 to 06:00:46 (UTC+8), the same ARM64 Linux VM ran the paced
scan and the idle profile with the reviewed combined kernel. The kernel came from product
`383b811df13db3893857c234f61697e703604575` and workerd tree
`58412bd065cd24df493e0aad6e72e5733bce90cb`. The ARM64 archive SHA-256 was
`286a42c05d043fb81a3b390f8a867c928102b7c0ea7f43e77d60a4ab4b285e53`.
The Linux daemons were built at `4c33e3b872e2b063bfc58b3010d82c655b7833ba` with
the same daemon and runtime sources. [Hosted ARM64 build](https://github.com/coworkerd/coworkerd/actions/runs/45688983959/job/139469141727)

All **1,000 / 1,000** paced SQL queries succeeded in **51.016 s** with the default
1.55 GiB admission budget and both idle timers at their defaults. Peak sampled RSS was
571.47 MiB. The scan saw at most 202 live roots and 203 occupied homes, and 797 idle
retirements by the final sample. The durable child kept its PID and generation.

The default-settings D1 profile reopened 64 databases in four rounds. All copies
were gone **10.39–10.60 s** after the last reply of each wave, including the polling
interval. A complete reopen added **9.46 ms at p50** and **14.38 ms at p99** over the
immediate warm repeat. Each condition had 256 samples. Each pair reopened one
SQLite home and read the confirmed row. The profile recorded zero rebuilds.

| Read path | p50 / p99 (ms) |
| --- | ---: |
| First read after copy release | 12.68 / 18.15 |
| Immediate warm repeat | 3.24 / 9.78 |
| Paired first-minus-repeat | +9.46 / +14.38 |

| Sample | Resident roots / loaded homes | RSS (MiB) | PSS (MiB) |
| --- | ---: | ---: | ---: |
| After child rotation | 0 / 0 | 24.27 | 14.29 |
| First database open | 1 / 1 | 57.31 | 34.87 |
| Open cohort | 32 / 32 | 144.82 | 122.38 |
| Open cohort | 64 / 64 | 238.73 | 216.29 |
| After first release wave | 0 / 0 | 208.46 | 186.02 |
| After final release wave | 0 / 0 | 167.52 | 145.08 |

In the patched profile, the 1–64 marginal slope was 2.88 MiB per open database for both
RSS and PSS. The first release lowered cohort RSS by 30.27 MiB, approximately 0.47 MiB
per database. RSS after release stayed above the pre-open sample.

[Patched paced scan samples](database-home-benchmark/platform-database-admission-patched.jsonl) ·
[Patched default-settings profile](database-home-benchmark/platform-database-idle-window-patched.jsonl)

### Reproduce the idle-window profile

Run the Object-side driver with the placement test executable and the matching unpacked kernel.
Build `coworkerd-platform-test` alongside the other Linux daemon targets from the #90 baseline
above. The profile runs on the baseline that retains an empty copy. Finish builds before starting
the timed run.

```sh
COWORKERD_CLUSTER_BIN_DIR="$PLACEMENT/build" node tests/cluster/database-home-benchmark.mjs \
  object "$PLACEMENT" "$KERNEL_DIR" "$PWD/platform-database-idle-window.jsonl" --idle-window
```

Use `--idle-window-default` and a new output filename to repeat the single-round probe with the
unmodified baseline timer.

Use `--idle-window-admission` and a new output filename to repeat the paced 1,000-query scan.
This mode accepts either kernel with its matching product checkout. It writes the full scan
before returning an error if any query fails or sampled RSS reaches the budget.

Use `--idle-window-patched` on the kernel with 0174 to measure the released and warm paths
with the default copy settings. The probe checks that all exclusive copies disappear within
15 seconds of a cohort's last reply and records their observed retirement times.

## Database admission at the ceiling

The unpaced workload issues a public SQL read against each of 1,000 distinct databases.
It waits for each reply before sending the next query. It uses the default home count,
memory budget and idle timers, records admission retirements, and checks that all queries
succeed in the same durable process generation. The process budget is derived from the
test node's memory budget.

The memory probe runs 125 queries with a 448 MiB process budget and 192 homes. It checks
that a new copy can reclaim a memory reservation while unused homes are still available.
The busy probes hold a long SQL read on each default home, observe the running count
per process, and submit one additional query per process to verify its capacity refusal.

```sh
export COWORKERD_CLUSTER_BIN_DIR="$PWD/build"
for mode in unpaced memory busy-one busy-two; do
  node tests/cluster/database-home-benchmark.mjs \
    object "$PWD" "$KERNEL_DIR" "$PWD/admission-$mode.jsonl" "--idle-admission-$mode"
done
node tests/cluster/run.mjs "$KERNEL_DIR" durable-threaded-database-admission
```

## Reproduce

Run from a checkout containing the benchmark. Make two checkouts at the exact daemon source commits above. On
Linux, build the daemon and launcher targets in each checkout as described in [Contributing](../CONTRIBUTING.md).
On a Mac, build those targets inside an ARM64 Linux container. Use a release build and verification inputs from
each checkout. The benchmark uses the checked-in Docker Compose topology's single-node profile.

```sh
# Run from the benchmark checkout. Set OBJECT and NATIVE to absolute paths.
for side in object native; do
  case "$side" in object) checkout="$OBJECT" ;; native) checkout="$NATIVE" ;; esac
  kernel=$(cd "$checkout" && python3 scripts/fetch-kernel.py)
  COWORKERD_CLUSTER_BIN_DIR="$checkout/build" node tests/cluster/database-home-benchmark.mjs \
    "$side" "$checkout" "$kernel" "$PWD/database-home-$side.jsonl"
done
```

The driver uses a fresh Compose project per side, and refuses to overwrite an existing recording. It emits the
input pins, binary hashes, sample times, latency arrays, process counters, resource snapshots and the phase at
which a run fails. Use `--smoke` for a smaller verification run. The smoke data do not meet the reporting sample
counts.

Append `--default-admission` and use a new output filename for the default-budget pair. The memory
planning override is omitted for that mode. The daemon uses a hard cgroup memory limit in preference
to the planning budget when one is configured.

Use `--cold-follow-up` for the one-client read control before any large or cold queries, then
the unpaced and paced cold cells on distinct database IDs. The raw follow-up files include
rebuild and tail-wait durations from the node for each cold cell.

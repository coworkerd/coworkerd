# SQLite Durable Objects and native database homes

The benchmark measures two threaded durable paths. Both use a stateless Worker binding on the same
machine. One node owns all sampled databases and confirms writes by flushing its local WAL. The
comparison uses the predeclared thresholds. [#79](https://github.com/coworkerd/coworkerd/issues/79)

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
  final count. The sample uses Linux `fdatasync` on a Docker named volume. Network replication and peer
  acknowledgements may change write latency and throughput.
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

### End-to-end latency

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

### Throughput and durable-child CPU

| Operation | Clients | Object completions | Object /s | Native completions | Native /s | Native ÷ Object |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| One-row read | 1 | 4,623 | 462.22 | 7,088 | 708.72 | 1.533× |
| One-row read | 8 | 13,005 | 1,299.99 | 12,666 | 1,266.10 | 0.974× |
| Confirmed insert | 1 | 4,618 | 461.71 | 5,248 | 524.47 | 1.136× |
| Confirmed insert | 8 | 7,183 | 717.89 | 7,415 | 740.74 | 1.032× |

The Object and Native traffic tables contained all 12,844 and 13,706 expected confirmed rows,
respectively, including warm-ups. The measured windows completed with zero request failures.

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

| Side | Simultaneous live roots / homes | Validated reads / attempts | What happened at capacity | Reservation per home or root | Matched warm RSS slope above |
| --- | ---: | ---: | --- | --- | ---: |
| Object | 372 / 372 | 365 / 388 | 23 reads in the first refused batch returned HTTP 503 with `OVERLOADED: process admission budget is full` | 16 KiB metered per live root, plus a 2.50 MiB initial isolate charge before source bytes and later heap and external allocations | 2.491 MiB |
| Native | 519 / 519 | 521 / 521 | The final two opens succeeded and the database-home count stayed at 519. The kernel reclaims idle native homes and reserves one of the 520 places for Durable Objects. | 224.30 KiB measured SQLite plus native home overhead, and 16 KiB metered per live root | 0.233 MiB |

The Object side's live root count includes roots whose read was refused after placement. The
365 successful reads establish a lower bound on usable open databases in that run. The 2.50 MiB
initial isolate charge is the source's 2 MiB heap seed plus 512 KiB fixed native overhead. Source
bytes and observed heap, external allocations and logs update the process charge. The published
Object lifecycle exposes the 16 KiB tree charge and live roots without exposing the full per-isolate
process charge. The Native lifecycle reported 119,203,920 bytes reserved across 519 database homes,
or 229,680 bytes per native home, in addition to the same tree charge.
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
  The Object minus Native cold-new p50 was -0.068 ms. Both 4 MiB and 20 ms are required.

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

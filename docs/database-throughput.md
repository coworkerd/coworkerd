# What limits database throughput on one node

On three standard Ubuntu runners, the **node daemon's main thread** set the ceiling for one-row
reads through the SQLite Durable Object path. One database reached 738 reads/s on the current
checkout. Eight databases through eight Workers shared the same node and reached 709 reads/s.
At 32 clients the main thread used 91–94% of one core across those two layouts and three runners.
The client used 14–17% of one core, and the busiest durable thread fell from about half a core
with one database to 8–9% with eight. The whole runner used at most 2.79 of its four cores in
these windows. The one-Worker control alone sustained a median of 2,063 replies/s, and a
no-op Durable Object call still took the node main thread to 94%. Moving work across eight
Workers and durable homes left the node as the shared stage. [#85](https://github.com/coworkerd/coworkerd/issues/85)

| Product | One database reads/s | One node, eight databases reads/s | One database inserts/s | One node, eight databases inserts/s |
| --- | ---: | ---: | ---: | ---: |
| Main Object | 738 | 709 | 578 | 563 |
| #84 Object | 746 | 699 | 570 | 552 |
| #84 Native | 594 | 806 | 485 | 624 |

These are the highest median rates among the tested client counts for each layout and operation.
The full tables show the spread across instances.

- **Native reads.** Eight databases at eight clients raised throughput on every runner, from
  348 to 438, 594 to 806, and 636 to 849 reads/s. Increasing the single database from eight
  to 32 clients left its throughput flat while the node main thread stayed at 78–82% and the
  durable home stayed at 30–34%. That pattern points to a limit on the single-home path, with
  the particular source of waiting unresolved. With eight Native databases the node main thread
  reached 90–93% while the hottest durable thread used about 5%.
- **Confirmed inserts.** The highest measured Object insert rates left the node main thread
  below 86%. The same runs showed Linux I/O wait in the insert windows and almost none in the
  read windows. With zero remote WAL peers, the node's confirmation path calls `fdatasync`
  before acknowledging writes. Waiting for local WAL confirmation is a likely contributor to
  the write plateau. The benchmark measures confirmation latency and CPU, so it cannot
  separately time the flush or other waits on the write path.
- **Scope.** The earlier figure of about 1,300 reads/s came from the Mac-to-Docker Desktop path
  with a different client and hardware. These measurements identify the limiting stages on the
  Linux runners. The earlier records contain too little CPU information to assign that exact
  desktop figure to a stage. [Original throughput comparison](database-home-benchmark.md#what-the-throughput-measures)

## Measurement setup

Three independent standard GitHub-hosted Ubuntu 24.04 x86-64 instances each ran the full matrix with
identical release binaries built in separate jobs. Each instance had four vCPUs and about 16 GiB of guest memory. Each product ran in
a fresh single-node Compose project. Product order rotated across the instances. Docker used a named
volume for the node's data. [Runner details and workflow artifacts](https://github.com/coworkerd/coworkerd/actions/runs/45637644215)

The HTTP client resolved `node-1` on the Compose frontend and used its ingress port directly.
Each connection had one request outstanding. The controller completed deployment and seeding before
timing. Each cell used a 1.5-second warm-up and a 10-second issuing interval, with the last in-flight
replies included in the elapsed time. The one-Worker baseline returned the read's JSON body. The object
baseline made one fetch to a Durable Object that returned the same body. The SQL read selected one row
by its primary key. The insert acknowledged one added row and waited for local WAL confirmation. The
driver checked each inserted row count and the confirmation counters after every insert window. The
node used zero remote WAL peers.

| Product | Daemon source | Database path |
| --- | --- | --- |
| Main Object | [`abab77c`](https://github.com/coworkerd/coworkerd/tree/abab77c7bdc5373743e6b5195d6ac83e4f2e29d0) | The #84 SQLite Durable Object fixture on the current node and kernel |
| #84 Object | [`3cd208d`](https://github.com/coworkerd/coworkerd/tree/3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd) | SQLite Durable Object through a namespace binding |
| #84 Native | [`e88acea`](https://github.com/coworkerd/coworkerd/tree/e88acea9a3630c67388dbccdd994b4d843a953eb) | D1 binding to the native SQLite home in the #59 experiment |

The #84 pair reuses the exact product revisions from the original Docker Desktop experiment and their
published Linux **x86-64** archives. The current Object trial reruns the same fixture on the recorded
revision of main. The JSONL provenance identifies the archives and binaries by SHA-256. Rebuilds,
container startup, image pulls, and post-run checks fall outside the windows.
[Original comparison](database-home-benchmark.md) · [Reproduction commands](../tests/cluster/README.md#database-throughput-and-cpu-attribution)

| Instance | CPU model | vCPUs | Guest memory | Runner image | Linux kernel | Trial order |
| --- | --- | ---: | ---: | --- | --- | --- |
| 1 | AMD EPYC 7763 | 4 | 15.61 GiB | 20261116.376.1 | 7.0.0-1017-azure | Main, Object, Native |
| 2 | Intel Xeon 6975P-C | 4 | 15.33 GiB | 20261116.133.1 | 7.0.0-1014-aws | Object, Native, Main |
| 3 | Intel Xeon 6975P-C | 4 | 15.33 GiB | 20261122.23.1 | 7.0.0-1018-aws | Native, Main, Object |

Docker Engine 28.0.4 ran on each guest. The runner agent, Docker and the cluster's control plane,
health checks and object store shared the guest with the benchmark. Each measurement job ran one
cluster at a time and reused the same product binaries as the other two jobs. Host busy CPU peaked
at 3.28 of four cores over all 144 windows. Reported guest steal time was at most 0.03% of aggregate
CPU time. Instance 1 delivered the lowest rates in most cells even at broadly similar CPU utilization.
The experiment did not separate CPU model, virtualization and other machine differences.

All 144 timed windows and their warm-ups completed without request failures. The calibration runs
reached the node and process admission ceilings while requests whose HTTP replies had arrived were
still retiring. The measured fixture uses higher admission ceilings for that reason. It sets the
node's root and nested limits to 512 and 1,024, the process in-flight limit to 512, and the durable
mailbox limits to 64 events and 1 MiB. [Calibration run 1](https://github.com/coworkerd/coworkerd/actions/runs/45631888750) ·
[Calibration run 2](https://github.com/coworkerd/coworkerd/actions/runs/45634696226)

The raw records include per-window throughput, latency, process and thread CPU, guest CPU and pressure
counters, and write confirmation deltas. [Instance 1](database-throughput/runner-1.jsonl) ·
[Instance 2](database-throughput/runner-2.jsonl) · [Instance 3](database-throughput/runner-3.jsonl) ·
[Measurement workflow](https://github.com/coworkerd/coworkerd/blob/f9e97002245a904f3e0b8b7e7d0ebba927033ba3/.github/workflows/database-throughput-measure.yml)

## Results by cell

Each rate is the median of the three runner instances and the brackets give the minimum and maximum.
Each process CPU column reports median user plus system CPU as a percentage of one logical CPU. The
node column also shows its main thread. The workerd and durable columns also show the busiest thread
present at both endpoints of the window. Process and thread counters are separate reads of Linux
CPU ticks, so rounding can differ. Ordinary workerd can schedule invocations of the same Worker on
different threads, so its process total exceeds a single thread's use.

The last numeric column reports busy CPU cores for each instance in order. It comes from the runner's
aggregate Linux CPU counters and includes the rest of the runner's work and the kernel's interrupt time.
Idle, I/O wait and steal time are recorded separately in the JSONL. The host figures use four available
logical CPUs, and the node and client containers had no CFS CPU quota.

### Main Object

| Path | Databases/Workers | Clients | Replies/s, med [min–max] | Node (all/main) | workerd (all/hot) | Durable (all/hot) | Client | Host busy cores (1/2/3 of 4) | Attribution |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Worker only | 0 / 1 | 1 | 1,181 [893–1,202] | 56% / 56% | 68% / 49% | 0% / 0% | 24% | 1.64 / 1.56 / 1.55 | One request in flight |
| Worker only | 0 / 1 | 8 | 2,063 [1,443–2,111] | 86% / 86% | 137% / 9% | 0% / 0% | 28% | 2.74 / 2.79 / 3.01 | Approaching node limit |
| Worker only | 0 / 1 | 32 | 2,016 [1,315–2,278] | 93% / 93% | 134% / 20% | 0% / 0% | 30% | 2.91 / 2.87 / 2.85 | Node main near limit |
| Object no-op | 0 / 1 | 1 | 402 [233–412] | 55% / 55% | 44% / 40% | 26% / 23% | 12% | 1.70 / 1.50 / 1.49 | One request in flight |
| Object no-op | 0 / 1 | 8 | 747 [410–785] | 88% / 88% | 92% / 9% | 43% / 38% | 17% | 2.56 / 2.53 / 2.55 | Approaching node limit |
| Object no-op | 0 / 1 | 32 | 840 [408–928] | 94% / 94% | 93% / 3% | 41% / 36% | 16% | 2.59 / 2.54 / 2.59 | Node main near limit |
| Read | 1 / 1 | 1 | 354 [190–359] | 53% / 53% | 38% / 35% | 35% / 31% | 11% | 1.54 / 1.46 / 1.46 | One request in flight |
| Read | 1 / 1 | 8 | 679 [361–701] | 89% / 89% | 81% / 9% | 60% / 54% | 16% | 2.69 / 2.61 / 2.62 | Approaching node limit |
| Read | 1 / 1 | 32 | 738 [449–743] | 92% / 93% | 85% / 3% | 56% / 49% | 14% | 2.67 / 2.58 / 2.59 | Node main near limit |
| Read | 8 / 8 | 8 | 687 [452–704] | 88% / 88% | 81% / 8% | 73% / 8% | 16% | 2.79 / 2.72 / 2.74 | Approaching node limit |
| Read | 8 / 8 | 32 | 709 [462–752] | 94% / 94% | 85% / 2% | 69% / 8% | 16% | 2.79 / 2.75 / 2.77 | Node main near limit |
| Insert | 1 / 1 | 1 | 301 [222–309] | 52% / 52% | 34% / 31% | 34% / 29% | 10% | 1.47 / 1.38 / 1.39 | One request in flight |
| Insert | 1 / 1 | 8 | 545 [365–554] | 79% / 78% | 65% / 6% | 52% / 45% | 14% | 2.37 / 2.24 / 2.28 | CPU headroom, WAL wait inferred |
| Insert | 1 / 1 | 32 | 578 [369–601] | 85% / 84% | 69% / 4% | 55% / 47% | 13% | 2.39 / 2.35 / 2.39 | CPU headroom, WAL wait inferred |
| Insert | 8 / 8 | 8 | 563 [355–564] | 80% / 80% | 66% / 6% | 64% / 7% | 14% | 2.43 / 2.41 / 2.42 | CPU headroom, WAL wait inferred |
| Insert | 8 / 8 | 32 | 562 [334–576] | 83% / 82% | 68% / 2% | 60% / 7% | 13% | 2.37 / 2.39 / 2.36 | CPU headroom, WAL wait inferred |

### #84 Object

| Path | Databases/Workers | Clients | Replies/s, med [min–max] | Node (all/main) | workerd (all/hot) | Durable (all/hot) | Client | Host busy cores (1/2/3 of 4) | Attribution |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Worker only | 0 / 1 | 1 | 1,192 [882–1,199] | 56% / 56% | 67% / 50% | 0% / 0% | 24% | 1.63 / 1.57 / 1.56 | One request in flight |
| Worker only | 0 / 1 | 8 | 2,046 [1,279–2,170] | 82% / 82% | 136% / 9% | 0% / 0% | 29% | 3.11 / 2.75 / 3.00 | Approaching node limit |
| Worker only | 0 / 1 | 32 | 2,031 [1,406–2,148] | 95% / 95% | 137% / 20% | 0% / 0% | 30% | 2.89 / 3.24 / 2.83 | Node main near limit |
| Object no-op | 0 / 1 | 1 | 402 [287–413] | 55% / 55% | 45% / 40% | 27% / 23% | 12% | 1.57 / 1.54 / 1.47 | One request in flight |
| Object no-op | 0 / 1 | 8 | 759 [499–782] | 88% / 88% | 88% / 8% | 43% / 38% | 17% | 2.60 / 2.47 / 2.53 | Approaching node limit |
| Object no-op | 0 / 1 | 32 | 852 [534–917] | 94% / 94% | 94% / 3% | 42% / 37% | 16% | 2.66 / 2.55 / 2.60 | Node main near limit |
| Read | 1 / 1 | 1 | 355 [250–356] | 53% / 53% | 38% / 35% | 35% / 31% | 11% | 1.53 / 1.46 / 1.47 | One request in flight |
| Read | 1 / 1 | 8 | 692 [373–712] | 91% / 91% | 82% / 10% | 64% / 58% | 17% | 2.68 / 2.71 / 2.65 | Approaching node limit |
| Read | 1 / 1 | 32 | 746 [378–750] | 92% / 92% | 87% / 2% | 56% / 50% | 15% | 2.66 / 2.61 / 2.62 | Node main near limit |
| Read | 8 / 8 | 8 | 673 [379–689] | 88% / 88% | 81% / 8% | 72% / 8% | 16% | 2.75 / 2.72 / 2.73 | Approaching node limit |
| Read | 8 / 8 | 32 | 699 [370–703] | 94% / 94% | 86% / 2% | 68% / 8% | 15% | 2.72 / 2.75 / 2.75 | Node main near limit |
| Insert | 1 / 1 | 1 | 306 [173–306] | 52% / 52% | 33% / 30% | 34% / 29% | 10% | 1.49 / 1.39 / 1.39 | One request in flight |
| Insert | 1 / 1 | 8 | 543 [295–554] | 79% / 79% | 65% / 6% | 52% / 45% | 14% | 2.26 / 2.26 / 2.25 | CPU headroom, WAL wait inferred |
| Insert | 1 / 1 | 32 | 570 [298–600] | 85% / 84% | 69% / 5% | 54% / 47% | 13% | 2.33 / 2.37 / 2.41 | CPU headroom, WAL wait inferred |
| Insert | 8 / 8 | 8 | 550 [294–570] | 80% / 80% | 65% / 6% | 64% / 7% | 14% | 2.43 / 2.39 / 2.44 | CPU headroom, WAL wait inferred |
| Insert | 8 / 8 | 32 | 552 [297–587] | 82% / 82% | 68% / 2% | 59% / 7% | 13% | 2.39 / 2.38 / 2.38 | CPU headroom, WAL wait inferred |

### #84 Native

| Path | Databases/Workers | Clients | Replies/s, med [min–max] | Node (all/main) | workerd (all/hot) | Durable (all/hot) | Client | Host busy cores (1/2/3 of 4) | Attribution |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Worker only | 0 / 1 | 1 | 1,186 [768–1,203] | 56% / 56% | 68% / 49% | 0% / 0% | 24% | 1.64 / 1.56 / 1.56 | One request in flight |
| Worker only | 0 / 1 | 8 | 2,147 [1,010–2,221] | 87% / 87% | 145% / 9% | 0% / 0% | 29% | 3.06 / 2.73 / 2.75 | Approaching node limit |
| Worker only | 0 / 1 | 32 | 2,001 [1,186–2,110] | 95% / 95% | 131% / 21% | 0% / 0% | 31% | 2.82 / 2.94 / 3.28 | Node main near limit |
| Object no-op | 0 / 1 | 1 | 402 [215–421] | 55% / 55% | 44% / 41% | 26% / 23% | 12% | 1.53 / 1.49 / 1.50 | One request in flight |
| Object no-op | 0 / 1 | 8 | 748 [396–817] | 88% / 88% | 88% / 10% | 43% / 37% | 18% | 2.60 / 2.52 / 2.61 | Approaching node limit |
| Object no-op | 0 / 1 | 32 | 866 [429–935] | 94% / 94% | 94% / 2% | 43% / 37% | 16% | 2.63 / 2.57 / 2.59 | Node main near limit |
| Read | 1 / 1 | 1 | 439 [263–451] | 59% / 59% | 48% / 44% | 23% / 20% | 13% | 1.61 / 1.53 / 1.51 | One request in flight |
| Read | 1 / 1 | 8 | 594 [348–636] | 81% / 81% | 73% / 8% | 35% / 31% | 17% | 2.28 / 2.15 / 2.18 | Single-home limit inferred |
| Read | 1 / 1 | 32 | 570 [345–617] | 80% / 80% | 72% / 2% | 36% / 32% | 16% | 2.25 / 2.15 / 2.16 | Single-home limit inferred |
| Read | 8 / 8 | 8 | 806 [438–849] | 90% / 90% | 95% / 9% | 43% / 5% | 16% | 2.63 / 2.60 / 2.64 | Approaching node limit |
| Read | 8 / 8 | 32 | 758 [385–814] | 92% / 92% | 94% / 3% | 43% / 5% | 17% | 2.58 / 2.60 / 2.63 | Node main near limit |
| Insert | 1 / 1 | 1 | 356 [193–365] | 56% / 56% | 41% / 37% | 23% / 20% | 11% | 1.53 / 1.44 / 1.45 | One request in flight |
| Insert | 1 / 1 | 8 | 485 [264–507] | 74% / 74% | 60% / 6% | 34% / 30% | 15% | 2.05 / 1.96 / 1.98 | CPU headroom, WAL wait inferred |
| Insert | 1 / 1 | 32 | 476 [269–521] | 77% / 76% | 62% / 2% | 37% / 32% | 15% | 2.12 / 2.04 / 2.06 | CPU headroom, WAL wait inferred |
| Insert | 8 / 8 | 8 | 624 [319–655] | 81% / 80% | 74% / 7% | 39% / 4% | 14% | 2.35 / 2.25 / 2.31 | CPU headroom, WAL wait inferred |
| Insert | 8 / 8 | 32 | 563 [322–633] | 82% / 81% | 72% / 2% | 40% / 5% | 14% | 2.30 / 2.24 / 2.29 | CPU headroom, WAL wait inferred |

# Single-node waiting-call baseline

This measurement records what a warmed stateless Worker and its node used while batches of 8, 16 and
24 ordinary HTTP calls awaited a local upstream. It covers one ARM64 machine and the product at
`dec0b3b816a70715760f12a6655a3a4c32fcfca8`. [Issue #31](https://github.com/coworkerd/coworkerd/issues/31)

## Results

All 144 calls reached the upstream and returned HTTP 200 with the expected body and node ID. The
node recorded zero placement refusals and zero egress denials or failures. The largest observed
overlap was 24 calls in each of the three batches at that size. Times below are ranges over the
three batches at each size. Client time runs from HTTP request creation to the end of the
response body. Upstream delay per request was 1,999.4–2,001.2 ms across all batches.

| Offered per batch | Simultaneously at upstream per batch | Completed across three batches | Common overlap (ms) | Client time (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 8 | 24/24 | 1,999.9–2,000.5 | 2,002.9–2,006.3 |
| 16 | 16 | 48/48 | 1,998.9–2,000.0 | 2,003.4–2,007.9 |
| 24 | 24 | 72/72 | 1,975.6–1,999.0 | 2,004.8–2,030.8 |

The waiting samples added 96 descriptors to the daemon and 48 to the ordinary workerd in every
24-call batch. The two processes stayed at 4 and 59 threads, respectively, at every batch sample.
The rise in RSS varied across repeats. The table gives the range of within-batch changes from
the before sample to the waiting sample.

| Calls | Daemon RSS (KiB) | Daemon descriptors | Ordinary workerd RSS (KiB) | Ordinary workerd descriptors | Fixture client and upstream RSS (KiB) | Fixture client and upstream descriptors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 0–1,116 | 31–35 | 20–264 | 16 | 0–668 | 16 |
| 16 | 44–1,276 | 64–66 | 40–2,312 | 32 | 32–604 | 32 |
| 24 | 292–2,456 | 96 | 60–2,116 | 48 | 24–288 | 48 |

The table below gives the median of each reading across the three 24-call batches. Each cell is
**RSS in KiB / threads / open descriptors**.

| Process | Before | Waiting | Drained | Recovered |
| --- | ---: | ---: | ---: | ---: |
| Node daemon | 30,692 / 4 / 124 | 32,524 / 4 / 220 | 32,668 / 4 / 127 | 32,668 / 4 / 126 |
| Ordinary workerd | 71,860 / 59 / 680 | 73,976 / 59 / 728 | 74,032 / 59 / 680 | 74,032 / 59 / 680 |
| Fixture client and upstream | 63,348 / 11 / 20 | 63,592 / 11 / 68 | 63,712 / 11 / 20 | 63,712 / 11 / 20 |
| Fixture API server | 18,296 / 1 / 7 | 18,296 / 1 / 8 | 18,296 / 1 / 7 | 18,296 / 1 / 7 |
| Fixture object store | 231,280 / 16 / 108 | 231,292 / 16 / 109 | 231,300 / 16 / 112 | 235,708 / 16 / 111 |

The ordinary workerd and fixture client returned to their previous descriptor counts after each
batch. In the 24-call batches, the daemon's recovered total was within three descriptors of its
before reading. The ordinary workerd held 78,208 KiB RSS, 539 threads and 2,120 descriptors after
initial warm-up. After 61 seconds without Worker traffic, the same process held 68,752 KiB, 59
threads and 680 descriptors. The native pool starts at its configured limit and trims excess
call threads after the default 60-second idle period. [Thread pool implementation](../patches/workerd/0042-threaded-server-s1a.patch#L3501)

These ranges describe three repeats per size on one VM. Later batches reused pages already touched
by earlier calls. Shared pages can appear in multiple RSS readings, and background service activity
contributes to variation. The raw samples also include the idle quarantine and durable children.
Log helpers, Docker processes and kernel allocations are outside the process readings. Each
waiting sample fit within an observation bracket of 228–278 ms. The drained sample was recorded
179–297 ms after the last client body, and the recovered sample at 5.39–5.50 s after it.

## Setup and time basis

- The host was an Apple M4 with 10 logical CPUs and 16 GiB RAM. Docker Desktop's ARM64 Linux VM
  reported 10 CPUs, 12,528,578,560 bytes of RAM, Linux `6.12.72-linuxkit` and Docker Engine 29.2.1.
  The daemons were Release builds with Debian Clang 19.1.7. The fixture client and upstream ran in one
  Node.js 22.23.5 process on the same VM. [Raw readings](waiting-call-capacity/arm64.jsonl)
- The public `linux-arm64` kernel archive was
  `kernel-v1.20261114.1-ce00bd62d2d6-run-45983799782-1`, SHA-256
  `9092f174c8af28d1c6e4a7a821401be054cc00c5d2e556db3b09737572946112`.
  The repository fetcher verified the archive and its files. The node reported the same binary hash
  for the serving `coworkerd-threaded` process. [Published kernel](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-ce00bd62d2d6-run-45983799782-1)
- The existing Compose fixture ran one node with ingress placement enabled, one ordinary threaded
  process, two quarantine processes, one durable process, the control plane and the object store.
  Sandbox mode `auto` started the ordinary process directly. Its configured native memory budget
  was 2,004,572,569 bytes. The fixture allowed the test tenant to reach one private upstream on the
  Compose backend, with the default egress policy set to deny. Each Worker request made one plain
  HTTP fetch, then read and returned the small upstream body. Client and upstream connections
  closed after each reply.
- The fixture used three batches per size in the order `8, 16, 24, 16, 24, 8, 24, 8, 16`. Each call had
  a unique ID. The client clocked the start of each HTTP request and the end of its response body.
  On the same monotonic clock, the upstream recorded when it entered the request handler and when
  its 2,000 ms timer released the response. The common overlap is the first release less the last
  arrival. Each waiting sample was bracketed by two upstream observations with the whole batch
  active. Node placement, the ordinary process and egress each reported the same in-flight count.
- The collector recorded per-process RSS from Linux `smaps_rollup`, thread count from `status` and
  open descriptors from `fd`. It sampled before the batch, during overlap, after the client read all
  response bodies and the counters drained, and after a further five-second idle period. A final
  warm call checked recovery after that last sample. Before the batches, the collector warmed the
  Worker and gave the native pool 61 seconds to retire excess idle threads. It used the same process
  generation throughout. Process reads are snapshots and were taken sequentially within each
  container.

## Current limits and remaining measurements

The default outbound concurrency limit on this path is 32 per tenant. The same configuration has a
node-wide root admission threshold of 128, a nested threshold of 384, and an in-flight limit of 512
on the ordinary process. The per-tenant placement limits are disabled. The outbound setting is the
first configured concurrency ceiling for this one-tenant workload. These batches leave at least
eight outbound slots free. [Egress limits](egress-and-auth.md#limits-and-metering) · [Placement admission](cluster-state.md#7-placement)

The [#31 finish line](https://github.com/coworkerd/coworkerd/issues/31) calls for 2,000 simultaneous
two-second waits without refusal, and for the largest supported waiting population and its limiting
resource to be measured. Those measurements remain open. The current outbound and root admission
limits are both below 2,000. The measured range also fits inside the threaded kernel's 32 warm call
threads, so it leaves the cost of growing the pool unmeasured. The production safety threshold and
the multi-node hot-Worker, latency and overload comparisons in #31 require further measurements.

## Reproduce

Use a local Docker engine with Compose, Node.js 22, Python 3 and Linux Release daemons built from the
measurement baseline. See the [build dependencies](../CONTRIBUTING.md#building) and [cluster fixture
instructions](../tests/cluster/README.md). From a checkout containing this report and collector, run
the following with the published kernel verified by the repository fetcher. The collector's only
arguments are the local kernel and the new JSONL path. Compose creates and removes the test cluster
and the collector removes the upstream.

```sh
npm ci --prefix cli --registry=https://registry.npmjs.org
kernel=$(python3 scripts/fetch-kernel.py)
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=4
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD="$kernel/workerd"
node tests/cluster/waiting-call-capacity.mjs "$kernel" build/waiting-call-capacity.jsonl
```

For the recorded baseline, use `dec0b3b` for the build and kernel fetch in a separate checkout.
Set `COWORKERD_CLUSTER_BIN_DIR` to its Linux build directory and pass its verified kernel directory
to the collector in the checkout containing this report.

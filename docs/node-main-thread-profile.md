# Node main-thread CPU profile

This profile follows four warm paths through one node, one Worker, one ordinary workerd child and one threaded durable child. [#94 throughput benchmark](database-throughput.md) · [#95, part 1](https://github.com/coworkerd/coworkerd/issues/95)

The node's main thread used **0.436 ms** per plain Worker response, **1.282 ms** per no-op Durable Object call, **1.429 ms** per one-row read and **1.507 ms** per confirmed one-row insert in the direct runs at the throughput knee. The bare HTTP hop used **0.072–0.096 ms** of its own main thread. The sampled no-op cost **0.813 ms** more than the sampled Worker response. Dispatch, binding and shared Cap’n Proto stacks together account for **0.382 ms** of that difference.

## Machine and workload

The measurement ran on GitHub Actions instance **1000001523** (`ip-10-0-128-131`), a standard `ubuntu-24.04` x86-64 VM with four vCPUs on an Intel Xeon 6975P-C, 16,464,273,408 bytes of RAM, Linux `7.0.0-1018-aws`, and ext4 for Docker volumes. The runner image was `ubuntu24 20261122.23.1`, Docker was 28.0.4, clang was 19.1.1, and perf was 7.0.14. The daemons were built before timing and restored from the build cache. The timed runs had no container CPU quota. [Runner record](node-main-thread-profile/runner.txt) · [Actions run and full stack artifact](https://github.com/coworkerd/coworkerd/actions/runs/45829785423)

The node code is from `326bec7c3d225aeb3d0758bed877786ee6be1b7f` on `main`. Benchmark checkout `fccc0ca6e413a0f5c8fc24b8d72c7d1baa00887b` applies a test-only patch which adds an in-memory trace of admission slot changes and local WAL flushes. The daemons use Release optimization with debug lines and frame pointers. The archived public workerd kernel is `v1.20261114.1`, built at `59542403f8a401479ed9528c53ef199670acb52e`. Its x86-64 archive SHA-256 is `369865c7c3188d0e9cba7a366e9d9b1b44f9aab4bd8da2a2bb17b170a88b5a83`. [Pinned release](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-33a085a436c3-run-45781366667-1) · [Window data and binary hashes](node-main-thread-profile/measurement.jsonl)

The first three are GETs returning `[{"value":1}]`. The read selects a seeded primary-key row. The insert is a POST with a unique ID, returns `1`, and waits for local WAL confirmation with zero remote peers. The driver verifies the HTTP status and body, the database row count, and the confirmation count. [Worker fixture](../tests/cluster/database-home-worker.js)

The client uses one outstanding request per lane and persistent HTTP/1.1 connections on the Compose frontend network. Each window warms for 1.5 s, measures for 6 s, and counts the final replies. There are three windows per cell, run serially. The knee is the smallest tested concurrency that gives at least 95% of the best median reply rate in the sweep. Direct, forwarded and sampled windows then use that same concurrency. The trial uses the #94 fixture's root admission threshold of 512, nested threshold of 1,024, per-process limit of 512 and 64-event mailbox. Across 96 timed windows, 566,409 replies passed validation with zero timed or warm-up failures. The 24 insert windows and their warm-ups produced exactly 94,300 confirmed inserts.

## Throughput and the bare KJ forwarder

Median replies per second by number of client lanes.

| Workload | 1 | 4 | 8 | 16 | 32 | Clients at knee |
| --- | --- | --- | --- | --- | --- | --- |
| Worker response | 1,208 | 2,039 | 2,159 | 2,269 | 2,316 | 16 |
| No-op Durable Object | 412 | 684 | 763 | 809 | 825 | 16 |
| One-row read | 355 | 574 | 663 | 704 | 729 | 16 |
| Confirmed one-row insert | 312 | 516 | 549 | 580 | 592 | 16 |

All four paths reached the knee at 16 lanes. Going to 32 raised the median by 2–4%, while no-op, read and insert tail latency grew substantially in some windows. At 32 the node main thread used 93–99% of one vCPU for the three non-writing paths and 82–85% for inserts. The host used 2.28–2.67 of its four vCPUs. CPU pressure (`some`) ranged from 11.5% to 23.9% of those windows.

The reference is a single-threaded KJ HTTP server connected to a KJ HTTP client with a fixed upstream. It forwards the same method, path, Host, request and response on the same runner. The upstream is the exact node and Worker used in the direct windows, so the no-op still calls the object and the forwarded inserts still reach the WAL. Both legs use persistent connections. The forwarded reply rate and client latency describe the two-process path at the same offered load. The forwarder CPU column measures only the reference process. Its cost provides a scale for plain HTTP forwarding. The node also translates HTTP to Cap’n Proto and back, so the difference includes the choice of transport and the node’s own bookkeeping. Every matched window opened 16 client connections during warm-up and none during measurement.

| Workload | Clients | Direct replies/s | Direct node CPU ms/reply | Forwarded replies/s | Forwarded node CPU ms/reply | Forwarder CPU ms/reply | Direct p50 / p99 ms | Forwarded p50 / p99 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Worker response | 16 | 2,193 | 0.436 | 2,173 | 0.431 | 0.072 | 6.34 / 26.47 | 6.95 / 17.63 |
| No-op Durable Object | 16 | 743 | 1.282 | 756 | 1.230 | 0.090 | 13.41 / 235.83 | 20.33 / 52.06 |
| One-row read | 16 | 677 | 1.429 | 650 | 1.383 | 0.093 | 15.31 / 147.53 | 23.18 / 58.10 |
| Confirmed one-row insert | 16 | 550 | 1.507 | 548 | 1.506 | 0.096 | 27.55 / 64.36 | 27.74 / 65.71 |

Reply rates and latency percentiles are medians of the three windows. CPU is summed across the same windows and divided by their successful replies. The forwarder used about 16% of the plain node's CPU per reply and 6–7% of the node's for the nested paths. Its end-to-end p50 rose on all four paths. Direct no-op p99 ranged from 51 to 247 ms and direct read p99 from 52 to 204 ms. This spread limits what the paired tails can say about the effect of the extra hop. Client latency includes queueing and work outside the node. The CPU columns count scheduled time on each named main thread.

## Where the node spends CPU

The profiler samples just the node daemon's main Linux TID at 499 Hz with frame-pointer stacks and the monotonic clock. For each timed window, the innermost recognizable application stage owns each sample. [Stack classifier](../tests/cluster/main-thread-profile/perf.mjs)

- **HTTP receive, route and reply** covers KJ HTTP parsing, ingress handling and the route lookup.
- **Lease and admission** covers lease allocation, inheritance and retirement, call-slot reservation, and admission checks.
- **Node to runtime** covers dispatch to a child and the node's side of the HTTP bridge to that child.
- **Runtime to node** covers binding RPC entry, child-call placement and dispatch selection.
- **Shared Cap’n Proto** covers framing, serialization, and promise and capability traffic when a generic stack has no directional application frame.
- **Logging and metering** covers work attributable to log handling, execution records, and usage accounting.
- **Durable log** covers object-side log functions, including append and confirmation. Any time the main thread sleeps inside the flush is outside this CPU bucket.
- **Allocation without an assigned caller** retains allocator frames whose available stacks have no matching stage.
- **Event loop, system and other resolved stacks** retains known KJ scheduler, Linux and other symbols with no request stage.
- **Unresolved stacks** have no usable symbol in the captured call chain.

The unscaled sample periods and `/proc/<tid>/stat` CPU are both recorded. The stage estimates use each window's sample-period proportions times its measured main-thread CPU. Dividing by the total validated replies across the sampled windows gives a CPU budget that adds to the sampled-run CPU total, up to rounding. The archived frame samples were classified again after adding explicit descriptor encoding, lease reaping, allocator and output symbols. The sample count and period sum are identical to the Actions artifact.

| Stage (CPU ms/reply) | Worker response | No-op Durable Object | One-row read | Confirmed one-row insert | No-op minus Worker |
| --- | --- | --- | --- | --- | --- |
| HTTP receive, route and reply | 0.035 | 0.083 | 0.096 | 0.093 | +0.048 |
| Lease and admission | 0.029 | 0.039 | 0.051 | 0.046 | +0.010 |
| Node to runtime dispatch and HTTP bridge | 0.071 | 0.183 | 0.194 | 0.198 | +0.112 |
| Runtime to node binding and placement | 0.000 | 0.100 | 0.102 | 0.110 | +0.100 |
| Shared Cap’n Proto protocol | 0.078 | 0.249 | 0.286 | 0.300 | +0.170 |
| Logging, execution records and metering | 0.006 | 0.024 | 0.024 | 0.029 | +0.018 |
| Durable log | 0.000 | 0.005 | 0.005 | 0.096 | +0.005 |
| Allocation without an assigned caller | 0.033 | 0.096 | 0.114 | 0.107 | +0.063 |
| Event loop, system and other resolved stacks | 0.073 | 0.185 | 0.216 | 0.220 | +0.111 |
| Unresolved stacks | 0.101 | 0.277 | 0.316 | 0.312 | +0.175 |
| Total | 0.427 | 1.240 | 1.405 | 1.511 | +0.813 |

A plain root sends an execution descriptor and bridges HTTP to the ordinary runtime. The no-op also returns through the binding RPC, places a child, opens a second execution and transports its response through the ordinary runtime. Against the plain root, the sampled no-op adds 0.112 ms in node-to-runtime dispatch and bridging, 0.100 ms in binding and placement, and 0.170 ms in shared Cap’n Proto work. It also adds 0.048 ms in HTTP handling and 0.028 ms combined in lease/admission and logging/metering. The difference includes 0.174 ms of unassigned allocator, event-loop and system work and 0.175 ms with unresolved stacks. The named protocol cost is an estimate from recognizable call chains. A follow-up DWARF smoke run on the hosted image gave no named application frames, so the report uses the frame-pointer capture. [DWARF smoke run](https://github.com/coworkerd/coworkerd/actions/runs/45833155739)

| Workload | CPU samples | Perf period ms/reply | Profiled /proc CPU ms/reply | Direct /proc CPU ms/reply | Profiled replies/s |
| --- | --- | --- | --- | --- | --- |
| Worker response | 8,486 | 0.427 | 0.427 | 0.436 | 2,203 |
| No-op Durable Object | 8,411 | 1.238 | 1.240 | 1.282 | 756 |
| One-row read | 8,267 | 1.404 | 1.405 | 1.429 | 655 |
| Confirmed one-row insert | 7,494 | 1.506 | 1.511 | 1.507 | 555 |

The unscaled sampling periods agree with independently measured CPU within 0.005 ms per reply. Relative to direct windows, sampled median throughput varied by −3.2% to +1.8%, and profiled CPU per reply differed by at most 0.042 ms. All variants ran the tracing build.

### Ordinary Release and tracing build

The primary runner was recycled after the first run. A second standard Ubuntu 24.04 VM ran both builds with the same node source, published workerd kernel, Compose topology, and 16-client load. GitHub Actions instance **1000001549** (`runnervm0j63q`) had four vCPUs on an AMD EPYC 7763, 16,760,320,000 bytes of RAM, Linux `7.0.0-1020-azure`, and ext4. Two windows of the ordinary Release build bracketed three traced windows per workload. The CPU comparison uses unsampled windows for both builds. Another three traced windows per workload captured stacks for the shared-object table. All 32 windows passed reply and insert checks with zero warm-up or timed failures. [Control runner and recording](node-main-thread-profile/controls) · [Control run and raw stacks](https://github.com/coworkerd/coworkerd/actions/runs/45834753741)

| Workload | Plain before / after CPU ms/reply | Plain pooled CPU ms/reply | Traced CPU ms/reply | Traced minus plain ms/reply | Plain / traced replies/s |
| --- | --- | --- | --- | --- | --- |
| Worker response | 0.646 / 0.639 | 0.643 | 0.678 | +0.035 | 1,425 / 1,391 |
| No-op Durable Object | 1.737 / 1.725 | 1.731 | 1.770 | +0.039 | 524 / 527 |
| One-row read | 1.949 / 1.941 | 1.945 | 1.960 | +0.015 | 450 / 459 |
| Confirmed one-row insert | 2.127 / 2.066 | 2.095 | 2.142 | +0.047 | 361 / 369 |

The traced build added 0.015–0.047 ms of main-thread CPU per reply on this paired control. This measures the combined effect of the trace patch and profiling build flags. The no-op and read in the primary run cost 1.282 and 1.429 ms, compared with 1.00–1.12 and 1.24–1.33 ms in #94. The paired build deltas for those paths were 0.039 and 0.015 ms. They explain only a fraction of the gap. The first experiment and #94 also used different runner instances, node revisions and workerd archive pins. [#94 throughput and CPU measurements](database-throughput.md)

### Shared objects in the residual samples

The second runner also kept the shared object (DSO) of the sampled instruction for the three residual buckets. Each DSO's sample periods are scaled to measured thread CPU using the same per-window method as the stage table.

| Residual DSO (CPU ms/reply) | Worker response | No-op Durable Object | One-row read | Confirmed one-row insert |
| --- | --- | --- | --- | --- |
| glibc | 0.139 | 0.361 | 0.442 | 0.447 |
| Linux kernel and modules | 0.071 | 0.193 | 0.216 | 0.215 |
| Node executable | 0.040 | 0.093 | 0.112 | 0.122 |
| libstdc++ | 0.008 | 0.013 | 0.012 | 0.015 |
| Other userspace objects | 0.034 | 0.061 | 0.050 | 0.059 |
| Unknown mapping | 0.000 | 0.000 | 0.000 | 0.000 |
| Total residual | 0.292 | 0.720 | 0.832 | 0.858 |

On the control runner, glibc and the kernel together held 72–79% of the residual, including 0.554 ms out of 0.720 ms on a no-op object call. The remaining node-executable samples include statically linked KJ and Cap’n Proto. A DSO identifies where the instruction ran. Attribution to an individual request phase still depends on its caller frames.

### Waiting for local WAL confirmation

| Confirmed replies | Flush calls | Elapsed ms/flush | CPU ms/flush | Off-CPU ms/reply | Largest elapsed ms |
| --- | --- | --- | --- | --- | --- |
| 9,849 | 9,849 | 0.209 | 0.033 | 0.176 | 3.770 |

Each of the 9,849 direct timed inserts at the knee had one local `fdatasync` call. The trace brackets that syscall with monotonic and thread CPU clocks. Elapsed minus CPU time was 0.176 ms per confirmed reply, or about 9.7% of the measured wall window at this rate. Combined with 82–83% scheduled node-main CPU, that accounts for about 92% of the window. The off-CPU interval can include scheduler delays during the call. The durable-log CPU row includes the flush's on-CPU work along with other log processing.

## Calls in flight and `placement.rootLimit`

The trace records the two placement counters on every successful reservation and release. Roots are admitted invocations, and nested slots include locally admitted binding calls. The trace counts slots through retirement. A lane can begin its next request after the HTTP body arrives while the preceding invocation still owns a slot. Each occupancy percentile is weighted by the time spent at that occupancy. The largest simultaneous total shows the actual root and nested pair at that instant. The last column shows the total **before** the most heavily loaded successful root reservation, reconstructed from the preceding counter values.

| Workload | Clients | Mean roots (range) | Mean nested (range) | Total p99 (range) | Largest simultaneous total | Most slots before root arrival |
| --- | --- | --- | --- | --- | --- | --- |
| Worker response | 16 | 19.5–26.5 | 0.0 | 32–35 | 48 + 0 = 48 | 47 |
| Worker response | 32 | 10.5–52.7 | 0.0 | 67–91 | 114 + 0 = 114 | 113 |
| No-op Durable Object | 16 | 11.0–17.8 | 6.3–10.4 | 32 | 21 + 15 = 36 | 34 |
| No-op Durable Object | 32 | 35.2–35.4 | 20.7–21.0 | 65 | 42 + 30 = 72 | 69 |
| One-row read | 16 | 9.7–17.6 | 6.0–10.5 | 31–32 | 22 + 15 = 37 | 36 |
| One-row read | 32 | 21.9–35.2 | 13.9–22.0 | 64–66 | 50 + 26 = 76 | 75 |
| Confirmed one-row insert | 16 | 17.7–17.8 | 10.5–10.6 | 32–33 | 25 + 16 = 41 | 34 |
| Confirmed one-row insert | 32 | 35.4–35.5 | 21.8–22.2 | 68–69 | 49 + 31 = 80 | 74 |

The default `placement.rootLimit` is 128. A new root is refused when the sum of active roots and nested calls is already at the limit. A reservation recheck discounts its own slot. Nested calls can continue admitting under the separate `nestedLimit` of 384, so peak total occupancy can exceed the root refusal threshold. Moving a Worker to another node separates its root from the database's nested slot. [Admission predicate](../node/placement-contract.h) · [Same-node and peer case in #106](https://github.com/coworkerd/coworkerd/issues/95#issuecomment-7183434931)

To replay the measured successful admissions, the threshold would have to be at least 48 at the 16-client knee and at least 114 for the 32-client sweep. The default of 128 covers all of these arrivals. A separate 64-lane calibration on another Linux VM reached 135–155 occupied slots before successful root admissions on the nested paths. In its heaviest plain Worker window, 4,890 replies were refused when occupancy reached the benchmark threshold of 512. Those windows establish that a larger burst can reach the default threshold well above the measured throughput knee. A root with one active local child consumes two slots, and earlier replies can retain a slot through retirement. Higher limits leave more room for bursts and queueing. [64-lane calibration and refusal log](https://github.com/coworkerd/coworkerd/actions/runs/45826913229)

## Reproduce

Use a quiet, standard Ubuntu 24.04 x86-64 Linux instance with Docker and Compose, Node.js 22, Python 3, and `perf` for the running Linux kernel. Use a fresh checkout and finish builds and downloads before timing. The commands below rerun the recorded source and public kernel pin. The profiler uses the Docker host's `sudo -n perf` to attach to the container's daemon main TID. [Build dependencies](../CONTRIBUTING.md#building)

```sh
git fetch origin refs/pull/116/head
git worktree add --detach ../coworkerd-profile fccc0ca6e413a0f5c8fc24b8d72c7d1baa00887b
cd ../coworkerd-profile
git apply tests/cluster/main-thread-profile/instrumentation.patch
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=4
bash scripts/build.sh -DCMAKE_CXX_FLAGS_RELEASE='-O3 -DNDEBUG -g1 -fno-omit-frame-pointer'
cmake --build build --target coworkerd-bench-http-forwarder
kernel=$(python3 scripts/fetch-kernel.py --arch linux-x86_64)
mkdir -p reports
COWORKERD_PROFILE_STACKS="$PWD/reports/stacks.jsonl" \
  node tests/cluster/database-home-benchmark.mjs object "$PWD" "$kernel" \
  "$PWD/reports/measurement.jsonl" --main-thread-profile
```

Use new output paths for a repeat, or `--main-thread-profile-smoke` to check the four routes, the KJ forwarder, the trace, and the profiler in short windows.

The report tables can be regenerated from the checked-in recording. Run the stack classifier against `stacks.jsonl` from the Actions artifact to regenerate the per-window histogram.

```sh
python3 tests/cluster/main-thread-profile/tables.py docs/node-main-thread-profile/measurement.jsonl
python3 tests/cluster/main-thread-profile/tables.py --controls docs/node-main-thread-profile/controls
node tests/cluster/main-thread-profile/perf.mjs /path/to/stacks.jsonl
```

From the checkout containing this report, create a second checkout for the build and shared-object controls. Build both binaries before timing. The three trials start fresh Compose clusters and bracket the traced run with ordinary Release windows.

```sh
report_checkout=$PWD
git fetch origin refs/pull/116/head
git worktree add --detach ../coworkerd-profile-controls 83c3eeb69649c4bd2329259f31bc6dec46be334c
cd ../coworkerd-profile-controls
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=4
bash scripts/build.sh
mkdir -p build/plain reports
cp build/coworkerd build/coworkerd-apiserver build/coworkerd-launcher build/plain/
git apply tests/cluster/main-thread-profile/instrumentation.patch
bash scripts/build.sh -DCMAKE_CXX_FLAGS_RELEASE='-O3 -DNDEBUG -g1 -fno-omit-frame-pointer'
cmake --build build --target coworkerd-bench-http-forwarder
kernel=$(python3 scripts/fetch-kernel.py --arch linux-x86_64)
COWORKERD_PROFILE_CONTROL=plain COWORKERD_CLUSTER_BIN_DIR="$PWD/build/plain" \
  node tests/cluster/database-home-benchmark.mjs object "$PWD" "$kernel" \
  "$PWD/reports/plain-before.jsonl" --main-thread-profile
COWORKERD_PROFILE_CONTROL=traced COWORKERD_PROFILE_STACKS="$PWD/reports/stacks.jsonl" \
  node tests/cluster/database-home-benchmark.mjs object "$PWD" "$kernel" \
  "$PWD/reports/traced.jsonl" --main-thread-profile
COWORKERD_PROFILE_CONTROL=plain COWORKERD_CLUSTER_BIN_DIR="$PWD/build/plain" \
  node tests/cluster/database-home-benchmark.mjs object "$PWD" "$kernel" \
  "$PWD/reports/plain-after.jsonl" --main-thread-profile
python3 "$report_checkout/tests/cluster/main-thread-profile/tables.py" --controls reports
```

## Durable log I/O

The append worker owns the active log descriptor. A completed append makes a record eligible for replication. With no selected peer, a successful local flush makes the covered prefix eligible for confirmation. The node rechecks the lease before each positive reply and preserves confirmation order within each database. Writes queued during I/O can share a flush. Epoch rotation drains the old file's queue before switching descriptors and refuses outstanding confirmations under the old identity. If a request's WAL record arrives while the old file drains or the new descriptor opens, the node refuses the write and uses the existing repair path. That window lasts through the drain and descriptor handoff.

### Paired measurement

Both builds ran in one job on `runnervm0j63q`, an Ubuntu 24.04 x86-64 VM with four vCPUs on an AMD EPYC 7763 and Linux `7.0.0-1020-azure`. They used the same published workerd kernel. Each build served through a single standalone node. The driver alternated builds on each volume, with five five-second one-client insert windows before three five-second 16-client windows. The seven-second mixed windows used one insert client and one client reading a warm SQLite Durable Object in a different tenant. Every insert request wrote one row. The driver checked every reply and verified a total of 42,281 confirmed inserts including warm-up. [Runner record](node-main-thread-profile/durable-log-io/runner.txt) · [Source revisions](node-main-thread-profile/durable-log-io/revisions.txt) · [Same-runner job and recording](https://github.com/coworkerd/coworkerd/actions/runs/45911331184)

The slow volume used ext4 on a loop device with a 20 ms device-mapper delay on block flushes. The delayed image had its own backing ext4 filesystem and journal, with no other files written there during the run. Both loop images were fully written before measurement and used direct I/O. The SQLite files, ordinary-volume log and probe output used separate mounts. The node probe records the thread, file and monotonic interval for writes and syncs on the log mounts. A read qualifies as concurrent when its complete HTTP round trip fits within one recorded `fdatasync` interval. [Linux `dm-delay` documentation](https://docs.kernel.org/admin-guide/device-mapper/delay.html)

| Log volume | Insert clients | Baseline confirmed inserts/s | Candidate confirmed inserts/s |
| --- | ---: | ---: | ---: |
| Ordinary ext4 | 1 | 245.8 | 239.0 |
| Ordinary ext4 | 16 | 413.0 | 463.6 |
| Delayed ext4 | 1 | 38.8 | 38.8 |
| Delayed ext4 | 16 | 40.8 | 312.0 |

Rates are medians. On the delayed volume, the 16-client rate rose 7.65×. Across the five one-client windows on that volume, baseline flush p99 ranged from 22.97 to 25.99 ms and candidate flush p99 ranged from 22.88 to 25.99 ms. Baseline insert p50 ranged from 25.16 to 25.61 ms and candidate insert p50 ranged from 25.32 to 25.76 ms.

| Log volume | Baseline longest main-thread stall | Candidate longest main-thread stall | Baseline reads wholly inside a flush | Candidate reads wholly inside a flush |
| --- | ---: | ---: | ---: | ---: |
| Ordinary ext4 | 5.269 ms | 4.787 ms | 0 / 1,304 | 0 / 1,452 |
| Delayed ext4 | 43.771 ms | 4.822 ms | 0 / 279 | 1,319 / 1,857 |

A stall is the interval between returning from one `epoll_wait` or `epoll_pwait` and entering the next. It includes time spent in blocking calls and time descheduled between polls. The table takes the maximum across all nine timed windows per cell. The ordinary-volume candidate maximum occurred in the second one-client insert-only window and included 0.236 ms of main-thread CPU. The delayed-volume maximum occurred in the second one-client insert-only window and included 0.396 ms of main-thread CPU. The delayed volume passed the 5 ms threshold. One recorded 3.50 ms read took place from 1.22 to 4.72 ms into a 21.55 ms flush.

All 19,934 observed candidate append calls and all 13,980 observed candidate flushes ran outside the main thread. The largest candidate flush was 26.961 ms on the delayed volume and 6.238 ms on the ordinary volume. The overlap recordings contain zero rows because no candidate flush exceeded 40 ms. [Delayed-volume overlap recording](node-main-thread-profile/durable-log-io/slow-long-flushes.jsonl) · [Ordinary-volume overlap recording](node-main-thread-profile/durable-log-io/normal-long-flushes.jsonl)

### Earlier run on shared backing storage

The first run put the delayed volume's backing image on the ordinary filesystem and ran each build's windows consecutively. It measured 2.878 ms as the candidate's longest main-thread stall on the delayed volume. The 5.035 ms ordinary-volume peak occurred in the first 16-client insert-only window with zero main-thread append or flush calls. That run measured a delayed-volume one-client rate of 28.8 inserts/s with the worker, down from 38.4. The candidate's flush p99 varied from 84.8 to 337.8 ms across those windows. The first recording retained flush summaries and one example per window, so concurrent file operations during those old outliers cannot be reconstructed. The alternating run with a private backing filesystem did not reproduce the one-client drop or the long candidate flushes. [First recording](node-main-thread-profile/durable-log-io/first-run/) · [First Actions job](https://github.com/coworkerd/coworkerd/actions/runs/45907925121)

### Reproduce the log comparison

Use one quiet Ubuntu 24.04 x86-64 machine with the [build dependencies](../CONTRIBUTING.md#building), Docker Compose, Node.js 22, and root access for the private loop mounts. The driver creates fresh clusters and removes its test volumes when done.

```sh
git fetch origin refs/pull/122/head
git worktree add --detach ../coworkerd-log-before 01a9af30fdca886e1cc4ae3b82399c202099fe78
git worktree add --detach ../coworkerd-log-after e938d72ed1fd4c6fabcce7617853359cd6845cbc
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=4
(cd ../coworkerd-log-before && bash scripts/build.sh)
(cd ../coworkerd-log-after && bash scripts/build.sh && npm ci --prefix cli)
cd ../coworkerd-log-after
kernel=$(python3 scripts/fetch-kernel.py --arch linux-x86_64)
bash tests/cluster/durable-log-io/run.sh ../coworkerd-log-before/build build "$kernel" reports/durable-log-io
```

Verify the checked-in recording from the checkout containing this report.

```sh
node tests/cluster/durable-log-io/verify.mjs docs/node-main-thread-profile/durable-log-io
```

# Hot Worker placement with spare CPU

This measurement covers the hot Worker and light tenant in [#31](https://github.com/coworkerd/coworkerd/issues/31).

| Configuration | Coworkerd product source | Public Linux ARM64 kernel pin |
| --- | --- | --- |
| Before, dynamic ingress | `dec0b3b` (main immediately before [#127](https://github.com/coworkerd/coworkerd/pull/127)) | [`ce00bd62d2d6`](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-ce00bd62d2d6-run-45983799782-1) |
| Landed, hot Worker pinned | `398c7a3` (main including [#129](https://github.com/coworkerd/coworkerd/pull/129)) | [`7309075a8750`](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-7309075a8750-run-46040948600-1) |
| Landed, dynamic ingress | `398c7a3` | [`7309075a8750`](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-7309075a8750-run-46040948600-1) |

Both product revisions declare cowctl `0.5.6`. The daemon binaries are local CMake Release builds
(`0.0.0-dev`) compiled with Clang 19.1.7. Both verified kernel archives use workerd `v1.20261114.1`.

The test machine was an Apple M4 with a Docker Desktop Linux ARM64 VM (`6.12.72-linuxkit`)
and 10 vCPUs. The client used Node.js `22.23.5`. Each node's CPU affinity and the client and
node cpusets covered `0–9`. The client and all three nodes reported `cpu.max = max 100000`,
so there was no container CPU quota. The fixtures ran serially after both builds finished.

## 15-second windows

The landed policy served the hot Worker on all three nodes in every loaded window. It completed
451 requests in 45 measured seconds (10.02/s), 1.79× the matched pinned control (5.60/s).
The light tenant stayed on `node-3`. Its p99 was 2.75–3.07 ms under load and 5.62–7.01 ms in the
adjacent quiet windows. The largest absolute difference was 4.17 ms, within the 5 ms criterion.

| Configuration and loaded window | Hot completions in 15 s (nodes 1/2/3) | Hot completions including drain (nodes 1/2/3) | Completed/s | `WORKER_SATURATED` (nodes 1/2/3) |
| --- | ---: | ---: | ---: | ---: |
| Before, each of H1–H3 | 0 / 0 / 84 | 0 / 0 / 165 | 5.60 | 0 / 0 / 0 |
| Landed pinned, each of H1–H3 | 0 / 0 / 84 | 0 / 0 / 165 | 5.60 | 0 / 0 / 0 |
| Landed dynamic, H1 | 22 / 51 / 74 | 24 / 67 / 74 | 9.80 | 0 / 24 / 91 |
| Landed dynamic, H2 | 22 / 61 / 70 | 22 / 66 / 77 | 10.20 | 0 / 22 / 88 |
| Landed dynamic, H3 | 19 / 59 / 73 | 21 / 67 / 77 | 10.07 | 0 / 21 / 88 |

Every configuration completed all 495 hot offers through the drain. All 334 `WORKER_SATURATED`
refusals were internal candidate refusals. `NODE_CPU_SATURATED`, `OVERLOADED`, `PLACEMENT_EXHAUSTED`,
and all other admission or client HTTP error codes had zero occurrences.

| Configuration | Light p99 in Q1 / Q2 / Q3 / Q4 (ms) | Light p99 in H1 / H2 / H3 (ms) | Largest absolute difference from adjacent quiet (ms) |
| --- | ---: | ---: | ---: |
| Before | 6.20 / 6.45 / 6.22 / 6.21 | 3.73 / 3.15 / 2.96 | 3.30 |
| Landed pinned | 6.96 / 6.67 / 5.94 / 5.75 | 3.34 / 2.67 / 3.12 | 4.00 |
| Landed dynamic | 5.75 / 7.01 / 5.62 / 6.59 | 2.84 / 3.07 / 2.75 | 4.17 |

Each light window scheduled 1,500 offers. Every loaded window returned 1,500 valid light replies.
In Q2 the before and landed dynamic clients missed 6 and 7 scheduled offers respectively
(`CLIENT_MISSED_OFFER`). They retained the highest ranks in the quiet p99 calculation. All other
quiet windows returned 1,500 valid replies. Differences use the unrounded readings.

The VM averaged 77.9–78.4% idle CPU across the three landed dynamic loaded windows. The lowest
one-second idle fraction in those windows was 66.3%, equivalent to 6.6 idle vCPUs out of 10.
The before and pinned loaded windows averaged 86.5–86.8% idle, with a lowest one-second reading
of 85.4% across both configurations.

## One minute at the same offer rate

The 15 s windows ended while the single-isolate controls still had room under the 128-root ceiling.
A follow-up used one 60 s loaded window per configuration to see what happens when the queue reaches
that ceiling. It kept the same product and kernel pairs, workload, ingress, and offer rates. Each
run offered 660 hot requests and 6,000 light requests with a 15 s quiet window on either side.

The driver sampled the home node's management state every five seconds. The `hot-placement` tenant
has just one Worker, so its admitted root count is the hot Worker's home occupancy. The fixture
enables per-tenant accounting with `tenantNestedLimit: 384`, equal to the aggregate nested ceiling.
The tenant root limit is zero and the node root limit is 128. The fixture checks that no nested
requests are in flight and that the two tenants account for all sampled home root slots. Light
replies record the actual execution node, including peers selected when the home fills.

Both controls reached 128 hot roots at home by the 25 s sample. Each sent about 29% of its light
replies through a peer. Landed dynamic placement kept all 6,000 light replies on the home node
and had at most 17 hot roots there in the samples. The number of hot calls still awaiting a
reply across all nodes peaked at 19 and ended the loaded window at 17 for landed dynamic
placement. Both controls had 128 calls awaiting a reply at the end of that window.

| Time (s) | Before hot roots at home | Landed pinned hot roots at home | Landed dynamic hot roots at home |
| ---: | ---: | ---: | ---: |
| 0 | 1 | 1 | 1 |
| 5 | 28 | 29 | 4 |
| 10 | 55 | 55 | 17 |
| 15 | 82 | 82 | 8 |
| 20 | 109 | 109 | 0 |
| 25 | 128 | 128 | 11 |
| 30 | 128 | 128 | 13 |
| 35 | 128 | 128 | 2 |
| 40 | 128 | 128 | 5 |
| 45 | 128 | 128 | 17 |
| 50 | 128 | 128 | 8 |
| 55 | 128 | 128 | 0 |
| 60 | 127 | 128 | 11 |

| Configuration | Loaded light p99 (ms) | Quiet p99 before / after (ms) | Largest absolute difference (ms) | Loaded light replies (nodes 1/2/3) | Other loaded light outcomes |
| --- | ---: | ---: | ---: | ---: | --- |
| Before | 3.34 | 6.97 / 6.49 | 3.63 | 1,764 / 0 / 4,235 | 1 `CLIENT_MISSED_OFFER` |
| Landed pinned | 3.25 | 6.92 / 6.24 | 3.67 | 1,760 / 0 / 4,239 | 1 `CLIENT_MISSED_OFFER` |
| Landed dynamic | 3.22 | 6.60 / 6.28 | 3.39 | 0 / 0 / 6,000 | 0 |

The light p99 criterion held with all three configurations, including the controls that rerouted
light traffic. No light request received an HTTP failure. The first quiet window of the landed
dynamic run missed one scheduled offer. The other five quiet windows completed all 1,500 offers.
Every valid quiet reply came from `node-3`.

| Configuration | Hot completions in 60 s (nodes 1/2/3) | Hot completions including drain (nodes 1/2/3) | Completed/s | Hot client refusals | Admission refusals by node |
| --- | ---: | ---: | ---: | --- | --- |
| Before | 0 / 196 / 336 | 0 / 197 / 463 | 8.87 | 0 | 325 `OVERLOADED` on node 3 |
| Landed pinned | 0 / 0 / 336 | 0 / 0 / 464 | 5.60 | 196 `OVERLOADED` | 445 `OVERLOADED` on node 3 |
| Landed dynamic | 91 / 268 / 284 | 91 / 274 / 295 | 10.72 | 0 | `WORKER_SATURATED` on node 2 (91) and node 3 (365) |

All other admission and hot-client error counts were zero. The VM averaged 80.6% idle in the
before loaded window, 86.5% pinned, and 77.0% landed dynamic. The respective lowest one-second
idle readings were 75.8%, 85.0%, and 65.4%.

## Method

- Use the three-node [ingress placement fixture](../tests/cluster/README.md#stateless-worker-ingress-placement)
  with one ordinary process per node. Use `homeProbability: 1` in all conditions so home selection
  is deterministic. The pinned control adds a static placement for only the hot Worker. It uses the
  same product binaries and kernel as the landed dynamic run. The ordinary ingress path uses the
  real readings and shipped thresholds. The fixture keeps the default 128-root admission limit.
- Deploy one ordinary Worker per tenant through cowctl. Both Workers have their home on `node-3`.
  The client sends every request to `node-3` over the Compose frontend network. The hot Worker
  performs 150 million fixed integer loop iterations per request and returns a checksum, request ID,
  and its execution node from `request.cf.nodeId`. The second tenant's Worker just returns its
  execution node. In the 15 s runs the driver requires every valid light reply to come from `node-3`.
- Warm each Worker at home with six sequential calls. For each product revision, first measure one
  statically pinned isolate with four continuously outstanding requests and count replies in 15 s.
  Each revision completed 85 requests inside the window (5.67/s) and four more during the drain.
  Use a fixed 11 hot requests/s in every loaded window, 1.94× that single-isolate rate.
- Run alternating 15 s quiet and loaded windows, starting and ending quiet. Each of the three loaded
  windows schedules 165 hot offers. Each window schedules 1,500 light offers at 100/s independently
  of outstanding requests. After each loaded window, drain all outstanding replies and wait 4 s
  for the isolate busy window to clear. Peers can first load the hot Worker during spillover.
- Count successful replies inside the 15 s window for throughput. Count validated replies through
  the drain by execution node so queued completions remain visible. Take nearest-rank p99 from the
  planned send time through receipt and validation of the full reply. Failed or missed offers rank
  above valid replies. Report the largest absolute difference between a loaded p99 and either
  neighboring quiet reading.
- Record client-visible outcomes by code and the per-node change in `placement.rejections` from
  before the window through its drain. The latter counts admission refusals that can be handled
  internally by trying another candidate. Sample the aggregate Linux `/proc/stat` idle fraction
  in 1 s intervals in every window.

The measurements cover one colocated ARM64 VM, one initial ingress, one compute loop and one
offered rate. The light p99 includes client scheduling and the Compose network. Throughput includes
each window's ramp into load and uses only completions inside that window.

## Reproduction

Use a quiet Linux Docker engine with spare CPU and the
[cluster build prerequisites](../tests/cluster/README.md#requirements-and-commands). Run from the
checkout containing this driver. Build both revisions before starting the measurements, and run the
clusters one at a time.

```sh
root=$PWD
before="$root/build/placement-before"
landed="$root/build/placement-landed"
mkdir -p "$root/build"
git worktree add --detach "$before" dec0b3b
git worktree add --detach "$landed" 398c7a3
npm ci --prefix cli --registry=https://registry.npmjs.org
for product in "$before" "$landed"; do
  (cd "$product" && bash scripts/build.sh -DCOWORKERD_TEST_WORKERD= &&
    cmake --build build --target coworkerd-launcher)
done
before_kernel=$(cd "$before" && python3 scripts/fetch-kernel.py)
landed_kernel=$(cd "$landed" && python3 scripts/fetch-kernel.py)
driver=tests/cluster/hot-worker-placement.mjs

node "$driver" calibrate "$landed" "$landed_kernel" build/landed-calibration.jsonl 150000000
node "$driver" calibrate "$before" "$before_kernel" build/before-calibration.jsonl 150000000
node "$driver" dynamic "$before" "$before_kernel" build/before-results.jsonl 150000000 11
node "$driver" pinned "$landed" "$landed_kernel" build/pinned-results.jsonl 150000000 11
node "$driver" dynamic "$landed" "$landed_kernel" build/landed-results.jsonl 150000000 11

node "$driver" dynamic "$before" "$before_kernel" build/before-long.jsonl 150000000 11 --long
node "$driver" pinned "$landed" "$landed_kernel" build/pinned-long.jsonl 150000000 11 --long
node "$driver" dynamic "$landed" "$landed_kernel" build/landed-long.jsonl 150000000 11 --long
```

The JSONL includes build and CPU readings, per-offer timing and outcomes, per-window summaries,
and refusal counts for each node. `--long` also records the home root counts. On a different
machine, calibrate first and set the same hot offer rate near twice that calibration for all three
conditions.

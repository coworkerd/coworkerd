# A hot Worker above three-node capacity

This measurement covers the overload acceptance case in [#31](https://github.com/coworkerd/coworkerd/issues/31).
It uses the ordinary ingress fixture and the stateless compute Worker from the
[hot Worker placement measurement](hot-worker-placement.md).

At 15.0 requests/s, 10.3% above the highest calibrated rate, all 900 requests completed.
There were zero client `OVERLOADED` and `PLACEMENT_EXHAUSTED` responses and zero other client
errors. Of the 900 replies, 415 completed after offers stopped. The overload p99 was
33.10–35.99 s, compared with 4.47–5.37 s in the control windows. [Measurement JSONL](hot-worker-overload/arm64.jsonl)

## Three-node calibration

The measured completion rate was 13.47–13.60 requests/s with 48–72 lanes. It fell to 9.63/s
with 96 lanes, and p99 rose to 28.37 s at that setting. Use the highest measured
30 s rate, 13.60/s, as the fixture's reference. The fixed control rate is 8.2/s and the
overload rate is 15.0/s, 10.3% above the reference.

| Closed-loop lanes | Scored completions by node (1 / 2 / 3) | Scored completions by 10 s interval | Completed/s | Valid replies including ramp and drain | p99 including ramp and drain (s) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 48 | 118 / 136 / 150 | 139 / 135 / 130 | 13.47 | 689 | 9.44 |
| 72 | 122 / 135 / 151 | 138 / 143 / 127 | 13.60 | 719 | 14.88 |
| 96 | 97 / 91 / 101 | 104 / 96 / 89 | 9.63 | 586 | 28.37 |

Every request in the calibration returned a valid reply. All measurements use the same
Worker, ordinary ingress, and three-node fixture. The capacity estimate applies to that path
and workload.

## Fixed-rate windows

| Window | Offers / valid replies | Replies during offers | Replies after stop | Last reply after stop (s) | p50 / p95 / p99 (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control 1 | 164 / 164 | 148 | 16 | 2.90 | 2.21 / 4.47 / 5.37 |
| Overload 1 | 300 / 300 | 161 | 139 | 35.39 | 9.50 / 33.34 / 35.99 |
| Control 2 | 164 / 164 | 150 | 14 | 3.58 | 2.23 / 4.32 / 4.74 |
| Overload 2 | 300 / 300 | 163 | 137 | 31.49 | 8.65 / 30.46 / 33.10 |
| Control 3 | 164 / 164 | 147 | 17 | 3.52 | 2.06 / 4.29 / 4.47 |
| Overload 3 | 300 / 300 | 161 | 139 | 34.76 | 9.07 / 32.94 / 35.56 |

All offers in the table were sent inside their 20 s window. There were zero missed offers,
timeouts, or HTTP errors in either condition. The client counted 137–139 requests awaiting a
reply at overload stop, and every one finished. All three nodes returned valid replies, and
their root counters returned to zero after each window.

| Window | Valid replies by node (1 / 2 / 3) | Peak sampled roots by node (1 / 2 / 3) | Roots at stop across nodes | Candidate `WORKER_SATURATED` by node (1 / 2 / 3) | Successful second passes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Control 1 | 42 / 48 / 74 | 15 / 14 / 19 | 16 | 0 / 42 / 90 | 0 |
| Overload 1 | 45 / 64 / 191 | 35 / 32 / 124 | 139 | 135 / 180 / 242 | 135 |
| Control 2 | 40 / 50 / 74 | 13 / 14 / 15 | 14 | 0 / 40 / 90 | 0 |
| Overload 2 | 45 / 78 / 177 | 35 / 31 / 117 | 137 | 121 / 166 / 244 | 121 |
| Control 3 | 32 / 56 / 76 | 10 / 15 / 13 | 17 | 0 / 32 / 88 | 0 |
| Overload 3 | 44 / 67 / 189 | 33 / 31 / 122 | 139 | 136 / 180 / 247 | 136 |

In each overload window, all three isolates were at or above 90% busy in four of the nine
samples taken from 4 s through the stop. Node 3 recorded one internal `OVERLOADED` in the
first overload window, and placement recovered the request. The other two overload windows
recorded zero hard refusals and 257 successful second passes between them. The Linux VM was
at least 66% idle in every one-second overload sample.

## Pilot

The pilot scored 11.57 replies/s with 24 closed-loop lanes and 13.53 replies/s with 48 lanes
after a 10 s ramp. The increase motivated the additional 72- and 96-lane calibrations.
It also tried three 30 s windows at 14.9 requests/s with controls at 10.8 requests/s. All
1,341 overload requests completed with zero client errors. Of these, 144, 186, and 181 per
window completed after offers stopped. The home reached its 128-root limit and recorded
1, 41, and 39 internal `OVERLOADED` outcomes. The pilot's overload p99 ranged from 36.13 to
37.14 s. The main reading uses 20 s offers to leave room below the hard limit.
[Pilot JSONL](hot-worker-overload/arm64-pilot.jsonl)

## Fixture and method

- Use an Apple M1 Pro host with a Docker Desktop Linux ARM64 VM (`6.10.14-linuxkit`,
  10 vCPUs, 7.65 GiB RAM) and Docker Engine 28.5.1. The client ran Node.js 22.23.5.
  The client cpuset and node affinity covered CPUs 0–9, and all reported `cpu.max = max 100000`.
- Build the Linux ARM64 product from main at `e7611cc1f2ff711f2c0cc578925719542f5b8655` in CMake
  Release mode with Clang 19.1.7. Use its verified [workerd v1.20261114.1 kernel archive](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-ae0129282c9c-run-46081583762-1).
  The archive SHA-256 is `922cd9bd77a8b4fd5a78540f95e06ad799fffeb2c0588434c4c718006a055922`.
- Start a disposable three-node Compose cluster with one ordinary process per node. Enable ingress placement
  with `homeProbability: 1`, live saturation readings, and the default limit of 128 roots per node.
  Send every request to `node-3`, the Worker's home, from a Node.js client on the Compose frontend network.
  The Worker performs 150 million integer iterations per request and returns its execution node, request ID,
  and a checksum. Warm the home with six sequential requests and let the busy reading clear.
- On that cluster, calibrate with 48, 72, and 96 closed-loop lanes, each with one outstanding request.
  Each calibration starts with 20 s to build load, then counts completions for 30 s. Stop sending at 50 s
  and drain the replies. Use the highest scored completion rate to set both fixed offer rates.
- Run three 20 s windows at 60% and three at 110% of the calibration rate, rounded to 0.1 request/s.
  Alternate control and overload and keep the same two rates for all three pairs. Schedule the offers
  independently of prior responses, stop at the end of the window, and wait for outstanding replies.
  Start each window with zero active roots and pause four seconds between drained windows.
- A completion is a successful reply with a known execution node, the expected request ID, and the
  expected checksum. Completion latency runs from the planned send time to validation of the full reply.
  Use nearest-rank percentiles of valid replies and count every missed offer, client error, and HTTP
  error by outcome. The client gives each request up to 45 s.
- Sample admitted root slots and the 3 s isolate busy reading on every node every two seconds.
  Read the placement counters before load and after the nodes drain. `WORKER_SATURATED` in the node
  counters measures first-pass candidate feedback. The ingress's `saturationFallback` decision counts
  successful second-pass placements. The client's HTTP outcomes measure what callers received.

## Reproduction

Use a quiet Docker engine with spare CPU. Build the daemons and launcher using the
[cluster build prerequisites](../tests/cluster/README.md#requirements-and-commands) before timing the run.

```sh
npm ci --prefix cli --registry=https://registry.npmjs.org
kernel=$(python3 scripts/fetch-kernel.py)
node tests/cluster/hot-worker-placement.mjs overload "$PWD" "$kernel" build/hot-worker-overload.jsonl 150000000
```

Use a new output filename for each run. The JSONL records the product commit, binary and kernel hashes,
CPU and placement readings, and every request's timing and outcome.

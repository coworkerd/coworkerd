# A hot Worker above three-node capacity

This measurement covers the overload acceptance case in [#31](https://github.com/coworkerd/coworkerd/issues/31).
It uses the ordinary ingress fixture and the stateless compute Worker from the
[hot Worker placement measurement](hot-worker-placement.md).

At 10.0 requests/s with bracketing scores of 9.07 and 9.30 replies/s, all 600 overload
requests completed. The client saw zero refusals or other errors, and 121 replies arrived
after offers stopped. The first reading at 15.0/s also completed every request and shows
how concentrating the queue on the home node lengthens the drain.

The pilot's 11.57 and 13.53 replies/s calibration scores and the first reading's 13.47 and
13.60 replies/s scores largely precede V8's change in tier. The 9.63/s score at 96 lanes and
the 12.23/s score after a 90 s ramp span the change. The 150 s ramp bracket scored after the
change on every node and measured 9.07 and 9.30 replies/s on the 8-vCPU M1 Pro VM. A separate
ten-minute run measured [steady throughput on an M4 VM](hot-worker-sustained.md#ten-minute-product-readings).

## First reading on a 10-vCPU VM

At 15.0 requests/s, all 900 requests completed. There were zero client `OVERLOADED` and
`PLACEMENT_EXHAUSTED` responses and zero other client errors. Of the 900 replies, 415 completed
after offers stopped. The overload p99 was 33.10–35.99 s, compared with 4.47–5.37 s in the
control windows. [First reading JSONL](hot-worker-overload/arm64.jsonl)

### Calibration and service-rate drift

The initial sweep scored the last 30 s of each 50 s closed-loop window. It used the highest
initial reading (13.60/s) to set the 8.2/s control and the 15.0/s overload rate. Service slowed
between the initial scores and the fixed-rate windows. The home served 5.00–5.03/s in the
48- and 72-lane scores. In the 72-lane drain it served 41 waiting requests at 3.50/s. The
slowdown was already visible before the 96-lane calibration. The home continued near 3.5/s in
the overload drains, even after the peers finished. The peers also delivered fewer replies
per second in the later calibration and fixed-rate windows. The shift follows each isolate's
invocation count, as explained in the [V8 diagnosis](hot-worker-sustained.md#why-the-rate-changes).

| Closed-loop lanes | Scored completions by node (1 / 2 / 3) | Scored completions by 10 s interval | Completed/s | Valid replies including ramp and drain | p99 including ramp and drain (s) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 48 | 118 / 136 / 150 | 139 / 135 / 130 | 13.47 | 689 | 9.44 |
| 72 | 122 / 135 / 151 | 138 / 143 / 127 | 13.60 | 719 | 14.88 |
| 96 | 97 / 91 / 101 | 104 / 96 / 89 | 9.63 | 586 | 28.37 |

Every request in the calibration returned a valid reply. Over seconds 5–20 of the three
overload windows, the cluster delivered 9.33, 9.47, and 9.27 replies/s. Relative to those
loaded rates the 15.0/s offer rate was 58–62% higher.

### Fixed-rate windows

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

### Where requests waited after offers stopped

| Overload window | Roots at stop by node (1 / 2 / 3) | Last peer reply after stop (s) | Last home reply after stop (s) | Home-only drain (replies / replies per second) | Backlog divided by loaded rate (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 / 14 / 124 | 4.09 | 35.39 | 110 / 3.51 | 14.89 |
| 2 | 1 / 26 / 110 | 7.50 | 31.49 | 84 / 3.50 | 14.47 |
| 3 | 0 / 17 / 122 | 4.76 | 34.76 | 105 / 3.50 | 15.00 |

When all candidates send soft saturation feedback, the second pass considers them in the
same order with the idle preference cleared. The home is the first candidate in this fixture,
so it can accumulate most of the wait behind its one isolate. Once both peers finished
their own backlog, the home served the remaining replies alone for 24.0–31.3 s. Measured
drain took 2.18–2.38 times the backlog divided by the loaded rate, above the waiting-call
spread bound in #31. [Placement policy](cluster-state.md#7-placement)

### 30-second pilot

The pilot scored 11.57 replies/s with 24 closed-loop lanes and 13.53 replies/s with 48 lanes
after a 10 s ramp. The increase motivated the additional 72- and 96-lane calibrations.
It also tried three 30 s windows at 14.9 requests/s with controls at 10.8 requests/s. All
1,341 overload requests completed with zero client errors. Of these, 144, 186, and 181 per
window completed after offers stopped. The home reached its 128-root limit and recorded
1, 41, and 39 internal `OVERLOADED` outcomes. The pilot's overload p99 ranged from 36.13 to
37.14 s. Later readings use 20 s offers to leave room below the hard limit.
[Pilot JSONL](hot-worker-overload/arm64-pilot.jsonl)

## Bracketed readings on an 8-vCPU VM

The first bracket used a 90 s ramp before the scored calibration. The home dropped from
about 5.0 to 3.4 replies/s around 100 s, during the score. The pre-window rate was 12.23/s and
the post-window rate was 9.10/s. During the fixed-rate overload, the cluster delivered
9.13–9.27/s over seconds 5–20. All 810 overload requests at 13.5/s completed with zero client
errors. The per-window p99 ranged from 24.22 to 25.33 s. The next bracket extended the ramp
to 150 s to score after the shift. [90 s ramp JSONL](hot-worker-overload/arm64-short-ramp.jsonl)

### 150-second ramp

The 30 s scores on the two sides of the fixed-rate windows were 272 replies (9.07/s)
and 279 replies (9.30/s), a 2.6% increase. The offer rate of 10.0/s was 10.3% above the
pre-window score and 7.5% above the post-window score. Over seconds 5–20, the cluster
delivered 9.20–9.27/s, making the offer rate about 8–9% higher than the loaded rate.
The 5.4/s controls also returned every reply. [150 s ramp JSONL](hot-worker-overload/arm64-150s-ramp.jsonl)

| Window | Offers / valid replies | Replies during offers | Replies after stop | p50 / p95 / p99 (s) |
| --- | ---: | ---: | ---: | ---: |
| Control 1 | 108 / 108 | 100 | 8 | 1.28 / 2.36 / 2.63 |
| Overload 1 | 200 / 200 | 158 | 42 | 3.81 / 7.67 / 9.27 |
| Control 2 | 108 / 108 | 101 | 7 | 1.20 / 2.49 / 3.02 |
| Overload 2 | 200 / 200 | 162 | 38 | 3.29 / 6.08 / 7.66 |
| Control 3 | 108 / 108 | 103 | 5 | 1.07 / 1.92 / 1.95 |
| Overload 3 | 200 / 200 | 159 | 41 | 3.59 / 7.15 / 8.65 |

The overload returned every outstanding reply and recorded 138 successful second passes.
Every client outcome was a valid reply, and the node counters showed zero hard refusals.
At stop the home held 13–14 roots and node 1 held 17–19. Drain took 1.19–1.25 times the
backlog divided by the loaded rate in these windows.

| Overload window | Roots at stop by node (1 / 2 / 3) | Last reply after stop by node (s) | Backlog divided by loaded rate (s) |
| --- | ---: | ---: | ---: |
| 1 | 19 / 9 / 14 | 5.68 / 2.67 / 4.15 | 4.57 |
| 2 | 17 / 7 / 14 | 4.87 / 1.86 / 4.13 | 4.10 |
| 3 | 19 / 9 / 13 | 5.46 / 2.53 / 3.72 | 4.46 |

Both bracketed runs used an 8-vCPU, 15.60 GiB VM. During the 150 s ramp run, a separate
local CI helper overlapped pre-calibration and used about three vCPUs in one sample.
Another Compose project overlapped part of the fixed-rate windows. At the post-calibration
check, the measurement fixture and its client were the only visible containers. The
fixture used the same resource limits for the entire run.

## Fixture and method

The 150-second ramp reading used the following setup.

- Use an Apple M1 Pro host with eight performance and two efficiency cores, a Docker Desktop
  Linux ARM64 VM (`6.10.14-linuxkit`), Docker Engine 28.5.1, and a Node.js 22.23.5 client.
  The pilot and first reading used 10 vCPUs and 7.65 GiB of VM memory. The bracketed reading
  used 8 vCPUs after updating the Docker VM configuration. The fixture and its client were the
  only Docker workloads observed during the pilot, first reading, and 90 s ramp run. The
  measured product was built before timing.
  Observations cover guest CPU load and affinity. Host workload and core placement were
  outside the readings. The client and nodes reported `cpu.max = max 100000` in both configurations.
- Build the Linux ARM64 product from main at `e7611cc1f2ff711f2c0cc578925719542f5b8655` in CMake
  Release mode with Clang 19.1.7. Use its verified [workerd v1.20261114.1 kernel archive](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-ae0129282c9c-run-46081583762-1).
  The archive SHA-256 is `922cd9bd77a8b4fd5a78540f95e06ad799fffeb2c0588434c4c718006a055922`.
- Start a disposable three-node Compose cluster with one ordinary process per node. Enable ingress placement
  with `homeProbability: 1`, live saturation readings, and the default limit of 128 roots per node.
  Send every request to `node-3`, the Worker's home, from a Node.js client on the Compose frontend network.
  The Worker performs 150 million integer iterations per request and returns its execution node, request ID,
  and a checksum. Warm the home with six sequential requests and let the busy reading clear.
- On that cluster, run 72 closed-loop lanes with one outstanding request per lane. Let them run for
  150 s and then count completions for 30 s. Use that scored rate to set both fixed offer rates.
  After the fixed-rate windows, run 72 lanes again with a 10 s ramp and score the next 30 s to
  check whether the first rate held.
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
The current driver waits for at least 550 validated warm-up replies per execution node before
calibrating. Both 30 s calibration scores follow a 10 s ramp. The fixed-rate control and overload
windows use the same timing and drain method as the historical readings above.

```sh
npm ci --prefix cli --registry=https://registry.npmjs.org
kernel=$(python3 scripts/fetch-kernel.py)
node tests/cluster/hot-worker-placement.mjs overload "$PWD" "$kernel" build/hot-worker-overload.jsonl 150000000
```

Use a new output filename for each run. The JSONL records the product commit, binary and kernel hashes,
CPU and placement readings, and every request's timing and outcome.
The first reading used the [driver at `4efbc69`](https://github.com/coworkerd/coworkerd/tree/4efbc69e5ce72ab9d340ab204acbbe35c2b91475/tests/cluster).
The 150 s ramp reading used the [driver at `b66d2b4`](https://github.com/coworkerd/coworkerd/tree/b66d2b4828ed1822d5299ea6cdd2bc74ec419f17/tests/cluster).

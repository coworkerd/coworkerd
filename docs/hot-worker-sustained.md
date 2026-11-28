# Sustained throughput of a hot Worker

The compute Worker from [#152](https://github.com/coworkerd/coworkerd/issues/152) changes rate
when its request handler tiers up to Maglev in V8. On each traced product isolate, the 513th
invocation took more CPU and started the lower sustained rate. Standalone upstream workerd of
the same version reproduces the change. The product uses the upstream tiering policy.
[Maintainer decision](https://github.com/coworkerd/coworkerd/issues/152#issuecomment-7234607013)

## Why the rate changes

In a 200 s run through the three-node ingress, all 2,718 offered requests returned valid replies
through drain. The threaded child traced the execution of each JS entry. The table uses 100 compute
entries on each side of the step and includes the six sequential warm-up calls on node 3 in its
ordinal. All three isolates crossed at entry 513, at different elapsed times.

| Execution node | CPU ms, entries 412–511 | CPU ms, entries 513–612 | Wall ms, entries 412–511 | Wall ms, entries 513–612 |
| --- | ---: | ---: | ---: | ---: |
| node-1 | 192.15 | 231.01 | 192.16 | 231.04 |
| node-2 | 192.16 | 230.89 | 192.18 | 230.95 |
| node-3 | 192.07 | 230.87 | 192.08 | 230.89 |

The measurements are medians of the entries that execute the loop, identified by a thread CPU
cost above 50 ms. Each node traced one compute entry per validated request. CPU and wall time
rise together on each node, while the guest averaged 69.4% idle over the run.

The standalone control uses Linux ARM64 upstream workerd `1.20261114.1` from npm, a dynamic Worker,
and one closed-loop lane. V8 first enters TurboFan for `checksum` through on-stack replacement and
later installs the regular TurboFan entry for that function, around invocation 103. That transition
precedes the slowdown. Immediately before invocation 513, the Worker’s `fetch` is compiled to
Maglev. The `--no-maglev-inlining` control compiles `fetch` to Maglev at the same point and
keeps the earlier rate. This isolates Maglev inlining as the cause of the higher loop cost.

| Upstream workerd, same 150-million-iteration loop | Median interval, calls 412–511 (ms) | Median interval, calls 513–612 (ms) |
| --- | ---: | ---: |
| Default inlining | 171.7 | 218.3 |
| `--no-maglev-inlining` | 171.5 | 171.8 |

The packaged Coworkerd `workerd` binary also reproduced the change outside the node daemon with a
dynamic loader. The earlier Node.js container control sustained 4.9–5.1 loops/s on each of three
threads for 200 s. The upstream workerd control matches the product's V8 version and dynamic Worker
execution path. [Original Node.js control](https://github.com/coworkerd/coworkerd/pull/148#issuecomment-7234033073)

## Ten-minute product readings

The one-node rate fell from 5.72 replies/s in the first minute to 4.49 replies/s
over minutes 3–10, a 21.5% decrease. Through ingress the corresponding rates were
14.00 and 11.93 replies/s, a 14.8% decrease. Minute 2 includes the change in tier.

| Minute | One node, replies/s | Ingress, replies/s | Ingress replies by node (1 / 2 / 3) |
| ---: | ---: | ---: | ---: |
| 1 | 5.72 | 14.00 | 252 / 272 / 316 |
| 2 | 5.08 | 14.13 | 278 / 278 / 292 |
| 3 | 4.50 | 12.07 | 230 / 234 / 260 |
| 4 | 4.50 | 11.93 | 231 / 225 / 260 |
| 5 | 4.50 | 11.93 | 225 / 231 / 260 |
| 6 | 4.50 | 11.98 | 231 / 228 / 260 |
| 7 | 4.48 | 11.95 | 227 / 230 / 260 |
| 8 | 4.48 | 11.97 | 230 / 229 / 259 |
| 9 | 4.45 | 11.93 | 231 / 225 / 260 |
| 10 | 4.48 | 11.67 | 214 / 226 / 260 |

All 2,826 one-node calls and all 7,486 ingress calls returned valid replies through drain.
The guest averaged 88.8% idle on one node and 69.4% idle through ingress.
[Machine-readable readings and selected trace samples](hot-worker-sustained/arm64.jsonl)


## Conditions and reproduction

The product binaries were built in CMake Release mode with Clang 19.1.7 from `78b9ecb`. The verified
Linux ARM64 archive was `kernel-v1.20261114.1-ad2ece2dcdd8-run-46095858558-1`, SHA-256
`428d8af8da7c8c11545bbdc3365b08ada2853a68ac6a8f6f46728fd7a6bae550`. The standalone upstream npm binary's
SHA-256 was `b29181875a482f744016f654b1d654f134a02632ce8723f6b2b1c980210ec39e`.

The host was an Apple M4 with four performance and six efficiency cores. Docker Desktop ran Linux
`6.12.76-linuxkit`, Engine 29.5.3, with ten vCPUs and 7.75 GiB visible. The nodes and client had
affinity to vCPUs 0–9 and `cpu.max = max 100000`. The client ran Node.js 22.23.5. The timed runs
were serial, after the local product build. Host and Docker CPU snapshots about every 15 s showed
two idle pre-existing containers and no overlapping local build.

For the product, use the `sustained` commands in the [cluster test guide](../tests/cluster/README.md#stateless-worker-ingress-placement).
Each request runs the same 150-million-iteration integer loop and returns its execution node, ID,
and checksum. The fixture validates six sequential calls, waits four seconds, then starts the timed
load. It counts validated completions in each minute and records every outstanding reply through
drain. Use 24 lanes on one node and 72 through ingress, with one outstanding request per lane.

For standalone workerd, obtain the upstream arm64 npm archive, unpack it, and run each control
separately on Linux arm64 with Node.js 22 or later. The fixture prints V8 compilation events and
validated reply timings through call 650.

```sh
mkdir -p build/upstream-workerd
npm pack --pack-destination build/upstream-workerd @cloudflare/workerd-linux-arm64@1.20261114.1
tar -xzf build/upstream-workerd/cloudflare-workerd-linux-arm64-1.20261114.1.tgz -C build/upstream-workerd
upstream=build/upstream-workerd/package/bin/workerd
node tests/cluster/hot-worker-v8-control.mjs "$upstream" > build/workerd-stock.jsonl
node tests/cluster/hot-worker-v8-control.mjs "$upstream" --no-maglev-inlining > build/workerd-no-maglev-inlining.jsonl
```

For placement and overload measurements, the regular driver warms participating isolates with at
least 550 validated replies per execution node. It cools the busy reading before starting the scored
work. See the [placement report](hot-worker-placement.md) and [overload report](hot-worker-overload.md)
for the distinction between the original early scores and the steady measurements.

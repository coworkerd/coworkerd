# Single-node waiting-call capacity

The fixture deploys one stateless Worker to one ordinary process. Each ordinary HTTP call makes
one outbound HTTP request through the node to a private upstream. The upstream records entry and
release on a monotonic clock and returns the request ID. The client verifies the reply and serving
node. The reported peak is the maximum overlap of those upstream intervals. Each batch has a fixed
offer count and a deadline.

The measurements used the published ARM64 kernel pinned by product `e972a74` and an eight-vCPU
Docker Desktop Linux VM (Linux 6.10.14, 25,163,026,432 bytes of guest RAM). Ingress placement was
enabled. The Worker could reach the private upstream, and the per-tenant outbound rate and
concurrency limits were off. The client and upstream had separate network namespaces and cgroups.
Each had a 768 MiB memory cap, a two-CPU quota, a 128-task limit and a 32,768-descriptor soft limit.
The node had 16 GiB and 40,000 tasks for the original and raised-default comparisons. The upper
capacity probes used 20 GiB and up to 50,000 tasks. [Fixture](../tests/cluster/README.md#single-node-waiting-calls) · [Published ARM64 kernel](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-ae0129282c9c-run-46081583762-1)

## Two-second waits

| Node settings | Offered | Verified replies | True upstream peak | Time at peak |
| --- | ---: | ---: | ---: | ---: |
| Original defaults | 2,000 × 2 | 128 each | 128 each | 1.977 s, 1.976 s |
| 6,000 roots, 6,256 process slots | 2,000 × 2 | 2,000 each | 2,000 each | 1.392 s, 1.524 s |
| 6,000 roots, 6,256 process slots | 4,500 | 4,500 | 4,500 | 0.753 s |
| 6,000 roots, 6,256 process slots | 6,000 | 6,000 | 3,483 | 1.592 s |
| 10,500 roots, 10,756 process slots | 6,000 | 6,000 | 6,000 | 0.091 s |
| 10,500 roots, 10,756 process slots | 7,500 | 7,500 | 7,028 | 0.001 s |
| 10,500 roots, 10,756 process slots | 9,000 | 9,000 | 7,705 | 0.055 s |

The original root limit was 128. Each 2,000-call batch produced 1,872 client-visible
`PLACEMENT_EXHAUSTED` responses, matching the node's `OVERLOADED` count, and every admitted call
waited two seconds. With the experimental limits, both 2,000-call batches finished with
zero refusals. Client p99 was 2.198–2.315 s. The client took
4.309 s to offer the first 6,000-call batch and 8.144 s for the 9,000-call batch. At 10,000 offered
the original 16 s collection deadline expired with 7,001 settled client results. This ended that
two-second probe before every offer was accounted for.

## Maximum waiting population

A separate 12-second series gives a request time to enter the upstream while earlier waits remain
active. The launch schedule was four seconds. The upstream and client timestamps record the actual
spread even when offers take longer.

| Root / process limit | Offered | Verified replies | True upstream peak | Time at peak |
| --- | ---: | ---: | ---: | ---: |
| 10,500 / 10,756 | 10,000 | 10,000 | 10,000 | 8.048 s |
| 10,500 / 10,756 | 10,500 | 10,500 | 10,500 | 1.407 s |
| 11,400 / 11,400 | 11,250 | 11,250 | 11,250 | 8.062 s |
| 11,400 / 11,400 | 11,500 | 11,442 | 11,400 | 0.004 s |

At 11,500 offered, placement refused 58 before execution. Another 42 successful calls completed outside the 11,400-call peak. This measured the configured root bound. On the same
20 GiB node budget, the startup check accepted 11,516 configured process slots and refused 11,517
because the 3.2 GiB native-process share would leave insufficient memory for an active Worker.
The test allowed 50,000 tasks and a 1,048,576 hard file-descriptor limit. The 11,517-slot
configuration required 46,216 tasks and fit its descriptor budget. The resource that bounded
further startup capacity in this container was the native process memory allowance.

## Resource cost

The following readings compare the settled idle node with a sample bracketed by 11,250 active
upstream waits. The node reported 11,250 roots, zero nested calls and a green load band. The ordinary
process stayed on the same generation.

| Reading | Idle | During 11,250 waits | Increase per wait |
| --- | ---: | ---: | ---: |
| Ordinary process threads | 59 | 11,291 | about 1 |
| Daemon open descriptors | 126 | 45,129 | about 4 |
| Ordinary process open descriptors | 11,569 | 67,765 | about 5 |
| Daemon RSS | 27,804 KiB | 1,951,572 KiB | about 171 KiB |
| Ordinary process RSS | 263,052 KiB | 3,266,908 KiB | about 267 KiB |
| Node cgroup memory | 883,818,496 bytes | 6,481,068,032 bytes | about 486 KiB |

Node cgroup memory is a container-wide reading. RSS rows describe individual processes. The client
and upstream cgroups separately used 230,301,696 and 176,107,520 bytes during this sample. The
three cgroups had no recorded OOM or task-limit events, and no listener overflows appeared at
the sample. The numbers describe this Worker and its HTTP connections. Separately from the live
measurements, the released kernel reserves 68 MiB plus 256 KiB per configured native call slot
against the process memory budget at startup. That reservation reduces the budget available
to resident Workers, including when the pool is idle. The daemon also sizes task and descriptor
headroom for quarantine and replacement generations. [Installation guide](guide/install.md#waiting-call-capacity)

## Reproduction

Build the daemons and launcher for the host Linux architecture and fetch the checkout's pinned
published kernel. Run each command with a new output path. To use the original binaries, set
`COWORKERD_CLUSTER_PRODUCT_COMMIT=e972a74` and point `COWORKERD_CLUSTER_BIN_DIR` to that build.
The fixture records binary SHA-256 digests, the effective configuration, kernel provenance, raw
timestamps and resource samples.

```sh
kernel=$(python3 scripts/fetch-kernel.py)
node tests/cluster/waiting-call-capacity.mjs "$kernel" build/original.jsonl \
  --batches 128,512,2000,2000 --ramp-ms 300
node tests/cluster/waiting-call-capacity.mjs "$kernel" build/two-second-acceptance-probe.jsonl \
  --root-limit 6000 --nested-limit 6256 --process-limit 6256 \
  --batches 2000,2000 --ramp-ms 300 --expect-full
node tests/cluster/waiting-call-capacity.mjs "$kernel" build/fixed-two-seconds.jsonl \
  --root-limit 10500 --nested-limit 10756 --process-limit 10756 \
  --node-memory-gib 20 --node-tasks 46000 --batches 6000,7500,9000,10000 --ramp-ms 300
node tests/cluster/waiting-call-capacity.mjs "$kernel" build/long-hold.jsonl \
  --root-limit 11400 --nested-limit 11401 --process-limit 11400 \
  --node-memory-gib 20 --node-tasks 50000 --batches 11250,11500 --wait-ms 12000 --ramp-ms 4000
```

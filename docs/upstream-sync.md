# Following upstream workerd

> Our changes to workerd, how they are maintained, and the operating manual for following upstream are in the [patch notes](../patches/README.md); this page describes the fork and how a sync with upstream is done and checked.

**The public fork**: the modified workerd is in the public fork [coworkerd/workerd](https://github.com/coworkerd/workerd): its branch `coworkerd-v1.20261012.1` is upstream `5358e3d` plus nine patch commits (tip `d11d84d`), and `patches/workerd/` is the reviewable copy exported from it. Following upstream means rebasing onto a new tag into a `coworkerd-<tag>` branch; future V8 patches go into the fork's `patches/v8/`.

- **How it is maintained**: stock workerd plus a patch series, kept in the public fork (above); synced about every 4 weeks, and immediately when V8 has a security fix.
- **Current version**: `v1.20261012.1` (`5358e3d`).

## 1. How a sync is done and checked

The sync to `v1.20261012.1` (`5358e3d`) ported the patches across 48 upstream releases at once; what it did and checked is the method for the next sync.

**The patches stay thin**: logic goes into files of our own, with only hooks left in upstream files. At this sync the CPU limit's logic moved out of `server.c++` as a block into `coworkerd-cpu-limit.{h,c++}` (381 lines), and the changes to upstream files went from +312 / -12 to **+60 / -15**.
`server.c++` keeps only the hooks: creating the enforcer (wrapping upstream's original empty implementation), handing it to `WorkerService`, wrapping the request entry.
The patches take their parameters from environment variables (such as `COWORKERD_CPU_LIMIT_MS`), so upstream's command line entry, now in Rust, is untouched; the server builds a separate environment for each workerd child process
without changing its own.

**The source**: the series, applied one by one to a separate clean upstream clone, gives a tree byte-identical to the checkout.

**Behaviour and numbers**: the production builds before and after a sync run the same measurements, three times each in the same environment and topology. After the sync to `v1.20261012.1`:

| Measurement | `v1.20261012.1` |
| --- | --- |
| Actual termination time of the CPU limits 50 / 200 / 1000 ms (finite loops) | 51.3 / 201.8 / 1001.7 ms |
| A long builtin that cannot be terminated: stuck until killed / killed until a new process is ready | 3005 / 210 ms |
| Repeatedly stuck for 60 seconds: processes killed; ordinary tenants' success rate afterwards | 1 ordinary + 11 quarantine; 100% (3,281 calls) |
| Cold start p50 / p99 | 4.45 / 12.9 ms |
| Steady-state p50 / p99: local execution; cross-machine Binding | 0.75 / 2.19; 1.68 / 3.55 ms |
| Resident memory per ordinary process: idle; with 100 Workers loaded | 31.9; 237.4 MiB |

Behaviour and numbers were unchanged by the sync, except that the cross-machine Binding's p99 dropped by about 1 ms, beyond the variation between runs; recorded, not attributed.

**The independent rebuild**: on Linux the independent rebuild of workerd was not byte-identical with the build. The two 143 MB binaries differ in only 28 bytes:
20 are the build ID, and 8 are in upstream KJ's `HttpInputStreamImpl::readRequestHeaders()`. The three execution paths through these instructions in that function were checked one by one:
the only difference is that the registers x8 and x9 swap roles; the locations and values written to memory are exactly the same, and the following function calls do not read these two registers -- **the two binaries behave equivalently**.
Repeating the same compile command three times always gives the rebuild's version; the build that differed is the exception, for reasons not found (it had been interrupted and resumed once). The server, rebuilt from scratch in a new build directory (Linux), was byte-identical.
Where an independent rebuild is not byte-identical, locate the difference byte by byte as done here, and judge whether the binaries behave equivalently.

**New upstream mechanisms**, as checked at the sync to `v1.20261012.1`:

- Isolate groups: in our production builds (Linux, macOS) `CanCreateNewGroups()` is false -- all Workers still share one pointer compression cage;
- Startup snapshots: the autogate is off, and no isolate for snapshots appeared while running; upstream currently only generates snapshots and does not use them yet (`TODO(soon): use snapshot artefact`);
- Compatibility flags 290 -> 294 (4 new, none removed), the maximum compatibility date 2026-09-03 -> 2026-10-19; the server's own copies of `EventDispatcher` and `WorkerdBootstrap`
  are compatible with the new version (upstream only added `udpConnect`).

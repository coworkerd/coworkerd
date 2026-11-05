# Patches

Coworkerd runs upstream [workerd](https://github.com/cloudflare/workerd) with a thin series of patches.

| Path | What it is |
| --- | --- |
| `workerd/UPSTREAM` | The upstream tag and commit the series applies to (`v1.20261012.1`, `5358e3d`) |
| `workerd/series` | The order of the patches |
| `workerd/0001-native-binding.patch` ... | One patch per file; the header of each states its purpose |
| `kj-http-abort.diff` | A change to Cap'n Proto's KJ that `CMakeLists.txt` applies when it builds the daemons: KJ's HTTP entity writers honor `abortWrite()`, so a response that fails midway ends with an error instead of looking complete |

## How the series is maintained

- **The fork.** The patched workerd is the fork [coworkerd/workerd](https://github.com/coworkerd/workerd): one branch
  per upstream tag, `coworkerd-<tag>` (now `coworkerd-v1.20261012.1`), which is the tag plus one commit per patch.
  `patches/workerd/` is exported from that branch so that every change to workerd can be reviewed here; a change to
  the branch and its exported copy go into the same change of this repository.
- **Thin patches.** Changes stay in `src/workerd/server/` (`scripts/workerd-patches.py` checks it), with one explicit
  exception: `0015-rpc-invocation-identity.patch` may also change `src/workerd/api/worker-loader.h`,
  `src/workerd/api/worker-loader.c++`, `src/workerd/io/io-channels.h`, and `src/workerd/io/io-channels.c++`.
  These hooks carry an owned invocation identity from the trusted platform loader to each dynamic entrypoint;
  they do not change request lifetimes or accept identity from tenant headers, props, or logs. The exception is
  bound to that patch and those exact paths. Other patches remain limited to the server directory; IoContext,
  V8 and the general RPC protocol are outside this exception. A second explicit exception,
  `0016-rpc-code-parent-death.patch`, changes `src/workerd/jsg/util.c++` at one conversion hook and
  `src/workerd/jsg/BUILD.bazel` to build our `coworkerd-rpc-code.h` and `coworkerd-rpc-code.c++` helpers there.
  The hook sets a platform error's `.code` from exception detail ID `0xc6f4a09d8b3e2175`, shared with the
  node's `common/rpc-error.h`; it never infers a code from an application message. That patch also watches
  the inherited parent pipe through the existing channel startup I/O provider in our server helpers.
  Keep implementation in our helpers
  and upstream hooks small so that following upstream stays manageable.
- **Following upstream.** About every four weeks, and at once when V8 has a security fix: a multi-tenant platform
  runs mutually untrusting code, and lagging behind upstream is itself an exposure. V8 patches, if any are needed, go
  into the fork's `patches/v8/`, where upstream keeps its own.
- **A contract with the daemons.** The patches and the node change together (the CPU limit's parameters, the culprit
  reports and the descriptors workerd inherits are a contract between them), and a release's `VERSION` names the
  upstream tag and the series it was built from.

## Working on the series

`scripts/workerd-patches.py` goes back and forth between the files here and commits in a workerd checkout (by default
`workerd/`, which this repository ignores):

```sh
git clone https://github.com/cloudflare/workerd.git workerd
git -C workerd switch --detach "$(sed -n 's/^tag=//p' patches/workerd/UPSTREAM)"
python3 scripts/workerd-patches.py import     # one commit per patch (apply: the changes staged, no commits)
python3 scripts/workerd-patches.py check      # the checkout is exactly UPSTREAM plus the series
```

To follow a new tag, rebase the commits onto it (in the fork, as the branch `coworkerd-<new tag>`), resolve conflicts
keeping only hooks in upstream files, write the new tag and commit into `UPSTREAM`, and export:

```sh
git -C workerd fetch --depth=1 https://github.com/cloudflare/workerd.git "refs/tags/$TAG:refs/tags/$TAG"
git -C workerd rebase --onto "$TAG" "$(sed -n 's/^commit=//p' patches/workerd/UPSTREAM)"
python3 scripts/workerd-patches.py export
python3 scripts/workerd-patches.py check
```

The export is deterministic (no commit hashes, signatures or statistics): exporting the same commits twice gives the
same files. A new series also needs the compatibility flags the node accepts regenerated
(`python3 scripts/compatibility-flags.py`, into `common/generated/`), new workerd builds for both architectures,
their system call allowlists (`scripts/syscall-profile.mjs`, then `scripts/syscall-allowlist.py`) and the pinned
hashes in `scripts/release.sh`. `check` is the source side of checking a sync: the complete checkout tree differs
from the tag in `UPSTREAM` by exactly the series, and each patch stays within its permitted paths. All four
operations validate the series before mutating a checkout or exporting files.

## Rust server port (v1.20261031.1)

The `port-0.5.4` series targets `v1.20261031.1` and is exported from
`coworkerd-v1.20261031.1`. It includes 0002, 0006, 0007, 0008, 0012, 0016, 0005,
0010, 0011, 0015 and 0017, in that order. Deferred patches remain outside `series`.

### 0005: startup identity

The synchronous loading request supplies an owned startup identity through the existing
`loadIsolate` / `load_isolate` argument. 0017 expands that slot into a creation record,
including the startup identity, without adding another loading method or argument.

### 0015: invocation identity

The API/io hooks listed above bind the invocation identity to each entrypoint. The exact Rust
exceptions are `src/workerd/server/server/bridge.rs` and `src/workerd/server/server/loader.rs`:
Rust owns the string and copies it into request metadata through one C++ setter before starting
the request. The factory prefers this identity to the fetch `cf` blob; actor classes are unchanged.

### 0017: explicit creation record

Only the platform runtime holds the WorkerLoader capability. Its optional third `get()` argument
is `{ coworkerdWorker: { workerId, versionId, generation?, cpuLimitMs?, memoryLimitMiB? } }`.
The API validates the two nonempty strings and nonnegative integer options before loading;
limits fit uint32. It derives `tenant` from the text before the first slash in `workerId`, or
the empty string without a slash. Generation is reserved and remains zero. Missing limits or
zero select the process environment values. `load()` has no creation-record option.

One flat cxx `CoworkerdLoadIdentity` carries `startup_identity`, `present`, `tenant`, `worker_id`,
`version_id`, `generation`, `cpu_limit_ms` and `memory_limit_mib`. Rust only transports it;
configuration workers receive an empty record. The factory fills `CoworkerdWorkerIdentity` with
the six creation fields. Record presence enables tenant CPU/memory accounting and selects the
tenant GC behavior; its limit fields configure the enforcer and `workerId` selects the shared
GC budget. Without a record the platform/static heap policy applies. Loader names remain opaque
to these hooks and event fields remain unchanged.

The exact API/io exceptions are `src/workerd/api/worker-loader.h`,
`src/workerd/api/worker-loader.c++` and `src/workerd/io/io-channels.h`. The exact Rust exceptions
are `src/workerd/server/server/bridge.rs`, `src/workerd/server/server/loader.rs` and
`src/workerd/server/server/worker.rs`; other Rust files remain excluded. Runtime callers must
supply this record when their integration is ported.

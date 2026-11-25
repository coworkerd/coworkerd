# Contributing

## Building

- **The daemons for the host** (macOS or Linux, with CMake 3.24 or newer, Ninja, OpenSSL, Git and a C++23 compiler):
  `bash scripts/build.sh` builds `build/coworkerd` and `build/coworkerd-apiserver`. It fetches Cap'n Proto from GitHub
  and patches KJ's [HTTP write aborts](patches/kj-http-abort.diff) and
  [WebSocket handshake cancellation](patches/kj-websocket-handshake-eof.diff). The launcher, `coworkerd-launcher`, is
  Linux only: there `build.sh` builds it too, which takes a C compiler as well.
- **cowctl**: `npm ci --prefix cli` installs its dependencies (Node.js 22.5 or newer: the wrangler and Miniflare it
  pins require Node.js 22, and its adapter uses `path.matchesGlob`); `node cli/bin/cowctl.mjs` runs it.
- **The Linux kernel binaries**: `python3 scripts/fetch-kernel.py` fetches and verifies the matching
  `workerd`, `coworkerd-threaded` and system call allowlist. It prints the installation directory under
  `build/kernel/`. It requires Python 3.9 or newer and uses curl for public GitHub Releases downloads. The checkout pins
  archive SHA-256 values in [patches/workerd/artifacts.json](patches/workerd/artifacts.json). Use `--local-dir DIR`
  for a local release mirror. Fetch verifies the same SHA-256 for a download, a local archive, or a cached archive.
  For an architecture with no published archive, see [Building the kernel from source](#building-the-kernel-from-source).
- **Linux binaries, a release package and its image**: `scripts/release.sh <version>` builds `coworkerd`,
  `coworkerd-apiserver` and `coworkerd-launcher` in a Linux toolchain container
  ([scripts/toolchain/](scripts/toolchain/) has setup instructions), calls the same fetch script, and assembles the
  package and image `coworkerd:<version>` with the verified binaries and allowlist. Use `--kernel-dir DIR`
  to consume a local release mirror. The toolchain container needs a configured CMake tree, and the release version
  must match `cli/package.json`. Build each product release archive on a machine of its architecture.

### Building the kernel from source

On Linux, install Bazelisk as `bazel`, Git, Python 3 and the libxml2 runtime library (`libxml2.so.2`).
Bazel downloads the compiler, linker and sysroot. The downloaded linker loads libxml2 from the host.

Clone and apply the exact upstream tag and series using the commands in [patches/README.md](patches/README.md).
On Linux, run from the repository root:

```bash
python3 scripts/workerd-patches.py check
(cd workerd && bazel build --config=release //src/workerd/server:workerd //src/workerd/threaded:coworkerd-threaded)
```

Generate the allowlist on the target architecture using profiles from `scripts/syscall-profile.mjs` and
`scripts/syscall-allowlist.py` as described in the system call allowlist section of the
[installation guide](docs/guide/install.md). Run the generator from a directory where you can pass relative
paths for every `--workerd` and `--profile` input, because it writes those paths into `allowlist.json`.
The profiling binary directory must contain both `workerd` and its sibling `coworkerd-threaded`.
The profiler records their separate hashes and selects the traced program by process role. A legacy GC
counter absent from the threaded lifecycle is recorded as unobserved, not as a successful collection.
The generated allowlist must name the exact `workerd` being packaged; keep the paired threaded hash
in the profile provenance with the archive record.
If `ALLOWLIST` is the generated directory, package and unpack your build into the same layout as the fetch command:

```bash
case "$(uname -m)" in
  x86_64) arch=linux-x86_64 ;;
  aarch64|arm64) arch=linux-arm64 ;;
esac
python3 scripts/package-kernel.py --arch "$arch" --allowlist "$ALLOWLIST" --out build/kernel-release
mkdir -p build/local-kernel
tar -xzf "build/kernel-release/coworkerd-kernel-$arch.tar.gz" -C build/local-kernel
export KERNEL_DIR="$PWD/build/local-kernel"
```

The executables are siblings, with the three generated files in `allowlist/` and a `kernel.json` build record
alongside them. Use `-DCOWORKERD_TEST_WORKERD="$KERNEL_DIR/workerd"` to test the local build.

### Publishing a changed kernel series

The author of a kernel change builds, profiles, and tests the Linux x86_64 binaries, packages those same binaries,
and publishes the archive and `SHA256SUMS` as a [GitHub pre-release](https://github.com/coworkerd/coworkerd/releases).
Verify the public download, post the release link and archive SHA-256 on the PR, and record the upstream, full
series SHA-256, and verified pin in `patches/workerd/artifacts.json` on the same PR. Use a null pin for arm64
until that architecture is built, tested, and pinned before a product release.

Follow the local build instructions above or dispatch
[Build Linux kernel archives](https://github.com/coworkerd/coworkerd/actions/workflows/kernel-build.yml) from
`main` to build both architectures. For a PR, including a fork PR, enter its number and full head SHA. After
checking a successful run, dispatch
[Publish Linux kernel archives](https://github.com/coworkerd/coworkerd/actions/workflows/kernel-publish.yml)
from `main` with the run ID, attempt number, and source SHA in the build summary.

On both Linux architectures the hosted build runs the factory and threaded Bazel tests and the upstream test files
extended by our patches. The tests reuse the release build's outputs. A test failure stops that architecture job
before packaging. The build summary records the elapsed time of the Bazel test invocation.

Use the series tag printed by `package-kernel.py` for a local publication. When adding an architecture to a
local release, update `SHA256SUMS` to include every published archive. For hosted builds, record the published
`releaseTag` in the pin file. The hosted tag adds `-run-<id>-<attempt>` to the series tag, so earlier pins keep
resolving to their original files.

## CI and local unit tests

The `CI` workflow runs on every pull request and on pushes to `main`, on GitHub-hosted Ubuntu 24.04 x86_64
and macOS 26 arm64 runners. Its checks are **`Build and unit tests (linux-x86_64)`** and
**`Build and unit tests (macos-arm64)`** (`build-and-unit-tests` in [.github/workflows/ci.yml](.github/workflows/ci.yml)).
Both runners build the daemons with `scripts/build.sh` and run the `check` target. The Ubuntu runner also builds the
Linux launcher. Build dependencies come from Ubuntu or the macOS runner image, with the pinned Cap'n Proto source
from public GitHub and CLI dependencies from the public npm registry (`cli/package-lock.json`). CTest fails on an
empty suite or any failed test. CI verifies that the JUnit report contains executed tests with no failures or skips.

To run the same checks locally on Ubuntu 24.04 x86-64 or arm64, install a current Node.js 22 release and run the
commands below from the repository root. Clang 19 uses the GCC 14 standard library headers for C++23 `<print>`
support. Set `KERNEL_DIR` to your unpacked kernel directory if you [built it from source](#building-the-kernel-from-source).

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  clang-19 libstdc++-14-dev cmake ninja-build libssl-dev git curl ca-certificates python3
npm ci --prefix cli --registry=https://registry.npmjs.org
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=4 WRANGLER_SEND_METRICS=false
kernel="${KERNEL_DIR:-$(python3 scripts/fetch-kernel.py)}"
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD="$kernel/workerd" -DCOWORKERD_TEST_JUNIT="$PWD/build/test-results.xml"
cmake --build build --target check
```

On macOS, install the Xcode Command Line Tools, CMake, Ninja, OpenSSL 3 and a current Node.js 22 release.
From the repository root, with OpenSSL installed through Homebrew:

```bash
npm ci --prefix cli --registry=https://registry.npmjs.org
export CMAKE_BUILD_PARALLEL_LEVEL=4 WRANGLER_SEND_METRICS=false
export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD= -DCOWORKERD_TEST_JUNIT="$PWD/build/test-results.xml"
cmake --build build --target check
```

`scripts/build.sh` forwards its arguments to CMake. The tests include native node/configuration, durable log and
object store components, the four `cli-*` suites and `node-error-contract`; their individual names and results
appear in CTest's output and `build/test-results.xml`.

Linux CI caches the archive by its pinned SHA-256 and verifies it before registering these tests:

| CTest name | Required artifact |
| --- | --- |
| `node-log-redaction` | Patched `workerd`, for real Worker diagnostics |
| `workerd-configuration` | Patched `workerd`, for the single-process configuration contract |
| `node-child-memory` (Linux only) | Patched `workerd` and its sibling `coworkerd-threaded`, for actual child processes |
| `node-threaded-durable-storage-lifecycle` (Linux only) | Both kernel binaries, for same-object writes/reads, empty storage after actual child and node restarts, and unchanged configuration |
| `node-threaded-durable-process-memory` (Linux only) | Both kernel binaries, for prompt process-budget refusal and recovery after buffer and heap allocations, preserving stored values and child |
| `node-threaded-durable-reset-progress` (Linux only) | Both kernel binaries, for application-abort and CPU-reset recovery under HTTP retries within two seconds, with storage, neighbor and child preserved |
| `node-threaded-durable-cpu-events` (Linux only) | Both kernel binaries, for actual per-event CPU attribution during interleaved HTTP awaits and cumulative CPU enforcement on the original event |
| `node-threaded-durable-launch` (Linux only) | Both kernel binaries, for durable test-role negotiation and storage refusal |
| `node-threaded-durable-home-reservation` (Linux only) | Both kernel binaries, for home reservation before acknowledgement, refusal with all homes occupied, and credit return on cancellation |
| `node-threaded-durable-warm-copies` (Linux only) | Both kernel binaries, for reusing matching warm copies after idle retirement in reverse definition order while preserving stored state |
| `node-threaded-durable-load-cancel` (Linux only) | Both kernel binaries, for a cold load whose only caller's lease runs out: the load ends, and a different definition placed on that home runs its own code |
| `node-threaded-durable-http` (Linux only) | Both kernel binaries, for HTTP writes/reads, brief idle re-entry, and first requests after three idle retirement/wake cycles |
| `node-threaded-durable-rpc-events` (Linux only) | Both kernel binaries, for an independent RPC held by the actor input gate, nested calls to and from a peer during an await, and admission credit return |
| `node-threaded-durable-rpc-output-gate` (Linux only) | Both kernel binaries, for an RPC result held while a disposable SQLite write awaits confirmation and a separate read of the saved value |
| `node-threaded-durable-rpc-cpu` (Linux only) | Both kernel binaries, for interleaved RPC methods with independent CPU accounts, a cumulative limit across awaits, one tree reset and an intact neighbor |
| `node-threaded-durable-http-isolation` (Linux only) | Both kernel binaries, for HTTP input gates, interleaving during an await, caller cancellation with home lifecycle tracing, facet storage/lifecycle, and JavaScript/SQLite event CPU resets with a surviving neighboring actor |
| `node-threaded-durable-http-output-gate` (Linux only) | Both kernel binaries, for an HTTP response held behind delayed test-storage confirmation and the saved value after release |
| `node-threaded-durable-http-overlap` (Linux only) | Both kernel binaries, for 16 overlapping HTTP bodies on four facets, continued interleaving, exact event-credit return and resource samples |
| `node-threaded-durable-group-refusal` (Linux only) | Both kernel binaries, for one injected empty V8-group factory result, returned reservations, successful retry and the original child remaining ready |
| `node-threaded-durable-http-memory` (Linux only) | Both kernel binaries, for idle-first memory eviction, closed placement to another copy, and a hard isolate reset with the original child ready |
| `node-threaded-durable-placement` (Linux only) | Both kernel binaries, for busy-home cold placement and pinned existing actors under HTTP traffic |
| `node-threaded-durable-queues` (Linux only) | Both kernel binaries, for per-object mailbox count/byte bounds and returned event and descriptor credits |
| `node-threaded-durable-fd-limit` (Linux only) | Both kernel binaries, for bounded refusal before exhausting the child's file descriptors |
| `node-threaded-durable-memory-reservation` (Linux only) | Both kernel binaries, for bounded refusal at the admission memory reservation ceiling |

The disposable threaded fixture, its modes, environment variables and measurement commands are documented
in the header of [`node/threaded-durable-launch.test.mjs`](node/threaded-durable-launch.test.mjs).

At configure time `COWORKERD_TEST_WORKERD=auto` first looks for the verified installation on Linux, then for
`workerd/bazel-bin/src/workerd/server/workerd`. For the Linux child test, use a directory with both binaries
as siblings. An explicitly configured missing binary is an error. An empty `-DCOWORKERD_TEST_WORKERD=` selects
the kernel-independent suite. Use `-DCOWORKERD_TEST_WORKERD=auto` to restore automatic detection.

`native-error-contract` checks results from a separate native KJ test. It needs the patched workerd source tree,
the test log and a passing receipt with source hashes from that run. Configure `COWORKERD_NATIVE_SOURCE_DIR`,
`COWORKERD_NATIVE_TEST_LOG` and `COWORKERD_NATIVE_TEST_RECEIPT` to register it locally. The release archive
contains the two executables and their allowlist, so the hosted job does not run this verifier.

`main` requires **`Build and unit tests (linux-x86_64)`** from GitHub Actions before a pull request can be merged.

## Trying a change on a local cluster

On Linux with the build dependencies above and Docker installed, fetch the pinned kernel and build the daemons.
For a kernel you built from source, use the `KERNEL_DIR` set in the source-build instructions.

```bash
KERNEL_DIR="${KERNEL_DIR:-$(python3 scripts/fetch-kernel.py)}"
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD="$KERNEL_DIR/workerd"
mkdir -p build/cluster/{bin,runtime,docker}
cp build/coworkerd build/coworkerd-apiserver build/coworkerd-launcher build/cluster/bin/
cp "$KERNEL_DIR/workerd" "$KERNEL_DIR/coworkerd-threaded" "$KERNEL_DIR"/allowlist/coworkerd-syscalls-*.txt build/cluster/bin/
cp runtime/runtime.js build/cluster/runtime/
cp docker/Dockerfile docker/entrypoint.sh build/cluster/docker/
export COWORKERD_VERSION=dev COWORKERD_PROJECT=coworkerd-dev
docker build --build-arg COWORKERD_VERSION="$COWORKERD_VERSION" -t "coworkerd:$COWORKERD_VERSION" \
  -f build/cluster/docker/Dockerfile build/cluster
(cd docker && docker compose --profile cluster up -d --wait)
```

To follow the [quickstart](docs/guide/quickstart.md) from section 2 with the checkout's CLI, run
`npm ci --prefix cli --registry=https://registry.npmjs.org`, set `export COWORKERD_PACKAGE="$PWD"`, and define
`cowctl() { node "$COWORKERD_PACKAGE/cli/bin/cowctl.mjs" "$@"; }` in your shell. Deploy `examples/hello-world`,
call it through each node and read its logs. Run `(cd docker && docker compose --profile cluster down -v)`
to stop the cluster and remove its volumes. Without `--profile cluster` one node starts.

To try the release-package layout, use `scripts/release.sh <version>` and run Compose from
`build/release/<version>/coworkerd-<version>-linux-<arch>/docker`. On macOS this uses the Linux toolchain
container and requires a published archive for its Linux architecture. [docs/guide/install.md](docs/guide/install.md)
covers the configuration, the sandbox (sections 3 and 4: the default compose file runs workerd without it, a host
prepared as section 4 describes runs it with `docker-compose.sandbox.yml`) and operations.

The [cluster integration suite](tests/cluster/README.md) starts its own three-node cluster and object store. Use its
runner to check a change on Linux, with all cases or a single case. Linux pull-request CI runs the same command.

## Sending a change

- `main` is protected: a change lands through a pull request that passes the repository's checks (listed above)
  and that a maintainer has approved
  ([.github/CODEOWNERS](.github/CODEOWNERS) lists the maintainers).
- A larger change starts as an issue that says what should change and when it is done; a small fix can go straight
  to a pull request. A pull request names the issue it closes.
- An issue's "Done when" list is its finish line. What you find beyond it becomes another issue, not more of the same
  pull request; a question the issue leaves open is asked in a comment on the issue.
- One change per pull request, with a description of what changes and why. Commit messages say what changed.
- Keep behavior changes and refactoring apart, and say in the description how you checked the change (for example
  the quickstart on a local cluster).
- Issues, pull requests, commit messages, code and comments are in English. Comments and documents say what holds
  for any reader on any machine: no names of your own hosts or paths, no references to notes kept outside this
  repository.
- After a review, add commits to the same branch instead of force-pushing it. A maintainer merges the pull request.
- A change to workerd is a commit on the fork's branch and the same change exported to `patches/workerd/` (`python3
  scripts/workerd-patches.py export`, which takes one commit per patch named in `patches/workerd/series`, so a new
  patch is named there first; it works on the checkout `workerd/`, or the one `--checkout` names, which must be clean
  and hold `UPSTREAM`'s tag at its commit) in this repository; patches stay in `src/workerd/server/` (with the two documented exceptions in
  [patches/README.md](patches/README.md)), with the logic in our own files and only hooks in upstream files.
- The `/v1` API changes only by adding fields and endpoints until 1.0; changing or removing something in it needs a
  discussion first.
- Never commit credentials, keys or tokens, including test values that look real.
- Security issues are not reported in public issues: see [SECURITY.md](SECURITY.md).

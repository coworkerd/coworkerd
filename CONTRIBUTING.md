# Contributing

## Building

- **The daemons for the host** (macOS or Linux, with CMake 3.24 or newer, Ninja, OpenSSL, Git and a C++23 compiler):
  `bash scripts/build.sh` builds `build/coworkerd` and `build/coworkerd-apiserver`. It fetches Cap'n Proto from GitHub
  and applies [patches/kj-http-abort.diff](patches/kj-http-abort.diff) to it. The launcher, `coworkerd-launcher`, is
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
The generated allowlist must name the exact `workerd` being packaged.
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

An author who changes the kernel patch series or its upstream builds and tests that kernel locally. A maintainer
checks out the PR head, builds and packages the release kernels on Linux x86_64 and arm64, and publishes the
archives and `SHA256SUMS` on [this repository's Releases page](https://github.com/coworkerd/coworkerd/releases). Use the
`kernel-<upstream tag>-<first 12 hex digits of the patch series SHA-256>` tag printed by the packaging script and
mark the release as a pre-release. Run the packaging command once per architecture. In the final release directory,
put the first archive in place before packaging or copying the second, then check that `SHA256SUMS` covers every
published archive. Generate that file with `sha256sum coworkerd-kernel-linux-*.tar.gz > SHA256SUMS` when combining
archives packaged on separate machines.

The publisher is responsible for building both binaries from the checked workerd source. The packaging script
checks the source tree against the patch series and records that tree as `workerdTree` in `kernel.json`.
`coworkerdCommit` records the commit in this repository where the packaging command ran, which may be the PR head.
If the allowlist uses profiles from another build of the series, record that in the release notes.

The maintainer posts the public release link and archive SHA-256 values on the PR. The author verifies the public
downloads and records the upstream, full series SHA-256 and verified archive pins in
`patches/workerd/artifacts.json` on the same PR. The initial arm64 archive is tracked in
[#25](https://github.com/coworkerd/coworkerd/issues/25).

## CI and local unit tests

The `CI` workflow runs on every pull request and on pushes to `main`, on GitHub-hosted Ubuntu 24.04 x86_64
and macOS 26 arm64 runners. Its checks are **`Build and unit tests (linux-x86_64)`** and
**`Build and unit tests (macos-arm64)`** (`build-and-unit-tests` in [.github/workflows/ci.yml](.github/workflows/ci.yml)).
Both runners build the daemons with `scripts/build.sh` and run the `check` target. The Ubuntu runner also builds the
Linux launcher. Build dependencies come from Ubuntu or the macOS runner image, with the pinned Cap'n Proto source
from public GitHub and CLI dependencies from the public npm registry (`cli/package-lock.json`). CTest fails on an
empty suite or any failed test. CI verifies that the JUnit report contains executed tests with no failures or skips.

To run the same checks locally on Ubuntu 24.04 x86_64, install Node.js 22 (a current 22.x release), then run from
the repository root. Clang 19 uses the GCC 14 standard library headers for C++23 `<print>` support.

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  clang-19 libstdc++-14-dev cmake ninja-build libssl-dev git curl ca-certificates python3
npm ci --prefix cli --registry=https://registry.npmjs.org
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=4 WRANGLER_SEND_METRICS=false
kernel=$(python3 scripts/fetch-kernel.py)
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
| `node-threaded-durable-launch` (Linux only) | Both kernel binaries, for durable test-role negotiation and storage refusal |
| `node-threaded-durable-events` (Linux only) | Both kernel binaries, for HTTP, RPC, actor and facet lifecycle, and event CPU limits |
| `node-threaded-durable-placement` (Linux only) | Both kernel binaries, for busy-home cold placement and pinned existing actors under HTTP traffic |
| `node-threaded-durable-queues` (Linux only) | Both kernel binaries, for per-object mailbox count/byte bounds and returned event and descriptor credits |
| `node-threaded-durable-fd-limit` (Linux only) | Both kernel binaries, for bounded refusal before exhausting the child's file descriptors |

The positional single-node fixture opts in with `durableObjects.threadedTestStorage: true`.
The node creates a fresh temporary stack for the run and refuses object-store configuration, peers, and
an existing Durable Object storage directory in the fixture stack. Use this setting only with disposable
fixture deployments. Each threaded durable child starts without a persistent log channel.
The fixture derives local namespaces from the active declarations and routes them only within the node.
Each child generation opens a fresh SQLite volume inside the fixture stack.
The fixture can vary the home and mailbox limits with `COWORKERD_DO_TEST_HOME_COUNT`,
`COWORKERD_DO_TEST_TREES_PER_COPY`, `COWORKERD_DO_TEST_MAILBOX_EVENTS` and
`COWORKERD_DO_TEST_MAILBOX_BYTES`. `COWORKERD_DO_TEST_PLACEMENT_BYTES` and
`COWORKERD_DO_TEST_BUSY_MS` control placement, and `COWORKERD_DO_TEST_FD_HEADROOM` sets the
descriptor headroom before accounting for the home loops. These settings apply to disposable runs.

On Linux, run `node node/threaded-durable-launch.test.mjs "$NODE" "$KERNEL_DIR/workerd" runtime/runtime.js measure 8 64`
to fill one durable child with eight copies of 64 real objects each. The tool checks the next object's refusal
and prints baseline and saturated thread count, RSS, descriptors, and admission counters.

The `placement` mode alternates bounded synchronous work on one object with requests to new objects.
It checks quiet-copy reuse, a distinct copy after the home becomes busy, pinned existing actors,
returned event credits, and an unchanged ready child without actor resets or idle retirement.
For example, `node node/threaded-durable-launch.test.mjs "$NODE" "$KERNEL_DIR/workerd" runtime/runtime.js placement 4 64 3000`
uses four homes, 64 roots per copy, and a 3000 ms busy threshold in the four-second window.
The last argument can vary the threshold for measurements; output includes every work and cold-request
duration plus the child's thread count, RSS, descriptors, and admission counters. These request durations
include transport and scheduling, so they are not direct samples of the kernel's synchronous busy clock.
With `placement-control 4 64 4000`, the same workload runs 32 rounds and must keep one copy;
the four-second threshold excludes busy-triggered splitting while leaving capacity and memory limits unchanged.

The `queues 4 64` mode measures admission for 64 objects with four events each, declaring 16 KiB per
event. It checks the four-event count limit separately using one-byte declarations, with another object
still admitted. This is a measurement before request bodies are read, not an actor execution or payload
allocation workload. It records reservations, RSS, threads and descriptors, cancels the queued calls,
and checks that reservations and descriptors return to baseline. The final argument varies the object count.
The `queue-limit 4 64` mode must run with a descriptor limit of 512; the CTest wrapper sets that limit
only for its own process tree. It asserts refusal at the reservation ceiling, continued control access,
256 descriptors of headroom and full credit return. Neither mode executes a Worker event.

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
runner to check a change on Linux, with all six cases or a single case. Linux pull-request CI runs the same command.

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

# Contributing

## Building

- **The daemons for the host** (macOS or Linux, with CMake 3.24 or newer, Ninja, OpenSSL, Git and a C++23 compiler):
  `bash scripts/build.sh` builds `build/coworkerd` and `build/coworkerd-apiserver`. It fetches Cap'n Proto from GitHub
  and applies [patches/kj-http-abort.diff](patches/kj-http-abort.diff) to it. The launcher, `coworkerd-launcher`, is
  Linux only: there `build.sh` builds it too, which takes a C compiler as well.
- **cowctl**: `npm ci --prefix cli` installs its dependencies (Node.js 22.5 or newer: the wrangler and Miniflare it
  pins require Node.js 22, and its adapter uses `path.matchesGlob`); `node cli/bin/cowctl.mjs` runs it.
- **workerd**: the upstream tag recorded in `patches/workerd/UPSTREAM`, checked out into `workerd/` with `python3 scripts/workerd-patches.py apply`; build
  `//src/workerd/server:workerd` and `//src/workerd/threaded:coworkerd-threaded` (the server of the ordinary Worker
  processes; a node expects it beside `workerd`) with Bazel (`--config=release`). [patches/README.md](patches/README.md)
  describes the series and how it is maintained.
- **Linux binaries, a release package and its image**: `scripts/release.sh <version>` builds `coworkerd`,
  `coworkerd-apiserver` and `coworkerd-launcher` in a Linux toolchain container
  ([scripts/toolchain/](scripts/toolchain/) has its image and how to set it up), takes a prebuilt workerd whose SHA is pinned in the script
  for the architecture and its matching system call allowlist, and assembles the package and the image `coworkerd:<version>`;
  the header of the script lists the environment it reads. It builds neither workerd nor its allowlist: it takes only
  the binary pinned by SHA-256 for the architecture, with the allowlist directory generated for it, and
  only the version `cli/package.json` names, and it needs the toolchain container running with a configured CMake
  tree. The script checks the architecture-specific workerd pin and matching syscall profiles, and packages all six guides. A release is built on a machine of each architecture.

## CI and local unit tests

The `CI` workflow runs on every pull request and on pushes to `main`, on a GitHub-hosted Ubuntu 24.04 x86_64
runner. Its check is **`Build and unit tests (linux-x86_64)`** (`build-and-unit-tests` in
[.github/workflows/ci.yml](.github/workflows/ci.yml)). It builds both daemons and the Linux launcher with
`scripts/build.sh`, then builds and runs the `check` target. Dependencies come from Ubuntu, public GitHub
(the pinned Cap'n Proto source) and the public npm registry (`cli/package-lock.json`). No private image or kernel
build is needed. CTest fails on an empty suite or any failed test; CI also verifies the JUnit report has executed
tests and no failures or skips.

To run the same checks locally on Ubuntu 24.04, install Node.js 22 (a current 22.x release), then run from the
repository root:

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends clang-18 cmake ninja-build libssl-dev git
npm ci --prefix cli --registry=https://registry.npmjs.org
export CC=clang-18 CXX=clang++-18 CMAKE_BUILD_PARALLEL_LEVEL=4 WRANGLER_SEND_METRICS=false
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD= -DCOWORKERD_TEST_JUNIT="$PWD/build/test-results.xml"
cmake --build build --target check
```

`scripts/build.sh` forwards its arguments to CMake. The tests include native node/configuration, durable log and
object store components, the four `cli-*` suites and `node-error-contract`; their individual names and results
appear in CTest's output and `build/test-results.xml`.

The following kernel-dependent tests are **not registered in this CI suite**, pending the public kernel artifacts
tracked in [#2](https://github.com/coworkerd/coworkerd/issues/2):

| CTest name | Required artifact |
| --- | --- |
| `node-log-redaction` | Patched `workerd`, for real Worker diagnostics |
| `workerd-configuration` | Patched `workerd`, for the single-process configuration contract |
| `node-child-memory` (Linux only) | Patched `workerd` and its sibling `coworkerd-threaded`, for actual child processes |
| `native-error-contract` | Native KJ contract-test log and receipt from a patched workerd source/build |

To include the first three, pass `-DCOWORKERD_TEST_WORKERD=/absolute/path/to/workerd` to `scripts/build.sh`.
A local `workerd/bazel-bin/src/workerd/server/workerd` is used by default if it exists; an explicitly configured
missing binary remains an error. An empty `-DCOWORKERD_TEST_WORKERD=` selects the kernel-independent suite even
when a local kernel exists. For `native-error-contract`, also configure `COWORKERD_NATIVE_TEST_LOG`,
`COWORKERD_NATIVE_TEST_RECEIPT` and `COWORKERD_NATIVE_SOURCE_DIR` with the matching native run's outputs/source.
Cluster smoke tests are tracked separately in [#1](https://github.com/coworkerd/coworkerd/issues/1).

After the workflow has landed on `main`, a maintainer must require **`Build and unit tests (linux-x86_64)`**
(GitHub Actions) in the ruleset targeting `main`. That repository setting is the remaining maintainer step in #3;
adding the workflow does not configure it.

## Trying a change on a local cluster

Build a package and its image for your machine's architecture with `scripts/release.sh <version>` (on a Mac with Apple
silicon this is `arm64`, built in Docker Desktop's Linux VM). The script leaves the unpacked package beside the
tarball, `build/release/<version>/coworkerd-<version>-linux-<arch>/`; run the cluster from there, as a user would:

```bash
cd build/release/<version>/coworkerd-<version>-linux-<arch>
export PATH="$PWD/bin:$PATH"
(cd docker && docker compose --profile cluster up -d --wait)     # three nodes, the object store, the control plane
```

Then follow [docs/guide/quickstart.md](docs/guide/quickstart.md) from its section 2: create a tenant and a credential,
deploy `examples/hello-world` with `cowctl`, call it through each node, read its logs. `docker compose --profile cluster
down -v` removes everything again. Without `--profile cluster` one node starts; [docs/guide/install.md](docs/guide/install.md)
covers the configuration, the sandbox (sections 3 and 4: the default compose file runs workerd without it, a host
prepared as section 4 describes runs it with `docker-compose.sandbox.yml`) and operations.

The integration tests -- cluster-level regression cases and the tool that runs them -- are not in this repository
yet; [#1](https://github.com/coworkerd/coworkerd/issues/1) tracks adding them. Until then, say in the pull request
how you checked the change on a local cluster.

## Sending a change

- `main` is protected: a change lands through a pull request that passes the repository's checks (listed above)
  and that a maintainer has approved
  ([.github/CODEOWNERS](.github/CODEOWNERS) lists the maintainers).
- A larger change starts as an issue that says what should change and when it is done; a small fix can go straight
  to a pull request. A pull request names the issue it closes.
- One change per pull request, with a description of what changes and why. Commit messages say what changed.
- Keep behavior changes and refactoring apart, and say in the description how you checked the change (for example
  the quickstart on a local cluster).
- A change to workerd is a commit on the fork's branch and the same change exported to `patches/workerd/` (`python3
  scripts/workerd-patches.py export`, which takes one commit per patch named in `patches/workerd/series`, so a new
  patch is named there first; it works on the checkout `workerd/`, or the one `--checkout` names, which must be clean
  and hold `UPSTREAM`'s tag at its commit) in this repository; patches stay in `src/workerd/server/` (with the two documented exceptions in
  [patches/README.md](patches/README.md)), with the logic in our own files and only hooks in upstream files.
- The `/v1` API changes only by adding fields and endpoints until 1.0; changing or removing something in it needs a
  discussion first.
- Never commit credentials, keys or tokens, including test values that look real.
- Security issues are not reported in public issues: see [SECURITY.md](SECURITY.md).

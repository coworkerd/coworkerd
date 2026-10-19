# Contributing

## Building

- **The daemons for the host** (macOS or Linux, with CMake 3.24 or newer, Ninja, OpenSSL and a C++23 compiler):
  `bash scripts/build.sh` builds `build/coworkerd` and `build/coworkerd-apiserver`. It fetches Cap'n Proto and applies
  [patches/kj-http-abort.diff](patches/kj-http-abort.diff) to it. The launcher, `coworkerd-launcher`, is Linux only.
- **cowctl**: `npm ci --prefix cli` installs its dependencies (Node.js 20 or newer); `node cli/bin/cowctl.mjs` runs it.
- **workerd**: the branch `coworkerd-v1.20261012.1` of the fork [coworkerd/workerd](https://github.com/coworkerd/workerd),
  or upstream `v1.20261012.1` checked out into `workerd/` with `python3 scripts/workerd-patches.py apply`; build
  `//src/workerd/server:workerd` with Bazel (`--config=release`). [patches/README.md](patches/README.md) describes the
  series and how it is maintained.
- **Linux binaries, a release package and its image**: `scripts/release.sh <version>` builds `coworkerd`,
  `coworkerd-apiserver` and `coworkerd-launcher` in a Linux toolchain container ([scripts/toolchain/](scripts/toolchain/)
  has its image and how to set it up), takes the workerd build of the series for the architecture and its system call
  allowlist, and assembles the package and the image `coworkerd:<version>`; the header of the script lists the
  environment it reads. A release is built on a machine of each architecture.

## Trying a change on a local cluster

Build a package and its image for your machine's architecture with `scripts/release.sh <version>` (on a Mac with Apple
silicon this is `arm64`, built in Docker Desktop's Linux VM). The script leaves the unpacked package beside the
tarball, `build/release/<version>/coworkerd-<version>-linux-<arch>/`; run the cluster from there, as a user would:

```bash
cd build/release/<version>/coworkerd-<version>-linux-<arch>
export PATH="$PWD/bin:$PATH"
(cd docker && docker compose --profile cluster up -d --wait)     # three nodes, etcd, object storage, the control plane
```

Then follow [docs/guide/quickstart.md](docs/guide/quickstart.md) from its section 2: create a tenant and a credential,
deploy `examples/hello-world` with `cowctl`, call it through each node, read its logs. `docker compose --profile cluster
down -v` removes everything again. Without `--profile cluster` one node starts; [docs/guide/install.md](docs/guide/install.md)
covers the configuration, the sandbox (sections 3 and 4: the default compose file runs workerd without it, a host
prepared as section 4 describes runs it with `docker-compose.sandbox.yml`) and operations.

The integration test suite -- the regression cases accumulated during development and the tools that run them
against clusters -- is kept in a separate repository (`coworkerd/lab`, private); smoke tests in this repository will
come in a later version.

## Sending a change

- One change per pull request, with a description of what changes and why. Commit messages say what changed.
- Keep behavior changes and refactoring apart, and say in the description how you checked the change (for example
  the quickstart on a local cluster).
- A change to workerd is a commit on the fork's branch and the same change exported to `patches/workerd/`
  (`python3 scripts/workerd-patches.py export`) in this repository; patches stay in `src/workerd/server/`, with the
  logic in our own files and only hooks in upstream files.
- The `/v1` API changes only by adding fields and endpoints until 1.0; changing or removing something in it needs a
  discussion first.
- Never commit credentials, keys or tokens, including test values that look real.
- Security issues are not reported in public issues: see [SECURITY.md](SECURITY.md).

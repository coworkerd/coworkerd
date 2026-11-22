# Cluster integration tests

Build the daemons from the checkout, then run the suite from the repository root. The suite uses the shipped
Dockerfile and Compose topology with three nodes, an API server and the pinned SeaweedFS object store. Each run
gets a new Compose project, volumes, credentials and loopback ports. The runner deletes its containers, volumes,
network, image tag and temporary projects when it exits.

## Requirements and commands

Use Linux x86-64 or arm64 with [Docker Engine 28 or later and Docker Compose](../../docs/guide/install.md),
Node.js 22.5 or later, Python 3.9 or later and the daemon build tools. The Docker client must be able to build
images and run containers on the local engine. The first run needs access to the public build and package
repositories to download dependencies and base images.

For a clean Ubuntu 24.04 system with Node.js and Docker already installed:

```sh
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  clang-19 libstdc++-14-dev cmake ninja-build libssl-dev git curl ca-certificates python3
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=4
npm ci --prefix cli --registry=https://registry.npmjs.org
kernel="${KERNEL_DIR:-$(python3 scripts/fetch-kernel.py)}"
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD="$kernel/workerd"
node tests/cluster/run.mjs "$kernel"
```

The commands fetch the architecture's pinned kernel by default. To use a kernel you built, set `KERNEL_DIR` to
the directory containing `workerd`, `coworkerd-threaded` and the two `allowlist/coworkerd-syscalls-*.txt` files.
See [building the kernel from source](../../CONTRIBUTING.md#building-the-kernel-from-source).
For a single case, pass its name after the kernel directory:

```sh
node tests/cluster/run.mjs "$kernel" durable-takeover
node tests/cluster/run.mjs --help
```

The runner reports every case, its elapsed time, and the total with cluster setup and teardown. A failed case,
setup or teardown exits nonzero. Linux pull-request CI uses the same entry point.

## Measured time and resources

A fresh GitHub-hosted Ubuntu 24.04 x86-64 runner with 4 vCPUs, 15.3 GiB RAM, Docker Engine 28.0.4 and Compose
2.38.2 passed all six cases in 149.6 seconds, including image build, cluster startup and teardown. Building the
daemons from a fresh checkout took another 2 minutes 30 seconds. The complete Linux CI job, including package
installation and the unit tests, took about 8 minutes.

The five running containers used about 586 MiB combined in the sample just after cluster startup. The `build/`
tree, including the unpacked kernel and test binaries, occupied 470 MiB. Allow additional disk for the kernel
archive, Docker images and temporary volumes. These are observed values from the
[hosted run](https://github.com/coworkerd/coworkerd/actions/runs/44911045714/job/136866725293#step:14:15).

## Cases

| Case | Product behavior whose regression fails the case |
| --- | --- |
| `deployment` | A new Worker version never reaches one of the three nodes, or an ingress continues to serve the previous version. |
| `sql-atomicity` | A successful SQL batch loses a write, or a write from a failed batch is visible after the duplicate-key error. |
| `cpu-isolation` | A CPU-bound Worker escapes the CPU watchdog, returns an error other than `503 CPU_LIMIT_EXCEEDED`, or makes another Worker in the same process wait for it. |
| `process-replacement` | Killing an ordinary workerd process leaves its slot unavailable, or its replacement cannot serve the next Worker request after readiness returns. |
| `log-quota` | A node keeps too many log lines per second, discards another tenant's lines, keeps an older line over the tenant's byte budget, or returns a shortened line without `truncated: true`. |
| `durable-takeover` | Another live node cannot take ownership after the original owner is killed, or it cannot read the exact write acknowledged before the kill. |

Tests deploy code with `cowctl` and observe the product through ingress HTTP, `/v1` and documented management
endpoints. The fixture disables stateless ingress forwarding so the CPU and process tests exercise the receiving
node's single ordinary slot. Docker delivers the two `SIGKILL` faults. The owner-loss case runs last and waits for
the regular membership expiry and recovery intervals.

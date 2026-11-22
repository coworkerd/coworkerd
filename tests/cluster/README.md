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
setup or teardown exits nonzero. Linux pull-request CI uses the same entry point. Each case that holds
peer acknowledgements starts a fresh Compose project. `durable-peer-ack` uses upstream durable processes.
Cases beginning with `durable-threaded-` enable the experimental backend on the nodes specified by the case.
Set `COWORKERD_CLUSTER_BIN_DIR` to use daemons and a launcher from a different build directory.

### Fatal attribution with the test kernel

In the imported Linux kernel checkout, build the separate shared-cage test executable and run the focused
kernel test. Then pass the test executable to the cluster runner from the product checkout.

```sh
(cd workerd && bazel build --config=release //src/workerd/threaded:coworkerd-threaded-fatal-test)
fatal_test_line=$(awk '/^KJ_TEST\("fatal record identifies/ { print NR; exit }' \
  workerd/src/workerd/threaded/threaded-server-test.c++)
(cd workerd && bazel test --config=release '//src/workerd/threaded:threaded-server-test@' \
  --test_arg="--filter=threaded-server-test.c++:$fatal_test_line")
COWORKERD_CLUSTER_FATAL_TEST_BINARY="$PWD/workerd/bazel-bin/src/workerd/threaded/coworkerd-threaded-fatal-test" \
  node tests/cluster/run.mjs "$kernel" fatal-attribution
```

The fixture mounts a read-only controller directory into one node. The controller selects the process and
Worker before issuing a normal HTTP call. The synthetic event hook exists only in the test executable.

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
| `process-replacement` | Killing an ordinary workerd process marks a tenant without evidence, leaves its slot unavailable, or prevents the replacement from serving the next Worker request. |
| `fatal-attribution` (test kernel) | A confirmed fatal exit fails to route the tenant's next call to quarantine, a peer fails to apply the suspect mark, a repeat exit affects the ordinary process, an unrelated tenant fails to recover, or shared-cage exhaustion marks the last allocator. |
| `log-quota` | A node keeps too many log lines per second, discards another tenant's lines, keeps an older line over the tenant's byte budget, or returns a shortened line without `truncated: true`. |
| `durable-peer-ack` | An HTTP reply or a JS RPC result from a Durable Object escapes while both replication peers hold positive append acknowledgements, or the write fails to return and read back its value after those acknowledgements are released. The run also checks a separate object's read during the hold. |
| `durable-takeover` | After the owner is killed during concurrent writes, another live node must take ownership in a later epoch and recover every write for which the client received a complete success response. Writes without such a response may survive or be lost. The case detects premature acknowledgements that lead to lost writes in the run. It measures neither a recovery deadline nor survival after multiple node losses. |
| `durable-threaded-mixed` | HTTP and RPC replies on a threaded owner wait for upstream peer acknowledgements while another home serves a read. Rotation waits for a gated write, and clean rotation, a cut WAL socket, and rotation of a stopped child must reopen confirmed data. A due alarm survives the threaded node's drain and fires upstream. The restarted threaded node then confirms a write as a peer and recovers it in a later ownership epoch. |
| `durable-threaded-takeover` | The owner-loss workload runs on three threaded nodes, with positive peer acknowledgements held at the moment of owner loss. Every fully acknowledged write must survive takeover. |
| `durable-threaded-fence` | A paused old owner has positive acknowledgements withheld until a live node acquires a higher shard epoch. Releasing the acknowledgements before the confirmation deadline must never return success to that old owner's write. The next owner still reads the earlier confirmed value. |
| `durable-threaded-migration` | An open stream keeps its version pin during a class rename. The renamed class keeps its namespace, object ID and confirmed storage. Class deletion removes the namespace from admission. |

Tests deploy code with `cowctl` and observe the product through ingress HTTP, `/v1` and documented management
endpoints. The fixture disables stateless ingress forwarding so the CPU and process tests exercise the receiving
node's single ordinary slot. Docker delivers the two `SIGKILL` faults. The owner-loss case runs last. It sends
unique write IDs to one Durable Object through a surviving node, with up to 16 concurrent requests. The test
requests the owner kill after the first 16 successes while writes are still pending. It drains the outstanding
requests, reports the number of successes out of all writes sent, and checks the recovered IDs after the regular
membership expiry and recovery intervals.

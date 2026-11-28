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
cmake --build build --target coworkerd-platform-test
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
setup or teardown exits nonzero. Linux pull-request CI runs four shards on separate Ubuntu runners,
using the same daemons and verified kernel from the Linux build. Each shard runs its assigned projects
sequentially. Cases that share a Compose project stay in their original order on the same shard.
The required Linux check passes when the Linux build, unit tests and all four shards pass.

```sh
node tests/cluster/run.mjs --list
node tests/cluster/run.mjs --list --shard 1/4
node tests/cluster/run.mjs "$kernel" --shard 1/4
```

Each case that holds peer acknowledgements starts a fresh Compose project. `durable-peer-ack` uses upstream durable processes.
Cases beginning with `durable-threaded-` enable the experimental backend on the nodes specified by the case.
Set `COWORKERD_CLUSTER_BIN_DIR` to use daemons and a launcher from a different build directory.

### Threaded admission refusal

`durable-threaded-admission-refusal` uses a single resident object slot on one owner and a one-event database
mailbox on another. It checks Worker and object calls through fetch and RPC, plus public HTTP SQL and the D1
binding. The child records the refusals without dispatching the writes. After capacity returns, each caller
retries once and the stored counter or row confirms one write per retry.

`durable-threaded-copy-reservation-refusal` consumes a one-shot marker at the cold-copy reservation step
after mailbox admission. Fetch, RPC, public SQL, and D1 `run()` and `exec()` each confirm that the write
did not start and that one retry applies it once. A Durable Object exception after writing and a SQL
constraint failure retain their ordinary error results.

### Single-node waiting-call baseline

The fixture enables ingress placement and sends fixed batches of 8, 16 and 24 stateless Worker
requests to a private HTTP upstream with a two-second delay. It verifies the actual upstream
overlap and completed replies. The JSONL records RSS, threads and open descriptors per process
before, during, after drain and after recovery.

```sh
node tests/cluster/waiting-call-capacity.mjs "$kernel" build/waiting-call-capacity.jsonl
```

Use a new output path for each run. The fixture removes its disposable cluster and upstream.

### Stateless Worker ingress placement

```sh
node tests/cluster/run.mjs "$kernel" ingress-placement-default
node tests/cluster/run.mjs "$kernel" ingress-placement
```

Each case sends one ordinary HTTP request through each node to the same stateless Worker. The Worker
returns its execution node as `{ nodeId: request.cf.nodeId }` in the response body. The runner prints
the execution node for each ingress. The default fixture serves each call on the receiving node.

To enable placement on all nodes for a new fixture, pass
`configureNode: () => ({ ingressPlacement: true })` to `Cluster`. The enabled smoke case also sets
`placement: { homeProbability: 1 }` so that all three requests go to the current Worker home,
including two requests forwarded to a peer. Later cases can choose their own placement settings
and use the same response field to see where each request ran.

The `isolate-busy` case uses the placement-enabled fixture and reads the serving node's management
state. An outstanding request waiting on a timer leaves a full idle window on its isolate. A second
ordinary request adds hold time to the same isolate's reading.

Run `ingress-placement-saturation` and `ingress-placement-all-busy` through the same runner
to test deterministic readings at the isolate and CPU thresholds. These cases check that an
ordinary request skips busy nodes and executes once on the idle candidate, that a lost peer
open stops placement, and that explicit soft refusals can be revisited. The all-busy case
fills the three root slots with sleeping requests to verify the hard concurrency limit.

The default suite also runs `ingress-placement-live-saturation` with the pinned kernel to
test the production reader without overrides. It warms an isolate on the Worker home,
runs one CPU-bound request there, and checks that a subsequent probe executes on a peer.

The separate [hot Worker placement measurement](../../docs/hot-worker-placement.md) uses ordinary
ingress near twice the measured single-isolate rate and compares another tenant's light-request p99
with adjacent quiet windows. Its driver runs on demand on a quiet test machine.

The same driver measures [one Worker above three-node capacity](../../docs/hot-worker-overload.md).
The `overload` mode calibrates the live cluster before and after the fixed-rate control and
overload windows. It records client replies through drain, per-node root slots, and placement counters.

### Threaded SQL deadline and disconnect

Run `node tests/cluster/run.mjs "$kernel" threaded-sql-deadline` with the matching Linux
kernel and release daemons. The case times HTTP and D1 statements and batches on its threaded owner,
including writes with a connected client. It checks the complete reply on a half-closed HTTP
connection and terminates an active HTTP writer with a TCP reset. Reads already queued behind
the interrupted writes must return the earlier confirmed value, with no additional SQLite opens.

### Long SQL isolation

Run this measurement on a dedicated Linux machine after finishing any builds and other load tests.
Use the verified kernel pin and the matching release daemons.

```sh
node tests/cluster/long-sql-isolation.mjs "$kernel" "$PWD/long-sql-isolation.jsonl"
```

The [manual GitHub Actions workflow](../../.github/workflows/long-sql-isolation-measure.yml) uses a
dedicated Ubuntu runner for the selected architecture. Set `ref` to the branch you want to measure.

```sh
gh workflow run long-sql-isolation-measure.yml --ref "$ref" -f architecture=linux-x86_64
gh workflow run long-sql-isolation-measure.yml --ref "$ref" -f architecture=linux-arm64
```

Each run uploads the JSONL report, measurement log and runner details.

The driver provisions a fresh single-node Compose cluster and uses `cowctl` to deploy the object
probe. On the Compose frontend network it offers 40 requests/s per probe in alternating quiet
and busy windows, three per condition. The same warm-up precedes each 30-second window.
Two probe paths read seeded rows through HTTP SQL on different databases.
One belongs to the tenant running the long statement and one belongs to a second tenant. The
third path calls a Worker Durable Object. Each busy window runs one SQL batch with a single
statement that reaches the wall deadline. A follow-up query checks that the batch rolled back.

The JSONL report includes client CPU and scheduling samples, owner dispatch records, durable thread
CPU samples, process identity, and WAL confirmation counters. The driver checks that each busy
probe completed during the statement and that all four targets ran in the same durable process
generation. It measures latency from the planned send time. Missed offers and errors rank above
successful requests, and extra misses or errors in the busy window fail the comparison. Each busy
window is compared with its adjacent quiet windows against the 5 ms gate in the
[design](../../docs/threaded-durable-processes.md#final-acceptance-gates).

### Threaded SQL default mailbox

Run `node tests/cluster/run.mjs "$kernel" threaded-sql-default-mailbox` on Linux x86-64 or arm64
with the matching kernel and release daemons. The three-node Compose fixture
enables threaded database placement on one owner. It uses the compiled mailbox limits, the default
process in-flight limit, and ordinary request deadlines. The case warms one database, launches 128
concurrent HTTP SQL requests through the control plane, and then launches 128 concurrent requests
to a Worker on a peer node. Its D1 binding routes all 128 calls to the same database owner. Each
request reads one seeded row by primary key. The case starts each burst with idle node admission
counters and requires all replies to contain the row, with the original threaded process generation
and no admission overloads.

A Linux ARM64 run with the Worker on the database owner served 121 of 128 D1 requests.
The other seven hit the node's root admission threshold, which counts both Worker roots
and active database calls on that node. Running the Worker on a peer gives its roots their
own default node admission budget. [Maintainer decision](https://github.com/coworkerd/coworkerd/issues/91#issuecomment-7183434485)

### Threaded SQLite memory limit

Run `node tests/cluster/run.mjs "$kernel" threaded-sql-memory-limit` with the matching Linux
binaries. The threaded owner uses a 1 MiB SQLite heap budget. The case checks repeated queries
below and above the budget, HTTP and D1 rollback, the confirmed `exec()` prefix, and recovery
on the same SQLite connection after sort, value size and reply size errors. It also forces a
WAL checkpoint and checks the child logs for allocator scope and accounting errors.

### Threaded SQL and CLI parity

Run `node tests/cluster/run.mjs "$kernel" threaded-sql-cli-parity` to run `sql-atomicity` with the
release daemon on all three threaded nodes. The case applies a `cowctl d1` migration,
imports a small SQL file, and restores both a SQL export and a published bookmark into new databases. It compares
the schema, migration ledger and confirmed rows, then writes through a D1 binding and reads the result through the
CLI. Owner dispatch records verify that the SQL calls reached the platform database in a threaded process.

### Threaded SQL limits

`threaded-sql-transaction-limit` sets the peer buffer on the threaded owner to allow a 254-frame
SQL transaction including the record envelope and RPC headroom. It
checks peer confirmation at the limit, HTTP 422 one frame over the limit, D1 batch rollback and the
confirmed `exec()` prefix. A follow-up write and read run on the same open database.

`threaded-sql-spill-limit` configures a threaded owner with 4 MiB per database and 5 MiB per
process. With a committed WAL already larger than the database budget it checks ordinary and
SQLite-initiated rollback, HTTP 507, D1 batch rollback and the confirmed `exec()` prefix. It inspects the live sort file under the durable storage directory
with the launcher off, then checks that a second database meets the process limit while the first
sort is open. It verifies both databases stay open and the spill charge returns to zero.

### Database placement across builds

`durable-threaded-database-admission` uses three homes on a threaded owner. SQL fills the homes and
refreshes the first database. A new database displaces the least recently used eligible one while a
third database waits for write confirmation with another query queued behind it. The case reopens the
displaced database and reads its previously confirmed row. It then holds all three homes busy and
checks the capacity refusal and recovery. A test fixture holds the final reservation past the
one-second deadline so the case also verifies that the refusal is counted and the home is reusable.

`durable-threaded-database-admission-mixed` fills the four default homes and runs a serial SQL scan
while tenant objects from six definitions compete for the released homes in the same process.

`durable-threaded-database-placement` uses the release daemon on the threaded owner and the upstream
database backend on the other nodes. It checks HTTP SQL and D1 in both directions, batch rollback, two
same-tenant databases in separate isolate copies of the same threaded process generation before and after
idle release, and acknowledged writes recovered at a later shard epoch after either build's owner is killed.
`durable-threaded-database-release` checks that a public SQL query succeeds through the platform
database in the threaded owner's release daemon. To run the placement test with an actual older node build,
point the runner at a checkout containing its built Linux `build/coworkerd` and its original `runtime/runtime.js`.

```sh
COWORKERD_CLUSTER_PREVIOUS_PRODUCT=/path/to/previous-checkout \
  node tests/cluster/run.mjs "$kernel" durable-threaded-database-placement
```

The runner uses the older daemon and runtime on node 2 and records the SHA-256 of both node binaries.
Set `COWORKERD_CLUSTER_PREVIOUS_KERNEL` to the older build's verified kernel directory when testing
across a change to the node-to-kernel protocol.

To measure idle WebSocket memory on fresh clusters:

```sh
node tests/cluster/websocket-memory.mjs "$kernel" local 3072
node tests/cluster/websocket-memory.mjs "$kernel" forwarded 3072
```

The probe sets the ingress ceiling to the requested sample size and raises the native process's in-flight limit
to accommodate all its invocation channels. It verifies the ingress and execution relay counts. It compares
median Linux `smaps_rollup` RSS with 32 and 3,072 idle connections to the same Worker and reports the delta per
added connection for the daemon and ordinary workerd process on each measured node.

The [database home benchmark](../../docs/database-home-benchmark.md) compares the published Object and native
kernels through stateless Worker bindings. Its driver starts a single-node cluster for each side and records
latency, throughput, CPU time, live database counts and idle memory.

### Database throughput and CPU attribution

For the node main-thread stack profile, bare KJ HTTP comparison and in-flight
call measurement on the same four one-Worker paths, see the
[main-thread report and reproduction commands](../../docs/node-main-thread-profile.md).

The throughput mode of the database home benchmark runs the load generator inside the Compose frontend
network. It uses one node, one ordinary workerd process and one threaded durable child. Each trial starts
with a fresh cluster. Run the trial on an otherwise quiet Linux machine with the build tools above.
The [Linux throughput report](../../docs/database-throughput.md) records the exact product revisions and raw runs.

Build the current Object product and the two products from [the original comparison](../../docs/database-home-benchmark.md).
The native SQLite experiment was built separately in #59. Its daemon and kernel pin come from that checkout.

```sh
# Run from the checkout containing this benchmark. Use absolute paths for the product checkouts.
object_main=$PWD
git fetch origin refs/pull/59/head
git worktree add --detach ../object-84 3cd208d51ebe78d1c19c7e7e7ec7e85cbf29e2dd
git worktree add --detach ../native-84 e88acea9a3630c67388dbccdd994b4d843a953eb

measure() {
  local side=$1 checkout=$2 report=$3 kernel
  (cd "$checkout" && bash scripts/build.sh) || return
  kernel=$(cd "$checkout" && python3 scripts/fetch-kernel.py) || return
  COWORKERD_CLUSTER_BIN_DIR="$checkout/build" node tests/cluster/database-home-benchmark.mjs \
    "$side" "$checkout" "$kernel" "$report" --throughput
}

measure object "$object_main" "$PWD/throughput-object-main.jsonl"
measure object "$(cd ../object-84 && pwd)" "$PWD/throughput-object-84.jsonl"
measure native "$(cd ../native-84 && pwd)" "$PWD/throughput-native-84.jsonl"
```

Finish builds and other CPU or I/O work before starting a timed trial. Use `--throughput-smoke` in place of
`--throughput` for a short run through every cell. Each report needs a new filename. For hosted runs, repeat the
matrix on independent instances and preserve all the JSONL recordings. Record the runner label, CPU model and
vCPU count, memory, and any known competing load.

Each client keeps a connection available and issues one call at a time. A window warms its own route for
1.5 seconds, measures for 10 seconds and includes the final in-flight replies. One database gets 1, 8 or 32
clients through one Worker. Eight databases each have a separate Worker and get 8 or 32 clients in total,
divided equally. The benchmark allows 16 homes and gives each mailbox 64 events and 1 MiB to accommodate
the offered concurrency. It raises the node placement limits to 512 root and 1,024 nested invocations
and the process in-flight limit to 512 to give requests time to retire after their HTTP replies. The
report counts warm-up failures separately and requires all timed requests and all warm-up inserts to
succeed. The one-Worker controls return the same small body either directly or through a no-op Durable
Object. A read selects a seeded primary-key row. An insert changes one row and waits for local WAL confirmation
with zero configured remote peers. The driver verifies the confirmation counters and each affected table.

The `throughput-window` records include successful replies per second, latency percentiles, failures, Linux CPU
ticks for the four processes and their main and busiest threads, and aggregate Linux CPU activity including
background processes. A process or thread at 100% uses one logical CPU. Compare the host's busy cores with its
total logical CPUs, and check I/O wait, steal time, CPU pressure and any cgroup CPU limit when interpreting a
plateau. The diagnostic output and per-window verification counts follow the same report.

Set `COWORKERD_BENCHMARK_THREADED_SQL=1` to measure reads and inserts through the product's
threaded database mapping. The benchmark also supports archived builds that use the placement
test executable. The client uses D1 bindings and checks that each seeded
database has its own threaded home before measuring. To compare kernel builds, run both
`object --throughput` trials in one job with the same product checkout and binaries and
a fresh stack and report for each kernel. Point `COWORKERD_BENCHMARK_KERNEL_CHECKOUT` at
the checkout with the tested kernel's patch series when it differs from the product checkout.

### Threaded SQL process memory

`node tests/cluster/run.mjs "$KERNEL_DIR" threaded-sql-process-memory` exercises the
threaded database mapping with a 112 MiB configured process budget. It checks that three open
databases reserve 3 MiB of SQLite heap. An atomic batch exceeding shared headroom rolls back and
returns retryable HTTP 503 `OVERLOADED`. The same child serves the confirmed row and accepts the
query after idle retirement returns the reservations. The separate `threaded-sql-memory-limit` case
checks HTTP 422 `SQL_MEMORY_LIMIT_EXCEEDED` at the database cap.

### Fatal attribution with the test kernel

In the imported Linux kernel checkout, build the separate shared-cage test executable and run the focused
kernel test. Then pass the test executable to the cluster runner from the product checkout.

```sh
(cd workerd && bazel build --config=release //src/workerd/threaded:coworkerd-threaded-fatal-test \
  '//src/workerd/threaded:threaded-server-test@' && \
  bazel test --config=release //src/workerd/threaded:fatal-attribution-test@ \
    //src/workerd/threaded:fatal-attribution-scope-test@)
for title in 'fatal record identifies' "the isolate's registered V8 fatal" \
    'the ordinary role registers the process-wide'; do
  line=$(grep -n -F "KJ_TEST(\"$title" workerd/src/workerd/threaded/threaded-server-test.c++ | cut -d: -f1)
  workerd/bazel-bin/src/workerd/threaded/threaded-server-test@ --filter="threaded-server-test.c++:$line"
done
COWORKERD_CLUSTER_FATAL_TEST_BINARY="$PWD/workerd/bazel-bin/src/workerd/threaded/coworkerd-threaded-fatal-test" \
  node tests/cluster/run.mjs "$kernel" fatal-attribution
```

The fixture mounts a read-only controller directory into one node. The controller selects the process and
Worker before issuing a normal HTTP call. The synthetic event hook exists only in the test executable. The first confirmed
exit is an API check that V8 reports through the isolate's fatal handler, so the case also fails when another
handler replaces the kernel's; the node logs V8's own location and message for it. The last one fails the same check
while the Worker loads cold in a new quarantine process, after its top level ran, where only the load's own scope
names the Worker.

To measure concurrent recovery on a fresh three-node cluster with default membership and rebuild settings:

```sh
node tests/cluster/recovery.mjs "$kernel" 200 loss
node tests/cluster/recovery.mjs "$kernel" 1000 loss
node tests/cluster/recovery.mjs "$kernel" 200 restart
```

The fixture writes distinct values to objects on 16 shards owned by one node. It kills that node and reads
back every confirmed value. The restart mode starts the same container on its original volume immediately
after the kill. Output includes the time from the kill request to detection or restart readiness, first,
median and last successful reads, and samples of lease and recovery counters. The 200-object loss case
requires all reads within 55 seconds of detection. To time an earlier build without that assertion, set
`COWORKERD_RECOVERY_MEASURE_BASELINE=1` and point `COWORKERD_CLUSTER_BIN_DIR` at its Linux binaries.

### Database first access and node loss

Build the daemons and launcher as above. Run each workload in its own quiet measurement window and
save the output. Each command starts a fresh cluster.

```sh
node tests/cluster/database-first-access.mjs "$kernel" cold | tee cold.log
node tests/cluster/database-first-access.mjs "$kernel" loss 100 | tee loss-100.log
node tests/cluster/database-first-access.mjs "$kernel" loss 500 | tee loss-500.log
node tests/cluster/database-first-access.mjs "$kernel" loss 1000 | tee loss-1000.log
node tests/cluster/database-first-access.mjs "$kernel" loss 2000 | tee loss-2000.log
```

The cold probe sends one `SELECT 1` to each of 200 untouched D1 databases serially, then sends
200 queries at once to a second untouched set. It uses one node and sends SQL to the peer endpoint
used by the control plane. The node reports the rebuild's tail-wait lap for each database and the
probe records the change in waiting-recovery metadata requests during each window.

The loss probe seeds one confirmed row in each database on one owner. Every database has its own
namespace and shard. The probe selects namespaces that default placement assigns to the original node
and then evenly to the two survivors. It sends `SIGKILL`, waits for both survivors to detect the
lost owner, and issues all reads. The 1,000- and 2,000-database workloads start 100 clients every
20 ms. The probe retries at 250 ms after unsuccessful responses. It checks every saved value and
rebuild on its expected successor. The probe records additional rebuilds and the durable process
generation and exit count at each end of the run, along with watchdog details, rebuild loop delay,
and the durable log, shard, and membership clients' object-store counters. Read times start at the kill
request. Percentiles use nearest rank. The loss probe requires zero extra rebuilds and zero durable process
exits. At 2,000 databases it requires the first read within 40 seconds and the last within 100 seconds of
detection. Set `COWORKERD_RECOVERY_MEASURE_BASELINE=1` to collect these observations on an earlier build.

The pre-change measurement used `c5e8524` with a timing-only patch that logs the wait from requesting
an epoch metadata permit until it is granted. Apply the patch in a separate baseline checkout. The
driver sums the two source-epoch waits for each database in the 500-database run. Those waits
can overlap the missing-owner deadline, and databases wait concurrently across the two survivors.

```sh
baseline="$PWD/../coworkerd-db-first-access-baseline"
git worktree add --detach "$baseline" c5e8524
git -C "$baseline" apply "$PWD/tests/cluster/database-epoch-gate-baseline.patch"
(cd "$baseline" && bash scripts/build.sh && cmake --build build --target coworkerd-launcher)
COWORKERD_RECOVERY_MEASURE_BASELINE=1 COWORKERD_CLUSTER_BIN_DIR="$baseline/build" \
  node tests/cluster/database-first-access.mjs "$kernel" loss 500 | tee baseline-loss-500.log
COWORKERD_CLUSTER_BIN_DIR="$baseline/build" \
  node tests/cluster/database-first-access.mjs "$kernel" cold | tee baseline-cold.log
```

The original first-access runs used the same ARM64 Docker Desktop VM (Apple M4 host, 10 CPUs and 7.75 GiB
assigned to the VM), SeaweedFS 4.48, default durable settings, release builds with Clang 19.1.7,
and the pinned [ARM64 kernel archive](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-3aa588b669ff-run-45491272877-1).
The progressive-recovery runs and the `5e8ae88` takeover baseline used the same setup with the
[current pinned archive at that revision](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-f3e7281d2642-run-45616161948-1).
The client ran on the host through Docker's loopback port forwarding. See the per-window VM pressure
and client load samples in the output. The [installation guide](../../docs/guide/install.md#database-recovery-after-a-node-loss)
lists the recovery results.

## Measured time and resources

A fresh GitHub-hosted Ubuntu 24.04 x86-64 runner with 4 vCPUs, 15.3 GiB RAM, Docker Engine 28.0.4 and Compose
2.38.2 passed all six cases in 149.6 seconds, including image build, cluster startup and teardown. Building the
daemons from a fresh checkout took another 2 minutes 30 seconds. The complete Linux CI job, including package
installation and the unit tests, took about 8 minutes.

The five running containers used about 586 MiB combined in the sample just after cluster startup. The `build/`
tree, including the unpacked kernel and test binaries, occupied 470 MiB. Allow additional disk for the kernel
archive, Docker images and temporary volumes. These are observed values from the
[hosted run](https://github.com/coworkerd/coworkerd/actions/runs/44911045714/job/136866725293#step:14:15).

## Failure diagnostics

For a failed case, `build/cluster-diagnostics/<project>/<case>/` keeps the failure report, Compose status
and logs, and each node's workerd logs after teardown. HTTP status checks and JSON parse failures include
the response body in the failure report.

On the hosted kernel archive job, download `cluster-failure-<arch>-<attempt>` from the workflow run to
get those files and the cluster runner's combined output in `cluster.log`.

## Cases

| Case | Product behavior whose regression fails the case |
| --- | --- |
| `deployment` | A new Worker version never reaches one of the three nodes, or an ingress continues to serve the previous version. |
| `websocket-admission` | The fixture starts with a soft descriptor limit of 1,024 and sets one ingress to two places. Two tenants fill it, and a third upgrade gets a retryable `503 WEBSOCKET_NODE_CAPACITY` before dispatch. A clean close, disconnect after upgrade and a declined handshake release places. An abandoned handshake releases its place when the Worker answers. Ordinary HTTP and another ingress keep serving. |
| `sql-atomicity` | A successful SQL batch loses a write, or a write from a failed batch is visible after the duplicate-key error. |
| `cpu-isolation` | A CPU-bound Worker escapes the CPU watchdog, returns an error other than `503 CPU_LIMIT_EXCEEDED`, or makes another Worker in the same process wait for it. |
| `process-replacement` | Killing an ordinary workerd process marks a tenant without evidence, leaves its slot unavailable, or prevents the replacement from serving the next Worker request. |
| `fatal-attribution` (test kernel) | A confirmed fatal exit fails to route the tenant's next call to quarantine, a peer fails to apply the suspect mark, a repeat exit affects the ordinary process, an unrelated tenant fails to recover, shared-cage exhaustion marks the last allocator, an exit through V8's own API check or one during the load stays unattributed, or V8's diagnostic is missing. |
| `log-quota` | A node keeps too many log lines per second, discards another tenant's lines, keeps an older line over the tenant's byte budget, or returns a shortened line without `truncated: true`. |
| `durable-peer-ack` | An HTTP reply or a JS RPC result from a Durable Object escapes while both replication peers hold positive append acknowledgements, or the write fails to return and read back its value after those acknowledgements are released. The run also checks a separate object's read during the hold. |
| `durable-takeover` | After the owner is killed during concurrent writes, another live node must take ownership in a later epoch and recover every write for which the client received a complete success response. Writes without such a response may survive or be lost. The case detects premature acknowledgements that lead to lost writes in the run. It measures neither a recovery deadline nor survival after multiple node losses. |
| `durable-slow-holder` | A TCP relay delays the previous holder's peer replies past the recovery deadline while the holder keeps its lease. The successor reports the waiting object and holder, then reads the confirmed value after the relay releases the replies. |
| `durable-threaded-mixed` | HTTP and RPC replies on a threaded owner wait for upstream peer acknowledgements while another home serves a read. Rotation waits for a gated write, and clean rotation, a cut WAL socket, and rotation of a stopped child must reopen confirmed data. A due alarm survives the threaded node's drain and fires upstream. The restarted threaded node then confirms a write as a peer and recovers it in a later ownership epoch. |
| `durable-threaded-database-alarm` | The upstream request and the native threaded event each clear a real alarm on `SqlDatabase`. After the owner's durable process rotates, the database still reads its saved SQL row, holds no alarm and receives no further dispatches for it. The test executable supplies the setup and observation requests. |
| `durable-threaded-database-accounting` | Public SQL on upstream and threaded owners is attributed to the same database Worker in admitted executions, dispatch logs and native metering. A non-database platform object on the threaded owner uses the same tenant's platform Worker. |
| `durable-threaded-takeover` | The owner-loss workload runs on three threaded nodes, with positive peer acknowledgements held at the moment of owner loss. Every fully acknowledged write must survive takeover. |
| `durable-platform-definition` | A separate node executable installs fixture definitions for two tenants. Ordinary bindings reach the shard owner's build across two builds. Copy tokens show warm reuse within a tenant and isolation across tenants. A platform object runs an alarm, reopens after idle release, and recovers every acknowledged SQL write after owner loss. |
| `durable-threaded-fence` | A paused old owner has positive acknowledgements withheld until a live node acquires a higher shard epoch. Releasing the acknowledgements before the confirmation deadline must never return success to that old owner's write. The next owner still reads the earlier confirmed value. |
| `durable-threaded-alarm-fence` | A nonretrying abort clears its schedule, and a constructor exception consumes retries with backoff. When both peers hold confirmations through shard takeover, the old owner's alarm clear cannot succeed or suppress the recovered alarm. The successor stops dispatching a previously confirmed clear, and replacing an inherited schedule prevents it from firing at the old time. |
| `durable-threaded-migration` | An open stream stays on its original version until the drain expires. A request still waiting for headers receives `DO_CODE_UPDATED` with an uncertain result and its confirmed write occurs once. The old RPC capability and WebSockets close, including a parked facet socket whose startup options are discarded across the definition change. The renamed class keeps its namespace, ID and confirmed storage and handles an alarm due during the drain. Class deletion removes the namespace from admission. |
| `durable-threaded-hibernation` | Two root connections and a nested facet connection park together and answer ping and automatic replies with zero actor sessions. The facet parks again after a root-only residency, then receives its frames in order and reaches the original bindings. An HTTP reopen verifies constructor-visible socket state. The case also checks startup option limits, unforwarded upgrades, facet abort and deletion, and fencing after a shard takeover. |
| `durable-threaded-socket-frame-budget` | With an eight-byte budget, a four-byte message uses all reserved bytes while its parked socket waits for adoption. A second socket receives `1013` and its object receives the close event. When adoption resumes, the first delivery returns the reservation and a subsequent binary message round-trips unchanged. |
| `durable-threaded-retirement` | A controllable home clock drives the ordinary socket deadline and the pending-work cutoff while a hibernatable transport survives with its attachment. The case exercises the HTTP-upgrade requirement, a send queued before the 101 response, a mutation during parking, and timeouts before and after park and adopt transfers. It also tests a reset while parked, a reset while attached, and a slow write before and during a park refusal. |

Tests deploy code with `cowctl` and observe the product through ingress HTTP, `/v1` and documented management
endpoints. The fixture disables stateless ingress forwarding so the CPU and process tests exercise the receiving
node's single ordinary slot. Docker delivers the two `SIGKILL` faults. The owner-loss case runs last. It sends
unique write IDs to one Durable Object through a surviving node, with up to 16 concurrent requests. The test
requests the owner kill after the first 16 successes while writes are still pending. It drains the outstanding
requests, reports the number of successes out of all writes sent, and checks the recovered IDs after the regular
membership expiry and recovery intervals.

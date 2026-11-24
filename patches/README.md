# Patches

Coworkerd runs upstream [workerd](https://github.com/cloudflare/workerd) with a thin series of patches.

| Path | What it is |
| --- | --- |
| `workerd/UPSTREAM` | The upstream tag and commit the series applies to (`v1.20261114.1`, `3d3093c8`) |
| `workerd/series` | The order of the patches |
| `workerd/0001-native-binding.patch` ... | One patch per file; the header of each states its purpose |
| `kj-http-abort.diff` | A change to Cap'n Proto's KJ that `CMakeLists.txt` applies when it builds the daemons: KJ's HTTP entity writers honor `abortWrite()`, so a response that fails midway ends with an error instead of looking complete |

## How the series is maintained

- **The fork.** The patched workerd is the fork [coworkerd/workerd](https://github.com/coworkerd/workerd): one branch
  per upstream tag, `coworkerd-<tag>` (now `coworkerd-v1.20261114.1`), which contains the ordered patch commits plus the v95 follow-up to 0010. Feature branches carry additions until they are merged into the tag branch.
  `patches/workerd/` is exported from that branch so that every change to workerd can be reviewed here; a change to
  the branch and its exported copy go into the same change of this repository.
- **Thin patches.** Changes normally stay in `src/workerd/server/` (`scripts/workerd-patches.py` checks it), with explicit
  exceptions: `0015-rpc-invocation-identity.patch` may also change `src/workerd/api/worker-loader.h`,
  `src/workerd/api/worker-loader.c++`, `src/workerd/io/io-channels.h`, and `src/workerd/io/io-channels.c++`.
  These hooks carry an owned invocation identity from the trusted platform loader to each dynamic entrypoint;
  they do not change request lifetimes or accept identity from tenant headers, props, or logs. The exception is
  bound to that patch and those exact paths. Outside the named path exceptions, patches remain limited to the server directory; IoContext,
  V8 and the general RPC protocol are outside this exception. A second explicit exception,
  `0016-rpc-code-parent-death.patch`, changes `src/workerd/jsg/util.c++` at one conversion hook and
  `src/workerd/jsg/BUILD.bazel` to build our `coworkerd-rpc-code.h` and `coworkerd-rpc-code.c++` helpers there.
  The hook sets a platform error's `.code` from exception detail ID `0xc6f4a09d8b3e2175`, shared with the
  node's `common/rpc-error.h`; it never infers a code from an application message. That patch also watches
  the inherited parent pipe through the existing channel startup I/O provider in our server helpers.
  A third explicit exception, `0043-v8-sandbox-per-isolate.patch`, changes V8 through the fork's
  `patches/v8/0055` and `0057`, the build configuration (`.bazelrc`, `build/deps/v8.MODULE.bazel`),
  four jsg hooks (`jsg.h`, `jsg.c++`, `v8-platform-wrapper.h`, `sandbox.h`, `sandbox.c++`,
  `setup.c++`, `BUILD.bazel`) and the threaded server of 0042; see its section below.
  For the threaded durable test role, `0047-durable-test-events.patch` lets the embedder pass an
  absolute thread CPU deadline to SQLite through `src/workerd/util/sqlite.{h,c++}`. The existing
  constructor continues to use the shipped SQL accounting.
  `0048-durable-event-cpu.patch` adds opt-in api/io/jsg hooks that carry delivered-event CPU
  accounting and lease authority through actor entry and asynchronous callbacks.
  `0081-threaded-durable-storage.patch` connects the threaded server and its Bazel target to the
  inherited WAL channel. Its two SQLite utility changes report a synchronous connection close to the
  embedder only after SQLite confirms that no dependent handles remain.
  Patches 0100, 0101 and 0104 dispatch node-authorized alarm events on threaded homes. Their
  `io/actor-sqlite.{h,c++}` hooks require an exact confirmed alarm time at entry and publish alarm changes
  after WAL confirmation for this host. The storage fixture holds confirmation to test these guarantees.
  Patches 0110–0113 execute D1-style SQL in the factory's native SQLite helper. Patch 0111
  also changes the threaded server and its Bazel target to admit and retire dedicated database homes.
  Patch 0113 reconciles SQLite's automatic rollbacks and bounds native SQL memory, WAL writes and queue time.
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
python3 scripts/workerd-patches.py import     # import the patch files (apply: stage without commits)
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

The current fork has the extra 0010 follow-up adjacent to its original commit, so whole-branch `export`
cannot map its commits to the series. Export each patch commit with `format_commit()` in
`scripts/workerd-patches.py` (the same deterministic `git format-patch` options). For 0010, create a
temporary commit with the first commit's parent and the follow-up's tree, preserving the original
author/date and both messages; export that combined commit. This leaves both accepted commits in
the fork's history. Run `check` against the complete fork tree after exporting.

The export is deterministic (no commit hashes, signatures or statistics): exporting the same commits twice gives the
same files. A new series also needs the compatibility flags the node accepts regenerated
(`python3 scripts/compatibility-flags.py`, into `common/generated/`), new workerd builds for both architectures,
their system call allowlists (`scripts/syscall-profile.mjs`, then `scripts/syscall-allowlist.py`) and the archive
pins in `patches/workerd/artifacts.json`. Follow the [publishing steps](../CONTRIBUTING.md#publishing-a-changed-kernel-series).
`check` is the source side of checking a sync: the complete checkout tree differs
from the tag in `UPSTREAM` by exactly the series, and each patch stays within its permitted paths. All four
operations validate the series before mutating a checkout or exporting files.

## Rust server port (v1.20261114.1)

### 0042: Native threaded execution and lifecycle

`src/workerd/threaded/{BUILD.bazel,threaded-server.c++,protocol.h,protocol.capnp}` adds the
`coworkerd-threaded` executable over the C++ worker and I/O libraries. Its accept
loop hands raw descriptors to a native thread pool; each invocation owns its KJ
loop and RPC connection on one thread. Worker versions share an isolate and enter
it under the library's lock. `WorkerdPlatform` supplies the frozen request clock.
The target links no Rust server crate. The node selects the ordinary and quarantine
backend with `processPool.ordinaryServer`; durable processes use upstream.

The path exception is limited to those four new files. Existing server files expose
the V8 platform library to that package and build the serving filter separately.
In the threaded executable the filter permits only the exact pthread `clone`
flags and answers `clone3` with `ENOSYS`; other syscall rules retain their tables.
The node selects this target with `processPool.ordinaryServer = "threaded"`.

The backend dispatches HTTP and JS RPC through the library's native entrypoints.
Private node channels supply bindings, egress and bounded tenant logs. Thread CPU
watchdogs and per-isolate heap/external-memory limits enforce invocation limits.
Process memory, shared address space, resident versions and per-tenant concurrency
are reserved before execution; inactive versions can be evicted. The lifecycle
shared-memory channel reports active contexts and resource counters for draining
and replacement generations. Only descriptors, cross-thread fulfillers and native
metadata cross invocation threads.

### 0041: Exact ordinary call deadlines

The private ordinary selection carries the lease deadline's original decimal text.
Rust parses it with the standard floating parser before constructing binding props
and tenant call metadata, preserving the node's exact deadline check across JSON.
This avoids one-ULP changes from serde_json's default numeric parser. The only path
is `src/workerd/server/server/root_loader.rs`; node admission rules are unchanged.

The current series targets `v1.20261114.1`, with the runtime root loader added to the
baseline from `coworkerd-v1.20261114.1`.
It includes 0002, 0006, 0007, 0008, 0012, 0016, 0005, 0010, 0011, 0015, 0017,
0001, 0003, 0004, 0018, 0013, 0014, 0020, 0021, 0022, 0023, 0024, 0025, 0026, 0027, 0028, 0029, 0030, 0031 and 0032, in that order.
0014 completes the Durable Object port; 0020 adds the private runtime root loader;
0021 bounds cumulative native SQL execution. Follow-up commits may be folded into their patch when exported.
The deferred patch list is empty.

### 0022: Registry failure cache and plan object index

Failed tenant installation and reloads share a 256-entry LRU with a fixed 30-second
lifetime per entry. Metadata lookups return a cached failure before the runtime
loads and serializes modules. Each plan indexes its own registered objects, so
retirement enumerates that plan's roots. Object registration checks, close fences,
acknowledged writes and ownership remain unchanged. The only Rust path is
`src/workerd/server/server/root_loader.rs`.

### 0023: Private plain Worker entry

A tenant plan with no namespaces uses the existing default bridge entrypoint and
idle reaper. A plain-only root loader rejects object plans and selections, and
ignores its storage path. Each selection constructs fresh native service and
Durable Object capabilities from trusted static declarations and call metadata.
Named tenant entrypoints remain bridge props. Loaded plain plans stay until idle
eviction or a newer active catalog definition supersedes them after activity ends;
there is no count limit, matching WorkerLoader. A loading plan holds a pin.

Load identity and lifecycle snapshots include tenant, Worker, version, plan
revision, load generation and instance. Reload generations increase across plan
reclamation. The exact Rust paths are `root_loader.rs`, `config.rs`, `worker.rs`
and `bridge.rs` below `src/workerd/server/server/`. No IoContext, V8, general RPC,
actor storage or acknowledged-write behavior changes.

### 0024: Direct plain selection and failure backoff

The private `getPlain` method specializes the configured registry channel through
the existing subrequest-channel API and returns a native Fetcher. Its selection
requires a plain plan. Object selection retains its bootstrap path. Both entries
retain the existing default bridge and private capability transfer restrictions.

Installation and reload failures share a per-key backoff of 1, 2, 4, 8, 16 and
30 seconds. A successful compilation, or 60 seconds without another failed
compilation, resets the sequence. Expired errors retain only their bounded LRU
history until that reset, so a retry can increase the next delay. The only Rust
path is `src/workerd/server/server/root_loader.rs`.

### 0025: Plain load accounting fields

Plain startup identity uses the existing accounting field-fragment format, with
escaped Worker/version ids and a load-specific invocation token. Startup CPU and
memory rows remain valid JSON and describe the load rather than its first caller.
The only Rust path is `src/workerd/server/server/root_loader.rs`.

The SQLite CTE backout is part of this upstream tag (`0d01fe9f6850d038d80e6debb54c683a51a9ac65`).
It requires no local patch or extra permitted path; the former 0019 is removed.

### 0002: CPU wrapper and startup validation

The CPU wrapper forwards `abandonAlarm()` to the wrapped Worker interface, preserving its returned
alarm time. Factory initialization validates and caches `COWORKERD_CPU_LIMIT_MS` once per process,
before loading any Worker. An empty value is checked by its numeric length so diagnostics contain
no NUL operand.

Upstream's queued outgoing WebSocket memory accounting (#8306) remains guarded by
`workerd-autogate-websocket-outgoing-queue-memory`. When that gate is enabled, queued payloads and
message records count toward isolate external memory and can reach the existing 0005 memory limit
earlier. The default Coworkerd configuration does not enable this gate.

### 0013: Durable Object WAL shipping

The factory uses 0013's original WAL hook, frame validation, commit boundaries and node
protocol. `ActorStorageSpec` carries the shipped flag, the root's `idFromName` name,
the facet name and its parent id. The root name follows the existing parent chain;
an unnamed root falls back to its storage key. Only durable storage with a directory
can be shipped, and cluster replication with the node's log is a configuration error.
C++ remains the first reader and validator of the inherited log descriptor through
the fallible `factory_durable_log_enabled` bridge.

The exact Rust exceptions are `src/workerd/server/server/actor.rs` and
`src/workerd/server/server/bridge.rs`: Rust owns the root metadata, fills the storage
spec and rejects shipped `cloneFacet()` before any abort or same-name early return.
The namespace handle supplies the shipped root name when deleting unopened facets.
C++ deletes descendants first, sends DELETE, then removes the database files.
After a reset, the existing Rust callback closes descendants, WAL mode is restored,
the shipper sends RESET, and descendant storage deletion sends the child DELETEs.
The existing cluster committer branch remains unchanged.

Bootstrap registers the same Tokio provider used by the parent-liveness channel.
The replicator wraps fd 9 once on first use and is shut down before that provider is
destroyed. Its empty static owner does not use the provider at process exit.
These hooks depend on actor/storage metadata and factory lifetime, not on Worker
loading shape.

### 0014: facet alarms and resident objects

Shipped facets report alarm changes through the existing shipper write chain and
report the stored alarm, including its absence, each time their database opens.
They use the same root name as WAL shipping. Non-shipped facets keep the upstream
alarm rejection; root alarms retain `ActorSqliteHooks` and `AlarmScheduler`.
The original shipper additions preserve alarm and residency messages, refusal
codes, and database size reports while retaining 0013's ownership adaptation.

Rust owns the namespace's resident-root count, LRU pass, and eviction/wake
accounting. A root or any resident descendant counts as one tree. C++ first reads
`COWORKERD_DO_RESIDENT_OBJECTS` (4096 by default; 0 is unlimited), and a successful
root start triggers one pass when over the limit. Facets are evicted before their
root; busy trees and namespaces with `preventEviction` are skipped, with activity
checked again after awaits. Hibernate-capable WebSockets use upstream shutdown
semantics. If every tree is busy, a later root start may trigger another pass.
Changed residency numbers are reported immediately, without a timer or throttle.
The limit is per namespace; Coworkerd deploys one namespace per process.

The exact Rust exceptions are `src/workerd/server/server/actor.rs`, for this
policy and `was_evicted` accounting, and `src/workerd/server/server/bridge.rs`,
for the C++-read cap and Rust-to-C++ residency report. These hooks depend on actor
and storage lifetime, not on the Worker loading shape or node/runtime changes.

### 0003 / 0004: dynamic Worker lifecycle

The C++ factory owns the idle reaper and each compiled worker owns its lifecycle handle.
Creation-record presence selects eligible workers; isolate names remain opaque and unchanged.
The single `worker_idle_ready` bridge receives the existing linked ChannelFactory after a
successful link. Idle and watermark eviction use its existing abort path, and `worker_unlink`
releases it before Rust drops its linked channels. Failed named startups are removed from the
namespace map only if it still holds that stub. These are the three exact Rust exceptions:
`server/server/worker.rs`, `server/server/loader.rs` and `server/server/bridge.rs` below
`src/workerd/`. 0004 has no Rust changes.

The publisher borrows the fd5 mapping established by 0008, preserving the released double-buffer
protocol and JSON fields. Lifecycle rows remain on stderr. 0004 borrows fd6, selects the original,
5000 ms or 1000 ms idle threshold, and evicts in LRU order with the existing destruction pacing.

### 0011 / 0018: GC accounting and idle collection

0011 contains GC callbacks, per-Worker CPU budget accounting and gc-pause rows. Its deferred idle
collector and lifecycle snapshot hunks are split into `0018-idle-collector-lifecycle-gc.patch`.
0018 attaches the collector after ready, holds the Worker through an asynchronous collection,
and detaches it when the compiled worker is destroyed. Activity and event-loop stalls postpone
collection; actor top-ups count activity and exclude actor isolates from idle collection.
`processGcJson()` supplies the original process-level `gc` object in fd5. Per-isolate actor and
collection counters remain in lifecycle events. No new Rust bridge or state is introduced.

### 0001: native external transport

Ported to the Rust server: upstream `rpc_client_start_request` supplies the
`startEvent` metadata. This patch adds only the schema variant, configuration
branch and Capnp transport's `start_request`. Its exact Rust exception is
`src/workerd/server/server/services/mod.rs`.

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

### 0020: private runtime root loader

A trusted process configuration can declare a `coworkerdRootLoader` service with an absolute
`storagePath` and give its runtime a private `coworkerdRootLoader` binding. Its inherited `fetch`
manages plans; `getRoot(selectionJson)` selects an explicit `{namespaceId, objectId, dataEpoch,
plan, dispatchChecked}` target for HTTP or RPC. It uses the existing process-local bootstrap
capability protocol and adds no method to ordinary Fetchers. Node dispatch checks must complete
before obtaining an instance. Tenant configurations contain neither this binding nor native
namespace declarations, including through `ctx.exports`. These capabilities cannot transfer.
The existing user entry path continues to use Host/app until the later integration stage.

The private service accepts `POST /install` with an owned deployment description: Worker and
version ids, compatibility date/flags, ES modules, namespace ids, class names, binding names,
storage unique keys and an explicit creation record. The creation record supplies tenant,
Worker/version ids, load generation, CPU limit in milliseconds and memory limit in MiB.
Missing records are rejected. The owned Cap'n Proto Worker config survives with its compiled
script. Limits continue to read only `WorkerSpec.load_identity`; loader keys and class names
are never parsed for identity.

`RootRegistry` indexes compiled Workers by immutable plan key and load generation, asserting
that an insertion cannot replace another instance. Namespace entries keep their storage while
each object retains its own source plan. The native index and SQLite file use storage id S;
the actor bridge presents public O as the same string used by Host/app, retaining the internal
R name for storage reporting only. A Node-dispatched request or RPC with a different version
or limit plan aborts only that object, then reopens the same storage with the selected class.
Direct WebSocket messages retain the instance's existing plan.

Own-worker facet class credentials retain a weak registry reference, immutable plan and props;
they survive unload without retaining the isolate. Variable/secret values stay in the compiled
environment. DO/D1 wrappers receive platform channels in props; outbound and streaming tails
are platform entrypoints. Event metadata is stamped from the explicit selection and deployment.

The existing ready hook attaches idle, watermark and GC lifecycle machinery. Idle retirement
synchronously detaches the old plan, then evicts its actor trees while retaining containers and
hibernation managers. Incoming requests wait for teardown before reload; stale generation and
instance callbacks cannot remove a replacement. Limit retirement aborts that plan's roots.
The initial generation comes from the trusted caller; reloads temporarily increment it in the
kernel until the later Node record-provider integration.

`GET /state` inspects the private registry, `/invoke?plan=...` forwards an event to a plan's
default entrypoint, and `POST /retire` conditionally retires a specified plan generation.
Install publishes its namespaces only after compilation, exported-class validation and link
succeed. `POST /hibernate` is the private component-test control for the existing actor-tree
eviction implementation; it retains WebSocket managers. No public Node endpoint is added.

The exact Rust paths permitted for this patch are `actor.rs`, `bindings.rs`, `bridge.rs`,
`channels.rs`, `config.rs`, `lib.rs`, `loader.rs`, `root_loader.rs` and `worker.rs` below
`src/workerd/server/server/`. Its C++ and schema changes remain in the server package. The
preceding patch files retain their original bytes.

### Upstream pin history

| Upstream tag | Commit | V8 | Series |
| --- | --- | --- | --- |
| `v1.20261031.1` | `8119fd824c29ba4a877824b01ebb0b5dbefe444c` | `15.7.112.5` | 15 patches after the stage 2 lifecycle port |
| `v1.20261108.1` | `88b5871e8873a833b278f346473eed69a4443855` | `15.8.62.5` | 15 replayed patches, the 0013 and 0014 Durable Object ports, and the 0019 SQLite CTE backout (18 total) |
| `v1.20261114.1` | `3d3093c8f01b9b53f4d36d20a8e4ea0a39787992` | `15.8.62.5` | 19 patches including the private root loader and SQL event budget; SQLite CTE backout upstream |

The November 8 replay keeps the upstream handoff metadata accessors alongside the
Coworkerd invocation-identity setter. The creation-record contract is unchanged;
0013 and 0014 each add only their two exact Rust path exceptions described above. Upstream cluster
features remain inactive.

The November 14 replay keeps promise-owned resolved props in 0015 and the unconditional
exception-to-JavaScript `.code` hook in 0016. The new upstream exception description path is used
for reporting; it does not replace the JavaScript conversion hook.

### 0021: cumulative native SQL execution budget

`util/sqlite.{h,c++}` installs one SQLite progress handler per connection and accounts for each compilation and row-step segment. A per-connection PRNG samples short native segments at 1/16. For a selected segment without a callback, the first 250 microseconds of CPU is multiplied by 16 and any excess counts once. This bounds amplification of an isolated slow segment, while undercounting a sequence of expensive callback-free segments. Segments reaching a progress callback are measured directly. The cached statement's VM counter is reset at the segment boundary so a short run cannot inherit a late callback from earlier runs. CPU clocks are gated by a cheap wall clock in callbacks. Defaults are 200 ms cumulative SQL CPU and 800 ms active SQL wall time.

`api/sql.{h,c++}` obtains the current owned event budget and supplies the native error code. Its abort helper validates that this native budget was exhausted and is registered only under the experimental flag used by the platform runtime; tenant deployments cannot request that flag. `io/limit-enforcer.h` adds only the budget getter with a default implementation; `server/factory/coworkerd-cpu-limit.c++` owns and replaces the budget at the existing `topUpActor()` calls. The SQL-only wrapper for platform objects delegates the previous CPU, memory and reporting behavior, including hibernation limits, without parsing a creation identity. No `io-context`, V8, 0013 confirmation or 0020 root-loader file changes belong to this patch.

SQLite's existing critical-error condition remains: an interrupted write can automatically roll back and break the output gate; an interrupted read need not do so. Exhausted budgets reject compilation and row steps before execution without setting the connection-wide interrupt flag, allowing trusted rollback, release and implicit transaction cleanup while other cursors remain open. The sampling seed uses the existing buffered entropy source. A failed connection close during reset restores the progress callback. The runtime rolls a failed database batch or ingest segment back before converting its failure and ending that database object. A function or blocking operation without progress callbacks can outlast the SQL budget; the process watchdog remains the fallback. See the [database](../docs/guide/databases.md) and [Durable Object](../docs/guide/durable-objects.md) guides for transaction behavior and the process configuration keys.

### 0026: Ordinary entry review corrections

Ordinary supersession requires a strictly newer trusted catalog generation. The private `/invoke`
control rejects ordinary plans; their events require the selection bridge and fresh props.
The install reply reports a cold generation once, so independent callers do not share request-owned
JavaScript preparation promises. Lookup/install retain `info.loaded` and `info.generation`.

`factory/coworkerd-idle-eviction.{h,c++}` polls only a separate queue of superseded instances.
`worker-factory.c++` supplies the existing wait-until TaskSet's emptiness as an additional retirement
condition. Foreground activity, streams and WebSockets keep the existing activity tokens. Ordinary
idle collection remains separate. Only `server/root_loader.rs` is added to this patch's Rust whitelist;
IoContext, V8 and general RPC semantics are unchanged.

The Node uses typed lifecycle labels for tenant residency, ordinary limit attribution, and generation
draining. Legacy isolate-name decisions remain only as comparison counters; durable limit attribution
remains undetermined in this round.


### 0027: Stateless dispatch through the private registry

The private selection carries resolved service versions and object namespace references separately from the immutable ordinary plan. Only declared names and their declared Worker may be resolved. The selected default bridge owns existing idle activity through its channel lifetime, covering the interval before the first request. Ordinary and supersession teardown have separate pending markers.

The factory lifecycle snapshot observes the existing wait-until task set and reports activity without changing IoContext. Ordinary compile attribution uses an owned, validated invocation identity supplied for that compilation, outside the plan and its fingerprint. Lookup/install cold state is observed without consuming it; the first request consumes it. Actor class providers reject namespace-free plans. All new Native changes remain within server factory and Rust server files; IoContext, V8 and general RPC semantics are unchanged.

0028 reads the background observer through the existing mutable KJ callback.

0029 carries only the trial-view flag. Both trial checks use the same ordinary plan and compiled instance; the second checks the DO wrapper in that instance without another load.

0030 gives ordinary compilation failures the same compiler diagnostic envelope as WorkerLoader; configured root-object diagnostics are unchanged.

0031 publishes actor request activity on the next loop turn, after its background drain is queued, and timestamps snapshots so the node can reject a pre-completion observation.

0032 gives ordinary cold preparations bounded Native reservations and validates selection before supersession. The lifecycle publication includes a bounded drain aggregate and an atomic completion trailer, so durable generation checks do not copy the resident array or depend on historical invocation count.

### 0043: the V8 sandbox, one 8 GiB sandbox per isolate group

Off unless the kernel is built with `--config=coworkerd-sandbox` (the fork's `.bazelrc`): the V8
sandbox, one pointer-compression cage per isolate group (so every isolate has its own sandbox),
libc++ hardening, and two parameters from the fork's V8 patch `patches/v8/0055`:
`V8_SANDBOX_SIZE_LOG2=33` (an 8 GiB sandbox: the 4 GiB cage followed by 4 GiB for buffers) and
`V8_SANDBOX_TYPED_ARRAY_ACCESS_MASKS` (no additional trailing guard region; V8's TypedArray access
masks take its place, as V8 does on Android). V8 ships the guard regions and the masks; it does not
ship a fully reserved 8 GiB sandbox (its sizes are 1 TiB, 128 GiB on Android, a partially reserved
16 GiB on iOS). Without memory protection keys an isolate reserves 136 GiB: V8's 64 GiB guard
regions in front of and behind its sandbox (a 32-bit index scaled by 8 plus a bounded-size offset).
The fork's V8 patch `patches/v8/0057` adds `IsolateGroup::TryCreate()`, which returns no group
instead of ending the process when the address space has no room for another sandbox.

The patch is the one exception that changes V8 and the build configuration. Its jsg hooks: the
memory protection key scope (compiled only with `WORKERD_V8_ISOLATE_MEMORY_PROTECTION_KEY`, which
only Cloudflare's V8 supports), the platform wrapper's zero segment, the Blob byte view's sandbox
base (upstream takes it from a sandbox without guard regions), and jsg's default ArrayBuffer
allocator, which holds its group so that a backing store outliving the isolate frees into a live
sandbox. In the server package and the threaded server (0042): the platform reserves the first
4 GiB and 1 MiB (V8 15.8 needs 4 GiB plus the largest object field offset); every Worker's isolate
gets its group first (`tryNewIsolateGroup()`: a refusal, `OVERLOADED`, when none fits), with an
in-sandbox pool allocator reserved before any isolate (V8's default reserves on the first
ArrayBuffer and ends the process when an earlier resizable buffer or Wasm memory left no 1 GiB);
the memory limits allocate in that group and bound `MaxAllocationSize()` by the group's 2 GiB
pool (only in this configuration: the limit itself stays with the allocators' accounting); a
refused group is a retryable refusal that lowers the process's resident limit to the residents it
holds; and
`coworkerd-sandbox` refuses to start a process (exit 78, with a `sandbox-refused` row on its event
channel) or to create a Worker whose sandbox V8 does not report as secure, whose zero segment is
not really held, or which shares the default group, writing one `[coworkerd-sandbox]` row per
check. `coworkerd-threaded --sandbox-facts` prints the process row for the node's startup plan
(`node/v8-sandbox.h`).

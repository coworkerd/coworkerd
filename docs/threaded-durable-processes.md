# Threaded Durable Object processes

Design for [issue #16](https://github.com/coworkerd/coworkerd/issues/16).
Source and baseline revision: `dfc8803731a33feaaf8796313a0053d403f5ba8a`.
The node continues to own placement and durable history. The threaded child hosts object events and native database
operations. Billing and separation by trust level are outside this design.

## Current execution and the proposed boundary

`ProcessPool` in [node/process-pool.c++](../node/process-pool.c++) maps an object key to a durable slot, explicitly
selects threaded execution only for non-durable slots, and gives the old durable child a generation-scoped log FD
and a writable object-data directory. The node routes a DO binding, SQL request or scheduled alarm through
`DurableShards` and `DurableLog::openObject`. The durable dispatch in [node/main.c++](../node/main.c++) checks
ownership, the object image and the selected generation's log channel again immediately before dispatch.
[node/root-objects.c++](../node/root-objects.c++) prepares the root definition. The trusted
[runtime](../runtime/runtime.js) loads it through `ROOTS`, builds the class wrapper and uses the root actor
registry for the actual call. A tenant Worker version shares an isolate across its objects.
`SqlDatabase` in that same runtime implements the SQL side of the D1 binding and the HTTP API using one shared
runtime isolate. Durable processes share the upstream event loop even across different isolates.

The new child reuses threaded process supervision, private ingress and node binding channels from
[patch 0042](../patches/workerd/0042-threaded-server-s1a.patch), the per-isolate V8 groups from
[patch 0043](../patches/workerd/0043-v8-sandbox-per-isolate.patch) and the interrupt and reservation work in
[patch 0044](../patches/workerd/0044-s1e-node-cost-and-long-operations.patch). These are useful building blocks.
The ordinary rule of one program's isolate per process and the selection of a call thread for each invocation
do not give a resident actor a stable home. Introduce a durable role and a separate dispatcher inside the
threaded child. The node chooses the same slot and generation as today. The child chooses a home thread and
isolate for an object when the object first becomes resident. It chooses a database thread when a database
first opens. Events and completion messages go to the selected home. Actor, isolate-bound values, SQLite
connection, cursors and callbacks never travel across threads.

The main thread of the child accepts and demultiplexes work. Home threads run their own event loops and may
host several object trees. Global thread, memory and descriptor reservations bound the pool. A native
database leases an execution thread with no other database or actor work scheduled on it for the duration of
a synchronous statement, and releases it when its connection is retired. Object workers sharing a home
have fair mailboxes and bounded synchronous execution. Each Worker definition may have multiple independent
isolate copies in the child. Each copy belongs to one home thread, and a woken tree is placed with a copy
that has safe capacity, or a bounded new copy on an eligible thread. A tree and its facets stay together.
Choose placement before constructing the actor, and reserve thread, address space, heap and native charges
before publishing it. Preserve the version pin and actor input/output gates, including supported
interleaving across awaits. The mailbox bounds waiting events, rather than turning every event into a
run-to-completion lock.

An isolate-wide V8 interrupt needs a stricter contract for object-local CPU reset than the ordinary path
does. Consume CPU only while that event executes (including its synchronous SQLite work). Once an interrupt
identifies the offending event, unwind its JavaScript and SQLite work, cancel its actor IO, then reset the
actor on its home and clear the termination state before re-entering the isolate. Serialize that sequence
with all isolate entry. Preserve unrelated objects' in-memory state. Use the existing storage gate for
committed writes and preserve their history. Prove reset isolation with two live objects in one isolate,
including a sibling with a suspended event. Treat recoverable resource exhaustion as a bounded refusal before
allocation. An unavoidable isolate-fatal error is a separate process containment path, not the intended
outcome of tenant CPU or memory pressure.

## Node and child protocol

Each durable slot declares its backend and version during private readiness. The node switches only new
generations, with the old leader fully drained before it authorizes a new child to open the same image.
If negotiation fails, the generation never becomes ready. Keep the launcher, generation identity, lifecycle
and parent-death channels, authenticated peer protocol, and node-owned desired state. Extend the private
protocol by role and feature negotiation within a node-child pair. No peer-format or persisted-format bump
is needed.

- **Open and invoke.** The node chooses ownership and verifies the image with the existing checks. It sends
  authenticated private metadata identifying node/slot generation, namespace, object and class, data and shard
  epochs, pinned version and bundle identity, invocation identity and lease, and event kind. Fetch uses a
  per-call HTTP stream as ordinary execution does. RPC and WebSocket upgrades retain their capabilities and
  streams beyond the dispatch acknowledgement. There is no JS object pointer in the request. The child
  validates the generation and target before reserving a bounded mailbox entry. Distinguish refusal before
  entry, acceptance, and cancellation/EOF after acceptance. Only existing explicit pre-execution refusals
  permit peer retry. Preserve streaming, flow control, caller abort, error/status mapping, and lease expiry.
- **Storage.** Keep the generation-scoped FD, WAL framing and confirmation boundary. A home thread publishes
  immutable frames via the child's log-channel owner, which serializes wire writes and routes correlated
  replies to the correct home. No home may wait while holding another home's lock. Keep cancellable byte-
  bounded queues, per-object commit ordering, SQLite checkpoint safety and node-side disk sequencing.
  Correlate opens, closes and completed checkpoints with generation and object-open identity. A lost
  acknowledgement, generation change or fenced shard invalidates the appropriate open. Never release a
  write's reply or RPC result at enqueue time.
- **Close and drain.** First stop admission for the affected open or generation. Flush committed work through
  the existing gate, close facets before their root, then report the close and release the home reservations.
  A generation may be drained or killed on a real fault. EOF invalidates live opens and lets the node run
  existing recovery. For a targeted reconstruction, wait for positively confirmed close or process death
  before swapping files. Stale completions cannot clear a newer registration.
- **Wake.** A user event, alarm or nonautomatic WebSocket event reaches the owner through the same open and
  epoch checks on a cold image as a fresh dispatch. If an object is already resident, post to its home.
  Serialize concurrent open, wake, close and move for one object. Never replay an accepted user event merely
  because transport or ownership failed.

## Durability and ownership contract

[The durable log protocol](cluster-state.md#12-durable-objects) remains authoritative. The child writes
SQLite through the existing WAL and output-gate discipline, while the node controls the raw log, peer
replication, fencing, immutable segments and manifests, snapshots, recovery and disk eviction.
Reuse `DurableLogShipper`'s frame extraction, acknowledgement and checkpoint ordering from
[patch 0013](../patches/workerd/0013-durable-object-log.patch) without using its actor-loop singleton from
multiple threads. Keep precisely which peers were selected, their positive record acknowledgements, the local
flush and empty-selected-peer fallback, and the current confirmation deadline. A successful response keeps
the existing confirmation guarantee, including the existing failure model for volatile peer tails.
A failed response can have an uncertain outcome. Confirm, trim, drop, replay, repair and exclusion still
operate on the same records. Keep sequence and epoch checks at the node and preserve the peer fence floor,
recoverability of old manifests and tails, and current quarantine of failed or uncertain writes.

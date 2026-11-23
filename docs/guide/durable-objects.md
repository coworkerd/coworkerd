# Durable Objects

> For release 0.5.6. A Durable Object is a named instance of a class with SQLite storage. Its shard has one owner in the cluster; a call through any ingress reaches that owner. Start with the [durable-counter example](../../examples/durable-counter), or follow the [quickstart](quickstart.md).

## 1. Write a class

Export a class extending `DurableObject` from `cloudflare:workers`. Its constructor receives `ctx` and `env`. Store persistent state in `ctx.storage`; ordinary instance fields disappear when the object is evicted, its version changes or its process restarts.

```js
import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counter (n INTEGER)");
    ctx.storage.sql.exec("INSERT INTO counter SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM counter)");
  }
  value() {
    return this.ctx.storage.sql.exec("SELECT n FROM counter").one().n;
  }
  async fetch(request) {
    if (request.method === "POST") this.ctx.storage.sql.exec("UPDATE counter SET n = n + 1");
    return Response.json({ count: this.value() });
  }
}

export default {
  fetch(request, env) {
    return env.COUNTERS.getByName("shared").fetch(request);
  },
};
```

`ctx.storage.sql` exposes workerd's SQLite API. Key-value methods such as `get`, `put`, `delete` and `list`, and storage transactions, use the same database and durability path. The object's env contains the version's variables, secrets, Durable Object bindings and D1 bindings; Service Bindings are not installed in it. Facets have separate databases but belong to the root object's storage, eviction and recovery tree; `ctx.facets.clone()` is refused.

Native SQL has a separate process-wide limit: by default, an object's event may spend 200 ms of cumulative SQL CPU, with an 800 ms active SQL wall-time fallback. Requests, alarms, delivered WebSocket messages, calls into RPC targets the object handed out, and messages on outbound WebSockets it accepted receive new budgets at the existing event boundaries. Uninterrupted synchronous execution does not replace the budget. Statements share the budget across timers, subrequests and storage awaits. Each native compilation or row step is sampled randomly with probability 1/16. A sampled segment without a SQLite progress callback contributes its first 250 microseconds of CPU multiplied by 16, plus any excess at its actual value. Segments invoking the progress callback are measured directly. Configure `processPool.sqlCpuLimitMs` and `processPool.sqlWallLimitMs` in the [node configuration](install.md).

A statement exceeding the budget throws `SQL_LIMIT_EXCEEDED`; later SQL in that event is refused. Transaction behavior follows SQLite:

| Interrupted statement | `transactionSync` outcome | Object outcome |
| --- | --- | --- |
| A write that has changed pages | SQLite rolls back the transaction; catching the error cannot commit it | The existing storage failure path resets the object |
| A read whose error leaves the callback | `transactionSync` rolls back | The tenant object remains available |
| A read whose error is caught inside the callback | Earlier completed statements may commit if the callback returns normally; later SQL in the event is refused | The tenant object remains available; the next event has a fresh budget |

An uncaught read error normally returns 500; a reset after an interrupted write returns 503. User error text or a user-set `code` does not create the platform's database error code. A smaller tenant CPU allowance does not take effect inside a running native SQL statement. With a 50 ms tenant CPU allowance and the default 200 ms SQL budget, a long SQL test returned `503 CPU_LIMIT_EXCEEDED` after about 220 ms, once native execution returned. Interruption is cooperative: a costly function or blocking I/O without progress callbacks can run past the SQL limit. A sequence of callback-free segments each taking more than 250 microseconds is undercounted by the sampled estimate, increasingly so as the segments become more expensive. The process watchdog, normally 3000 ms, remains its fallback.

After a declared secret or binding changes, the object's next request selects the updated definition. On the experimental threaded host, this uses the same bounded drain as a code update. Other objects reopen on their own next requests. Stateless Workers keep their loaded values until their isolates are evicted.

## 2. Declare the namespace

Add to the Worker project's Wrangler configuration:

```jsonc
{
  "durable_objects": {
    "bindings": [{ "name": "COUNTERS", "class_name": "Counter" }]
  },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter"] }]
}
```

`cowctl deploy` builds the project and the migrations declare the class. Each class gets a random namespace id recorded in the tenant manifest. `new_classes` also declares a SQLite class; `renamed_classes` preserves its namespace id, `deleted_classes` marks it deleting, and migration tags are applied in order. A binding may name only a class of this Worker (`script_name`, if given, must name itself); `transferred_classes` is refused. An active object request uses the Worker's currently active version, so redeployment changes the class code on the same namespace and data. Keep schema changes compatible with that data.

On the experimental threaded host, a running request keeps its code version during the drain. The first new-definition request refused with `503 DO_VERSION_DRAINING` starts the `durableObjects.versionDrainTimeoutMs` clock, which defaults to 30 seconds. If old calls are still active when it expires, the node resets that tree and the new definition opens on the same confirmed storage. A call that is still waiting for response headers can retry after `503 DO_CODE_UPDATED` ("Durable Object reset because its code was updated"). A response body already streaming ends early. A class rename preserves object IDs and the existing databases. After the node applies a class deletion, it refuses old-generation opens for that namespace.

```bash
cowctl do list
cowctl do inspect Counter
```

The first lists this Worker's namespaces; the second reports its shard owners and approximate object and byte totals. Deleting a namespace through `/v1` or a migration makes it unreachable; retained deleting namespaces still count against the namespace quota. 0.5 does not reclaim their stored history or allow their names to be reused.

## 3. IDs, fetch and methods

```js
const id = env.COUNTERS.idFromName("shared");
const stub = env.COUNTERS.get(id);
const response = await stub.fetch("https://counter/", { method: "POST" });
const count = await stub.value();
const again = env.COUNTERS.get(env.COUNTERS.idFromString(id.toString()));
```

`getByName(name)` is shorthand for `get(idFromName(name))`. Named IDs are SHA-256 of the namespace's 32 bytes, the text `name`, and the supplied name. `newUniqueId()` uses 32 random bytes. IDs stringify as 64 lowercase hexadecimal digits; `idFromString` checks that shape, without proving an ID came from this namespace. `get` rejects an ID object made by another namespace; the namespace always separates the underlying storage. `locationHint` and the options passed to `get` / `getByName` are ignored. `jurisdiction()` and `newUniqueId({jurisdiction})` are refused.

A stub exposes `fetch` and the class's public methods. workerd serializes RPC arguments and results in the isolates; the node validates the trusted envelope and proxies the capability, also across nodes. Returned capabilities last only for the tree's lease. See [compatibility](../compatibility.md), section 7, for RPC types, the 32 MiB serialized limit and exception shapes.

A stateless Worker's call to an object consumes its call tree's subrequest budget. An object's call to another object starts a fresh tree and is not charged to the caller's budget; a chain longer than eight object hops is refused with `508 DO_HOP_LIMIT`. These calls follow ownership, independently of `placement.subcalls`, which controls stateless Service Binding placement.

## 4. WebSockets and hibernation

Accept a connection through the object's context to let it hibernate:

```js
async fetch(request) {
  const pair = new WebSocketPair();
  this.ctx.acceptWebSocket(pair[1], ["room"]);
  return new Response(null, { status: 101, webSocket: pair[0] });
}
webSocketMessage(ws, message) { ws.send(message); }
webSocketClose(ws, code, reason, wasClean) { ws.close(code, reason); }
webSocketError(ws, error) { console.error(String(error)); }
```

Use `ctx.getWebSockets(tag?)`, `ctx.getTags(ws)` and `ctx.setWebSocketAutoResponse()` to manage accepted connections. An idle object's eviction leaves hibernatable connections open; the next delivered message constructs the object again with those connections and the same storage. Reconstruct in-memory state from storage in the constructor. Calling `ws.accept()` instead uses the ordinary WebSocket API and does not opt into this hibernation path.

The upstream host keeps connections in its durable process. Process rotation, node drain and node exit end those connections. Incoming messages are at most 32 MiB and the ingress default is 1000 open connections per tenant per node (`429 WEBSOCKET_LIMIT` beyond it). Compression is disabled. CPU is charged as message handlers execute.

On the experimental threaded host, an eligible tree retires after about ten seconds from its last event or the end of its last blocker. An otherwise idle tree with a standard `ws.accept()` connection has a deadline between 70 and 140 seconds. An I/O or wait-until operation can protect the tree for up to 15 minutes from that operation's start. The home checks for queued and live requests before retiring a tree and closes its idle standard sockets on retirement.

When a threaded tree retires, its node keeps hibernatable WebSockets, including tags and serialized attachments. The node answers WebSocket pings and matching `ctx.setWebSocketAutoResponse()` messages while the object sleeps. An application message or a close requiring a handler opens the object on its current shard owner. The node restores parked sockets before the constructor runs. Parked connections belong to that node and shard epoch. Node drain and loss of shard ownership close them with `1001` when the transport is still reachable, and the client must reconnect after a node exit.

## 5. Alarms

```js
async schedule() { await this.ctx.storage.setAlarm(Date.now() + 1000); }
async alarm({ retryCount, isRetry }) {
  // Make the work safe to repeat after a failure.
  console.log("alarm", retryCount, isRetry);
}
```

`setAlarm`, `getAlarm` and `deleteAlarm` store the time in the object's database, replicated and recovered with it. The shard owner schedules due alarms and invokes `alarm(alarmInfo)` as an event. A successful handler's unchanged alarm is deleted; setting another time preserves that new alarm. A throwing handler is retried with backoff; a class without a handler has its alarm removed.

On the experimental threaded host, each object runs one alarm handler at a time. A failed handler gets up to six retries on the firing owner, with delays of about 2, 4, 8, 16, 32 and 64 seconds. Capacity and recovery deferrals leave the retry count unchanged. When the handler opts out of retries or reaches the final failed retry, the owner clears that schedule through the same confirmation path as other storage writes. A new owner starts its own retry count, so handlers must tolerate repeated delivery across takeover. Threaded alarms share ordinary request admission without a reserved share per tenant. Many slow alarms from one tenant can occupy the node. Recovery preparation remains bounded. The `alarmFireConcurrency` setting applies to the upstream host.

After an owner node stops, the surviving nodes with durable processes find its shards that have pending alarms, and each such shard's next owner takes it without waiting for a request: at each membership change and every `durableObjects.alarmDiscoveryIntervalMs` (60 s), at most `durableObjects.alarmTakeoverConcurrency` (4) takeovers at a time on a node. An object is opened only when its alarm is due. A shard without pending alarms gets its new owner when a request reaches it. This needs the node storage permissions on `alarm-wake/by-owner/` (see the [installation guide](install.md)); without them alarms fire after the shard's next request.

Tenants take turns, and eligible alarms within a tenant run in due-time order. An unavailable durable process or a full node closes that destination's gate. One alarm probes it every 200 ms, and a cancelled probe releases the gate for another probe.

On the upstream host, `durableObjects.alarmFireConcurrency` defaults to two background alarms per node. Each completed alarm makes room for the next. With two slots, another alarm can progress while one waits on an external request. A tenant can occupy both slots with slow handlers. Higher concurrency puts more background work ahead of user requests on the upstream event loop.

A cold alarm queues its rebuild and releases its firing slot. Completion wakes the schedule, and a failed rebuild retains the one-second platform retry delay. Cold dispatches admit at most 256 alarms preparing or waiting for rebuild. A hot dispatch can also discover that its object needs to be rebuilt. Additional cold alarms wait for preparation capacity in the scheduler. This queue rotates between tenants independently of the hot ready queue and follows due-time order within a tenant. With rebuild width `B >= 3`, background alarms have a target of two rebuild slots and foreground requests have a target of `B - 2`. Either class borrows idle capacity. At `B = 1` and `B = 2`, the two queues alternate. A user request joining an object still queued for background rebuild promotes it to the foreground queue.

Unknown namespace metadata and temporarily inactive namespaces retain their alarms. Local catalog rechecks start after 500 ms and back off to 60 seconds, with at most eight such namespaces checked per 500 ms batch. Once a namespace is enabled, its retained alarms resume in batches of 64 on successive scheduler turns, without waiting for that recheck timer or the remaining unknown namespaces. Catalog visibility and the bounded queue drain determine the enable-to-fire delay. Only a known deletion removes the schedule. Discovery resumes its saved LIST position when candidate capacity becomes available; an expired storage continuation token is retried from the directory's beginning on a later patrol. A node also checks markers left under its own live generation, at most 16 checks per minute. Retained markers use increasing retry intervals; one failed check does not interrupt publication or missing-marker repair.

Loss of a machine has an additional per-object recovery cost: each cold object's first rebuild waits up to membership TTL plus liveness grace, and holds a rebuild slot while waiting. With the defaults this is 25 seconds and four slots per surviving node. A capacity estimate for `S` cold objects and `N` survivors is `ceil(S / (N * 4)) * 25 seconds`, plus detection, discovery, storage and handler work. For two survivors, the recovery portion alone is about 325 seconds for 100 objects and 3,125 seconds for 1,000. These are capacity estimates, not measured end-to-end latency or delivery guarantees.

With `rebuildConcurrency` set to 1 and many objects left by a dead node, a user's first request can wait through the default 30-second `recoverTimeoutMs` and receive `503 DO_RECOVERING`; retry it. Promotion to the foreground queue still leaves that request behind earlier foreground requests, and the two classes alternate use of the single slot.

Measured recovery of 24 small objects whose alarms were already due, with two survivors, four rebuild slots per node, the default 15-second TTL and 10-second grace, and concurrent user traffic:

| Discovery path | Kill-to-fire median | Kill-to-fire p99 | Earlier design example |
| --- | ---: | ---: | ---: |
| Membership change | 75.19 s | 100.53 s | about 43 s |
| Next 60-second patrol | 104.90 s | 130.28 s | about 98 s |

Each node had a 1 CPU cgroup quota and 1,536 MiB memory, with one durable process. Eight clients each made ten hot-object requests per second, and two more continuously read other objects. Every measured alarm fired once and user requests had no failures. The member path took about 24.30 seconds to detect the loss; the patrol path added about 31.10 seconds between detection and its first metadata read. A rebuild occupied a slot for about 25.2 seconds, and the median object waited another 25.4 seconds in the rebuild queue. This per-object wait explains the additional time over the earlier examples. The measured drain rate was about 0.318 objects/second, close to the capacity estimate of `8 / 25 = 0.32`. Adding measured discovery overhead to the formula estimates about 350 / 379 seconds for 100 objects and 3,150 / 3,179 seconds for 1,000, for the member / patrol paths respectively. Those larger counts are projections.

In a separate live-owner test with 2,010 alarms, the same per-node CPU and memory quotas, eight hot clients and two continuously requesting stateless clients, concurrency 2 drained the alarms in 14.73 seconds; concurrency 1 took 17.15 seconds. Hot-request p99 was 57.5 / 79.1 ms in each run's preceding static window and 165.5 / 133.9 ms during drain, respectively. Node 3 was already CPU-quota saturated before firing began, so these measurements include quota contention. Evaluate latency on the deployment's own hardware with fixed-rate traffic and CPU headroom before increasing the setting.

Object-store hints identify which objects the next shard owner should wake, but the database decides whether an alarm is due. A stale hint can cause a harmless wake; a missing hint delays discovery until the object is opened again. Treat delivery as retryable, with no exactly-once side-effect guarantee.

An acknowledged alarm whose hint or owner marker has not published before its node stops can miss background discovery. Alarms set before the hint/marker implementation join background discovery when their shard is next opened. A large backlog, cold rebuilds, unavailable storage, or insufficient resources can delay firing further. Database fencing does not make external side effects exactly once; handlers must make those effects safe to retry.

## 6. Limits and quotas

| Limit | Default and scope |
| --- | --- |
| CPU | 200 ms per fetch, RPC method, alarm or delivered WebSocket message; Worker / tenant limits can override it |
| Memory | 128 MiB per version isolate in each durable process, shared by its objects and classes |
| Resident objects | A target of 4096 object trees per durable process; idle trees leave in LRU order, facets before their root, with hibernation. Active trees are protected and may exceed the target. `0` removes the target |
| Object bytes | 10 GiB (10,737,418,240 bytes), configurable with `durableObjects.objectBytes`; root and facet database sizes are measured at checkpoints / open / close, not exact pending transaction sizes |
| Tenant namespaces | 16 by default (`durableObjectNamespaces`), counting DO class namespaces, D1 databases and deleting namespaces |
| Tenant bytes | 10 GiB by default (`durableObjectBytes`), using lagging per-node summaries; deleting namespaces / databases stop counting |
| Logs | 256 KiB per event before truncation; the tenant's line rate, line size and buffer limits also apply |

An ordinary class exception leaves the instance available; fetch returns `500 EXECUTION_FAILED` with its message. CPU or memory termination invalidates the version's isolate in that durable process: all its objects there are constructed again on their next event, recovering storage subject to the limitations in section 7. A durable slot does not move a suspect tenant into quarantine.

A write past the object or tenant cap is refused at confirmation with `413 DO_OBJECT_TOO_LARGE` or `422 QUOTA_EXCEEDED`; reads and transactions that do not grow the database remain possible, including deleting rows. The size estimate can lag. A refusal immediately marks the object for repair; later requests wait for one consistent recovered history. The SQL statement may already have executed locally. A failed response alone does not prove rollback: records without a persistent exclusion remain possible writes. See [error codes](errors.md).

## 7. When ownership changes

A class namespace has 1024 shards; the first eight ID bytes, interpreted big-endian, modulo the shard count select the shard. A live owner keeps it; it is not moved merely to balance load. Any eligible node may acquire the next immutable ownership epoch after release or confirmed expiry. A node with `durableObjects.processes: 0` forwards calls but owns no shards. A store without atomic create-if-absent serves no objects.

Before a write's response is released, reachable peers acknowledge its committed frames in memory; with an empty selected peer list the owner calls a local disk flush. If selected peers exist but all are unreachable, confirmation waits and can time out instead. A failed local flush is logged and returns `STORAGE_UNAVAILABLE`; it does not confirm the write. Confirmed frames enter uploaded segments and per-object manifests, with snapshots to bound replay. The new owner restores the snapshot, replays segments and asks surviving peers for the remaining tail. The request waits, with a default 30 s recovery timeout (`503 DO_RECOVERING`). The class is constructed again; volatile fields and connections on failed processes are gone.

An owner confirms a write only while its own membership lease holds, judged just before the answer by the node's own clock, so an owner whose process was stopped or whose event loop was stalled past its lease does not confirm on resuming -- even when a peer's acknowledgement is already there -- although another node may meanwhile have taken the shard over: that write returns `503 STORAGE_UNAVAILABLE` (retry it) and the node gives up its shards at once. While the store shows a previous owner's heartbeat still live -- or that heartbeat cannot be read, carries no time, or names another node or epoch -- a takeover does not cut its history short: the object stays in `503 DO_RECOVERING` until that owner answers or its heartbeat is seen old enough, with no upper bound. Both rely on every node using the same membership TTL, interval and grace ([installation guide](install.md)). An owner whose clock stood still -- its virtual machine was frozen -- is stopped by its peers instead: once the new owner has asked them for the tail, they refuse that owner's later writes, which fail with `503 STORAGE_UNAVAILABLE`; the owner then gives the shard up, and requests go to the new owner. This holds only for the peers the new owner asked, when every node runs a version that keeps that floor, and a peer forgets it when it restarts ([cluster state](../cluster-state.md), section 12). A write that a peer refused because its buffer was full is no longer counted as replicated: it is sent again for a while and otherwise fails the same way.

Simultaneous loss of the owner and all confirming peers can lose the tail not yet uploaded. The normal segment interval is 30 s, but an upload outage can make that tail longer. Every daemon restart discards local database, WAL, SHM, facet-index and staging files; each object is rebuilt from stored history and surviving logs at its first request. While the object store is unreachable, objects of a restarted node answer `503`. A durable-process failure invalidates the objects it still had open. Raw log prefixes and durable exclusions are retained across power loss; losing the only disk before upload still loses its tail.

A failed confirmation returns `STORAGE_UNAVAILABLE`. The node synchronizes its repair/exclusion before replacing files, publishing exclusions or writing on a repaired image. Peers receive drops, including on reopened sessions; manifests carry the exclusions before the repaired object becomes LOCAL. Another node can still adopt an uncertain write before it receives that exclusion, but later history must remain consistent with that choice. A log append, short-write or flush failure freezes that epoch's append/confirmation: every later DO write
on this node is refused, even after disk space is freed. Restart the node daemon to begin a new node epoch
before retrying writes. A sidecar failure similarly freezes DO service until restart. If an open marker cannot be written, even a first read is unavailable. [Installation](install.md#upgrading-to-056) describes the required new stateful installation and stateless package replacement; [cluster state](../cluster-state.md), sections 11–12, explains the fences and recovery gates. Use application operation IDs or inspect state before retrying uncertain increments.

The remaining WAL, recovery and resource limitations in
[Known issues in 0.5.6](install.md#known-issues-in-056) apply to DO and D1 data. There is no in-place stateful
upgrade: use new data directories and a new bucket, and move data at the application level.

## 8. Checked differences from Cloudflare

Coworkerd runs the Worker's own SQLite classes, with different ID derivation, no jurisdiction or cross-Worker classes, and no facet clone. `locationHint` is ignored. Class env has no Service Bindings. A limit resets the shared version isolate's objects in that durable process, rather than only the offending object. Fetch exceptions return the platform's HTTP error shape; method exceptions use workerd's RPC exception transport. DO-to-DO calls start independent trees bounded by eight hops. Durability, takeover and alarm discovery have the limits above, including the loss of an unuploaded tail when no surviving copy remains (section 7). The [compatibility table](../compatibility.md), section 7, records the verified differences; [databases](databases.md) describes D1's supported subset.

Only root objects can set alarms; calling `setAlarm()` in a child facet throws `Facets currently cannot set alarms.`

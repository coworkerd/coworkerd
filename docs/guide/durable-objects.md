# Durable Objects

> For release 0.5.0. A Durable Object is a named instance of a class with SQLite storage. Its shard has one owner in the cluster; a call through any ingress reaches that owner. Start with the [durable-counter example](../../examples/durable-counter), or follow the [quickstart](quickstart.md).

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

A process or node exit closes its connections: hibernation does not preserve a relay on a failed node. Draining and rotation close with `1001`, so clients reconnect. Incoming messages are at most 32 MiB; the ingress default is 1000 open connections per tenant per node (`429 WEBSOCKET_LIMIT` beyond it). There is no idle timeout or compression. A connection counts once in request usage; CPU is added as its messages execute.

## 5. Alarms

```js
async schedule() { await this.ctx.storage.setAlarm(Date.now() + 1000); }
async alarm({ retryCount, isRetry }) {
  // Make the work safe to repeat after a failure.
  console.log("alarm", retryCount, isRetry);
}
```

`setAlarm`, `getAlarm` and `deleteAlarm` store the time in the object's database, replicated and recovered with it. The shard owner schedules due alarms and invokes `alarm(alarmInfo)` as an event. A successful handler's unchanged alarm is deleted; setting another time preserves that new alarm. A throwing handler is retried with backoff; a class without a handler has its alarm removed.

Object-store hints identify which objects the next shard owner should wake, but the database decides whether an alarm is due. A stale hint can cause a harmless wake; a missing hint delays discovery until the object is opened again. Treat delivery as retryable, with no exactly-once side-effect guarantee.

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

A write past the object or tenant cap is refused at confirmation with `413 DO_OBJECT_TOO_LARGE` or `422 QUOTA_EXCEEDED`; reads remain possible. The size estimate can lag and a refused transaction can remain visible locally until reopen repairs it from confirmed history. No rollback should be inferred from these errors. See [error codes](errors.md).

## 7. When ownership changes

A class namespace has 1024 shards; the first eight ID bytes, interpreted big-endian, modulo the shard count select the shard. A live owner keeps it; it is not moved merely to balance load. Any eligible node may acquire the next immutable ownership epoch after release or confirmed expiry. A node with `durableObjects.processes: 0` forwards calls but owns no shards. A store without atomic create-if-absent serves no objects.

Before a write's response is released, reachable peers acknowledge its committed frames in memory; with an empty selected peer list the owner calls a local disk flush. If selected peers exist but all are unreachable, confirmation waits and can time out instead. The flush return value is currently unchecked, so a successful answer does not establish that the flush succeeded. Confirmed frames enter uploaded segments and per-object manifests, with snapshots to bound replay. The new owner restores the snapshot, replays segments and asks surviving peers for the remaining tail. The request waits, with a default 30 s recovery timeout (`503 DO_RECOVERING`). The class is constructed again; volatile fields and connections on failed processes are gone.

Simultaneous loss of the owner and all confirming peers can lose the tail not yet uploaded. The normal segment interval is 30 s, but an upload outage can make that tail longer. 0.5.0 has a known recovery defect: a counter acknowledged as 1 became 0 after a single-node abrupt restart on the same disk, with both the Linux container and the native Mac runtime. Do not rely on that restart preserving acknowledged writes. Losing the only disk before upload also loses its tail. A failed confirmation returns `STORAGE_UNAVAILABLE`; the node isolates its records and requests peer removal, but peer-drop delivery and later manifest publication can fail, so the result remains uncertain. Do not retry an uncertain increment without an application operation ID or checking state. [Cluster state](../cluster-state.md), sections 11–12, explains fences, disk reuse and the accepted failure semantics.

## 8. Checked differences from Cloudflare

Coworkerd runs the Worker's own SQLite classes, with different ID derivation, no jurisdiction or cross-Worker classes, and no facet clone. `locationHint` is ignored. Class env has no Service Bindings. A limit resets the shared version isolate's objects in that durable process, rather than only the offending object. Fetch exceptions return the platform's HTTP error shape; method exceptions use workerd's RPC exception transport. DO-to-DO calls start independent trees bounded by eight hops. Durability, takeover and alarm discovery have the limits above, including the reproduced loss after abrupt same-disk restart in section 7. The [compatibility table](../compatibility.md), section 7, records the verified differences; [databases](databases.md) describes D1's supported subset.

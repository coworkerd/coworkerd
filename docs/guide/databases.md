# Databases (D1's shape)

> For release 0.5.0. A database is a Durable Object: it gets the same replication, the same rebuild
> after its owner changes and the same limits ([compatibility scope](compat.md), [durable objects](durable-objects.md)).
> The API is D1's, so an application written for D1 runs unchanged within the subset below.

## 1. What a database is

One database is **one Durable Object namespace with one shard and one object**, and its class is the platform's own SQL
Worker. It therefore behaves like any other object of this cluster: the node that owns its shard runs it, every
transaction uses the object confirmation path before its answer goes out and is later uploaded as segments, and a node that
takes the shard over rebuilds it from that history. One database holds at most 10 GiB by default (10,737,418,240 bytes), and counts as
one of the tenant's 16 namespaces (`durableObjectNamespaces`); what all of a tenant's objects hold together is bounded
by `durableObjectBytes` (10 GiB by default).

## 2. Make one

From a directory with `cowctl.json` initialized for your tenant (see the [quickstart](quickstart.md)):

```bash
cowctl d1 create books
```

The command waits up to 15 s for non-draining nodes to report the new revision (warning rather than failing when they remain behind), then polls a `SELECT 1` query. A successful probe establishes that one serving path works; it does not test every node. `--no-wait` skips both waits. The control plane's own
endpoints are `POST`, `GET` and `DELETE /v1/tenants/{t}/databases/{d}` and `GET /v1/tenants/{t}/databases`.

## 3. Bind it to a Worker

In the wrangler configuration, `database_name` is the name above (a `database_id` is accepted and ignored: a database is
named within its tenant):

```jsonc
{
  "name": "shop",
  "main": "src/index.js",
  "compatibility_date": "2026-10-01",
  "d1_databases": [{ "binding": "DB", "database_name": "books" }]
}
```

`cowctl deploy` refuses the version if the database does not exist or is being deleted.

## 4. Use it

`env.DB` is D1's API when the binding is installed. **Known 0.5.0 defect:** a Worker with D1 bindings but no declared Durable Object class gets no database bindings in its env. The example below describes the API, but fails in such a D1-only Worker. The [durable-counter example](../../examples/durable-counter) declares a DO class and exercises the binding successfully; HTTP SQL in section 5 also works without a Worker.

```js
export default {
  async fetch(request, env) {
    await env.DB.exec("CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY, title TEXT, pages INTEGER)");
    await env.DB.prepare("INSERT INTO books (title, pages) VALUES (?, ?)").bind("Dune", 412).run();
    const one = await env.DB.prepare("SELECT title FROM books WHERE pages > ?").bind(400).first();
    const all = await env.DB.prepare("SELECT id, title FROM books ORDER BY id").all();
    const rows = await env.DB.prepare("SELECT title, pages FROM books").raw({ columnNames: true });
    const batch = await env.DB.batch([
      env.DB.prepare("INSERT INTO books (title, pages) VALUES ('Emma', 474)"),
      env.DB.prepare("SELECT count(*) AS n FROM books"),
    ]);
    return Response.json({ one, all, rows, batch });
  },
};
```

The runtime provides this client over the platform SQL Worker. What is there: `prepare()`, `bind()`, `run()`, `all()`, `first()` and `first("column")`, `raw({columnNames})`, `batch()`
and `exec()`, with D1's `meta` (`changes`, `last_row_id`, `rows_read`, `rows_written`, `changed_db`, `size_after`) and
D1's errors ([error codes](errors.md), section 4). What is not: a migration tool, import and export, restore to a point
in time, read replicas, and the Sessions API -- `withSession()`, `dump()` and `query()` are refused.

`batch()` is one request to the database, so its statements run one after another on the same object; it is not a transaction. The client checks only the first result for throwing; inspect later results for `success: false` even when the promise resolves. `exec()` splits its argument by lines and runs each as a statement, as D1 does, and reports the first failure after the request has executed its statements; later statements may already have run (`D1_EXEC_ERROR` names the line).

## 5. Without a Worker

The same protocol is an HTTP endpoint, with the tenant's own credential:

```bash
cowctl d1 list
cowctl d1 execute books --sql "SELECT title FROM books WHERE pages > ?" --param 400 --json
```

```bash
curl -s -X POST "$CONTROL_PLANE/v1/tenants/acme/databases/books/query" \
  -H "Authorization: Bearer $COWCTL_TOKEN" -H 'content-type: application/json' \
  -d '{"sql":"SELECT title FROM books WHERE pages > ?","params":[400]}'
```

Set `CONTROL_PLANE` to your API URL and `COWCTL_TOKEN` to your tenant credential. CLI `--param` values are strings (the SQL engine applies its conversions); the HTTP `params` array can also carry numbers, null and byte arrays. The public endpoint is `/query`; `/execute` is not routed. It accepts an object or an array of statement objects and always returns row objects (`ARRAY_OF_OBJECTS`); its `resultsFormat` query parameter has no effect. The internal SQL Worker also supports `ROWS_AND_COLUMNS`, which the JavaScript client uses for `raw()`. The answer is D1's: `{"success":true,"results":[...],"meta":{...}}`, or `{"success":false,"error":"..."}` for a
statement that failed. An array of `{"sql","params"}` runs several statements and answers an array.

## 6. Delete one

```bash
cowctl d1 delete books --yes
```

The database is marked deleting: no Binding to it works and no new statement reaches it, and it stops counting against
the tenant's storage. Its data stays in the object store (there is no data reclamation in 0.5), so the name cannot
be used again in this release.

## 7. Failure and consistency

Each SQL HTTP request is one event of the database object; statements in a batch share that event's limits. The same durability and estimated tenant storage quota apply as to other objects. `batch()` is not an atomic multi-statement transaction, and a failed `exec()` does not undo earlier statements. SQL errors use D1 error prefixes; the JavaScript binding can distinguish platform failures such as `STORAGE_UNAVAILABLE`, `DO_RECOVERING`, `DO_OBJECT_TOO_LARGE` and `QUOTA_EXCEEDED` (see [errors](errors.md)). The public HTTP SQL route only forwards a node answer with status 200; it tries other members after a non-200 answer or transport error and otherwise returns `503 STORAGE_UNAVAILABLE`, losing the original code. This retry can repeat a statement whose result was uncertain, so HTTP SQL does not provide at-most-once writes. A confirmation failure can leave changes readable locally until the object is reopened and repaired: do not blindly repeat a non-idempotent statement. Simultaneous loss of the owner and all confirming peers may lose the unuploaded tail. The same-disk abrupt-restart defect and unchecked local flush described in [Durable Objects](durable-objects.md), section 7, also affect the database storage path. No read replicas or Sessions API provide another consistency mode.

Database names currently need lowercase letters / digits with a letter first to be usable by nodes (for example `books` or `counterdb`). The API and CLI also accept names with hyphens or a leading digit, but the deployment-state decoder incorrectly treats them as class names; creation succeeds and serving fails. Use the narrower form until that defect is fixed.

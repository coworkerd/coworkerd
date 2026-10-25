# Databases (D1's shape)

> For release 0.5.0 (in development). A database is a Durable Object: it gets the same replication, the same rebuild
> after its owner changes and the same limits ([compatibility scope](compat.md), [durable objects](../s3-truth-and-durable-objects.md)).
> The API is D1's, so an application written for D1 runs unchanged within the subset below.

## 1. What a database is

One database is **one Durable Object namespace with one shard and one object**, and its class is the platform's own SQL
Worker. It therefore behaves like any other object of this cluster: the node that owns its shard runs it, every
transaction is held by peer nodes before the answer goes out and reaches the object store as segments, and a node that
takes the shard over rebuilds it from that history. One database holds at most 10 GB, as on Cloudflare, and counts as
one of the tenant's 16 namespaces (`durableObjectNamespaces`); what all of a tenant's objects hold together is bounded
by `durableObjectBytes` (10 GB by default).

## 2. Make one

```bash
cowctl d1 create books
```

The command waits until every node serves the database, as `cowctl deploy` waits for a version. The control plane's own
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

`env.DB` is D1's API:

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

What is there: `prepare()`, `bind()`, `run()`, `all()`, `first()` and `first("column")`, `raw({columnNames})`, `batch()`
and `exec()`, with D1's `meta` (`changes`, `last_row_id`, `rows_read`, `rows_written`, `changed_db`, `size_after`) and
D1's errors ([error codes](errors.md), section 4). What is not: a migration tool, import and export, restore to a point
in time, read replicas, and the Sessions API -- `withSession()`, `dump()` and `query()` are refused.

`batch()` is one request to the database, so its statements run one after another on the same object; it is not a
transaction. `exec()` splits its argument by lines and runs each as a statement, as D1 does, and stops at the first that
fails (`D1_EXEC_ERROR` names the line).

## 5. Without a Worker

The same protocol is an HTTP endpoint, with the tenant's own credential:

```bash
cowctl d1 execute books --sql "SELECT title FROM books ORDER BY id"
```

```bash
curl -s -X POST "$CONTROL_PLANE/v1/tenants/acme/databases/books/query" \
  -H "Authorization: Bearer $COWCTL_TOKEN" -H 'content-type: application/json' \
  -d '{"sql":"SELECT title FROM books WHERE pages > ?","params":[400]}'
```

The answer is D1's: `{"success":true,"results":[...],"meta":{...}}`, or `{"success":false,"error":"..."}` for a
statement that failed. An array of `{"sql","params"}` runs several statements and answers an array.

## 6. Delete one

```bash
cowctl d1 delete books --yes
```

The database is marked deleting: no Binding to it works and no new statement reaches it, and it stops counting against
the tenant's storage. Its data stays in the object store until a later release's cleanup removes it, so the name cannot
be used again in this release.

# Databases (D1's shape)

> For release 0.5.6. A database is a Durable Object: it gets the same replication, the same rebuild
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

`env.DB` is installed for every declared D1 binding, including Workers with no Durable Object classes. The following example needs only the `d1_databases` entry above.

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
D1's errors ([error codes](errors.md), section 4). What is not: import and export, restore to a point
in time, read replicas, and the Sessions API -- `withSession()`, `dump()` and `query()` are refused.

`batch()` executes its statements in order within one transaction. Any statement failure rolls back the whole batch and rejects the promise with `D1_ERROR:`. `exec()` splits its argument by lines and runs each as a statement, stops at the first failure, and keeps earlier successful lines; `D1_EXEC_ERROR` names the failing line. It does not roll back earlier lines.

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

Set `CONTROL_PLANE` to your API URL and `COWCTL_TOKEN` to your tenant credential. CLI `--param` values are strings (the SQL engine applies its conversions); the HTTP `params` array can also carry numbers, null and byte arrays. The public endpoint is `/query`; `/execute` is not routed. It accepts an object or an array of statement objects. `resultsFormat=ARRAY_OF_OBJECTS` (the default) returns row objects; `resultsFormat=ROWS_AND_COLUMNS` returns `{columns,rows}`, also used by the JavaScript client for `raw()`. Other format names are refused. The answer is D1's: `{"success":true,"results":[...],"meta":{...}}`, or `{"success":false,"error":"..."}` for a
statement that failed. An array of `{"sql","params"}` runs one transaction and answers an array; a failing statement rolls the transaction back.

A single object request may contain multiple SQL statements in its `sql` string. SQLite handles statement boundaries,
including trigger bodies, comments and semicolons inside strings. That object request is **not a transaction**: if a later
statement fails, earlier effects remain. Use an array of statement objects for a transaction that rolls back as a whole.

## 6. Versioned migrations

```bash
cowctl d1 migrations create books "create books table"
# Edit migrations/0001_create_books_table.sql, then:
cowctl d1 migrations list books
cowctl d1 migrations apply books --yes
```

Files are top-level `.sql` files. `create` chooses the next leading number, pads it to at least four digits and changes
spaces in the description to underscores; the file starts with a comment ready for SQL. `list` shows pending filenames.
`apply` runs files in numeric order, with a character-order tiebreaker, and records their filenames in `d1_migrations`.
It skips files already recorded; a comment-only or empty file is recorded as a no-op. `--json` returns structured results.
Without `--yes`, an interactive terminal asks for confirmation; non-interactive/CI runs proceed without prompting.

`--dir DIR` and `--table TABLE` take priority over the matching `database_name` entry's `migrations_dir` and
`migrations_table` in Wrangler configuration. Defaults are `migrations` and `d1_migrations`. Configured directories are
relative to the Wrangler file; `--dir` is relative to the current directory. The adapter accepts these settings and
`migrations_pattern` during deployment, but deployment does not use them. Migration commands reject `migrations_pattern`
and subdirectory layouts in this release.

Each file and its ledger INSERT are two items in **one array request and one transaction**. A failed file rolls back
all of that file, including its ledger row; earlier successful files remain applied, and later files are not attempted.
The command names the file and SQLite error and exits nonzero. Fix the file and run `apply` again. SQL is not split:
the CLI appends a separator and a no-op `SELECT 1` so a missing final semicolon or trailing comment leaves a concrete
last statement for the SQL interface. The ledger INSERT remains a separate array item in the same transaction.

Migration files must not contain transaction-control statements (`BEGIN`, `COMMIT`, `SAVEPOINT`); trigger
`BEGIN … END` bodies are supported. The full JSON-encoded request, including escapes and the ledger statement, must be
at most **1 MiB**. An oversized file is rejected locally before its execution request is sent; split it into smaller files.
One file consumes one database-object event. The node's default CPU configuration is 200 ms, but enforcement of that
budget for this internal SQL event is **not verified** by this change; do not treat 200 ms as a verified migration limit.

On a timeout or `503 RESULT_UNCERTAIN`, the CLI does **not resend the file**. It reads the ledger: if the filename is
present it continues, otherwise it stops with instructions to run `apply` again. If the ledger read also fails it stops.
The earlier write might have committed despite a lost reply; a later `apply` checks the ledger before choosing files.

## 7. Delete one

```bash
cowctl d1 delete books --yes
```

The database is marked deleting: no Binding to it works and no new statement reaches it, and it stops counting against
the tenant's storage. Its data stays in the object store (there is no data reclamation in 0.5), so the name cannot
be used again in this release.

## 8. Failure and consistency

Each SQL HTTP request is one event of the database object; statements in a batch share that event's limits. The same durability and estimated tenant storage quota apply as to other objects. A failed `exec()` keeps earlier successful lines; `batch()` rolls back on a statement failure. SQL errors use D1 error prefixes. HTTP SQL preserves the node's status and body, including `success:false` with status 200 and platform errors such as `STORAGE_UNAVAILABLE`, `DO_OBJECT_TOO_LARGE` and `QUOTA_EXCEEDED`. It tries another member only when connection establishment failed or the node returned a refusal known to precede execution ([errors](errors.md), section 4). Losing the reply after connecting returns `503 RESULT_UNCERTAIN` without replaying the request. A confirmation failure can leave changes readable locally until the object is reopened and repaired: do not blindly repeat a non-idempotent statement. Simultaneous loss of the owner and all confirming peers may lose the unuploaded tail. Deletions and other transactions that do not grow the database remain permitted over a byte limit. No read replicas or Sessions API provide another consistency mode.

Database names follow `[a-z0-9][a-z0-9-]{0,62}`: hyphens and a leading digit are supported. A malformed entry in the deployment's database or namespace table is skipped independently; `deployment.invalidNamespaceEntryCount` at the node's state endpoint counts skipped entries.

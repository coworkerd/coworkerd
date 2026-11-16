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
D1's errors ([error codes](errors.md), section 4). What is not: restore to a point
in time, read replicas, and the Sessions API -- `withSession()`, `dump()` and `query()` are refused.

`batch()` executes its statements in order within one transaction. Any statement failure rolls back the whole batch and rejects the promise with `D1_ERROR:`. `exec()` splits its argument by lines and runs each as a statement, stops at the first failure, and keeps earlier successful lines; `D1_EXEC_ERROR` names the failing line. It does not roll back earlier lines.

### Binary values and numbers

BLOB values round trip as bytes. Bind an `ArrayBuffer`, a typed-array view, or a JavaScript array of byte integers;
HTTP SQL uses a nested byte array in `params`. The SQL Worker validates every element as an integer from 0 through 255
before binding it as binary data. An empty array is a zero-length BLOB. Invalid arrays and ordinary objects return
`D1_TYPE_ERROR` instead of becoming text; a view containing an out-of-range element, such as `new Uint16Array([256])`,
is rejected. `all()`, `raw()`, `first()` and both HTTP result formats return BLOB cells as **arrays of numbers**.

```js
await env.DB.prepare("INSERT INTO attachments (body) VALUES (?)")
  .bind(new Uint8Array([0, 1, 127, 128, 255])).run();
const bytes = await env.DB.prepare("SELECT body FROM attachments LIMIT 1").first("body");
// bytes is [0, 1, 127, 128, 255]
```

**Historical data:** releases 0.5.0 through 0.5.6 stored binary values supplied through parameter binding as
comma-separated number text, despite reporting success. This runtime fix does not automatically rewrite those rows.
SQL BLOB literals and migration/import files stored the binary value correctly. To look for candidate affected rows,
replace the example table/column names (and `rowid` with your primary key where needed):

```sql
SELECT rowid, typeof(body) AS stored_type, length(body) AS stored_length, body
FROM attachments
WHERE typeof(body) = 'text' AND body NOT GLOB '*[^0-9,]*';
```

This query is only a hint: legitimate text, empty strings and invalid byte lists can also match. Check against the
original data before changing any row; this release provides no automatic repair.

As with Cloudflare D1, JavaScript numbers cannot preserve all integers outside the safe integer range: for example,
`9007199254740993` reads as `9007199254740992`. A stored REAL `1.0` reads as JSON `1`. To retrieve the precise stored
representation, select `quote(value)` or an appropriate `printf()` format as text; `typeof(value)` reports SQLite's
storage type. BigInt is not supported by the D1 API.

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

The public HTTP JSON request may be **at most 1 MiB (1,048,576 bytes)**. A single byte-array parameter therefore holds less
binary data than the body size: commas and decimal digits count too. With a short INSERT and all bytes equal to 255,
262,128 binary bytes (about 256 KiB) fit in a 1,048,574-byte request; one more byte made a 1,048,578-byte request and was
rejected with HTTP 413 `REQUEST_TOO_LARGE` at the control-plane body limit. SQL text, other parameters and the byte values change that boundary.
64 KiB and 200 KiB arrays containing the full byte range were verified end to end.

A single object request may contain multiple SQL statements in its `sql` string. SQLite handles statement boundaries,
including trigger bodies, comments and semicolons inside strings. Each `/query` object request runs **one transaction**:
if any statement fails, all effects of that object roll back. An array of statement objects is also one transaction.
`env.DB.exec()` uses `/execute` and keeps earlier successful lines when a later line fails.

### Result size

HTTP SQL and `env.DB` share a **7 MiB (7,340,032-byte) limit on the complete UTF-8 JSON reply**. It includes column
names, `meta`, every statement's result in an array request, and all JSON framing. BLOB cells count as their expanded
number arrays. Exactly 7,340,032 bytes are accepted; use `LIMIT` and pagination to keep each reply within the limit.

An oversized result returns HTTP 200 with `{"success":false,"error":"D1_RESULT_TOO_LARGE: query response exceeds
7340032 bytes; use LIMIT to fetch results in smaller batches"}`. An array request returns an array containing that
failure. `env.DB` rejects with `D1_ERROR: D1_RESULT_TOO_LARGE: ...`; `exec()` uses its existing
`D1_EXEC_ERROR: Error in line N: ...` wrapper. No partial rows are returned.

The SQL Worker stops encoding rows after detecting overflow and consumes the remaining cursor before returning the
error. The database remains usable without resetting the object. A single `/query` object such as
`INSERT ... RETURNING` rolls back its writes when its result is rejected. An array `/query` request, including
`batch()`, also rolls back all its writes on this error.
`exec()` keeps its earlier successful lines, as with other statement errors.

This is a reply-size limit, not a process-memory limit or an execution deadline. Consuming the remaining cursor still
has no separate time limit. A large row and JSON serialization can use more memory than the reply size.

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
at most **1 MiB** (1,048,576 bytes). An oversized file is rejected locally before its execution request is sent; split it into smaller files.
One file consumes one database-object event. The node's default CPU configuration is 200 ms, but enforcement of that
budget for this internal SQL event is **not verified** by this change; do not treat 200 ms as a verified migration limit.

On a timeout or `503 RESULT_UNCERTAIN`, the CLI does **not resend the file**. It reads the ledger: if the filename is
present it continues, otherwise it stops with instructions to run `apply` again. If the ledger read also fails it stops.
The earlier write might have committed despite a lost reply; a later `apply` checks the ledger before choosing files.

## 7. Import a SQL file

```bash
cowctl d1 execute books --file books.sql --yes
```

`--file` reads a UTF-8 SQL file and is mutually exclusive with `--sql` and `--param`. Without `--yes`, an interactive
terminal asks before importing. Progress goes to standard error; `--json` returns the result, including
`committedOffset` (a zero-based byte offset), `statementCount` and `chunks`. A SQL error exits nonzero and reports the
last confirmed offset, statement count and SQLite error. There is no resume option in this release.

To move a database from Cloudflare, export it, create an empty destination, then import:

```bash
wrangler d1 export books --remote --output books.sql
cowctl d1 create books
cowctl d1 execute books --file books.sql --yes
```

**Each chunk is one transaction; the entire file is not.** A failing chunk rolls back completely, while earlier
chunks stay committed. Import into a newly created, empty database. Fixing a file and rerunning it can repeat earlier
INSERTs, so the reported offset is diagnostic rather than an automatic retry point. A timeout or `503 RESULT_UNCERTAIN`
stops immediately, reports the last confirmed offset and says that the current chunk may have committed. It never
resends that chunk.

The CLI reads 32 KiB at a time and sends the previous SQLite `remainder` with the next text. JSON-encoded requests are
capped at 960 KiB, leaving 64 KiB below the control plane's 1 MiB boundary, including escapes. The small read size also
lets statements below SQLite's SQL-length limit span several reads. An oversized incomplete statement is rejected
locally with its starting byte offset and a lower bound on its size; no oversized request is sent. Earlier incomplete
prefixes may have reached SQLite without executing a statement. SQLite also enforces its own statement limits and can
return `statement too long: SQLITE_TOOBIG` below the HTTP limit.

At EOF, a final statement without a semicolon receives a newline and semicolon. Already terminated SQL receives no extra
empty statement; trailing whitespace/comments need no execution. A small file that fits one read uses one transaction,
including when its final semicolon is missing. Remaining incomplete SQL, such as an unclosed string, is an error.
Files must not contain transaction-control statements (`BEGIN`, `COMMIT`, `SAVEPOINT`); trigger `BEGIN … END` bodies work.

Foreign keys are checked for each committed chunk. `PRAGMA defer_foreign_keys=TRUE;` lasts only until that chunk's
transaction ends. If a child row refers to a parent row in a later chunk, the child chunk fails with `EXECUTION_FAILED` and the object is reset and rolled back to its last known good state.
The observed reason ends with `FOREIGN KEY constraint failed: SQLITE_CONSTRAINT (extended: SQLITE_CONSTRAINT_FOREIGNKEY)`. Put referenced tables and their rows before
referencing rows; the pragma at the start of a Wrangler export does not defer constraints across the whole import.

The HTTP protocol also accepts a standalone `POST /v1/tenants/{t}/databases/{d}/query` body:

```json
{"ingest":"CREATE TABLE books(id INTEGER PRIMARY KEY); INSERT INTO books VALUES(1);"}
```

`ingest` must be a string, without `sql` or `params`, and cannot appear in an array. SQLite executes its complete
statements within `transactionSync`; an error rolls back that request. A successful answer has `success: true`,
`results: []`, the usual `meta`, and `ingest: {remainder, statementCount, rowsRead, rowsWritten}`. Send the returned
remainder unchanged before the next text; SQLite chooses statement boundaries, including trigger bodies and quoted
semicolons. Failure uses the usual `success: false` and SQLite error text. Existing SQL object/array requests are unchanged.

## 8. Export a SQL file

```bash
cowctl d1 export books --output books.sql
cowctl d1 export books --output schema.sql --no-data
cowctl d1 export books --output selected.sql --table authors --table books
```

The file starts with `PRAGMA defer_foreign_keys=TRUE;`, without `BEGIN` or `COMMIT`. Tables follow their creation
order, with each table's CREATE followed by its INSERTs. `sqlite_sequence` comes last, with DELETE followed by its
rows; indexes, triggers and views follow the table data. Migration ledger tables are ordinary exported tables.
Internal `_cf_` tables and other `sqlite_` tables are excluded, with Wrangler's exceptions for `sqlite_sequence`
and `sqlite_stat*` statistics. Virtual tables are refused.

Values are SQL expressions produced by SQLite's `quote()` in the database: large integers stay exact, and BLOBs
become hex literals. The command only joins those values into INSERT statements. Text containing NUL uses a
SQLite-produced `CAST(X'...' AS TEXT)` expression, because `quote(TEXT)` alone would truncate it.

Options:

- `--no-schema` writes data without CREATE statements; `--no-data` writes schema without INSERTs. They cannot be combined.
- Repeat `--table NAME` to select names. As with Wrangler, later index/trigger/view objects are included only when
  their own names match the filter. Omit `--table` for a complete backup, including sequences and all schema objects.
- An existing output file is preserved unless `--force` is given. The completed file is published only after all
  checks pass; a failed export leaves no partial replacement.
- `--json` prints a result with file, row/table counts, bytes, mode, pages, retries, consistency and oversized-INSERT
  counts. Progress and warnings go to stderr.
- `--allow-writes` accepts changes detected during pagination. The JSON result, stderr and SQL file then explicitly
  mark the export as potentially inconsistent.

After discovering the schema, the command first tries to fetch the schema and all data in **one array request**.
If that complete reply fits within 7,340,032 bytes, it is an atomic read of one database state. Writes before or after
that request do not make its contents inconsistent. Larger databases use key pagination: rowid where accessible,
otherwise the primary key (including composite keys for WITHOUT ROWID tables). Each table starts at **1,000 rows per
page**. `D1_RESULT_TOO_LARGE` halves the page size and retries the same unread page; if one row still exceeds the reply
limit, the command fails with its table and exact row key.

A paginated export compares `total_changes()` and the complete schema at the beginning and end. A difference fails
the export by default, with instructions to stop writes and retry. This check is **best effort**: an object restart
resets the connection's counter, and matching endpoints do not prove no writes occurred between them. For a reliable
backup, stop all writes for the entire export and prevent database-object restarts. `--allow-writes` provides no
snapshot guarantee. Export does not block writers. For a consistent historical export, first restore into an unbound new database as described below.

An INSERT exceeding **100,000 UTF-8 bytes** is written normally, but the final result and warning count such
statements by table. Those statements cannot be imported by `cowctl d1 execute --file`; splitting the export into
smaller pages does not make an individual row's INSERT shorter. Other tables can also require referenced rows to be
loaded first: export keeps creation order and does not reorder tables for foreign keys. The importer's per-chunk
foreign-key checks still apply.

The SQL layout follows Wrangler 4.136.1. Byte-for-byte output differs: SQLite quote literals keep newlines inside
text rather than Wrangler's replace/char expressions, hex case can differ, and this exporter preserves integers
outside JavaScript's safe range that Wrangler's local exporter can round. Warnings here use the exact 100,000-byte
import limit. These differences do not require a different SQL importer.

For a backup and restore, stop writers, export the complete database, retain the SQL file, then restore into a new,
empty database and compare schema, row counts and content before switching applications:

```bash
cowctl d1 export books --output books-backup.sql
cowctl d1 create books-restored
cowctl d1 execute books-restored --file books-backup.sql --yes
```

To move this file to a local Wrangler database, configure its D1 binding and run
`wrangler d1 execute books --local --file books-backup.sql`. For a remote destination, use `--remote` and its
credentials. Check that destination's SQL and statement limits first, review oversized-INSERT warnings, and verify
the restored data. `env.DB.dump()` remains unsupported; export is a CLI operation.

## 9. Delete one

```bash
cowctl d1 delete books --yes
```

The database is marked deleting: no Binding to it works and no new statement reaches it, and it stops counting against
the tenant's storage. Its data stays in the object store (there is no data reclamation in 0.5), so the name cannot
be used again in this release.

## 10. Failure and consistency

Each SQL HTTP request is one event of the database object; statements in a batch share that event's limits. The same durability and estimated tenant storage quota apply as to other objects. A failed `exec()` keeps earlier successful lines; `batch()` rolls back on a statement failure. SQL errors use D1 error prefixes. HTTP SQL preserves the node's status and body, including `success:false` with status 200 and platform errors such as `STORAGE_UNAVAILABLE`, `DO_OBJECT_TOO_LARGE` and `QUOTA_EXCEEDED`. It tries another member only when connection establishment failed or the node returned a refusal known to precede execution ([errors](errors.md), section 4). Losing the reply after connecting returns `503 RESULT_UNCERTAIN` without replaying the request. A confirmation failure can leave changes readable locally until the object is reopened and repaired: do not blindly repeat a non-idempotent statement. Simultaneous loss of the owner and all confirming peers may lose the unuploaded tail. Deletions and other transactions that do not grow the database remain permitted over a byte limit. No read replicas or Sessions API provide another consistency mode.

Database names follow `[a-z0-9][a-z0-9-]{0,62}`: hyphens and a leading digit are supported. A malformed entry in the deployment's database or namespace table is skipped independently; `deployment.invalidNamespaceEntryCount` at the node's state endpoint counts skipped entries.


## 11. Restore to a published point

Time travel creates a **new database**. It does not change the source or switch its Bindings; the source can continue
serving writes. Cloudflare D1 restores in place; Coworkerd deliberately restores under a new name.

```bash
cowctl d1 time-travel info books
cowctl d1 time-travel info books --timestamp 2026-11-16T04:00:00Z
cowctl d1 time-travel restore books --timestamp 2026-11-16T04:00:00Z --to books-restored --yes
cowctl d1 time-travel restore books --bookmark BOOKMARK --to books-restored --yes
```

The command displays the resolved revision and its publication time **before** asking for confirmation or starting
(`--yes` skips the question). `--timestamp` accepts ISO time with timezone or Unix milliseconds. It selects the highest
published revision whose generation time is no later than T. Writes after that point are absent. Writes before T
that have not been published can also be absent: the default segment interval is about 30 seconds. A bookmark names
one exact immutable revision; a bookmark from another database is rejected. A time before all matching history fails;
a time after the latest publication selects that latest revision. Clocks can differ at an owner takeover: revision
order wins. The lookup searches revision numbers with exact GETs, splitting owner epochs before searching their times.
A lookup is limited to 128 history reads (up to 256 HTTP attempts if each read retries once). Exceeding this returns
422 `HISTORY_READ_LIMIT`; choose a bookmark or a later time. A bookmark needs one history read.

The CLI saves its random operation id and **resolved bookmark**, without a credential, under
`~/.cowctl/restore-operations/`. If interrupted, rerun the same command to resume; `--operation-id ID` can specify a
stable id explicitly (16–128 letters/digits/hyphens). Keep the saved state when moving a retry to another machine.
After showing a completed result, the CLI removes that command's local operation record. Running the same command
again without an explicit id creates a new operation and is refused because the destination name already exists.
The control plane and nodes keep progress only in memory; after a restart they may answer unknown. Resubmission
recognizes the already published first snapshot, and registration returns the same database if it already happened,
provided the source database still exists and is active. Resubmitting after deleting the source is refused because
the source is resolved first; this does not affect the already restored database.
A different new operation can use a new `--operation-id`. Reusing an operation with changed parameters is refused while
its request is cached. If the destination name is taken at the final registration, the restore fails; its unreferenced
history remains and no database is registered. Deleted database records still count against namespace quota.

History currently never expires. Future reclamation will retain at least the latest **7 days**. This feature is for
D1 databases only; KV and R2 time travel are unsupported. A target is registered only after its complete first snapshot
has been verified. Do not bind a database intended as a stable export source:

```bash
cowctl d1 time-travel restore books --bookmark BOOKMARK --to books-export --yes
cowctl d1 export books-export --output books-at-bookmark.sql
```

Leave that new database unbound and do not write to it during export. This uses the existing exporter; there is no
`export --bookmark` mode.

Each node runs one restore at a time. Retained records for the selected object are limited to **128 MiB of payload**
and **100,000 records**; other objects in a shared segment do not count. A shared segment's transient decode is
separately bounded at **384 MiB**, with a total incremental memory budget of **512 MiB**. Snapshot hashing streams data.
Disk admission refuses a reservation larger than the node limit before evicting cache files; otherwise it first
evicts eligible cold cache files, then reserves two images under the node's existing disk limit.
A resource refusal reports the resource, required amount and limit as `SEED_RESOURCE_LIMIT`; it never truncates
history or silently chooses a newer point. Name and tenant quotas are checked before work and again at registration.
`RESTORE_UNAVAILABLE` can be retried with the saved operation and bookmark; a used target returns `SEED_TARGET_IN_USE`
at the node. A lost SQL reply is still `RESULT_UNCERTAIN` and must not be blindly replayed.

The control plane caches at most 16 pending restore requests per tenant and 1024 requests overall. Idle unfinished
requests expire after 60 seconds; completed/failed receipts expire after 10 minutes and can be evicted sooner.
Requests accepted by no node are removed immediately. Polling refreshes the idle timeout. An expired request can
be resubmitted with the saved bookmark and operation id; the immutable snapshot remains its receipt.

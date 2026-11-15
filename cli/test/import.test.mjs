import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { applyMigrations, importSql, migrationBody, MAX_QUERY_BYTES, IMPORT_CHUNK_BYTES, IMPORT_REQUEST_BYTES } from "../adapter.mjs";

async function fixture(fn) {
  const directory = await mkdtemp(join(tmpdir(), "cowctl-import-"));
  try { await fn(directory); } finally { await rm(directory, { recursive: true, force: true }); }
}
const answer = (remainder = "", statementCount = 1) => ({ success: true, results: [], ingest: { remainder, statementCount, rowsRead: 0, rowsWritten: 1 } });

test("migration JSON accepts exactly 1 MiB and rejects one more byte before sending", () => fixture(async directory => {
  const base = "SELECT 1;", name = "boundary.sql", table = "ledger";
  const text = base + " ".repeat(MAX_QUERY_BYTES - Buffer.byteLength(migrationBody(base, name, table)));
  assert.equal(Buffer.byteLength(migrationBody(text, name, table)), MAX_QUERY_BYTES);
  await writeFile(join(directory, name), text);
  const sent = [];
  const query = async body => { sent.push(Buffer.byteLength(body)); return [{success:true}, {success:true}]; };
  assert.deepEqual(await applyMigrations({directory, table, files:[name], query}), [name]);
  assert.deepEqual(sent, [MAX_QUERY_BYTES]);
  await writeFile(join(directory, name), text + " ");
  await assert.rejects(applyMigrations({directory, table, files:[name], query}), /too large/);
  assert.deepEqual(sent, [MAX_QUERY_BYTES]);
  assert.equal(IMPORT_REQUEST_BYTES, MAX_QUERY_BYTES - 64 * 1024);
}));

test("small file without a semicolon is one request and reports UTF-8 bytes", () => fixture(async directory => {
  const file = join(directory, "small.sql"), text = "INSERT INTO t VALUES('中文;🌍')";
  await writeFile(file, text); const sent = [];
  const result = await importSql({ file, query: async body => { sent.push(JSON.parse(body).ingest); return answer(); } });
  assert.deepEqual(sent, [text + "\n;"]);
  assert.equal(result.committedOffset, Buffer.byteLength(text)); assert.equal(result.chunks, 1);
}));

for (const timeout of [false, true]) test(`${timeout ? "wrapped timeout" : "503 RESULT_UNCERTAIN"} stops at the previous offset without replay`, () => fixture(async directory => {
  const file = join(directory, "uncertain.sql"); await writeFile(file, " ".repeat(IMPORT_CHUNK_BYTES) + "SELECT 2;");
  let calls = 0;
  await assert.rejects(importSql({ file, query: async () => {
    calls++;
    if (calls === 1) return answer("", 1);
    throw timeout ? new Error("fetch failed", { cause: Object.assign(new Error("timeout"), { name: "TimeoutError" }) })
      : Object.assign(new Error("reply lost"), { status: 503, code: "RESULT_UNCERTAIN" });
  } }), error => {
    assert.equal(error.import.committedOffset, IMPORT_CHUNK_BYTES); assert.equal(error.import.statementCount, 1);
    assert.equal(error.import.uncertain, true); assert.match(error.message, /not resent/); return true;
  });
  assert.equal(calls, 2);
}));

test("oversized incomplete statement never sends an oversized request", () => fixture(async directory => {
  const file = join(directory, "huge.sql"); await writeFile(file, "SELECT '" + "\"".repeat(MAX_QUERY_BYTES) + "';");
  const sizes = [];
  await assert.rejects(importSql({ file, query: async body => {
    sizes.push(Buffer.byteLength(body)); return answer(JSON.parse(body).ingest, 0);
  } }), error => {
    assert.match(error.message, /statement at file byte 0.*too large.*source bytes.*not sent/);
    assert.equal(error.import.statementCount, 0); assert.equal(error.import.committedOffset, 0); return true;
  });
  assert(sizes.length > 0 && sizes.every(bytes => bytes <= IMPORT_REQUEST_BYTES && bytes < MAX_QUERY_BYTES));
}));

test("SQL failure preserves the last committed boundary and stops", () => fixture(async directory => {
  const file = join(directory, "bad.sql"); await writeFile(file, " ".repeat(IMPORT_CHUNK_BYTES) + "bad SQL;");
  let calls = 0;
  await assert.rejects(importSql({ file, query: async () => ++calls === 1 ? answer(" ", 3) : { success: false, error: "near bad: SQLITE_ERROR" } }), error => {
    assert.equal(error.import.committedOffset, IMPORT_CHUNK_BYTES - 1);
    assert.equal(error.import.statementCount, 3); assert.match(error.message, /near bad: SQLITE_ERROR/); return true;
  });
  assert.equal(calls, 2);
}));

test("an unfinished EOF string fails with a nonzero import error", () => fixture(async directory => {
  const file = join(directory, "unfinished.sql"); await writeFile(file, "SELECT 'open");
  await assert.rejects(importSql({ file, query: async body => answer(JSON.parse(body).ingest, 0) }), /incomplete statement at end of file/);
}));

test("terminated SQL with quoted comment markers gets no extra empty statement", () => fixture(async directory => {
  const file = join(directory, "comments.sql"), text = "SELECT '--x; /*literal*/'; -- tail ;";
  await writeFile(file, text); const sent = [];
  await importSql({ file, query: async body => { sent.push(JSON.parse(body).ingest); return answer(); } });
  assert.deepEqual(sent, [text]);
  await writeFile(file, "-- comment\n/* only ; */");
  const result = await importSql({ file, query: async () => assert.fail("comments must not execute") });
  assert.equal(result.chunks, 0); assert.equal(result.statementCount, 0);
}));

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { applyMigrations, compareMigrations, createMigration, migrationBody, migrationFiles,
  migrationSettings, unsupported, durableObjectsOf } from "../adapter.mjs";

async function fixture(fn) {
  const directory = await mkdtemp(join(tmpdir(), "cowctl-migrations-"));
  try { await fn(directory); } finally { await rm(directory, { recursive: true, force: true }); }
}
const ok = results => ({ success: true, results });

test("numeric order, lexical ties, next number and no-op files", () => fixture(async directory => {
  const names = ["0010_last.sql", "0003_third.sql", "0002_b.sql", "0002_A.sql", "unversioned.sql"];
  for (const name of names) await writeFile(join(directory, name), "-- empty");
  assert.deepEqual(await migrationFiles(directory), ["0002_A.sql", "0002_b.sql", "0003_third.sql", "0010_last.sql", "unversioned.sql"]);
  assert.equal(await createMigration(directory, "add two columns"), "0011_add_two_columns.sql");
  assert.equal(compareMigrations("2.sql", "10.sql"), -8);
  assert.deepEqual(JSON.parse(migrationBody("-- only\n/* comment */\n", "empty.sql", 'a"b')), [
    { sql: "SELECT 1" }, { sql: 'INSERT INTO "a""b" (name) VALUES (?)', params: ["empty.sql"] }]);
  await assert.rejects(createMigration(directory, "../bad"), /path separators/);
  await mkdir(join(directory, "nested"));
  await assert.rejects(migrationFiles(directory), /subdirectory/);
}));

for (const timeout of [false, true]) for (const committed of [false, true]) {
  test(`${timeout ? "timeout" : "RESULT_UNCERTAIN"}: ${committed ? "ledger confirms and next file runs" : "absent ledger stops without resending"}`, () => fixture(async directory => {
    for (const name of ["0001_a.sql", "0002_b.sql"]) await writeFile(join(directory, name), "SELECT 1");
    const sent = [], reads = [];
    const query = async text => {
      const body = JSON.parse(text);
      if (!Array.isArray(body)) { reads.push(body.sql); return ok(body.sql.includes("sqlite_master") ? [{ name: "d1_migrations" }] : committed ? [{ name: "0001_a.sql" }] : []); }
      const name = body[1].params[0]; sent.push(name);
      if (sent.length === 1) throw timeout ? Object.assign(new Error("timed out"), { name: "TimeoutError" }) : Object.assign(new Error("uncertain"), { status: 503, code: "RESULT_UNCERTAIN" });
      return [ok([]), ok([])];
    };
    const run = () => applyMigrations({ directory, table: "d1_migrations", files: ["0001_a.sql", "0002_b.sql"], query });
    if (committed) assert.deepEqual(await run(), ["0001_a.sql", "0002_b.sql"]);
    else await assert.rejects(run(), /0001_a.sql:.*run apply again.*not resent/);
    assert.deepEqual(sent, committed ? ["0001_a.sql", "0002_b.sql"] : ["0001_a.sql"]);
    assert.equal(reads.length, 2);
  }));
}

test("escaped JSON body is capped before sending and SQL failures name the file", () => fixture(async directory => {
  await writeFile(join(directory, "huge.sql"), '"'.repeat(512 * 1024));
  let calls = 0;
  await assert.rejects(applyMigrations({ directory, table: "ledger", files: ["huge.sql"], query: async () => { calls++; } }), /huge.sql:.*too large.*split/);
  assert.equal(calls, 0);
  await writeFile(join(directory, "bad.sql"), "bad SQL");
  await assert.rejects(applyMigrations({ directory, table: "ledger", files: ["bad.sql"], query: async () => ({ success: false, error: "near bad: syntax error" }) }), /bad.sql: near bad/);
}));

test("configuration priority and adapter migration metadata", () => fixture(async directory => {
  const path = join(directory, "wrangler.json");
  const config = { name: "sample", main: "worker.js", compatibility_date: "2026-10-01", d1_databases: [{ binding: "DB", database_name: "books", migrations_dir: "schema", migrations_table: "ledger" }] };
  await writeFile(path, JSON.stringify(config));
  assert.deepEqual(await migrationSettings({ database: "books", configPath: path }), { directory: join(directory, "schema"), table: "ledger" });
  assert.deepEqual(await migrationSettings({ database: "books", configPath: path, cwd: directory, dir: "override", table: "custom" }), { directory: join(directory, "override"), table: "custom" });
  config.d1_databases[0].migrations_pattern = "schema/*.sql";
  assert.deepEqual(unsupported(config), []);
  const durable = durableObjectsOf(config, ["sample"]);
  assert.deepEqual(durable.durableObjects, [{ binding: "DB", className: "__coworkerdSqlDatabase", database: "books" }]);
  await writeFile(path, JSON.stringify(config));
  await assert.rejects(migrationSettings({ database: "books", configPath: path }), /migrations_pattern/);
  config.d1_databases[0].unknown = true;
  assert(unsupported(config).includes("d1_databases[0].unknown"));
}));

test("existing CLI entry points retain version, help and usage failures", () => {
  const cli = new URL("../bin/cowctl.mjs", import.meta.url);
  for (const args of [["--version"], ["--help"], ["d1", "migrations", "unknown"]]) {
    const result = spawnSync(process.execPath, [cli.pathname, ...args], { encoding: "utf8" });
    assert.equal(result.status, args.includes("unknown") ? 2 : 0, result.stderr);
    if (args[0] === "--help") assert.match(result.stdout, /migrations create/);
  }
});

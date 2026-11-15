import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, readdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import { exportSql, EXPORT_PAGE_ROWS } from '../adapter.mjs';

async function fixture(run, cap = 7340032) {
  const directory = await mkdtemp(join(tmpdir(), 'cowctl-export-')), db = new DatabaseSync(':memory:');
  const calls = []; let hook;
  const query = async body => {
    const input = JSON.parse(body), statements = Array.isArray(input) ? input : [input]; calls.push(input);
    db.exec('SAVEPOINT read_export');
    try {
      const answers = statements.map(s => ({ success: true, results: db.prepare(s.sql).all(...(s.params ?? [])), meta: {} }));
      const answer = Buffer.byteLength(JSON.stringify(answers)) > cap ? [{ success: false, error: 'D1_RESULT_TOO_LARGE: exceeds 7340032 bytes; LIMIT' }] : answers;
      db.exec('RELEASE read_export');
      if (hook) await hook(statements, answer);
      return Array.isArray(input) ? answer : answer[0];
    } catch (error) { db.exec('ROLLBACK TO read_export; RELEASE read_export'); throw error; }
  };
  try { await run({ directory, db, query, calls, setHook: fn => { hook = fn; }, file: join(directory, 'dump.sql') }); }
  finally { db.close(); await rm(directory, { recursive: true, force: true }); }
}

test('atomic export preserves exact SQL values and schema, with sqlite_sequence last', () => fixture(async f => {
  f.db.exec(`CREATE TABLE "data"(id INTEGER PRIMARY KEY AUTOINCREMENT,v,b,n,r);
    CREATE INDEX lookup ON data(v); CREATE TABLE audit(n);
    CREATE TRIGGER capture AFTER INSERT ON data BEGIN INSERT INTO audit VALUES(new.id); END;
    CREATE VIEW titles AS SELECT v FROM data;
    INSERT INTO data(v,b,n,r) VALUES('中文; quote''\nline',X'00ff',9007199254740993,1.25);
    INSERT INTO data(v,b,n,r) VALUES(CAST(X'610062' AS TEXT),X'',NULL,1.0);`);
  const report = await exportSql({ file: f.file, query: f.query });
  assert.equal(report.mode, 'atomic'); assert.equal(report.consistency.checked, false);
  const text = await readFile(f.file, 'utf8'); assert.ok(text.startsWith('PRAGMA defer_foreign_keys=TRUE;\n'));
  assert.ok(text.includes('9007199254740993')); assert.ok(text.includes("CAST(X'610062' AS TEXT)"));
  assert.ok(text.indexOf('DELETE FROM sqlite_sequence') > text.indexOf('CREATE TABLE audit'));
  assert.ok(text.indexOf('CREATE TRIGGER') > text.indexOf('DELETE FROM sqlite_sequence'));
  const restored = new DatabaseSync(':memory:');
  try {
    restored.exec(text);
    const fingerprint = db => db.prepare('SELECT quote(v) v,hex(CAST(v AS BLOB)) bytes,quote(b) b,quote(n) n,quote(r) r FROM data ORDER BY id').all();
    assert.deepEqual(fingerprint(restored), fingerprint(f.db));
    restored.exec("INSERT INTO data(v) VALUES('next')"); f.db.exec("INSERT INTO data(v) VALUES('next')");
    assert.deepEqual(restored.prepare('SELECT * FROM sqlite_sequence').all(), f.db.prepare('SELECT * FROM sqlite_sequence').all());
  } finally { restored.close(); }
  assert.ok(f.calls.some(c => Array.isArray(c) && c.length > 2));
}));

test('pagination halves on quota, uses exact composite keys, and preserves every row', () => fixture(async f => {
  f.db.exec('CREATE TABLE keys(a TEXT,b INTEGER,v TEXT,PRIMARY KEY(a,b)) WITHOUT ROWID');
  for (let n = 0; n < 25; n++) f.db.prepare('INSERT INTO keys VALUES(?,?,?)').run(n % 2 ? "quote'" : '中文', n, 'x'.repeat(300));
  const report = await exportSql({ file: f.file, query: f.query });
  assert.equal(report.mode, 'paged'); assert.equal(report.rows, 25); assert.ok(report.retries > 1);
  assert.equal(report.initialPageRows, EXPORT_PAGE_ROWS); assert.equal(report.consistency.checked, true);
  assert.equal(report.consistency.changesDetected, false);
  const restored = new DatabaseSync(':memory:');
  try { restored.exec(await readFile(f.file, 'utf8')); assert.deepEqual(restored.prepare('SELECT * FROM keys ORDER BY a,b').all(), f.db.prepare('SELECT * FROM keys ORDER BY a,b').all()); }
  finally { restored.close(); }
}, 4000));

test('writes during pagination fail without publishing; allow-writes marks the file', () => fixture(async f => {
  f.db.exec('CREATE TABLE t(id INTEGER PRIMARY KEY,v);');
  for (let n = 1; n <= 15; n++) f.db.prepare('INSERT INTO t VALUES(?,?)').run(n, 'x'.repeat(300));
  let inserted = false;
  f.setHook(statements => { if (!inserted && statements.length === 1 && statements[0].sql.includes(' LIMIT ')) { inserted = true; f.db.exec("INSERT INTO t VALUES(100,'during')"); } });
  await assert.rejects(exportSql({ file: f.file, query: f.query }), /database changed.*stop writes/);
  assert.deepEqual(await readdir(f.directory), []);
  inserted = false;
  f.setHook(statements => { if (!inserted && statements.length === 1 && statements[0].sql.includes(' LIMIT ')) { inserted = true; f.db.exec("INSERT INTO t VALUES(101,'allowed')"); } });
  const report = await exportSql({ file: f.file, query: f.query, allowWrites: true });
  assert.equal(report.consistency.changesDetected, true); assert.ok((await readFile(f.file, 'utf8')).includes('--allow-writes'));
}, 4000));

test('single oversized row identifies its exact key and leaves no output', () => fixture(async f => {
  f.db.exec("CREATE TABLE huge(id INTEGER PRIMARY KEY,v); INSERT INTO huge VALUES(9007199254740993,zeroblob(1500))");
  await assert.rejects(exportSql({ file: f.file, query: f.query }), /table huge, row key \(9007199254740993\): one row exceeds/);
  assert.deepEqual(await readdir(f.directory), []);
}, 2500));

test('options match name filtering; files are replaced only with force', () => fixture(async f => {
  f.db.exec("CREATE TABLE a(n); CREATE TABLE b(n); CREATE TABLE _cf_hidden(n); INSERT INTO a VALUES(1); INSERT INTO b VALUES(2); CREATE INDEX a_idx ON a(n)");
  await assert.rejects(exportSql({ file: f.file, query: f.query, noSchema: true, noData: true }), /cannot be combined/);
  await exportSql({ file: f.file, query: f.query, noData: true, tables: ['a'] });
  let text = await readFile(f.file, 'utf8'); assert.ok(text.includes('CREATE TABLE a')); assert.ok(!text.includes('INSERT')); assert.ok(!text.includes('CREATE TABLE b'));
  await assert.rejects(exportSql({ file: f.file, query: f.query }), /already exists/); assert.equal(await readFile(f.file, 'utf8'), text);
  await exportSql({ file: f.file, query: f.query, noSchema: true, tables: ['a', 'b'], force: true });
  text = await readFile(f.file, 'utf8'); assert.ok(!text.includes('CREATE')); assert.ok(text.includes('INSERT INTO "a"')); assert.ok(text.includes('INSERT INTO "b"')); assert.ok(!text.includes('_cf_'));
  await exportSql({ file: f.file, query: f.query, force: true });
  text = await readFile(f.file, 'utf8'); assert.ok(!text.includes('_cf_')); assert.ok(text.includes('CREATE INDEX a_idx'));
}));

test('a target created during export is never overwritten without force', () => fixture(async f => {
  f.db.exec('CREATE TABLE t(n)'); let created = false;
  f.setHook(async () => { if (!created) { created = true; await writeFile(f.file, 'keep'); } });
  await assert.rejects(exportSql({ file: f.file, query: f.query }), /EEXIST/);
  assert.equal(await readFile(f.file, 'utf8'), 'keep'); assert.deepEqual(await readdir(f.directory), ['dump.sql']);
}));

test('oversized INSERT warnings count UTF-8 bytes and identify tables', () => fixture(async f => {
  f.db.exec('CREATE TABLE wide(v)'); f.db.prepare('INSERT INTO wide VALUES(?)').run('界'.repeat(34000));
  const result = await exportSql({ file: f.file, query: f.query });
  assert.equal(result.oversizedStatements, 1); assert.deepEqual(result.oversizedTables, { wide: 1 });
  assert.ok(Buffer.byteLength((await readFile(f.file, 'utf8')).split('\n')[2]) > 100000);
}));

test('schema changes are detected even when total_changes does not change', () => fixture(async f => {
  f.db.exec('CREATE TABLE t(n,v)');
  for (let n = 0; n < 20; n++) f.db.prepare('INSERT INTO t VALUES(?,?)').run(n, 'x'.repeat(300));
  let altered = false;
  f.setHook(statements => { if (!altered && statements.length === 1 && statements[0].sql.includes(' LIMIT ')) { altered = true; f.db.exec('CREATE TABLE added(n)'); } });
  await assert.rejects(exportSql({ file: f.file, query: f.query }), /database changed.*stop writes/);
  assert.deepEqual(await readdir(f.directory), []);
}, 4000));

test('virtual tables are rejected explicitly', () => fixture(async f => {
  f.db.exec('CREATE VIRTUAL TABLE search USING fts5(v)');
  await assert.rejects(exportSql({ file: f.file, query: f.query }), /cannot export virtual table search/);
  assert.deepEqual(await readdir(f.directory), []);
}));

test('native cell-size failure also narrows to an exact offending row', () => fixture(async f => {
  f.db.exec('CREATE TABLE huge(id INTEGER PRIMARY KEY,b); INSERT INTO huge VALUES(43,zeroblob(5000))');
  const query = async body => {
    const input = JSON.parse(body), statements = Array.isArray(input) ? input : [input];
    if (statements.some(s => s.sql.includes(' AS v0') && s.sql.includes('FROM "huge"')))
      throw new Error('string or blob too big: SQLITE_TOOBIG');
    return f.query(body);
  };
  await assert.rejects(exportSql({ file: f.file, query }), /table huge, row key \(43\): one row exceeds/);
  assert.deepEqual(await readdir(f.directory), []);
}));

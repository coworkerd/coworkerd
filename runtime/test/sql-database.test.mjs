import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import vm from 'node:vm';
import { DatabaseSync } from 'node:sqlite';

const source = await readFile(new URL('../sql-database.js', import.meta.url), 'utf8');
const classSource = source.replace(/^import .*;$/m, '').replace(/^export /gm, '');
function fixture({ wallDeadline = false } = {}) {
  const db = new DatabaseSync(':memory:');
  const sqlite = {
    get databaseSize() { return db.prepare('PRAGMA page_count').get().page_count * 4096; },
    checkExecutionDeadline() {},
    prepare(text) {
      return (...params) => {
        const statement = db.prepare(text), columns = statement.columns().map(column => column.name);
        const rows = statement.all(...params).map(row => columns.map(column => {
          const value = row[column];
          // workerd exposes SQLite BLOB cells as ArrayBuffer; node:sqlite exposes Uint8Array.
          return value instanceof Uint8Array ? Uint8Array.from(value).buffer : value;
        }));
        return { columnNames: columns, raw: () => rows.values(), rowsRead: rows.length, rowsWritten: 0 };
      };
    },
  };
  const storage = { sql: sqlite, async sync() {}, transactionSync(fn) {
    db.exec('SAVEPOINT batch');
    try { const result = fn(); db.exec('RELEASE batch'); return result; }
    catch (error) { db.exec('ROLLBACK TO batch; RELEASE batch'); throw error; }
  } };
  const C = vm.runInNewContext(classSource + '; SqlDatabase', {
    DurableObject: class { constructor(ctx) { this.ctx = ctx; } },
    ArrayBuffer, Uint8Array, Error, Request, Response, URL, TextEncoder,
  });
  C.wallDeadline = wallDeadline;
  const worker = new C({ storage });
  const fetch = (body, endpoint = 'query', format = 'ARRAY_OF_OBJECTS') =>
    worker.fetch(new Request(`http://sql/${endpoint}?resultsFormat=${format}`, { method: 'POST', body: JSON.stringify(body) }));
  return { db, storage, fetch, close: () => db.close(), async query(body, endpoint, format) {
    return (await fetch(body, endpoint, format)).json();
  } };
}

test('byte parameters including empty and all byte values round trip in both result formats', async () => {
  const f = fixture();
  try {
    await f.query({ sql: 'CREATE TABLE blobs(id INTEGER,b BLOB)' });
    const values = [[], [0,1,127,128,255], Array.from({length:256}, (_,i) => i)];
    for (const [id,value] of values.entries()) assert.equal((await f.query({ sql:'INSERT INTO blobs VALUES(?,?)', params:[id,value] })).success, true);
    for (const format of ['ARRAY_OF_OBJECTS','ROWS_AND_COLUMNS']) {
      const answer = await f.query({ sql:'SELECT id,b,typeof(b) AS type,length(b) AS len FROM blobs ORDER BY id' },'query',format);
      const rows = format === 'ROWS_AND_COLUMNS' ? answer.results.rows.map(row => Object.fromEntries(answer.results.columns.map((column,i) => [column,row[i]]))) : answer.results;
      assert.deepEqual(rows, values.map((value,id) => ({id,b:value,type:'blob',len:value.length})));
    }
  } finally { f.close(); }
});

test('invalid byte values and objects report D1_TYPE_ERROR without inserting rows', async () => {
  const f=fixture();
  try {
    await f.query({sql:'CREATE TABLE rejected(b)'});
    for (const value of [[256],[-1],[1.5],['1'],[[1]],{}, {toString:null}]) {
      const answer=await f.query({sql:'INSERT INTO rejected VALUES(?)',params:[value]});
      assert.equal(answer.success,false); assert.match(answer.error,/D1_TYPE_ERROR: Type 'object' not supported for value/);
    }
    assert.match((await f.query({sql:'SELECT ?',params:{}})).error,/D1_TYPE_ERROR/);
    assert.deepEqual((await f.query({sql:'SELECT * FROM rejected'})).results,[]);
  } finally { f.close(); }
});

test('query batches bind BLOBs and a later invalid binding rolls the whole batch back', async () => {
  const f=fixture();
  try {
    await f.query({sql:'CREATE TABLE batch(id,b)'});
    const insert=(id,b)=>({sql:'INSERT INTO batch VALUES(?,?)',params:[id,b]});
    assert((await f.query([insert(1,[0]),insert(2,[255])])).every(row=>row.success));
    const bad=await f.query([insert(3,[1]),insert(4,[1.5])]);
    assert.equal(bad[0].success,false);assert.match(bad[0].error,/D1_TYPE_ERROR/);
    assert.deepEqual((await f.query({sql:'SELECT id,b FROM batch ORDER BY id'})).results,[{id:1,b:[0]},{id:2,b:[255]}]);
  } finally { f.close(); }
});

test('execute converts parameters and BLOB cells while retaining earlier successful statements', async () => {
  const f=fixture();
  try {
    await f.query({sql:'CREATE TABLE executed(id,b)'});
    const one=await f.query({sql:'SELECT ? AS b',params:[[0,255]]},'execute','ROWS_AND_COLUMNS');
    assert.deepEqual(one.results,{columns:['b'],rows:[[[0,255]]]});
    const answers=await f.query([{sql:'INSERT INTO executed VALUES(1,?)',params:[[127]]},{sql:'INSERT INTO executed VALUES(2,?)',params:[[256]]},{sql:'INSERT INTO executed VALUES(3,?)',params:[[1]]}],'execute');
    assert.equal(answers.length,2);assert.equal(answers[0].success,true);assert.match(answers[1].error,/D1_TYPE_ERROR/);
    assert.deepEqual((await f.query({sql:'SELECT id,b FROM executed'})).results,[{id:1,b:[127]}]);
  } finally { f.close(); }
});

test('threaded execute syncs each successful statement before executing the next', async () => {
  const f = fixture({ wallDeadline: true });
  try {
    await f.query({ sql: 'CREATE TABLE entries(id INTEGER PRIMARY KEY)' });
    const ids = () => f.db.prepare('SELECT id FROM entries ORDER BY id').all().map(row => row.id);
    const synced = [];
    f.storage.sync = async () => { synced.push(ids()); };
    const statements = [1, 2, 2, 3].map(id => ({ sql: 'INSERT INTO entries VALUES (?)', params: [id] }));
    const response = await f.fetch(statements, 'execute');
    assert.equal(response.status, 200);
    const answers = await response.json();
    assert.deepEqual(answers.map(answer => answer.success), [true, true, false]);
    assert.match(answers[2].error, /UNIQUE constraint failed/);
    assert.deepEqual(synced, [[1], [1, 2]]);
    assert.deepEqual(ids(), [1, 2]);
  } finally { f.close(); }
});

test('a threaded deadline returns the typed 503 and rolls back a statement or batch', async () => {
  const f = fixture({ wallDeadline: true });
  try {
    await f.query({ sql: 'CREATE TABLE entries(id)' });
    const reason = 'SQL_LIMIT_EXCEEDED: wall limit 30000ms exceeded';
    const error = Object.assign(new Error(reason), { code: 'SQL_LIMIT_EXCEEDED' });
    const statement = { sql: 'INSERT INTO entries VALUES (1)' };
    for (const body of [statement, [statement]]) {
      let checks = 0;
      f.storage.sql.checkExecutionDeadline = () => { if (++checks === 2) throw error; };
      const response = await f.fetch(body);
      assert.equal(response.status, 503);
      assert.deepEqual(await response.json(), { success: false, error: reason, code: error.code, reason });
      assert.equal(f.db.prepare('SELECT count(*) AS count FROM entries').get().count, 0);
    }
  } finally { f.close(); }
});

test('a threaded disconnect is passed to the runtime as the original exception', async () => {
  const f = fixture({ wallDeadline: true });
  try {
    const error = Object.assign(new Error('SQL client disconnected'), { code: 'SQL_CLIENT_DISCONNECTED' });
    f.storage.sql.checkExecutionDeadline = () => { throw error; };
    const statement = { sql: 'SELECT 1' };
    await assert.rejects(f.fetch(statement), actual => actual === error);
    await assert.rejects(f.fetch([statement]), actual => actual === error);
  } finally { f.close(); }
});

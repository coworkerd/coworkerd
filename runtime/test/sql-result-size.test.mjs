import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import vm from 'node:vm';
import { DatabaseSync } from 'node:sqlite';

const source = await readFile(new URL('../sql-database.js', import.meta.url), 'utf8');
const classSource = source.replace(/^import .*;$/m, '').replace(/^export /gm, '');
function fixture() {
  const db = new DatabaseSync(':memory:');
  const sqlite = {
    checkExecutionDeadline() {},
    get databaseSize() { return db.prepare('PRAGMA page_count').get().page_count * 4096; },
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
    ArrayBuffer, Uint8Array, Request, Response, URL, TextEncoder,
  });
  const worker = new C({ storage });
  return { db, worker, close: () => db.close(), async query(body, endpoint = 'query', format = 'ARRAY_OF_OBJECTS') {
    return (await worker.fetch(new Request(`http://sql/${endpoint}?resultsFormat=${format}`, { method: 'POST', body: JSON.stringify(body) }))).json();
  } };
}


const MAX=7340032;
test('complete response bytes at the exact limit in both formats, then one more row fails',async()=>{
 const f=fixture();
 try{
  for(const format of ['ARRAY_OF_OBJECTS','ROWS_AND_COLUMNS']){
   const sql="WITH n(i) AS (VALUES(1),(2)) SELECT CASE i WHEN 1 THEN printf('%0*d',?,0) ELSE 'x' END AS v FROM n LIMIT ?";
   const baseline=await f.query({sql,params:[1,1]},'query',format);
   const payload=MAX-(Buffer.byteLength(JSON.stringify(baseline))-1);
   let counted;const original=f.worker.resultResponse;
   f.worker.resultResponse=function(answer,budget){counted=budget.bytes;return original.call(this,answer,budget)};
   const answer=await f.query({sql,params:[payload,1]},'query',format);
   assert.equal(answer.success,true);assert.equal(Buffer.byteLength(JSON.stringify(answer)),MAX);assert.equal(counted,MAX);
   f.worker.resultResponse=original;
   const rejected=await f.query({sql,params:[payload,2]},'query',format);
   assert.equal(rejected.success,false);assert.match(rejected.error,/^D1_RESULT_TOO_LARGE:.*7340032.*LIMIT/);
  }
 }finally{f.close()}
});

test('the complete array envelope is limited and every write in a rejected batch rolls back',async()=>{
 const f=fixture();
 try{
  await f.query({sql:'CREATE TABLE marker(n)'});
  const large={sql:"SELECT printf('%0*d',?,0) AS v",params:[3*1024*1024]};
  const answer=await f.query([{sql:'INSERT INTO marker VALUES(1)'},large,large,large]);
  assert.equal(answer.length,1);assert.equal(answer[0].success,false);assert.match(answer[0].error,/D1_RESULT_TOO_LARGE/);
  assert.deepEqual((await f.query({sql:'SELECT * FROM marker'})).results,[]);
 }finally{f.close()}
});

test('oversized INSERT RETURNING rolls back and the same database remains usable',async()=>{
 const f=fixture();
 try{
  await f.query({sql:'CREATE TABLE written(v)'});
  const answer=await f.query({sql:"WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<128) INSERT INTO written SELECT printf('%0*d',65536,0) FROM n RETURNING v"});
  assert.equal(answer.success,false);assert.match(answer.error,/D1_RESULT_TOO_LARGE/);
  assert.deepEqual((await f.query({sql:'SELECT count(*) AS n FROM written'})).results,[{n:0}]);
  assert.equal((await f.query({sql:"INSERT INTO written VALUES('after')"})).success,true);
 }finally{f.close()}
});

test('after overflow the cursor is exhausted without encoding subsequent rows',async()=>{
 let drained=0,encodedAfter=false;
 const sqlite={databaseSize:0,checkExecutionDeadline(){},prepare(sql){return()=>({columnNames:['v'],rowsRead:4,rowsWritten:0,raw:function*(){
  if(sql.includes('total_changes()')){yield[0,0];return}
  if(sql==='oversized'){
   drained++;yield['x'.repeat(MAX)];
   for(let i=0;i<3;i++){drained++;yield[{toJSON(){encodedAfter=true;throw new Error('must not encode after overflow')}}]}
  }else yield[1];
 }})}};
 const C=vm.runInNewContext(classSource+'; SqlDatabase',{DurableObject:class{constructor(ctx){this.ctx=ctx}},ArrayBuffer,Uint8Array,Request,Response,URL,TextEncoder});
 const worker=new C({storage:{sql:sqlite,async sync(){},transactionSync:fn=>fn()}});
 const request=sql=>new Request('http://sql/query',{method:'POST',body:JSON.stringify({sql})});
 const response=await worker.fetch(request('oversized')),answer=await response.json();
 assert.equal(response.status,200);assert.match(answer.error,/D1_RESULT_TOO_LARGE/);assert.equal(drained,4);assert.equal(encodedAfter,false);
 assert.equal((await(await worker.fetch(request('small'))).json()).success,true);
});

test('execute caps its complete array reply while preserving earlier committed writes', async () => {
  const f = fixture();
  try {
    await f.query({ sql: 'CREATE TABLE marker(n)' });
    const large = { sql: "SELECT printf('%0*d',?,0) AS v", params: [3 * 1024 * 1024] };
    const answer = await f.query([{ sql: 'INSERT INTO marker VALUES(1)' }, large, large, large], 'execute');
    assert.equal(answer.length, 4);
    assert.equal(answer[0].success, true);
    assert.equal(answer[1].success, true);
    assert.equal(answer[2].success, true);
    assert.equal(answer[3].success, false);
    assert.match(answer[3].error, /^D1_RESULT_TOO_LARGE:/);
    assert.ok(Buffer.byteLength(JSON.stringify(answer)) <= MAX);
    assert.deepEqual((await f.query({ sql: 'SELECT n FROM marker' })).results, [{ n: 1 }]);
  } finally { f.close(); }
});

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, unlink, writeFile } from 'node:fs/promises';
import { availableParallelism, cpus } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { eventually, json, request } from './cluster.mjs';

const cli = fileURLToPath(new URL('../../cli/bin/cowctl.mjs', import.meta.url));
const slow = 'WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n<1000000000) SELECT sum(n) AS total FROM r';
const missingTable = 'SELECT * FROM missing_table';
const oversizedResult = 'SELECT zeroblob(4000000)';
const automaticRollbacks = [
  'INSERT OR ROLLBACK INTO ledger VALUES(1)',
  'INSERT INTO rollback_conflict VALUES(1)',
  'INSERT INTO rollback_trigger VALUES(1)',
];

async function database(cluster, tenant, name, { create = true, warm = true } = {}) {
  const path = `${tenant.path}/databases/${name}`;
  const { database: { namespaceId } } = await cluster.api(path, { method: create ? 'POST' : 'GET', token: tenant.token });
  const raw = (body, format = 'ARRAY_OF_OBJECTS', timeout = 40000) =>
    request(cluster.urls.apiserver, `${path}/query?resultsFormat=${format}`,
      { method: 'POST', token: tenant.token, body, timeout });
  const query = async (body, format) => json(await raw(body, format), 200, name);
  const ready = async () => assert.deepEqual((await query({ sql: 'SELECT 1 AS n' })).results, [{ n: 1 }]);
  if (warm) await eventually(`${tenant.name}/${name} is ready`, ready);
  return { name, namespaceId, path, raw, query, ready };
}

const put = (id) => ({ sql: 'INSERT INTO ledger VALUES(?)', params: [id] });

async function bindingProbe(cluster, tenant, name) {
  await cluster.deploy(tenant, 'sql-binding', `
    export default { async fetch(request, { DB }) {
      const url = new URL(request.url);
      try {
        if (url.pathname.endsWith('/exec')) {
          return Response.json(await DB.exec(
            'INSERT INTO ledger VALUES(3);\\nINSERT INTO ledger VALUES(1);\\nINSERT INTO ledger VALUES(4);'));
        }
        if (url.pathname.endsWith('/unconfirmed')) {
          return Response.json(await DB.prepare('INSERT INTO ledger VALUES(97)').run());
        }
        if (url.pathname.endsWith('/read-error')) {
          return Response.json(await DB.prepare(${JSON.stringify(missingTable)}).all());
        }
        if (url.pathname.endsWith('/batch-error')) {
          const insert = DB.prepare('INSERT INTO ledger VALUES(?)');
          return Response.json(await DB.batch([insert.bind(9), insert.bind(1)]));
        }
        if (url.pathname.endsWith('/size-error')) {
          return Response.json(await DB.prepare(${JSON.stringify(oversizedResult)}).all());
        }
        if (url.pathname.endsWith('/quota-growth')) {
          return Response.json(await DB.prepare('INSERT INTO ledger(payload) VALUES(zeroblob(131072))').run());
        }
        if (url.pathname.endsWith('/auto-rollback')) {
          const variant = Number(url.searchParams.get('variant'));
          return Response.json(await DB.exec([
            'INSERT INTO rollback_prefix VALUES(' + variant + ')',
            ${JSON.stringify(automaticRollbacks)}[variant],
            'INSERT INTO rollback_prefix VALUES(' + (variant + 10) + ')',
          ].join('\\n')));
        }
        if (url.pathname.endsWith('/timeout')) return Response.json(await DB.prepare(${JSON.stringify(slow)}).raw());
        return Response.json(await DB.prepare('SELECT id FROM ledger ORDER BY id').all());
      } catch (error) {
        return Response.json({ message: error.message, cause: error.cause?.message }, { status: 503 });
      }
    } };
  `, { wrangler: {
    d1_databases: [{ binding: 'DB', database_name: name, database_id: '00000000-0000-4000-8000-000000000001' }],
  } });
  return (suffix = '', timeout = 40000) => request(cluster.urls[cluster.nodes[0]],
    `/${tenant.name}/sql-binding${suffix}`, { timeout });
}

// Both backends execute this contract through the same public paths.
export async function sqlProtocol(cluster) {
  const tenant = await cluster.tenant('sql-protocol');
  const db = await database(cluster, tenant, 'protocol');
  assert.equal((await db.query({ sql: 'CREATE TABLE ledger(id INTEGER PRIMARY KEY)' })).success, true);
  const results = await db.query([put(1), put(2)]);
  assert.deepEqual(results.map(({ meta }) => [meta.changes, meta.last_row_id, meta.changed_db]), [[1, 1, true], [1, 2, true]]);
  assert.ok(results.every(({ meta }) => meta.size_after > 0 && meta.rows_written === 1));
  const typed = { sql: 'SELECT ? AS a, ? AS a, ? AS note, ? AS missing', params: [3, [0, 255], '🐝\u0000\n', null] };
  assert.deepEqual((await db.query(typed)).results, [{ a: [0, 255], note: '🐝\u0000\n', missing: null }]);
  assert.deepEqual((await db.query(typed, 'ROWS_AND_COLUMNS')).results,
    { columns: ['a', 'a', 'note', 'missing'], rows: [[3, [0, 255], '🐝\u0000\n', null]] });
  assert.match((await db.query({ sql: 'SELECT ?', params: [[256]] })).error, /D1_TYPE_ERROR/);
  assert.match((await db.query({ sql: 'SELECT ?' })).error, /parameter bindings/);
  assert.match((await db.query({ sql: 'BEGIN' })).error, /state.storage.transaction/);
  assert.match((await db.query({ sql: 'CREATE TABLE _cf_test(x)' })).error, /prohibited|authorized|SQLITE_AUTH/);
  assert.match((await db.query([put(9), { sql: oversizedResult }]))[0].error, /D1_RESULT_TOO_LARGE/);
  assert.match((await db.query({ ingest: 'INSERT INTO ledger VALUES(9); INSERT INTO ledger VALUES(1);' })).error,
    /UNIQUE constraint failed/);
  const probe = await bindingProbe(cluster, tenant, db.name);
  const before = await eventually('D1 binding answers with the HTTP result', async () => {
    const answer = json(await probe(), 200, 'D1 binding');
    assert.deepEqual(answer.results, [{ id: 1 }, { id: 2 }]);
    return answer;
  });
  assert.equal(before.success, true);
  const rollbackSetup = [
    'CREATE TABLE rollback_prefix(id INTEGER PRIMARY KEY)',
    'CREATE TABLE rollback_conflict(id INTEGER PRIMARY KEY ON CONFLICT ROLLBACK)',
    'INSERT INTO rollback_conflict VALUES(1)',
    'CREATE TABLE rollback_trigger(id INTEGER PRIMARY KEY)',
    "CREATE TRIGGER reject_insert BEFORE INSERT ON rollback_trigger BEGIN SELECT RAISE(ROLLBACK, 'trigger rolled back'); END",
  ];
  assert.ok((await db.query(rollbackSetup.map(sql => ({ sql })))).every(result => result.success));
  for (const [variant, sql] of automaticRollbacks.entries()) {
    const expected = variant === 2 ? /trigger rolled back/ : /UNIQUE constraint failed/;
    const batch = await db.raw([put(9), { sql }]);
    const rejected = json(await probe('/auto-rollback?variant=' + variant), 503, 'D1 execute rollback');
    const prefix = (await db.query({ sql: 'SELECT id FROM rollback_prefix ORDER BY id' })).results;
    if (cluster.threadedNodes.size) {
      const [batchError] = json(batch, 200, 'native automatic rollback');
      assert.equal(batchError.success, false);
      assert.match(batchError.error, expected);
      assert.match(rejected.message, /^D1_EXEC_ERROR: Error in line 2:/);
      assert.match(rejected.cause, expected);
      assert.deepEqual(prefix, Array.from({ length: variant + 1 }, (_, id) => ({ id })));
    } else {
      // The legacy actor tries to roll back a savepoint SQLite already removed.
      assert.equal(json(batch, 500, 'legacy automatic rollback').code, 'EXECUTION_FAILED');
      assert.match(rejected.message, /^D1_EXEC_ERROR:/);
      assert.deepEqual(prefix, []);
    }
  }
  const readError = await db.query({ sql: missingTable });
  const [batchError] = await db.query([put(9), put(1)]);
  const sizeError = await db.query({ sql: oversizedResult });
  for (const [route, result, expected] of [
    ['/read-error', readError, /no such table.*missing_table/],
    ['/batch-error', batchError, /UNIQUE constraint failed/],
    ['/size-error', sizeError, /D1_RESULT_TOO_LARGE/],
  ]) {
    assert.equal(result.success, false);
    assert.match(result.error, expected);
    assert.deepEqual(json(await probe(route), 503, route), {
      message: `D1_ERROR: ${result.error}`,
      cause: result.error,
    });
  }
  assert.deepEqual(json(await probe(), 200, 'D1 retry after statement failures').results, [{ id: 1 }, { id: 2 }]);
  const rejected = json(await probe('/exec'), 503, 'D1 execute error');
  assert.match(rejected.message, /^D1_EXEC_ERROR: Error in line 2:/);
  assert.match(rejected.cause, /UNIQUE constraint failed/);
  assert.deepEqual((await db.query({ sql: 'SELECT * FROM ledger ORDER BY id' })).results,
    [{ id: 1 }, { id: 2 }, { id: 3 }]);

  // Exercise the same public disconnect against the JavaScript fallback and
  // the native home. Either way, losing the client leaves no partial batch.
  const abort = new AbortController();
  const disconnected = fetch(cluster.urls.apiserver + db.path + '/query', {
    method: 'POST',
    headers: { authorization: `Bearer ${tenant.token}`, 'content-type': 'application/json' },
    body: JSON.stringify([put(101), { sql: slow }]),
    signal: abort.signal,
  }).then(response => ({ response }), error => ({ error }));
  await delay(40);
  abort.abort();
  assert.equal((await disconnected).error?.name, 'AbortError');
  const afterDisconnect = await eventually('the database answers after a disconnected batch', async () => {
    const answer = await db.raw({ sql: 'SELECT id FROM ledger WHERE id = 101' }, 'ARRAY_OF_OBJECTS', 2500);
    return json(answer, 200, 'read after a disconnected batch');
  });
  assert.deepEqual(afterDisconnect.results, []);
}

export async function sqlCli(cluster) {
  const tenant = await cluster.tenant('sql-cli');
  const cwd = join(cluster.directory, 'sql-cli');
  await mkdir(join(cwd, 'migrations'), { recursive: true });
  await writeFile(join(cwd, 'cowctl.json'), JSON.stringify({ tenant: tenant.name, controlPlane: cluster.urls.apiserver }));
  await writeFile(join(cwd, 'migrations', '0001_create.sql'),
    "CREATE TABLE books(title TEXT PRIMARY KEY); INSERT INTO books VALUES ('one');");
  const run = (...args) => cluster.command(process.execPath, [cli, 'd1', ...args], { cwd, token: tenant.token });
  const source = await database(cluster, tenant, 'books');
  const migrationStarted = Date.now();
  await run('migrations', 'apply', source.name, '--yes');
  assert.deepEqual((await source.query({ sql: 'SELECT title FROM books' })).results, [{ title: 'one' }]);
  assert.equal((await source.query({ sql: 'SELECT count(*) AS n FROM d1_migrations' })).results[0].n, 1);
  await run('migrations', 'apply', source.name, '--yes');
  await run('export', source.name, '--output', 'backup.sql');
  const restored = await database(cluster, tenant, 'restored');
  await run('execute', restored.name, '--file', 'backup.sql', '--yes');
  for (const sql of ['SELECT title FROM books', 'SELECT name FROM d1_migrations ORDER BY name']) {
    assert.deepEqual((await source.query({ sql })).results, (await restored.query({ sql })).results);
  }

  const point = await eventually('the migration is in published SQL history', async () => {
    const { point } = JSON.parse(await run('time-travel', 'info', source.name, '--json'));
    assert.ok(point.bookmark);
    assert.ok(Number(point.writtenAtUnixMs) >= migrationStarted, JSON.stringify(point));
    return point;
  }, { timeout: 90000, interval: 1000 });
  assert.equal((await source.query({ sql: "INSERT INTO books VALUES ('two')" })).success, true);
  const rewound = 'rewound';
  const receipt = JSON.parse(await run('time-travel', 'restore', source.name, '--bookmark', point.bookmark,
    '--to', rewound, '--yes', '--json', '--timeout-ms', '50000'));
  assert.equal(receipt.phase, 'completed');
  assert.equal(receipt.point.bookmark, point.bookmark);
  const clone = await database(cluster, tenant, rewound, { create: false });
  const titles = { sql: 'SELECT title FROM books ORDER BY title' };
  assert.notEqual(clone.namespaceId, source.namespaceId);
  assert.deepEqual((await clone.query(titles)).results, [{ title: 'one' }]);
  assert.equal((await clone.query({ sql: "INSERT INTO books VALUES ('three')" })).success, true);
  assert.deepEqual((await clone.query(titles)).results, [{ title: 'one' }, { title: 'three' }]);
  assert.deepEqual((await source.query(titles)).results, [{ title: 'one' }, { title: 'two' }]);
}

function rows(log, prefix) {
  return log.split('\n').flatMap(line => {
    const index = line.indexOf(prefix);
    if (index === -1) return [];
    return [JSON.parse(line.slice(index + prefix.length))];
  });
}

async function acceptedOn(cluster, namespaceId, objectId) {
  const log = await cluster.compose(['logs', '--no-color', '--tail', '150']);
  const matching = rows(log, '[do] ').filter(row => row.namespaceId === namespaceId.slice(0, 12) &&
    (!objectId || row.objectId === objectId) && row.status === 200 && row.processId);
  assert.ok(matching.length, `no owning process recorded for ${namespaceId}`);
  return matching.at(-1);
}

export async function sqlConfirmationAndTakeover(cluster) {
  const tenant = await cluster.tenant('sql-takeover');
  const db = await database(cluster, tenant, 'books');
  assert.equal((await db.query({ sql: 'CREATE TABLE ledger(id INTEGER PRIMARY KEY)' })).success, true);
  let owner = (await acceptedOn(cluster, db.namespaceId)).nodeId;
  const kept = [];
  for (let i = 0; i < 8; ++i) {
    assert.equal((await db.query(put(i))).success, true);
    kept.push({ id: i });
  }

  // The peers can hold positive acknowledgements independently of the owner.
  const hold = join(cluster.peerAckGate, 'hold');
  await writeFile(hold, '');
  try {
    const refused = json(await db.raw(put(98)), 503, 'SQL WAL confirmation timeout');
    assert.equal(refused.code, 'STORAGE_UNAVAILABLE');
  } finally {
    await unlink(hold);
  }
  owner = await eventually('the database reopens without the unconfirmed write', async () => {
    assert.deepEqual((await db.query({ sql: 'SELECT id FROM ledger ORDER BY id' })).results, kept);
    return (await acceptedOn(cluster, db.namespaceId)).nodeId;
  });
  const call = await bindingProbe(cluster, tenant, db.name);
  await eventually('the D1 binding reads the confirmed SQL prefix', async () => {
    assert.deepEqual(json(await call(), 200, 'D1 binding').results, kept);
  });
  await writeFile(hold, '');
  try {
    const refused = json(await call('/unconfirmed'), 503, 'D1 WAL confirmation timeout');
    assert.match(refused.message, /^D1_ERROR: STORAGE_UNAVAILABLE:/);
  } finally {
    await unlink(hold);
  }
  owner = await eventually('the D1 write is excluded before the next owner test', async () => {
    assert.deepEqual((await db.query({ sql: 'SELECT id FROM ledger ORDER BY id' })).results, kept);
    return (await acceptedOn(cluster, db.namespaceId)).nodeId;
  });
  const peers = cluster.nodes.filter(node => node !== owner);
  await eventually('the two survivors released any held acknowledgements', async () => {
    const observations = await Promise.all(peers.map(node => cluster.state(node)));
    assert.ok(observations.every(state => state.durableLog.peers.heldAcksForTest === 0));
  });
  let settled = false;
  await writeFile(hold, '');
  const pending = db.raw(put(99)).then(response => ({ response }), error => ({ error }))
    .finally(() => { settled = true; });
  try {
    await eventually('both peers held a database append', async () => {
      const states = await Promise.all(peers.map(node => cluster.state(node)));
      assert.ok(states.every(state => state.durableLog.peers.heldAcksForTest > 0));
    }, { interval: 20, timeout: 3000 });
    assert.equal(settled, false, 'database replied before WAL confirmation');
    await cluster.compose(['kill', '-s', 'SIGKILL', owner]);
  } finally {
    await unlink(hold);
  }
  const lost = await pending;
  if (lost.error) throw lost.error;
  assert.equal(json(lost.response, 503, 'database response lost on owner death').code, 'RESULT_UNCERTAIN');
  const recovered = await eventually('the next owner recovered every confirmed SQL write', async () => {
    const result = await db.query({ sql: 'SELECT id FROM ledger ORDER BY id' });
    assert.deepEqual(result.results.filter(({ id }) => id !== 99), kept);
    const placement = await acceptedOn(cluster, db.namespaceId);
    assert.notEqual(placement.nodeId, owner);
    return placement;
  }, { timeout: 90000 });
  console.log('SQL takeover', JSON.stringify({ previous: owner, next: recovered.nodeId, confirmedRows: kept.length }));
}

const sameProcess = (a, b) => a.nodeId === b.nodeId && a.processId === b.processId &&
  a.processGeneration === b.processGeneration;

async function colocatedDatabase(cluster, tenant, placement) {
  for (let i = 0; i < 30; ++i) {
    const db = await database(cluster, tenant, `probe-${i}`);
    if (sameProcess(await acceptedOn(cluster, db.namespaceId), placement)) return db;
  }
  assert.fail(`could not place ${tenant.name}'s probe in ${placement.nodeId}/${placement.processId}`);
}

async function prepareColocatedObject(cluster, tenant, placement, name = 'sql-neighbor') {
  await cluster.deploy(tenant, name, `
    import { DurableObject } from 'cloudflare:workers';
    export class Probe extends DurableObject {
      fetch() { return new Response(this.ctx.id.toString()); }
    }
    export default { async fetch(request, env) {
      try {
        return await env.PROBES.getByName(new URL(request.url).searchParams.get('key')).fetch(request);
      } catch (error) {
        return Response.json({ code: error.code, message: error.message }, { status: 503 });
      }
    } };
  `, { wrangler: {
    durable_objects: { bindings: [{ name: 'PROBES', class_name: 'Probe' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Probe'] }],
  } });
  const { namespaceId } = await cluster.api(`${tenant.path}/workers/${name}/namespaces/Probe`, { token: tenant.token });
  return async () => {
    for (let i = 0; i < 30; ++i) {
      const run = () => request(cluster.urls[placement.nodeId], `/${tenant.name}/${name}?key=${i}`);
      const id = await eventually('Durable Object probe is available', async () => {
        const answer = await run();
        assert.equal(answer.status, 200, answer.text);
        assert.match(answer.text, /^[\da-f]{64}$/);
        return answer.text;
      }, { timeout: 5000 });
      const objectId = id.slice(0, 12);
      if (sameProcess(await acceptedOn(cluster, namespaceId, objectId), placement)) {
        return { namespaceId, objectId, check: async () => assert.deepEqual(await run(), { status: 200, text: id }) };
      }
    }
    assert.fail('could not colocate a Durable Object with the blocking database');
  };
}

export async function sqlResourceBounds(cluster) {
  const tenant = await cluster.tenant('sql-capacity');
  const other = await cluster.tenant('sql-capacity-neighbor');
  const databases = await Promise.all(Array.from({ length: 9 }, (_, i) =>
    database(cluster, tenant, 'candidate-' + i, { warm: false })));
  const { storage: { tenantsRevision } } = await cluster.api('/v1/status');
  await eventually('every node sees the databases and the same durable members', async () => {
    const states = await Promise.all(cluster.nodes.map(node => cluster.state(node)));
    for (const { storage, membership } of states) {
      assert.ok(BigInt(storage.appliedTenantsRevision) >= BigInt(tenantsRevision));
      const eligible = membership.members.filter(member => member.state === 'active' && member.durableProcesses > 0);
      assert.deepEqual(eligible.map(member => member.nodeId).sort(), [...cluster.nodes].sort());
    }
  });
  // Cold databases have one shard. Group them by the node that would claim
  // shard zero so the saturation check always hits the blocking process.
  const groups = new Map(cluster.nodes.map(node => [node, []]));
  for (const db of databases) {
    const scored = cluster.nodes.map(node => ({ node,
      score: createHash('sha256').update(`${db.namespaceId}/0\0${node}`).digest().readBigUInt64BE() }));
    scored.sort((a, b) => a.score === b.score ? a.node.localeCompare(b.node) : a.score > b.score ? -1 : 1);
    groups.get(scored[0].node).push(db);
  }
  const [targetNode, [holder, ...candidates]] = [...groups].sort((a, b) => b[1].length - a[1].length)[0];
  const schema = { sql: 'CREATE TABLE ledger(id INTEGER PRIMARY KEY, payload BLOB)' };
  await eventually('the blocking database is ready', holder.ready);
  const placement = await acceptedOn(cluster, holder.namespaceId);
  assert.equal(placement.nodeId, targetNode);
  // Before deploying the Durable Object, these node-wide open and process
  // memory gauges cover only the newly opened database and its first read.
  const { durableLog, processes } = await cluster.state(placement.nodeId);
  const { openSamples, maxOpenNs } = durableLog.threadedHomeIo;
  const child = processes.entries.find(entry => entry.processId === placement.processId);
  const { homes, databaseHomes, maxHomeMemoryBytes, nativeDatabaseReservedBytes } = child.lifecycle.durableAdmission;
  console.log('Native SQL cold open', JSON.stringify({ openSamples, maxOpenNs, homes, databaseHomes,
    sqliteAllocatedBytes: maxHomeMemoryBytes, sqliteReservedBytes: nativeDatabaseReservedBytes,
    processResidentBytes: child.residentBytes }));
  assert.equal((await holder.query(schema)).success, true);
  const placeNeighbor = await prepareColocatedObject(cluster, other, placement);
  await holder.ready();
  const location = `/var/lib/coworkerd/do/coworkerd-do-root-v1-${holder.namespaceId}`;
  const disk = async () => {
    const listing = await cluster.compose(
      ['exec', '-T', placement.nodeId, 'find', location, '-maxdepth', '1', '-type', 'f', '-printf', '%f\t%s\t%b\n']);
    const totals = { logical: 0, allocated: 0, walLogical: 0 };
    for (const line of listing.split('\n').filter(Boolean)) {
      const [name, bytes, blocks] = line.split('\t');
      totals.logical += Number(bytes);
      totals.allocated += Number(blocks) * 512;
      if (name.endsWith('.sqlite-wal')) totals.walLogical += Number(bytes);
    }
    return totals;
  };
  const resources = async () => {
    const { processes } = await cluster.state(placement.nodeId);
    const owner = processes.entries.find(entry => entry.processId === placement.processId);
    assert.equal(owner.generation, placement.processGeneration);
    const descriptors = await cluster.compose(['exec', '-T', placement.nodeId, 'ls', '-1', `/proc/${owner.pid}/fd`]);
    const { databaseHomes, nativeDatabaseReservedBytes, nativeSqlSpillBytes,
      nativeSqlSpillMaximum, reservedDescriptors, running } = owner.lifecycle.durableAdmission;
    return { databaseHomes, nativeDatabaseReservedBytes, nativeSqlSpillBytes, nativeSqlSpillMaximum,
      reservedDescriptors, running,
      residentBytes: owner.residentBytes, openDescriptors: descriptors.trim().split('\n').length };
  };
  const before = await disk();
  // Keep sampling every named file, while checking the WAL's rollback
  // independently. Reopening the home can append small bookkeeping records.
  const assertSpillReclaimed = (sample) => {
    assert.ok(sample.walLogical <= before.walLogical && sample.allocated <= before.allocated,
      JSON.stringify({ before, sample }));
  };
  const idle = await resources();
  const abort = new AbortController();
  let settled = false;
  const busy = Array.from({ length: 4 }, () =>
    ({ sql: 'INSERT INTO ledger(payload) VALUES(randomblob(2097152))' }));
  const pending = fetch(cluster.urls.apiserver + holder.path + '/query', {
    method: 'POST',
    headers: { authorization: `Bearer ${tenant.token}`, 'content-type': 'application/json' },
    body: JSON.stringify([...busy, { sql: slow }]),
    signal: abort.signal,
  }).then(response => ({ response }), error => ({ error })).finally(() => { settled = true; });
  let during, saturated;
  const attempts = [];
  try {
    await delay(1200);
    await eventually('the database home is executing the staged transaction', async () => {
      assert.equal((await resources()).running, 1);
    }, { timeout: 5000, interval: 100 });
    during = await disk();
    assert.equal(settled, false, 'the staging transaction ended before the disk sample');
    const walGrowth = during.walLogical - before.walLogical;
    assert.ok(walGrowth > 0 && walGrowth <= cluster.nativeSqlDatabaseSpillBytes,
      JSON.stringify({ before, during }));
    for (const candidate of candidates) {
      const answer = await candidate.raw({ sql: 'SELECT 1' });
      const { code } = json(answer, 503, `database ${candidate.name} is refused while the home is occupied`);
      assert.match(code, /^(OVERLOADED|DO_UNAVAILABLE)$/);
      attempts.push({ database: candidate.name, status: answer.status, code });
    }
    await (await placeNeighbor()).check();
    saturated = await resources();
    assert.equal(saturated.databaseHomes, 1);
    assert.ok(saturated.nativeDatabaseReservedBytes > 0 && saturated.nativeDatabaseReservedBytes < 32 * 1024 * 1024);
    assert.ok(saturated.nativeSqlSpillBytes > 0 && saturated.nativeSqlSpillBytes <= cluster.nativeSqlDatabaseSpillBytes);
    assert.equal(settled, false, "another tenant's Durable Object was not served during the database request");
  } finally {
    abort.abort();
  }
  assert.equal((await pending).error?.name, 'AbortError');
  const disconnectedAt = performance.now();
  const recovered = await eventually('the database serves its next request before the original deadline', async () => {
    const retry = await holder.raw({ sql: 'SELECT id FROM ledger' }, 'ARRAY_OF_OBJECTS', 1500);
    return json(retry, 200, 'read after disconnect');
  }, { timeout: 6000, interval: 100 });
  assert.deepEqual(recovered.results, []);
  const retryMs = performance.now() - disconnectedAt;
  const after = await disk();
  assertSpillReclaimed(after);
  const recoveredProcess = await eventually('the child reports the SQL home idle again', async () => {
    const snapshot = await resources();
    assert.equal(snapshot.running, 0);
    assert.equal(snapshot.nativeSqlSpillBytes, 0);
    return snapshot;
  });
  console.log('Native SQL resource bounds', JSON.stringify({ attempts, retryMs,
    disk: { before, during, after }, process: { idle, saturated, recovered: recoveredProcess } }));

  const declined = await holder.raw(Array.from({ length: 10 }, () => busy[0]));
  assert.equal(json(declined, 507, 'oversized transaction').code, 'SQL_SPILL_LIMIT_EXCEEDED');
  assert.deepEqual((await holder.query({ sql: 'SELECT id FROM ledger' })).results, []);
  const afterRefusal = await disk();
  assertSpillReclaimed(afterRefusal);

  const hold = join(cluster.peerAckGate, 'hold');
  const peers = cluster.nodes.filter(node => node !== placement.nodeId);
  const insertId = id => ({ sql: 'INSERT INTO ledger(id) VALUES(?)', params: [id] });
  await writeFile(hold, '');
  const first = holder.raw(insertId(98)).then(response => ({ response }), error => ({ error }));
  let queued;
  try {
    await eventually('both peers are holding a database commit', async () => {
      const observations = await Promise.all(peers.map(node => cluster.state(node)));
      assert.ok(observations.every(state => state.durableLog.peers.heldAcksForTest > 0));
    }, { interval: 20, timeout: 2000 });
    queued = holder.raw([insertId(99), { sql: slow }])
      .then(response => ({ response }), error => ({ error }));
    await delay(2500);
  } finally {
    await unlink(hold);
  }
  const committed = await first;
  if (committed.error) throw committed.error;
  assert.equal(json(committed.response, 200, 'previous confirmed transaction').success, true);
  const queuedResult = await queued;
  if (queuedResult.error) throw queuedResult.error;
  assert.equal(json(queuedResult.response, 503, 'queued SQL deadline').code, 'SQL_LIMIT_EXCEEDED');
  const queuedTiming = await eventually('the child reports the queued deadline', async () => {
    const log = await cluster.compose(['exec', '-T', placement.nodeId, 'grep', '-F', '[native-sql] ',
      '/var/lib/coworkerd/logs/workerd.log']);
    const event = rows(log, '[native-sql] ').findLast(row => row.namespaceId === holder.namespaceId.slice(0, 12)
      && row.status === 503);
    assert.ok(event);
    return event;
  });
  assert.ok(queuedTiming.finishedMonotonicNs - queuedTiming.startedMonotonicNs < 29e9,
    JSON.stringify(queuedTiming));
  assert.deepEqual((await holder.query({ sql: 'SELECT id FROM ledger' })).results, [{ id: 98 }]);

  const insert = size => ({ sql: 'INSERT INTO ledger(payload) VALUES(zeroblob(?))', params: [size] });
  for (const [name, quota, size, status, code] of [
    ['object-limit', undefined, 1258291, 413, 'DO_OBJECT_TOO_LARGE'],
    ['tenant-quota', { durableObjectBytes: 262144 }, 524288, 422, 'QUOTA_EXCEEDED'],
  ]) {
    const limitedTenant = await cluster.tenant(name, quota);
    const db = await database(cluster, limitedTenant, 'books');
    assert.equal((await db.query(schema)).success, true);
    const call = await bindingProbe(cluster, limitedTenant, db.name);
    await eventually('the D1 binding sees an empty database', async () => {
      assert.deepEqual(json(await call(), 200, name).results, []);
    });
    assert.equal((await db.query(insert(size))).success, true);
    const owner = (await acceptedOn(cluster, db.namespaceId)).nodeId;
    await eventually('the owner has accounted for the confirmed file', async () => {
      const { durableLog } = await cluster.state(owner);
      const usage = durableLog.storage.tenants.find(entry => entry.tenant === limitedTenant.name);
      assert.ok(Number(usage?.bytesHere) > (quota?.durableObjectBytes ?? 1024 * 1024));
    });
    assert.equal(json(await db.raw(insert(131072)), status, name).code, code);
    await eventually('confirmed rows are readable after the HTTP refusal', async () => {
      assert.deepEqual((await db.query({ sql: 'SELECT id FROM ledger' })).results, [{ id: 1 }]);
    });
    const binding = json(await call('/quota-growth'), 503, name);
    assert.ok(binding.message.startsWith(`D1_ERROR: ${code}:`), binding.message);
    assert.ok(binding.cause.startsWith(`${code}:`), binding.cause);
    await eventually('confirmed rows are readable after the D1 refusal', async () => {
      assert.deepEqual((await db.query({ sql: 'SELECT id FROM ledger' })).results, [{ id: 1 }]);
    });
  }
}

async function soleDatabaseProcess(cluster, database) {
  const [node] = cluster.durableOwners;
  const placement = await acceptedOn(cluster, database.namespaceId);
  assert.equal(placement.nodeId, node);
  const { storage: { tenantsRevision } } = await cluster.api('/v1/status');
  const state = async () => {
    const { membership, processes, storage } = await cluster.state(node);
    assert.ok(BigInt(storage.appliedTenantsRevision) >= BigInt(tenantsRevision));
    assert.deepEqual(membership.members.filter(member => member.state === 'active' && member.durableProcesses > 0)
      .map(member => member.nodeId), [node]);
    const child = processes.entries.find(entry => entry.processId === placement.processId);
    assert.equal(child.generation, placement.processGeneration);
    assert.equal(child.sandbox.durableServer, 'threaded');
    return { ...child.lifecycle.durableAdmission, residentBytes: child.residentBytes };
  };
  await eventually('the sole durable owner sees the current tenant index', state);
  return { node, placement, state };
}

function abortableQuery(cluster, tenant, database, statements, abort) {
  return fetch(cluster.urls.apiserver + database.path + '/query', {
    method: 'POST', headers: { authorization: `Bearer ${tenant.token}`, 'content-type': 'application/json' },
    body: JSON.stringify(statements), signal: abort.signal,
  }).then(response => ({ response }), error => ({ error }));
}

export async function sqlProcessSpill(cluster) {
  const tenant = await cluster.tenant('sql-process-spill');
  const first = await database(cluster, tenant, 'first');
  const second = await database(cluster, tenant, 'second');
  const { state } = await soleDatabaseProcess(cluster, second);
  const createLedger = { sql: 'CREATE TABLE ledger(id INTEGER PRIMARY KEY, payload BLOB)' };
  for (const db of [first, second]) assert.equal((await db.query(createLedger)).success, true);
  const insert = { sql: 'INSERT INTO ledger(payload) VALUES(randomblob(2097152))' };
  const hold = (db, count, abort) => abortableQuery(cluster, tenant, db,
    [...Array.from({ length: count }, () => insert), { sql: slow }], abort);
  const abortFirst = new AbortController();
  const abortSecond = new AbortController();
  const runningFirst = hold(first, 3, abortFirst);
  let concurrent;
  try {
    await eventually('the first database is spilling without confirmation', async () => {
      const sample = await state();
      assert.equal(sample.running, 1);
      assert.ok(sample.nativeSqlSpillBytes > 4 * 1024 * 1024);
    });
    const runningSecond = hold(second, 2, abortSecond);
    try {
      concurrent = await eventually('two databases are simultaneously spilling in one process', async () => {
        const sample = await state();
        assert.equal(sample.running, 2);
        assert.ok(sample.nativeSqlSpillBytes > 8 * 1024 * 1024);
        assert.ok(sample.nativeSqlSpillBytes <= cluster.nativeSqlProcessSpillBytes);
        return sample;
      });
    } finally {
      abortSecond.abort();
      assert.equal((await runningSecond).error?.name, 'AbortError');
    }
    const empty = { sql: 'SELECT id FROM ledger' };
    await eventually('the second database rolls back after cancellation', async () => {
      const answer = await second.raw(empty, 'ARRAY_OF_OBJECTS', 1000);
      assert.deepEqual(json(answer, 200, 'second database after disconnect').results, []);
    }, { timeout: 4000, interval: 30 });
    const answer = await second.raw(Array.from({ length: 4 }, () => insert));
    assert.equal(json(answer, 507, 'process spill cap').code, 'SQL_SPILL_LIMIT_EXCEEDED');
    assert.deepEqual((await second.query(empty)).results, []);
  } finally {
    abortFirst.abort();
    assert.equal((await runningFirst).error?.name, 'AbortError');
  }
  await eventually('all spill credit is returned after rollback', async () => {
    assert.deepEqual((await first.query({ sql: 'SELECT id FROM ledger' })).results, []);
    assert.equal((await state()).nativeSqlSpillBytes, 0);
  }, { timeout: 6000 });
  const recovered = await state();
  console.log('Native SQL process spill', JSON.stringify({
    concurrentBytes: concurrent.nativeSqlSpillBytes, limitBytes: concurrent.nativeSqlSpillMaximum,
    concurrentDatabases: concurrent.running, recoveredBytes: recovered.nativeSqlSpillBytes,
  }));
}

export async function sqlWalAndTemporaryFiles(cluster) {
  const tenant = await cluster.tenant('sql-wal-and-temp');
  const db = await database(cluster, tenant, 'ledger');
  assert.equal((await db.query({ sql: 'CREATE TABLE ledger(payload BLOB)' })).success, true);
  const placement = await acceptedOn(cluster, db.namespaceId);
  const state = () => cluster.state(placement.nodeId);
  const ownerProcess = async () => {
    const { processes } = await state();
    const owner = processes.entries.find(entry => entry.processId === placement.processId);
    assert.equal(owner.generation, placement.processGeneration);
    return owner;
  };
  const before = (await state()).durableLog;
  const cap = BigInt(before.settings.recordLimitBytes);
  const insert = size => ({ sql: 'INSERT INTO ledger VALUES(zeroblob(?))', params: [size] });
  const common = Array.from({ length: 31 }, () => insert(2 * 1024 * 1024));
  const near = await db.query([...common, insert(1024 * 1024)]);
  assert.equal(near.length, 32);
  assert.ok(near.every(row => row.success));
  const confirmed = (await state()).durableLog;
  const encodedBytes = BigInt(confirmed.log.bytes) - BigInt(before.log.bytes);
  assert.ok(encodedBytes <= cap && encodedBytes > cap - 1024n * 1024n,
    `replicated record of ${encodedBytes} bytes should be just below ${cap}`);
  assert.equal(confirmed.confirm.confirmations, before.confirm.confirmations + 1);
  const acknowledgedPeers = confirmed.peers.streams.filter(peer =>
    BigInt(peer.ackedSeq) >= BigInt(confirmed.log.nextSeq));
  assert.ok(acknowledgedPeers.length > 0, 'a peer confirmed the near-limit record');

  const walPath = `/var/lib/coworkerd/do/coworkerd-do-root-v1-${db.namespaceId}`;
  const walBytes = async () => {
    const text = await cluster.compose(['exec', '-T', placement.nodeId, 'find', walPath,
      '-maxdepth', '1', '-name', '*.sqlite-wal', '-printf', '%s\n']);
    return text.split('\n').filter(Boolean).reduce((total, size) => total + Number(size), 0);
  };
  const beforeRefusal = await walBytes();
  const over = await db.raw([...common, insert(2 * 1024 * 1024)]);
  assert.equal(json(over, 422, 'WAL transaction above the replication limit').code, 'SQL_TRANSACTION_TOO_LARGE');
  assert.deepEqual((await db.query({ sql: 'SELECT count(*) AS rows, sum(length(payload)) AS bytes FROM ledger' })).results,
    [{ rows: 32, bytes: 63 * 1024 * 1024 }]);
  const after = (await state()).durableLog;
  assert.equal(after.log.records, confirmed.log.records);
  assert.equal(after.recovery.recordLimitRefusals, confirmed.recovery.recordLimitRefusals);
  assert.equal(after.threadedHomeIo.openSamples, confirmed.threadedHomeIo.openSamples);
  assert.ok(await walBytes() <= beforeRefusal);
  assert.equal((await ownerProcess()).lifecycle.durableAdmission.nativeSqlSpillBytes, 0);

  // The LIMIT preserves the sorted subquery as a coroutine. Its temporary
  // b-tree stays open while the first output row evaluates a correlated sum.
  const sort = {
    sql: `WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x<12288)
      SELECT ordered.x, (WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n<1000000000)
        SELECT sum(n) + ordered.x FROM r)
      FROM (SELECT x FROM seq ORDER BY randomblob(8192) LIMIT 10000) AS ordered LIMIT 1`,
  };
  const abort = new AbortController();
  const pending = abortableQuery(cluster, tenant, db, sort, abort);
  let held;
  try {
    held = await eventually('SQLite has a live temporary sort file on durable storage', async () => {
      const { pid, lifecycle: { durableAdmission } } = await ownerProcess();
      assert.equal(durableAdmission.running, 1);
      assert.ok(durableAdmission.nativeSqlSpillBytes > 1024 * 1024);
      const listing = await cluster.compose(['exec', '-T', placement.nodeId, 'ls', '-l', `/proc/${pid}/fd`]);
      // Linux gives unnamed O_TMPFILE inodes a #number, while the portable
      // implementation creates and immediately unlinks a named etilqs file.
      const temporary = listing.split('\n').map(line => line.split(' -> ')[1] ?? '')
        .filter(path => /\/(?:etilqs_[^/ ]+|#\d+) \(deleted\)$/.test(path));
      assert.ok(temporary.length > 0, listing);
      assert.ok(temporary.every(path => path.startsWith('/var/lib/coworkerd/do/')), temporary.join('\n'));
      return { bytes: durableAdmission.nativeSqlSpillBytes, locations: temporary };
    }, { timeout: 10000, interval: 100 });
  } finally {
    abort.abort();
    assert.equal((await pending).error?.name, 'AbortError');
  }
  await eventually('the interrupted sorter releases its spill', async () => {
    assert.equal((await ownerProcess()).lifecycle.durableAdmission.nativeSqlSpillBytes, 0);
    await db.ready();
  }, { timeout: 6000 });
  console.log('Native SQL WAL and temporary files', JSON.stringify({
    encodedBytes: String(encodedBytes), recordLimitBytes: String(cap),
    frameCapacityBytes: before.settings.nativeSqlWalFrameLimitBytes,
    acknowledgedPeers: acknowledgedPeers.map(peer => peer.nodeId),
    openSamples: after.threadedHomeIo.openSamples, tempSpill: held, releasedBytes: 0,
  }));
}

export async function sqlHomePressure(cluster) {
  const tenant = await cluster.tenant('sql-home-pressure', { durableObjectNamespaces: 1000 });
  const other = await cluster.tenant('sql-home-pressure-neighbor');
  const createStarted = performance.now();
  const databases = [];
  for (let offset = 0; offset < 1000; offset += 8) {
    databases.push(...await Promise.all(Array.from({ length: 8 }, (_, i) =>
      database(cluster, tenant, `db-${offset + i}`, { warm: false }))));
  }
  const creationMs = performance.now() - createStarted;
  const cross = await database(cluster, other, 'cross-tenant');
  const { node, placement, state } = await soleDatabaseProcess(cluster, cross);
  const placeFirst = await prepareColocatedObject(cluster, other, placement);
  const placeSecond = await prepareColocatedObject(cluster, other, placement, 'sql-second-class');

  const warm = [databases[0], databases[1], cross];
  for (const db of warm) await db.ready();
  const full = await eventually('database homes fill all but the Durable Object home', async () => {
    const sample = await state();
    assert.equal(sample.databaseHomes, sample.workerThreads - 1);
    assert.equal(sample.databaseHomes, warm.length);
    return sample;
  });
  const blockers = warm.map(db => {
    const abort = new AbortController();
    return { abort, running: abortableQuery(cluster, db === cross ? other : tenant, db, { sql: slow }, abort) };
  });
  try {
    await eventually('all database homes are executing', async () => {
      assert.equal((await state()).running, warm.length);
    }, { timeout: 5000, interval: 30 });
    const obj = await placeFirst();
    await obj.check();
    const before = await state();
    const answer = await request(cluster.urls[node], `/${other.name}/sql-second-class?key=0`);
    json(answer, 503, 'second Durable Object class with all database homes busy');
    await eventually('the child records a pre-execution capacity refusal', async () => {
      const sample = await state();
      assert.equal(sample.running, warm.length);
      assert.ok(sample.overloaded > before.overloaded);
    }, { timeout: 2000, interval: 30 });
  } finally {
    for (const { abort } of blockers) abort.abort();
    for (const { running } of blockers) assert.equal((await running).error?.name, 'AbortError');
  }

  // An interrupted home closes. Refill the database placements, then send
  // the first successful call to the other class while all three are idle.
  for (const db of warm) await eventually(`reopen ${db.name}`, db.ready, { timeout: 5000 });
  const retireStarted = performance.now();
  const fresh = await placeSecond();
  const reclaimed = await eventually('a fresh namespace immediately displaces an idle database home', async () => {
    const sample = await state();
    assert.equal(sample.databaseHomes, full.databaseHomes - 1);
    return sample;
  }, { timeout: 4000, interval: 25 });
  const reclaimMs = performance.now() - retireStarted;
  assert.ok(reclaimMs < 10000, `${reclaimMs} ms`);
  console.log('Native SQL home pressure', JSON.stringify({ reclaimMs, availableHomes: full.workerThreads,
    fullDatabaseHomes: full.databaseHomes, reclaimedDatabaseHomes: reclaimed.databaseHomes }));

  // Exercise the default home count and memory capacity on the one owner. The
  // other tenant continues using both interfaces while the databases rotate.
  const measurements = [[], [], []];
  const measure = async (check, output) => {
    const start = performance.now();
    await check();
    output.push({ ms: performance.now() - start, lag: 0 });
  };
  const sweepStarted = performance.now();
  const lastStarted = new Array(databases.length);
  const revisit = [];
  let peakHomes = 0;
  let peakReservedBytes = 0;
  let peakResidentBytes = 0;
  const sweepMs = [];
  for (let round = 0; round < 2; ++round) {
    const start = performance.now();
    for (let i = 0; i < databases.length; ++i) {
      const sent = performance.now();
      if (round > 0) revisit.push({ ms: sent - lastStarted[i], lag: 0 });
      lastStarted[i] = sent;
      const jobs = [measure(databases[i].ready, measurements[0])];
      if (i % 4 === 0) jobs.push(measure(cross.ready, measurements[1]), measure(fresh.check, measurements[2]));
      await Promise.all(jobs);
      if (i % 20 !== 0 && i !== databases.length - 1) continue;
      const sample = await state();
      assert.ok(sample.databaseHomes <= sample.workerThreads - 1);
      peakHomes = Math.max(peakHomes, sample.databaseHomes);
      peakReservedBytes = Math.max(peakReservedBytes, sample.nativeDatabaseReservedBytes);
      peakResidentBytes = Math.max(peakResidentBytes, sample.residentBytes);
    }
    sweepMs.push(performance.now() - start);
  }
  console.log('Native SQL 1000 databases', JSON.stringify({ node, creationMs,
    elapsedMs: performance.now() - sweepStarted, sweepMs, revisit: stats(revisit),
    peakHomes, peakReservedBytes, peakResidentBytes,
    database: stats(measurements[0]), crossTenantDatabase: stats(measurements[1]), durableObject: stats(measurements[2]) }));
}

function stats(samples) {
  const values = samples.filter(sample => !sample.error).map(sample => sample.ms).sort((a, b) => a - b);
  const percentile = p => values[Math.ceil(values.length * p) - 1];
  return { attempted: samples.length, samples: values.length, errors: samples.filter(sample => sample.error).map(sample => sample.error),
    p50: percentile(0.5), p99: percentile(0.99), maxStartDelay: Math.max(...samples.map(sample => sample.lag)) };
}

const intervalMs = 50;
const durationMs = 28000;
const windowMs = 30000;
const warmStartMs = 750;
async function sampleWindow(probes, samples) {
  const start = performance.now();
  const jobs = [];
  for (let due = start; due < start + durationMs; due += intervalMs) {
    await delay(Math.max(0, due - performance.now()));
    const sent = performance.now();
    jobs.push(...probes.map(async (probe, i) => {
      let error;
      try { await probe.check(); } catch (failure) { error = failure.message; }
      samples[i].push({ ms: performance.now() - sent, lag: sent - due, error });
    }));
  }
  await Promise.all(jobs);
}

export async function longSqlIsolation(cluster) {
  const tenant = await cluster.tenant('sql-isolation');
  const other = await cluster.tenant('sql-isolation-other');
  const blocker = await database(cluster, tenant, 'blocker');
  assert.equal((await blocker.query({ sql: 'CREATE TABLE ledger(id INTEGER PRIMARY KEY)' })).success, true);
  const placement = await acceptedOn(cluster, blocker.namespaceId);
  const slot = async () => {
    const { processes } = await cluster.state(placement.nodeId);
    const slot = processes.entries.find(entry => entry.processId === placement.processId);
    assert.equal(slot.generation, placement.processGeneration);
    assert.equal(slot.sandbox.durableServer, 'threaded');
    return slot;
  };
  const original = await slot();
  const same = await colocatedDatabase(cluster, tenant, placement);
  const cross = await colocatedDatabase(cluster, other, placement);
  const object = await (await prepareColocatedObject(cluster, tenant, placement))();
  const probes = [{ ...same, check: same.ready }, { ...cross, check: cross.ready }, object];
  const quiet = probes.map(() => []);
  const busy = probes.map(() => []);
  const blockers = [];
  const quietWindow = async () => {
    const start = performance.now();
    await delay(warmStartMs);
    await sampleWindow(probes, quiet);
    await delay(Math.max(0, windowMs - (performance.now() - start)));
  };

  for (let round = 0; round < 2; ++round) {
    await Promise.all(probes.map(probe => probe.check()));
    if (round === 0) await quietWindow();
    const logSince = new Date().toISOString();
    const pending = blocker.raw([put(round), { sql: slow }])
      .then(response => ({ response }), error => ({ error }));
    await delay(warmStartMs);
    await sampleWindow(probes, busy);
    const answer = await pending;
    if (answer.error) throw answer.error;
    const result = json(answer.response, 503, '30-second SQL deadline');
    assert.equal(result.code, 'SQL_LIMIT_EXCEEDED');

    const block = await eventually('the durable child wrote its SQL execution and confirmation times', async () => {
      const log = await cluster.compose(['exec', '-T', placement.nodeId, 'grep', '-F', '[native-sql] ',
        '/var/lib/coworkerd/logs/workerd.log']);
      const row = rows(log, '[native-sql] ').findLast(row => row.pid === original.pid &&
        row.generation === original.generation && row.namespaceId === blocker.namespaceId.slice(0, 12) &&
        row.status === 503 && row.startedMonotonicNs > (blockers.at(-1)?.startedMonotonicNs ?? 0));
      assert.ok(row);
      return row;
    }, { timeout: 5000, interval: 100 });
    // A line limit drops early probes when the node logs several events per
    // dispatch. Fetch from the beginning of the window and use the child's
    // monotonic timestamps to select the completed overlapping deliveries.
    const log = await cluster.compose(
      ['logs', '--no-color', '--since', logSince, placement.nodeId], { maxBuffer: 32 * 1024 * 1024 });
    const start = block.startedMonotonicNs / 1e6;
    const end = block.finishedMonotonicNs / 1e6;
    assert.ok(end - start >= 28900 && end - start <= 31000, JSON.stringify(block));
    const deliveries = rows(log, '[do] ').filter(row => sameProcess(row, placement) && row.status === 200 &&
      row.startedMonotonicMs >= start && row.finishedMonotonicMs <= end);
    const overlap = probes.map(probe => deliveries.filter(row =>
      row.namespaceId === probe.namespaceId.slice(0, 12) && (!probe.objectId || row.objectId === probe.objectId)).length);
    blockers.push({ ...block, overlap });
    assert.equal((await slot()).pid, original.pid);
    assert.deepEqual((await blocker.query({ sql: 'SELECT * FROM ledger' })).results, []);
    if (round === 1) {
      await Promise.all(probes.map(probe => probe.check()));
      await quietWindow();
    }
  }

  const report = {
    engine: 'threaded native SQLite', cpu: cpus()[0]?.model, logicalCpus: availableParallelism(),
    windowsPerCondition: 2, secondsPerWindow: windowMs / 1000, measuredSeconds: durationMs / 1000,
    offeredRateHz: 1000 / intervalMs,
    node: placement.nodeId, process: placement.processId, generation: original.generation, pid: original.pid, blockers,
    probes: ['same-tenant DB', 'cross-tenant DB', 'Durable Object'].map((probe, i) => ({
      probe, quiet: stats(quiet[i]), busy: stats(busy[i]), observedInBlocker: blockers.reduce((n, b) => n + b.overlap[i], 0),
    })),
  };
  console.log('Long SQL isolation', JSON.stringify(report));

  // Each medium-length statement fits by itself; the batch shares one deadline.
  const medium = { sql: slow.replace('1000000000', '2000000') };
  const started = performance.now();
  assert.equal((await blocker.query(medium)).success, true);
  const repeats = Math.max(150, Math.ceil(80000 / (performance.now() - started)));
  const result = json(await blocker.raw([put(100), ...Array(repeats).fill(medium)]), 503, 'batch deadline');
  assert.equal(result.code, 'SQL_LIMIT_EXCEEDED');
  assert.deepEqual((await blocker.query({ sql: 'SELECT * FROM ledger' })).results, []);

  const call = await bindingProbe(cluster, tenant, blocker.name);
  await eventually('the D1 binding is warm', async () => assert.equal(json(await call(), 200, 'warm binding').success, true));
  const timedOut = json(await call('/timeout'), 503, 'D1 SQL deadline');
  assert.match(timedOut.message, /^D1_ERROR: SQL_LIMIT_EXCEEDED:/);
  assert.match(timedOut.cause, /^SQL_LIMIT_EXCEEDED:/);
  assert.equal(json(await call(), 200, 'D1 after deadline').success, true);

  for (const sample of report.probes) {
    assert.ok(sample.quiet.samples >= 1000 && sample.busy.samples >= 1000, JSON.stringify(sample));
    assert.ok(sample.observedInBlocker >= 1000, JSON.stringify(sample));
    assert.ok(sample.busy.errors.length <= sample.quiet.errors.length, JSON.stringify(sample));
    if (process.env.COWORKERD_ASSERT_LONG_SQL_ISOLATION === '1') {
      assert.ok(sample.busy.p99 - sample.quiet.p99 <= 5, JSON.stringify(sample));
    }
  }
}

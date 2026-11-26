import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { mkdir, rename, stat, unlink, writeFile } from 'node:fs/promises';
import { get } from 'node:http';
import { createRequire } from 'node:module';
import { createConnection } from 'node:net';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { assertStatus, eventually, json, request } from './cluster.mjs';

function respondWith(value) {
  return `export default { fetch() { return new Response(${JSON.stringify(value)}); } };`;
}

async function assertStreamingStatus(response, status, description) {
  if (response.status === status) return;
  const text = await response.text().catch(error => `Could not read response body: ${error.message}`);
  assertStatus({ status: response.status, text }, status, description);
}

async function deployment(cluster) {
  const tenant = await cluster.tenant('deploy-test');
  const path = `/${tenant.name}/hello`;
  for (const version of ['v1', 'v2']) {
    const expected = `hello from ${version}`;
    await cluster.deploy(tenant, 'hello', respondWith(expected), { version });
    await eventually(`${version} serves on all three ingresses`, async () => {
      const observed = await Promise.all(cluster.nodes.map(node => request(cluster.urls[node], path)));
      assert.deepEqual(Object.fromEntries(cluster.nodes.map((node, i) => [node, observed[i]])),
        Object.fromEntries(cluster.nodes.map(node => [node, { status: 200, text: expected }])));
    });
  }
}

async function webSocketAdmission(cluster) {
  const [node, otherNode] = cluster.nodes;
  const { WebSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const source = `export default { async fetch(request) {
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') return new Response('ready');
    const path = new URL(request.url).pathname;
    if (path.endsWith('/pending')) {
      await new Promise(resolve => setTimeout(resolve, 6000));
      return new Response(null, { status: 403 });
    }
    if (path.endsWith('/decline')) {
      const body = new ReadableStream({ start(controller) {
        controller.enqueue(new TextEncoder().encode('declined'));
        setTimeout(() => controller.close(), 15000);
      } });
      return new Response(body, { status: 403 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.addEventListener('close', event => server.close(event.code, event.reason));
    return new Response(null, { status: 101, webSocket: client });
  } };`;
  const first = await cluster.tenant('socket-first');
  const second = await cluster.tenant('socket-second');
  for (const tenant of [first, second]) {
    await cluster.deploy(tenant, 'socket', source);
    await eventually('Worker ready on both ingresses', async () => {
      for (const ingress of [node, otherNode]) {
        assert.deepEqual(await workerCall(cluster, ingress, tenant, 'socket'), { status: 200, text: 'ready' });
      }
    });
  }
  const sockets = [];
  const url = (ingress, tenant, suffix = '') => new URL(`/${tenant.name}/socket${suffix}`, cluster.urls[ingress]);
  const waitFor = (socket, event) => once(socket, event, { signal: AbortSignal.timeout(5000) });
  function connect(ingress, tenant, suffix) {
    const target = url(ingress, tenant, suffix);
    target.protocol = 'ws:';
    const socket = new WebSocket(target);
    socket.on('error', () => {});
    sockets.push(socket);
    return socket;
  }
  async function open(ingress, tenant) {
    const socket = connect(ingress, tenant);
    await waitFor(socket, 'open');
    return socket;
  }
  const decline = () => new Promise((resolve, reject) => {
    const request = get(url(node, second, '/decline'), {
      headers: { Connection: 'Upgrade', Upgrade: 'websocket', 'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': randomBytes(16).toString('base64') },
    });
    const timeout = setTimeout(() => request.destroy(new Error('upgrade response headers timed out')), 5000);
    request.once('response', response => {
      clearTimeout(timeout);
      resolve(response);
    }).once('error', error => {
      clearTimeout(timeout);
      reject(error);
    }).once('upgrade', (_response, socket) => {
      clearTimeout(timeout);
      socket.destroy();
      reject(new Error('the Worker should refuse the upgrade'));
    });
  });
  const state = async () => (await cluster.state(node)).webSockets;
  const inUse = (count, timeout = 5000) => eventually(`node has ${count} WebSocket reservations`, async () => {
    assert.equal((await state()).inUse, count);
  }, { timeout });
  try {
    assert.equal((await state()).limitPerNode, 2);
    const otherState = await cluster.state(otherNode);
    assert.equal(otherState.webSockets.limitPerNode, 2048);
    const descriptors = await cluster.compose(['exec', '-T', otherNode, 'cat', `/proc/${otherState.server.pid}/limits`]);
    assert.match(descriptors, /^Max open files\s+15488\s+524288\s/m);
    const a = await open(node, first);
    const b = await open(node, second);
    const full = await state();
    assert.equal(full.inUse, 2);
    assert.deepEqual(full.places, { [first.name]: 1, [second.name]: 1 });
    const denied = await decline();
    assert.equal(denied.statusCode, 503);
    let body = '';
    for await (const chunk of denied) body += chunk;
    const { code, limit, inFlight, dispatched, retryable } = JSON.parse(body);
    assert.deepEqual({ code, limit, inFlight, dispatched, retryable },
      { code: 'WEBSOCKET_NODE_CAPACITY', limit: 2, inFlight: 2, dispatched: false, retryable: true });
    assert.deepEqual(await workerCall(cluster, node, second, 'socket'), { status: 200, text: 'ready' });
    await open(otherNode, second);

    const closed = waitFor(a, 'close');
    a.close(1000, 'done');
    assert.equal((await closed)[0], 1000);
    await inUse(1);

    const pending = connect(node, first, '/pending');
    await inUse(2);
    assert.equal(pending.readyState, WebSocket.CONNECTING);
    pending.terminate();
    assert.equal((await state()).inUse, 2, 'an abandoned handshake holds its place while waiting for the Worker');
    await inUse(1, 15000);

    const declined = await decline();
    declined.on('error', () => {});
    try {
      assert.equal(declined.statusCode, 403);
      await inUse(1);
      assert.equal(declined.complete, false, 'the upgrade place is free while the HTTP body is still streaming');
      assert.equal(declined.destroyed, false);
      await open(node, first);
      await inUse(2);
    } finally {
      declined.destroy();
    }

    b.terminate();
    await inUse(1);
    await open(node, second);
    await inUse(2);
  } finally {
    for (const socket of sockets) socket.terminate();
  }
}

async function sqlAtomicity(cluster) {
  const { path, token } = await cluster.tenant('sql-test');
  const database = `${path}/databases/ledger`;
  const { database: created } = await cluster.api(database, { method: 'POST', token });
  const query = (body) => cluster.api(`${database}/query`, { method: 'POST', token, body, timeout: 35000 });
  // The database's namespace can take time to become visible on the nodes.
  await eventually('database serves SELECT', async () => {
    const probe = await query({ sql: 'SELECT 1 AS ready' });
    assert.deepEqual(probe.results, [{ ready: 1 }]);
  });

  const insert = (id, value) => ({ sql: 'INSERT INTO ledger VALUES (?, ?)', params: [id, value] });
  const read = async () => (await query({ sql: 'SELECT * FROM ledger ORDER BY id' })).results;
  assert.equal((await query({ sql: 'CREATE TABLE ledger(id INTEGER PRIMARY KEY, value TEXT)' })).success, true);
  const committed = await query([insert(1, 'one'), insert(2, 'two')]);
  assert.deepEqual(committed.map(result => result.success), [true, true]);
  const rows = [{ id: 1, value: 'one' }, { id: 2, value: 'two' }];
  assert.deepEqual(await read(), rows, 'both committed statements are visible');

  const refused = await query([insert(3, 'partial'), insert(1, 'duplicate'), insert(4, 'after error')]);
  assert.equal(refused[0]?.success, false, 'the duplicate primary key must reject the transaction');
  assert.match(refused[0].error, /UNIQUE constraint failed/);
  assert.deepEqual(await read(), rows, 'the failed transaction left the committed rows intact');
  return created;
}

async function createDatabaseHomes(cluster, tenant, counts, { limit = 36, successors } = {}) {
  const databases = new Map([...counts.keys()].map(node => [node, []]));
  let revision = 0;
  for (let i = 0; i < limit && [...counts].some(([node, count]) => databases.get(node).length < count); i++) {
    const path = `${tenant.path}/databases/db-${i}`;
    const { database, revision: createdAt } = await cluster.api(path, { method: 'POST', token: tenant.token });
    revision = Math.max(revision, createdAt);
    // Rendezvous order determines the first owner and the successor used in takeover tests.
    const [owner, successor] = shardOwnerOrder(cluster, database.namespaceId, 0);
    const selected = databases.get(owner);
    const expectedSuccessor = successors?.get(owner);
    if (selected && selected.length < counts.get(owner) && (!expectedSuccessor || expectedSuccessor === successor)) {
      selected.push({ ...database, path, owner });
    }
  }
  for (const [node, count] of counts) assert.equal(databases.get(node).length, count, `database placement on ${node}`);
  assert.ok(revision > 0, 'database creation returns a catalog revision');
  // Avoid opening a shard while its designated owner is still waiting for the namespace catalog.
  await eventually('all peers have applied the database catalog', async () => {
    await Promise.all(cluster.nodes.map(async node => {
      const state = await cluster.state(node);
      assert.ok(Number(state.storage.appliedTenantsRevision) >= revision, `${node} has applied revision ${revision}`);
    }));
  });
  return { databases, revision };
}

async function threadedDatabaseRelease(cluster) {
  const { path, token } = await cluster.tenant('sql-release');
  const database = `${path}/databases/ledger`;
  await cluster.api(database, { method: 'POST', token });
  const processes = await Promise.all(cluster.nodes.map(node => durableGeneration(cluster, node)));
  assert.ok(processes.every(process => process.lifecycle.durableServer === 'threaded'));
  await eventually('the public SQL route preserves the threaded database refusal in the release executable', async () => {
    const refusal = await cluster.api(`${database}/query`, {
      method: 'POST', token, body: { sql: 'SELECT 1' }, status: 501,
    });
    assert.equal(refusal.code, 'NOT_SUPPORTED');
  });
}

function shardOwnerOrder(cluster, namespaceId, shard) {
  const scores = cluster.nodes.map(node => [node, createHash('sha256')
    .update(`${namespaceId}/${shard}\0${node}`).digest().readBigUInt64BE()]);
  scores.sort((a, b) => a[1] > b[1] ? -1 : a[1] < b[1] ? 1 : a[0].localeCompare(b[0]));
  return scores.map(([node]) => node);
}

async function databaseDispatches(cluster, db, owner) {
  const logs = await cluster.compose(['logs', '--no-color', owner]);
  return logs.split('\n').filter(line => line.includes('[do] ')).map(line => JSON.parse(line.split('[do] ')[1]))
    .filter(row => row.nodeId === owner && row.namespaceId === db.namespaceId.slice(0, 12) && row.status === 200);
}

async function waitForDurableMembership(cluster) {
  // An early call can claim a shard or confirm a write using an incomplete
  // membership view.
  await eventually('all nodes see every durable candidate', async () => {
    await Promise.all(cluster.nodes.map(async node => {
      const { membership } = await cluster.state(node);
      const active = membership.members.filter(member => member.durableProcesses > 0 && member.state === 'active');
      assert.deepEqual(active.map(member => member.nodeId).sort(), cluster.nodes, node);
    }));
  });
}

async function waitForWalPeers(cluster, owner) {
  const peers = cluster.nodes.filter(node => node !== owner);
  await eventually('the database owner has both WAL confirmation streams', async () => {
    const { durableLog } = await cluster.state(owner);
    assert.equal(durableLog.peers.want, peers.length);
    assert.ok(peers.every(peer => durableLog.peers.streams.some(stream => stream.nodeId === peer && stream.open)));
  });
  return peers;
}

async function threadedSqlCliParity(cluster) {
  await waitForDurableMembership(cluster);
  const ledger = await sqlAtomicity(cluster);
  const tenant = await cluster.tenant('sql-cli');
  const cwd = join(cluster.directory, 'd1-cli');
  await mkdir(cwd);
  await writeFile(join(cwd, 'cowctl.json'), JSON.stringify({ tenant: tenant.name, controlPlane: cluster.urls.apiserver }));
  const cowctl = fileURLToPath(new URL('../../cli/bin/cowctl.mjs', import.meta.url));
  const cli = async (...args) => JSON.parse(await cluster.command(process.execPath,
    [cowctl, 'd1', ...args, '--json'], { cwd, token: tenant.token, env: { HOME: cwd } }));

  const { database: source } = await cli('create', 'books');
  const migration = await cli('migrations', 'create', source.name, 'create books');
  await writeFile(join(migration.directory, migration.created), `CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
CREATE UNIQUE INDEX book_title ON books(title);
INSERT INTO books VALUES (1, 'One');
`);
  assert.deepEqual((await cli('migrations', 'list', source.name)).pending, [migration.created]);
  assert.deepEqual((await cli('migrations', 'apply', source.name, '--yes')).applied, [migration.created]);
  assert.deepEqual((await cli('migrations', 'list', source.name)).pending, []);

  const importFile = async (name, file) => {
    const result = await cli('execute', name, '--file', file, '--yes');
    assert.equal(result.success, true);
    assert.equal(result.committedOffset, (await stat(join(cwd, file))).size);
  };
  await writeFile(join(cwd, 'seed.sql'), "INSERT INTO books VALUES (2, 'Two; 二')\n");
  await importFile(source.name, 'seed.sql');
  const rowsSql = 'SELECT id, title FROM books ORDER BY id';
  const rows = [{ id: 1, title: 'One' }, { id: 2, title: 'Two; 二' }];
  const execute = async (name, sql) => {
    const result = await cli('execute', name, '--sql', sql);
    assert.equal(result.success, true, result.error);
    return result.results;
  };
  const schemaSql = "SELECT type, name, sql FROM sqlite_schema WHERE name IN ('books', 'book_title', 'd1_migrations') ORDER BY name";
  const schema = await execute(source.name, schemaSql);
  assert.equal(schema.length, 3);
  assert.deepEqual(await execute(source.name, rowsSql), rows);

  const sourceOwner = shardOwnerOrder(cluster, source.namespaceId, 0)[0];
  const log = async () => (await cluster.state(sourceOwner)).durableLog.log;
  const publishedPoint = async () => {
    const confirmed = BigInt((await log()).nextSeq);
    assert.ok(confirmed > 0n);
    await eventually('the owner publishes the confirmed source writes', async () => {
      assert.ok(BigInt((await log()).sealedSeq) >= confirmed);
    });
    return (await cli('time-travel', 'info', source.name)).point;
  };
  const point = await publishedPoint();
  assert.ok(point.bookmark);

  const exported = await cli('export', source.name, '--output', 'backup.sql');
  assert.equal(exported.success, true);
  assert.equal(exported.mode, 'atomic');
  const { database: backup } = await cli('create', 'books-backup');
  await importFile(backup.name, 'backup.sql');

  await execute(source.name, "INSERT INTO books VALUES (3, 'After bookmark')");
  const latest = await publishedPoint();
  assert.ok(BigInt(latest.revision) > BigInt(point.revision));
  const restored = await cli('time-travel', 'restore', source.name, '--bookmark', point.bookmark,
    '--to', 'books-restored', '--yes', '--timeout-ms', '45000');
  assert.equal(restored.phase, 'completed');
  assert.equal(restored.database, 'books-restored');
  assert.equal(restored.point.bookmark, point.bookmark);
  assert.notEqual(restored.namespaceId, source.namespaceId);
  const historical = { name: restored.database, namespaceId: restored.namespaceId };
  for (const db of [backup, historical]) {
    await eventually(`${db.name} serves the restored schema`, async () => {
      assert.deepEqual(await execute(db.name, schemaSql), schema);
    });
    assert.deepEqual(await execute(db.name, 'SELECT name FROM d1_migrations ORDER BY id'), [{ name: migration.created }]);
    assert.deepEqual(await execute(db.name, rowsSql), rows, `${db.name} contains the confirmed snapshot rows`);
  }

  await cluster.deploy(tenant, 'read-write', `export default { async fetch(request, env) {
    const db = env[new URL(request.url).searchParams.get('db')];
    if (request.method === 'POST') {
      await db.prepare('INSERT INTO books VALUES (?, ?)').bind(4, 'D1 write').run();
    }
    return Response.json((await db.prepare(${JSON.stringify(rowsSql)}).all()).results);
  } };`, { wrangler: { d1_databases: [backup, historical].map(({ name }, index) => ({
    binding: `DB${index}`, database_name: name,
  })) } });
  const withWrite = [...rows, { id: 4, title: 'D1 write' }];
  for (const [index, db] of [backup, historical].entries()) {
    const [, ingress] = shardOwnerOrder(cluster, db.namespaceId, 0);
    const call = async method => json(await workerCall(cluster, ingress, tenant, `read-write?db=DB${index}`, { method }),
      200, `${db.name} D1 ${method}`);
    await eventually(`${db.name} is readable through its D1 binding`, async () => assert.deepEqual(await call('GET'), rows));
    assert.deepEqual(await call('POST'), withWrite);
    assert.deepEqual(await execute(db.name, rowsSql), withWrite);
  }
  assert.deepEqual(await execute(source.name, rowsSql), [...rows, { id: 3, title: 'After bookmark' }]);

  for (const db of [ledger, source, backup, historical]) {
    const [owner] = shardOwnerOrder(cluster, db.namespaceId, 0);
    const process = await durableGeneration(cluster, owner);
    assert.equal(process.lifecycle.durableServer, 'threaded');
    const callers = db === backup || db === historical ? ['node', 'worker'] : ['node'];
    await eventually(`${db.name} dispatched SqlDatabase in ${owner}'s threaded generation`, async () => {
      const calls = await databaseDispatches(cluster, db, owner);
      for (const caller of callers) {
        assert.ok(calls.some(call => call.caller === caller && call.className === '__coworkerdSqlDatabase' &&
          call.processId === process.processId && call.processGeneration === process.generation), `${db.name} ${caller}`);
      }
    });
    console.log(`${db.name}: ${callers.join(' and ')} dispatch through ${owner}/${process.processId} generation ${process.generation}`);
  }
  console.log(`Restored published revision ${point.revision} and the atomic SQL export`);
}

async function threadedDatabaseAlarm(cluster) {
  const [threaded] = cluster.nodes;
  const tenant = await cluster.tenant('sql-alarm', { durableObjectNamespaces: 64 });
  const databases = new Map();
  let revision = 0;
  for (let i = 0; i < 64 && databases.size < 2; i++) {
    const path = `${tenant.path}/databases/db-${i}`;
    const created = await cluster.api(path, { method: 'POST', token: tenant.token });
    revision = Math.max(revision, created.revision);
    const [owner] = shardOwnerOrder(cluster, created.database.namespaceId, 0);
    const backend = owner === threaded ? 'threaded' : 'upstream';
    if (!databases.has(backend)) databases.set(backend, { ...created.database, path, owner });
  }
  assert.equal(databases.size, 2, 'both backends have a database');
  await eventually('each peer has the database catalog and full durable membership', async () => {
    await Promise.all(cluster.nodes.map(async node => {
      const state = await cluster.state(node);
      assert.ok(Number(state.storage.appliedTenantsRevision) >= revision, `${node} has applied revision ${revision}`);
      const candidates = state.membership.members.filter(member => member.durableProcesses > 0 && member.state === 'active');
      assert.deepEqual(candidates.map(member => member.nodeId).sort(), cluster.nodes, `${node} sees every candidate`);
    }));
  });

  await Promise.all([...databases].map(async ([backend, db]) => {
    const query = body => cluster.api(`${db.path}/query`, { method: 'POST', token: tenant.token, body });
    const alarm = async () => {
      const result = await query({ alarmForTest: true });
      assert.equal(result.success, true, result.error);
      return result.results[0].alarm;
    };
    const stats = async () => (await cluster.state(db.owner)).durableLog.alarms;
    const process = await durableGeneration(cluster, db.owner);
    if (backend === 'threaded') assert.equal(process.lifecycle.durableServer, 'threaded');
    const before = await stats();
    assert.equal(before.fireConcurrency === 0, backend === 'threaded',
      `${backend} owner uses the expected alarm dispatcher`);
    assert.equal(before.scheduled, 0);
    const seed = await query([
      { sql: 'CREATE TABLE records (value TEXT PRIMARY KEY)' },
      { sql: 'INSERT INTO records VALUES (?)', params: [db.name] },
    ]);
    assert.deepEqual(seed.map(result => result.success), [true, true]);
    const at = await alarm();
    assert.ok(Number.isSafeInteger(at) && at > Date.now(), `${backend} database holds a pending alarm`);
    await eventually(`${backend} owner observes the scheduled alarm`, async () => {
      const pending = (await stats()).next.find(entry => entry.namespaceId === db.namespaceId.slice(0, 12));
      assert.equal(pending?.atUnixMs, at);
    });
    const cleared = await eventually(`${backend} owner settles the database alarm`, async () => {
      const current = await stats();
      assert.equal(current.cleared, before.cleared + 1);
      assert.equal(current.scheduled, 0);
      assert.equal(current.inFlight, 0);
      return current;
    });
    const attempts = cleared.dispatchAttempts - before.dispatchAttempts;
    assert.ok(attempts >= 1, `${backend} dispatched the due alarm`);
    assert.equal(cleared.retries, before.retries, `${backend} did not retry the alarm handler`);
    assert.equal(cleared.fired - before.fired, backend === 'upstream' ? 1 : 0,
      `${backend} uses its own missing-handler outcome`);
    assert.equal(await alarm(), null, `${backend} cleared the alarm in storage`);

    const rotation = await rotationRequest(cluster, db.owner, process.processId);
    await waitForDurableRotation(cluster, db.owner, process, rotation, 'drained');
    const read = await query({ sql: 'SELECT value FROM records' });
    assert.deepEqual(read.results, [{ value: db.name }], `${backend} reopens with the saved row`);
    assert.equal(await alarm(), null, `${backend} reopens with the alarm still cleared`);
    // Let the scheduler pass the first alarm retry window with the database reopened.
    await delay(2500);
    const reopened = await stats();
    assert.equal(reopened.scheduled, 0);
    assert.equal(reopened.dispatchAttempts, cleared.dispatchAttempts, `${backend} did not dispatch the alarm again`);
    console.log(`Database alarm on ${db.owner} (${backend}): ${attempts} dispatch attempt(s), cleared and reopened with saved row`);
  }));
}

async function threadedDatabasePlacement(cluster) {
  const [threaded, upstream] = cluster.nodes;
  if (cluster.previousProduct) {
    const digest = async node => (await cluster.compose(
      ['exec', '-T', node, 'sha256sum', '/opt/coworkerd/bin/coworkerd'])).split(' ')[0];
    const [current, previous] = await Promise.all([digest(threaded), digest(upstream)]);
    assert.notEqual(current, previous, 'the compatibility test uses different node binaries');
    console.log(`Database compatibility binaries (SHA-256): upstream=${previous}, threaded=${current}`);
  }
  const tenant = await cluster.tenant('sql-placement', { durableObjectNamespaces: 128 });
  const { databases, revision } = await createDatabaseHomes(cluster, tenant, new Map([[threaded, 2], [upstream, 1]]), {
    limit: 96, successors: new Map([[threaded, upstream], [upstream, threaded]]),
  });
  const selected = [...databases.values()].flat();
  const peerUrls = new Map(await Promise.all([threaded, upstream].map(async node => {
    const address = await cluster.compose(['port', node, '8934']);
    assert.match(address, /^127\.0\.0\.1:\d+$/);
    return [node, `http://${address}`];
  })));
  const sqlAt = async (ingress, db, body) => json(await request(peerUrls.get(ingress),
    `/__coworkerd/databases/${tenant.name}/${db.name}/query`,
    { method: 'POST', token: cluster.management, body, timeout: 15000 }), 200, `${ingress} SQL to ${db.name}`);
  const publicSql = (db, body) => cluster.api(`${db.path}/query`, {
    method: 'POST', token: tenant.token, body, timeout: 35000,
  });
  const read = async db => {
    const reply = await publicSql(db, { sql: 'SELECT value FROM records' });
    assert.deepEqual(reply.results, [{ value: db.name }]);
  };
  const assertMapping = (call, generation) => {
    assert.equal(call.workerId, `${tenant.name}/__databases`);
    assert.equal(call.className, '__coworkerdSqlDatabase');
    assert.equal(call.processId, generation.processId);
    assert.equal(call.processGeneration, generation.generation);
  };
  const process = await durableGeneration(cluster, threaded);
  assert.equal(process.lifecycle.durableServer, 'threaded');
  assert.equal(process.lifecycle.durableAdmission.residentRoots, 0);
  assert.equal(process.lifecycle.durableAdmission.homes, 0);
  for (const db of selected) {
    const ingress = db.owner === threaded ? upstream : threaded;
    await eventually(`${ingress} can query ${db.name} on ${db.owner}`, async () => {
      const result = await sqlAt(ingress, db, { sql: 'SELECT 1 AS ready' });
      assert.deepEqual(result.results, [{ ready: 1 }]);
    });
    assert.equal((await sqlAt(ingress, db, { sql: 'CREATE TABLE records (value TEXT PRIMARY KEY)' })).success, true);
    const ingest = await publicSql(db, { ingest: `INSERT INTO records VALUES ('${db.name}');` });
    assert.equal(ingest.success, true, ingest.error);
  }

  await cluster.deploy(tenant, 'read', `export default { async fetch(request, env) {
    const result = await env[new URL(request.url).searchParams.get('db')]
      .prepare('SELECT value FROM records ORDER BY value').all();
    return Response.json(result.results);
  } };`, { wrangler: { d1_databases: selected.map((db, index) => ({
    binding: `DB${index}`, database_name: db.name,
  })) } });
  for (const [index, db] of selected.entries()) {
    const ingress = db.owner === threaded ? upstream : threaded;
    await eventually(`${ingress} can bind ${db.name} on ${db.owner}`, async () => {
      const response = await workerCall(cluster, ingress, tenant, `read?db=DB${index}`);
      assert.deepEqual(json(response, 200, `${ingress} D1 to ${db.name}`), [{ value: db.name }]);
    });
    const rejected = await sqlAt(ingress, db, [
      { sql: "INSERT INTO records VALUES ('partial')" }, { sql: 'INSERT INTO records VALUES (?)', params: [db.name] },
    ]);
    assert.match(rejected[0]?.error, /UNIQUE constraint failed/);
    await read(db);
  }

  const assertWarmCopies = async () => {
    const warm = await durableGeneration(cluster, threaded);
    assert.equal(warm.pid, process.pid);
    assert.equal(warm.generation, process.generation);
    assert.equal(warm.lifecycle.durableAdmission.residentRoots, 2);
    assert.equal(warm.lifecycle.durableAdmission.homes, 2);
  };
  await eventually('both threaded database roots occupy distinct warm homes in the original process', assertWarmCopies);
  const epochs = new Map();
  for (const db of selected) {
    const from = db.owner === threaded ? upstream : threaded;
    const ownerProcess = db.owner === threaded ? process : await durableGeneration(cluster, upstream);
    await eventually(`both requests reached the local SqlDatabase on ${db.owner}`, async () => {
      const calls = (await databaseDispatches(cluster, db, db.owner)).filter(row => row.route === 'received' && row.node === from);
      for (const caller of ['node', 'worker']) {
        const call = calls.find(row => row.caller === caller);
        assert.ok(call, `${from} forwarded ${caller} to ${db.owner}`);
        assertMapping(call, ownerProcess);
        assert.ok(BigInt(call.shardEpoch) > 0n);
        epochs.set(db.name, BigInt(call.shardEpoch));
      }
    });
  }
  const advance = join(cluster.durableLifecycleGate, 'idle-offset-next');
  await writeFile(advance, '180000');
  await rename(advance, join(cluster.durableLifecycleGate, 'idle-offset-ms'));
  await eventually('idle databases release both threaded roots and their copies', async () => {
    const idle = await durableGeneration(cluster, threaded);
    assert.equal(idle.pid, process.pid);
    assert.equal(idle.generation, process.generation);
    assert.equal(idle.lifecycle.durableAdmission.residentRoots, 0);
    assert.equal(idle.lifecycle.durableAdmission.homes, 0);
  });
  for (const db of databases.get(threaded)) await read(db);
  await eventually('reopened databases retain two exclusive homes in the same generation', assertWarmCopies);
  console.log(`Database placement in ${threaded}/${process.processId} generation ${process.generation}: 2 roots, 2 homes before and after idle release`);

  const takeOver = async (original, successor) => {
    await cluster.compose(['kill', '-s', 'SIGKILL', original]);
    for (const db of databases.get(original)) {
      await eventually(`${successor} takes over ${db.name} and reads its acknowledged row`, async () => {
        await read(db);
        const calls = await databaseDispatches(cluster, db, successor);
        const call = calls.find(row => row.caller === 'node' && (row.route === 'local' || row.route === 'received') &&
          BigInt(row.shardEpoch) > epochs.get(db.name));
        assert.ok(call, `${successor} served ${db.name} at a later epoch than ${original}`);
        assertMapping(call, await durableGeneration(cluster, successor));
        console.log(`Database ${db.name} takeover: ${original} -> ${successor}, epoch ${epochs.get(db.name)} -> ${call.shardEpoch}`);
      }, { timeout: 100000, interval: 500 });
    }
  };
  await takeOver(upstream, threaded);
  await cluster.compose(['start', upstream]);
  await cluster.refreshUrl(upstream);
  await eventually('the upstream node rejoins before the threaded owner is killed', async () => {
    await durableGeneration(cluster, upstream);
    const state = await cluster.state(upstream);
    assert.ok(Number(state.storage.appliedTenantsRevision) >= revision);
    await Promise.all(cluster.nodes.filter(node => node !== upstream).map(async node => {
      const { membership } = await cluster.state(node);
      assert.ok(membership.members.some(member => member.nodeId === upstream && member.durableProcesses > 0));
    }));
  }, { timeout: 45000 });
  await takeOver(threaded, upstream);
}

// One leaf, page 1 and 252 overflow pages. Each overflow page holds 4092
// bytes of a blob on the fixture's 4096-byte SQLite pages.
const sqlWalFrames = 254;
const sqlWalFrameBytes = 4096 + 24;
const sqlWalHeaderBytes = 32;
const sqlWalRecordBytes = sqlWalFrames * sqlWalFrameBytes + 116 + 129 + 64 + sqlWalHeaderBytes;
const sqlWalPeerBufferBytes = sqlWalRecordBytes + 4096; // Peer RPC headroom.

async function threadedSqlTransactionLimit(cluster) {
  const [owner, ingress] = cluster.nodes;
  const tenant = await cluster.tenant('sql-record-limit', { durableObjectNamespaces: 48 });
  const { databases } = await createDatabaseHomes(cluster, tenant, new Map([[owner, 1]]));
  const [db] = databases.get(owner);
  const query = (body, status = 200) => cluster.api(`${db.path}/query`,
    { method: 'POST', token: tenant.token, body, status });
  const log = async () => (await cluster.state(owner)).durableLog;
  await eventually('the owner sees both durable peers', async () => {
    const { membership } = await cluster.state(owner);
    for (const node of cluster.nodes.slice(1)) {
      assert.ok(membership.members.some(member => member.nodeId === node && member.durableProcesses > 0), node);
    }
  });
  assert.equal(Number((await log()).settings.recordLimitBytes), sqlWalRecordBytes);
  const insert = 'INSERT INTO ledger VALUES (?, zeroblob(?))';
  const fill = (id, pages) => ({ sql: insert, params: [id, pages * 4092] });
  const read = { sql: 'SELECT id, length(value) AS bytes FROM ledger ORDER BY id' };
  await query([
    { sql: 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, value BLOB)' },
    { sql: 'INSERT INTO ledger VALUES (1, x\'01\')' },
  ]);
  const before = await log();
  assert.equal(before.threadedHomeIo.openSamples, 1);
  assert.equal((await query(fill(2, 252))).success, true);
  const atLimit = await log();
  assert.equal(atLimit.log.records - before.log.records, 1);
  assert.equal(atLimit.log.frames - before.log.frames, sqlWalFrames);
  // The first record already sent the generation header. Its reserved 32 bytes
  // still count against the transaction capacity.
  assert.equal(Number(atLimit.log.bytes) - Number(before.log.bytes) + sqlWalHeaderBytes, sqlWalRecordBytes);
  assert.equal(atLimit.confirm.confirmations - before.confirm.confirmations, 1);
  assert.ok(atLimit.peers.streams.some(stream => stream.open &&
    BigInt(stream.ackedSeq) >= BigInt(atLimit.log.nextSeq)), 'a peer acknowledged the boundary record');

  const reason = 'the transaction exceeds one recoverable WAL record';
  const code = 'SQL_TRANSACTION_TOO_LARGE';
  const failure = `${code}: ${reason}`;
  const refused = await query(fill(3, 253), 422);
  assert.deepEqual(refused, { success: false, error: failure, reason, code });
  const after = await log();
  assert.equal(after.log.records, atLimit.log.records, 'the refused transaction never entered the node log');
  assert.equal(after.recovery.recordLimitRefusals, before.recovery.recordLimitRefusals,
    'the node did not have to refuse a locally committed record');
  const committed = [{ id: 1, bytes: 1 }, { id: 2, bytes: 252 * 4092 }];
  assert.deepEqual((await query(read)).results, committed);

  const oversizedInsert = `INSERT INTO ledger VALUES (6, zeroblob(${253 * 4092}))`;
  await cluster.deploy(tenant, 'probe', `export default { async fetch(request, { DB }) {
    try {
      if (new URL(request.url).searchParams.has('exec')) {
        await DB.exec("INSERT INTO ledger VALUES (5, x'05')\\n" +
          ${JSON.stringify(oversizedInsert + '\n')} +
          "INSERT INTO ledger VALUES (7, x'07')");
      } else {
        await DB.batch([DB.prepare("INSERT INTO ledger VALUES (4, x'04')"),
          DB.prepare(${JSON.stringify(insert)}).bind(6, ${253 * 4092})]);
      }
      return Response.json({ success: true });
    } catch (error) {
      return Response.json({ message: error.message, cause: error.cause?.message });
    }
  } };`, { wrangler: { d1_databases: [{ binding: 'DB', database_name: db.name }] } });
  const binding = async (suffix = '') => json(await request(cluster.urls[ingress],
    `/${tenant.name}/probe${suffix}`), 200, 'D1 transaction limit');
  assert.deepEqual(await binding(), { message: `D1_ERROR: ${failure}`, cause: failure });
  assert.deepEqual((await query(read)).results, committed, 'the earlier D1 batch statement was rolled back');
  const execFailure = `Error in line 2: ${oversizedInsert}: ${failure}`;
  assert.deepEqual(await binding('?exec'), { message: `D1_EXEC_ERROR: ${execFailure}`, cause: execFailure });
  assert.deepEqual((await query(read)).results, [...committed, { id: 5, bytes: 1 }],
    'exec keeps its confirmed prefix and skips the rest');
  assert.equal((await query({ sql: 'INSERT INTO ledger VALUES (3, x\'03\')' })).success, true);
  assert.deepEqual((await query(read)).results,
    [...committed, { id: 3, bytes: 1 }, { id: 5, bytes: 1 }]);
  const final = await log();
  assert.equal(final.threadedHomeIo.openSamples, before.threadedHomeIo.openSamples, 'the same database stayed open');
  assert.equal(final.objects.repairs, before.objects.repairs, 'the connection needed no repair');
}

async function threadedDatabaseAccounting(cluster) {
  const [threaded, upstream] = cluster.nodes;
  const tenant = await cluster.tenant('sql-accounting', { durableObjectNamespaces: 48 });
  const databaseWorker = `${tenant.name}/__databases`;
  const platformWorker = `${tenant.name}/:platform`;
  const { databases } = await createDatabaseHomes(cluster, tenant, new Map([[upstream, 1], [threaded, 1]]));
  await cluster.deploy(tenant, 'object', `
    import { DurableObject } from 'cloudflare:workers';
    export class Echo extends DurableObject {
      fetch() { return new Response('expected the platform class', { status: 500 }); }
    }
    export default { fetch(request, env) {
      return env.ECHO.getByName(new URL(request.url).searchParams.get('name')).fetch(request);
    } };
  `, { wrangler: {
    durable_objects: { bindings: [{ name: 'ECHO', class_name: 'Echo' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Echo'] }],
  } });
  const { namespaceId, shards } = await cluster.api(`${tenant.path}/workers/object/namespaces/Echo`, { token: tenant.token });
  const candidate = Array.from({ length: 64 }, (_, index) => `object-${index}`).find(name => {
    const id = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update(name).digest();
    const shard = Number(id.readBigUInt64BE() % BigInt(shards));
    return shardOwnerOrder(cluster, namespaceId, shard)[0] === threaded;
  });
  assert.ok(candidate, 'the platform object is assigned to the threaded database owner');
  assert.equal((await durableGeneration(cluster, threaded)).lifecycle.durableServer, 'threaded');

  for (const [node, [db]] of databases) {
    await eventually(`public SQL serves the database on ${node}`, async () => {
      const reply = await cluster.api(`${db.path}/query`, {
        method: 'POST', token: tenant.token, body: { sql: 'SELECT 42 AS value' },
      });
      assert.deepEqual(reply.results, [{ value: 42 }]);
    });
  }
  await eventually('the same tenant loads the non-database platform class on the threaded owner', async () => {
    const response = await workerCall(cluster, threaded, tenant, `object?name=${candidate}`);
    assert.deepEqual(json(response, 200, 'platform control request'), { service: 'echo', build: threaded });
  });

  for (const [node, [db]] of databases) {
    const expected = new Map([[db.namespaceId.slice(0, 12), databaseWorker]]);
    if (node === threaded) expected.set(namespaceId.slice(0, 12), platformWorker);
    await eventually(`dispatch logs for the accounting requests on ${node}`, async () => {
      const logs = await cluster.compose(['logs', '--no-color', node]);
      const dispatches = logs.split('\n').filter(line => line.includes('[do] '))
        .map(line => JSON.parse(line.split('[do] ')[1]))
        .filter(row => row.status === 200 && expected.has(row.namespaceId));
      const { executions } = await cluster.state(node);
      for (const [namespace, worker] of expected) {
        const call = dispatches.find(row => row.namespaceId === namespace);
        assert.ok(call, `${node} served namespace ${namespace}`);
        assert.equal(call.workerId, worker, 'dispatch log Worker');
        const admitted = executions.find(row => row.invocationId === call.invocationId);
        assert.equal(admitted?.dispatched, true, 'the event was admitted for execution');
        assert.equal(admitted.target, `${worker}@${call.versionId}`, 'admitted Worker and version');
      }
    });

    await eventually(`native metering rows and tenant usage on ${node}`, async () => {
      const { processes, usage } = await cluster.state(node);
      if (node === threaded) {
        // The native host's completion rows carry the Worker and peak memory.
        // The upstream database uses a static runtime class.
        for (const worker of expected.values()) {
          assert.ok(processes.workerMemoryStats.some(row => row.workerId === worker && row.peakUsedBytes > 0),
            `native usage row for ${worker}`);
        }
      }
      const requestCount = Object.values(usage.hours).reduce((count, hour) => count + (hour[tenant.name]?.requests ?? 0), 0);
      assert.ok(requestCount >= expected.size, `durable requests are counted for ${tenant.name}`);
    });
  }
}

async function threadedSqlDeadline(cluster) {
  const [owner, ingress] = cluster.nodes;
  const tenant = await cluster.tenant('sql-deadline', { durableObjectNamespaces: 48 });
  const { databases: homes } = await createDatabaseHomes(cluster, tenant, new Map([[owner, 4]]));
  const databases = homes.get(owner);
  const sql = (db, body, timeout = 40000) => request(cluster.urls.apiserver, `${db.path}/query`,
    { method: 'POST', token: tenant.token, body, timeout });
  const read = { sql: 'SELECT id FROM ledger ORDER BY id' };
  const put = id => ({ sql: 'INSERT INTO ledger VALUES (?)', params: [id] });
  const long = 'WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n < 1000000000)';
  const select = `${long} SELECT sum(n) FROM r`;
  const insert = `${long} INSERT INTO ledger SELECT sum(n) FROM r`;
  const medium = select.replace('1000000000', '2000000');
  for (const db of databases) {
    await eventually(`${db.name} reaches its database home`, async () => {
      assert.equal(json(await sql(db, { sql: 'SELECT 1' }), 200, db.name).success, true);
    });
    assert.equal(json(await sql(db, { sql: 'CREATE TABLE ledger(id INTEGER PRIMARY KEY)' }), 200, db.name).success, true);
    assert.equal(json(await sql(db, put(1)), 200, db.name).success, true);
  }
  const original = await eventually('four database homes are resident on the threaded owner', async () => {
    const snapshot = await durableGeneration(cluster, owner);
    assert.equal(snapshot.lifecycle.durableServer, 'threaded');
    assert.equal(snapshot.lifecycle.durableAdmission.residentRoots, databases.length);
    return snapshot;
  });

  const measuredAt = performance.now();
  assert.equal(json(await sql(databases[0], { sql: medium }), 200, 'one short statement').success, true);
  const repeats = Math.max(180, Math.ceil(85000 / (performance.now() - measuredAt)));
  const batch = [put(2), ...Array(repeats).fill({ sql: medium })];
  await cluster.deploy(tenant, 'probe', `export default { async fetch(request, env) {
    const { searchParams } = new URL(request.url);
    const db = env['DB' + searchParams.get('db')];
    try {
      switch (searchParams.get('op')) {
        case 'single': return Response.json(await db.prepare(${JSON.stringify(select)}).all());
        case 'batch': return Response.json(await db.batch(${JSON.stringify(batch)}.map(q => db.prepare(q.sql).bind(...(q.params ?? [])))));
        case 'write': return Response.json(await db.batch([db.prepare('INSERT INTO ledger VALUES (2)'), db.prepare(${JSON.stringify(insert)})]));
        default: return Response.json((await db.prepare(${JSON.stringify(read.sql)}).all()).results);
      }
    } catch (e) {
      return Response.json({ message: e.message, cause: e.cause?.message }, { status: 503 });
    }
  } };`, { wrangler: { d1_databases: databases.map((db, index) => ({ binding: `DB${index}`, database_name: db.name })) } });
  const bindingPath = (i, op = 'read') => `/${tenant.name}/probe?db=${i}&op=${op}`;
  const binding = (i, op) => request(cluster.urls[ingress], bindingPath(i, op), { timeout: 40000 });
  for (const [i, db] of databases.entries()) {
    await eventually(`${db.name} is reachable by its D1 binding`, async () => {
      assert.deepEqual(json(await binding(i), 200, db.name), [{ id: 1 }]);
    });
  }

  const reason = 'SQL_LIMIT_EXCEEDED: wall limit 30000ms exceeded';
  const withTiming = async (label, run, check) => {
    const started = performance.now();
    const result = json(await run(), 503, label);
    const elapsed = performance.now() - started;
    check(result);
    assert.ok(elapsed >= 28500 && elapsed < 32000, `${label} took ${elapsed.toFixed(1)} ms`);
    console.log(`${label}: ${elapsed.toFixed(1)} ms`);
  };
  const checkHttp = result => {
    assert.equal(result.code, 'SQL_LIMIT_EXCEEDED', JSON.stringify(result));
    assert.equal(result.reason, reason);
  };
  const checkBinding = result => {
    assert.equal(result.message, `D1_ERROR: ${reason}`, JSON.stringify(result));
    assert.equal(result.cause, reason);
  };
  await Promise.all([
    withTiming('HTTP single deadline', () => sql(databases[0], { sql: select }), checkHttp),
    withTiming('HTTP batch deadline', () => sql(databases[1], batch), checkHttp),
    withTiming('D1 single deadline', () => binding(2, 'single'), checkBinding),
    withTiming('D1 batch deadline', () => binding(3, 'batch'), checkBinding),
    delay(2500).then(() => withTiming('HTTP deadline includes time queued behind another query',
      () => sql(databases[0], { sql: select }), checkHttp)),
  ]);
  for (const [i, db] of databases.entries()) {
    assert.deepEqual(json(await sql(db, read), 200, db.name).results, [{ id: 1 }]);
    assert.deepEqual(json(await binding(i), 200, db.name), [{ id: 1 }]);
  }

  const activity = async (running, waiting = 0) => {
    const snapshot = await durableGeneration(cluster, owner);
    assert.equal(snapshot.pid, original.pid);
    assert.equal(snapshot.generation, original.generation);
    assert.equal(snapshot.lifecycle.durableAdmission.running, running);
    assert.equal(snapshot.lifecycle.durableAdmission.waiting, waiting);
  };
  const sqlWire = (db, value) => {
    const url = new URL(`${db.path}/query`, cluster.urls.apiserver);
    const body = JSON.stringify(value);
    return `POST ${url.pathname} HTTP/1.1\r\nHost: ${url.host}\r\nAuthorization: Bearer ${tenant.token}\r\n` +
      `Content-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`;
  };
  const apiserverUrl = new URL(cluster.urls.apiserver);
  const halfClosed = createConnection({ host: apiserverUrl.hostname, port: Number(apiserverUrl.port) });
  const chunks = [];
  halfClosed.on('data', chunk => chunks.push(chunk));
  try {
    halfClosed.end(sqlWire(databases[0], { sql: medium }));
    await once(halfClosed, 'end', { signal: AbortSignal.timeout(5000) });
    const reply = Buffer.concat(chunks).toString();
    assert.match(reply, /^HTTP\/1\.1 200 /, 'a SQL client can half-close its write side');
    const body = JSON.parse(reply.slice(reply.indexOf('\r\n\r\n') + 4));
    assert.deepEqual(body.results, [{ 'sum(n)': 2000001000000 }], 'a half-closed client receives the complete result');
  } finally {
    halfClosed.destroy();
  }

  const write = [put(2), { sql: insert }];
  const checkQueuedHttp = result => {
    const failure = json(result, 503, 'HTTP read queued when a write reset the database');
    assert.equal(failure.code, 'DO_UNAVAILABLE', JSON.stringify(failure));
  };
  const checkQueuedBinding = result => {
    const failure = json(result, 503, 'D1 read queued when a write reset the database');
    assert.match(failure.message, /^D1_ERROR: .*DO_UNAVAILABLE/);
    assert.match(failure.cause, /DO_UNAVAILABLE/);
  };
  const openSamples = async () => (await cluster.state(owner)).durableLog.threadedHomeIo.openSamples;
  const reopened = async (db, label, run, results) => {
    const before = await openSamples();
    await eventually(`${label} reopens ${db.name} after its write was rolled back`, async () => {
      assert.deepEqual(results(json(await run(), 200, label)), [{ id: 1 }]);
    }, { timeout: 5000, interval: 50 });
    assert.equal(await openSamples(), before + 1, `${label} needed a fresh SQLite open`);
    await eventually(`${label} keeps the same process`, () => activity(0));
  };

  await eventually('SQL homes are idle before the live write deadlines', () => activity(0));
  const timedWrites = Promise.all([
    withTiming('HTTP write deadline', () => sql(databases[0], write), checkHttp),
    withTiming('D1 write deadline', () => binding(2, 'write'), checkBinding),
  ]);
  void timedWrites.catch(() => {});
  await eventually('both writes are running before requests queue behind them', () => activity(2));
  const queued = Promise.all([sql(databases[0], read), binding(2, 'read')]);
  void queued.catch(() => {});
  await eventually('an HTTP and a D1 query are queued behind the writes', () => activity(2, 2));
  await timedWrites;
  const [queuedHttp, queuedBinding] = await queued;
  checkQueuedBinding(queuedBinding);
  checkQueuedHttp(queuedHttp);
  await reopened(databases[0], 'HTTP after its write deadline', () => sql(databases[0], read, 1500), reply => reply.results);
  await reopened(databases[2], 'D1 after its write deadline', () => binding(2, 'read'), reply => reply);

  // Send the reset directly to the listener. A Docker port proxy can turn it into a FIN.
  const apiserver = await cluster.compose(['ps', '-q', 'apiserver']);
  const writer = spawn('docker', ['run', '--rm', '-i', '--network', `container:${apiserver}`,
    'node:22-trixie', 'node', '-e', `
      const { createConnection } = require('node:net');
      const { createInterface } = require('node:readline');
      const socket = createConnection({ host: '127.0.0.1', port: 8940 });
      const input = createInterface({ input: process.stdin });
      input.once('line', line => socket.write(JSON.parse(line)));
      input.once('close', () => socket.resetAndDestroy());
    `], { cwd: cluster.directory, env: cluster.env, stdio: ['pipe', 'ignore', 'inherit'], signal: cluster.signal });
  const writerClosed = once(writer, 'close');
  void writerClosed.catch(() => {});
  writer.stdin.on('error', () => {});
  try {
    writer.stdin.write(`${JSON.stringify(sqlWire(databases[0], write))}\n`);
    await eventually('the HTTP write is running before disconnect', () => activity(1));
    const behindDisconnect = sql(databases[0], read, 5000);
    void behindDisconnect.catch(() => {});
    await eventually('the read is queued behind the HTTP write', () => activity(1, 1));
    const disconnectedAt = performance.now();
    writer.stdin.end();
    assert.deepEqual(await writerClosed, [0, null], 'the TCP client sent its reset');
    checkQueuedHttp(await behindDisconnect);
    await reopened(databases[0], 'HTTP after disconnect', () => sql(databases[0], read, 1500), reply => reply.results);
    console.log(`HTTP disconnect and recovery: ${(performance.now() - disconnectedAt).toFixed(1)} ms`);
  } finally {
    writer.stdin.end();
  }
}

async function invocationBudgets(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant('invocation-budget');
  const bindings = [{ binding: 'CHILD', service: 'budget-child' }];
  const runner = `export default { async fetch(request, env) {
    const params = new URL(request.url).searchParams;
    const count = Number(params.get('count') ?? '1');
    const binding = params.has('lowerChild') ? env.LOWER_CHILD : env.CHILD;
    const target = new URL('https://binding.test/');
    if (params.has('childCalls')) target.searchParams.set('count', params.get('childCalls'));
    const limit = request.cf.lease.subrequestLimit;
    let result;
    for (let completed = 0; completed < count; ++completed) {
      const reply = await binding.fetch(target);
      const value = await reply.json();
      if (!reply.ok) return Response.json({ limit, completed, refusal: { status: reply.status, code: value.code } });
      result = value;
    }
    return Response.json({ limit, completed: count, result });
  } };`;
  const leaf = [{ binding: 'CHILD', service: 'budget-leaf' }];
  await cluster.deploy(tenant, 'budget-leaf', 'export default { fetch() { return Response.json("ok"); } };');
  await cluster.deploy(tenant, 'budget-child', runner, { wrangler: { services: leaf } });
  await cluster.deploy(tenant, 'budget-small-child', runner,
    { wrangler: { services: leaf, limits: { subrequests: 2 } } });
  await cluster.deploy(tenant, 'budget-root', runner,
    { wrangler: { services: [...bindings, { binding: 'LOWER_CHILD', service: 'budget-small-child' }] } });
  await cluster.deploy(tenant, 'budget-one', runner,
    { wrangler: { services: bindings, limits: { subrequests: 1 } } });
  const call = async (worker, query) => json(
    await workerCall(cluster, node, tenant, `${worker}?${query}`),
    200, 'the budget test Worker handled its request');
  const normal = await call('budget-root', 'count=100');
  assert.deepEqual(normal, { limit: 10_000, completed: 100, result: { limit: 10_000, completed: 1, result: 'ok' } });
  const nested = await call('budget-one', 'childCalls=200');
  assert.deepEqual(nested, { limit: 1, completed: 1, result: { limit: 10_000, completed: 200, result: 'ok' } });
  const exhausted = { status: 429, code: 'SUBREQUEST_BUDGET_EXHAUSTED' };
  const lowerChild = await call('budget-root', 'childCalls=3&lowerChild');
  assert.deepEqual(lowerChild, { limit: 10_000, completed: 1, result: { limit: 2, completed: 2, refusal: exhausted } });

  await cluster.deploy(tenant, 'budget-object', `
    import { DurableObject } from 'cloudflare:workers';
    export class BudgetActor extends DurableObject {
      async fetch(request) {
        if (new URL(request.url).pathname === '/leaf') return new Response('ok');
        const leaf = this.env.OBJECT.getByName('leaf');
        for (let completed = 0; completed < 200; ++completed) {
          const reply = await leaf.fetch('https://object.test/leaf');
          if (!reply.ok) return reply;
          await reply.text();
        }
        return Response.json({ completed: 200 });
      }
    }
    export default {
      fetch(_request, env) { return env.OBJECT.getByName('parent').fetch('https://object.test/parent'); }
    };
  `, { wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['BudgetActor'] }],
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'BudgetActor' }] } } });
  assert.deepEqual(json(await workerCall(cluster, node, tenant, 'budget-object'), 200, 'Durable Object subrequest budget'),
    { completed: 200 });

  await cluster.api(tenant.path, { method: 'PATCH', body: { defaultLimits: { subrequests: 5, d1Queries: 2 } } });
  const lower = await eventually('tenant budget is visible', async () => {
    const value = await call('budget-root', 'count=6');
    assert.equal(value.limit, 5);
    return value;
  });
  assert.deepEqual(lower, { limit: 5, completed: 5, refusal: exhausted });

  const wrangler = { services: bindings, limits: { subrequests: 10_000_000 } };
  await cluster.deploy(tenant, 'budget-wide', runner, { wrangler });
  const wide = await call('budget-wide', 'count=10');
  assert.equal(wide.limit, 10_000_000);
  assert.equal(wide.completed, 10);
  assert.equal(wide.refusal, undefined);

  const loop = 'budget-loop';
  await cluster.deploy(tenant, loop, 'export default { fetch(request, env) { return env.SELF.fetch(request); } };', {
    wrangler: { services: [{ binding: 'SELF', service: loop }], limits: { subrequests: 10_000 } },
  });
  const loopReply = await workerCall(cluster, node, tenant, loop);
  assert.equal(json(loopReply, 508, 'recursive binding hop limit').code, 'BINDING_HOP_LIMIT');

  await cluster.deploy(tenant, 'budget-egress', `export default { async fetch(request, env) {
    const reply = await env.CHILD.fetch('https://binding.test/');
    if (!reply.ok) return reply;
    await reply.text();
    try {
      await fetch('http://127.0.0.1/');
      return new Response('egress unexpectedly ran');
    } catch (error) {
      return new Response(error.message);
    }
  } };`, { wrangler: { services: bindings, limits: { subrequests: 1 } } });
  const egress = await workerCall(cluster, node, tenant, 'budget-egress');
  assert.equal(egress.status, 200);
  assert.match(egress.text, /SUBREQUEST_BUDGET_EXHAUSTED/);
}

async function d1InvocationBudgets(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant('d1-budget');
  await cluster.api(tenant.path, { method: 'PATCH', body: { defaultLimits: { subrequests: 5, d1Queries: 2 } } });
  const { storage: { tenantsRevision } } = await cluster.api('/v1/status');
  await eventually('the D1 budget is visible on every node', async () => {
    const states = await Promise.all(cluster.nodes.map(id => cluster.state(id)));
    assert.ok(states.every(state => BigInt(state.storage.appliedTenantsRevision) >= BigInt(tenantsRevision)));
  });
  await cluster.api(`${tenant.path}/databases/budget-db`, { method: 'POST', token: tenant.token });
  await cluster.deploy(tenant, 'budget-child', respondWith('ok'));
  await cluster.deploy(tenant, 'd1-edge', `
    async function exercise(env) {
      const statement = n => env.DB.prepare('SELECT ? AS value').bind(n);
      const batch = await env.DB.batch([statement(1), statement(2)]);
      const query = await statement(3).first('value');
      let refusal;
      try { await statement(4).run(); } catch (error) { refusal = error.message; }
      const bindings = [];
      for (let i = 0; i < 4; i++) {
        const reply = await env.CHILD.fetch('https://binding.test/');
        bindings.push(reply.ok ? await reply.text() : (await reply.json()).code);
      }
      return { batch: batch.map(result => result.results[0].value), query, refusal, bindings };
    }
    export default { async fetch(request, env) {
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') return Response.json(await exercise(env));
      const [client, socket] = Object.values(new WebSocketPair());
      socket.accept();
      socket.addEventListener('message', async () => {
        try { socket.send(JSON.stringify(await exercise(env))); }
        catch (error) { socket.send(JSON.stringify({ unexpected: error.message })); }
      });
      return new Response(null, { status: 101, webSocket: client });
    } };
  `, { wrangler: { services: [{ binding: 'CHILD', service: 'budget-child' }],
    d1_databases: [{ binding: 'DB', database_name: 'budget-db' }] } });
  const verify = ({ batch, query, refusal, bindings, unexpected }) => {
    assert.equal(unexpected, undefined);
    assert.deepEqual(batch, [1, 2]);
    assert.equal(query, 3);
    assert.match(refusal, /^D1_ERROR: D1_QUERY_BUDGET_EXHAUSTED:/);
    assert.deepEqual(bindings, ['ok', 'ok', 'ok', 'SUBREQUEST_BUDGET_EXHAUSTED']);
  };
  verify(json(await workerCall(cluster, node, tenant, 'd1-edge'), 200, 'D1 budget over HTTP'));
  const { WebSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const url = new URL(`/${tenant.name}/d1-edge`, cluster.urls[node]);
  url.protocol = 'ws:';
  const socket = new WebSocket(url);
  try {
    const waitFor = event => once(socket, event, { signal: AbortSignal.timeout(10000) });
    await waitFor('open');
    for (const message of ['first', 'second']) {
      const reply = waitFor('message');
      socket.send(message);
      verify(JSON.parse((await reply)[0]));
    }
  } finally {
    socket.terminate();
  }

  await cluster.deploy(tenant, 'd1-actor', `
    import { DurableObject } from 'cloudflare:workers';
    export class BudgetActor extends DurableObject {
      async fetch(request) {
        if (new URL(request.url).pathname === '/child') return new Response('child');
        const statement = n => this.env.DB.prepare('SELECT ? AS value').bind(n);
        const batch = await this.env.DB.batch([statement(1), statement(2)]);
        const query = await statement(3).first('value');
        let refusal;
        try { await statement(4).run(); } catch (error) { refusal = error.message; }
        const child = this.env.OBJECT.getByName('child');
        const ran = await child.fetch('https://object.test/child');
        const denied = await child.fetch('https://object.test/child');
        return Response.json({ batch: batch.map(result => result.results[0].value), query, refusal,
          child: await ran.text(), status: denied.status, code: (await denied.json()).code });
      }
    }
    export default {
      fetch(_request, env) {
        return env.OBJECT.getByName('parent').fetch('https://object.test/parent');
      }
    };
  `, { wrangler: { limits: { subrequests: 3 },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['BudgetActor'] }],
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'BudgetActor' }] },
    d1_databases: [{ binding: 'DB', database_name: 'budget-db' }] } });
  const object = json(await workerCall(cluster, node, tenant, 'd1-actor'), 200, 'D1 budget from a Durable Object');
  assert.deepEqual(object.batch, [1, 2]);
  assert.equal(object.query, 3);
  assert.match(object.refusal, /^D1_ERROR: D1_QUERY_BUDGET_EXHAUSTED:/);
  assert.deepEqual([object.child, object.status, object.code], ['child', 429, 'SUBREQUEST_BUDGET_EXHAUSTED']);
}

const budgetLifetime = { tenant: 'budget-lifetime', leaseMs: 5_000 };

async function invocationBudgetLifetime(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant(budgetLifetime.tenant);
  const withD1 = cluster.threadedNodes.size === 0;
  if (withD1) {
    await cluster.api(tenant.path, { method: 'PATCH', body: { defaultLimits: { d1Queries: 1 } } });
    const { storage: { tenantsRevision } } = await cluster.api('/v1/status');
    await eventually('the lifetime fixture has the new D1 limit', async () => {
      const states = await Promise.all(cluster.nodes.map(id => cluster.state(id)));
      assert.ok(states.every(state => BigInt(state.storage.appliedTenantsRevision) >= BigInt(tenantsRevision)));
    });
    await cluster.api(`${tenant.path}/databases/lifetime-db`, { method: 'POST', token: tenant.token });
  }
  await cluster.deploy(tenant, 'budget-lifetime', `
    import { DurableObject } from 'cloudflare:workers';
    const waitMs = ${budgetLifetime.leaseMs + 500};
    const origin = 'http://budget-origin:8080/';
    async function spend(env) {
      const first = await (await fetch(origin)).text();
      const second = env.DB ? await env.DB.prepare('SELECT 41 AS value').first('value') : await (await fetch(origin)).text();
      let extraFetch, extraQuery;
      try { await fetch(origin); } catch (error) { extraFetch = error.message; }
      if (env.DB) {
        try { await env.DB.prepare('SELECT 42').run(); } catch (error) { extraQuery = error.message; }
      }
      return { first, second, extraFetch, extraQuery };
    }
    export class BudgetLifetime extends DurableObject {
      async fetch(request) {
        const action = new URL(request.url).searchParams.get('action');
        if (action === 'schedule') {
          await this.ctx.storage.setAlarm(Date.now() + 50);
          return Response.json('scheduled');
        }
        if (action === 'result') return Response.json((await this.ctx.storage.get('result')) ?? null);
        if (action === 'alarm-started') return Response.json(this.alarmDone !== undefined);
        if (action === 'interleave') {
          const result = await spend(this.env).finally(() => this.interleavedSpend());
          await this.alarmDone;
          return Response.json(result);
        }
        if (action === 'timer') {
          const { invocationId, nodeId } = request.cf;
          setTimeout(() => {
            this.ctx.waitUntil(spend(this.env).then(
              result => console.log('budget-timer:' + JSON.stringify({ ...result, invocationId })),
              error => console.log('budget-timer:' + JSON.stringify({ unexpected: error.message, invocationId })),
            ));
          }, waitMs);
          return Response.json({ invocationId, nodeId });
        }
        if (action === 'start') {
          const first = await spend(this.env);
          const resumed = new Promise(resolve => { this.resumeBackground = resolve; });
          this.background = resumed.then(() => spend(this.env)).catch(error => ({ unexpected: error.message }));
          this.ctx.waitUntil(this.background);
          return Response.json(first);
        }
        await scheduler.wait(50);
        this.resumeBackground();
        return Response.json(await this.background);
      }
      async alarm() {
        const done = Promise.withResolvers();
        const interleaved = Promise.withResolvers();
        this.alarmDone = done.promise;
        this.interleavedSpend = interleaved.resolve;
        try {
          await Promise.all([scheduler.wait(waitMs), interleaved.promise]);
          const result = await spend(this.env).catch(error => ({ unexpected: error.message }));
          await this.ctx.storage.put('result', result);
        } finally {
          done.resolve();
        }
      }
    }
    export default { async fetch(request, env) {
      const action = new URL(request.url).searchParams.get('action');
      if (action === 'late-fetch') {
        await scheduler.wait(waitMs);
        const reply = await env.OBJECT.getByName('past-deadline').fetch('https://object.test/?action=result');
        const { code } = await reply.json();
        return Response.json({ ...await spend({}), invocationId: request.cf.invocationId,
          lateObject: { status: reply.status, code } });
      }
      const actor = action === 'timer' ? 'timer' :
        action === 'start' || action === 'resume' ? 'background' : 'alarm';
      return env.OBJECT.getByName(actor).fetch(request);
    } };
  `, { wrangler: { limits: { subrequests: 2 }, migrations: [{ tag: 'v1', new_sqlite_classes: ['BudgetLifetime'] }],
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'BudgetLifetime' }] },
    ...(withD1 ? { d1_databases: [{ binding: 'DB', database_name: 'lifetime-db' }] } : {}) } });

  const call = async action => json(await workerCall(cluster, node, tenant, `budget-lifetime?action=${action}`, { timeout: 20_000 }),
    200, `invocation budget lifetime ${action}`);
  const verify = (value, checksD1 = withD1) => {
    assert.equal(value.unexpected, undefined);
    assert.equal(value.first, 'origin');
    assert.equal(value.second, checksD1 ? 41 : 'origin');
    assert.match(value.extraFetch, /^SUBREQUEST_BUDGET_EXHAUSTED:/);
    if (checksD1) assert.match(value.extraQuery, /^D1_ERROR: D1_QUERY_BUDGET_EXHAUSTED:/);
  };
  assert.equal(await call('schedule'), 'scheduled');
  const timer = await call('timer');
  assert.ok(timer.invocationId && cluster.nodes.includes(timer.nodeId));
  await eventually('the alarm is waiting before the interleaved request', async () => {
    assert.equal(await call('alarm-started'), true);
  });
  // Exhaust a newer request's allowance while the alarm is waiting, and keep it live until the alarm calls out.
  const [late, interleaved] = await Promise.all([call('late-fetch'), call('interleave')]);
  verify(interleaved);
  verify(late, false);
  assert.deepEqual(late.lateObject, { status: 410, code: 'LEASE_EXPIRED' });
  assert.ok(late.invocationId);
  await eventually('the completed invocation releases its expired lease record', async () => {
    assert.ok((await cluster.state(node)).leases.every(lease => lease.invocationId !== late.invocationId));
  });
  await eventually('the alarm uses its budget after the lease expires', async () => {
    const result = await call('result');
    assert.ok(result);
    verify(result);
  });
  // Read the result out of band so the timer object receives no newer request.
  const timerResult = await eventually('the timer logs its result after the lease expires', async () => {
    const { lines } = await cluster.api(`${tenant.path}/workers/budget-lifetime/logs`, { token: tenant.token });
    const marker = lines.find(line => line.message?.startsWith('budget-timer:'));
    assert.ok(marker);
    return JSON.parse(marker.message.slice('budget-timer:'.length));
  });
  assert.equal(timerResult.invocationId, timer.invocationId);
  verify(timerResult);
  await eventually('the drained timer releases its budget on the owning node', async () => {
    assert.ok((await cluster.state(timer.nodeId)).leases.every(lease => lease.invocationId !== timer.invocationId));
  });
  // The original event spends both subrequests. Its resumed callback uses the newer request's balance.
  verify(await call('start'));
  verify(await call('resume'));
}

async function ordinaryProcess(cluster, node) {
  const { processes } = await cluster.state(node);
  const ordinary = processes.entries.filter(entry => entry.processClass === 'ordinary');
  assert.equal(ordinary.length, 1, `${node} must have exactly one ordinary slot`);
  return ordinary[0];
}

function workerCall(cluster, node, tenant, worker, options) {
  return request(cluster.urls[node], `/${tenant.name}/${worker}`, options);
}

async function cpuIsolation(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant('cpu-test');
  const burner = `export default { fetch(request) {
    if (new URL(request.url).pathname.endsWith('/burn')) {
      for (;;) { /* let the per-request CPU watchdog terminate this invocation */ }
    }
    return new Response('warm');
  } };`;
  await cluster.deploy(tenant, 'burner', burner);
  await cluster.deploy(tenant, 'neighbor', respondWith('neighbor'));
  await cluster.api(`${tenant.path}/workers/burner`, {
    token: tenant.token, method: 'PATCH', body: { limits: { cpuMs: 1500 } },
  });
  await eventually('both Workers warm in the target node', async () => {
    const state = await cluster.state(node);
    const status = await cluster.api('/v1/status');
    assert.equal(String(state.storage.appliedTenantsRevision), String(status.storage.tenantsRevision));
    assert.deepEqual(await workerCall(cluster, node, tenant, 'burner'), { status: 200, text: 'warm' });
    assert.deepEqual(await workerCall(cluster, node, tenant, 'neighbor'), { status: 200, text: 'neighbor' });
  });

  const before = await eventually('the warmed slot is idle', async () => {
    const slot = await ordinaryProcess(cluster, node);
    assert.equal(slot.inFlight, 0);
    return slot;
  });
  let burnFinished = false;
  const burn = workerCall(cluster, node, tenant, 'burner/burn', { timeout: 12000 })
    .then(response => ({ response }), error => ({ error }))
    .finally(() => { burnFinished = true; });
  try {
    await eventually('CPU-bound invocation is in the ordinary process', async () => {
      const current = await ordinaryProcess(cluster, node);
      assert.equal(current.pid, before.pid);
      assert.equal(current.inFlight, 1);
    }, { timeout: 1100, interval: 20 });
    assert.equal(burnFinished, false, 'the burner is still running when the neighbor starts');
    assert.deepEqual(await workerCall(cluster, node, tenant, 'neighbor', { timeout: 1000 }),
      { status: 200, text: 'neighbor' }, 'the neighbor answers within one second');
    assert.equal(burnFinished, false, 'the neighbor answers before the burner gets its limit response');
  } finally {
    await burn;
  }
  const { response, error } = await burn;
  if (error) throw error;
  assert.equal(json(response, 503, 'CPU-bound Worker').code, 'CPU_LIMIT_EXCEEDED');
  const after = await ordinaryProcess(cluster, node);
  assert.equal(after.pid, before.pid, 'both invocations used the same live process generation');
  assert.equal(after.generation, before.generation);
  assert.equal(after.dispatchedCount, before.dispatchedCount + 2, 'both invocations were dispatched to that process');
}

async function processReplacement(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant('restart-test');
  await cluster.deploy(tenant, 'hello', respondWith('alive'));
  await eventually('worker runs before the fault', async () => {
    assert.deepEqual(await workerCall(cluster, node, tenant, 'hello'), { status: 200, text: 'alive' });
  });

  const before = await ordinaryProcess(cluster, node);
  assert.ok(Number.isInteger(before.pid) && before.pid > 1);
  await cluster.compose(['exec', '-T', node, '/bin/sh', '-c', 'kill -KILL "$1"', 'sh', String(before.pid)]);
  await eventually(`${node} replaces ${before.processId} generation ${before.generation}`, async () => {
    const after = await ordinaryProcess(cluster, node);
    assert.equal(after.processId, before.processId);
    assert.notEqual(after.pid, before.pid);
    assert.ok(after.generation > before.generation, `generation is still ${after.generation}`);
    assert.equal(after.state, 'ready');
    assertStatus(await request(cluster.urls[node], '/__coworkerd/ready'), 200, `${node} readiness`);
  });
  const { processes } = await cluster.state(node);
  const exit = processes.recentFatalExits.find(entry => entry.pid === before.pid && entry.generation === before.generation);
  assert.equal(exit?.culpritStatus, 'undetermined', 'a process exit without evidence leaves the tenant unmarked');
  assert.equal(exit.culprit, null);
  assert.ok(!processes.suspects.entries.some(entry => entry.tenant === tenant.name));
  assert.deepEqual(await workerCall(cluster, node, tenant, 'hello'), { status: 200, text: 'alive' },
    'the first Worker call after the replacement is ready succeeds');
}

export async function fatalAttribution(cluster) {
  const [node, peer] = cluster.nodes;
  const tenant = await cluster.tenant('fatal-test');
  const neighbor = await cluster.tenant('fatal-neighbor');
  const target = `${tenant.name}/hello`;
  await cluster.deploy(tenant, 'hello', respondWith('hello'));
  await cluster.deploy(neighbor, 'hello', `export default { async fetch(request) {
    if (new URL(request.url).pathname.endsWith('/held')) {
      await new Promise(resolve => setTimeout(resolve, 15000));
    }
    return new Response('neighbor');
  } };`);
  const hello = (at = node) => workerCall(cluster, at, tenant, 'hello');
  const near = (worker = 'hello') => workerCall(cluster, node, neighbor, worker, { timeout: 18000 });
  const pool = async (at = node) => (await cluster.state(at)).processes;
  const sameGeneration = (entry, process) => entry.processId === process.processId && entry.generation === process.generation;
  const assertUncertain = (result, description) => assert.equal(json(result, 503, description).code, 'RESULT_UNCERTAIN');
  const assertQuarantineRoute = async (at, process) => {
    const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', at]);
    const prefix = '[execution] ';
    const call = logs.split('\n').filter(line => line.startsWith(prefix))
      .map(line => JSON.parse(line.slice(prefix.length)))
      .findLast(entry => entry.kind === 'ingress' && entry.target === `${target}@v1`);
    assert.equal(call?.processId, process.processId);
    assert.equal(call.processGeneration, process.generation);
  };
  const recovered = (process) => eventually('a new ordinary process is ready', async () => {
    const current = await ordinaryProcess(cluster, node);
    assert.equal(current.processId, process.processId);
    assert.ok(current.generation > process.generation);
    assert.equal(current.state, 'ready');
    return current;
  });
  const replacedQuarantine = (process) => eventually('a new quarantine process is ready', async () => {
    const current = (await pool()).entries.find(entry => entry.processId === process.processId);
    assert.ok(current?.generation > process.generation);
    assert.equal(current.state, 'ready');
    assert.ok(current.pid > 1);
    return current;
  });
  // What V8 itself said before the process with `pid` exited, from the node's log.
  const diagnosticOf = async (pid) => {
    const prefix = '[v8-fatal-diagnostic] ';
    const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', node]);
    return logs.split('\n').filter(line => line.startsWith(prefix))
      .map(line => JSON.parse(line.slice(prefix.length))).find(entry => entry.pid === pid);
  };
  const syntheticExit = async (process, event, check = result => assertUncertain(result, `synthetic ${event} from ${process.processClass}`)) => {
    const command = join(cluster.fatalControl, `command.${process.pid}`);
    const pending = `${command}.pending`;
    await writeFile(pending, `${event}\n${target}`, { flag: 'wx', mode: 0o644 });
    await rename(pending, command);
    try {
      check(await hello());
    } finally {
      await unlink(command);
    }
  };
  await eventually('both tenants run before the fault', async () => {
    assert.deepEqual(await hello(), { status: 200, text: 'hello' });
    assert.deepEqual(await near(), { status: 200, text: 'neighbor' });
    assert.deepEqual(await hello(peer), { status: 200, text: 'hello' });
  });

  const initial = await ordinaryProcess(cluster, node);
  await syntheticExit(initial, 'shared-oom');
  const before = await recovered(initial);
  const oom = (await pool()).recentMemoryCrashes.find(entry => sameGeneration(entry, initial));
  assert.equal(oom?.culpritStatus, 'undetermined');
  assert.equal(oom.culprit, null);
  assert.ok(!(await pool()).suspects.entries.some(entry => entry.tenant === tenant.name));
  assert.deepEqual(await hello(), { status: 200, text: 'hello' });

  const held = near('hello/held').then(response => ({ response }), error => ({ error }));
  let firstRoute;
  try {
    await eventually('the other tenant is in flight in the ordinary process', async () => {
      const current = await ordinaryProcess(cluster, node);
      assert.equal(current.pid, before.pid);
      assert.equal(current.inFlight, 1);
    }, { timeout: 5000, interval: 30 });
    // V8 reports this one itself, through the fatal handler the isolate ended up with.
    await syntheticExit(before, 'api-check');
    // Send the first call before polling state or waiting for an ordinary replacement.
    assert.deepEqual(await hello(), { status: 200, text: 'hello' });
    firstRoute = await pool();
  } finally {
    await held;
  }
  const { response, error } = await held;
  if (error) throw error;
  assertUncertain(response, 'neighbor affected by the ordinary exit');
  const first = firstRoute.recentFatalExits.find(entry => sameGeneration(entry, before));
  assert.equal(first?.culpritStatus, 'confirmed');
  assert.equal(first.culprit.workerId, target);
  assert.ok(first.inFlight.some(call => call.workerId === `${neighbor.name}/hello`));
  assert.deepEqual(await diagnosticOf(before.pid),
    { pid: before.pid, location: 'v8::ToLocalChecked', line: 0, message: 'Empty MaybeLocal' });
  const quarantine = firstRoute.entries.find(entry => entry.processClass === 'quarantine');
  assert.equal(quarantine?.state, 'ready');
  await assertQuarantineRoute(node, quarantine);
  const replacement = await recovered(before);
  assert.deepEqual(await near(), { status: 200, text: 'neighbor' });

  const adopted = await eventually('another node learns which tenant to quarantine', async () => {
    const view = await pool(peer);
    assert.ok(view.suspects.entries.some(entry => entry.tenant === tenant.name && entry.workerId === target));
    return view;
  });
  assert.deepEqual(await hello(peer), { status: 200, text: 'hello' });
  await assertQuarantineRoute(peer, adopted.entries.find(entry => entry.processClass === 'quarantine'));

  await syntheticExit(quarantine, 'fatal');
  const second = (await pool()).recentFatalExits.find(entry => sameGeneration(entry, quarantine));
  assert.equal(second?.culpritStatus, 'confirmed');
  assert.equal(second.culprit.workerId, target);
  assert.equal(second.processClass, 'quarantine');
  assert.deepEqual(await near(), { status: 200, text: 'neighbor' });
  assert.equal((await ordinaryProcess(cluster, node)).pid, replacement.pid);

  // V8's API check while the Worker loads cold in the replaced quarantine process, after its top level
  // ran (handler validation), where only the load's own scope names the Worker.
  const reloaded = await replacedQuarantine(quarantine);
  let loadResult;
  await syntheticExit(reloaded, 'load-api-check', result => { loadResult = result; });
  const third = (await pool()).recentFatalExits.find(entry => sameGeneration(entry, reloaded));
  assert.equal(third?.culpritStatus, 'confirmed', JSON.stringify(third));
  // The call that started the load is answered as a load failure.
  assert.equal(json(loadResult, 503, 'the call whose load ended the process').code, 'LOAD_FAILED');
  assert.equal(third.culprit.workerId, target);
  assert.equal((await diagnosticOf(reloaded.pid))?.message, 'Empty MaybeLocal');
  assert.deepEqual(await near(), { status: 200, text: 'neighbor' });
}

async function logQuota(cluster) {
  const [node] = cluster.nodes;
  const rate = 2;
  const buffer = 8192;
  const burst = 24;
  const limited = await cluster.tenant('log-limited', { logLinesPerSecond: rate, logBufferBytes: buffer });
  const other = await cluster.tenant('log-other');
  await cluster.deploy(limited, 'logger', `export default { fetch() {
    console.log('long-line:' + 'x'.repeat(8192));
    for (let i = 1; i < ${burst}; ++i) console.log('burst-' + i);
    return new Response('logged');
  } };`);
  await cluster.deploy(limited, 'latest', `export default { fetch() {
    console.log('latest-one:' + 'y'.repeat(3000));
    console.log('latest-two:' + 'z'.repeat(3000));
    return new Response('logged');
  } };`);
  await cluster.deploy(other, 'logger', `export default { fetch() {
    for (let i = 0; i < 4; ++i) console.log('other-' + i);
    return new Response('logged');
  } };`);
  const logs = (tenant, worker) => cluster.api(`${tenant.path}/workers/${worker}/logs`, { token: tenant.token });
  const onNode = (answer) => {
    const report = answer.nodes.find(entry => entry.nodeId === node);
    assert.ok(report, `logs must include ${node}`);
    assert.equal(report.problem, undefined);
    return report;
  };
  const log = { status: 200, text: 'logged' };
  assert.deepEqual(await Promise.all([
    workerCall(cluster, node, limited, 'logger'), workerCall(cluster, node, other, 'logger'),
  ]), [log, log]);

  const first = await eventually('all burst logs were processed and drops reported', async () => {
    const answer = await logs(limited, 'logger');
    const report = onNode(answer);
    assert.equal(report.keptLines + report.droppedLines, burst);
    assert.ok(answer.lines.some(line => line.reason === 'rate' && line.dropped > 0), 'missing rate-drop record');
    return answer;
  });
  const kept = first.lines.filter(line => typeof line.message === 'string');
  const windows = Map.groupBy(kept, line => Math.floor(line.atUnixMs / 1000));
  for (const lines of windows.values()) assert.ok(lines.length <= rate, `kept ${lines.length} lines in one second`);
  const long = kept.find(line => line.message.startsWith('long-line:'));
  assert.ok(long, 'the first line of the burst is visible');
  assert.equal(long.nodeId, node);
  assert.equal(long.truncated, true);
  assert.ok(Buffer.byteLength(long.message) <= 4096 && long.messageBytes > Buffer.byteLength(long.message),
    'the long line is marked and its retained message fits the documented 4 KiB limit');

  await eventually('the other tenant keeps all its lines on the same node', async () => {
    const answer = await logs(other, 'logger');
    assert.deepEqual(answer.lines.map(line => line.message), ['other-0', 'other-1', 'other-2', 'other-3']);
    assert.equal(onNode(answer).droppedLines, 0);
  });

  // The burst can straddle a second boundary. Let its last rate window expire before testing eviction.
  await delay(1000);
  assert.deepEqual(await workerCall(cluster, node, limited, 'latest'), log);
  const latest = await eventually('new log entries reach the tenant buffer', async () => {
    const answer = await logs(limited, 'latest');
    assert.equal(answer.lines.filter(line => line.message?.startsWith('latest-')).length, 2);
    return answer;
  });
  assert.ok(onNode(latest).bufferBytes <= buffer);
  assert.ok(onNode(latest).evictedLines > 0);
  assert.ok((await logs(limited, 'logger')).lines.every(line => !line.message?.startsWith('long-line:')),
    'new entries evict the oldest line across Workers of the same tenant');
}

async function heldPeerWrite(cluster, peers, url, value, readNeighbor) {
  await eventually('both peers finished releasing earlier acknowledgements', async () => {
    const observations = await Promise.all(peers.map(node => cluster.state(node)));
    assert.ok(observations.every(state => state.durableLog.peers.heldAcksForTest === 0));
  });
  const gate = join(cluster.peerAckGate, 'hold');
  await writeFile(gate, '');
  let receivedHeaders = false;
  const write = fetch(url, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify(value), signal: AbortSignal.timeout(10000),
  }).then(async response => {
    receivedHeaders = true;
    return { status: response.status, text: await response.text() };
  }).catch(error => ({ error }));
  let result;
  let early;
  try {
    await eventually(`both peers hold positive append acknowledgements for the ${value} write`, async () => {
      const observations = await Promise.all(peers.map(node => cluster.state(node)));
      assert.ok(observations.every(state => state.durableLog.peers.heldAcksForTest > 0));
    }, { timeout: 3000, interval: 25 });
    await readNeighbor();
    await delay(250);
    early = receivedHeaders;
  } finally {
    await unlink(gate);
    result = await write;
  }
  if (result.error) throw result.error;
  assert.equal(json(result, 200, `${value} write after acknowledgement release`), value);
  return early;
}

async function durablePeerAck(cluster) {
  const tenant = await cluster.tenant('durable-ack-test');
  const worker = 'object';
  await cluster.deploy(tenant, worker, `
    import { DurableObject } from 'cloudflare:workers';
    export class Record extends DurableObject {
      write(value) {
        void this.ctx.storage.put('value', value);
        return value;
      }
      read() { return this.ctx.storage.get('value'); }
      async fetch(request) {
        if (request.method === 'POST') return Response.json(this.write(await request.json()));
        return Response.json(await this.read() ?? null);
      }
    }
    export class Neighbor extends DurableObject {
      fetch() { return new Response('neighbor'); }
    }
    export default {
      async fetch(request, env) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/neighbor')) return env.NEIGHBORS.getByName('neighbor').fetch(request);
        const record = env.RECORDS.getByName('record');
        if (path.endsWith('/rpc')) return Response.json(await record.write(await request.json()));
        return record.fetch(request);
      },
    };
  `, { wrangler: {
    durable_objects: { bindings: [
      { name: 'RECORDS', class_name: 'Record' },
      { name: 'NEIGHBORS', class_name: 'Neighbor' },
    ] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record', 'Neighbor'] }],
  } });
  const [ingress] = cluster.nodes;
  const path = `/${tenant.name}/${worker}`;
  const read = suffix => request(cluster.urls[ingress], path + suffix);
  await eventually('both objects serve reads', async () => {
    assert.equal(json(await read(''), 200, 'warm record read'), null);
    assert.deepEqual(await read('/neighbor'), { status: 200, text: 'neighbor' });
  });
  const owner = await eventually('record has a live owner', async () => {
    const { owners } = await cluster.api(`${tenant.path}/workers/${worker}/namespaces/Record`, { token: tenant.token });
    assert.equal(owners.length, 1);
    assert.ok(!owners[0].released && cluster.nodes.includes(owners[0].nodeId));
    return owners[0].nodeId;
  });
  const peers = cluster.nodes.filter(node => node !== owner);
  const warm = await workerCall(cluster, ingress, tenant, worker, { method: 'POST', body: 'warm' });
  assert.equal(json(warm, 200, 'warm write'), 'warm');
  const before = await cluster.state(owner);
  assert.equal(before.durableLog.peers.want, peers.length);
  const earlyResponses = [];
  for (const [suffix, value] of [['', 'http'], ['/rpc', 'rpc']]) {
    const early = await heldPeerWrite(cluster, peers, cluster.urls[ingress] + path + suffix, value, async () => {
      assert.deepEqual(await read('/neighbor'), { status: 200, text: 'neighbor' });
    });
    if (early) earlyResponses.push(value);
    assert.equal(json(await read(''), 200, `${value} stored value`), value);
  }
  assert.deepEqual(earlyResponses, [], 'responses arrived while both peers held their acknowledgements');
}

async function takeoverWriter(cluster, tenantName) {
  const tenant = await cluster.tenant(tenantName);
  await cluster.deploy(tenant, 'object', `
    import { DurableObject } from 'cloudflare:workers';
    export class Record extends DurableObject {
      async fetch(request) {
        if (request.method === 'POST') {
          const id = await request.json();
          await this.ctx.storage.put(String(id), true);
          return Response.json(id);
        }
        return Response.json(Array.from((await this.ctx.storage.list()).keys(), Number));
      }
    }
    export default { fetch(request, env) { return env.RECORDS.getByName('takeover').fetch(request); } };
  `, { wrangler: {
    durable_objects: { bindings: [{ name: 'RECORDS', class_name: 'Record' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record'] }],
  } });
  return tenant;
}

async function durableTakeover(cluster, platformTenant) {
  const tenant = platformTenant ?? await takeoverWriter(cluster, 'durable-test');
  const [node] = cluster.nodes;
  const namespace = () => cluster.api(`${tenant.path}/workers/object/namespaces/Record`, { token: tenant.token });
  await eventually('object and its shard are ready', async () => {
    assert.deepEqual(json(await workerCall(cluster, node, tenant, 'object'), 200, 'initial object read'), []);
  });
  const original = await eventually('public namespace state identifies the owner', async () => {
    const { owners } = await namespace();
    assert.equal(owners.length, 1);
    assert.ok(cluster.nodes.includes(owners[0].nodeId) && !owners[0].released);
    return owners[0];
  });
  await eventually('the owner sees both surviving members', async () => {
    const { membership } = await cluster.state(original.nodeId);
    const peers = membership.members.filter(member => member.durableProcesses > 0).map(member => member.nodeId);
    for (const peer of cluster.nodes.filter(id => id !== original.nodeId)) assert.ok(peers.includes(peer), `missing ${peer}`);
  });

  const survivor = cluster.nodes.find(id => id !== original.nodeId);
  const concurrency = 16;
  const warm = Promise.withResolvers();
  const acknowledged = new Set();
  let sent = 0;
  let pending = 0;
  let interrupted = 0;
  let writing = true;
  let killing = false;
  const writers = Promise.allSettled(Array.from({ length: concurrency }, async () => {
    try {
      while (writing) {
        const id = ++sent;
        ++pending;
        let response;
        try {
          response = await workerCall(cluster, survivor, tenant, 'object', { method: 'POST', body: id, timeout: 10000 });
        } catch (error) {
          if (!killing) throw error;
        } finally {
          --pending;
        }
        if (killing && (!response || response.status >= 500)) {
          ++interrupted;
          writing = false;
          return;
        }
        assert.equal(json(response, 200, `object write ${id}`), id);
        acknowledged.add(id);
        if (acknowledged.size === concurrency) warm.resolve();
      }
    } catch (error) {
      writing = false;
      warm.reject(error);
      throw error;
    }
  }));
  let inFlightAtKill;
  let results;
  let gate;
  try {
    await warm.promise;
    if (cluster.threadedNodes.size) {
      assert.equal((await durableGeneration(cluster, original.nodeId)).lifecycle?.durableServer, 'threaded');
      gate = join(cluster.peerAckGate, 'hold');
      await writeFile(gate, '');
      await eventually('the surviving peers withhold positive confirmations during active writes', async () => {
        const peers = cluster.nodes.filter(node => node !== original.nodeId);
        const states = await Promise.all(peers.map(node => cluster.state(node)));
        assert.ok(states.every(state => state.durableLog.peers.heldAcksForTest > 0));
      }, { timeout: 3000, interval: 25 });
    }
    inFlightAtKill = pending;
    assert.ok(inFlightAtKill > 0, 'writes are in flight when the kill is requested');
    killing = true;
    await cluster.compose(['kill', '-s', 'SIGKILL', original.nodeId]);
  } finally {
    writing = false;
    if (gate) await unlink(gate);
    results = await writers;
  }
  for (const result of results) {
    if (result.status === 'rejected') throw result.reason;
  }
  console.log(`Durable writes through ${survivor} to ${original.nodeId}: ${acknowledged.size}/${sent} acknowledged, ` +
    `${inFlightAtKill} pending at kill request, ${interrupted} interrupted`);
  assert.ok(interrupted > 0, 'the owner loss interrupted at least one write');

  const recovered = await eventually('a surviving node serves a read after owner loss', async () => {
    const read = await workerCall(cluster, survivor, tenant, 'object', { timeout: 35000 });
    return json(read, 200, 'takeover read');
  }, { timeout: 100000, interval: 1000 });
  const stored = new Set(recovered);
  const missing = [...acknowledged].filter(id => !stored.has(id));
  assert.equal(missing.length, 0, `takeover lost acknowledged writes: ${missing.join(', ')}`);
  assert.ok(recovered.every(id => Number.isSafeInteger(id) && id > 0 && id <= sent), 'takeover returned an unsent write');
  await eventually('the shard has a new live owner and a later epoch', async () => {
    const { owners } = await namespace();
    const owner = owners.find(entry => entry.shard === original.shard);
    assert.ok(owner && !owner.released && owner.nodeId !== original.nodeId && cluster.nodes.includes(owner.nodeId));
    assert.ok(BigInt(owner.epoch) > BigInt(original.epoch));
  });
}

async function threadedPlatformDefinition(cluster) {
  const tenants = await Promise.all(['platform-first', 'platform-second'].map(name => cluster.tenant(name)));
  const classNames = ['Record', 'Probe', 'Echo'];
  const wrangler = {
    durable_objects: { bindings: classNames.map(name => ({ name: name.toUpperCase(), class_name: name })) },
    migrations: [{ tag: 'v1', new_sqlite_classes: classNames }],
  };
  const worker = `
    import { DurableObject } from 'cloudflare:workers';
    export class Record extends DurableObject { fetch() { return new Response('tenant Record ran', { status: 500 }); } }
    export class Probe extends DurableObject { fetch() { return new Response('tenant Probe ran', { status: 500 }); } }
    export class Echo extends DurableObject { fetch() { return new Response('tenant Echo ran', { status: 500 }); } }
    export default { fetch(request, env) {
      const params = new URL(request.url).searchParams;
      if (params.has('echo')) return env.ECHO.getByName('echo').fetch(request);
      const probe = params.get('probe');
      return probe === null ? env.RECORD.getByName('takeover').fetch(request) : env.PROBE.getByName(probe).fetch(request);
    } };
  `;
  for (const tenant of tenants) await cluster.deploy(tenant, 'object', worker, { wrangler });
  const [ingress] = cluster.nodes;
  const call = (tenant, name, node = ingress, action = 'identity') => workerCall(cluster, node, tenant,
    `object?${new URLSearchParams({ probe: name, action })}`);
  const generations = await eventually('all fixture nodes have a ready threaded durable generation', async () => {
    const entries = await Promise.all(cluster.nodes.map(async node => [node, await durableGeneration(cluster, node)]));
    for (const [, generation] of entries) assert.equal(generation.lifecycle.durableServer, 'threaded');
    return new Map(entries);
  });
  await eventually('the second class loads through the same definition', async () => {
    const echo = json(await workerCall(cluster, ingress, tenants[0], 'object?echo'), 200, 'second platform class');
    assert.equal(echo.service, 'echo');
    assert.ok(cluster.nodes.includes(echo.build));
  });

  const observations = tenants.map(() => new Map());
  let sameOwner;
  for (let attempt = 0; attempt < 24; ++attempt) {
    for (const [index, tenant] of tenants.entries()) {
      const name = `copy-${attempt}`;
      const identity = await eventually(`${tenant.name}/${name} runs the platform class`, async () =>
        json(await call(tenant, name), 200, 'platform copy identity'));
      assert.ok(cluster.nodes.includes(identity.build));
      assert.deepEqual(identity.env, []);
      const seen = observations[index];
      if (!seen.has(identity.build)) seen.set(identity.build, []);
      seen.get(identity.build).push({ name, ...identity });
    }
    sameOwner = cluster.nodes.find(node => observations.every(seen => seen.get(node)?.length >= 2));
    if (sameOwner && observations[0].size >= 2) break;
  }
  assert.ok(sameOwner, 'two objects of each tenant run in the same durable process');
  const [first, second] = observations.map(seen => seen.get(sameOwner).slice(0, 2));
  for (const [one, two] of [first, second]) {
    assert.equal(one.copy, two.copy, 'objects of the same tenant reuse the warm copy');
    assert.notEqual(one.instance, two.instance, 'the calls reached different objects');
  }
  assert.notEqual(first[0].copy, second[0].copy, 'different tenants never share the warm copy');
  const process = await durableGeneration(cluster, sameOwner);
  assert.equal(process.pid, generations.get(sameOwner).pid);
  assert.equal(process.generation, generations.get(sameOwner).generation);

  const otherOwner = [...observations[0].keys()].find(node => node !== sameOwner);
  assert.ok(otherOwner, 'the namespace placed objects on two fixture builds');
  const [other] = observations[0].get(otherOwner);
  for (const [sender, owner, object] of [[sameOwner, otherOwner, other], [otherOwner, sameOwner, first[0]]]) {
    const identity = json(await call(tenants[0], object.name, sender), 200, `${sender} forwards to ${owner}`);
    assert.equal(identity.build, owner);
    assert.equal(identity.implementation, `PlatformStore${owner.at(-1)}`, 'the owner selects its own exported class');
    assert.equal(identity.instance, object.instance, 'the forwarded call reaches the existing object');
  }

  const deniedTenant = await cluster.tenant('platform-denied');
  await assert.rejects(cluster.deploy(deniedTenant, 'object', 'export default { fetch() {} };', { wrangler: {
    durable_objects: { bindings: [{ name: 'STORE', class_name: 'PlatformStore' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['PlatformStore'] }],
  } }), /PlatformStore|class export/i);
  await assert.rejects(cluster.deploy(deniedTenant, 'object', worker, { wrangler: {
    durable_objects: { bindings: [{ name: 'PROBE', class_name: 'Probe', script_name: 'platform-first/object' }] },
  } }), /script_name names Worker/);
  const crossTenant = await request(cluster.urls.apiserver, `${tenants[0].path}/workers/object/namespaces/Probe`, {
    token: tenants[1].token,
  });
  assert.ok(crossTenant.status === 403 || crossTenant.status === 404, 'another tenant cannot access the platform namespace');

  const { name, instance } = first[0];
  assert.equal(json(await call(tenants[0], name, ingress, 'alarm'), 200, 'set platform alarm'), true);
  await eventually('the platform alarm commits on the owner', async () => {
    assert.equal(json(await call(tenants[0], name, ingress, 'alarmed'), 200, 'platform alarm result'), 1);
  });
  const advance = join(cluster.durableLifecycleGate, 'idle-offset-next');
  await writeFile(advance, '180000');
  await rename(advance, join(cluster.durableLifecycleGate, 'idle-offset-ms'));
  await eventually('idle platform objects release their home sessions', async () => {
    const { durableAdmission } = (await durableGeneration(cluster, sameOwner)).lifecycle;
    assert.equal(durableAdmission.residentRoots, 0);
  });
  assert.notEqual(json(await call(tenants[0], name), 200, 'reopen platform object').instance, instance);
  assert.equal(json(await call(tenants[0], name, ingress, 'alarmed'), 200, 'reopen persisted alarm result'), 1);

  await durableTakeover(cluster, tenants[0]);
}

function assertDurableCapacityRefusal(result) {
  assert.equal(result.code, 'OVERLOADED');
  assert.equal(result.dispatched, false);
  assert.equal(result.retryable, true);
  assert.equal(result.reason, 'The durable process refused the call before it started. The operation did not execute and can be retried.');
}

async function durableAdmissionRefusal(cluster) {
  await waitForDurableMembership(cluster);
  const [objectOwner, databaseOwner] = cluster.nodes;
  const tenant = await cluster.tenant('durable-refusal', { durableObjectNamespaces: 48 });
  const { databases } = await createDatabaseHomes(cluster, tenant, new Map([[databaseOwner, 1]]));
  const [db] = databases.get(databaseOwner);
  await cluster.deploy(tenant, 'counter', `
    import { DurableObject } from 'cloudflare:workers';
    async function call(env, name, op) {
      const stub = env.COUNTER.getByName(name);
      if (op !== 'rpc') return stub.fetch('http://object/' + op, op === 'write' ? { method: 'POST', body: '1' } : {});
      try { return Response.json(await stub.write()); }
      catch (e) { return Response.json({ code: e.code, message: e.message }, { status: 503 }); }
    }
    export class Counter extends DurableObject {
      attempts = 0;
      async write() {
        this.attempts++;
        const count = 1 + ((await this.ctx.storage.get('count')) ?? 0);
        await this.ctx.storage.put('count', count);
        return { count, attempts: this.attempts };
      }
      async fetch(request) {
        const { pathname, searchParams } = new URL(request.url);
        if (pathname === '/write') {
          if (await request.text() !== '1') return new Response('missing write body', { status: 400 });
          return Response.json(await this.write());
        }
        if (pathname === '/relay') return call(this.env, searchParams.get('target'), searchParams.get('op'));
        return Response.json({ count: (await this.ctx.storage.get('count')) ?? 0, attempts: this.attempts });
      }
    }
    export default { async fetch(request, env) {
      const { searchParams } = new URL(request.url);
      const op = searchParams.get('op');
      if (op === 'd1' || op === 'exec') {
        try {
          const result = op === 'd1' ? await env.DB.prepare('INSERT INTO ledger VALUES (2)').run()
            : await env.DB.exec('INSERT INTO ledger VALUES (3)');
          return Response.json(result);
        }
        catch (e) { return Response.json({ message: e.message, cause: e.cause?.message }, { status: 503 }); }
      }
      if (searchParams.has('via')) {
        return env.COUNTER.getByName(searchParams.get('via')).fetch('http://object/relay?' + searchParams);
      }
      return call(env, searchParams.get('target'), op);
    } };
  `, { wrangler: {
    durable_objects: { bindings: [{ name: 'COUNTER', class_name: 'Counter' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Counter'] }],
    d1_databases: [{ binding: 'DB', database_name: db.name }],
  } });
  const { namespaceId, shards } = await cluster.api(`${tenant.path}/workers/counter/namespaces/Counter`, { token: tenant.token });
  const names = node => Array.from({ length: 64 }, (_, i) => `counter-${i}`).filter(name => {
    const id = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update(name).digest();
    return shardOwnerOrder(cluster, namespaceId, Number(id.readBigUInt64BE() % BigInt(shards)))[0] === node;
  });
  const [blocker, target] = names(objectOwner);
  const [caller] = names(databaseOwner);
  assert.ok(blocker && target && caller, 'the fixture has objects on both owners');
  const call = (node, params) => workerCall(cluster, node, tenant, `counter?${new URLSearchParams(params)}`);
  const read = (node, target) => call(node, { target, op: 'read' });
  const zero = { count: 0, attempts: 0 };
  await eventually('the blocker and relay are resident on separate nodes', async () => {
    assert.deepEqual(json(await read(objectOwner, blocker), 200, 'blocker'), zero);
    assert.deepEqual(json(await read(databaseOwner, caller), 200, 'relay'), zero);
  });
  const initial = await eventually('the target owner publishes the occupied slot', async () => {
    const snapshot = await durableGeneration(cluster, objectOwner);
    assert.equal(snapshot.lifecycle.durableAdmission.residentRoots, 1);
    assert.equal(snapshot.inFlight, 0);
    return snapshot;
  });
  const calls = [
    ['Worker fetch', () => call(objectOwner, { target, op: 'write' }), false],
    ['Worker RPC', () => call(objectOwner, { target, op: 'rpc' }), true],
    ['object fetch', () => call(databaseOwner, { target, op: 'write', via: caller }), false],
    ['object RPC', () => call(databaseOwner, { target, op: 'rpc', via: caller }), true],
  ];
  for (const [label, write, rpc] of calls) {
    const reply = json(await write(), 503, `${label} refused before dispatch`);
    if (rpc) {
      assert.equal(reply.code, 'OVERLOADED');
      assert.match(reply.message, /^OVERLOADED: /);
      assertDurableCapacityRefusal(JSON.parse(reply.message.slice('OVERLOADED: '.length)));
    } else assertDurableCapacityRefusal(reply);
  }
  await eventually('only the child overload counter changed on the target owner', async () => {
    const after = await durableGeneration(cluster, objectOwner);
    assert.equal(after.generation, initial.generation);
    assert.equal(after.dispatchedCount, initial.dispatchedCount);
    assert.equal(after.lifecycle.durableAdmission.overloaded, initial.lifecycle.durableAdmission.overloaded + calls.length);
  });
  const advance = join(cluster.durableLifecycleGate, 'idle-offset-next');
  await writeFile(advance, '180000');
  await rename(advance, join(cluster.durableLifecycleGate, 'idle-offset-ms'));
  await eventually('the blocker retires and releases capacity', async () => {
    assert.equal((await durableGeneration(cluster, objectOwner)).lifecycle.durableAdmission.residentRoots, 0);
  });
  assert.deepEqual(json(await read(objectOwner, target), 200, 'target after refusals'), zero);
  for (const [index, [label, write]] of calls.entries()) {
    assert.deepEqual(json(await write(), 200, `${label} retried once`), { count: index + 1, attempts: index + 1 });
  }
  assert.deepEqual(json(await read(objectOwner, target), 200, 'target after retries'), { count: 4, attempts: 4 });

  const sql = (sql, timeout = 10000) => request(cluster.urls.apiserver, `${db.path}/query`, {
    method: 'POST', token: tenant.token, body: { sql }, timeout,
  });
  const d1 = (op = 'd1') => call(databaseOwner, { op });
  const insert = 'INSERT INTO ledger VALUES (1)';
  const rows = 'SELECT value FROM ledger ORDER BY value';
  await eventually('the threaded database is ready', async () => {
    assert.equal(json(await sql('SELECT 1'), 200, 'database ready').success, true);
  });
  assert.equal(json(await sql('CREATE TABLE ledger(value INTEGER)'), 200, 'create ledger').success, true);
  const peers = await waitForWalPeers(cluster, databaseOwner);
  const gate = join(cluster.peerAckGate, 'hold');
  await writeFile(gate, '');
  const held = sql('INSERT INTO ledger VALUES (0)');
  void held.catch(() => {});
  let queued;
  try {
    await eventually('the first SQL write is held by the output gate', async () => {
      const states = await Promise.all(peers.map(node => cluster.state(node)));
      assert.ok(states.every(state => state.durableLog.peers.heldAcksForTest > 0));
    });
    queued = sql(rows);
    void queued.catch(() => {});
    const before = await eventually('one read fills the database mailbox', async () => {
      const snapshot = await durableGeneration(cluster, databaseOwner);
      assert.equal(snapshot.lifecycle.durableAdmission.running, 1);
      assert.equal(snapshot.lifecycle.durableAdmission.waiting, 1);
      return snapshot;
    });
    const http = json(await sql(insert), 503, 'public HTTP SQL refused before dispatch');
    assertDurableCapacityRefusal(http);
    const binding = json(await d1(), 503, 'D1 refused before dispatch');
    assert.equal(binding.cause, `OVERLOADED: ${http.reason}`);
    assert.equal(binding.message, `D1_ERROR: ${binding.cause}`);
    const exec = json(await d1('exec'), 503, 'D1 exec refused before dispatch');
    assert.equal(exec.cause, `Error in line 1: INSERT INTO ledger VALUES (3): ${binding.cause}`);
    assert.equal(exec.message, `D1_EXEC_ERROR: ${exec.cause}`);
    await eventually('the full mailbox refused all three SQL writes without dispatch', async () => {
      const after = await durableGeneration(cluster, databaseOwner);
      assert.equal(after.generation, before.generation);
      assert.equal(after.dispatchedCount, before.dispatchedCount);
      assert.equal(after.lifecycle.durableAdmission.overloaded, before.lifecycle.durableAdmission.overloaded + 3);
    });
  } finally {
    await unlink(gate);
    await Promise.allSettled([held, queued]);
  }
  assert.equal(json(await held, 200, 'held insert after release').success, true);
  assert.deepEqual(json(await queued, 200, 'queued read after refusals').results, [{ value: 0 }]);
  assert.equal(json(await sql(insert), 200, 'HTTP SQL retry').success, true);
  assert.equal(json(await d1(), 200, 'D1 retry').success, true);
  assert.equal(json(await d1('exec'), 200, 'D1 exec retry').count, 1);
  assert.deepEqual(json(await sql(rows), 200, 'one row per retried insert').results,
    [0, 1, 2, 3].map(value => ({ value })));
}

async function durableCopyReservationRefusal(cluster) {
  const [owner, ingress] = cluster.nodes;
  await waitForDurableMembership(cluster);
  const tenant = await cluster.tenant('copy-reservation-refusal', { durableObjectNamespaces: 48 });
  const { databases } = await createDatabaseHomes(cluster, tenant, new Map([[owner, 1]]));
  const [db] = databases.get(owner);
  const sql = sql => request(cluster.urls.apiserver, `${db.path}/query`, {
    method: 'POST', token: tenant.token, body: { sql }, timeout: 10000,
  });
  assert.equal(json(await sql('CREATE TABLE ledger(value INTEGER PRIMARY KEY)'), 200, 'create SQL ledger').success, true);
  await waitForWalPeers(cluster, owner);
  const marker = join(cluster.durableLifecycleGate, 'copy-reservation-refusal');
  let refusal = 0;
  const refuseNext = async call => {
    const initial = await durableGeneration(cluster, owner);
    await writeFile(`${marker}-next`, String(++refusal));
    await rename(`${marker}-next`, marker);
    const reply = await call();
    await eventually('the refused event released its mailbox credit in the same process', async () => {
      const current = await durableGeneration(cluster, owner);
      assert.equal(current.generation, initial.generation);
      assert.equal(current.lifecycle.durableAdmission.overloaded, initial.lifecycle.durableAdmission.overloaded);
      assert.equal(current.lifecycle.durableAdmission.running, 0);
      assert.equal(current.lifecycle.durableAdmission.waiting, 0);
    });
    return reply;
  };

  const worker = `
    import { DurableObject } from 'cloudflare:workers';
    export class Counter extends DurableObject {
      attempts = 0;
      async count() { return (await this.ctx.storage.get('count')) ?? 0; }
      async write() {
        ++this.attempts;
        const count = 1 + await this.count();
        await this.ctx.storage.put('count', count);
        return { count, attempts: this.attempts };
      }
      async writeThenThrow() {
        await this.write();
        throw new Error('OVERLOADED: process admission budget is full');
      }
      async fetch(request) {
        if (request.method === 'POST') return Response.json(await this.write());
        return Response.json({ count: await this.count(), attempts: this.attempts });
      }
    }
    export default { async fetch(request, env) {
      const params = new URL(request.url).searchParams;
      const op = params.get('op');
      if (op === 'run' || op === 'exec') {
        try {
          return Response.json(op === 'run' ? await env.DB.prepare('INSERT INTO ledger VALUES (2)').run()
            : await env.DB.exec('INSERT INTO ledger VALUES (3)'));
        } catch (e) { return Response.json({ message: e.message, cause: e.cause?.message }, { status: 503 }); }
      }
      const stub = env.COUNTER.getByName(params.get('name'));
      if (op === 'read') return stub.fetch('http://counter/');
      if (op === 'fetch') return stub.fetch('http://counter/', { method: 'POST', body: '1' });
      try { return Response.json(op === 'throw' ? await stub.writeThenThrow() : await stub.write()); }
      catch (e) { return Response.json({ code: e.code, message: e.message }, { status: 503 }); }
    } };
  `;
  const wrangler = {
    durable_objects: { bindings: [{ name: 'COUNTER', class_name: 'Counter' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Counter'] }],
    d1_databases: [{ binding: 'DB', database_name: db.name }],
  };
  const callWorker = (workerName, op, name = '') => workerCall(cluster, ingress, tenant,
    `${workerName}?${new URLSearchParams({ name, op })}`);
  await cluster.deploy(tenant, 'counter-fetch', worker, { wrangler });

  const rows = async () => json(await sql('SELECT value FROM ledger ORDER BY value'), 200, 'SQL ledger').results;
  const executedSql = async () => (await cluster.state(owner)).executions
    .filter(row => row.target.startsWith(`${tenant.name}/__databases@`) && row.dispatched === true).length;
  let offset = 0;
  const expected = [];
  const writes = [
    () => sql('INSERT INTO ledger VALUES (1)'),
    () => callWorker('counter-fetch', 'run'),
    () => callWorker('counter-fetch', 'exec'),
  ];
  const waitForDatabaseCopy = (label, count) => eventually(label, async () => {
    const { lifecycle } = await durableGeneration(cluster, owner);
    assert.equal(lifecycle.durableAdmission.residentRoots, count);
    assert.equal(lifecycle.durableAdmission.homes, count);
  });
  for (const [index, write] of writes.entries()) {
    await waitForDatabaseCopy('the current database root and copy are reported resident', 1);
    offset += 180000;
    const advance = join(cluster.durableLifecycleGate, 'idle-offset-next');
    await writeFile(advance, String(offset));
    await rename(advance, join(cluster.durableLifecycleGate, 'idle-offset-ms'));
    await waitForDatabaseCopy('the database root and its exclusive copy retire', 0);
    const before = await executedSql();
    const reply = json(await refuseNext(write), 503, `SQL caller ${index + 1} received refusal`);
    if (index === 0) {
      assertDurableCapacityRefusal(reply);
    } else {
      const reason = 'OVERLOADED: The durable process refused the call before it started. The operation did not execute and can be retried.';
      assert.equal(reply.cause, index === 1 ? reason : `Error in line 1: INSERT INTO ledger VALUES (3): ${reason}`);
      assert.equal(reply.message, `${index === 1 ? 'D1_ERROR' : 'D1_EXEC_ERROR'}: ${reply.cause}`);
    }
    assert.equal(await executedSql(), before, 'the refusal did not count as an executed SQL request');
    assert.deepEqual(await rows(), expected, 'the refused insert left the ledger unchanged');
    json(await write(), 200, `SQL caller ${index + 1} retries once`);
    expected.push({ value: index + 1 });
    assert.deepEqual(await rows(), expected, 'one row per retried operation');
  }
  const before = await executedSql();
  const conflict = json(await writes[0](), 200, 'SQL handler attempts a duplicate insert');
  assert.equal(conflict.success, false);
  assert.match(conflict.error, /UNIQUE constraint failed/);
  assert.equal(conflict.retryable, undefined);
  assert.equal(await executedSql(), before + 1);
  assert.deepEqual(await rows(), expected);

  for (const op of ['fetch', 'rpc']) {
    // Each definition has its own cold copy. The stateless caller runs on a peer.
    const workerName = `counter-${op}`;
    if (op === 'rpc') await cluster.deploy(tenant, workerName, worker, { wrangler });
    const { namespaceId, shards } = await cluster.api(`${tenant.path}/workers/${workerName}/namespaces/Counter`, { token: tenant.token });
    const name = Array.from({ length: 64 }, (_, i) => `counter-${i}`).find(candidate => {
      const id = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update(candidate).digest();
      return shardOwnerOrder(cluster, namespaceId, Number(id.readBigUInt64BE() % BigInt(shards)))[0] === owner;
    });
    assert.ok(name, `the ${op} target belongs to ${owner}`);
    const call = operation => callWorker(workerName, operation, name);
    const reply = json(await refuseNext(() => call(op)), 503, `${op} copy reservation refused`);
    if (op === 'rpc') {
      assert.equal(reply.code, 'OVERLOADED');
      assert.match(reply.message, /^OVERLOADED: /);
      assertDurableCapacityRefusal(JSON.parse(reply.message.slice('OVERLOADED: '.length)));
    } else assertDurableCapacityRefusal(reply);
    assert.deepEqual(json(await call('read'), 200, `${op} target after refusal`), { count: 0, attempts: 0 });
    assert.deepEqual(json(await call(op), 200, `${op} retry`), { count: 1, attempts: 1 });
    if (op === 'rpc') {
      const error = json(await call('throw'), 503, 'application exception after the second write');
      assert.equal(error.code, undefined);
      assert.equal(error.message, 'OVERLOADED: process admission budget is full');
      assert.deepEqual(json(await call('read'), 200, 'RPC target after the exception'), { count: 2, attempts: 2 });
    }
  }
}

async function durableSlowHolder(cluster) {
  const tenant = await takeoverWriter(cluster, 'slow-holder-test');
  const [first] = cluster.nodes;
  const namespace = () => cluster.api(`${tenant.path}/workers/object/namespaces/Record`, { token: tenant.token });
  await eventually('the original holder confirms the value', async () => {
    const response = await workerCall(cluster, first, tenant, 'object', { method: 'POST', body: 19 });
    assert.equal(json(response, 200, 'confirmed value'), 19);
  });
  const { namespaceId, owners: [original] } = await namespace();
  assert.ok(original && !original.released);
  const originalState = await cluster.state(original.nodeId);
  const ingress = cluster.nodes.find(node => node !== first && node !== original.nodeId);
  const id = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update('takeover').digest('hex');
  const gate = join(cluster.recoveryRelayGate, original.nodeId);
  await writeFile(gate, '');
  try {
    // Inject the normal handoff marker while keeping the previous node and its
    // heartbeat alive. The relay withholds Cap'n Proto replies from that node.
    await cluster.releaseShardForRecoveryTest(namespaceId, { ...original, nodeEpoch: originalState.durableLog.nodeEpoch });
    let answered = false;
    const read = workerCall(cluster, ingress, tenant, 'object', { timeout: 34000 })
      .then(value => { answered = value.status === 200; return { value }; }, error => ({ error }));
    const takeover = await eventually('a successor has acquired the released shard', async () => {
      const { owners } = await namespace();
      const newOwner = owners.find(owner => owner.shard === original.shard);
      assert.ok(newOwner && !newOwner.released && newOwner.nodeId !== original.nodeId &&
        BigInt(newOwner.epoch) > BigInt(original.epoch));
      return newOwner.nodeId;
    });
    await eventually('the successor is waiting past the deadline for the live holder', async () => {
      const state = await cluster.state(takeover);
      const entry = state.durableLog.recovery.scheduler.entries.find(row => row.object.startsWith(`${namespaceId}/${id}/`));
      assert.ok(state.durableLog.recovery.cutsHeldOwnerLive > 0 && entry);
      assert.equal(entry.owner, original.nodeId);
      assert.equal(entry.ownerEpoch, originalState.durableLog.nodeEpoch);
      assert.ok(entry.waitingMs >= 8500);
      assert.equal(state.durableLog.objects.activeRebuilds, 0);
      assert.ok(state.peerConnections[original.nodeId].tailFailures > 0, 'a real page timeout marked its connection');
    }, { timeout: 22000, interval: 75 });
    assert.equal(answered, false, 'the successor cannot serve the object while the holder is slow');
    const stillAlive = await cluster.state(original.nodeId);
    assert.equal(stillAlive.membership.selfExpired, false);
    assert.equal(stillAlive.membership.leaseFences, originalState.membership.leaseFences);
    await unlink(gate);
    const { value, error } = await read;
    if (error) throw error;
    assert.deepEqual(json(value, 200, 'the successor read after the holder replied'), [19]);
    const settled = await cluster.state(takeover);
    assert.equal(settled.durableLog.recovery.ownerSilent, 0);
    assert.equal(settled.durableLog.recovery.bounded, 0);
  } finally {
    await unlink(gate).catch(error => { if (error.code !== 'ENOENT') throw error; });
  }
}

async function durableThreadedFence(cluster) {
  const tenant = await takeoverWriter(cluster, 'fenced-durable-test');
  const [first] = cluster.nodes;
  const namespace = () => cluster.api(`${tenant.path}/workers/object/namespaces/Record`, { token: tenant.token });
  await eventually('the object serves a confirmed write', async () => {
    assert.equal(json(await workerCall(cluster, first, tenant, 'object', { method: 'POST', body: 1 }),
      200, 'baseline write'), 1);
  });
  const { namespaceId, owners: [old] } = await namespace();
  assert.ok(old && cluster.nodes.includes(old.nodeId));
  const objectId = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update('takeover').digest('hex');
  const peers = cluster.nodes.filter(node => node !== old.nodeId);
  const [ingress] = peers;
  await eventually('both peers are reachable before the fence', async () => {
    const { durableLog } = await cluster.state(old.nodeId);
    assert.equal(durableLog.peers.want, 2);
    assert.ok(peers.every(peer => durableLog.peers.streams.some(stream => stream.nodeId === peer && stream.open)));
  });
  const child = await durableGeneration(cluster, old.nodeId);
  assert.equal(child.lifecycle?.durableServer, 'threaded');
  const { durableLog: before } = await cluster.state(old.nodeId);
  const sequence = (BigInt(before.log.nextSeq) + 1n).toString();
  const gate = join(cluster.peerAckGate, 'hold');
  await writeFile(gate, '');
  const sentAt = performance.now();
  const staleWrite = workerCall(cluster, ingress, tenant, 'object', { method: 'POST', body: 2, timeout: 30000 })
    .catch(error => ({ error }));
  let paused = false;
  let recovery;
  try {
    await eventually('the old generation has a write waiting on two peers', async () => {
      const states = await Promise.all(peers.map(peer => cluster.state(peer)));
      assert.ok(states.every(state => state.durableLog.peers.heldAcksForTest > 0));
      const { durableLog } = await cluster.state(old.nodeId);
      assert.equal(durableLog.log.nextSeq, sequence);
      assert.equal(durableLog.confirm.unconfirmed, 1);
    }, { timeout: 3000, interval: 25 });
    await cluster.compose(['pause', old.nodeId]);
    paused = true;
    recovery = eventually('the survivor can read after acquiring the shard', async () =>
      json(await workerCall(cluster, ingress, tenant, 'object', { timeout: 10000 }), 200, 'fenced takeover read'),
    { timeout: 45000, interval: 100 }).catch(error => ({ error }));
    await eventually('a different node acquires a higher epoch while the old child stays frozen', async () => {
      const { owners } = await namespace();
      const owner = owners.find(entry => entry.shard === old.shard);
      assert.ok(owner && peers.includes(owner.nodeId) && BigInt(owner.epoch) > BigInt(old.epoch));
    }, { timeout: 20000, interval: 100 });
    assert.ok(performance.now() - sentAt < cluster.confirmTimeoutMs - 1000,
      'the old write has time left on its confirmation deadline when the new epoch is known');
  } finally {
    await unlink(gate);
    if (paused) await cluster.compose(['unpause', old.nodeId]);
  }
  const result = await staleWrite;
  assert.ok(result.error || result.status >= 400, 'a write from the fenced generation never returned success');
  await eventually('the old owner excludes the pending write because it lost ownership', async () => {
    const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', old.nodeId]);
    const prefix = '[do-log] ';
    const events = logs.split('\n').filter(line => line.startsWith(prefix))
      .map(line => JSON.parse(line.slice(prefix.length)));
    const fence = events.find(row => row.event === 'owner-fenced' && row.namespaceId === namespaceId &&
      row.shard === old.shard && BigInt(row.throughShardEpoch) >= BigInt(old.epoch));
    assert.ok(fence, `no fence for shard ${old.shard} epoch ${old.epoch} on ${old.nodeId}`);
    const excluded = events.find(row => row.event === 'isolated' && row.object.startsWith(`${namespaceId}/${objectId}/`) &&
      row.records === 1 && row.from === sequence);
    assert.ok(excluded, `no isolation for held sequence ${sequence} on ${old.nodeId}`);
    assert.ok(excluded.reason === fence.reason || excluded.reason === 'the node\'s membership lease ended before confirmation',
      `unexpected reason for isolating sequence ${sequence} on ${old.nodeId}: ${excluded.reason}`);
  });
  const saved = await recovery;
  if (saved.error) throw saved.error;
  assert.ok(saved.includes(1), 'the later owner keeps the earlier confirmed write');
}

async function threadedAlarmTakeover(cluster) {
  const tenant = await cluster.tenant('threaded-alarm-fence');
  const [entry] = cluster.nodes;
  const namespace = () => cluster.api(`${tenant.path}/workers/alarms/namespaces/Alarm`,
    { token: tenant.token });
  const deploy = (version, brokenConstructor = '') => cluster.deploy(tenant, 'alarms', `
    import { DurableObject } from 'cloudflare:workers';
    export class Alarm extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        if (ctx.id.toString() === ${JSON.stringify(brokenConstructor)}) {
          throw new Error('alarm actor constructor failed');
        }
      }
      async fetch(request) {
        if (request.method === 'POST') {
          const at = await request.json();
          if (new URL(request.url).searchParams.has('stop')) await this.ctx.storage.put('stop', true);
          await this.ctx.storage.setAlarm(at);
          return Response.json(at);
        }
        if (request.method === 'DELETE') {
          await this.ctx.storage.deleteAlarm();
          return Response.json(await this.ctx.storage.getAlarm());
        }
        return Response.json({
          at: await this.ctx.storage.getAlarm(),
          fired: (await this.ctx.storage.get('fired')) ?? 0,
        });
      }
      async alarm() {
        if (await this.ctx.storage.get('stop')) this.ctx.abort('stop this alarm', { retryAlarm: false });
        await this.ctx.storage.put('fired', ((await this.ctx.storage.get('fired')) ?? 0) + 1);
      }
    }
    export default { fetch(request, env) {
      const name = new URL(request.url).searchParams.get('object');
      return env.ALARMS.getByName(name).fetch(request);
    } };
  `, { version, wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['Alarm'] }],
    durable_objects: { bindings: [{ name: 'ALARMS', class_name: 'Alarm' }] } } });
  await deploy('v1');
  const { namespaceId, shards } = await namespace();
  const idOf = name => createHash('sha256').update(Buffer.from(namespaceId, 'hex'))
    .update('name').update(name).digest('hex');
  const shardOf = name => {
    const id = idOf(name);
    return Number(BigInt(`0x${id.slice(0, 16)}`) % BigInt(shards));
  };
  const confirmed = 'confirmed';
  const shard = shardOf(confirmed);
  const sameShard = label => {
    for (let i = 0; i < shards * 16; ++i) {
      const name = `${label}-${i}`;
      if (shardOf(name) === shard) return name;
    }
    assert.fail(`no ${label} object maps to shard ${shard}`);
  };
  const unconfirmed = sameShard('unconfirmed');
  const rescheduled = sameShard('rescheduled');
  const ownerOfShard = async () => (await namespace()).owners.find(owner => owner.shard === shard && !owner.released);
  const call = (node, object, options) => workerCall(cluster, node, tenant, `alarms?object=${object}`, options);
  const alarmEvents = async (object, ...nodes) => {
    const logs = await cluster.compose(['logs', '--no-color', ...nodes]);
    const prefix = '[do-log] ';
    return logs.split('\n').filter(line => line.includes(prefix)).map(line => JSON.parse(line.split(prefix)[1]))
      .filter(row => row.namespaceId === namespaceId && row.objectId === idOf(object).slice(0, 12) &&
        row.event.startsWith('alarm-'));
  };

  // Both behaviors are exercised while the three nodes can still confirm writes normally.
  const constructorFailure = 'constructor-failure';
  const noRetry = 'no-retry';
  const reviewAt = Date.now() + 12000;
  await eventually('schedule the two reviewed alarm outcomes', async () => {
    const set = { method: 'POST', body: reviewAt };
    assert.equal(json(await call(entry, constructorFailure, set), 200, 'constructor schedule'), reviewAt);
    assert.equal(json(await call(entry, noRetry + '&stop', set), 200, 'nonretrying schedule'), reviewAt);
  });
  await deploy('v2', idOf(constructorFailure));
  await eventually('abort without retry clears the schedule and constructor failures are counted', async () => {
    const stopped = await alarmEvents(noRetry);
    assert.equal(stopped.filter(row => row.event === 'alarm-no-retry').length, 1);
    assert.deepEqual(json(await call(entry, noRetry), 200, 'nonretrying alarm after clear'), { at: null, fired: 0 });
    const failed = (await alarmEvents(constructorFailure)).filter(row => row.event === 'alarm-failed');
    assert.deepEqual(failed.slice(0, 2).map(row => row.retries), [1, 2]);
    assert.ok(failed[0].afterMs >= 1750 && failed[0].afterMs < 2250);
    assert.ok(failed[1].afterMs >= 3500 && failed[1].afterMs < 4500);
  }, { timeout: 45000, interval: 500 });

  const at = Date.now() + 65000;
  await eventually('both original schedules are confirmed under the same shard owner', async () => {
    for (const object of [confirmed, unconfirmed, rescheduled]) {
      assert.equal(json(await call(entry, object, { method: 'POST', body: at }), 200, object), at);
    }
  });
  const original = await ownerOfShard();
  assert.ok(original && cluster.nodes.includes(original.nodeId));
  const peers = cluster.nodes.filter(node => node !== original.nodeId);
  const [survivor] = peers;
  const clear = { method: 'DELETE', timeout: 30000 };
  assert.equal(json(await call(entry, confirmed, clear), 200, 'confirmed clear'), null);
  const gate = join(cluster.peerAckGate, 'hold');
  await writeFile(gate, '');
  const sentAt = performance.now();
  const pending = call(survivor, unconfirmed, clear).catch(error => ({ error }));
  let paused = false;
  let recovery;
  try {
    await eventually('the old owner has started a clear that cannot be confirmed', async () => {
      const states = await Promise.all(peers.map(node => cluster.state(node)));
      assert.ok(states.every(state => state.durableLog.peers.heldAcksForTest > 0));
      assert.equal((await cluster.state(original.nodeId)).durableLog.confirm.unconfirmed, 1);
    }, { timeout: 3000, interval: 25 });
    await cluster.compose(['pause', original.nodeId]);
    paused = true;
    recovery = eventually('the new owner reopens the confirmed clear', async () =>
      json(await call(survivor, confirmed, { timeout: 10000 }), 200, 'fenced alarm takeover'),
    { timeout: 45000, interval: 100 }).catch(error => ({ error }));
    await eventually('a survivor acquires the alarm shard at a higher epoch while the old owner is frozen', async () => {
      const owner = await ownerOfShard();
      assert.ok(owner && peers.includes(owner.nodeId) && BigInt(owner.epoch) > BigInt(original.epoch));
    }, { timeout: 20000, interval: 100 });
    assert.ok(performance.now() - sentAt < cluster.confirmTimeoutMs - 1000,
      'the old clear still has time on its confirmation deadline when the new epoch is known');
  } finally {
    await unlink(gate);
    if (paused) await cluster.compose(['unpause', original.nodeId]);
  }
  const result = await pending;
  assert.ok(result.error || result.status >= 400, 'the fenced clear never returns success');
  const saved = await recovery;
  if (saved.error) throw saved.error;
  assert.deepEqual(saved, { at: null, fired: 0 }, 'the previously confirmed clear survives recovery');
  const newTime = at + 120000;
  await eventually('the successor replaces the inherited schedule', async () => {
    assert.equal(json(await call(survivor, rescheduled, { method: 'POST', body: newTime, timeout: 10000 }),
      200, 'replace the inherited schedule'), newTime);
  });
  assert.ok(Date.now() < at, 'the successor replaces the alarm before its former due time');
  await eventually('the surviving owner delivers the alarm whose clear was unconfirmed', async () => {
    assert.deepEqual(json(await call(survivor, unconfirmed), 200, 'unconfirmed clear recovery'),
      { at: null, fired: 1 });
    assert.deepEqual(json(await call(survivor, confirmed), 200, 'confirmed clear recovery'),
      { at: null, fired: 0 });
    assert.deepEqual(json(await call(survivor, rescheduled), 200, 'superseded alarm hint'),
      { at: newTime, fired: 0 }, 'the previous due time cannot fire the replacement schedule');
  }, { timeout: 85000, interval: 250 });
  const previouslyConfirmed = (await alarmEvents(confirmed, ...peers)).length;
  assert.ok(Date.now() >= at, 'observe across the previous scheduled time');
  await delay(2500);
  assert.equal((await alarmEvents(confirmed, ...peers)).length, previouslyConfirmed,
    'the successor stops dispatching for the confirmed clear');
  assert.deepEqual(json(await call(survivor, rescheduled), 200, 'replacement after previous due time'),
    { at: newTime, fired: 0 });
}

async function durableGeneration(cluster, node) {
  const { processes } = await cluster.state(node);
  const durable = processes.entries.filter(entry => entry.processClass === 'durable');
  assert.equal(durable.length, 1, `${node} must have one durable slot`);
  assert.equal(durable[0].state, 'ready', `${node}'s durable slot is ready`);
  return durable[0];
}

async function rotationRequest(cluster, node, slot) {
  const { requests: [rotation] } = json(await request(cluster.urls[node], `/__coworkerd/rotate?process=${slot}`, {
    method: 'POST', token: cluster.management,
  }), 200, 'durable rotation request');
  return rotation.requestId;
}

async function rotationStatus(cluster, node, requestId) {
  return json(await request(cluster.urls[node], `/__coworkerd/rotation?requests=${requestId}`, {
    token: cluster.management,
  }), 200, 'durable rotation status');
}

async function waitForDurableRotation(cluster, node, before, requestId, drainOutcome) {
  await eventually(`durable generation ${before.generation} rotates after ${drainOutcome}`, async () => {
    const { requests: [status], records: [record] } =
      await rotationStatus(cluster, node, requestId);
    assert.equal(status.status, 'completed');
    assert.equal(record.oldGeneration, before.generation);
    assert.equal(record.drainOutcome, drainOutcome);
    const after = await durableGeneration(cluster, node);
    assert.ok(after.generation > before.generation && after.pid !== before.pid);
  }, { timeout: 45000 });
}

async function mixedDurableStorage(cluster) {
  const [threaded, ingress, other] = cluster.nodes;
  const peers = [ingress, other];
  const tenant = await cluster.tenant('mixed-durable-test');
  const worker = `
    import { DurableObject } from 'cloudflare:workers';
    export class Record extends DurableObject {
      write(value) {
        void this.ctx.storage.put('value', value);
        return value;
      }
      async fetch(request) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/checkpoint')) {
          const payload = await request.json();
          await this.ctx.storage.put('blob', payload);
          return Response.json(payload.length);
        }
        if (path.endsWith('/alarm')) {
          const at = await request.json();
          await this.ctx.storage.put('fired', false);
          await this.ctx.storage.setAlarm(at);
          return Response.json(at);
        }
        if (path.endsWith('/clone-facet')) {
          try {
            this.ctx.facets.clone('source', 'destination');
            return Response.json({ cloned: true });
          } catch (error) {
            return Response.json({ error: String(error) });
          }
        }
        if (request.method === 'POST') return Response.json(this.write(await request.json()));
        const value = await this.ctx.storage.get('value');
        const fired = await this.ctx.storage.get('fired');
        return Response.json({ id: this.ctx.id.toString(), value: value ?? null, fired: fired ?? false });
      }
      async alarm() { await this.ctx.storage.put('fired', true); }
    }
    export default {
      async fetch(request, env) {
        const url = new URL(request.url);
        const record = env.RECORDS.getByName(url.searchParams.get('object'));
        if (url.pathname.endsWith('/rpc')) return Response.json(await record.write(await request.json()));
        return record.fetch(request);
      },
    };
  `;
  const wrangler = {
    durable_objects: { bindings: [{ name: 'RECORDS', class_name: 'Record' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record'] }],
  };
  for (const name of ['writer', 'reader']) await cluster.deploy(tenant, name, worker, { wrangler });
  const { workerId, versions: [{ versionId, definitionHash, contentHash }] } =
    await cluster.api(`${tenant.path}/workers/writer`, { token: tenant.token });
  const trial = json(await request(cluster.urls[threaded], '/__coworkerd/trial-load', {
    method: 'POST', token: cluster.management, timeout: 10000,
    body: { workerId, versionId, definitionHash, contentHash, routed: true },
  }), 200, 'trial load on a threaded node');
  assert.equal(trial.ok, true, trial.message);
  assert.equal(trial.dispatched, true);
  assert.match(trial.processId, /^q\d+$/);
  const findObject = async name => {
    const namespace = () => cluster.api(`${tenant.path}/workers/${name}/namespaces/Record`, { token: tenant.token });
    const { namespaceId, shards } = await namespace();
    for (let i = 0; i < 32; ++i) {
      const object = `object-${i}`;
      const id = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update(object).digest('hex');
      const shard = Number(BigInt(`0x${id.slice(0, 16)}`) % BigInt(shards));
      const path = action => `/${tenant.name}/${name}${action}?object=${object}`;
      const read = async (node = ingress) =>
        json(await request(cluster.urls[node], path(''), { timeout: 35000 }), 200, name + ' read');
      const owner = async () => (await namespace()).owners.find(entry => entry.shard === shard && !entry.released);
      await eventually(`${name} candidate serves on its owner`, async () => assert.equal((await read()).id, id));
      if ((await owner())?.nodeId === threaded) return { id, path, read, owner };
    }
    assert.fail(`no ${name} candidate acquired on the threaded node`);
  };
  const target = await findObject('writer');
  const neighbor = await findObject('reader');
  const write = async (suffix, value, node = ingress) => json(await request(cluster.urls[node], target.path(suffix), {
    method: 'POST', body: value,
  }), 200, 'mixed durable write');
  assert.equal(await write('', 'warm'), 'warm');
  assert.match((await write('/clone-facet', null)).error, /NOT_SUPPORTED/);
  const first = await eventually('separate definitions occupy two homes in the same threaded generation', async () => {
    const generation = await durableGeneration(cluster, threaded);
    assert.equal(generation.lifecycle?.durableServer, 'threaded');
    assert.ok(generation.lifecycle.durableAdmission.homes >= 2);
    assert.ok(generation.lifecycle.durableAdmission.residentRoots >= 2);
    return generation;
  });
  const early = [];
  for (const [suffix, value] of [['', 'threaded-http'], ['/rpc', 'threaded-rpc']]) {
    if (await heldPeerWrite(cluster, peers, cluster.urls[ingress] + target.path(suffix), value, async () => {
      assert.equal((await neighbor.read()).id, neighbor.id);
      assert.equal((await durableGeneration(cluster, threaded)).pid, first.pid);
    })) early.push(value);
  }
  assert.deepEqual(early, [], 'threaded responses escaped while both upstream peers held acknowledgements');
  assert.equal((await target.read()).value, 'threaded-rpc');
  assert.equal(await write('/checkpoint', 'x'.repeat(65536)), 65536);
  const io = await eventually('the threaded SQLite open and checkpoint are measured', async () => {
    const { durableLog } = await cluster.state(threaded);
    const io = durableLog.threadedHomeIo;
    assert.ok(io.openSamples > 0 && io.maxOpenNs > 0);
    assert.ok(io.checkpointSamples > 0 && io.maxCheckpointNs > 0 && io.maxCheckpointWalBytes >= 65536);
    return io;
  });
  console.log(`Threaded home I/O for the isolated workload ${JSON.stringify(io)}`);
  let rotation;
  const confirmed = 'committed-during-drain';
  assert.equal(await heldPeerWrite(cluster, peers, cluster.urls[ingress] + target.path(''), confirmed, async () => {
    rotation = await rotationRequest(cluster, threaded, first.processId);
    await eventually('rotation waits in its drain phase while the write is unconfirmed', async () => {
      const { requests: [status], records: [record] } = await rotationStatus(cluster, threaded, rotation);
      assert.equal(status.status, 'active');
      assert.equal(record.phase, 'draining');
    }, { timeout: 1500, interval: 25 });
  }), false);
  await waitForDurableRotation(cluster, threaded, first, rotation, 'drained');
  assert.equal((await target.read()).value, confirmed, 'clean child rotation reopens the value committed during drain');

  const beforeLoss = await durableGeneration(cluster, threaded);
  const loss = join(cluster.peerAckGate, `loss-${threaded}`);
  const channelId = `${beforeLoss.processId}-${beforeLoss.generation}`;
  await writeFile(loss, channelId);
  try {
    await eventually('the daemon cuts the WAL socket of the live child', async () => {
      const logs = await cluster.compose(['logs', '--no-color', '--tail', '120', threaded]);
      assert.ok(logs.split('\n').some(line => line.includes('"event":"channel-cut-for-test"') &&
        line.endsWith(`"processId":"${channelId}","pid":${beforeLoss.pid}}`)));
    });
  } finally {
    await unlink(loss);
  }
  await eventually('channel loss recovery reopens the confirmed value after replacing the child', async () => {
    assert.equal((await target.read()).value, confirmed);
    const afterLoss = await durableGeneration(cluster, threaded);
    assert.ok(afterLoss.generation > beforeLoss.generation && afterLoss.pid !== beforeLoss.pid);
  }, { timeout: 45000 });

  const stalled = await durableGeneration(cluster, threaded);
  await cluster.compose(['exec', '-T', threaded, '/bin/sh', '-c', 'kill -STOP "$1"', 'sh', String(stalled.pid)]);
  const stalledRotation = await rotationRequest(cluster, threaded, stalled.processId);
  await waitForDurableRotation(cluster, threaded, stalled, stalledRotation, 'timeout');
  assert.equal((await target.read()).value, confirmed, 'unresponsive generation exits before the image reopens');

  const localAlarm = Date.now() + 500;
  assert.equal(await write('/alarm', localAlarm), localAlarm);
  await eventually('the threaded owner delivers a due alarm', async () => {
    assert.equal((await target.read()).fired, true);
    assert.equal((await target.owner()).nodeId, threaded);
  });

  const at = Date.now() + 25000;
  assert.equal(await write('/alarm', at), at);
  assert.equal((await target.read()).fired, false, 'the replacement alarm is still pending');
  const startingOwner = await target.owner();
  await cluster.compose(['stop', '--timeout', '45', threaded], { timeout: 70000 });
  const drained = await cluster.compose(['ps', '-a', '-q', threaded]);
  assert.equal(await cluster.command('docker', ['inspect', '--format', '{{.State.ExitCode}}', drained]), '0',
    'the threaded node completed its drain before Compose stopped it');
  const upstreamOwner = await eventually('an upstream owner recovers the threaded write and fires the due alarm', async () => {
    const saved = await target.read();
    assert.equal(saved.value, confirmed);
    assert.equal(saved.fired, true);
    const owner = await target.owner();
    assert.ok(peers.includes(owner.nodeId) && BigInt(owner.epoch) > BigInt(startingOwner.epoch));
    const logs = await cluster.compose(['logs', '--no-color', owner.nodeId]);
    assert.ok(logs.split('\n').some(line => line.includes('[do-log]') &&
      line.includes('"event":"alarm-fired"') && line.includes(`"objectId":"${target.id.slice(0, 12)}"`) &&
      line.includes(`"atUnixMs":${at},`)), 'the new upstream owner dispatched this exact alarm');
    return owner;
  }, { timeout: 120000, interval: 1000 });

  await cluster.compose(['start', threaded]);
  await cluster.refreshUrl(threaded);
  await eventually('the restarted threaded node rejoins as a live peer', async () => {
    await durableGeneration(cluster, threaded);
    const { membership } = await cluster.state(upstreamOwner.nodeId);
    assert.ok(membership.members.some(member => member.nodeId === threaded && member.durableProcesses > 0));
  }, { timeout: 45000 });
  const secondPeer = peers.find(node => node !== upstreamOwner.nodeId);
  const upstreamEarly = await heldPeerWrite(cluster, [threaded, secondPeer],
    cluster.urls[upstreamOwner.nodeId] + target.path(''), 'upstream-write', async () => {});
  assert.equal(upstreamEarly, false, 'upstream waits for the restarted threaded peer');
  await cluster.compose(['stop', '--timeout', '45', secondPeer], { timeout: 70000 });
  await cluster.compose(['kill', '-s', 'SIGKILL', upstreamOwner.nodeId]);
  await eventually('the restarted threaded owner replays confirmed upstream history', async () => {
    const saved = await target.read(threaded);
    assert.equal(saved.value, 'upstream-write');
    assert.equal(saved.fired, true);
    const owner = await target.owner();
    assert.equal(owner.nodeId, threaded);
    assert.ok(BigInt(owner.epoch) > BigInt(upstreamOwner.epoch));
  }, { timeout: 140000, interval: 1000 });
}

async function threadedClassMigration(cluster) {
  const { WebSocket: TestSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const [ingress] = cluster.nodes;
  const tenant = await cluster.tenant('threaded-migration');
  const name = 'records';
  const url = `/${tenant.name}/${name}`;
  const call = (path, options) => request(cluster.urls[ingress], url + path, options);
  const namespace = className => cluster.api(`${tenant.path}/workers/${name}/namespaces/${className}`,
    { token: tenant.token });
  const initial = { tag: 'v1', new_sqlite_classes: ['Record'] };
  const renamed = { tag: 'v2', renamed_classes: [{ from: 'Record', to: 'RenamedRecord' }] };
  const deleted = { tag: 'v3', deleted_classes: ['RenamedRecord'] };
  const source = (className, version) => `
    import { DurableObject, RpcTarget } from 'cloudflare:workers';
    class Retained extends RpcTarget {
      ping() { return '${version}'; }
    }
    export class MigrationFacet extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        this.constructedWith = ctx.getWebSockets().length;
      }
      fetch(request) {
        if (!request.headers.has('upgrade')) {
          return Response.json({ constructedWith: this.constructedWith, version: this.ctx.props.version });
        }
        const [client, socket] = Object.values(new WebSocketPair());
        this.ctx.acceptWebSocket(socket);
        return new Response(null, { status: 101, webSocket: client });
      }
      webSocketMessage(socket, message) { socket.send(this.ctx.props.version + ':' + message); }
    }
    export class ${className} extends DurableObject {
      retain() { return new Retained(); }
      async fetch(request) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/facet-socket') || path.endsWith('/facet-status')) {
          return this.ctx.facets.get('migration', () => ({
            class: this.ctx.exports.MigrationFacet({ props: { version: '${version}' } }),
          })).fetch(request);
        }
        if (path.endsWith('/alarm')) {
          const at = await request.json();
          await this.ctx.storage.setAlarm(at);
          return Response.json(at);
        }
        if (path.endsWith('/socket') || path.endsWith('/hibernatable')) {
          const [client, socket] = Object.values(new WebSocketPair());
          if (path.endsWith('/hibernatable')) {
            this.ctx.acceptWebSocket(socket, ['old']);
          } else {
            socket.accept();
            socket.addEventListener('message', event => socket.send('${version}:' + event.data));
          }
          return new Response(null, { status: 101, webSocket: client });
        }
        if (path.endsWith('/waiting')) return Response.json(await this.ctx.storage.get('waiting') ?? false);
        if (path.endsWith('/wait-starts')) return Response.json(await this.ctx.storage.get('wait-starts') ?? 0);
        if (path.endsWith('/wait-for-headers')) {
          await this.ctx.storage.put('wait-starts', (await this.ctx.storage.get('wait-starts') ?? 0) + 1);
          await this.ctx.storage.put('waiting', true);
          await scheduler.wait(90000);
          return new Response('old handler completed');
        }
        if (path.endsWith('/held')) {
          const encoder = new TextEncoder();
          return new Response(new ReadableStream({
            async start(controller) {
              controller.enqueue(encoder.encode('entered:'));
              for (let i = 0; i < 45; i++) {
                await scheduler.wait(2000);
                controller.enqueue(encoder.encode('${version}'));
              }
              controller.close();
            },
          }));
        }
        if (request.method === 'POST') await this.ctx.storage.put('value', await request.json());
        return Response.json({
          id: this.ctx.id.toString(),
          value: await this.ctx.storage.get('value'),
          alarmVersion: (await this.ctx.storage.get('alarm-version')) ?? null,
          version: '${version}',
        });
      }
      webSocketMessage(socket, message) { socket.send('${version}:' + message); }
      async alarm() { await this.ctx.storage.put('alarm-version', '${version}'); }
    }
    export default { async fetch(request, env) {
      const path = new URL(request.url).pathname;
      const stub = env.OBJECT.getByName(path.endsWith('/facet-socket') || path.endsWith('/facet-status') ?
        'parked' : 'stable');
      if (!path.endsWith('/retain')) return stub.fetch(request);
      const retained = await stub.retain();
      const encoder = new TextEncoder();
      return new Response(new ReadableStream({
        async start(controller) {
          controller.enqueue(encoder.encode('retained:' + await retained.ping()));
          await scheduler.wait(18000);
          try {
            controller.enqueue(encoder.encode('still:' + await retained.ping()));
          } catch {
            controller.enqueue(encoder.encode('released'));
          }
          controller.close();
        },
      }));
    } };
  `;
  const deploy = (version, className, migrations, activate = true) => cluster.deploy(tenant, name, source(className, version), {
    version, activate, wrangler: { migrations, durable_objects: { bindings: [{ name: 'OBJECT', class_name: className }] } },
  });
  await deploy('v1', 'Record', [initial]);
  await deploy('v2', 'RenamedRecord', [initial, renamed], false);
  const first = await eventually('v1 creates a threaded record', async () => {
    const value = json(await call('', { method: 'POST', body: 'retained' }), 200, 'write v1');
    assert.equal(value.version, 'v1');
    return value;
  });
  const { namespaceId: identity, owners: [owner] } = await namespace('Record');
  const generation = (await durableGeneration(cluster, owner.nodeId)).generation;
  const openSocket = async mode => {
    const address = new URL(url + mode, cluster.urls[ingress]);
    address.protocol = 'ws:';
    const socket = new TestSocket(address);
    await once(socket, 'open', { signal: AbortSignal.timeout(5000) });
    const echoed = once(socket, 'message', { signal: AbortSignal.timeout(5000) });
    socket.send('probe');
    assert.equal((await echoed)[0].toString(), 'v1:probe');
    return socket;
  };
  const sockets = await Promise.all(['/socket', '/hibernatable'].map(openSocket));
  const facetSocket = await openSocket('/facet-socket');
  const facetClosed = once(facetSocket, 'close', { signal: AbortSignal.timeout(65000) });
  void facetClosed.catch(() => {});
  await eventually('the old definition has a parked facet WebSocket', async () => {
    const statistics = await Promise.all(cluster.nodes.map(node => cluster.state(node)));
    assert.ok(statistics.some(state => state.webSockets.parked > 0));
    assert.equal(facetSocket.readyState, TestSocket.OPEN);
  }, { timeout: 25000 });
  const socketsClosed = sockets.map(socket => once(socket, 'close', { signal: AbortSignal.timeout(35000) }));
  for (const closing of socketsClosed) void closing.catch(() => {});
  const heldRpc = await fetch(cluster.urls[ingress] + url + '/retain', { signal: AbortSignal.timeout(45000) });
  await assertStreamingStatus(heldRpc, 200, 'retained RPC stream');
  const rpcReader = heldRpc.body.getReader();
  assert.equal(Buffer.from((await rpcReader.read()).value).toString(), 'retained:v1');
  const waiting = call('/wait-for-headers', { timeout: 60000 }).then(
    value => ({ value }), error => ({ error }),
  );
  await eventually('the second v1 request is waiting before response headers', async () => {
    assert.equal(json(await call('/waiting'), 200, 'v1 handler entered'), true);
  });
  const held = await fetch(cluster.urls[ingress] + url + '/held', { signal: AbortSignal.timeout(30000) });
  await assertStreamingStatus(held, 200, 'held v1 stream');
  const reader = held.body.getReader();
  try {
    const firstChunk = await reader.read();
    assert.equal(Buffer.from(firstChunk.value).toString(), 'entered:');
    // v2 is already packaged and uploaded. The alarm is due within the ten-second version drain.
    const due = Date.now() + 9000;
    assert.equal(json(await call('/alarm', { method: 'POST', body: due }), 200, 'schedule v1 alarm'), due);
    await cluster.api(`${tenant.path}/workers/${name}/versions/v2:activate`, { method: 'POST', token: tenant.token });
    await eventually('the first v2 request is refused while v1 still holds the tree', async () => {
      const response = await call('');
      assertStatus(response, 503, 'the first v2 request while v1 still holds the tree');
      assert.match(response.text, /DO_VERSION_DRAINING/);
    });
    assert.ok(Date.now() < due, 'the v2 version drain starts before the scheduled alarm is due');
    const oldChunk = await reader.read();
    assert.match(Buffer.from(oldChunk.value).toString(), /^(v1)+$/, 'an open stream keeps its code pin');
    await assert.rejects(async () => {
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) return;
      }
    }, /terminated|closed|reset|aborted/i, 'the new definition cuts the old stream at the drain bound');
    const pending = await waiting;
    if (pending.error) throw pending.error;
    assertStatus(pending.value, 503, 'an old call without response headers has an uncertain result');
    assert.match(pending.value.text, /DO_CODE_UPDATED/);
    assert.match(pending.value.text, /Durable Object reset because its code was updated/);
    assert.match(pending.value.text, /execution result uncertain/);
  } finally {
    await reader.cancel().catch(() => {});
  }
  const freshFacet = await eventually('the new definition opens without the old facet socket or props', async () => {
    return json(await call('/facet-status'), 200, 'new facet');
  });
  assert.deepEqual(freshFacet, { constructedWith: 0, version: 'v2' });
  const [facetCode, facetReason] = await facetClosed;
  assert.equal(facetCode, 1001);
  assert.match(facetReason.toString(), /definition changed/i);

  await eventually('renamed class uses the existing threaded storage', async () => {
    assert.equal((await namespace('RenamedRecord')).namespaceId, identity);
    const entry = json(await call(''), 200, 'read v2');
    assert.deepEqual(entry, { id: first.id, value: 'retained', version: 'v2', alarmVersion: 'v2' });
  });
  assert.equal((await durableGeneration(cluster, owner.nodeId)).generation, generation,
    'the target tree releases its sessions without replacing the process');
  assert.equal(json(await call('/wait-starts'), 200, 'confirmed call count'), 1,
    'the platform does not replay an old call with an uncertain result');
  assert.equal(Buffer.from((await rpcReader.read()).value).toString(), 'released',
    'a retained RPC capability from the old tree is revoked');
  await rpcReader.cancel();
  for (const closed of socketsClosed) assert.equal((await closed)[0], 1001, 'version drain closes both socket kinds');
  await cluster.deploy(tenant, name, respondWith('v3'), {
    version: 'v3', wrangler: { migrations: [initial, renamed, deleted] },
  });
  await eventually('deletion removes admission to the renamed namespace', async () => {
    assert.deepEqual(await call(''), { status: 200, text: 'v3' });
    const gone = await request(cluster.urls.apiserver,
      `${tenant.path}/workers/${name}/namespaces/RenamedRecord`, { token: tenant.token });
    if (gone.status === 200) {
      const record = JSON.parse(gone.text);
      assert.equal(record.namespaceId, identity);
      assert.ok(record.status === 'deleting' || record.status === 'deleted');
    } else {
      assertStatus(gone, 404, 'renamed namespace after deletion');
    }
  });
}

async function threadedSocketHibernation(cluster) {
  const { WebSocket: PingSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const tenant = await cluster.tenant('threaded-hibernation');
  const [ingress] = cluster.nodes;
  const path = `/${tenant.name}/sleeper`;
  const namespace = () => cluster.api(`${tenant.path}/workers/sleeper/namespaces/Sleeper`,
    { token: tenant.token });
  await cluster.deploy(tenant, 'sleeper', `
    import { DurableObject, WorkerEntrypoint } from 'cloudflare:workers';
    export class ServiceProbe extends WorkerEntrypoint {
      async fetch() { return Response.json({ target: 'service', label: this.ctx.props.label }); }
    }
    export class ObjectProbe extends DurableObject {
      async fetch() { return Response.json({ target: 'object', id: this.ctx.id.toString() }); }
    }
    class SocketActor extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        this.role = ctx.props?.role ?? 'root';
        this.attachment = { field: 42, role: this.role };
        this.autoRequest = this.role === 'root' ? 'tick' : 'facet-tick';
        this.autoReply = this.role === 'root' ? 'tock' : 'facet-tock';
        const retained = ctx.getWebSockets('retained');
        const automatic = ctx.getWebSocketAutoResponse();
        this.constructedWith = retained.length;
        this.restored = retained.every(socket =>
          ctx.getTags(socket).join(',') === 'retained,' + this.role &&
          JSON.stringify(socket.deserializeAttachment()) === JSON.stringify(this.attachment)) &&
          (retained.length === 0 || (automatic?.request === this.autoRequest &&
            automatic.response === this.autoReply));
        this.sequence = 0;
        ctx.blockConcurrencyWhile(async () => {
          this.instance = (await ctx.storage.get('constructors') ?? 0) + 1;
          await ctx.storage.put('constructors', this.instance);
        });
      }
      async fetch(request) {
        const route = new URL(request.url).pathname;
        if (route.endsWith('/terminal')) {
          return Response.json(await this.ctx.storage.get('terminal') ?? null);
        }
        if (route.endsWith('/hold')) {
          return new Response(new ReadableStream({
            async start(controller) {
              controller.enqueue(new TextEncoder().encode('entered'));
              await scheduler.wait(20000);
              controller.close();
            },
          }));
        }
        if (route.endsWith('/discarded')) {
          return Response.json({ sockets: this.ctx.getWebSockets('retained').length,
            state: this.lastSocket?.readyState });
        }
        if (request.headers.get('upgrade')?.toLowerCase() === 'websocket') {
          const [client, socket] = Object.values(new WebSocketPair());
          this.ctx.acceptWebSocket(socket, ['retained', this.role]);
          this.lastSocket = socket;
          socket.serializeAttachment(this.attachment);
          this.ctx.setWebSocketAutoResponse(
            new WebSocketRequestResponsePair(this.autoRequest, this.autoReply));
          return new Response(null, { status: 101, webSocket: client });
        }
        return Response.json({ instance: this.instance, sockets: this.ctx.getWebSockets('retained').length,
          constructedWith: this.constructedWith, restored: this.restored, role: this.role });
      }
      async webSocketMessage(socket, message) {
        if (message === 'binding targets') {
          const { service, object } = this.ctx.props.bindings;
          socket.send(JSON.stringify(await Promise.all([service, object].map(async binding =>
            (await binding.fetch('http://binding/probe')).json()))));
          return;
        }
        socket.send(JSON.stringify({
          instance: this.instance, constructedWith: this.constructedWith, restored: this.restored,
          role: this.role, sequence: ++this.sequence, id: this.ctx.id.toString(),
          message: typeof message === 'string' ? message : [...new Uint8Array(message)],
          attachment: socket.deserializeAttachment(),
          retained: this.ctx.getWebSockets('retained').length,
          automatic: this.ctx.getWebSocketAutoResponseTimestamp(socket)?.getTime(),
        }));
      }
      async webSocketClose(socket, code, reason, wasClean) {
        await this.ctx.storage.put('terminal', { code, reason, wasClean });
        if (wasClean) socket.close(code, reason);
      }
      async webSocketError(socket, error) {
        await this.ctx.storage.put('terminal', { error: String(error) });
      }
    }
    export class SocketFacet extends SocketActor {}
    export class Parent extends DurableObject {
      fetch(request) {
        return this.ctx.facets.get('leaf', () => {
          if (new URL(request.url).pathname.endsWith('/status')) throw new Error('The saved facet must reopen.');
          return {
            class: this.ctx.exports.SocketFacet({ props: { role: 'leaf', bindings: {
              service: this.ctx.exports.ServiceProbe({ props: { label: 'original' } }),
              object: this.env.PROBE.getByName('original-object'),
            } } }),
            id: 'original-facet',
          };
        }).fetch(request);
      }
    }
    export class Sleeper extends SocketActor {
      async fetch(request) {
        const route = new URL(request.url).pathname;
        if (route.endsWith('/delete-facet')) {
          this.ctx.facets.delete('parent');
          return new Response(null, { status: 204 });
        }
        const parts = route.split('/');
        const actionIndex = parts.findIndex(part => ['delete-facet', 'abort-facet', 'probe-facet'].includes(part));
        const [action, child] = actionIndex === -1 ? [] : parts.slice(actionIndex);
        if (action === 'delete-facet' || action === 'abort-facet') {
          if (action === 'delete-facet') this.ctx.facets.delete(child);
          else this.ctx.facets.abort(child, new Error('test abort'));
          return new Response(null, { status: 204 });
        }
        if (action === 'probe-facet') {
          const facet = this.ctx.facets.get(child, () => ({
            id: child,
            class: this.ctx.exports.SocketFacet({ props: {
              role: 'leaf',
              ...(child === 'too-large' || child.startsWith('large-') ?
                { payload: 'x'.repeat(child === 'too-large' ? 70000 : 63000) } : {}),
            } }),
          }));
          try {
            const response = await facet.fetch(request);
            if (child === 'discard' && response.status === 101) {
              try { response.webSocket.accept(); } catch (error) {
                if (route.endsWith('/throw')) throw new Error('Discarding the upgrade after a throw.');
                return Response.json({ name: error.name });
              }
              throw new Error('Accepted an unforwarded facet peer.');
            }
            return response;
          } catch (error) {
            if (child === 'discard' && route.endsWith('/throw')) throw error;
            return Response.json({ name: error.name, message: error.message }, { status: 422 });
          }
        }
        if (route.includes('/facet/')) {
          return this.ctx.facets.get('parent', () => ({ class: this.ctx.exports.Parent })).fetch(request);
        }
        return super.fetch(request);
      }
    }
    export default {
      fetch(request, env) { return env.TREE.getByName('parked').fetch(request); }
    };
  `, { wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['Sleeper', 'ObjectProbe'] }],
    durable_objects: { bindings: [
      { name: 'TREE', class_name: 'Sleeper' }, { name: 'PROBE', class_name: 'ObjectProbe' },
    ] } } });

  await eventually('the threaded object answers successfully', async () => {
    assert.deepEqual(json(await request(cluster.urls[ingress], path), 200, 'open sleeper'),
      { instance: 1, sockets: 0, constructedWith: 0, restored: true, role: 'root' });
  });
  const owner = (await namespace()).owners[0];
  assert.ok(owner && cluster.nodes.includes(owner.nodeId));
  const activity = async () => (await durableGeneration(cluster, owner.nodeId)).lifecycle.durableAdmission;
  const retired = after => eventually('the tree and its live sessions leave the home', async () => {
    const state = await activity();
    assert.ok(state.idleRetirements > after, JSON.stringify(state));
    assert.equal(state.residentRoots, 0, JSON.stringify(state));
    assert.equal(state.homeSessions, 0, JSON.stringify(state));
    assert.equal(state.running, 0);
    return state.idleRetirements;
  }, { timeout: 25000 });

  const socketUrl = new URL(path, cluster.urls[owner.nodeId]);
  socketUrl.protocol = 'ws:';
  const first = new PingSocket(socketUrl);
  const second = new PingSocket(socketUrl);
  const facet = new PingSocket(new URL(socketUrl.pathname + '/facet/socket', socketUrl));
  const sockets = [first, second, facet];
  const next = async (socket, event, timeout = 7000) => {
    const abort = new AbortController();
    const deadline = setTimeout(() => abort.abort(new Error(`WebSocket ${event} timed out`)), timeout);
    try {
      return await once(socket, event, { signal: abort.signal });
    } catch (error) {
      if (abort.signal.aborted) throw abort.signal.reason;
      throw error;
    } finally {
      clearTimeout(deadline);
    }
  };
  const ping = async (socket, text) => {
    const pong = next(socket, 'pong');
    socket.ping(text);
    assert.equal((await pong)[0].toString(), text);
  };
  const send = async (socket, text) => {
    const answer = next(socket, 'message');
    socket.send(text);
    return (await answer)[0].toString();
  };
  const facetPath = (name, operation = 'socket') => `/probe-facet/${name}/${operation}`;
  const openFacet = async name => {
    const socket = new PingSocket(new URL(path + facetPath(name), socketUrl));
    sockets.push(socket);
    await next(socket, 'open');
    return socket;
  };
  const upgradeResponse = async (name, operation = 'socket') => {
    const socket = new PingSocket(new URL(path + facetPath(name, operation), socketUrl));
    sockets.push(socket);
    const [, response] = await next(socket, 'unexpected-response');
    try {
      const chunks = [];
      for await (const chunk of response) chunks.push(chunk);
      const text = Buffer.concat(chunks).toString();
      const body = response.headers['content-type']?.startsWith('application/json') ? JSON.parse(text) : text;
      return { status: response.statusCode, body };
    } finally {
      socket.on('error', () => {});
      socket.terminate();
    }
  };
  const lookup = suffix => request(cluster.urls[owner.nodeId], path + suffix);
  const closeFacet = async (socket, action, name) => {
    const closed = next(socket, 'close');
    void closed.catch(() => {});
    assertStatus(await lookup(`/${action}-facet/${name}`), 204, `${action} facet ${name}`);
    const [code, reason] = await closed;
    assert.equal(code, 1001);
    assert.match(reason.toString(), new RegExp(action === 'abort' ? 'facet was aborted' : 'facet was deleted', 'i'));
  };
  let paused = false;
  try {
    await Promise.all(sockets.map(socket => next(socket, 'open')));
    await ping(first, 'before');
    assert.equal(await send(first, 'tick'), 'tock');
    const firstEvent = JSON.parse(await send(first, 'warm'));
    assert.equal(firstEvent.instance, 1);
    assert.equal(firstEvent.retained, 2);
    assert.deepEqual(firstEvent.attachment, { field: 42, role: 'root' });
    assert.ok(Number.isSafeInteger(firstEvent.automatic));
    await ping(facet, 'facet before retirement');
    assert.equal(await send(facet, 'facet-tick'), 'facet-tock');
    const warmFacet = JSON.parse(await send(facet, 'warm facet'));
    assert.equal(warmFacet.instance, 1);
    assert.equal(warmFacet.id, 'original-facet');
    assert.ok(Number.isSafeInteger(warmFacet.automatic));
    const warmBindings = JSON.parse(await send(facet, 'binding targets'));
    assert.deepEqual(warmBindings[0], { target: 'service', label: 'original' });
    assert.equal(warmBindings[1].target, 'object');
    assert.match(warmBindings[1].id, /^[0-9a-f]{64}$/);

    for (const operation of ['socket', 'throw']) {
      const discarded = await upgradeResponse('discard', operation);
      if (operation === 'throw') assert.ok(discarded.status >= 500, JSON.stringify(discarded));
      else assert.deepEqual(discarded, { status: 200, body: { name: 'NotSupportedError' } });
      await eventually(`the unforwarded socket closes and leaves its manager after ${operation}`, async () => {
        assert.deepEqual(json(await lookup(facetPath('discard', 'discarded')), 200, 'discarded socket'),
          { sockets: 0, state: PingSocket.CLOSED });
      }, { timeout: 7000 });
    }

    assertStatus(await lookup(facetPath('too-large', 'status')), 200, 'large facet status');
    const rejected = await upgradeResponse('too-large');
    assert.equal(rejected.status, 422, JSON.stringify(rejected.body));
    assert.equal(rejected.body.name, 'NotSupportedError');
    assert.match(rejected.body.message, /startup options cannot be restored/i);
    assert.deepEqual(json(await lookup(facetPath('too-large', 'discarded')), 200, 'rejected socket'),
      { sockets: 0, state: PingSocket.CLOSED });

    const largeSockets = [];
    for (let i = 0; i < 16; ++i) largeSockets.push(await openFacet(`large-${i}`));
    const treeLimit = await upgradeResponse('large-over-limit');
    assert.equal(treeLimit.status, 422, JSON.stringify(treeLimit.body));
    assert.equal(treeLimit.body.name, 'NotSupportedError');
    assert.match(treeLimit.body.message, /1 MiB/);
    // Each deletion also schedules a close event on this home.
    for (const [i, socket] of largeSockets.entries()) await closeFacet(socket, 'delete', `large-${i}`);

    await closeFacet(await openFacet('idle-abort'), 'abort', 'idle-abort');
    const busy = await openFacet('busy-abort');
    const signal = AbortSignal.timeout(15000);
    const streaming = await fetch(cluster.urls[owner.nodeId] + path + facetPath('busy-abort', 'hold'),
      { signal });
    await assertStreamingStatus(streaming, 200, 'busy facet stream');
    const reader = streaming.body.getReader();
    try {
      assert.equal(Buffer.from((await reader.read()).value).toString(), 'entered');
      await closeFacet(busy, 'abort', 'busy-abort');
      const end = await reader.read().catch(() => ({ done: true }));
      assert.equal(end.done, true);
      assert.equal(signal.aborted, false, 'the facet abort cancels its running request');
    } finally {
      await reader.cancel().catch(() => {});
    }

    const firstRetirement = await retired(0);
    const parked = (await cluster.state(owner.nodeId)).webSockets;
    assert.equal(parked.parked, 3);
    await ping(second, 'while parked');
    assert.equal(await send(second, 'tick'), 'tock');
    await ping(facet, 'facet while parked');
    assert.equal(await send(facet, 'facet-tick'), 'facet-tock');
    const afterAutomatic = await activity();
    assert.equal(afterAutomatic.residentRoots, 0);
    assert.equal(afterAutomatic.homeSessions, 0);
    assert.equal(afterAutomatic.idleRetirements, firstRetirement);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.socketWakes, parked.socketWakes);
    assert.deepEqual(json(await request(cluster.urls[owner.nodeId], path), 200, 'normal HTTP wake'),
      { instance: 2, sockets: 2, constructedWith: 2, restored: true, role: 'root' },
      'a normal reopen restores the root sockets before the constructor runs');
    const secondRetirement = await retired(firstRetirement);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 3,
      'the facet socket parks again even though only the root opened');
    assert.equal(await send(facet, 'facet-tick'), 'facet-tock');
    const burst = ['facet wake', Buffer.from([0, 128, 255]), 'last'];
    const replies = [];
    const onMessage = body => replies.push(JSON.parse(body.toString()));
    facet.on('message', onMessage);
    try {
      const last = eventually('the sleeping facet receives all queued frames', () => {
        assert.equal(replies.length, burst.length);
      }, { timeout: 7000 });
      for (const frame of burst) facet.send(frame);
      await last;
      const marker = next(facet, 'message');
      facet.send('marker');
      assert.equal(JSON.parse((await marker)[0].toString()).message, 'marker');
    } finally {
      facet.off('message', onMessage);
    }
    assert.deepEqual(replies.map(reply => [reply.sequence, reply.message]), [
      [1, 'facet wake'], [2, [0, 128, 255]], [3, 'last'], [4, 'marker'],
    ]);
    for (const reply of replies) {
      assert.equal(reply.id, 'original-facet');
      assert.equal(reply.role, 'leaf');
      assert.equal(reply.instance, 2);
      assert.equal(reply.constructedWith, 1);
      assert.equal(reply.retained, 1);
      assert.equal(reply.restored, true);
      assert.deepEqual(reply.attachment, { field: 42, role: 'leaf' });
      assert.ok(reply.automatic > warmFacet.automatic);
    }
    assert.deepEqual(JSON.parse(await send(facet, 'binding targets')), warmBindings,
      'the reopened facet reaches the original service binding and Durable Object stub');

    assert.equal(await send(first, 'tick'), 'tock');
    const wake = JSON.parse(await send(first, 'wake'));
    assert.equal(wake.instance, 3);
    assert.equal(wake.message, 'wake');
    assert.equal(wake.retained, 2);
    assert.equal(wake.constructedWith, 2);
    assert.deepEqual(wake.attachment, { field: 42, role: 'root' });
    assert.ok(wake.automatic > firstEvent.automatic);
    assert.deepEqual(JSON.parse(await send(second, Buffer.from([0, 128, 255]))).message, [0, 128, 255]);

    const thirdRetirement = await retired(secondRetirement);
    assert.deepEqual(json(await lookup('/facet/status'), 200, 'facet HTTP wake'),
      { instance: 3, sockets: 1, constructedWith: 1, restored: true, role: 'leaf' },
      'a normal reopen restores the facet socket, tags, attachment and automatic reply before its constructor');

    const readTerminal = async () => json(await request(cluster.urls[owner.nodeId], path + '/terminal'),
      200, 'socket termination');
    const clean = new PingSocket(socketUrl);
    await next(clean, 'open');
    const cleanClose = next(clean, 'close');
    clean.close(1000, 'finished');
    assert.equal((await cleanClose)[0], 1000);
    await eventually('the actor receives the clean close code and its reason', async () => {
      assert.deepEqual(await readTerminal(), { code: 1000, reason: 'finished', wasClean: true });
    }, { timeout: 7000 });
    const interrupted = new PingSocket(socketUrl);
    await next(interrupted, 'open');
    assert.equal(JSON.parse(await send(interrupted, 'armed')).message, 'armed');
    interrupted.terminate();
    await eventually('the actor receives one abnormal close', async () => {
      const terminal = await readTerminal();
      assert.equal(terminal?.code, 1006);
      assert.equal(terminal.wasClean, false);
    }, { timeout: 7000 });
    const fourthRetirement = await retired(thirdRetirement);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 3);
    const facetClosed = next(facet, 'close');
    void facetClosed.catch(() => {});
    assertStatus(await lookup('/delete-facet'), 204, 'delete parent facet');
    const [facetCloseCode, facetCloseReason] = await facetClosed;
    assert.equal(facetCloseCode, 1001);
    assert.match(facetCloseReason.toString(), /facet was deleted/i);
    assert.equal(await send(first, 'tick'), 'tock');
    await retired(fourthRetirement);

    const survivor = cluster.nodes.find(node => node !== owner.nodeId);
    const closed = [first, second].map(socket => next(socket, 'close', 45000));
    closed.forEach(promise => void promise.catch(() => {}));
    await cluster.compose(['pause', owner.nodeId]);
    paused = true;
    await eventually('a surviving node takes the parked socket shard', async () => {
      const read = json(await request(cluster.urls[survivor], path, { timeout: 7000 }), 200, 'takeover wake');
      assert.deepEqual(read, { instance: 6, sockets: 0, constructedWith: 0, restored: true, role: 'root' });
      const nextOwner = (await namespace()).owners[0];
      assert.notEqual(nextOwner.nodeId, owner.nodeId);
      assert.ok(BigInt(nextOwner.epoch) > BigInt(owner.epoch));
    }, { timeout: 35000 });
    await cluster.compose(['unpause', owner.nodeId]);
    paused = false;
    for (const close of closed) assert.equal((await close)[0], 1001, 'loss of shard fences the parked socket');
  } finally {
    if (paused) await cluster.compose(['unpause', owner.nodeId]);
    for (const socket of sockets) socket.terminate();
  }
}

async function threadedSocketRetirement(cluster) {
  const { WebSocket: Socket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const tenant = await cluster.tenant('threaded-retirement');
  const [ingress] = cluster.nodes;
  const path = `/${tenant.name}/sleeper`;
  const call = (route, options) => request(cluster.urls[ingress], path + route, options);
  await cluster.deploy(tenant, 'sleeper', `
    import { DurableObject } from 'cloudflare:workers';
    export class Sleeper extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        ctx.blockConcurrencyWhile(async () => {
          this.instance = (await ctx.storage.get('constructors') ?? 0) + 1;
          await ctx.storage.put('constructors', this.instance);
        });
      }
      async fetch(request) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/ordinary') || path.endsWith('/managed')) {
          const [client, socket] = Object.values(new WebSocketPair());
          if (path.endsWith('/managed')) {
            this.ctx.acceptWebSocket(socket, ['managed']);
            socket.serializeAttachment({ token: 'preserved' });
            this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('tick', 'tock'));
            socket.send('welcome');
          } else {
            socket.accept();
            socket.addEventListener('message', event => {
              if (event.data === 'reset') this.ctx.abort('reset the old home after transfer');
              const managed = this.ctx.getWebSockets('managed')[0];
              const changes = [
                () => managed.serializeAttachment({ token: 'lost' }),
                () => this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('tick', 'lost')),
                () => managed.send('lost'),
                () => managed.close(1000, 'lost'),
              ];
              socket.send(JSON.stringify(changes.map(change => {
                try { change(); return 'accepted'; } catch (error) { return error.name; }
              })));
            });
          }
          return new Response(null, { status: 101, webSocket: client });
        }
        if (path.endsWith('/local')) {
          const [peer, socket] = Object.values(new WebSocketPair());
          this.ctx.acceptWebSocket(socket, ['local']);
          try { peer.accept(); } catch (error) {
            return Response.json({ name: error.name, message: error.message });
          }
          return Response.json({ name: 'accepted' });
        }
        if (path.endsWith('/hold')) this.ctx.waitUntil(scheduler.wait(120000));
        if (path.endsWith('/write')) this.ctx.getWebSockets('managed')[0].send('write-finished');
        if (path.endsWith('/reset')) this.ctx.abort('reset the listening socket');
        return Response.json(this.instance);
      }
      webSocketMessage(socket, message) {
        socket.send(this.instance + ':' + socket.deserializeAttachment().token);
      }
      webSocketClose(socket, code, reason) { if (code !== 1006) socket.close(code, reason); }
    }
    export default { fetch(request, env) { return env.TREE.getByName('stable').fetch(request); } };
  `, { wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['Sleeper'] }],
    durable_objects: { bindings: [{ name: 'TREE', class_name: 'Sleeper' }] } } });
  await eventually('a hibernatable pair cannot be used without an HTTP upgrade', async () => {
    const local = json(await call('/local'), 200, 'local pair');
    assert.equal(local.name, 'NotSupportedError');
    assert.match(local.message, /requires an HTTP upgrade/);
  });
  const { owners: [owner] } = await cluster.api(`${tenant.path}/workers/sleeper/namespaces/Sleeper`, { token: tenant.token });
  const activity = async () => (await durableGeneration(cluster, owner.nodeId)).lifecycle.durableAdmission;
  const generation = (await durableGeneration(cluster, owner.nodeId)).generation;
  const control = name => join(cluster.durableLifecycleGate, name);
  const advance = async elapsed => {
    const next = control('idle-offset-next');
    await writeFile(next, String(elapsed), { mode: 0o644 });
    await rename(next, control('idle-offset-ms'));
    await delay(550);
  };
  const open = route => {
    const address = new URL(path + route, cluster.urls[owner.nodeId]);
    address.protocol = 'ws:';
    return new Socket(address);
  };
  const next = (socket, event, timeout = 7000) => once(socket, event, { signal: AbortSignal.timeout(timeout) })
    .catch(error => { throw new Error(`WebSocket ${socket.url} did not deliver ${event}`, { cause: error }); });
  const send = async (socket, message) => {
    const answer = next(socket, 'message');
    socket.send(message);
    return (await answer)[0].toString();
  };
  const retired = () => eventually('the ordinary socket or expired work releases the tree with its managed socket parked', async () => {
    const current = await activity();
    assert.equal(current.homeSessions, 0);
    assert.equal(current.residentRoots, 0);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 1);
  });
  const ordinary = open('/ordinary'), managed = open('/managed');
  const welcome = next(managed, 'message');
  try {
    await Promise.all([next(ordinary, 'open'), next(managed, 'open')]);
    assert.equal((await welcome)[0].toString(), 'welcome', 'writes queued before 101 move to the upgraded transport');
    const before = json(await call('/status'), 200, 'read before retirement');
    const onlyOrdinarySession = () => eventually('only the ordinary transport still holds an event ticket', async () => {
      const state = await activity();
      assert.equal(state.homeSessions, 1);
      assert.equal(state.running, 1);
    });
    await onlyOrdinarySession();
    await advance(65000);
    assert.equal(ordinary.readyState, Socket.OPEN);
    await writeFile(control('hold-socket-park'), '');
    await advance(150000);
    assert.equal(ordinary.readyState, Socket.OPEN, 'the ordinary socket remains open until the managed socket is parked');
    assert.deepEqual(JSON.parse(await send(ordinary, 'mutate')), Array(4).fill('InvalidStateError'),
      'an ordinary handler cannot change the managed transport after the snapshot');
    let settled = false;
    const racingRequest = call('/status', { timeout: 7000 })
      .then(response => { settled = true; return response; });
    void racingRequest.catch(() => {});
    await delay(400);
    assert.equal(settled, false, 'new requests wait at the retiring tree fence');
    const raced = await racingRequest;
    if (raced.status === 200) assert.equal(JSON.parse(raced.text), before);
    else assert.equal(raced.status, 503, raced.text);
    await eventually('a timed out park resumes admission to the same tree', async () => {
      assert.equal(json(await call('/status'), 200, 'park refused'), before);
    });
    await onlyOrdinarySession();
    await unlink(control('hold-socket-park'));
    await writeFile(control('hold-socket-park-reply'), '');
    const ordinaryClosed = next(ordinary, 'close', 13000);
    await advance(305000);
    await eventually('the node parks the socket while the original reply is held', async () => {
      assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 1);
    });
    await ordinaryClosed.catch(async error => {
      const state = await cluster.state(owner.nodeId);
      throw new Error(`Ordinary socket stayed open. ${JSON.stringify({ activity: await activity(), websockets: state.webSockets })}`,
        { cause: error });
    });
    await unlink(control('hold-socket-park-reply'));
    await retired();
    assert.equal(await send(managed, 'tick'), 'tock');
    assert.equal((await activity()).homeSessions, 0);

    await writeFile(control('hold-socket-adopt'), '');
    const stalled = await call('/status', { timeout: 7500 }).catch(() => null);
    if (stalled) assertStatus(stalled, 503, 'no request starts before adoption succeeds');
    assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 1);
    await unlink(control('hold-socket-adopt'));
    await writeFile(control('hold-socket-adopt-reply'), '');
    const cancelled = call('/status', { timeout: 2200 }).catch(() => null);
    await eventually('the node adopts while the reply is held', async () => {
      assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 0);
    });
    assert.equal(await cancelled, null, 'the first caller disconnects while waiting for adoption');
    await delay(5400);
    await advance(322000);
    await retired();
    await unlink(control('hold-socket-adopt-reply'));
    const restored = await eventually('a new open restores the parked socket after adoption lost its reply', async () => {
      const instance = json(await call('/status'), 200, 'adopt after timeout');
      // The interrupted ingress may finish its submitted event when adoption
      // resolves. Either way a single subsequent open restores the socket.
      assert.ok(instance === before + 1 || instance === before + 2, String(instance));
      return instance;
    });
    assert.equal(await send(managed, 'resumed'), restored + ':preserved');

    const oldHome = open('/ordinary');
    try {
      await next(oldHome, 'open');
      await onlyOrdinarySession();
      await writeFile(control('hold-socket-park-reply'), '');
      await advance(482000);
      await eventually('the node has parked the managed socket before the old home resets', async () => {
        assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 1);
      });
      const oldHomeClosed = next(oldHome, 'close', 3500);
      oldHome.send('reset');
      await oldHomeClosed;
      await retired();
      await unlink(control('hold-socket-park-reply'));
      assert.equal(await send(managed, 'tick'), 'tock', 'a reset of the old home leaves the parked socket live');
      assert.equal(await send(managed, 'after old home reset'), (restored + 1) + ':preserved');
    } finally {
      oldHome.terminate();
    }

    assert.equal(json(await call('/hold'), 200, 'start pending work'), restored + 1);
    await eventually('the pending-work request releases its session', async () => {
      assert.equal((await activity()).homeSessions, 0);
    });
    await advance(482000 + 14 * 60 * 1000);
    assert.equal((await activity()).residentRoots, 1, 'pending work protects the tree before 15 minutes');
    const afterPendingExpiry = 482000 + 15 * 60 * 1000 + 1000;
    await advance(afterPendingExpiry);
    await retired();
    assert.equal(await send(managed, 'after the deadline'), (restored + 2) + ':preserved',
      'expiry parks the attachment before aborting pending work');
    const resetClosed = next(managed, 'close');
    await call('/reset', { timeout: 7000 }).catch(() => null);
    const [code, reason] = await resetClosed;
    assert.equal(code, 1011);
    assert.equal(reason.toString(), 'Durable Object reset');
    await eventually('the forced reset releases its home before the next upgrade', async () => {
      const current = await activity();
      assert.equal(current.homeSessions, 0);
      assert.equal(current.residentRoots, 0);
    });
    await eventually('a fresh request opens on the surviving generation', async () => {
      json(await call('/status'), 200, 'read after reset');
    });

    const slow = open('/managed');
    const greeting = next(slow, 'message');
    void greeting.catch(() => {});
    const nodeLogs = () => cluster.compose(['logs', '--no-color', '--tail', '100', owner.nodeId]);
    const waitForHeldWrite = count => eventually('the managed write reaches the node test gate', async () => {
      assert.equal((await nodeLogs()).match(/\[websocket-test\] managed write held/g)?.length ?? 0, count);
    });
    try {
      await next(slow, 'open');
      assert.equal((await greeting)[0].toString(), 'welcome');
      await writeFile(control('hold-socket-write'), '');
      const delivered = next(slow, 'message', 10000);
      json(await call('/write'), 200, 'start a slow write');
      await waitForHeldWrite(1);
      await delay(5300);
      assert.equal(slow.readyState, Socket.OPEN, 'a write may take more than five seconds outside parking');
      await unlink(control('hold-socket-write'));
      assert.equal((await delivered)[0].toString(), 'write-finished');
      await writeFile(control('hold-socket-write'), '');
      json(await call('/write'), 200, 'start a write that will block parking');
      await waitForHeldWrite(2);
      const stalledClosed = next(slow, 'close', 12000);
      await advance(afterPendingExpiry + 15 * 60 * 1000 + 3000);
      await stalledClosed;
      const logs = await nodeLogs();
      assert.ok(logs.includes('"cause":"park-write-timeout"'), 'the node closed the write that prevented parking');
      await unlink(control('hold-socket-write'));
    } finally {
      slow.terminate();
    }
    assert.equal((await durableGeneration(cluster, owner.nodeId)).generation, generation);
  } finally {
    ordinary.terminate();
    managed.terminate();
  }
}

// The profiler uses the standard cluster. Its node-level fault runs last.
export const cases = { deployment, 'sql-atomicity': sqlAtomicity,
  'invocation-budgets': invocationBudgets, 'd1-invocation-budgets': d1InvocationBudgets, 'cpu-isolation': cpuIsolation,
  'process-replacement': processReplacement, 'log-quota': logQuota,
  'durable-takeover': durableTakeover };

export const webSocketCases = { 'websocket-admission': webSocketAdmission };

export const durableGatedCases = {
  'invocation-budget-lifetime': { test: invocationBudgetLifetime, budgetLifetime },
  'invocation-budget-threaded-background': {
    test: invocationBudgetLifetime, budgetLifetime, nodes: ['node-1', 'node-2', 'node-3'],
  },
  'durable-peer-ack': { test: durablePeerAck },
  'durable-slow-holder': {
    test: durableSlowHolder, delayRecoveryPeers: true,
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
  },
  'durable-threaded-mixed': { test: mixedDurableStorage, nodes: ['node-1'] },
  'durable-threaded-database-release': {
    test: threadedDatabaseRelease, nodes: ['node-1', 'node-2', 'node-3'], holdPeerAcks: false,
  },
  'durable-threaded-database-alarm': {
    test: threadedDatabaseAlarm, nodes: ['node-1'], databaseFixtureNodes: ['node-1'],
    databaseAlarmFixture: true, holdPeerAcks: false,
  },
  'durable-threaded-database-placement': {
    test: threadedDatabasePlacement, nodes: ['node-1'], databaseFixtureNodes: ['node-1'],
    publishPeerPorts: true, controlDurableLifecycle: true, holdPeerAcks: false,
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
  },
  'durable-threaded-database-accounting': {
    test: threadedDatabaseAccounting, nodes: ['node-1'], databaseFixtureNodes: ['node-1'], holdPeerAcks: false,
    platformFixture: () => [{ worker: 'sql-accounting/object', bindingClass: 'Echo', className: 'PlatformEcho' }],
  },
  'durable-threaded-admission-refusal': {
    test: durableAdmissionRefusal, nodes: ['node-1', 'node-2'], databaseFixtureNodes: ['node-2'],
    controlDurableLifecycle: true, confirmTimeoutMs: 25000,
    configureNode: node => node === 'node-1' ? ({
      durableObjects: { threadedStorage: true, residentObjects: 1, confirmTimeoutMs: 25000 },
    }) : ({}),
    nodeEnvironment: node => node === 'node-2' ? { COWORKERD_DO_TEST_MAILBOX_EVENTS: '1' } : {},
  },
  'durable-threaded-copy-reservation-refusal': {
    test: durableCopyReservationRefusal, nodes: ['node-1'], databaseFixtureNodes: ['node-1'],
    controlDurableLifecycle: true, holdPeerAcks: false,
  },
  'threaded-sql-deadline': {
    test: threadedSqlDeadline, nodes: ['node-1'], databaseFixtureNodes: ['node-1'],
  },
  'threaded-sql-transaction-limit': {
    test: threadedSqlTransactionLimit, nodes: ['node-1'], databaseFixtureNodes: ['node-1'], holdPeerAcks: false,
    // Only the owner uses the small record cap. Its two peers need room to
    // hold the schema record and the boundary record together.
    configureNode: node => node === 'node-1' ? ({
      durableObjects: { threadedStorage: true, peerBufferBytes: sqlWalPeerBufferBytes,
        checkpointBytes: 8 * 1024 * 1024 },
    }) : ({}),
  },
  'threaded-sql-cli-parity': {
    test: threadedSqlCliParity, nodes: ['node-1', 'node-2', 'node-3'],
    databaseFixtureNodes: ['node-1', 'node-2', 'node-3'], holdPeerAcks: false,
    configureNode: () => ({ durableObjects: { threadedStorage: true, segmentIntervalMs: 1000 } }),
  },
  'durable-threaded-takeover': { test: durableTakeover, nodes: ['node-1', 'node-2', 'node-3'] },
  'durable-platform-definition': {
    test: threadedPlatformDefinition, nodes: ['node-1', 'node-2', 'node-3'], controlDurableLifecycle: true,
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
    platformFixture: node => ['platform-first/object', 'platform-second/object'].flatMap(worker => [
      // Each node deliberately forwards a different class name for the same namespace.
      ...['Record', 'Probe'].map(bindingClass => ({ worker, bindingClass, className: `PlatformStore${node.at(-1)}` })),
      { worker, bindingClass: 'Echo', className: 'PlatformEcho' },
    ]),
  },
  'durable-threaded-fence': {
    test: durableThreadedFence, nodes: ['node-1', 'node-2', 'node-3'],
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
    confirmTimeoutMs: 25000,
  },
  'durable-threaded-alarm-fence': {
    test: threadedAlarmTakeover, nodes: ['node-1', 'node-2', 'node-3'],
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
    confirmTimeoutMs: 25000,
  },
  'durable-threaded-migration': {
    test: threadedClassMigration, nodes: ['node-1', 'node-2', 'node-3'], versionDrainTimeoutMs: 10000,
  },
  'durable-threaded-hibernation': {
    test: threadedSocketHibernation, nodes: ['node-1', 'node-2', 'node-3'],
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
  },
  'durable-threaded-retirement': {
    test: threadedSocketRetirement, nodes: ['node-1', 'node-2', 'node-3'], controlDurableLifecycle: true,
  },
};

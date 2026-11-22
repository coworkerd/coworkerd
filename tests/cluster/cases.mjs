import assert from 'node:assert/strict';
import { unlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { eventually, json, request } from './cluster.mjs';

function respondWith(value) {
  return `export default { fetch() { return new Response(${JSON.stringify(value)}); } };`;
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

async function sqlAtomicity(cluster) {
  const { path, token } = await cluster.tenant('sql-test');
  const database = `${path}/databases/ledger`;
  await cluster.api(database, { method: 'POST', token });
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
    assert.equal((await request(cluster.urls[node], '/__coworkerd/ready')).status, 200);
  });
  assert.deepEqual(await workerCall(cluster, node, tenant, 'hello'), { status: 200, text: 'alive' },
    'the first Worker call after the replacement is ready succeeds');
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
  const gate = join(cluster.peerAckGate, 'hold');
  const earlyResponses = [];
  for (const [suffix, value] of [['', 'http'], ['/rpc', 'rpc']]) {
    await eventually('both peers finished releasing earlier acknowledgements', async () => {
      const observations = await Promise.all(peers.map(node => cluster.state(node)));
      assert.ok(observations.every(state => state.durableLog.peers.heldAcksForTest === 0));
    });
    await writeFile(gate, '');
    let receivedHeaders = false;
    const write = fetch(cluster.urls[ingress] + path + suffix, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify(value), signal: AbortSignal.timeout(10000),
    }).then(async response => {
      receivedHeaders = true;
      return { status: response.status, text: await response.text() };
    }).catch(error => ({ error }));
    let result;
    try {
      await eventually(`both peers hold positive append acknowledgements for the ${value} write`, async () => {
        const observations = await Promise.all(peers.map(node => cluster.state(node)));
        assert.ok(observations.every(state => state.durableLog.peers.heldAcksForTest > 0));
      }, { timeout: 3000, interval: 25 });
      assert.deepEqual(await read('/neighbor'), { status: 200, text: 'neighbor' });
      await delay(250);
      if (receivedHeaders) earlyResponses.push(value);
    } finally {
      await unlink(gate);
      result = await write;
    }
    if (result.error) throw result.error;
    assert.equal(json(result, 200, `${value} write after acknowledgement release`), value);
    assert.equal(json(await read(''), 200, `${value} stored value`), value);
  }
  assert.deepEqual(earlyResponses, [], 'responses arrived while both peers held their acknowledgements');
}

async function durableTakeover(cluster) {
  const tenant = await cluster.tenant('durable-test');
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
  try {
    await warm.promise;
    inFlightAtKill = pending;
    assert.ok(inFlightAtKill > 0, 'writes are in flight when the kill is requested');
    killing = true;
    await cluster.compose(['kill', '-s', 'SIGKILL', original.nodeId]);
  } finally {
    writing = false;
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

// The only node-level fault runs last so earlier cases keep all three nodes.
export const cases = { deployment, 'sql-atomicity': sqlAtomicity, 'cpu-isolation': cpuIsolation,
  'process-replacement': processReplacement, 'log-quota': logQuota,
  'durable-peer-ack': durablePeerAck, 'durable-takeover': durableTakeover };

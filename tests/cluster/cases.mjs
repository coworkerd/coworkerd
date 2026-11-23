import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { unlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
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

async function durableTakeover(cluster) {
  const tenant = await takeoverWriter(cluster, 'durable-test');
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
    import { DurableObject } from 'cloudflare:workers';
    export class ${className} extends DurableObject {
      async fetch(request) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/alarm')) {
          const at = await request.json();
          await this.ctx.storage.setAlarm(at);
          return Response.json(at);
        }
        if (path.endsWith('/socket')) {
          const [client, socket] = Object.values(new WebSocketPair());
          socket.accept();
          socket.addEventListener('message', event => socket.send('${version}:' + event.data));
          return new Response(null, { status: 101, webSocket: client });
        }
        if (path.endsWith('/waiting')) return Response.json(await this.ctx.storage.get('waiting') ?? false);
        if (path.endsWith('/wait-for-headers')) {
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
      async alarm() { await this.ctx.storage.put('alarm-version', '${version}'); }
    }
    export default { fetch(request, env) { return env.OBJECT.getByName('stable').fetch(request); } };
  `;
  const deploy = (version, className, migrations) => cluster.deploy(tenant, name, source(className, version), {
    version, wrangler: { migrations, durable_objects: { bindings: [{ name: 'OBJECT', class_name: className }] } },
  });
  await deploy('v1', 'Record', [initial]);
  const first = await eventually('v1 creates a threaded record', async () => {
    const value = json(await call('', { method: 'POST', body: 'retained' }), 200, 'write v1');
    assert.equal(value.version, 'v1');
    return value;
  });
  const identity = (await namespace('Record')).namespaceId;
  const socketUrl = new URL(url + '/socket', cluster.urls[ingress]);
  socketUrl.protocol = 'ws:';
  const socket = new TestSocket(socketUrl);
  const socketEvent = (event, timeoutMs) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error(`waiting for WebSocket ${event}`)), timeoutMs);
    return once(socket, event, { signal: controller.signal }).finally(() => clearTimeout(timer));
  };
  await socketEvent('open', 5000);
  const echoed = socketEvent('message', 5000);
  socket.send('probe');
  assert.equal((await echoed)[0].toString(), 'v1:probe');
  const socketClosed = socketEvent('close', 25000);
  void socketClosed.catch(() => {});
  const due = Date.now() + 8000;
  assert.equal(json(await call('/alarm', { method: 'POST', body: due }), 200, 'schedule v1 alarm'), due);
  const waiting = call('/wait-for-headers', { timeout: 60000 }).then(
    value => ({ value }), error => ({ error }),
  );
  await eventually('the second v1 request is waiting before response headers', async () => {
    assert.equal(json(await call('/waiting'), 200, 'v1 handler entered'), true);
  });
  const held = await fetch(cluster.urls[ingress] + url + '/held', { signal: AbortSignal.timeout(30000) });
  assert.equal(held.status, 200);
  const reader = held.body.getReader();
  try {
    const firstChunk = await reader.read();
    assert.equal(Buffer.from(firstChunk.value).toString(), 'entered:');
    await deploy('v2', 'RenamedRecord', [initial, renamed]);
    await eventually('the first v2 request is refused while v1 still holds the tree', async () => {
      const response = await call('');
      assert.equal(response.status, 503);
      assert.match(response.text, /DO_VERSION_DRAINING/);
    });
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
    assert.equal(pending.value.status, 503, 'an old call without response headers can be retried');
    assert.match(pending.value.text, /DO_CODE_UPDATED/);
    assert.match(pending.value.text, /Durable Object reset because its code was updated/);
  } finally {
    await reader.cancel().catch(() => {});
  }

  await eventually('renamed class uses the existing threaded storage', async () => {
    assert.equal((await namespace('RenamedRecord')).namespaceId, identity);
    const entry = json(await call(''), 200, 'read v2');
    assert.deepEqual(entry, { id: first.id, value: 'retained', version: 'v2', alarmVersion: 'v2' });
  });
  assert.equal((await socketClosed)[0], 1001, 'version drain closes the old connection');
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
      assert.equal(gone.status, 404);
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
    import { DurableObject } from 'cloudflare:workers';
    export class Sleeper extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        this.constructedWith = ctx.getWebSockets('retained').length;
        ctx.blockConcurrencyWhile(async () => {
          this.instance = (await ctx.storage.get('constructors') ?? 0) + 1;
          await ctx.storage.put('constructors', this.instance);
        });
      }
      async fetch(request) {
        if (new URL(request.url).pathname.endsWith('/terminal')) {
          return Response.json(await this.ctx.storage.get('terminal') ?? null);
        }
        if (request.headers.get('upgrade')?.toLowerCase() === 'websocket') {
          const [client, socket] = Object.values(new WebSocketPair());
          this.ctx.acceptWebSocket(socket, ['retained']);
          socket.serializeAttachment({ field: 42 });
          this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('tick', 'tock'));
          return new Response(null, { status: 101, webSocket: client });
        }
        return Response.json({ instance: this.instance, sockets: this.ctx.getWebSockets('retained').length,
          constructedWith: this.constructedWith });
      }
      webSocketMessage(socket, message) {
        socket.send(JSON.stringify({
          instance: this.instance, constructedWith: this.constructedWith,
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
    export default {
      fetch(request, env) { return env.TREE.getByName('parked').fetch(request); }
    };
  `, { wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['Sleeper'] }],
    durable_objects: { bindings: [{ name: 'TREE', class_name: 'Sleeper' }] } } });

  await eventually('the threaded object answers successfully', async () => {
    assert.deepEqual(json(await request(cluster.urls[ingress], path), 200, 'open sleeper'),
      { instance: 1, sockets: 0, constructedWith: 0 });
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
  const sockets = [first, second];
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
  let paused = false;
  try {
    await Promise.all(sockets.map(socket => next(socket, 'open')));
    await ping(first, 'before');
    assert.equal(await send(first, 'tick'), 'tock');
    const firstEvent = JSON.parse(await send(first, 'warm'));
    assert.equal(firstEvent.instance, 1);
    assert.equal(firstEvent.retained, 2);
    assert.deepEqual(firstEvent.attachment, { field: 42 });
    assert.ok(Number.isSafeInteger(firstEvent.automatic));

    const firstRetirement = await retired(0);
    const parked = (await cluster.state(owner.nodeId)).webSockets;
    assert.equal(parked.parked, 2);
    await ping(second, 'while parked');
    assert.equal(await send(second, 'tick'), 'tock');
    const afterAutomatic = await activity();
    assert.equal(afterAutomatic.residentRoots, 0);
    assert.equal(afterAutomatic.homeSessions, 0);
    assert.equal(afterAutomatic.idleRetirements, firstRetirement);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.socketWakes, parked.socketWakes);
    assert.deepEqual(json(await request(cluster.urls[owner.nodeId], path), 200, 'normal HTTP wake'),
      { instance: 2, sockets: 2, constructedWith: 2 }, 'a normal reopen restores both sockets before the constructor runs');

    const secondRetirement = await retired(firstRetirement);
    assert.equal(await send(first, 'tick'), 'tock');
    const wake = JSON.parse(await send(first, 'wake'));
    assert.equal(wake.instance, 3);
    assert.equal(wake.message, 'wake');
    assert.equal(wake.retained, 2);
    assert.equal(wake.constructedWith, 2);
    assert.deepEqual(wake.attachment, { field: 42 });
    assert.ok(wake.automatic > firstEvent.automatic);
    assert.deepEqual(JSON.parse(await send(second, Buffer.from([0, 128, 255]))).message, [0, 128, 255]);

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
    await retired(secondRetirement);

    const survivor = cluster.nodes.find(node => node !== owner.nodeId);
    const closed = sockets.map(socket => next(socket, 'close', 45000));
    closed.forEach(promise => void promise.catch(() => {}));
    await cluster.compose(['pause', owner.nodeId]);
    paused = true;
    await eventually('a surviving node takes the parked socket shard', async () => {
      const read = json(await request(cluster.urls[survivor], path, { timeout: 7000 }), 200, 'takeover wake');
      assert.deepEqual(read, { instance: 4, sockets: 0, constructedWith: 0 });
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

// The profiler uses the standard cluster. Its node-level fault runs last.
export const cases = { deployment, 'sql-atomicity': sqlAtomicity, 'cpu-isolation': cpuIsolation,
  'process-replacement': processReplacement, 'log-quota': logQuota,
  'durable-takeover': durableTakeover };

export const durableGatedCases = {
  'durable-peer-ack': { test: durablePeerAck },
  'durable-threaded-mixed': { test: mixedDurableStorage, nodes: ['node-1'] },
  'durable-threaded-takeover': { test: durableTakeover, nodes: ['node-1', 'node-2', 'node-3'] },
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
};

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
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
  const { owners: [old] } = await namespace();
  assert.ok(old && cluster.nodes.includes(old.nodeId));
  const peers = cluster.nodes.filter(node => node !== old.nodeId);
  const [ingress] = peers;
  await eventually('both peers are reachable before the fence', async () => {
    const { durableLog } = await cluster.state(old.nodeId);
    assert.equal(durableLog.peers.want, 2);
    assert.ok(peers.every(peer => durableLog.peers.streams.some(stream => stream.nodeId === peer && stream.open)));
  });
  const child = await durableGeneration(cluster, old.nodeId);
  assert.equal(child.lifecycle?.durableServer, 'threaded');
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
    }, { timeout: 18000, interval: 100 });
    assert.ok(performance.now() - sentAt < cluster.confirmTimeoutMs - 1000,
      'the old write has time left on its confirmation deadline when the new epoch is known');
  } finally {
    await unlink(gate);
    if (paused) await cluster.compose(['unpause', old.nodeId]);
  }
  const result = await staleWrite;
  assert.ok(result.error || result.status >= 400, 'a write from the fenced generation never returned success');
  const saved = await recovery;
  if (saved.error) throw saved.error;
  assert.ok(saved.includes(1), 'the later owner keeps the earlier confirmed write');
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
          await this.ctx.storage.setAlarm(at);
          return Response.json(at);
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

  const at = Date.now() + 500;
  assert.equal(await write('/alarm', at), at);
  await delay(1600);
  assert.equal((await target.read()).fired, false, 'the threaded generation does not deliver a due alarm');
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

// The only node-level fault runs last so earlier cases keep all three nodes.
export const cases = { deployment, 'sql-atomicity': sqlAtomicity, 'cpu-isolation': cpuIsolation,
  'process-replacement': processReplacement, 'log-quota': logQuota,
  'durable-peer-ack': durablePeerAck, 'durable-takeover': durableTakeover };

export const durableThreadedCases = {
  'durable-threaded-mixed': { test: mixedDurableStorage, nodes: ['node-1'] },
  'durable-threaded-takeover': { test: durableTakeover, nodes: ['node-1', 'node-2', 'node-3'] },
  'durable-threaded-fence': {
    test: durableThreadedFence, nodes: ['node-1', 'node-2', 'node-3'],
    membership: { ttlSeconds: 4, heartbeatIntervalMs: 800, livenessGraceMs: 1000, listIntervalMs: 200 },
    confirmTimeoutMs: 20000,
  },
};

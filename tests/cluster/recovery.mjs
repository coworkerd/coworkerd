import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [kernel, countArg, mode] = process.argv.slice(2);
const count = Number(countArg);
if (!kernel || ![200, 1000].includes(count) || !['loss', 'restart'].includes(mode)) {
  console.error('Usage: node tests/cluster/recovery.mjs <kernel-directory> <200|1000> <loss|restart>');
  process.exit(2);
}

const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-recovery-')), resolve(kernel), controller.signal);
const seconds = milliseconds => Math.round(milliseconds) / 1000;

try {
  await cluster.start();
  const tenant = await cluster.tenant('recovery-test');
  await cluster.deploy(tenant, 'recovery', `
    import { DurableObject } from 'cloudflare:workers';
    export class Record extends DurableObject {
      async fetch(request) {
        if (request.method === 'PUT') await this.ctx.storage.put('value', await request.json());
        return Response.json(await this.ctx.storage.get('value') ?? null);
      }
    }
    export default { fetch(request, env) {
      return env.RECORDS.getByName(new URL(request.url).pathname.split('/').at(-1)).fetch(request);
    } };
  `, { wrangler: {
    durable_objects: { bindings: [{ name: 'RECORDS', class_name: 'Record' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record'] }],
  } });
  const namespace = () => cluster.api(`${tenant.path}/workers/recovery/namespaces/Record`, { token: tenant.token });
  const call = (node, name, options) => request(cluster.urls[node], `/${tenant.name}/recovery/${name}`, options);
  const [first] = cluster.nodes;
  await eventually('deployment serves', async () => assert.equal(json(await call(first, 'warm'), 200, 'warm'), null));
  const { namespaceId } = await namespace();
  const shardOf = name => Number(createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update(name)
    .digest().readBigUInt64BE() % 1024n);
  // Use 16 shards from one holder and distinct object names on those shards.
  const seeds = new Map();
  for (let serial = 0; seeds.size < 64; ++serial) seeds.set(shardOf(`seed-${serial}`), `seed-${serial}`);
  for (const name of seeds.values()) assert.equal(json(await call(first, name), 200, name), null);
  const { owners } = await namespace();
  const original = cluster.nodes.find(node => owners.filter(owner => owner.nodeId === node && !owner.released).length >= 16);
  assert.ok(original, 'one node owns at least 16 of the seeded shards');
  const shards = new Set(owners.filter(owner => owner.nodeId === original && !owner.released).slice(0, 16).map(owner => owner.shard));
  const objects = [];
  for (let serial = 0; objects.length < count; ++serial) {
    const name = `record-${serial}`;
    if (shards.has(shardOf(name))) objects.push(name);
  }
  const survivors = cluster.nodes.filter(node => node !== original);
  const confirm = async name => {
    const value = await eventually(`confirm ${name}`, async () => {
      const response = await call(first, name, { method: 'PUT', body: name, timeout: 35000 });
      return json(response, 200, `confirmed ${name}`);
    });
    assert.equal(value, name);
  };
  for (let offset = 0; offset < count; offset += 16) await Promise.all(objects.slice(offset, offset + 16).map(confirm));
  const before = Object.fromEntries(await Promise.all(survivors.map(async node => [node, await cluster.state(node)])));
  const originalState = await cluster.state(original);
  // Disable only this project container's restart policy before delivering SIGKILL.
  const container = await cluster.compose(['ps', '-q', original]);
  await cluster.command('docker', ['update', '--restart=no', container]);
  const fault = performance.now();
  await cluster.compose(['kill', '-s', 'SIGKILL', original]);
  if (mode === 'restart') {
    await cluster.compose(['start', original]);
    await cluster.refreshUrl(original);
  }
  let detected;
  if (mode === 'loss') {
    await eventually('the surviving members detect the lost holder', async () => {
      for (const state of await Promise.all(survivors.map(node => cluster.state(node))))
        assert.ok(!state.membership.members.some(member => member.nodeId === original));
    }, { timeout: 60000, interval: 200 });
    detected = performance.now();
  } else {
    await eventually('the original disk is ready after node restart', async () => {
      const state = await cluster.state(original);
      assert.ok(state.durableLog.localRecovery.startupReady);
      assert.notEqual(state.durableLog.nodeEpoch, originalState.durableLog.nodeEpoch);
    }, { timeout: 60000, interval: 200 });
    detected = performance.now();
  }
  console.log(JSON.stringify({ mode, count, holder: original, shards: shards.size, detectionSeconds: seconds(detected - fault) }));
  const readTimes = [];
  const sample = {};
  const metadataSamples = {};
  const polling = new AbortController();
  const sampleStates = async () => {
    while (!polling.signal.aborted) {
      for (const node of mode === 'loss' ? survivors : [original, ...survivors]) {
        const { membership, durableLog, peerConnectBudget } = await cluster.state(node);
        if (node !== original) {
          assert.equal(membership.leaseFences, before[node].membership.leaseFences, `${node} lease fences`);
          assert.equal(durableLog.confirm.leaseLosses, before[node].durableLog.confirm.leaseLosses, `${node} lease losses`);
        }
        const { activeRebuilds, activeBackgroundRebuilds, waitingRebuilds = 0 } = durableLog.objects;
        assert.ok(activeBackgroundRebuilds <= activeRebuilds && activeRebuilds <= 4);
        const item = sample[node] ??= { maxActive: 0, maxWaiting: 0 };
        item.maxActive = Math.max(item.maxActive, activeRebuilds);
        item.maxWaiting = Math.max(item.maxWaiting, waitingRebuilds);
        if (peerConnectBudget) {
          assert.ok(peerConnectBudget.peak <= 4);
          item.peerConnectBudget = peerConnectBudget;
        }
        if (durableLog.recovery.scheduler) {
          const { entries, ...scheduler } = durableLog.recovery.scheduler;
          const now = performance.now();
          const previous = metadataSamples[node];
          if (previous) assert.ok(scheduler.metadataRequests - previous.requests <= 4 + Math.ceil(16 * (now - previous.at) / 1000),
            `${node} exceeded its metadata request budget`);
          metadataSamples[node] = { at: now, requests: scheduler.metadataRequests };
          assert.ok(scheduler.peakMetadataRequests <= 2 && scheduler.peakMetadataBytes <= 16 * 1024 * 1024);
          item.scheduler = scheduler;
        }
      }
      await delay(1000, undefined, { signal: polling.signal }).catch(error => {
        if (error.name !== 'AbortError') throw error;
      });
    }
  };
  const monitoring = sampleStates();
  monitoring.catch(() => controller.abort());
  try {
    await Promise.all(objects.map(async (name, index) => {
      const node = mode === 'restart' ? original : survivors[index % survivors.length];
      const limit = process.env.COWORKERD_RECOVERY_MEASURE_BASELINE ? count * 10000 : mode === 'loss' && count === 200 ? 55000 : 300000;
      await delay(Math.floor(index / 100) * 20);
      const value = await eventually(`read ${name}`, async () => {
        controller.signal.throwIfAborted();
        const response = await call(node, name, { timeout: 33000 });
        return json(response, 200, `read ${name}`);
      }, { timeout: limit, interval: 250 });
      assert.equal(value, name, 'the acknowledged value survived');
      readTimes.push(performance.now() - fault);
      if (readTimes.length % 100 === 0)
        console.log(JSON.stringify({ count, readable: readTimes.length, seconds: seconds(readTimes.at(-1)) }));
    }));
  } finally {
    polling.abort();
    await monitoring;
  }
  const last = Math.max(...readTimes);
  console.log(JSON.stringify({ mode, count, firstReadSeconds: seconds(Math.min(...readTimes)),
    medianReadSeconds: seconds(readTimes[Math.ceil(count / 2) - 1]), allReadSeconds: seconds(last),
    afterDetectionSeconds: seconds(last - (detected - fault)), nodes: sample }));
  if (mode === 'loss' && count === 200 && !process.env.COWORKERD_RECOVERY_MEASURE_BASELINE)
    assert.ok(last - (detected - fault) <= 55000, 'all 200 objects are readable within 55 s after detection');
} catch (error) {
  await cluster.diagnostics();
  throw error;
} finally {
  await cluster.close();
}

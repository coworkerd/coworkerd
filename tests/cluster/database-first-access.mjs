import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile } from 'node:fs/promises';
import { arch, cpus, loadavg, tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [kernel, mode, countArg, ...extra] = process.argv.slice(2);
const count = mode === 'cold' ? 200 : Number(countArg);
const measureBaseline = process.env.COWORKERD_RECOVERY_MEASURE_BASELINE === '1';
if (!kernel || !['cold', 'loss'].includes(mode) || extra.length ||
    (mode === 'cold' ? countArg !== undefined : ![100, 500, 1000, 2000].includes(count))) {
  console.error('Usage: node tests/cluster/database-first-access.mjs <kernel> <cold | loss <100|500|1000|2000>>');
  process.exit(2);
}

const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-db-first-access-')), resolve(kernel),
  controller.signal, { singleNode: mode === 'cold', publishPeerPorts: true });
const [original, ...survivors] = cluster.nodes;
const tenantName = 'db-first-access';
const peerUrls = {};
const write = (event, fields) => console.log(JSON.stringify({ event, at: new Date().toISOString(), ...fields }));

function rankOwners(namespace) {
  // DurableShards ranks the member for shard zero by these rendezvous scores.
  return cluster.nodes.map(node => [node, createHash('sha256')
    .update(namespace + '/0\0' + node).digest().readBigUInt64BE()])
    .sort(([, left], [, right]) => left > right ? -1 : left < right ? 1 : 0).map(([node]) => node);
}

async function databases(tenant, total) {
  const selected = [];
  const perSuccessor = new Map(survivors.map(node => [node, 0]));
  for (let index = 0; selected.length < total; ++index) {
    const name = 'db-' + index;
    const created = await cluster.api(`${tenant.path}/databases/${name}`, { method: 'POST', token: tenant.token });
    assert.equal(created.database.shards, 1);
    const namespace = created.database.namespaceId;
    const [owner, successor] = rankOwners(namespace);
    if (mode === 'loss') {
      if (owner !== original || perSuccessor.get(successor) >= total / survivors.length) continue;
      perSuccessor.set(successor, perSuccessor.get(successor) + 1);
    }
    selected.push({ name, namespace, successor });
  }
  const { revision } = await cluster.api('/v1/tenants');
  await eventually('the database catalog revision is applied', async () => {
    for (const node of cluster.nodes) {
      const { deployment } = await cluster.state(node);
      assert.ok(Number(deployment.revision) >= Number(revision), `${node} sees revision ${revision}`);
    }
  }, { timeout: 60000 });
  assert.equal(new Set(selected.map(database => database.namespace)).size, total);
  return selected;
}

async function query(node, database, sql, retries = 0) {
  const path = `/__coworkerd/databases/${tenantName}/${database.name}/query`;
  const options = { method: 'POST', token: cluster.management, timeout: 35000, body: { sql } };
  let response = await request(peerUrls[node], path, options);
  for (let remaining = retries; remaining > 0 && response.status === 503; --remaining) {
    write('seed-retry', { database: database.name, status: response.status, response: response.text });
    await delay(250, undefined, { signal: controller.signal });
    response = await request(peerUrls[node], path, options);
  }
  return json(response, 200, database.name);
}

function verifyRow(database, answer, value) {
  assert.equal(answer.success, true, JSON.stringify(answer).slice(0, 500));
  assert.deepEqual(answer.results, [{ value }], database.name);
}

function summarize(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const at = fraction => sorted[Math.ceil(sorted.length * fraction) - 1];
  return { firstMs: sorted[0], p50Ms: at(0.5), p99Ms: at(0.99), lastMs: sorted.at(-1) };
}

async function hostSample(node) {
  const vm = await cluster.compose(['exec', '-T', node, 'sh', '-ec',
    'cat /proc/loadavg; awk \'/^(MemAvailable|SwapFree):/\' /proc/meminfo; cat /proc/pressure/cpu /proc/pressure/memory']);
  return { clientLoad: loadavg(), vm };
}

async function scheduler(node) {
  const { membership, durableLog, durableShards, peerConnectBudget, processes } = await cluster.state(node);
  const { entries, ...metrics } = durableLog.recovery.scheduler;
  const durable = processes.entries.find(entry => entry.processClass === 'durable');
  return { ...metrics, leaseFences: membership.leaseFences, leaseLosses: durableLog.confirm.leaseLosses,
    peakPeerConnect: peerConnectBudget?.peak, activeRebuilds: durableLog.objects.activeRebuilds,
    durableGeneration: durable?.generation, durableExitCount: durable?.exitCount,
    durableLastExit: durable?.lastExitCategory, durableProbeFailures: durable?.probeFailures,
    durablePeakProbeMs: Math.max(0, ...(durable?.probeDelayMs ?? [])),
    store: { log: durableLog.storeClient, shards: durableShards.store, membership: membership.storeClient },
    heartbeatPut: membership.heartbeatPut };
}

async function logs(node, since) {
  const container = await cluster.compose(['ps', '-a', '-q', node]);
  const output = await cluster.command('docker', ['logs', '--timestamps', '--since', since, container],
    { timeout: 30000, maxBuffer: 32 * 1024 * 1024 });
  const entries = [];
  const kills = [];
  for (const line of output.split('\n')) {
    const index = line.indexOf('[do-log] {');
    if (index >= 0) entries.push({ ...JSON.parse(line.slice(index + '[do-log] '.length)),
      loggedAtMs: Date.parse(line.slice(0, line.indexOf(' '))) });
    const kill = line.indexOf('[process-kill] {');
    if (kill >= 0) kills.push(JSON.parse(line.slice(kill + '[process-kill] '.length)));
  }
  return { entries, kills };
}

async function measureLogs(node, since, selected) {
  const byNamespace = new Map(selected.map(database => [database.namespace, database]));
  const samples = new Map();
  const waits = new Map();
  const starts = new Map();
  const rebuiltAt = [];
  let additionalRebuilds = 0;
  const { entries, kills } = await logs(node, since);
  for (const entry of entries) {
    if (entry.event === 'rebuild-started') {
      const [namespace] = entry.object.split('/');
      if (byNamespace.has(namespace)) starts.set(namespace, (starts.get(namespace) ?? 0) + 1);
    }
    if (entry.event === 'rebuilt') {
      const [namespace] = entry.object.split('/');
      if (!byNamespace.has(namespace)) continue;
      rebuiltAt.push(entry.loggedAtMs);
      if (samples.has(namespace)) {
        assert.equal(mode, 'loss', `${node} repeated cold rebuild of ${namespace}`);
        ++additionalRebuilds;
      } else {
        samples.set(namespace, entry);
      }
    }
    if (entry.event === 'measurement-epoch-gate') {
      const [, namespace] = entry.key.split('/');
      if (!byNamespace.has(namespace)) continue;
      if (!waits.has(namespace)) waits.set(namespace, []);
      waits.get(namespace).push(entry.waitMs);
    }
  }
  assert.equal(samples.size, selected.length, `${node} rebuilt all its assigned databases`);
  assert.equal(starts.size, selected.length, `${node} started all its assigned rebuilds`);
  for (const database of selected) {
    database.tailWaitMs = samples.get(database.namespace).laps.tailWaitMs;
    database.rebuildLoopDelayMs = samples.get(database.namespace).loopDelayMs;
    database.rebuiltAtMs = samples.get(database.namespace).loggedAtMs;
    database.epochGateWaitMs = waits.get(database.namespace);
  }
  const watchdogKills = kills.filter(entry => entry.processClass === 'durable' && entry.reason === 'watchdog timeout');
  return { additionalRebuilds,
    additionalStarts: [...starts.values()].reduce((sum, count) => sum + count - 1, 0),
    watchdogKills: watchdogKills.map(kill => ({ ...kill, databasesRebuiltDuringProbe: rebuiltAt.filter(at =>
      at <= kill.observedAtUnixMs && at >= kill.observedAtUnixMs - kill.stuckDurationMs).length })) };
}

async function runCold(databases, schedule) {
  const before = await scheduler(original);
  const hostBefore = await hostSample(original);
  const since = await cluster.compose(['exec', '-T', original, 'date', '-u', '+%Y-%m-%dT%H:%M:%S.%NZ']);
  const start = performance.now();
  const read = async database => {
    const sent = performance.now();
    verifyRow(database, await query(original, database, 'SELECT 1 AS value'), 1);
    database.elapsedMs = performance.now() - sent;
  };
  if (schedule === 'serial') {
    for (const database of databases) await read(database);
  } else {
    await Promise.all(databases.map(read));
  }
  const wallMs = performance.now() - start;
  const after = await scheduler(original);
  const hostAfter = await hostSample(original);
  await measureLogs(original, since, databases);
  for (const database of databases) write('cold-read', { schedule, ...database });
  write('cold-summary', { schedule, count, wallMs, latency: summarize(databases.map(db => db.elapsedMs)),
    tailWait: summarize(databases.map(db => db.tailWaitMs)), before, after, hostBefore, hostAfter });
}

async function runLoss(databases) {
  const sql = value => 'CREATE TABLE IF NOT EXISTS sample(value TEXT PRIMARY KEY); ' +
    `INSERT OR IGNORE INTO sample VALUES ('${value}'); SELECT value FROM sample`;
  const beforeSeed = await cluster.state(original);
  for (let offset = 0; offset < databases.length; offset += 16) {
    await Promise.all(databases.slice(offset, offset + 16)
      .map(async db => verifyRow(db, await query(original, db, sql(db.name), 2), db.name)));
  }
  const seeded = await cluster.state(original);
  assert.ok(seeded.durableLog.confirm.confirmations >= beforeSeed.durableLog.confirm.confirmations + count);
  assert.equal(seeded.durableLog.confirm.unconfirmed, 0);
  const before = Object.fromEntries(await Promise.all(survivors.map(async node => [node, await scheduler(node)])));
  const hostBefore = await hostSample(original);
  const since = await cluster.compose(['exec', '-T', original, 'date', '-u', '+%Y-%m-%dT%H:%M:%S.%NZ']);
  const fault = performance.now();
  await cluster.compose(['kill', '-s', 'SIGKILL', original]);
  await eventually('surviving members detect the lost owner', async () => {
    for (const state of await Promise.all(survivors.map(node => cluster.state(node))))
      assert.ok(!state.membership.members.some(member => member.nodeId === original));
  }, { timeout: 60000, interval: 200 });
  const detectionMs = performance.now() - fault;
  write('owner-lost', { databases: count, original, detectionMs });
  const reads = await Promise.all(databases.map(async (database, index) => {
    // Give the client a short ramp at larger counts to avoid a connection burst.
    if (count > 500) await delay(Math.floor(index / 100) * 20, undefined, { signal: controller.signal });
    let attempts = 0;
    const answer = await eventually(`read ${database.name}`, async () => {
      controller.signal.throwIfAborted();
      ++attempts;
      return query(database.successor, database, 'SELECT value FROM sample');
    }, { timeout: count === 2000 ? 300000 : 180000, interval: 250 });
    verifyRow(database, answer, database.name);
    return { ...database, elapsedMs: performance.now() - fault, attempts };
  }));
  reads.sort((left, right) => left.elapsedMs - right.elapsedMs);
  const readTimes = summarize(reads.map(db => db.elapsedMs));
  write('loss-readable', { count, detectionMs, reads: readTimes });
  const hostAfter = await hostSample(survivors[0]);
  const after = Object.fromEntries(await Promise.all(survivors.map(async node => [node, await scheduler(node)])));
  const rebuilds = {};
  for (const node of survivors) {
    assert.equal(after[node].leaseFences, before[node].leaseFences);
    assert.equal(after[node].leaseLosses, before[node].leaseLosses);
    rebuilds[node] = await measureLogs(node, since, reads.filter(db => db.successor === node));
  }
  for (const database of reads) write('loss-read', database);
  const gateTotals = reads.filter(db => db.epochGateWaitMs).map(db => db.epochGateWaitMs.reduce((sum, ms) => sum + ms, 0));
  write('loss-summary', { count, detectionMs, reads: readTimes, rebuilds,
    epochGateSamples: gateTotals.length, ...(gateTotals.length && { epochGateWait: summarize(gateTotals) }),
    before, after, hostBefore, hostAfter });
  if (!measureBaseline) {
    for (const node of survivors) {
      assert.equal(after[node].durableGeneration, before[node].durableGeneration, `${node} durable generation changed`);
      assert.equal(after[node].durableExitCount, before[node].durableExitCount, `${node} durable process exited`);
      assert.equal(rebuilds[node].watchdogKills.length, 0, `${node} watchdog killed a durable process`);
      assert.equal(rebuilds[node].additionalStarts, 0, `${node} started additional rebuilds`);
      assert.equal(rebuilds[node].additionalRebuilds, 0, `${node} completed additional rebuilds`);
      assert.ok(after[node].peakMetadataRequests <= 2, `${node} exceeded its metadata concurrency budget`);
      assert.ok(after[node].peakMetadataBytes <= after[node].metadataLimitBytes, `${node} exceeded its metadata memory budget`);
    }
    if (count === 2000) {
      assert.ok(readTimes.firstMs - detectionMs < 40000, 'first read exceeded 40 seconds after detection');
      assert.ok(readTimes.lastMs - detectionMs < 100000, 'last read exceeded 100 seconds after detection');
    }
  }
}

try {
  const metadata = JSON.parse(await readFile(join(resolve(kernel), 'kernel.json'), 'utf8'));
  write('conditions', { mode, count, architecture: metadata.architecture, patchSeries: metadata.patchSeriesSha256,
    client: { node: process.version, arch: arch(), cpu: cpus()[0].model, cores: cpus().length },
    docker: await cluster.command('docker', ['info', '--format',
      '{{.ServerVersion}} {{.OSType}}/{{.Architecture}} {{.NCPU}} CPU {{.MemTotal}} bytes']) });
  await cluster.start();
  for (const node of cluster.nodes) {
    const address = await cluster.compose(['port', node, '8934']);
    assert.match(address, /^127\.0\.0\.1:\d+$/);
    peerUrls[node] = 'http://' + address;
  }
  const tenant = await cluster.tenant(tenantName, { durableObjectNamespaces: Math.max(3000, 4 * count) });
  const selected = await databases(tenant, mode === 'cold' ? 2 * count : count);
  if (mode === 'cold') {
    await runCold(selected.slice(0, count), 'serial');
    await delay(1000, undefined, { signal: controller.signal });
    await runCold(selected.slice(count), 'concurrent');
  } else {
    await runLoss(selected);
  }
} catch (error) {
  await cluster.diagnostics();
  throw error;
} finally {
  await cluster.close();
}

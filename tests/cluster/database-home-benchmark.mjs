import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { arch, cpus, freemem, loadavg, platform, release, tmpdir, totalmem } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [side, checkoutArg, kernelArg, outputArg, mode, ...extra] = process.argv.slice(2);
if (!['object', 'native'].includes(side) || !checkoutArg || !kernelArg || !outputArg ||
    (mode !== undefined && !['--smoke', '--default-admission'].includes(mode)) || extra.length) {
  console.error('Usage: node tests/cluster/database-home-benchmark.mjs <object|native> <product-checkout> <pinned-kernel-directory> <new-report.jsonl> [--smoke|--default-admission]');
  process.exit(2);
}
const smoke = mode === '--smoke';
const defaultAdmission = mode === '--default-admission';
const checkout = resolve(checkoutArg);
const kernel = resolve(kernelArg);
const output = resolve(outputArg);
const binaries = resolve(process.env.COWORKERD_CLUSTER_BIN_DIR ?? join(checkout, 'build'));
const settings = {
  homes: smoke ? 16 : 520,
  density: smoke ? [0, 3, 8] : [0, 100, 500],
  coldSamples: smoke ? 6 : 200,
  warmSamples: smoke ? 20 : 1000,
  throughputMs: smoke ? 500 : 10000,
  densityClients: smoke ? 4 : 24,
};
const native = side === 'native';
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error('Interrupted by ' + signal)));
}

await writeFile(output, '', { flag: 'wx' });
const directory = await mkdtemp(join(tmpdir(), 'coworkerd-database-home-'));
process.env.COWORKERD_CLUSTER_BIN_DIR = binaries;
const cluster = new Cluster(directory, kernel, controller.signal, {
  singleNode: true,
  productRoot: checkout,
  configureNode: () => ({
    quarantineProcesses: 1,
    // The admission share derived from a small VM can refuse fewer than 500 copies.
    // Raise the planning budget while keeping an explicit, equal process cap.
    ...(!defaultAdmission && { memoryBudgetBytes: 16 * 1024 ** 3 }),
    processPool: { count: 1, inFlightLimit: 128,
      ...(!defaultAdmission && { threadedMemoryBytes: 3 * 1024 ** 3 }) },
    durableObjects: { processes: 1, threadedStorage: true, peers: 0 },
  }),
  nodeEnvironment: () => ({
    COWORKERD_DO_TEST_HOME_COUNT: String(settings.homes),
    // The normal cluster fixture also uses 32 so eight clients can queue at the same home.
    COWORKERD_DO_TEST_MAILBOX_EVENTS: '32',
  }),
});
const [owner] = cluster.nodes;
const tenantName = 'database-benchmark';
const workload = 'traffic';
const databases = Array.from({ length: defaultAdmission ? settings.homes + 1 : settings.density.at(-1) },
  (_, index) => 'db-' + String(index).padStart(4, '0'));
let nextId = 1;
let child;
let hz;

async function record(metric, data) {
  const row = JSON.stringify({ metric, side, at: new Date().toISOString(), ...data });
  await appendFile(output, row + '\n');
  if (!data.samplesMs) console.log(row);
  else console.log(JSON.stringify({ metric, side, n: data.n, p50Ms: data.p50Ms, p99Ms: data.p99Ms }));
}

const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const jsonFile = async path => JSON.parse(await readFile(path, 'utf8'));
const productFiles = ['docker/Dockerfile', 'docker/docker-compose.yml', 'docker/entrypoint.sh',
  'docker/storage-entrypoint.sh', 'runtime/runtime.js'];

async function provenance() {
  const commit = await cluster.command('git', ['rev-parse', 'HEAD'], { cwd: checkout });
  const pins = await jsonFile(join(checkout, 'patches/workerd/artifacts.json'));
  const metadata = await jsonFile(join(kernel, 'kernel.json'));
  assert.equal(metadata.patchSeriesSha256, pins.patchSeriesSha256);
  assert.equal(metadata.upstream, pins.upstream);
  const archiveSha256 = pins.archives[metadata.architecture];
  assert.ok(archiveSha256, 'checkout has a pinned release for ' + metadata.architecture);
  const hashes = {};
  for (const file of ['workerd', 'coworkerd-threaded', 'allowlist/coworkerd-syscalls-startup.txt',
    'allowlist/coworkerd-syscalls-steady.txt']) {
    hashes[file] = sha256(await readFile(join(kernel, file)));
    assert.equal(hashes[file], metadata.files[file], 'verified archive file ' + file);
  }
  const productHashes = {};
  for (const file of productFiles) {
    productHashes[file] = sha256(await readFile(join(checkout, file)));
  }
  for (const file of ['coworkerd', 'coworkerd-apiserver', 'coworkerd-launcher']) {
    hashes[file] = sha256(await readFile(join(binaries, file)));
  }
  await record('provenance', {
    commit, archive: pins.releaseTag, architecture: metadata.architecture, archiveSha256, hashes, productHashes,
    kernelBuildCommit: metadata.coworkerdCommit, kernelRun: metadata.githubActions?.url,
    settings, mode: mode ?? 'full', node: process.version,
    clientHost: { platform: platform(), arch: arch(), release: release(), model: cpus()[0].model,
      cpus: cpus().length, memoryBytes: totalmem() },
    docker: await cluster.command('docker', ['info', '--format',
      '{{.ServerVersion}} {{.OSType}}/{{.Architecture}} {{.NCPU}} CPU {{.MemTotal}} B']),
  });
  return hashes['coworkerd-threaded'];
}

async function workerSource() {
  const runtime = await readFile(join(checkout, 'runtime/runtime.js'), 'utf8');
  const start = runtime.indexOf('export class SqlDatabase extends DurableObject');
  const end = runtime.indexOf('export class BoundWorker', start);
  assert.ok(start >= 0 && end > start, 'locate the pinned SqlDatabase class');
  const alarmHeader = runtime.match(/^const DO_ALARM_HEADER = .*;$/m)?.[0];
  assert.ok(alarmHeader);
  // The platform class can use the internal prepare API. A deployed class has the public exec API.
  const database = runtime.slice(start, end)
    .replace('const counters = sqlite.prepare(', 'const counters = () => sqlite.exec(')
    .replace('sqlite.prepare(sql)(...bindings)', 'sqlite.exec(sql, ...bindings)')
    .replace('this.ctx.storage.sql.abortExecutionLimit()', 'this.ctx.storage.sql.abortExecutionLimit?.()');
  assert.ok(!database.includes('sqlite.prepare('), 'adapt all platform-only prepare calls');
  const fixture = await readFile(new URL('./database-home-worker.js', import.meta.url), 'utf8');
  return 'import { DurableObject } from "cloudflare:workers";\n' + alarmHeader + '\n' +
    'const native = ' + native + ';\n' + database + fixture;
}

async function setup() {
  const targets = [workload, ...databases];
  const tenant = await cluster.tenant(tenantName, { workers: targets.length, durableObjectNamespaces: targets.length });
  if (native) {
    for (const name of targets) {
      await cluster.api(tenant.path + '/databases/' + name, { method: 'POST', token: tenant.token });
    }
  }
  const packageDir = join(directory, 'workers');
  const source = await workerSource();
  const workerSourceSha256 = sha256(source);
  const codePackage = JSON.stringify({ modules: [
    { bytes: Buffer.byteLength(source), name: 'index.js', sha256: workerSourceSha256, type: 'esm' },
  ] });
  const contentHash = sha256(codePackage);
  const workers = [];
  const routes = [];
  for (const name of targets) {
    const path = join(packageDir, name, 'v1');
    await mkdir(path, { recursive: true, mode: 0o755 });
    await writeFile(join(path, 'index.js'), source);
    await writeFile(join(path, 'package.json'), codePackage);
    const durableObjects = native
      ? [{ binding: 'DB', className: '__coworkerdSqlDatabase', database: name }]
      : [{ binding: 'DB', className: 'BenchmarkDatabase' }];
    workers.push({ workerId: name, activeVersion: 'v1', versions: [{ versionId: 'v1', format: 'v2',
      compatibilityDate: '2026-10-01', mainModule: 'index.js', contentHash, durableObjects,
      ...(!native && { classes: { declared: ['BenchmarkDatabase'] } }) }] });
    routes.push({ host: '*', pathPrefix: '/' + tenantName + '/' + name, workerId: name });
  }
  await writeFile(join(packageDir, 'manifest.json'),
    JSON.stringify({ compatibilityDate: '2026-10-01', routes, workers }));
  cluster.env.COWORKERD_PROJECT_DIR = packageDir;
  // One desired-state update leaves deployment and trial loading outside all timing windows.
  await cluster.compose(['run', '--rm', '-T', '--no-deps', 'tool', 'upsert', '--manifest', '/work/manifest.json',
    '--tenant', tenantName, '--no-trial'], { timeout: 240000 });
  await eventually('all Worker routes applied on the owner', async () => {
    const state = await cluster.state(owner);
    assert.equal(state.deployment.validWorkerCount, targets.length);
    assert.equal(state.deployment.effectiveRouteCount, targets.length);
    const members = state.membership.members.filter(member => member.state === 'active');
    assert.deepEqual(members.map(member => member.nodeId).sort(), [...cluster.nodes].sort());
    assert.deepEqual(members.filter(member => member.durableProcesses > 0).map(member => member.nodeId), [owner]);
  }, { timeout: 120000 });
  await record('deployment', { workers: targets.length, workerSourceSha256 });
}

async function call(target, operation, query = '') {
  const url = cluster.urls[owner] + '/' + tenantName + '/' + target + '/' + operation + query;
  const start = performance.now();
  const response = await fetch(url, { signal: AbortSignal.any([controller.signal, AbortSignal.timeout(40000)]) });
  const body = await response.text();
  const elapsed = performance.now() - start;
  if (response.status !== 200) {
    const error = new Error(target + '/' + operation + ': HTTP ' + response.status + ' ' + body.slice(0, 400));
    error.httpStatus = response.status;
    throw error;
  }
  return { elapsed, bytes: Buffer.byteLength(body), body: JSON.parse(body), copy: response.headers.get('x-benchmark-copy') };
}

async function query(target, operation, suffix) {
  const answer = await call(target, operation, suffix);
  if (operation === 'ping') assert.equal(answer.body, 'ready');
  else if (operation === 'cold' || operation === 'read') assert.deepEqual(answer.body, [{ value: 1 }]);
  else if (operation === 'insert' || operation === 'seedLarge') assert.equal(answer.body, 1);
  else if (operation === 'create' || operation === 'createLarge') assert.equal(answer.body, 0);
  else if (operation === 'noop') assert.deepEqual(answer.body, []);
  else if (operation === 'emptyBatch') assert.equal(answer.body, 'empty batch reply decoded');
  else if (operation === 'large') {
    assert.equal(answer.body.length, 1);
    assert.equal(answer.body[0].value.length, 1044480);
    assert.equal(answer.body[0].value[0] + answer.body[0].value.at(-1), 'xx');
  }
  return answer;
}

function insert(target, id = ++nextId) {
  return query(target, 'insert', '?id=' + id);
}

async function parallel(items, clients, run) {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(clients, items.length) }, async () => {
    while (next < items.length) await run(items[next++]);
  }));
}

async function durable() {
  const state = await cluster.state(owner);
  const entries = state.processes.entries.filter(entry => entry.processClass === 'durable');
  assert.equal(entries.length, 1);
  const entry = entries[0];
  assert.equal(entry.sandbox.durableServer, 'threaded');
  assert.ok(entry.pid > 0 && entry.lifecycle?.durableAdmission);
  return { entry, log: state.durableLog, processPool: state.processes.configuration };
}

async function identify(kernelHash) {
  const { entry, processPool } = await eventually('threaded durable child is ready', async () => {
    const snapshot = await durable();
    assert.equal(snapshot.entry.binarySha256, kernelHash);
    assert.equal(snapshot.entry.lifecycle.durableAdmission.workerThreads, settings.homes);
    return snapshot;
  }, { timeout: 30000 });
  child = { processId: entry.processId, generation: entry.generation, pid: entry.pid };
  await record('child', { ...child, processPool });
}

async function validateConfirmation() {
  await query(workload, 'create');
  await insert(workload, 1);
  const before = (await durable()).log;
  await insert(workload);
  const after = (await durable()).log;
  assert.equal(after.peers.want, 0);
  assert.ok(after.confirm.confirmations > before.confirm.confirmations);
  assert.equal(after.confirm.unconfirmed, 0);
  await record('confirmation-probe', { configuredPeers: after.peers.configured,
    confirmationsBefore: before.confirm.confirmations, confirmationsAfter: after.confirm.confirmations });
}

const times = samples => {
  const ordered = [...samples].sort((a, b) => a - b);
  const at = fraction => ordered[Math.ceil(fraction * ordered.length) - 1];
  return { n: samples.length, p50Ms: at(0.5), p99Ms: at(0.99), samplesMs: samples };
};

function execOwner(code, ...args) {
  return cluster.compose(['exec', '-T', owner, 'sh', '-ec', code, 'sample', ...args.map(String)]);
}

async function hostSample() {
  const linux = await execOwner("head -n 1 /proc/stat; cat /proc/loadavg; awk '/^(MemAvailable|SwapFree):/' /proc/meminfo; cat /proc/pressure/cpu /proc/pressure/memory");
  return { at: new Date().toISOString(), clientLoad: loadavg(), clientFreeBytes: freemem(), linux };
}

async function cpuSample() {
  const stat = await execOwner('cat "/proc/$1/stat"', child.pid);
  const fields = stat.slice(stat.lastIndexOf(')') + 2).split(/\s+/);
  return { ticks: Number(fields[11]) + Number(fields[12]), start: fields[19] };
}

async function runCold() {
  const selected = databases.slice(0, settings.coldSamples);
  await parallel(selected, 16, name => query(name, 'ping'));
  const before = await hostSample();
  const samples = [], copies = new Set();
  for (const name of selected) {
    const { elapsed, copy } = await query(name, 'cold');
    samples.push(elapsed);
    if (!native) {
      assert.ok(copy && !copies.has(copy), 'each cold object uses a distinct isolate copy');
      copies.add(copy);
    }
  }
  await record('cold-new', { ...times(samples), distinctCopyIds: copies.size, before, after: await hostSample() });
  if (native) return;

  // One copy admits up to 64 trees by default. Seed a matching copy for each group of 50 opens.
  samples.length = 0;
  const warmCopies = new Set();
  for (let start = 0; start < selected.length; start += 50) {
    const name = selected[start];
    const { copy } = await query(name, 'cold');
    assert.ok(copy);
    warmCopies.add(copy);
    for (let index = start; index < Math.min(start + 50, selected.length); index++) {
      const result = await query(name, 'cold', '?key=fresh-' + index);
      assert.equal(result.copy, copy, 'new SQLite object used the existing copy of its class');
      samples.push(result.elapsed);
    }
  }
  await record('cold-warm-copy', { ...times(samples), seededCopies: warmCopies.size, after: await hostSample() });
}

async function runWarm() {
  await query(workload, 'createLarge');
  await query(workload, 'seedLarge');
  for (const operation of ['noop', 'emptyBatch', 'read', 'insert', 'large']) {
    const run = () => operation === 'insert' ? insert(workload) : query(workload, operation);
    for (let i = 0; i < (smoke ? 2 : 32); ++i) await run();
    const before = await hostSample();
    const start = await cpuSample();
    const samples = [];
    let bytes;
    for (let i = 0; i < settings.warmSamples; ++i) {
      const result = await run();
      samples.push(result.elapsed);
      bytes = result.bytes;
    }
    const finish = await cpuSample();
    assert.equal(finish.start, start.start, 'same child through CPU window');
    assert.ok(finish.ticks >= start.ticks);
    await record('warm-' + operation, { ...times(samples), responseBytes: bytes, before, after: await hostSample(),
      cpu: { hz, ticks: finish.ticks - start.ticks, msPerQuery: (finish.ticks - start.ticks) * 1000 / hz / samples.length } });
  }
}

async function runThroughput() {
  for (const operation of ['read', 'insert']) {
    for (const clients of [1, 8]) {
      const run = () => operation === 'insert' ? insert(workload) : query(workload, operation);
      await Promise.all(Array.from({ length: clients }, run));
      const before = await hostSample();
      const start = performance.now();
      let n = 0;
      await Promise.all(Array.from({ length: clients }, async () => {
        while (performance.now() - start < settings.throughputMs) {
          await run();
          ++n;
        }
      }));
      const elapsedMs = performance.now() - start;
      await record('throughput-' + operation, { clients, n, elapsedMs, perSecond: n * 1000 / elapsedMs,
        before, after: await hostSample() });
    }
  }
  assert.deepEqual((await query(workload, 'count')).body, [{ value: nextId }], 'all acknowledged inserts are present');
  const { entry, log } = await durable();
  assert.equal(entry.pid, child.pid);
  assert.equal(entry.generation, child.generation);
  assert.equal(log.peers.want, 0);
  assert.equal(log.confirm.failures, 0);
  assert.equal(log.confirm.unconfirmed, 0);
  assert.ok(log.confirm.confirmations >= nextId);
  await record('confirmation', { acknowledgedRows: nextId, confirm: log.confirm });
}

async function memorySample() {
  const output = await execOwner("awk '/^(Rss|Pss):/ {print $2}' \"/proc/$1/smaps_rollup\"", child.pid);
  const [rssKiB, pssKiB] = output.split('\n').map(Number);
  assert.ok(rssKiB >= pssKiB && pssKiB > 0);
  return { rssKiB, pssKiB };
}

async function assertResident(n) {
  const { entry } = await durable();
  assert.equal(entry.pid, child.pid);
  assert.equal(entry.generation, child.generation);
  assert.equal(entry.inFlight, 0, 'durable child is idle');
  const state = entry.lifecycle.durableAdmission;
  assert.equal(state.residentRoots, n, 'live roots at memory sample');
  assert.equal(state[native ? 'databaseHomes' : 'homes'], n, 'one home per populated database');
  assert.equal(state.homeSessions, 0);
  return state;
}

async function seed(names) {
  await parallel(names, settings.densityClients, async name => {
    await query(name, 'create');
    await insert(name, 1);
  });
}

async function rotateChild(kernelHash) {
  const previous = child;
  const { requests: [rotation] } = json(await request(cluster.urls[owner],
    '/__coworkerd/rotate?process=' + previous.processId, { method: 'POST', token: cluster.management }), 200, 'rotate child');
  await eventually('durable child rotated', async () => {
    const { requests: [status] } = json(await request(cluster.urls[owner],
      '/__coworkerd/rotation?requests=' + rotation.requestId, { token: cluster.management }), 200, 'rotation state');
    assert.equal(status.status, 'completed');
    const { entry } = await durable();
    assert.ok(entry.generation > previous.generation && entry.pid !== previous.pid);
  }, { timeout: 45000 });
  await identify(kernelHash);
}

async function runDensity(kernelHash) {
  console.log('Seeding small tables before rotating the density child');
  await seed(databases);
  await rotateChild(kernelHash);
  const results = [];
  for (const n of settings.density) {
    const before = await hostSample();
    const start = performance.now();
    await parallel(databases.slice(0, n), settings.densityClients, name => query(name, 'read'));
    const openMs = performance.now() - start;
    await delay(600);
    const residencyBefore = await assertResident(n);
    const samples = [];
    for (let index = 0; index < 5; index++) {
      samples.push(await memorySample());
      if (index < 4) await delay(150);
    }
    const residencyAfter = await assertResident(n);
    const median = key => samples.map(sample => sample[key]).sort((a, b) => a - b)[2];
    const result = { n, openMs, samples, rssKiB: median('rssKiB'), pssKiB: median('pssKiB'),
      residencyBefore, residencyAfter };
    results.push(result);
    await record('memory', { ...result, before, after: await hostSample() });
  }
  const [zero, middle, high] = results;
  const slope = key => ({
    zeroTo100: (middle[key] - zero[key]) / middle.n,
    hundredTo500: (high[key] - middle[key]) / (high.n - middle.n),
    zeroTo500: (high[key] - zero[key]) / high.n,
  });
  await record('memory-slope', { counts: results.map(result => result.n), rssKiB: slope('rssKiB'), pssKiB: slope('pssKiB') });
}

async function runDefaultAdmission(kernelHash) {
  // Prepare confirmed pages and shard ownership in groups below the observed default
  // admission ceiling. Rotate between groups so seeding never needs all homes at once.
  // The measured reopen wave then fits within the kernel's ten-second idle window.
  const groupSize = Math.ceil(databases.length / 2);
  for (let offset = 0; offset < databases.length; offset += groupSize) {
    await seed(databases.slice(offset, offset + groupSize));
    await rotateChild(kernelHash);
  }
  await assertResident(0);
  const before = await hostSample();
  const start = performance.now();
  let tried = 0, opened = 0;
  const refused = [];
  for (const goal of [100, settings.homes - 1, databases.length]) {
    while (tried < goal && refused.length === 0) {
      const batch = databases.slice(tried, Math.min(tried + settings.densityClients, goal));
      tried += batch.length;
      await Promise.all(batch.map(async name => {
        try {
          await query(name, 'read');
          ++opened;
        } catch (error) {
          if (error.httpStatus !== 503 || !/OVERLOADED|DO_UNAVAILABLE/.test(error.message)) throw error;
          refused.push(error.message);
        }
      }));
    }
    await delay(200);
    const { entry } = await durable();
    assert.equal(entry.pid, child.pid);
    assert.equal(entry.generation, child.generation);
    const residency = entry.lifecycle.durableAdmission;
    assert.equal(residency.homeSessions, 0);
    const openMs = performance.now() - start;
    await record('default-admission', { tried, opened, refused, openMs, residency, ...await memorySample() });
    if (refused.length) break;
  }
  await record('default-admission-resources', { before, after: await hostSample() });
}

try {
  const kernelHash = await provenance();
  await cluster.start();
  await setup();
  await identify(kernelHash);
  if (defaultAdmission) {
    await runDefaultAdmission(kernelHash);
  } else {
    hz = Number(await execOwner('getconf CLK_TCK'));
    assert.ok(hz > 0);
    await validateConfirmation();
    await runCold();
    await runWarm();
    await runThroughput();
    await runDensity(kernelHash);
  }
  await record('complete', { project: cluster.project });
} catch (error) {
  await record('failure', { message: error.message });
  if (cluster.started) {
    try {
      console.error(await execOwner('tail -n 40 /var/lib/coworkerd/logs/workerd.log'));
    } catch (diagnosticError) {
      console.error(diagnosticError.message);
    }
  }
  await cluster.diagnostics();
  throw error;
} finally {
  await cluster.close();
}

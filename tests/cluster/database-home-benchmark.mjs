import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { arch, cpus, freemem, loadavg, platform, release, tmpdir, totalmem } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [side, checkoutArg, kernelArg, outputArg, mode, ...extra] = process.argv.slice(2);
const idleAdmission = mode === '--idle-window-admission';
const defaultIdleWindow = mode === '--idle-window-default';
const patchedIdleWindow = mode === '--idle-window-patched';
const idleWindow = mode === '--idle-window' || defaultIdleWindow || patchedIdleWindow || idleAdmission;
const modes = ['--smoke', '--default-admission', '--cold-follow-up', '--throughput', '--throughput-smoke',
  '--idle-window', '--idle-window-default', '--idle-window-patched', '--idle-window-admission'];
if (!['object', 'native'].includes(side) || !checkoutArg || !kernelArg || !outputArg ||
    (mode !== undefined && !modes.includes(mode)) || (idleWindow && side !== 'object') || extra.length) {
  console.error('Usage: node tests/cluster/database-home-benchmark.mjs <object|native> <product-checkout> <kernel-directory> <new-report.jsonl> [' + modes.join('|') + ']');
  process.exit(2);
}
const throughput = mode === '--throughput' || mode === '--throughput-smoke';
const smoke = mode === '--smoke' || mode === '--throughput-smoke';
const clientImage = 'node:22-trixie@sha256:f92a4668ece057eff6b83ed531b5f6277892b5c8fa8c1df4ea4ef470bf92b91e';
const throughputSettings = { warmMs: smoke ? 200 : 1500, measureMs: smoke ? 500 : 10000 };
const throughputMailbox = { events: 64, bytes: 1024 * 1024 };
const throughputPlacement = { rootLimit: 512, nestedLimit: 1024 };
const throughputInFlightLimit = 512;
const defaultAdmission = mode === '--default-admission';
const coldFollowUp = mode === '--cold-follow-up';
const native = side === 'native';
const useD1Binding = native || idleWindow;
const checkout = resolve(checkoutArg);
const kernel = resolve(kernelArg);
const output = resolve(outputArg);
const binaries = resolve(process.env.COWORKERD_CLUSTER_BIN_DIR ?? join(checkout, 'build'));
// The test-only database mapping is part of the product checkout being measured.
const BenchmarkCluster = idleWindow
  ? (await import(pathToFileURL(join(checkout, 'tests/cluster/cluster.mjs')).href)).Cluster
  : Cluster;
const settings = {
  homes: idleWindow && !idleAdmission ? 80 : smoke || throughput ? 16 : 520,
  density: idleWindow ? [0, 1, 32, 64] : smoke ? [0, 3, 8] : [0, 100, 500],
  coldSamples: smoke ? 6 : 200,
  warmSamples: smoke ? 20 : 1000,
  pacedColdDelayMs: 100,
  throughputMs: smoke ? 500 : 10000,
  densityClients: idleWindow ? 8 : smoke ? 4 : 24,
};
const reopenRounds = defaultIdleWindow ? 1 : 4;
// Shorten the copy's post-retirement grace period for the benchmark.
const profileCopyIdleMs = mode === '--idle-window' ? 12000 : 300000;
const admissionDatabases = 1000;
const admissionSpacingMs = 50;
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error('Interrupted by ' + signal)));
}

await writeFile(output, '', { flag: 'wx' });
const directory = await mkdtemp(join(tmpdir(), 'coworkerd-database-home-'));
process.env.COWORKERD_CLUSTER_BIN_DIR = binaries;
const cluster = new BenchmarkCluster(directory, kernel, controller.signal, {
  singleNode: true,
  productRoot: checkout,
  databaseFixtureNode: idleWindow ? 'node-1' : undefined,
  configureNode: () => ({
    quarantineProcesses: 1,
    // A completed HTTP reply can precede retirement of its invocation.
    ...(throughput && { placement: throughputPlacement }),
    ...(mode === '--idle-window' && { idleEvictMs: profileCopyIdleMs, placement: { idleMs: 1000 } }),
    // The admission share derived from a small VM can refuse fewer than 500 copies.
    // Raise the planning budget while keeping an explicit, equal process cap.
    ...(!defaultAdmission && !idleWindow && { memoryBudgetBytes: 16 * 1024 ** 3 }),
    processPool: { count: 1, inFlightLimit: throughput ? throughputInFlightLimit : 128,
      ...(!defaultAdmission && !idleWindow && { threadedMemoryBytes: 3 * 1024 ** 3 }) },
    durableObjects: { processes: 1, threadedStorage: true, peers: 0 },
  }),
  nodeEnvironment: () => ({
    COWORKERD_DO_TEST_HOME_COUNT: String(settings.homes),
    // Keep both mailbox limits above the offered concurrency and its routing bytes.
    COWORKERD_DO_TEST_MAILBOX_EVENTS: String(throughput ? throughputMailbox.events : 32),
    ...(throughput && { COWORKERD_DO_TEST_MAILBOX_BYTES: String(throughputMailbox.bytes) }),
  }),
});
const [owner] = cluster.nodes;
if (idleWindow) assert.equal(cluster.databaseFixtureNode, owner, 'supply a database placement checkout');
const tenantName = 'database-benchmark';
const workload = 'traffic';
const databaseCount = idleAdmission ? admissionDatabases
  : throughput ? 8 : defaultAdmission ? settings.homes + 1 : settings.density.at(-1);
const databases = Array.from({ length: databaseCount },
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
  'docker/storage-entrypoint.sh', 'runtime/runtime.js', ...(idleWindow ? ['runtime/sql-database.js'] : [])];

async function provenance() {
  const commit = await cluster.command('git', ['rev-parse', 'HEAD'], { cwd: checkout });
  const pins = await jsonFile(join(checkout, 'patches/workerd/artifacts.json'));
  const metadata = await jsonFile(join(kernel, 'kernel.json'));
  const pinned = metadata.patchSeriesSha256 === pins.patchSeriesSha256 && metadata.upstream === pins.upstream;
  if (idleWindow) {
    // A placement build may be profiled before its release pin lands. Match the actual patch bytes.
    const series = await cluster.command('python3', ['-B', '-c',
      'import json, sys; sys.path.insert(0, "scripts"); from kernel_artifacts import series_identity; print(json.dumps(series_identity()))'],
    { cwd: checkout });
    const [identity, names] = JSON.parse(series);
    for (const [key, value] of Object.entries(identity)) assert.equal(metadata[key], value, key);
    assert.deepEqual(metadata.patchSeries, names);
  } else {
    assert.ok(pinned, 'the kernel matches the checkout pin');
  }
  const archiveSha256 = pinned ? pins.archives[metadata.architecture] : null;
  if (pinned) assert.ok(archiveSha256, 'checkout has a pinned release for ' + metadata.architecture);
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
  const executables = ['coworkerd', 'coworkerd-apiserver', 'coworkerd-launcher'];
  if (idleWindow) executables.push('coworkerd-platform-test');
  for (const file of executables) {
    hashes[file] = sha256(await readFile(join(binaries, file)));
  }
  await record('provenance', {
    commit, archive: pinned ? pins.releaseTag : null, architecture: metadata.architecture, archiveSha256,
    hashes, productHashes, kernelBuildCommit: metadata.coworkerdCommit, kernelRun: metadata.githubActions?.url,
    ...(idleWindow && { kernelPatchSeries: metadata.patchSeriesSha256, workerdTree: metadata.workerdTree,
      reopenRounds, profileCopyIdleMs }),
    settings: throughput ? { homes: settings.homes, mailbox: throughputMailbox,
      placement: throughputPlacement, inFlightLimit: throughputInFlightLimit, ...throughputSettings } : settings,
    mode: mode ?? 'full', node: process.version,
    ...(throughput && { clientImage,
      runner: process.env.GITHUB_ACTIONS === 'true' ? {
        runId: process.env.GITHUB_RUN_ID, attempt: process.env.GITHUB_RUN_ATTEMPT,
        image: process.env.ImageOS, imageVersion: process.env.ImageVersion,
        instance: process.env.RUNNER_NAME,
      } : null }),
    clientHost: { platform: platform(), arch: arch(), release: release(), model: cpus()[0].model,
      cpus: cpus().length, memoryBytes: totalmem() },
    docker: await cluster.command('docker', ['info', '--format',
      '{{.ServerVersion}} {{.OSType}}/{{.Architecture}} {{.NCPU}} CPU {{.MemTotal}} B']),
  });
  return hashes['coworkerd-threaded'];
}

async function workerSource() {
  const runtime = await readFile(join(checkout, 'runtime/runtime.js'), 'utf8');
  const shared = await readFile(join(checkout, 'runtime/sql-database.js'), 'utf8').catch(error => {
    if (error.code === 'ENOENT') return null;
    throw error;
  });
  const source = shared ?? runtime;
  const start = source.indexOf('export class SqlDatabase extends DurableObject');
  const end = shared ? source.length : source.indexOf('export class BoundWorker', start);
  assert.ok(start >= 0 && end > start, 'locate the pinned SqlDatabase class');
  const alarmHeader = source.match(/^(?:export )?(const DO_ALARM_HEADER = .*;)$/m)?.[1];
  assert.ok(alarmHeader);
  // The platform class can use the internal prepare API. A deployed class has the public exec API.
  const database = source.slice(start, end)
    .replace('const counters = sqlite.prepare(', 'const counters = () => sqlite.exec(')
    .replace('sqlite.prepare(sql)(...bindings)', 'sqlite.exec(sql, ...bindings)')
    .replace('this.ctx.storage.sql.abortExecutionLimit()', 'this.ctx.storage.sql.abortExecutionLimit?.()');
  assert.ok(!database.includes('sqlite.prepare('), 'adapt all platform-only prepare calls');
  const fixture = await readFile(new URL('./database-home-worker.js', import.meta.url), 'utf8');
  return 'import { DurableObject } from "cloudflare:workers";\n' + alarmHeader + '\n' +
    'const useD1Binding = ' + useD1Binding + ';\n' + database + fixture;
}

async function setup() {
  const targets = idleWindow ? databases : [workload, ...databases];
  const tenant = await cluster.tenant(tenantName, { workers: targets.length,
    durableObjectNamespaces: targets.length + Number(throughput) });
  if (useD1Binding) {
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
    const durableObjects = useD1Binding
      ? [{ binding: 'DB', className: '__coworkerdSqlDatabase', database: name }]
      : [{ binding: 'DB', className: 'BenchmarkDatabase' }];
    const classes = useD1Binding ? [] : ['BenchmarkDatabase'];
    if (throughput && name === workload) {
      durableObjects.push({ binding: 'NOOP', className: 'BenchmarkNoop' });
      classes.push('BenchmarkNoop');
    }
    workers.push({ workerId: name, activeVersion: 'v1', versions: [{ versionId: 'v1', format: 'v2',
      compatibilityDate: '2026-10-01', mainModule: 'index.js', contentHash, durableObjects,
      ...(classes.length > 0 && { classes: { declared: classes } }) }] });
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
  if (idleWindow) assert.equal(processPool.idleEvictMs, profileCopyIdleMs);
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

async function recoveryLaps(since, expected) {
  const output = await cluster.compose(['logs', '--no-color', '--since', since, owner], { timeout: 30000 });
  const prefix = '[do-log] {"event":"rebuilt"';
  const rebuilds = [];
  for (const line of output.split('\n')) {
    const start = line.indexOf(prefix);
    if (start >= 0) rebuilds.push(JSON.parse(line.slice(start + '[do-log] '.length)));
  }
  assert.ok(rebuilds.length > 0 && rebuilds.length <= expected, 'rebuild telemetry belongs to this cold window');
  return {
    rebuild: times(rebuilds.map(event => event.ms)),
    tailWait: times(rebuilds.map(event => event.laps.tailWaitMs)),
  };
}

async function runCold({ paced = false, offset = 0 } = {}) {
  const selected = databases.slice(offset, offset + settings.coldSamples);
  const paceMs = paced ? settings.pacedColdDelayMs : 0;
  const pace = () => delay(paceMs, undefined, { signal: controller.signal });
  await parallel(selected, 16, name => query(name, 'ping'));
  const before = await hostSample();
  const recoveryStart = () => coldFollowUp ? execOwner('date -u +%Y-%m-%dT%H:%M:%S.%NZ') : undefined;
  let since = await recoveryStart();
  const samples = [], copies = new Set();
  for (const name of selected) {
    const { elapsed, copy } = await query(name, 'cold');
    samples.push(elapsed);
    if (!native) {
      assert.ok(copy && !copies.has(copy), 'each cold object uses a distinct isolate copy');
      copies.add(copy);
    }
    if (paced) await pace();
  }
  await record('cold-new', { ...times(samples), distinctCopyIds: copies.size, paceMs, before, after: await hostSample(),
    ...(coldFollowUp && { recovery: await recoveryLaps(since, samples.length) }) });
  if (native) return;

  // One copy admits up to 64 trees by default. Seed a matching copy for each group of 50 opens.
  samples.length = 0;
  const warmBefore = await hostSample();
  since = await recoveryStart();
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
      if (paced) await pace();
    }
  }
  await record('cold-warm-copy', { ...times(samples), seededCopies: warmCopies.size, paceMs,
    before: warmBefore, after: await hostSample(),
    ...(coldFollowUp && { recovery: await recoveryLaps(since, samples.length) }) });
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

async function runThroughputCell(operation, clients, metric = 'throughput-' + operation) {
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
  await record(metric, { clients, n, elapsedMs, perSecond: n * 1000 / elapsedMs, before, after: await hostSample() });
}

async function runThroughput() {
  for (const operation of ['read', 'insert']) {
    for (const clients of [1, 8]) await runThroughputCell(operation, clients);
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

function throughputProcesses(state) {
  const snapshot = role => {
    const entries = state.processes.entries.filter(entry => entry.processClass === role);
    assert.equal(entries.length, 1, 'one ' + role + ' process on the node');
    const { pid, processId, generation } = entries[0];
    assert.ok(pid > 0);
    return { pid, processId, generation };
  };
  assert.ok(state.server.pid > 0);
  return { node: { pid: state.server.pid }, worker: snapshot('ordinary'), durable: snapshot('durable') };
}

async function runNetworkThroughput() {
  const container = await cluster.compose(['ps', '-q', owner]);
  assert.match(container, /^[0-9a-f]{12,64}$/);
  const base = 'http://' + owner + ':8930/' + tenantName;
  const source = fileURLToPath(new URL('./database-throughput-client.mjs', import.meta.url));
  await cluster.command('docker', ['pull', clientImage], { timeout: 240000 });
  const nodeCpuMax = await execOwner('if test -r /sys/fs/cgroup/cpu.max; then cat /sys/fs/cgroup/cpu.max; fi');
  await seed([workload, ...databases]);
  const rows = new Map([workload, ...databases].map(name => [name, 1]));
  const cells = [];
  for (const operation of ['worker-only', 'object-noop', 'read', 'insert']) {
    for (const clients of [1, 8, 32]) cells.push({ operation, clients, targets: [workload] });
    if (operation === 'read' || operation === 'insert') {
      for (const clients of [8, 32]) cells.push({ operation, clients, targets: databases });
    }
  }

  const processes = throughputProcesses(await cluster.state(owner));
  assert.deepEqual(processes.durable, child);
  const pids = Object.fromEntries(Object.entries(processes).map(([role, entry]) => [role, entry.pid]));
  await record('throughput-processes', { processes, nodeCpuMax: nodeCpuMax || null });
  for (const { operation, clients, targets } of cells) {
    const ids = targets.map(name => rows.get(name));
    const args = JSON.stringify({ operation, clients, targets, ids, ...throughputSettings, pids });
    const writes = operation === 'insert';
    const before = await cluster.state(owner);
    assert.deepEqual(throughputProcesses(before), processes);
    const result = JSON.parse(await cluster.command('docker', [
      'run', '--rm', '--network', cluster.project + '_frontend', '--pid', 'container:' + container,
      '--mount', 'type=bind,src=' + source + ',dst=/client.mjs,readonly',
      clientImage, 'node', '/client.mjs', base, args,
    ], { timeout: throughputSettings.measureMs + throughputSettings.warmMs + 45000 }));
    const after = await cluster.state(owner);
    await record('throughput-window', { operation, ...result,
      ...(writes && { confirm: { before: before.durableLog.confirm, after: after.durableLog.confirm } }) });
    assert.deepEqual(throughputProcesses(after), processes, 'same server and children throughout the window');
    assert.equal(result.measurement.failures, 0, 'all timed requests succeeded');
    if (writes) assert.equal(result.warmup.failures, 0, 'all warm-up inserts succeeded');
    assert.ok(result.n > 0);
    if (writes) {
      let acknowledged = 0;
      for (const [index, name] of targets.entries()) {
        const inserted = result.warmup.completed[index] + result.measurement.completed[index];
        acknowledged += inserted;
        rows.set(name, rows.get(name) + inserted);
        assert.deepEqual((await query(name, 'count')).body, [{ value: rows.get(name) }],
          'every acknowledged row is present in ' + name);
      }
      assert.equal(after.durableLog.peers.want, 0);
      assert.equal(after.durableLog.confirm.failures, 0);
      assert.equal(after.durableLog.confirm.unconfirmed, 0);
      const confirmations = after.durableLog.confirm.confirmations - before.durableLog.confirm.confirmations;
      assert.ok(confirmations >= acknowledged, 'local WAL confirmed every acknowledged insert');
      await record('throughput-confirmation', { clients, targets, acknowledged, confirmations });
    }
  }
}

async function memorySample() {
  const output = await execOwner("awk '/^(Rss|Pss):/ {print $2}' \"/proc/$1/smaps_rollup\"", child.pid);
  const [rssKiB, pssKiB] = output.split('\n').map(Number);
  assert.ok(rssKiB >= pssKiB && pssKiB > 0);
  return { rssKiB, pssKiB };
}

async function assertResident(n, homes = n) {
  const { entry } = await durable();
  assert.equal(entry.pid, child.pid);
  assert.equal(entry.generation, child.generation);
  assert.equal(entry.inFlight, 0, 'durable child is idle');
  const state = entry.lifecycle.durableAdmission;
  assert.equal(state.residentRoots, n, 'live roots at memory sample');
  assert.equal(state[native ? 'databaseHomes' : 'homes'], homes, 'expected number of occupied homes');
  assert.equal(state.homeSessions, 0);
  if (idleWindow) {
    assert.equal(state.resettingHomes, 0);
    assert.equal(state.pressureRetirements, 0);
  }
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

async function memoryWindow(n, homes = n) {
  const residencyBefore = await assertResident(n, homes);
  const samples = [];
  for (let index = 0; index < 5; index++) {
    samples.push(await memorySample());
    if (index < 4) await delay(150, undefined, { signal: controller.signal });
  }
  const residencyAfter = await assertResident(n, homes);
  const median = key => samples.map(sample => sample[key]).sort((a, b) => a - b)[2];
  return { n, samples, rssKiB: median('rssKiB'), pssKiB: median('pssKiB'),
    residencyBefore, residencyAfter };
}

async function runDensity(kernelHash) {
  console.log('Seeding small tables before rotating the density child');
  const confirmationsBeforeSeed = idleWindow ? (await durable()).log.confirm.confirmations : 0;
  await seed(databases);
  if (idleWindow) {
    const { log: afterSeed } = await durable();
    assert.equal(afterSeed.peers.want, 0);
    assert.equal(afterSeed.confirm.failures, 0);
    assert.equal(afterSeed.confirm.unconfirmed, 0);
    const confirmations = afterSeed.confirm.confirmations - confirmationsBeforeSeed;
    assert.ok(confirmations >= databases.length, 'local WAL confirmed the seed writes');
    await record('idle-window-seed', { rows: databases.length, confirmations });
  }
  await rotateChild(kernelHash);
  const results = [];
  let lastReadCompleted;
  let previousCount = 0;
  for (const n of settings.density) {
    const before = await hostSample();
    const start = performance.now();
    await parallel(databases.slice(idleWindow ? previousCount : 0, n), settings.densityClients,
      name => query(name, 'read'));
    lastReadCompleted = performance.now();
    previousCount = n;
    await delay(600, undefined, { signal: controller.signal });
    const result = { ...await memoryWindow(n), openMs: lastReadCompleted - start };
    results.push(result);
    await record('memory', { ...result, before, after: await hostSample() });
  }
  if (idleWindow) {
    const zero = results[0];
    const high = results.at(-1);
    await record('idle-window-memory-slope', { counts: results.map(result => result.n),
      rssKiB: (high.rssKiB - zero.rssKiB) / high.n, pssKiB: (high.pssKiB - zero.pssKiB) / high.n });
    return lastReadCompleted;
  }
  const [zero, middle, high] = results;
  const slope = key => ({
    zeroTo100: (middle[key] - zero[key]) / middle.n,
    hundredTo500: (high[key] - middle[key]) / (high.n - middle.n),
    zeroTo500: (high[key] - zero[key]) / high.n,
  });
  await record('memory-slope', { counts: results.map(result => result.n), rssKiB: slope('rssKiB'), pssKiB: slope('pssKiB') });
}

async function waitForIdleRelease(lastReadCompleted, retiredWaves) {
  if (defaultIdleWindow) await delay(305000, undefined, { signal: controller.signal });
  let lastNonzeroMs;
  const allClosedMs = await eventually('all database roots and homes retire', async () => {
    controller.signal.throwIfAborted();
    const { entry } = await durable();
    assert.equal(entry.pid, child.pid);
    assert.equal(entry.generation, child.generation);
    const afterLastReadMs = performance.now() - lastReadCompleted;
    const { residentRoots, homes, homeSessions, resettingHomes, idleRetirements, pressureRetirements } =
      entry.lifecycle.durableAdmission;
    assert.equal(resettingHomes, 0);
    assert.equal(pressureRetirements, 0);
    if (residentRoots !== 0 || homes !== 0) lastNonzeroMs = afterLastReadMs;
    assert.equal(residentRoots + homes + homeSessions + entry.inFlight, 0);
    assert.equal(idleRetirements, retiredWaves * databases.length, 'every tree retired through the idle path');
    return afterLastReadMs;
  }, { timeout: patchedIdleWindow ? 15000 : 30000, interval: 200 });
  assert.ok(lastNonzeroMs > 0, 'observed residents before idle release');
  await record('idle-window-retirement', { retiredWaves, lastNonzeroMs, allClosedMs });
}

async function measureReopen(round, homes, samples) {
  // Keep each calling Worker warm while leaving the database's tree closed.
  await parallel(databases, settings.densityClients, name => query(name, 'ping'));
  await assertResident(0, homes);
  const before = await hostSample();
  const { log: start } = await durable();
  for (const name of databases) {
    const first = await query(name, 'read');
    const next = await query(name, 'read');
    samples.reopened.push(first.elapsed);
    samples.warm.push(next.elapsed);
    samples.deltas.push(first.elapsed - next.elapsed);
  }
  const lastReadCompleted = performance.now();
  await eventually('all reopened databases are served on distinct homes', () => assertResident(databases.length));
  const { log: end } = await durable();
  const rebuilds = end.objects.rebuilds - start.objects.rebuilds;
  const homeOpens = end.threadedHomeIo.openSamples - start.threadedHomeIo.openSamples;
  assert.equal(rebuilds, 0, 'same storage epoch with no rebuild');
  assert.equal(homeOpens, databases.length, 'one SQLite open per pair');
  await record('idle-window-round', { round, retainedCopies: homes, pairs: databases.length, homeOpens,
    rebuilds, before, after: await hostSample() });
  return lastReadCompleted;
}

async function runIdleReopen(lastReadCompleted) {
  const retainedCopy = { reopened: [], warm: [], deltas: [] };
  const releasedCopy = { reopened: [], warm: [], deltas: [] };
  const wavesPerRound = patchedIdleWindow ? 1 : 2;
  for (let round = 1; round <= reopenRounds; round++) {
    if (!patchedIdleWindow) {
      await eventually('all trees retire with their copies still loaded', () => assertResident(0, databases.length));
      if (round === 1) {
        await record('idle-window-empty-copy-memory', { afterLastReadMs: performance.now() - lastReadCompleted,
          ...await memoryWindow(0, databases.length) });
      }
      lastReadCompleted = await measureReopen(round, databases.length, retainedCopy);
    }
    await waitForIdleRelease(lastReadCompleted, wavesPerRound * round);
    if (round === 1) await record('idle-window-released-memory', await memoryWindow(0));
    lastReadCompleted = await measureReopen(round, 0, releasedCopy);
  }
  if (!defaultIdleWindow) {
    await waitForIdleRelease(lastReadCompleted, wavesPerRound * reopenRounds + 1);
    await record('idle-window-released-memory', await memoryWindow(0));
  }
  const conditions = patchedIdleWindow ? [['released', releasedCopy]]
    : [['retained', retainedCopy], ['released', releasedCopy]];
  for (const [copy, { reopened, warm, deltas }] of conditions) {
    await record('idle-window-reopen', { copy, ...times(reopened) });
    await record('idle-window-warm', { copy, ...times(warm) });
    await record('idle-window-paired-delta', { copy, ...times(deltas) });
  }
}

async function runIdleAdmission() {
  const tenant = await cluster.tenant(tenantName, { durableObjectNamespaces: databases.length });
  let revision = 0;
  for (const name of databases) {
    const created = await cluster.api(tenant.path + '/databases/' + name, { method: 'POST', token: tenant.token });
    revision = Math.max(created.revision, revision);
  }
  assert.ok(revision > 0, 'database creation returns a catalog revision');
  await eventually('owner applies the database catalog', async () => {
    const state = await cluster.state(owner);
    assert.ok(Number(state.storage.appliedTenantsRevision) >= revision);
  });
  await record('idle-admission-setup', { databases: databases.length, revision, admissionSpacingMs,
    guest: await hostSample() });

  const start = performance.now();
  let succeeded = 0;
  let peakRssBytes = 0;
  const { processPool } = await durable();
  const memoryBudgetBytes = processPool.threadedMemoryBytes;
  const failures = [];
  for (const [index, name] of databases.entries()) {
    const tried = index + 1;
    const started = performance.now();
    assert.ok(started - start < profileCopyIdleMs, 'serial scan finishes inside the copy window');
    const reply = await request(cluster.urls.apiserver, tenant.path + '/databases/' + name + '/query', {
      method: 'POST', body: { sql: 'SELECT 1 AS value' }, token: tenant.token,
    });
    if (reply.status === 200) {
      assert.deepEqual(JSON.parse(reply.text).results, [{ value: 1 }]);
      ++succeeded;
    } else {
      failures.push({ tried, status: reply.status, body: reply.text });
    }
    if (tried % 25 === 0 || (failures.length === 1 && failures[0].tried === tried)) {
      const { entry } = await durable();
      assert.equal(entry.pid, child.pid);
      assert.equal(entry.generation, child.generation);
      peakRssBytes = Math.max(peakRssBytes, entry.residentBytes);
      await record('idle-admission-sample', { tried, succeeded, elapsedMs: performance.now() - start,
        rssBytes: entry.residentBytes, memoryBudgetBytes,
        firstFailure: failures[0], residency: entry.lifecycle.durableAdmission });
    }
    // Let children retire earlier trees before the scan reaches the simultaneous-open ceiling.
    const pause = admissionSpacingMs - (performance.now() - started);
    if (pause > 0 && tried < databases.length) await delay(pause, undefined, { signal: controller.signal });
  }
  const elapsedMs = performance.now() - start;
  assert.ok(elapsedMs < profileCopyIdleMs, 'serial scan finishes inside the copy window');
  await record('idle-admission-result', { tried: databases.length, succeeded, elapsedMs, peakRssBytes,
    memoryBudgetBytes, failures, guest: await hostSample() });
  assert.equal(failures.length, 0, 'every idle-admission query succeeded');
  assert.ok(peakRssBytes < memoryBudgetBytes, 'sampled RSS remains below the child memory budget');
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
  if (!idleAdmission) await setup();
  await identify(kernelHash);
  if (idleAdmission) {
    await runIdleAdmission();
  } else if (throughput) {
    await runNetworkThroughput();
  } else if (idleWindow) {
    const lastReadCompleted = await runDensity(kernelHash);
    await runIdleReopen(lastReadCompleted);
  } else if (defaultAdmission) {
    await runDefaultAdmission(kernelHash);
  } else if (coldFollowUp) {
    await validateConfirmation();
    for (let i = 0; i < 32; ++i) await query(workload, 'read');
    await runThroughputCell('read', 1, 'throughput-read-control');
    await runCold();
    await runCold({ paced: true, offset: settings.coldSamples });
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

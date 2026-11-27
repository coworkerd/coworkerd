import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Cluster, eventually } from './cluster.mjs';

const [kernel, output, ...extra] = process.argv.slice(2);
assert.ok(kernel && output && extra.length === 0,
  'Usage: node tests/cluster/long-sql-isolation.mjs <verified-kernel-directory> <new-report.jsonl>');
const root = fileURLToPath(new URL('../../', import.meta.url));
const binaries = resolve(process.env.COWORKERD_CLUSTER_BIN_DIR ?? join(root, 'build'));
const report = resolve(output);
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}

await writeFile(report, '', { flag: 'wx' });
const record = (metric, data) => appendFile(report, JSON.stringify({ metric, ...data }) + '\n');
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-sql-isolation-')), resolve(kernel), controller.signal, {
  singleNode: true, threadedNodes: ['node-1'],
  configureNode: () => ({ durableObjects: { processes: 1, threadedStorage: true, peers: 0 } }),
});
const [owner] = cluster.nodes;
const objectClass = 'IsolationProbe';

async function provisionDatabase(tenant, name) {
  const { database, revision } = await cluster.api(`${tenant.path}/databases/${name}`, {
    method: 'POST', token: tenant.token,
  });
  return { path: `${tenant.path}/databases/${name}/query`, namespace: database.namespaceId.slice(0, 12),
    token: tenant.token, revision, name };
}

async function seed({ path, token, name }) {
  const result = await cluster.api(path, { method: 'POST', token, body: [
    { sql: 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, value TEXT NOT NULL)' },
    { sql: 'INSERT INTO ledger VALUES (1, ?)', params: [name] },
  ] });
  assert.deepEqual(result.map(statement => statement.success), [true, true]);
}

function dispatchesFromLogs(log, roles) {
  const dispatches = log.split('\n').filter(line => line.includes('[do] '))
    .map(line => JSON.parse(line.split('[do] ')[1]));
  const roleFor = entry => entry.className === objectClass ? 'object' : roles[entry.namespaceId];
  return dispatches.filter(entry => roleFor(entry)).map(entry => ({ role: roleFor(entry),
    ...Object.fromEntries(['nodeId', 'className', 'namespaceId', 'objectId', 'processId', 'processGeneration', 'route',
      'status', 'code', 'elapsedMs', 'observedAtUnixMs'].map(key => [key, entry[key]])) }));
}

function verifyDispatches(rows, windows, child) {
  const deadlines = rows.filter(entry => entry.role === 'blocking' && entry.status === 503 && entry.elapsedMs >= 29000);
  assert.equal(deadlines.length, windows.filter(row => row.condition === 'busy').length);
  for (const row of rows) {
    assert.equal(row.processId, child.processId);
    assert.equal(row.processGeneration, child.generation);
    assert.equal(row.nodeId, owner);
    assert.equal(row.route, 'local');
    assert.equal(row.className, row.role === 'object' ? objectClass : '__coworkerdSqlDatabase');
  }
  for (const window of windows) {
    if (window.condition === 'busy') {
      assert.equal(deadlines.filter(row => row.observedAtUnixMs >= window.executionThroughAt &&
        row.observedAtUnixMs <= window.deadline.doneAt + 25).length, 1, 'the owner reported the timed-out dispatch');
    }
    for (const [role, { samples }] of Object.entries(window.probes)) {
      const actual = rows.filter(row => row.role === role && row.status === 200 &&
        row.observedAtUnixMs >= window.firstScheduledAt && row.observedAtUnixMs <= window.lastReplyAt + 25);
      assert.equal(actual.length, samples.length, `${window.condition} ${role} successful owner dispatches`);
      if (window.condition === 'busy') {
        assert.ok(actual.every(row => row.observedAtUnixMs >= window.runningAt &&
          row.observedAtUnixMs <= window.executionThroughAt), `owner-side overlap for ${role}`);
      }
    }
  }
}

function p99(values, failures = 0) {
  // Missed offers and failed requests have no successful completion at any latency.
  const rank = Math.ceil((values.length + failures) * 0.99);
  return values.toSorted((a, b) => a - b)[rank - 1] ?? null;
}

function summarize(probes) {
  const samples = probes.flatMap(probe => probe.samples);
  const errors = probes.reduce((total, probe) => total + probe.errors.length, 0);
  const missed = probes.reduce((total, probe) => total + probe.misses.length, 0);
  return { p99Ms: p99(samples.map(sample => sample.scheduledElapsedMs), errors + missed),
    responseP99Ms: p99(samples.map(sample => sample.elapsedMs)), samples: samples.length, errors, missed };
}

function compare(quiet, busy) {
  const deltaMs = quiet.p99Ms === null || busy.p99Ms === null ? null : busy.p99Ms - quiet.p99Ms;
  return { quiet, busy, deltaMs, pass: deltaMs !== null && deltaMs <= 5 &&
    busy.errors <= quiet.errors && busy.missed <= quiet.missed };
}

function summarizeWindows(windows) {
  return Object.keys(windows[0].probes).map(role => {
    const quiet = summarize(windows.filter(window => window.condition === 'quiet').map(window => window.probes[role]));
    const busy = summarize(windows.filter(window => window.condition === 'busy').map(window => window.probes[role]));
    const adjacent = [];
    for (let i = 1; i < windows.length; i += 2) {
      const running = windows[i];
      assert.equal(running.condition, 'busy');
      for (const baseline of [windows[i - 1], windows[i + 1]].filter(Boolean)) {
        assert.equal(baseline.condition, 'quiet');
        adjacent.push({ quietWindow: baseline.index, busyWindow: running.index,
          ...compare(summarize([baseline.probes[role]]), summarize([running.probes[role]])) });
      }
    }
    return { role, ...compare(quiet, busy), adjacent,
      pass: quiet.samples >= 1000 && busy.samples >= 1000 && adjacent.every(pair => pair.pass) };
  });
}

async function run() {
  const pins = JSON.parse(await readFile(join(root, 'patches/workerd/artifacts.json'), 'utf8'));
  const manifest = JSON.parse(await readFile(join(cluster.kernel, 'kernel.json'), 'utf8'));
  assert.equal(manifest.patchSeriesSha256, pins.patchSeriesSha256);
  assert.equal(manifest.upstreamCommit, pins.upstreamCommit);
  const sha256 = async path => createHash('sha256').update(await readFile(path)).digest('hex');
  const kernelHash = await sha256(join(cluster.kernel, 'coworkerd-threaded'));
  assert.equal(kernelHash, manifest.files['coworkerd-threaded']);
  await record('provenance', {
    commit: await cluster.command('git', ['rev-parse', 'HEAD'], { cwd: root }),
    architecture: manifest.architecture, kernelRelease: pins.releaseTag,
    archiveSha256: pins.archives[manifest.architecture], kernelHash,
    daemonSha256: await sha256(join(binaries, 'coworkerd')),
    apiServerSha256: await sha256(join(binaries, 'coworkerd-apiserver')),
    docker: await cluster.command('docker', ['info', '--format',
      '{{.ServerVersion}} {{.OSType}}/{{.Architecture}} {{.NCPU}} CPU {{.MemTotal}} B'], { cwd: root }),
  });
  await cluster.start();
  const first = await cluster.tenant('isolation-a');
  const second = await cluster.tenant('isolation-b');
  const blocking = await provisionDatabase(first, 'blocking');
  const sameTenant = await provisionDatabase(first, 'same-tenant');
  const otherTenant = await provisionDatabase(second, 'other-tenant');
  await eventually('the threaded owner has applied the three database catalogs', async () => {
    const { storage } = await cluster.state(owner);
    assert.ok(Number(storage.appliedTenantsRevision) >= otherTenant.revision);
  });
  for (const db of [blocking, sameTenant, otherTenant]) await seed(db);
  await cluster.deploy(first, 'object', `
    export class ${objectClass} { fetch() { return new Response('object'); } }
    export default { fetch(request, env) { return env.OBJECT.getByName('probe').fetch(request); } };
  `, { wrangler: {
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: objectClass }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: [objectClass] }],
  } });

  const child = (await cluster.state(owner)).processes.entries.find(entry => entry.processClass === 'durable');
  assert.ok(child?.pid > 0 && child.state === 'ready');
  assert.equal(child.lifecycle.durableServer, 'threaded');
  assert.equal(child.binarySha256, kernelHash);
  const identity = { pid: child.pid, processId: child.processId, generation: child.generation };
  await record('child', { ...identity, workerThreads: child.lifecycle.durableAdmission.workerThreads });
  const container = await cluster.compose(['ps', '-q', owner]);
  const input = join(cluster.directory, 'isolation.json');
  await writeFile(input, JSON.stringify({ child: identity, blocking, sameTenant, otherTenant,
    objectPath: `/${first.name}/object`, management: cluster.management }), { mode: 0o600 });
  const client = fileURLToPath(new URL('./long-sql-isolation-client.mjs', import.meta.url));
  const result = await cluster.command('docker', ['run', '--rm', '--network', `${cluster.project}_frontend`,
    '--pid', `container:${container}`, '-v', `${input}:/isolation.json:ro`, '-v', `${client}:/client.mjs:ro`,
    'node:22-trixie', 'node', '/client.mjs'], { timeout: 280000, maxBuffer: 16 * 1024 * 1024 });
  const windows = result.split('\n').map(line => JSON.parse(line));
  for (const window of windows) await record('window', window);
  const log = await cluster.compose(['logs', '--no-color', owner], { maxBuffer: 64 * 1024 * 1024 });
  const roles = { [blocking.namespace]: 'blocking', [sameTenant.namespace]: 'sameTenant',
    [otherTenant.namespace]: 'otherTenant' };
  const dispatches = dispatchesFromLogs(log, roles);
  await record('owner-dispatches', { dispatches });
  verifyDispatches(dispatches, windows, identity);

  const summary = summarizeWindows(windows);
  await record('summary', { thresholdMs: 5, summary });
  console.table(summary.flatMap(({ role, adjacent }) => adjacent.map(pair => ({ role,
    quietWindow: pair.quietWindow, busyWindow: pair.busyWindow, quietP99Ms: pair.quiet.p99Ms?.toFixed(3),
    busyP99Ms: pair.busy.p99Ms?.toFixed(3), deltaMs: pair.deltaMs?.toFixed(3),
    quietMissed: pair.quiet.missed, busyMissed: pair.busy.missed, pass: pair.pass }))));
  if (summary.some(row => !row.pass)) process.exitCode = 1;
}

try {
  await run();
} finally {
  await cluster.close();
}

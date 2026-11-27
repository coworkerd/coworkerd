import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Cluster, eventually } from './cluster.mjs';

const [kernelDirectory, outputPath, ...extra] = process.argv.slice(2);
if (!kernelDirectory || !outputPath || extra.length) {
  console.error('Usage: node tests/cluster/waiting-call-capacity.mjs <kernel-directory> <new-output.jsonl>');
  process.exit(2);
}

const kernel = resolve(kernelDirectory);
const output = resolve(outputPath);
await writeFile(output, '', { flag: 'wx' });
const record = row => appendFile(output, JSON.stringify(row) + '\n');
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-waiting-calls-')), kernel, controller.signal, {
  singleNode: true,
  configureNode: () => ({
    ingressPlacement: true,
    egress: { defaultPolicy: 'deny', clusterAddresses: ['apiserver:8940'],
      tenants: [{ tenant: 'waiting-capacity', allowInternal: ['waiting-origin:8080'] }] },
  }),
});
const [node] = cluster.nodes;
const tenantName = 'waiting-capacity';
const peer = `${cluster.project}-waiting-origin`;
const peerSource = fileURLToPath(new URL('./waiting-call-peer.mjs', import.meta.url));
const peerImage = 'node:22-trixie';
const recoveryMs = 5000;
let peerStarted = false;

async function control(path, method = 'GET') {
  const text = await cluster.command('docker', ['exec', peer, 'node', '-e', `
    fetch('http://127.0.0.1:8081' + process.argv[1], { method: process.argv[2] }).then(async response => {
      if (!response.ok) throw new Error('fixture control returned HTTP ' + response.status);
      console.log(await response.text());
    }).catch(error => { console.error(error); process.exitCode = 1; });
  `, path, method], { timeout: 10000 });
  return JSON.parse(text);
}

async function processId(service, executable, firstArgument) {
  const pid = await cluster.compose(['exec', '-T', service, 'sh', '-ec', String.raw`
    for proc in /proc/[0-9]*; do
      if [ "$(readlink "$proc/exe" 2>/dev/null)" = "$1" ] &&
         [ "$(tr '\000' ' ' < "$proc/cmdline" | cut -d' ' -f2)" = "$2" ]; then
        parent=$(awk '/^PPid:/ {print $2; exit}' "$proc/status")
        if [ "$(readlink "/proc/$parent/exe" 2>/dev/null)" != "$1" ]; then
          basename "$proc"
        fi
      fi
    done
  `, 'find-process', executable, firstArgument]);
  assert.match(pid, /^[1-9]\d*$/, `one ${executable} process in ${service}`);
  return Number(pid);
}

const procSample = `
  for pid do
    proc=/proc/$pid
    rss=$(awk '/^Rss:/ {print $2; exit}' "$proc/smaps_rollup")
    threads=$(awk '/^Threads:/ {print $2; exit}' "$proc/status")
    descriptors=$(ls -1 "$proc/fd")
    fds=$(printf '%s\n' "$descriptors" | wc -l)
    nofile=$(awk '/^Max open files / {print $4, $5; exit}' "$proc/limits")
    printf '%s %s %s %s %s\n' "$pid" "$rss" "$threads" "$fds" "$nofile"
  done
`;

async function readProcesses(command, roles) {
  const result = await command(['sh', '-ec', procSample, 'sample-processes', ...roles.map(({ pid }) => String(pid))]);
  const rows = result.split('\n');
  assert.equal(rows.length, roles.length);
  return roles.map((role, index) => {
    const [pid, rssKiB, threads, fds, nofileSoft, nofileHard] = rows[index].trim().split(/\s+/).map(Number);
    assert.equal(pid, role.pid);
    assert.ok([rssKiB, threads, fds, nofileSoft, nofileHard].every(value => Number.isSafeInteger(value) && value > 0));
    return { ...role, rssKiB, threads, fds, nofileSoft, nofileHard };
  });
}

function productProcesses(state) {
  return [{ role: 'daemon', pid: state.server.pid }, ...state.processes.entries.filter(entry => entry.pid > 0)
    .map(({ processClass: role, pid, processId, generation }) => ({ role, pid, processId, generation }))];
}

function checkOccupancy(state, count) {
  assert.deepEqual(state.placement.inFlight, { roots: count, nested: 0, total: count });
  const [ordinary] = state.processes.entries.filter(entry => entry.processClass === 'ordinary');
  assert.equal(ordinary.inFlight, count);
  assert.equal(state.egress.tenants[tenantName]?.inFlight ?? 0, count);
  assert.equal(state.placement.loadBand, 'green');
}

async function run() {
  await cluster.start();
  await cluster.command('docker', ['run', '--detach', '--rm', '--name', peer, '--network', `${cluster.project}_backend`,
    '--network-alias', 'waiting-origin', '--mount', `type=bind,source=${peerSource},target=/peer.mjs,readonly`,
    peerImage, 'node', '/peer.mjs']);
  peerStarted = true;
  const tenant = await cluster.tenant(tenantName);
  await cluster.deploy(tenant, 'wait', `export default { async fetch(request) {
    const id = new URL(request.url).searchParams.get('call');
    if (id !== 'warm' && !/^[0-8]-[0-9]{1,2}$/.test(id ?? '')) return new Response(null, { status: 404 });
    const path = id === 'warm' ? '/warm' : '/hold/' + id;
    const response = await fetch('http://waiting-origin:8080' + path, { redirect: 'manual' });
    if (!response.ok) return new Response('fixture upstream failed', { status: 502 });
    return Response.json({ nodeId: request.cf.nodeId, upstream: await response.text() });
  } };`);

  const status = () => control('/status');
  const { plan, waitMs, nodeVersion } = await eventually('local upstream ready', status);
  const warm = async () => {
    const reply = await control('/warm', 'POST');
    assert.deepEqual(reply, { status: 200, nodeId: node, upstream: 'warm' });
    return reply;
  };
  await warm();
  const quiet = () => eventually('node and ordinary process drained', async () => {
    const state = await cluster.state(node);
    checkOccupancy(state, 0);
    return state;
  }, { timeout: 3000, interval: 50 });
  const initial = await quiet();
  const identity = productProcesses(initial);
  const [ordinary] = initial.processes.entries.filter(entry => entry.processClass === 'ordinary');
  assert.equal(initial.placement.ingressPlacement, true);
  assert.equal(ordinary.sandbox.ordinaryServer, 'threaded');
  const { egress } = initial;
  const tenantEgress = egress.tenantPolicies.find(policy => policy.tenant === tenantName);
  const egressLimit = tenantEgress.concurrency || egress.concurrency;
  assert.ok(plan.every(count => count < Math.min(initial.placement.rootLimit, egressLimit,
    initial.processes.configuration.inFlightLimit)));
  const fixtures = [
    { service: 'apiserver', role: 'apiserver', executable: '/opt/coworkerd/bin/coworkerd-apiserver', argument: '--config' },
    { service: 'storage', role: 'objectStore', executable: '/usr/bin/weed', argument: 'server' },
  ];
  const support = await Promise.all(fixtures.map(async ({ service, role, executable, argument }) =>
    ({ service, role, pid: await processId(service, executable, argument) })));
  const sample = async (state, count) => {
    assert.deepEqual(productProcesses(state), identity, 'processes rotated during measurement');
    checkOccupancy(state, count);
    const groups = await Promise.all([
      readProcesses(args => cluster.compose(['exec', '-T', node, ...args]), identity),
      readProcesses(args => cluster.command('docker', ['exec', peer, ...args]), [{ role: 'upstreamAndClient', pid: 1 }]),
      ...support.map(({ service, role, pid }) => readProcesses(
        args => cluster.compose(['exec', '-T', service, ...args]), [{ role, pid }])),
    ]);
    const entry = state.processes.entries.find(entry => entry.processClass === 'ordinary');
    return { atMs: (await status()).atMs, inFlight: state.placement.inFlight, ordinaryInFlight: entry.inFlight,
      loadBand: state.placement.loadBand, placementRejections: state.placement.rejections,
      egress: state.egress.tenants[tenantName], processes: groups.flat() };
  };

  const projectRoot = fileURLToPath(new URL('../../', import.meta.url));
  const binaries = resolve(process.env.COWORKERD_CLUSTER_BIN_DIR ?? join(projectRoot, 'build'));
  const files = { daemon: join(binaries, 'coworkerd'), apiserver: join(binaries, 'coworkerd-apiserver'),
    launcher: join(binaries, 'coworkerd-launcher'), workerd: join(kernel, 'workerd'), threaded: join(kernel, 'coworkerd-threaded') };
  const hashes = Object.fromEntries(await Promise.all(Object.entries(files).map(async ([role, path]) =>
    [role, createHash('sha256').update(await readFile(path)).digest('hex')])));
  assert.equal(ordinary.binarySha256, hashes.threaded);
  const [productCommit, engine, image] = await Promise.all([
    cluster.command('git', ['rev-parse', 'HEAD'], { cwd: projectRoot }),
    cluster.command('docker', ['info', '--format', '{{json .}}']).then(JSON.parse),
    cluster.command('docker', ['inspect', '--format', '{{.Image}}', peer]),
  ]);
  const kernelRecord = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
  const pins = JSON.parse(await readFile(join(projectRoot, 'patches/workerd/artifacts.json'), 'utf8'));
  const settleMs = initial.processes.configuration.threadedThreadIdleMs + 1000;
  await record({ type: 'provenance', at: new Date().toISOString(), productCommit, hashes, kernel: kernelRecord,
    ...(pins.patchSeriesSha256 === kernelRecord.patchSeriesSha256 && {
      publishedPin: { releaseTag: pins.releaseTag, sha256: pins.archives[kernelRecord.architecture] },
    }),
    linux: { architecture: engine.Architecture, release: engine.KernelVersion, cpus: engine.NCPU, memoryBytes: engine.MemTotal,
      os: engine.OperatingSystem, dockerVersion: engine.ServerVersion },
    fixture: { image, nodeVersion, plan, waitMs, recoveryMs, settleMs },
    placement: { ingressPlacement: initial.placement.ingressPlacement, rootLimit: initial.placement.rootLimit,
      nestedLimit: initial.placement.nestedLimit, tenantRootLimit: initial.placement.tenantAdmission.rootLimit,
      tenantNestedLimit: initial.placement.tenantAdmission.nestedLimit },
    egress: { concurrency: egressLimit, requestsPerSecond: tenantEgress.requestsPerSecond || egress.requestsPerSecond,
      timeoutMs: egress.timeoutMs, defaultPolicy: egress.defaultPolicy },
    pool: initial.processes.configuration, sandbox: ordinary.sandbox });

  const startup = await sample(await quiet(), 0);
  // The native process starts its full call-thread pool and retires excess idle threads on a timer.
  console.log(`Waiting ${settleMs} ms for the worker thread pool to settle`);
  await delay(settleMs, undefined, { signal: controller.signal });
  const settled = await sample(await quiet(), 0);
  await record({ type: 'settling', startup, settled, warmReply: await warm() });
  for (const [number, offered] of plan.entries()) {
    const phases = { before: await sample(await quiet(), 0) };
    let overlapStart, overlapEnd;
    assert.deepEqual(await control('/batch', 'POST'), { number, offered });
    try {
      overlapStart = await eventually('every offered request is simultaneously waiting upstream', async () => {
        const observation = await status();
        assert.equal(observation.active, offered);
        return observation;
      }, { timeout: 1500, interval: 50 });
      phases.waiting = await sample(await cluster.state(node), offered);
      overlapEnd = await status();
      assert.equal(overlapEnd.active, offered);
      assert.equal(overlapEnd.completed, 0);
    } finally {
      const result = await eventually('every client request settled', async () => {
        const result = await control('/result');
        assert.equal(result.completed, offered);
        return result;
      }, { timeout: 10000, interval: 50 });
      try {
        phases.after = await sample(await quiet(), 0);
        await delay(recoveryMs, undefined, { signal: controller.signal });
        phases.recovered = await sample(await quiet(), 0);
        phases.recovered.probe = await warm();
        await quiet();
      } finally {
        await record({ type: 'batch', number, offered, overlapStart, overlapEnd, phases, ...result });
      }
      for (const call of result.calls) {
        assert.deepEqual(call.result, { status: 200, nodeId: node, upstream: call.id });
        assert.ok(call.releasedMs - call.enteredMs >= waitMs - 1, 'each request waited for the upstream timer');
      }
      assert.equal(result.entered, offered);
      assert.equal(result.released, offered);
      assert.deepEqual(phases.after.placementRejections, phases.before.placementRejections);
      for (const field of ['denied', 'failed']) assert.deepEqual(phases.after.egress[field], phases.before.egress[field]);
    }
    console.log(`Batch ${number + 1}: ${offered} overlapping upstream requests and ${offered} verified replies`);
  }
}

try {
  await run();
} catch (error) {
  await cluster.diagnostics('waiting-call-capacity', error);
  throw error;
} finally {
  try {
    if (peerStarted) await cluster.command('docker', ['stop', '--time', '2', peer], { cleanup: true });
  } finally {
    await cluster.close();
  }
}

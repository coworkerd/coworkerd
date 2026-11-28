import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { Cluster, eventually, request } from './cluster.mjs';

const { positionals: [kernelDirectory, outputPath, ...extra], values: options } = parseArgs({
  allowPositionals: true,
  options: {
    batches: { type: 'string' },
    'root-limit': { type: 'string' },
    'nested-limit': { type: 'string' },
    'process-limit': { type: 'string' },
    'ordinary-limit': { type: 'string' },
    'ramp-ms': { type: 'string', default: '0' },
    'wait-ms': { type: 'string', default: '2000' },
    'node-memory-gib': { type: 'string', default: '16' },
    'node-tasks': { type: 'string', default: '40000' },
    'node-cpus': { type: 'string' },
    'node-descriptors': { type: 'string' },
    'expect-first-resource': { type: 'boolean' },
    'idle-worker-count': { type: 'string' },
    'expect-full': { type: 'boolean' },
  },
});
assert.ok(kernelDirectory && outputPath && !extra.length,
  'Usage: node tests/cluster/waiting-call-capacity.mjs <kernel-directory> <new-output.jsonl> ' +
  '[--batches 128,512,2000] [--root-limit N --nested-limit N] [--process-limit N] [--ramp-ms N] ' +
  '[--wait-ms N] [--node-memory-gib N] [--node-tasks N] [--node-cpus N] [--node-descriptors N] ' +
  '[--expect-full | --expect-first-resource | --idle-worker-count N --ordinary-limit N]');
const maxCalls = 12000;
const plan = options.batches === undefined ? [8, 16, 24, 16, 24, 8, 24, 8, 16] :
  options.batches.split(',').map(value => integer(value, 1, maxCalls));
assert.ok(plan.length <= 12 && plan.reduce((total, count) => total + count, 0) <= 60000,
  'use at most 12 batches and 60,000 offers in one run');
const waitMs = integer(options['wait-ms'], 2000, 24000);
const rampMs = integer(options['ramp-ms'], 0, waitMs - 500);
const expectFull = options['expect-full'] || options.batches === undefined;
const rootLimit = optionalLimit('root-limit');
const nestedLimit = optionalLimit('nested-limit');
const processLimit = optionalLimit('process-limit');
const ordinaryLimit = options['ordinary-limit'] === undefined ? undefined : integer(options['ordinary-limit'], 1, 1048576);
const idleWorkerCount = options['idle-worker-count'] === undefined ? undefined : integer(options['idle-worker-count'], 1, 128);
assert.ok(idleWorkerCount === undefined || (ordinaryLimit !== undefined && options.batches === undefined),
  'the idle comparison needs an explicit ordinary ceiling and no batches');
assert.ok(!options['expect-first-resource'] ||
    (options.batches !== undefined && !expectFull && rootLimit === undefined && nestedLimit === undefined &&
     processLimit === undefined && ordinaryLimit === undefined && idleWorkerCount === undefined),
  'the first-resource check uses an explicit offer plan and default admission');

assert.ok((rootLimit === undefined && nestedLimit === undefined) ||
  (rootLimit !== undefined && nestedLimit !== undefined && nestedLimit > rootLimit),
  'set both placement limits, with the nested limit greater than the root limit');

function integer(value, min, max) {
  assert.match(value, /^\d+$/);
  const parsed = Number(value);
  assert.ok(parsed >= min && parsed <= max, 'integer ' + value + ' outside ' + min + '–' + max);
  return parsed;
}

function optionalLimit(name) {
  return options[name] === undefined ? undefined : integer(options[name], 1, 16384);
}

const kernel = resolve(kernelDirectory);
const output = resolve(outputPath);
await writeFile(output, '', { flag: 'wx' });
const record = row => appendFile(output, JSON.stringify(row) + '\n');
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error('Interrupted by ' + signal)));
}
const nodeMemory = integer(options['node-memory-gib'], 2, 128) + 'g';
const nodeResources = { mem_limit: nodeMemory, memswap_limit: nodeMemory,
  pids_limit: integer(options['node-tasks'], 256, 250000) };
if (options['node-cpus'] !== undefined) nodeResources.cpus = integer(options['node-cpus'], 1, 128);
if (options['node-descriptors'] !== undefined) {
  const hard = integer(options['node-descriptors'], 1024, 1048576);
  nodeResources.ulimits = { nofile: { soft: Math.min(hard, 1024), hard } };
}
const peerResources = ['--memory', '768m', '--memory-swap', '768m', '--pids-limit', '128',
  '--ulimit', 'nofile=32768:32768', '--cpus', '2'];
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-waiting-calls-')), kernel, controller.signal, {
  singleNode: true,
  nodeComposeResources: nodeResources,
  configureNode: () => ({
    ingressPlacement: true,
    egress: { defaultPolicy: 'deny', clusterAddresses: ['apiserver:8940'],
      tenants: [{ tenant: 'waiting-capacity', allowInternal: ['waiting-origin:8080'] }] },
    ...(rootLimit !== undefined || nestedLimit !== undefined ? { placement: { rootLimit, nestedLimit } } : {}),
    ...(processLimit !== undefined || ordinaryLimit !== undefined ?
      { processPool: { count: 1, inFlightLimit: processLimit, ordinaryInFlightLimit: ordinaryLimit } } : {}),
  }),
});
const [node] = cluster.nodes;
const tenantName = 'waiting-capacity';
const sides = Object.fromEntries(['client', 'upstream'].map(role => [role, cluster.project + '-' + role]));
const peerSource = fileURLToPath(new URL('./waiting-call-peer.mjs', import.meta.url));
const peerImage = 'node:22-trixie';
const recoveryMs = 5000;
const started = [];

async function control(role, path, method = 'GET') {
  const text = await cluster.command('docker', ['exec', sides[role], 'node', '-e', `
    fetch('http://127.0.0.1:8081' + process.argv[1], { method: process.argv[2] }).then(async response => {
      if (!response.ok) throw new Error('fixture control returned HTTP ' + response.status);
      console.log(await response.text());
    }).catch(error => { console.error(error); process.exitCode = 1; });
  `, path, method], { timeout: 10000 });
  return JSON.parse(text);
}

const procSample = `
  for pid do
    proc=/proc/$pid
    rss=$(awk '/^Rss:/ {print $2; exit}' "$proc/smaps_rollup")
    threads=$(awk '/^Threads:/ {print $2; exit}' "$proc/status")
    fds=$(ls -1U "$proc/fd" | wc -l)
    nofile=$(awk '/^Max open files / {print $4, $5; exit}' "$proc/limits")
    nproc=$(awk '/^Max processes / {print $3, $4; exit}' "$proc/limits")
    printf '%s %s %s %s %s %s\\n' "$pid" "$rss" "$threads" "$fds" "$nofile" "$nproc"
  done
  printf '%s\\n' --
  cd /sys/fs/cgroup
  for file in memory.current memory.max memory.peak memory.events pids.current pids.max pids.events cpu.max cpu.stat; do
    if [ -r "$file" ]; then
      printf '%s %s\\n' "$file" "$(tr '\\n' ',' < "$file")"
    fi
  done
  printf 'somaxconn %s\\n' "$(cat /proc/sys/net/core/somaxconn)"
  printf 'ip_local_port_range %s\\n' "$(cat /proc/sys/net/ipv4/ip_local_port_range)"
  printf 'threads_max %s\\n' "$(cat /proc/sys/kernel/threads-max)"
  printf 'pid_max %s\\n' "$(cat /proc/sys/kernel/pid_max)"
  awk '/^TcpExt:/ {
    if (++row == 1) for (i = 2; i <= NF; i++) names[i] = $i
    else for (i = 2; i <= NF; i++) if (names[i] ~ /^Listen(Overflows|Drops)$/) print names[i], $i
  }' /proc/net/netstat
`;

async function readProcesses(command, roles) {
  const result = await command(['sh', '-ec', procSample, 'sample-processes', ...roles.map(({ pid }) => String(pid))]);
  const [processOutput, cgroupOutput] = result.split('\n--\n');
  const rows = processOutput.split('\n');
  assert.equal(rows.length, roles.length);
  const processes = roles.map((role, index) => {
    const [pid, rssKiB, threads, fds, nofileSoft, nofileHard, nprocSoft, nprocHard] = rows[index].trim().split(/\s+/);
    assert.equal(Number(pid), role.pid);
    assert.ok([rssKiB, threads, fds, nofileSoft, nofileHard].every(value => Number(value) > 0));
    return { ...role, rssKiB: Number(rssKiB), threads: Number(threads), fds: Number(fds),
      nofileSoft: Number(nofileSoft), nofileHard: Number(nofileHard), nprocSoft, nprocHard };
  });
  const cgroup = Object.fromEntries(cgroupOutput.split('\n').map(line => {
    const space = line.indexOf(' ');
    return [line.slice(0, space), line.slice(space + 1).trimEnd().replace(/,$/, '')];
  }));
  return { processes, cgroup };
}

function productProcesses(state) {
  return [{ role: 'daemon', pid: state.server.pid }, ...state.processes.entries.filter(entry => entry.pid > 0)
    .map(({ processClass: role, pid, processId, generation }) => ({ role, pid, processId, generation }))];
}

function nodeReading(state) {
  return { inFlight: state.placement.inFlight, loadBand: state.placement.loadBand,
    placementRejections: state.placement.rejections, callResources: state.placement.callResources,
    egress: state.egress.tenants[tenantName],
    processes: state.processes.entries.map(({ processId, processClass, generation, pid, inFlight }) =>
      ({ processId, processClass, generation, pid, inFlight })) };
}

function summarize(calls, waits, waitMs) {
  const intervals = waits.filter(call => call.enteredMs !== undefined);
  const released = intervals.filter(call => call.releasedMs !== undefined);
  const events = intervals.flatMap(({ enteredMs, releasedMs, abortedMs }) =>
    [[enteredMs, 1], [releasedMs ?? abortedMs, -1]]);
  assert.ok(events.every(([time]) => Number.isFinite(time)), 'unsettled upstream interval');
  events.sort(([a, stepA], [b, stepB]) => a - b || stepA - stepB);
  let active = 0, peak = 0, peakOverlapMs = 0;
  for (const [index, [at, step]] of events.entries()) {
    active += step;
    const duration = (events[index + 1]?.[0] ?? at) - at;
    if (active > peak) {
      peak = active;
      peakOverlapMs = 0;
    }
    if (active === peak) peakOverlapMs += duration;
  }
  const failureCounts = new Map();
  const valid = calls.filter(({ id, result }) => {
    if (result.status === 200 && result.nodeId === node && result.upstream === id) return true;
    const failure = result.transportError ?? result.code ??
      (result.status === 200 ? 'unexpected reply' : 'HTTP ' + result.status);
    failureCounts.set(failure, (failureCounts.get(failure) ?? 0) + 1);
    return false;
  });
  const heldFullDelay = released.filter(call => call.releasedMs - call.enteredMs >= waitMs).length;
  const latencies = valid.map(call => call.finishedMs - call.sentMs).sort((a, b) => a - b);
  const percentile = p => latencies[Math.ceil(latencies.length * p) - 1] ?? null;
  const firstResource = calls.filter(call => call.result.resource)
    .reduce((first, call) => first === null || call.finishedMs < first.finishedMs ? call : first, null);
  return { replies: valid.length, failureCounts: Object.fromEntries(failureCounts), firstResource,
    entered: intervals.length, released: released.length,
    heldFullDelay, peak, peakOverlapMs, p50Ms: percentile(.5), p99Ms: percentile(.99), maxMs: percentile(1),
    offerSpanMs: Math.max(...calls.map(call => call.sentMs)) - Math.min(...calls.map(call => call.sentMs)),
    upstreamEntrySpanMs: intervals.length ?
      Math.max(...intervals.map(call => call.enteredMs)) - Math.min(...intervals.map(call => call.enteredMs)) : null };
}

async function capture(operation) {
  try { return await operation(); } catch (error) { return { error: error.message }; }
}

async function idleComparison() {
  const readIdle = async () => {
    const state = await cluster.state(node);
    const ordinary = state.processes.entries.filter(entry => entry.processClass === 'ordinary');
    assert.equal(ordinary.length, 1);
    assert.equal(state.placement.inFlight.total, 0);
    assert.equal(state.processes.configuration.ordinaryInFlightLimit, ordinaryLimit);
    assert.equal(state.processes.configuration.demandDrivenCalls, true);
    return { pool: state.processes.configuration, ordinary: ordinary[0],
      ...await readProcesses(args => cluster.compose(['exec', '-T', node, ...args]), productProcesses(state)) };
  };
  const startup = await readIdle();
  await delay(61000, undefined, { signal: controller.signal });
  const settled = await readIdle();
  assert.equal(startup.ordinary.generation, settled.ordinary.generation);
  const tenant = await cluster.tenant(tenantName);
  for (let index = 0; index < idleWorkerCount; ++index) {
    // Warm earlier Workers as well so the residency check is independent of deployment time.
    const name = 'idle-' + index;
    await cluster.deploy(tenant, name, 'export default {fetch() { return new Response("idle worker ' + index + '"); }}');
  }
  for (let index = 0; index < idleWorkerCount; ++index) {
    const reply = await request(cluster.urls[node], '/' + tenantName + '/idle-' + index);
    assert.equal(reply.status, 200);
    assert.equal(reply.text, 'idle worker ' + index);
  }
  const residents = await eventually('resident Worker set published', async () => {
    const reading = await readIdle();
    const workers = reading.ordinary.lifecycle?.residentWorkers
      ?.filter(worker => worker.workerId?.startsWith(tenantName + '/idle-')) ?? [];
    assert.equal(new Set(workers.map(worker => worker.workerId)).size, idleWorkerCount);
    return reading;
  });
  await record({ type: 'idle-ceiling', ordinaryLimit, idleWorkerCount, startup, settled, residents });
  console.log(JSON.stringify({ ordinaryLimit, idleWorkerCount,
    startupMemory: startup.cgroup['memory.current'], settledMemory: settled.cgroup['memory.current'] }));
}

async function run() {
  const projectRoot = fileURLToPath(new URL('../../', import.meta.url));
  const binaries = resolve(process.env.COWORKERD_CLUSTER_BIN_DIR ?? join(projectRoot, 'build'));
  const files = { daemon: join(binaries, 'coworkerd'), apiserver: join(binaries, 'coworkerd-apiserver'),
    launcher: join(binaries, 'coworkerd-launcher'), workerd: join(kernel, 'workerd'), threaded: join(kernel, 'coworkerd-threaded') };
  const hashes = Object.fromEntries(await Promise.all(Object.entries(files).map(async ([role, path]) =>
    [role, createHash('sha256').update(await readFile(path)).digest('hex')])));
  const kernelRecord = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
  const pins = JSON.parse(await readFile(join(projectRoot, 'patches/workerd/artifacts.json'), 'utf8'));
  assert.equal(kernelRecord.patchSeriesSha256, pins.patchSeriesSha256);
  assert.ok(pins.archives[kernelRecord.architecture], 'use the published kernel pinned by this checkout');
  assert.equal(hashes.workerd, kernelRecord.files.workerd);
  assert.equal(hashes.threaded, kernelRecord.files['coworkerd-threaded']);
  const productRevision = process.env.COWORKERD_CLUSTER_PRODUCT_COMMIT ?? 'HEAD';
  const [productCommit, fixtureCommit, engine] = await Promise.all([
    cluster.command('git', ['rev-parse', '--verify', productRevision + '^{commit}'], { cwd: projectRoot }),
    cluster.command('git', ['rev-parse', 'HEAD'], { cwd: projectRoot }),
    cluster.command('docker', ['info', '--format', '{{json .}}']).then(JSON.parse),
  ]);
  await record({ type: 'provenance', at: new Date().toISOString(), productCommit, fixtureCommit, hashes, kernel: kernelRecord,
    publishedPin: { releaseTag: pins.releaseTag, sha256: pins.archives[kernelRecord.architecture] },
    linux: { architecture: engine.Architecture, release: engine.KernelVersion, cpus: engine.NCPU,
      memoryBytes: engine.MemTotal, os: engine.OperatingSystem, dockerVersion: engine.ServerVersion },
    fixture: { peerImage, plan, rampMs, waitMs, recoveryMs, nodeResources, peerResources,
      rootLimit, nestedLimit, processLimit, ordinaryLimit, idleWorkerCount } });
  await cluster.start();
  if (idleWorkerCount !== undefined) return idleComparison();
  for (const [role, name] of Object.entries(sides)) {
    await cluster.command('docker', ['run', '--detach', '--rm', '--name', name, '--network', cluster.project + '_backend',
      '--network-alias', role === 'upstream' ? 'waiting-origin' : 'waiting-client', ...peerResources,
      '--mount', 'type=bind,source=' + peerSource + ',target=/peer.mjs,readonly',
      peerImage, 'node', '/peer.mjs', role, plan.join(','), String(rampMs), String(waitMs)]);
    started.push(name);
  }
  const tenant = await cluster.tenant(tenantName);
  await cluster.deploy(tenant, 'wait', `export default { async fetch(request) {
    const id = new URL(request.url).searchParams.get('call');
    if (id !== 'warm' && !/^\\d{1,2}-\\d{1,5}$/.test(id ?? '')) return new Response(null, { status: 404 });
    const path = id === 'warm' ? '/warm' : '/hold/' + id;
    const response = await fetch('http://waiting-origin:8080' + path, { redirect: 'manual' });
    if (!response.ok) return new Response('fixture upstream failed', { status: 502 });
    return Response.json({ nodeId: request.cf.nodeId, upstream: await response.text() });
  } };`);

  const [origin, client, image] = await Promise.all([
    eventually('local upstream ready', () => control('upstream', '/status')),
    eventually('local client ready', () => control('client', '/status')),
    cluster.command('docker', ['inspect', '--format', '{{.Image}}', sides.upstream]),
  ]);
  assert.equal(origin.waitMs, waitMs);
  const warm = async () => {
    const reply = await control('client', '/warm', 'POST');
    assert.deepEqual(reply, { status: 200, nodeId: node, upstream: 'warm' });
    return reply;
  };
  await warm();
  const quiet = () => eventually('node and ordinary process drained', async () => {
    const state = await cluster.state(node);
    assert.deepEqual(state.placement.inFlight, { roots: 0, nested: 0, total: 0 });
    assert.ok(state.processes.entries.every(entry => entry.inFlight === 0));
    assert.equal(state.egress.tenants[tenantName]?.inFlight ?? 0, 0);
    return state;
  }, { timeout: 3000, interval: 50 });
  const initial = await quiet();
  const ordinaryProcesses = initial.processes.entries.filter(entry => entry.processClass === 'ordinary');
  assert.equal(ordinaryProcesses.length, 1);
  const [ordinary] = ordinaryProcesses;
  assert.equal(initial.placement.ingressPlacement, true);
  assert.equal(initial.processes.configuration.tenantInFlightLimit, 0);
  assert.equal(ordinary.sandbox.ordinaryServer, 'threaded');
  assert.equal(ordinary.binarySha256, hashes.threaded);
  const { egress } = initial;
  const tenantEgress = egress.tenantPolicies.find(policy => policy.tenant === tenantName);
  const settleMs = initial.processes.configuration.threadedThreadIdleMs + 1000;
  await record({ type: 'effective', image, nodeVersion: client.nodeVersion, waitMs, settleMs,
    placement: { ingressPlacement: initial.placement.ingressPlacement, rootLimit: initial.placement.rootLimit,
      nestedLimit: initial.placement.nestedLimit, tenantRootLimit: initial.placement.tenantAdmission.rootLimit,
      tenantNestedLimit: initial.placement.tenantAdmission.nestedLimit },
    egress: { concurrency: tenantEgress.concurrency || egress.concurrency,
      requestsPerSecond: tenantEgress.requestsPerSecond || egress.requestsPerSecond, timeoutMs: egress.timeoutMs,
      defaultPolicy: egress.defaultPolicy },
    pool: initial.processes.configuration, sandbox: ordinary.sandbox });

  const sample = async (quietState) => {
    const start = await control('upstream', '/status');
    const [product, ...peers] = await Promise.all([
      capture(async () => {
        const state = quietState ?? await cluster.state(node);
        return { state: nodeReading(state),
          ...await readProcesses(args => cluster.compose(['exec', '-T', node, ...args]), productProcesses(state)) };
      }),
      ...Object.keys(sides).map(role => capture(() => readProcesses(
        args => cluster.command('docker', ['exec', sides[role], ...args]), [{ role, pid: 1 }]))),
    ]);
    const end = await control('upstream', '/status');
    return { start, end, product, client: peers[0], upstream: peers[1] };
  };
  const startup = await sample(initial);
  console.log('Waiting ' + settleMs + ' ms for the worker thread pool to settle');
  await delay(settleMs, undefined, { signal: controller.signal });
  const settled = await sample(await quiet());
  await record({ type: 'settling', startup, settled, warmReply: await warm() });

  for (const [number, offered] of plan.entries()) {
    const before = await sample(await quiet());
    const expected = { number, offered };
    assert.deepEqual(await control('upstream', '/batch', 'POST'), expected);
    assert.deepEqual(await control('client', '/batch', 'POST'), expected);
    const during = Promise.all([250, Math.max(1050, Math.ceil((rampMs + waitMs) / 2)), waitMs - 250].map(ms => capture(async () => {
      await delay(ms, undefined, { signal: controller.signal });
      return sample();
    })));
    try {
      await eventually('every client request settled', async () => {
        const observation = await control('client', '/status');
        assert.equal(observation.completed, offered);
      }, { timeout: rampMs + waitMs + 16000, interval: 300 });
    } catch (error) {
      const [client, upstream, samples] = await Promise.all([
        capture(() => control('client', '/result')), capture(() => control('upstream', '/result')), during,
      ]);
      await record({ type: 'incomplete-batch', number, offered, before, during: samples, client, upstream });
      throw error;
    }
    const [clientResult, upstreamResult, samples] = await Promise.all([
      control('client', '/result'),
      eventually('local upstream drained', async () => {
        const result = await control('upstream', '/result');
        assert.equal(result.active, 0);
        return result;
      }, { timeout: 5000 }),
      during,
    ]);
    const summary = summarize(clientResult.calls, upstreamResult.calls, waitMs);
    const after = await capture(async () => sample(await quiet()));
    await record({ type: 'batch', number, offered, summary, before, during: samples, after,
      calls: clientResult.calls, waits: upstreamResult.calls });
    const recovered = await capture(async () => {
      await delay(recoveryMs, undefined, { signal: controller.signal });
      const sampleAfter = await sample(await quiet());
      return { sample: sampleAfter, probe: await warm() };
    });
    await record({ type: 'recovery', number, ...recovered });
    console.log(JSON.stringify({ offered, ...summary, recovered: !recovered.error }));
    assert.ok(!recovered.error, recovered.error);
    if (options['expect-first-resource']) {
      assert.equal(summary.heldFullDelay, summary.replies, 'each admitted call must wait for the full hold');
      if (number < plan.length - 1) {
        assert.equal(summary.replies, offered, 'sub-capacity batch refused');
        assert.equal(summary.peak, offered, 'sub-capacity batch did not fully overlap');
      } else {
        const first = after.product?.state?.callResources?.firstRefusal;
        assert.ok(first?.resource, 'the largest batch must produce a node resource record');
        assert.match(first.resource.kind, /^(node|process)-(memory|tasks|descriptors)$/);
        assert.ok(first.resource.limit > 0 && first.resource.current > 0 && first.inFlight > 0);
        const refused = clientResult.calls.filter(({ result }) => result.resource);
        assert.equal(refused.length, offered - summary.replies, 'unexpected non-capacity failure');
        assert.ok(refused.every(({ result }) => result.code === 'OVERLOADED' && result.dispatched === false &&
          result.retryable === true), 'a resource refusal must confirm it can be retried');
        assert.ok(refused.some(({ result }) => result.resource.kind === first.resource.kind),
          "the client never received the node's first resource kind");
      }
    }
    if (expectFull) {
      assert.equal(summary.replies, offered);
      assert.equal(summary.heldFullDelay, offered);
      assert.equal(summary.peak, offered);
      assert.ok(summary.peakOverlapMs > 0);
    }
  }
}

try {
  await run();
} catch (error) {
  await record({ type: 'error', message: error.message });
  await cluster.diagnostics('waiting-call-capacity', error);
  throw error;
} finally {
  try {
    if (started.length) await cluster.command('docker', ['stop', '--time', '2', ...started], { cleanup: true });
  } finally {
    await cluster.close();
  }
}

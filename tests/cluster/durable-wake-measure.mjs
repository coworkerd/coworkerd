import assert from 'node:assert/strict';
import { execFile as execFileCallback } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [kernelArg, reportArg, ...extra] = process.argv.slice(2);
assert.ok(kernelArg && reportArg && extra.length === 0,
  'Usage: node tests/cluster/durable-wake-measure.mjs <verified-kernel-directory> <new-report.jsonl>');
assert.equal(process.platform, 'linux', 'Use a dedicated Linux runner');
const root = fileURLToPath(new URL('../../', import.meta.url));
const kernel = resolve(kernelArg);
const report = resolve(reportArg);
const binaries = resolve(process.env.COWORKERD_CLUSTER_BIN_DIR ?? join(root, 'build'));
const require = createRequire(new URL('../../cli/package.json', import.meta.url));
const { WebSocket } = require('ws');
const execFile = promisify(execFileCallback);
const exec = async (program, args) => (await execFile(program, args, { cwd: root })).stdout.trim();
const sha256 = async filename => createHash('sha256').update(await readFile(filename)).digest('hex');
const record = (metric, fields) => appendFile(report, JSON.stringify({ metric, at: new Date().toISOString(), ...fields }) + '\n');
const samplesPerPath = 120;
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}

const worker = `
  import { DurableObject } from 'cloudflare:workers';
  let moduleId;
  export class Record extends DurableObject {
    constructor(ctx, env) {
      super(ctx, env);
      moduleId ??= crypto.randomUUID();
      this.actorId = crypto.randomUUID();
      this.id = ctx.id.toString();
      const sockets = ctx.getWebSockets('retained');
      this.constructedWith = sockets.length;
      const automatic = ctx.getWebSocketAutoResponse();
      this.restored = sockets.every(socket => ctx.getTags(socket).join(',') === 'retained' &&
        socket.deserializeAttachment()?.id === this.id &&
        automatic?.request === 'auto' && automatic.response === 'automatic');
      const index = Number.parseInt(this.id.slice(-4), 16);
      if (index >= 1000 && index < 2000) console.log('wake-constructor ' + this.id + ' ' + this.actorId);
    }
    async fetch(request) {
      const value = new URL(request.url).pathname.split('/').at(-1);
      if (request.headers.get('upgrade')?.toLowerCase() === 'websocket') {
        await this.ctx.storage.put('value', value);
        const [client, socket] = Object.values(new WebSocketPair());
        this.ctx.acceptWebSocket(socket, ['retained']);
        this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('auto', 'automatic'));
        socket.serializeAttachment({ id: this.id, value });
        return new Response(null, { status: 101, webSocket: client });
      }
      if (request.method === 'POST') await this.ctx.storage.put('value', value);
      return Response.json({ moduleId, actorId: this.actorId, value: await this.ctx.storage.get('value') });
    }
    async webSocketMessage(socket, message) {
      socket.send(JSON.stringify({
        moduleId, actorId: this.actorId, message, constructedWith: this.constructedWith,
        restored: this.restored, attachment: socket.deserializeAttachment(),
        value: await this.ctx.storage.get('value'),
      }));
    }
  }
  export default { fetch(request, env) {
    const [kind, index] = new URL(request.url).pathname.split('/').at(-1).split('-');
    const slot = Number(index) + (kind === 'socket' ? 1000 : kind === 'ready' ? 2000 : 0);
    const id = env.RECORDS.idFromString(slot.toString(16).padStart(64, '0'));
    return env.RECORDS.get(id).fetch(request);
  } };
`;

async function hostSample(label, project = null) {
  const containers = await exec('docker', ['ps', '--format', '{{.Names}}\t{{.Label "com.docker.compose.project"}}']);
  const processes = await exec('ps', ['-eo', 'pid,ppid,pcpu,comm', '--sort=-pcpu']);
  const load = await exec('vmstat', ['1', '3']);
  await record('host-load', {
    label, project, containers, load, uptime: await exec('uptime', []),
    memory: await exec('free', ['-m']), disk: await exec('df', ['-h', '.']),
    topProcesses: processes.split('\n').slice(0, 18),
  });
  if (project) {
    const rows = containers.split('\n').filter(Boolean);
    assert.ok(rows.length >= 3 && rows.every(row => row.split('\t')[1] === project), containers);
  } else {
    assert.equal(containers, '', 'The runner has another active container');
  }
  assert.doesNotMatch(processes, /\b(bazel[a-z-]*|ninja|clang[-+0-9]*|cc1plus)\s*$/m,
    'Compiler or build processes are active');
}

function summarize(rows) {
  const successful = rows.filter(row => !row.error).map(row => row.ms).toSorted((a, b) => a - b);
  const rank = p => successful[Math.ceil(p * successful.length) - 1] ?? null;
  return { attempts: rows.length, successes: successful.length, errors: rows.length - successful.length,
    p50Ms: rank(0.5), p99Ms: rank(0.99) };
}

function exchange(ws, message, ping = false) {
  const reply = once(ws, ping ? 'pong' : 'message', { signal: AbortSignal.timeout(7000) });
  if (ping) ws.ping(message);
  else ws.send(message);
  return reply.then(([data]) => data.toString());
}

async function measure(backend, kernelHashes) {
  const threaded = backend === 'threaded';
  const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-durable-wake-')), kernel, controller.signal, {
    singleNode: true, binariesDirectory: binaries, threadedNodes: threaded ? ['node-1'] : [],
    configureNode: () => ({ durableObjects: { processes: 1, peers: 0, threadedStorage: threaded } }),
  });
  const [node] = cluster.nodes;
  const sockets = [];
  try {
    await cluster.start();
    const tenant = await cluster.tenant('acceptance-wake', { logLinesPerSecond: 1000 });
    await cluster.deploy(tenant, 'objects', worker, { wrangler: {
      durable_objects: { bindings: [{ name: 'RECORDS', class_name: 'Record' }] },
      migrations: [{ tag: 'v1', new_sqlite_classes: ['Record'] }],
    } });
    const path = `/${tenant.name}/objects/`;
    const call = async (name, method = 'GET') =>
      json(await request(cluster.urls[node], path + name, { method, timeout: 7000 }), 200, name);
    await eventually('object deployment', () => call('ready-0'));

    const readState = async () => {
      const state = await cluster.state(node);
      const children = state.processes.entries.filter(entry => entry.processClass === 'durable');
      assert.equal(children.length, 1);
      const { processId, pid, generation, state: phase, binarySha256, sandbox, lifecycle } = children[0];
      assert.ok(pid > 0);
      assert.equal(phase, 'ready');
      assert.equal(sandbox.durableServer, backend);
      assert.equal(binarySha256, kernelHashes[backend]);
      const resident = threaded ? lifecycle?.durableAdmission : state.durableLog.resident
        .find(row => row.processId === processId + '-' + generation);
      assert.ok(resident, 'the durable process reported residency');
      return { child: { processId, pid, generation, binarySha256 },
        sandboxMode: sandbox.mode, cpuLimitMs: sandbox.defaultCpuLimitMs, resident,
        webSockets: state.webSockets, rebuilds: state.durableLog.objects.rebuilds };
    };
    const initial = await eventually('durable process telemetry', readState);
    await record('initial', { backend, ...initial });
    const onSameChild = async () => {
      const state = await readState();
      assert.deepEqual(state.child, initial.child);
      return state;
    };

    const saved = [];
    for (let i = 0; i < samplesPerPath; ++i) {
      const name = 'http-' + i;
      const value = await call(name, 'POST');
      assert.equal(value.value, name);
      saved.push({ name, ...value });
    }
    for (let i = 0; i < samplesPerPath; ++i) {
      const name = 'socket-' + i;
      const id = (1000 + i).toString(16).padStart(64, '0');
      const url = new URL(path + name, cluster.urls[node]);
      url.protocol = 'ws:';
      const ws = new WebSocket(url);
      ws.on('error', () => {});
      await once(ws, 'open', { signal: AbortSignal.timeout(7000) });
      const warm = JSON.parse(await exchange(ws, 'warm'));
      assert.equal(warm.value, name);
      assert.deepEqual(warm.attachment, { id, value: name });
      sockets.push({ ws, name, id, warm });
    }
    const warmCopies = new Set([...saved.map(value => value.moduleId), ...sockets.map(({ warm }) => warm.moduleId)]);
    await record('warm-copies', { backend, moduleIds: [...warmCopies] });

    const constructorLog = async () => {
      let messages;
      if (threaded) {
        // The threaded child writes these console lines to its process log.
        const log = await cluster.compose(['exec', '-T', node, 'cat', '/var/lib/coworkerd/logs/workerd.log']);
        messages = log.split('\n');
      } else {
        const logs = await cluster.api(`${tenant.path}/workers/objects/logs?limit=1000`, { token: tenant.token });
        assert.equal(logs.nodes.length, 1);
        assert.equal(logs.nodes[0].problem, undefined);
        assert.equal(logs.nodes[0].droppedLines, 0);
        messages = logs.lines.map(line => line.message ?? '');
      }
      const constructors = messages.flatMap(line => {
        const match = line.match(/wake-constructor ([a-f0-9]{64}) ([a-f0-9-]{36})/);
        return match ? [{ socketId: match[1], actorId: match[2] }] : [];
      });
      return constructors.sort((a, b) => a.socketId.localeCompare(b.socketId) || a.actorId.localeCompare(b.actorId));
    };
    const expected = sockets.map(({ id, warm }) => ({ socketId: id, actorId: warm.actorId }));
    const before = await eventually('all trees are retired and connections hibernating', async () => {
      const state = await onSameChild();
      if (threaded) {
        assert.equal(state.resident.residentRoots, 0);
        assert.equal(state.resident.homeSessions, 0);
        assert.equal(state.webSockets.parked, sockets.length);
      } else {
        assert.equal(state.resident.objects, 0);
      }
      return state;
    }, { timeout: 30000 });
    const beforeConstructors = await eventually('externally visible constructor logs', async () => {
      const observed = await constructorLog();
      assert.deepEqual(observed, expected);
      return observed;
    });
    await record('asleep', { backend, ...before, constructors: beforeConstructors });

    await hostSample(backend + '-before', cluster.project);
    for (const { ws, name } of sockets) {
      assert.equal(await exchange(ws, name, true), name);
      assert.equal(await exchange(ws, 'auto'), 'automatic');
    }
    const afterControls = await onSameChild();
    if (threaded) {
      assert.equal(afterControls.resident.residentRoots, 0);
      assert.equal(afterControls.resident.homeSessions, 0);
      assert.equal(afterControls.resident.idleRetirements, before.resident.idleRetirements);
      assert.equal(afterControls.webSockets.parked, sockets.length);
      assert.equal(afterControls.webSockets.socketWakes, before.webSockets.socketWakes);
    } else {
      assert.equal(afterControls.resident.objects, 0);
      assert.equal(afterControls.resident.woken, before.resident.woken);
    }
    assert.deepEqual(await constructorLog(), beforeConstructors);
    await record('automatic-traffic', { backend, pingPong: sockets.length, automaticReplies: sockets.length,
      constructors: beforeConstructors.length, ...afterControls });

    const socketRows = [];
    for (const { ws, name, id, warm } of sockets) {
      const start = performance.now();
      try {
        const reply = await exchange(ws, 'wake-' + name);
        const ms = performance.now() - start;
        const result = JSON.parse(reply);
        assert.equal(result.message, 'wake-' + name);
        assert.notEqual(result.actorId, warm.actorId);
        assert.ok(warmCopies.has(result.moduleId), 'the socket woke in a prewarmed Worker copy');
        assert.equal(result.constructedWith, 1);
        assert.equal(result.restored, true);
        assert.deepEqual(result.attachment, { id, value: name });
        assert.equal(result.value, name);
        socketRows.push({ name, id, ms, result });
      } catch (error) {
        socketRows.push({ name, id, ms: performance.now() - start, error: error.message });
      }
    }
    const httpRows = [];
    for (const seed of saved) {
      const start = performance.now();
      try {
        const result = await call(seed.name);
        const ms = performance.now() - start;
        assert.notEqual(result.actorId, seed.actorId);
        assert.ok(warmCopies.has(result.moduleId), 'the object reopened in a prewarmed Worker copy');
        assert.equal(result.value, seed.value);
        httpRows.push({ name: seed.name, ms, seed, result });
      } catch (error) {
        httpRows.push({ name: seed.name, ms: performance.now() - start, seed, error: error.message });
      }
    }
    await record('samples', { backend, child: initial.child, webSocket: socketRows, http: httpRows });
    const after = await onSameChild();
    assert.equal(after.rebuilds, before.rebuilds);
    const counters = { webSocket: summarize(socketRows), http: summarize(httpRows) };
    await record('backend-summary', { backend, ...after, ...counters });
    await hostSample(backend + '-after', cluster.project);
    assert.equal(counters.webSocket.errors + counters.http.errors, 0, 'all timed samples must pass');
    const afterConstructors = await eventually('exactly one additional constructor per socket', async () => {
      const observed = await constructorLog();
      const wanted = [...beforeConstructors, ...socketRows.map(row => ({
        socketId: row.id, actorId: row.result.actorId,
      }))].sort((a, b) => a.socketId.localeCompare(b.socketId) || a.actorId.localeCompare(b.actorId));
      assert.deepEqual(observed, wanted);
      return observed;
    });
    await record('constructors-after', { backend, constructors: afterConstructors });
    return counters;
  } catch (error) {
    await record('backend-error', { backend, error: error.stack });
    await cluster.diagnostics('wake-measurement', error);
    throw error;
  } finally {
    for (const { ws } of sockets) ws.terminate();
    await cluster.close();
  }
}

await writeFile(report, '', { flag: 'wx' });
const pins = JSON.parse(await readFile(join(root, 'patches/workerd/artifacts.json'), 'utf8'));
const manifest = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
assert.equal(manifest.patchSeriesSha256, pins.patchSeriesSha256);
assert.equal(manifest.upstreamCommit, pins.upstreamCommit);
const kernelHashes = {
  upstream: await sha256(join(kernel, 'workerd')),
  threaded: await sha256(join(kernel, 'coworkerd-threaded')),
};
assert.equal(kernelHashes.upstream, manifest.files.workerd);
assert.equal(kernelHashes.threaded, manifest.files['coworkerd-threaded']);
await record('provenance', {
  commit: await exec('git', ['rev-parse', 'HEAD']),
  kernelRelease: pins.releaseTag, upstream: pins.upstream, architecture: manifest.architecture,
  patchSeriesSha256: pins.patchSeriesSha256, archiveSha256: pins.archives[manifest.architecture], kernelHashes,
  daemons: Object.fromEntries(await Promise.all(['coworkerd', 'coworkerd-apiserver', 'coworkerd-launcher']
    .map(async file => [file, await sha256(join(binaries, file))]))),
  os: await exec('uname', ['-a']), cpu: await exec('lscpu', []),
  docker: await exec('docker', ['version', '--format', '{{.Server.Version}}']),
  compose: await exec('docker', ['compose', 'version']), node: process.version,
  workload: { samplesPerPath, nodes: 1, durableProcesses: 1, selectedPeers: 0, offeredConcurrency: 1 },
});
const results = {};
for (const backend of ['upstream', 'threaded']) {
  await hostSample(backend + '-before-cluster');
  try {
    results[backend] = await measure(backend, kernelHashes);
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
  await hostSample(backend + '-after-cluster');
}
if (results.upstream && results.threaded) {
  const comparison = Object.fromEntries(['http', 'webSocket'].map(path => {
    const upstream = results.upstream[path];
    const threaded = results.threaded[path];
    const p50DeltaMs = threaded.p50Ms - upstream.p50Ms;
    const p99DeltaMs = threaded.p99Ms - upstream.p99Ms;
    return [path, { upstream, threaded, p50DeltaMs, p99DeltaMs, pass: p50DeltaMs <= 5 && p99DeltaMs <= 5 }];
  }));
  await record('comparison', comparison);
  console.log(JSON.stringify(comparison, null, 2));
  if (Object.values(comparison).some(row => !row.pass)) process.exitCode = 1;
}

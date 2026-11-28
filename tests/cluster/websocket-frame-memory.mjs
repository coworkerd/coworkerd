import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rename, unlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import { Cluster, eventually } from './cluster.mjs';

const MiB = 1024 * 1024;
const memoryLimitBytes = 512 * MiB;
const connectionCount = 8;
const waveSize = 2;
const [kernel, mode, ...extra] = process.argv.slice(2);
if (!kernel || extra.length || (mode && mode !== '--default-mailbox')) {
  console.error('Usage: node tests/cluster/websocket-frame-memory.mjs <kernel-directory> [--default-mailbox]');
  process.exit(2);
}
const defaultMailbox = mode === '--default-mailbox';
const messageBytes = defaultMailbox ? 3 * MiB : MiB / 2;
const reservationBytes = defaultMailbox ? 128 * MiB : 2 * MiB;
const sampleIntervalSeconds = defaultMailbox ? 0.02 : 0.005;

const { WebSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}
const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(420_000)]);
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-ws-frame-memory-')), resolve(kernel), signal, {
  singleNode: true, threadedNodes: ['node-1'], controlDurableLifecycle: true,
  nodeComposeResources: { cpus: 2, mem_limit: memoryLimitBytes, memswap_limit: memoryLimitBytes },
  configureNode: () => ({
    processPool: { count: 1, inFlightLimit: 32 }, quarantineProcesses: 0,
    ingress: { webSocketsPerNode: connectionCount },
    durableObjects: { threadedStorage: true, peers: 0, socketFrameBytesPerNode: reservationBytes },
  }),
});
const [node] = cluster.nodes;
const sockets = [];
const closures = [];
const control = name => join(cluster.durableLifecycleGate, name);
const gate = control('hold-socket-adopt');
const stopSampler = control('stop-rss-sampler');
const firstHalf = Buffer.alloc(messageBytes / 2, 0x5a);
const secondHalf = Buffer.alloc(messageBytes / 2, 0xa5);
const fullMessage = Buffer.concat([firstHalf, secondHalf]);
const next = (socket, event) => once(socket, event, { signal: AbortSignal.any([signal, AbortSignal.timeout(15_000)]) });
const state = () => cluster.state(node);
const expectBudget = async reservedBytes => {
  const budget = (await state()).webSockets.homeFrames;
  assert.deepEqual(budget, { limitBytes: reservationBytes, reservedBytes, refused: 0 });
  return budget;
};
function processIds({ server, processes }) {
  const ids = [server.pid, ...['ordinary', 'durable'].map(role =>
    processes.entries.find(entry => entry.processClass === role && entry.state === 'ready')?.pid)];
  assert.ok(ids.every(pid => Number.isInteger(pid) && pid > 0), 'the daemon and both children are ready');
  return ids;
}
const homeOverloads = ({ processes }) => processes.entries.find(entry => entry.processClass === 'durable')
  .lifecycle.durableAdmission.overloaded;

// Keep sampling inside the tested node so a Docker exec round trip does not set the sampling interval.
// Each row contains daemon, ordinary and durable RSS followed by the node cgroup's current memory.
const sampleProc = `while [ ! -e /tmp/cluster-test-durable-lifecycle/stop-rss-sampler ]; do
  awk '/^Rss:/ { printf "%s ", $2 } FILENAME == "/sys/fs/cgroup/memory.current" { print $1 }' \
    "$1" "$2" "$3" /sys/fs/cgroup/memory.current
  sleep ${sampleIntervalSeconds}
done`;
const samples = [];
let sampler;
let samplerClosed;
let samplerError;
function summarize(rows) {
  if (!rows.length) return { count: 0, peak: null };
  const roles = ['daemonRssKiB', 'ordinaryRssKiB', 'durableRssKiB', 'nodeCgroupBytes'];
  return { count: rows.length, peak: Object.fromEntries(roles.map((role, i) => [role,
    Math.max(...rows.map(row => row[i]))])),
  };
}
async function snapshot() {
  const index = samples.length;
  await eventually('five RSS samples', () => {
    signal.throwIfAborted();
    if (samplerError) throw samplerError;
    assert.ok(samples.length >= index + 5);
  }, { timeout: 2000, interval: 25 });
  return summarize(samples.slice(index, index + 5));
}

try {
  await cluster.start();
  const tenant = await cluster.tenant('socket-frame-memory');
  await cluster.deploy(tenant, 'socket', `
    import { DurableObject } from 'cloudflare:workers';
    export class Receiver extends DurableObject {
      fetch() {
        const [client, socket] = Object.values(new WebSocketPair());
        this.ctx.acceptWebSocket(socket);
        return new Response(null, { status: 101, webSocket: client });
      }
      webSocketMessage(socket, message) {
        const bytes = new Uint8Array(message);
        const middle = bytes.length / 2;
        socket.send(JSON.stringify([bytes.length, bytes[0], bytes[middle - 1], bytes[middle], bytes.at(-1)]));
      }
    }
    export default {
      fetch(request, env) { return env.RECEIVER.getByName('fixture').fetch(request); }
    };
  `, { wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['Receiver'] }],
    durable_objects: { bindings: [{ name: 'RECEIVER', class_name: 'Receiver' }] } } });
  const address = new URL(`/${tenant.name}/socket`, cluster.urls[node]);
  address.protocol = 'ws:';
  for (let i = 0; i < connectionCount; ++i) {
    const socket = new WebSocket(address, { perMessageDeflate: false });
    sockets.push(socket);
    socket.on('error', () => {});
    socket.on('close', (code, reason) => closures.push({ socket: i, code, reason: reason.toString() }));
    await next(socket, 'open');
  }
  await writeFile(control('idle-offset-next'), '60000');
  await rename(control('idle-offset-next'), control('idle-offset-ms'));
  await eventually('all eight sockets park on the single node', async () => {
    assert.equal((await state()).webSockets.parked, connectionCount);
  });

  const pids = processIds(await state());
  sampler = spawn('docker', cluster.composeArgs(['exec', '-T', node, 'sh', '-ec', sampleProc, 'sample-rss',
    ...pids.map(pid => `/proc/${pid}/smaps_rollup`)]), { env: cluster.env, stdio: ['ignore', 'pipe', 'inherit'] });
  samplerClosed = new Promise(resolveClose => sampler.once('close', resolveClose));
  sampler.once('error', error => { samplerError = error; });
  createInterface({ input: sampler.stdout }).on('line', line => {
    const row = line.trim().split(/\s+/).map(Number);
    if (row.length === 4 && row.every(value => value > 0)) samples.push(row);
  });
  const baseline = { ...await snapshot(), budget: await expectBudget(0) };

  const waves = [];
  let defaultRefusal;
  if (defaultMailbox) {
    const previous = homeOverloads(await state());
    const [socket] = sockets;
    const closed = next(socket, 'close');
    const start = samples.length;
    socket.send(fullMessage);
    const [code, reason] = await closed;
    assert.equal(code, 1013);
    assert.equal(reason.toString(), 'Durable Object socket wake refused');
    const active = summarize(samples.slice(start));
    const overloaded = await eventually('the home recorded one admission refusal', async () => {
      const delta = homeOverloads(await state()) - previous;
      assert.equal(delta, 1);
      return delta;
    });
    defaultRefusal = { code, reason: reason.toString(), overloads: overloaded, active, settledBudget: await expectBudget(0) };
  }

  for (let offset = 0; !defaultMailbox && offset < sockets.length; offset += waveSize) {
    signal.throwIfAborted();
    const wave = sockets.slice(offset, offset + waveSize);
    const gated = offset === 0;
    let receiving;
    if (gated) {
      await writeFile(gate, '');
      await Promise.all(wave.map(async socket => {
        const pong = next(socket, 'pong');
        socket.send(firstHalf, { fin: false });
        socket.ping('received');
        assert.equal((await pong)[0].toString(), 'received');
      }));
      await expectBudget(0);
      receiving = { ...await snapshot(), budget: await expectBudget(0) };
    }
    const sendStart = samples.length;
    const replies = Promise.all(wave.map(async socket => {
      const reply = next(socket, 'message');
      socket.send(gated ? secondHalf : fullMessage, { fin: true });
      const [message, isBinary] = await reply;
      assert.equal(isBinary, false);
      assert.deepEqual(JSON.parse(message.toString()), [messageBytes, 0x5a, 0x5a, 0xa5, 0xa5]);
    }));
    void replies.catch(() => {});
    let held;
    if (gated) {
      await eventually('both complete messages have reserved capacity', () => expectBudget(reservationBytes),
        { timeout: 4000, interval: 25 });
      held = { ...await snapshot(), budget: await expectBudget(reservationBytes) };
    }
    const handoffStart = gated ? samples.length : sendStart;
    if (gated) await unlink(gate);
    await replies;
    const active = summarize(samples.slice(handoffStart));
    const settledBudget = await eventually('uploads returned their reservations', () => expectBudget(0), { timeout: 3000 });
    waves.push({ receiving, held, active, settledBudget });
  }
  await delay(300, undefined, { signal });
  const drained = await snapshot();
  const finalState = await state();
  assert.deepEqual(processIds(finalState), pids, 'the measured processes stayed on the same generation');
  const remaining = defaultMailbox ? sockets.slice(1) : sockets;
  assert.ok(remaining.every(socket => socket.readyState === WebSocket.OPEN), 'the other connections stayed open');
  const sampled = summarize(samples);
  await writeFile(stopSampler, '');
  await Promise.race([samplerClosed, delay(5000, undefined, { ref: false }).then(() => {
    throw new Error('the RSS sampler did not stop');
  })]);
  assert.equal(sampler.exitCode, 0, 'the RSS sampler exited cleanly');
  const cgroup = await cluster.compose(['exec', '-T', node, 'sh', '-ec', `
    cd /sys/fs/cgroup
    cat memory.max memory.swap.max memory.peak
    awk '$1 == "oom_kill" {print $2}' memory.events`]);
  const [limitBytes, swapLimitBytes, peakBytes, oomKills] = cgroup.split('\n').map(Number);
  assert.equal(limitBytes, memoryLimitBytes);
  assert.equal(swapLimitBytes, 0);
  assert.ok(peakBytes > 0 && peakBytes <= limitBytes);
  assert.equal(oomKills, 0);
  console.log(JSON.stringify({ node, pids, connectionCount, waveSize, messageBytes, reservationBytes,
    baseline, waves, defaultRefusal, drained, sampled, cgroup: { limitBytes, swapLimitBytes, peakBytes, oomKills } }));
} catch (error) {
  console.error('Socket closes before fixture cleanup:', closures);
  console.error('Socket send backlog:', sockets.map(socket => socket.bufferedAmount));
  if (samples.length) console.error('Sampled memory through failure:', summarize(samples));
  if (cluster.urls[node]) {
    const observed = await state().catch(() => null);
    if (observed) console.error('Final reservations and home overloads:', observed.webSockets.homeFrames,
      observed.processes.entries.find(entry => entry.processClass === 'durable')?.lifecycle?.durableAdmission?.overloaded);
  }
  await cluster.diagnostics('WebSocket frame memory', error);
  throw error;
} finally {
  controller.abort();
  await Promise.all([unlink(gate).catch(() => {}), writeFile(stopSampler, '').catch(() => {})]);
  for (const socket of sockets) socket.terminate();
  try {
    await cluster.close();
  } finally {
    await samplerClosed;
  }
}

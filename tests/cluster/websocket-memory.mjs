import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Cluster, eventually } from './cluster.mjs';

const [kernel, mode, countArg = '3072'] = process.argv.slice(2);
const count = Number(countArg);
const warmCount = 32;
if (!kernel || !['local', 'forwarded'].includes(mode) || !Number.isSafeInteger(count) || count <= warmCount ||
    count + warmCount > 16384) {
  console.error('Usage: node tests/cluster/websocket-memory.mjs <kernel-directory> <local|forwarded> [connections]');
  process.exit(2);
}

const { WebSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM'])
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
const sockets = [];
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-ws-memory-')), resolve(kernel), controller.signal, {
  configureNode: node => ({
    // Each open stateless WebSocket also holds a native invocation channel in its workerd process.
    processPool: { count: 1, inFlightLimit: count + warmCount },
    ...(node === 'node-1' ? { ingress: { webSocketsPerNode: count } } : {}),
    ...(node === 'node-1' && mode === 'forwarded' ? { ingressPlacement: true, placement: { homeProbability: 1 } } : {}),
  }),
});
const [ingress, peer] = cluster.nodes;
const executing = mode === 'forwarded' ? peer : ingress;
const measuredNodes = [...new Set([ingress, executing])];

function home(workerId) {
  const score = node => createHash('sha256').update(workerId).update('\0').update(node).digest().readBigUInt64BE();
  return [...cluster.nodes].sort((a, b) => score(a) > score(b) ? -1 : 1)[0];
}

async function openTo(target, size) {
  while (sockets.length < size) {
    controller.signal.throwIfAborted();
    const batch = Math.min(size - sockets.length, 16);
    await Promise.all(Array.from({ length: batch }, async () => {
      const index = sockets.length;
      const socket = new WebSocket(target, { handshakeTimeout: 15000 });
      sockets.push(socket);
      socket.on('error', () => {});
      await once(socket, 'open', { signal: controller.signal }).catch(error => {
        throw new Error(`opening WebSocket ${index + 1}`, { cause: error });
      });
    }));
    if (sockets.length === size || sockets.length % 512 === 0) console.log(`Opened ${sockets.length} WebSockets`);
  }
  assert.ok(sockets.every(socket => socket.readyState === WebSocket.OPEN), 'every measured connection is still open');
}

const rssSamples = async () => {
  const read = async (node, pids) => {
    const rss = await cluster.compose(['exec', '-T', node, 'sh', '-ec',
      'for pid do awk \'/^Rss:/ {print $2; exit}\' "/proc/$pid/smaps_rollup"; done', 'sample-rss', ...pids]);
    const samples = rss.split('\n').map(Number);
    assert.equal(samples.length, pids.length);
    assert.ok(samples.every(value => value > 0));
    return samples;
  };
  const pids = await Promise.all(measuredNodes.map(async node => {
    const state = await cluster.state(node);
    const ordinary = state.processes.entries.find(entry => entry.processClass === 'ordinary');
    return [String(state.server.pid), String(ordinary.pid)];
  }));
  const values = [];
  for (let i = 0; i < 5; ++i) {
    values.push(await Promise.all(measuredNodes.map((node, index) => read(node, pids[index]))));
    await delay(200, undefined, { signal: controller.signal });
  }
  return { pids, rssKiB: Object.fromEntries(measuredNodes.map((node, i) => {
    const median = column => values.map(row => row[i][column]).sort((a, b) => a - b)[2];
    return [node, { daemon: median(0), workerd: median(1) }];
  })) };
};

try {
  await cluster.start();
  const tenant = await cluster.tenant('websocket-memory');
  let worker = 'local';
  if (mode === 'forwarded') {
    for (let i = 0; ; ++i) {
      worker = `peer-${i}`;
      if (home(`${tenant.name}/${worker}`) === executing) break;
    }
  }
  await cluster.deploy(tenant, worker, `export default { fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.addEventListener('close', event => server.close(event.code, event.reason));
    return new Response(null, { status: 101, webSocket: client });
  } };`);
  const target = new URL(`/${tenant.name}/${worker}`, cluster.urls[ingress]);
  target.protocol = 'ws:';
  const verify = (total) => eventually(`${total} connections routed to ${executing}`, async () => {
    for (const node of cluster.nodes) {
      const { webSockets } = await cluster.state(node);
      assert.equal(webSockets.ingress.open, node === ingress ? total : 0, node);
      assert.equal(webSockets.execution.open, node === executing ? total : 0, node);
    }
  });
  await openTo(target, warmCount);
  await verify(warmCount);
  const baseline = await rssSamples();
  await openTo(target, count);
  await delay(2000, undefined, { signal: controller.signal });
  await verify(count);
  const loaded = await rssSamples();
  assert.deepEqual(loaded.pids, baseline.pids, 'the sampled processes must not have rotated');
  const perSocketKiB = Object.fromEntries(measuredNodes.map(node => [node,
    Object.fromEntries(['daemon', 'workerd'].map(role => [role,
      (loaded.rssKiB[node][role] - baseline.rssKiB[node][role]) / (count - warmCount)])),
  ]));
  console.log(JSON.stringify({ mode, ingress, executing, warmCount, count, baseline: baseline.rssKiB,
    loaded: loaded.rssKiB, perSocketKiB }));
} catch (error) {
  const states = {};
  for (const { readyState } of sockets) states[readyState] = (states[readyState] ?? 0) + 1;
  console.error(`Socket states on failure: ${JSON.stringify(states)}`);
  throw error;
} finally {
  for (const socket of sockets) socket.terminate();
  await cluster.close();
}

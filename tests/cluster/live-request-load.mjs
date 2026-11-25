import assert from 'node:assert/strict';
import { get } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { eventually, request } from './cluster.mjs';

const source = `export default {
  fetch(request) {
    if (!new URL(request.url).pathname.endsWith('/idle')) return new Response('ready');
    return new Response(new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode('data: ready\\n\\n'));
    } }), { headers: { 'content-type': 'text/event-stream' } });
  },
};`;

async function sample(cluster, nodes) {
  return Promise.all(nodes.map(async node => {
    const state = await cluster.state(node);
    const pid = state.server.pid;
    const stat = await cluster.compose(['exec', '-T', node, 'cat', `/proc/${pid}/task/${pid}/stat`]);
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(/\s+/);
    return {
      node, at: performance.now(), pid,
      ticks: Number(fields[11]) + Number(fields[12]),
      sent: state.peerTransport.sent, received: state.peerTransport.received,
      outgoing: state.placement.proxyingIngress,
      executing: state.placement.inFlight.roots,
    };
  }));
}

function rates(before, after, ticksPerSecond) {
  return after.map((end, i) => {
    const start = before[i];
    assert.equal(end.pid, start.pid, `${end.node} retained its daemon`);
    const seconds = (end.at - start.at) / 1000;
    return {
      node: end.node, sentMessagesPerSecond: (end.sent - start.sent) / seconds,
      receivedMessagesPerSecond: (end.received - start.received) / seconds,
      mainThreadCpuPercent: 100 * (end.ticks - start.ticks) / ticksPerSecond / seconds,
    };
  });
}

export async function liveRequestCapacity(cluster) {
  const nodes = cluster.nodes.slice(0, 2);
  const tenant = await cluster.tenant('lease-load');
  // The rendezvous homes of idle-5 and idle-2 are node-2 and node-1 respectively.
  const workers = ['idle-5', 'idle-2'];
  const urls = workers.map((worker, i) => `${cluster.urls[nodes[i]]}/${tenant.name}/${worker}`);
  for (const [i, worker] of workers.entries()) {
    await cluster.deploy(tenant, worker, source);
    await eventually(`${worker} is deployed`, async () => {
      assert.deepEqual(await request(urls[i], ''), { status: 200, text: 'ready' });
    });
  }
  const ticksPerSecond = Number(await cluster.compose(['exec', '-T', nodes[0], 'getconf', 'CLK_TCK']));
  assert.ok(ticksPerSecond > 0);
  const idleStart = await sample(cluster, nodes);
  await delay(10_000, undefined, { signal: cluster.signal });
  const idleEnd = await sample(cluster, nodes);

  const clients = [];
  let readyCount = 0;
  let closing = false;
  let bad = 0;
  const open = url => new Promise((resolve, reject) => {
    const client = get(url + '/idle', { agent: false }, response => {
      response.on('error', reject);
      response.once('data', chunk => {
        if (response.statusCode !== 200 || !chunk.toString().startsWith('data: ready')) {
          reject(new Error(`${url} returned ${response.statusCode}: ${chunk.toString().slice(0, 200)}`));
          return;
        }
        readyCount++;
        resolve();
      });
      response.on('close', () => { if (!closing) bad++; });
    });
    client.setTimeout(20_000, () => client.destroy(new Error(`${url} did not start its stream`)));
    client.once('response', () => client.setTimeout(0));
    client.on('error', reject);
    clients.push(client);
  });
  const activeCount = 10_000;
  const abortClients = () => { for (const client of clients) client.destroy(); };
  cluster.signal.addEventListener('abort', abortClients, { once: true });
  try {
    for (let offset = 0; offset < activeCount; offset += 100) {
      cluster.signal.throwIfAborted();
      await Promise.all(Array.from({ length: 100 }, (_, i) => open(urls[(offset + i) % urls.length])));
    }
    assert.equal(readyCount, activeCount);
    const activeStart = await sample(cluster, nodes);
    await delay(20_000, undefined, { signal: cluster.signal });
    const activeEnd = await sample(cluster, nodes);
    assert.equal(bad, 0, 'every idle stream survived the sample');
    for (const measured of [...activeStart, ...activeEnd]) {
      assert.equal(measured.outgoing, 5000, `${measured.node} held 5000 forwarded clients`);
      assert.equal(measured.executing, 5000, `${measured.node} executed 5000 idle calls`);
    }
    const baseline = rates(idleStart, idleEnd, ticksPerSecond);
    const connected = rates(activeStart, activeEnd, ticksPerSecond);
    for (const [i, node] of connected.entries()) {
      assert.ok(node.sentMessagesPerSecond <= baseline[i].sentMessagesPerSecond + 10,
        `${node.node} generated traffic proportional to idle requests`);
    }
    console.log(JSON.stringify({
      connectedRequests: activeCount,
      baseline, connected,
    }));
  } finally {
    closing = true;
    cluster.signal.removeEventListener('abort', abortClients);
    abortClients();
  }
}

import assert from 'node:assert/strict';
import { eventually, json, request } from './cluster.mjs';

export async function isolateBusy(cluster) {
  const tenant = await cluster.tenant('isolate-busy');
  const workerId = `${tenant.name}/reading`;
  await cluster.deploy(tenant, 'reading', `export default { async fetch(request) {
    if (new URL(request.url).searchParams.has('wait')) await scheduler.wait(8000);
    return Response.json({ nodeId: request.cf.nodeId });
  } };`);

  const ingress = cluster.urls[cluster.nodes[0]];
  const call = async (query = '') =>
    json(await request(ingress, `/${workerId}${query}`, { timeout: 15000 }), 200, 'stateless Worker request');
  const { nodeId } = await call();
  assert.ok(cluster.nodes.includes(nodeId));

  const read = async () => {
    const { processes } = await cluster.state(nodeId);
    const [process] = processes.entries.filter(entry => entry.processClass === 'ordinary');
    const row = process?.lifecycle?.residentWorkers.find(row => row.workerId === workerId && row.versionId === 'v1');
    assert.ok(row?.isolateBusy, `missing isolate busy reading for ${workerId} on ${nodeId}`);
    const { isolateBusy: busy } = row;
    assert.equal(busy.windowMs, 3000);
    assert.ok(Number.isSafeInteger(busy.throughMonotonicMs) && busy.throughMonotonicMs > 0);
    assert.ok(Number.isSafeInteger(busy.busyNs) && busy.busyNs >= 0 && busy.busyNs <= busy.windowMs * 1e6);
    assert.equal(busy.permille, Math.floor(busy.busyNs / (busy.windowMs * 1000)));
    return {
      busy, active: row.active,
      identity: [process.processId, process.generation, row.planRevision, row.instance],
    };
  };
  const warm = await eventually('ordinary isolate lifecycle reading', read, { timeout: 10000, interval: 100 });

  const waiting = call('?wait');
  waiting.catch(() => {}); // Report failures when joining the request below.
  try {
    const quiet = await eventually('a full idle window during an outstanding request', async () => {
      const reading = await read();
      assert.deepEqual(reading.identity, warm.identity);
      assert.ok(reading.active > 0);
      assert.ok(reading.busy.throughMonotonicMs >= warm.busy.throughMonotonicMs + warm.busy.windowMs);
      assert.equal(reading.busy.busyNs, 0);
      return reading.busy;
    }, { timeout: 7000, interval: 100 });

    assert.equal((await call()).nodeId, nodeId);
    const busy = await eventually('a warm request adds isolate hold time', async () => {
      const reading = await read();
      assert.deepEqual(reading.identity, warm.identity);
      assert.ok(reading.busy.throughMonotonicMs > quiet.throughMonotonicMs);
      assert.ok(reading.busy.busyNs > 0);
      return reading.busy;
    }, { timeout: 3000, interval: 50 });
    console.log(`Isolate hold time on ${nodeId} for ${workerId}: ${quiet.busyNs} ns awaiting, ${busy.busyNs} ns after a warm request`);
  } finally {
    assert.equal((await waiting).nodeId, nodeId);
  }
}

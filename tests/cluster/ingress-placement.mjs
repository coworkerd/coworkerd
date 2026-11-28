import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { eventually, json, request } from './cluster.mjs';

async function checkIngressPlacement(cluster, enabled) {
  const tenant = await cluster.tenant('ingress-placement');
  const worker = 'where';
  const workerId = `${tenant.name}/${worker}`;
  await cluster.deploy(tenant, worker, `export default { fetch(request) {
    return Response.json({ nodeId: request.cf.nodeId });
  } };`);

  const home = enabled && await eventually('all ingresses agree on the Worker home', async () => {
    const homes = await Promise.all(cluster.nodes.map(async ingress => {
      const response = await request(cluster.urls[ingress], `/__coworkerd/placement?workers=${workerId}`,
        { token: cluster.management });
      const [placement] = json(response, 200, `placement at ${ingress}`);
      assert.deepEqual([...placement.candidates].sort(), cluster.nodes, `${ingress} sees every candidate`);
      return placement.home;
    }));
    assert.ok(cluster.nodes.includes(homes[0]));
    assert.ok(homes.every(node => node === homes[0]), `homes in ingress order: ${homes.join(', ')}`);
    return homes[0];
  });

  const executed = Object.fromEntries(await Promise.all(cluster.nodes.map(async ingress => {
    const response = await request(cluster.urls[ingress], `/${workerId}`);
    const { nodeId } = json(response, 200, `Worker request through ${ingress}`);
    return [ingress, nodeId];
  })));
  console.log(`Worker execution by ingress: ${JSON.stringify(executed)}`);
  const expected = Object.fromEntries(cluster.nodes.map(ingress => [ingress, home || ingress]));
  assert.deepEqual(executed, expected);
}

export const ingressPlacementDefault = cluster => checkIngressPlacement(cluster, false);
export const ingressPlacementEnabled = cluster => checkIngressPlacement(cluster, true);

async function saturationWorker(cluster, source) {
  const tenant = await cluster.tenant('ingress-saturation');
  const [home, ingress] = cluster.nodes;
  const names = Array.from({ length: 32 }, (_, i) => `where-${i}`);
  const placements = await eventually('the ingress sees all candidates', async () => {
    const path = `/__coworkerd/placement?workers=${names.map(name => `${tenant.name}/${name}`).join(',')}`;
    const rows = json(await request(cluster.urls[ingress], path, { token: cluster.management }), 200, 'placement');
    assert.deepEqual([...rows[0].candidates].sort(), cluster.nodes);
    return rows;
  });
  const index = placements.findIndex(row => row.home === home);
  assert.notEqual(index, -1, `one of the fixture names has home ${home}`);
  const name = names[index];
  const workerId = `${tenant.name}/${name}`;
  await cluster.deploy(tenant, name, source ?? `export default { async fetch(request) {
    if (new URL(request.url).searchParams.has('hold')) await new Promise(resolve => setTimeout(resolve, 8000));
    return Response.json({ nodeId: request.cf.nodeId });
  } };`);
  const call = (query = '') => request(cluster.urls[ingress], `/${workerId}${query}`, { timeout: 15000 });
  return { workerId, call };
}

async function placementLogs(cluster, workerId, tag) {
  const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', ...cluster.nodes]);
  return logs.split('\n').filter(line => line.startsWith(`[${tag}] `))
    .map(line => JSON.parse(line.slice(tag.length + 3)))
    .filter(row => row.target?.startsWith(`${workerId}@`));
}

function assertSoftRefusal(attempt, nodeId, code) {
  assert.equal(attempt.nodeId, nodeId);
  assert.equal(attempt.saturationFallback, false);
  assert.deepEqual({
    code: attempt.failure?.code, dispatched: attempt.failure?.dispatched, retryable: attempt.failure?.retryable,
  }, { code, dispatched: false, retryable: true });
}

export async function ingressPlacementSaturation(cluster) {
  const [home, ingress, idle] = cluster.nodes;
  const { workerId, call } = await saturationWorker(cluster);
  assert.equal(json(await call(), 200, 'call finds an idle candidate').nodeId, idle);
  const attempts = (await placementLogs(cluster, workerId, 'attempt')).sort((a, b) => a.attempt - b.attempt);
  assert.equal(attempts.length, 3);
  assertSoftRefusal(attempts[0], home, 'WORKER_SATURATED');
  assertSoftRefusal(attempts[1], ingress, 'NODE_CPU_SATURATED');
  assert.equal(attempts[2].nodeId, idle);
  assert.equal(attempts[2].failure, null);
  const executions = await placementLogs(cluster, workerId, 'execution');
  assert.deepEqual(executions.map(row => [row.treeId, row.nodeId]), [[attempts[0].treeId, idle]]);
  console.log(`Busy isolate on ${home}, full CPU on ${ingress}, executed once on ${idle}`);

  // The ingress has a live peer connection from the successful call. Pause the receiver after
  // that connection was established so the next open goes unanswered.
  await cluster.compose(['pause', idle]);
  try {
    const uncertain = json(await call(), 503, 'an unanswered peer open ends placement');
    assert.equal(uncertain.code, 'RESULT_UNCERTAIN');
    const additional = (await placementLogs(cluster, workerId, 'attempt'))
      .filter(row => row.treeId !== attempts[0].treeId).sort((a, b) => a.attempt - b.attempt);
    assert.equal(additional.length, 3);
    assertSoftRefusal(additional[0], home, 'WORKER_SATURATED');
    assertSoftRefusal(additional[1], ingress, 'NODE_CPU_SATURATED');
    assert.equal(additional[2].nodeId, idle);
    assert.equal(additional[2].failure?.code, 'RESULT_UNCERTAIN');
    assert.equal(additional[2].retryable, false);
  } finally {
    await cluster.compose(['unpause', idle]);
  }
}

export async function ingressPlacementAllBusy(cluster) {
  const [home, ingress, peer] = cluster.nodes;
  const { workerId, call } = await saturationWorker(cluster);
  const readRoots = () => Promise.all(cluster.nodes.map(async node =>
    (await cluster.state(node)).placement.inFlight.roots));
  for (const node of [home, ingress, peer]) {
    assert.equal(json(await call(), 200, 'all-busy fallback with tied load').nodeId, node);
    await eventually('every root slot was released', async () => assert.deepEqual(await readRoots(), [0, 0, 0]));
  }

  const attempts = await placementLogs(cluster, workerId, 'attempt');
  const firstPass = [[home, 'WORKER_SATURATED'], [ingress, 'NODE_CPU_SATURATED'], [peer, 'WORKER_SATURATED']];
  const fallbacks = attempts.filter(row => row.saturationFallback);
  assert.equal(fallbacks.length, cluster.nodes.length);
  for (const fallback of fallbacks) {
    const probes = attempts.filter(row => row.treeId === fallback.treeId && !row.saturationFallback)
      .sort((a, b) => a.attempt - b.attempt);
    assert.equal(probes.length, firstPass.length);
    firstPass.forEach(([node, code], i) => assertSoftRefusal(probes[i], node, code));
    assert.equal(fallback.failure, null);
  }
  const executions = await placementLogs(cluster, workerId, 'execution');
  assert.deepEqual(new Map(executions.map(row => [row.treeId, row.nodeId])),
    new Map(fallbacks.map(row => [row.treeId, row.nodeId])));
  assert.equal(executions.length, cluster.nodes.length, 'each root executes once');

  const held = [];
  try {
    for (let count = 1; count <= 6; ++count) {
      const response = call('?hold');
      // Keep a rejection observed while checking the slots through state.
      response.catch(() => {});
      held.push(response);
      await eventually(`fallback filled ${count} root slots evenly`, async () => {
        const roots = await readRoots();
        assert.equal(roots.reduce((sum, value) => sum + value, 0), count);
        assert.ok(Math.max(...roots) - Math.min(...roots) <= 1, `in-flight roots: ${roots}`);
      }, { timeout: 5000 });
    }
    const exhausted = json(await call(), 503, 'all candidates at the hard concurrency limit');
    assert.equal(exhausted.code, 'PLACEMENT_EXHAUSTED');
    assert.deepEqual(exhausted.attempts.map(attempt => attempt.failure.code),
      ['OVERLOADED', 'OVERLOADED', 'OVERLOADED']);
  } finally {
    const served = await Promise.all(held.map(async response => json(await response, 200, 'held root').nodeId));
    if (held.length === 6) assert.deepEqual(served.sort(), [...cluster.nodes, ...cluster.nodes].sort());
  }
  console.log('All-busy roots rotated on equal load, filled both slots per node, and respected hard admission');
}

export const placementStreamWorker = 'saturation-neighbor/idle-stream';

export async function ingressPlacementUnrelatedStreams(cluster) {
  const [home, ingress, peer] = cluster.nodes;
  const { call } = await saturationWorker(cluster);
  const streamCount = 24;
  const tenant = await cluster.tenant('saturation-neighbor');
  await cluster.deploy(tenant, 'idle-stream', `export default { fetch(request) {
    return new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(request.cf.nodeId));
        setTimeout(() => controller.close(), 20000);
      },
    }));
  } };`);
  const controller = new AbortController();
  const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(15000)]);
  const readRoots = async () => (await cluster.state(peer)).placement.inFlight.roots;
  try {
    await Promise.all(Array.from({ length: streamCount }, async () => {
      const response = await fetch(`${cluster.urls[peer]}/${placementStreamWorker}`, { signal });
      assert.equal(response.status, 200);
      const { value } = await response.body.getReader().read();
      assert.equal(Buffer.from(value).toString(), peer);
    }));
    assert.equal(await readRoots(), streamCount);

    for (const node of [home, ingress, peer]) {
      const response = json(await call(), 200, 'all-busy fallback with unrelated idle streams');
      assert.equal(response.nodeId, node, 'an unrelated stream does not add to the hot Worker count');
      await eventually('the hot root released its slot and all idle streams remain', async () => {
        assert.equal(await readRoots(), streamCount);
        assert.equal((await cluster.state(home)).placement.inFlight.roots, 0);
        assert.equal((await cluster.state(ingress)).placement.inFlight.roots, 0);
      });
    }
  } finally {
    controller.abort();
  }
  await eventually('idle stream aborts released all root slots', async () => assert.equal(await readRoots(), 0),
    { timeout: 8000 });
}

export async function ingressPlacementLiveSaturation(cluster) {
  const [home] = cluster.nodes;
  const { workerId, call } = await saturationWorker(cluster, `export default { async fetch(request) {
    let checksum = 1234;
    if (new URL(request.url).searchParams.has('busy')) {
      const until = Date.now() + 8500;
      do {
        for (let i = 0; i < 200_000_000; ++i)
          checksum = (Math.imul(checksum ^ i, 1664525) + 1013904223) | 0;
        await scheduler.wait(1);
      } while (Date.now() < until);
    }
    return Response.json({ nodeId: request.cf.nodeId, checksum });
  } };`);
  assert.equal(json(await call(), 200, 'warm call').nodeId, home);
  const read = async () => {
    const state = await cluster.state(home);
    const process = state.processes.entries.find(entry => entry.processClass === 'ordinary');
    const isolate = process?.lifecycle?.residentWorkers.find(row => row.workerId === workerId && row.versionId === 'v1');
    assert.ok(isolate?.isolateBusy, 'the kernel must publish the busy reading');
    return { permille: isolate.isolateBusy.permille, instance: isolate.instance,
      softRefusals: state.placement.rejections.WORKER_SATURATED ?? 0 };
  };
  const warm = await eventually('the warm isolate has a busy-time reading', read);
  const busyCall = call('?busy');
  busyCall.catch(() => {});
  try {
    const busy = await eventually('the serving isolate crosses the threshold', async () => {
      const reading = await read();
      assert.equal(reading.instance, warm.instance);
      assert.ok(reading.permille >= 900, `sample was ${reading.permille} permille`);
      return reading;
    }, { timeout: 6500, interval: 100 });

    // Let the node's once-per-second reader observe the kernel publication.
    await delay(1250);
    const { nodeId: peer } = json(await call(), 200, 'probe while the home isolate is busy');
    assert.ok(cluster.nodes.includes(peer) && peer !== home);
    assert.equal((await read()).softRefusals, busy.softRefusals + 1);
    console.log(`Live isolate on ${home} reached ${busy.permille} permille. Probe ran on ${peer}`);
  } finally {
    assert.equal(json(await busyCall, 200, 'busy call').nodeId, home);
  }
}

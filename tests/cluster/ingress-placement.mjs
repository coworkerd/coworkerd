import assert from 'node:assert/strict';
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

async function saturationWorker(cluster) {
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
  await cluster.deploy(tenant, name, `export default { async fetch(request) {
    if (new URL(request.url).searchParams.has('hold')) await new Promise(resolve => setTimeout(resolve, 8000));
    return Response.json({ nodeId: request.cf.nodeId });
  } };`);
  const call = (hold = false) => request(cluster.urls[ingress], `/${workerId}${hold ? '?hold' : ''}`, { timeout: 15000 });
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
  assert.equal(json(await call(), 200, 'all-busy fallback').nodeId, home);
  const attempts = (await placementLogs(cluster, workerId, 'attempt')).sort((a, b) => a.attempt - b.attempt);
  assert.equal(attempts.length, 4);
  assertSoftRefusal(attempts[0], home, 'WORKER_SATURATED');
  assertSoftRefusal(attempts[1], ingress, 'NODE_CPU_SATURATED');
  assertSoftRefusal(attempts[2], peer, 'WORKER_SATURATED');
  assert.equal(attempts[3].nodeId, home);
  assert.equal(attempts[3].saturationFallback, true);
  assert.equal(attempts[3].failure, null);
  const executions = await placementLogs(cluster, workerId, 'execution');
  assert.deepEqual(executions.map(row => [row.treeId, row.nodeId]), [[attempts[0].treeId, home]]);

  const held = [];
  try {
    for (const node of [home, ingress, peer]) {
      const response = call(true);
      // Keep a rejection observed while checking the slot through state.
      response.catch(() => {});
      held.push([node, response]);
      await eventually(`fallback filled the root slot on ${node}`, async () => {
        assert.equal((await cluster.state(node)).placement.inFlight.roots, 1);
      }, { timeout: 5000 });
    }
    const exhausted = json(await call(), 503, 'all candidates at the hard concurrency limit');
    assert.equal(exhausted.code, 'PLACEMENT_EXHAUSTED');
    assert.deepEqual(exhausted.attempts.map(attempt => attempt.failure.code),
      ['OVERLOADED', 'OVERLOADED', 'OVERLOADED']);
    console.log(`All-busy fallback executed once on ${home}, hard limits refused on all three nodes`);
  } finally {
    await Promise.all(held.map(async ([node, response]) => {
      assert.equal(json(await response, 200, `held call on ${node}`).nodeId, node);
    }));
  }
}

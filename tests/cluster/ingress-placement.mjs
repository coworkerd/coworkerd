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

import assert from 'node:assert/strict';
import { eventually, request } from './cluster.mjs';

function respondWith(value) {
  return `export default { fetch() { return new Response(${JSON.stringify(value)}); } };`;
}

async function deployment(cluster) {
  const tenant = await cluster.tenant('deploy-test');
  const path = `/${tenant.name}/hello`;
  for (const version of ['v1', 'v2']) {
    const expected = `hello from ${version}`;
    await cluster.deploy(tenant, 'hello', respondWith(expected), { version });
    await eventually(`${version} serves on all three ingresses`, async () => {
      const observed = await Promise.all(cluster.nodes.map(node => request(cluster.urls[node], path)));
      assert.deepEqual(Object.fromEntries(cluster.nodes.map((node, i) => [node, observed[i]])),
        Object.fromEntries(cluster.nodes.map(node => [node, { status: 200, text: expected }])));
    });
  }
}

export const cases = { deployment };

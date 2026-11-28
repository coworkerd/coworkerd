import assert from 'node:assert/strict';
import { eventually, json, request } from './cluster.mjs';

const tenantName = 'egress-quotas';
const origin = 'egress-origin:8080';
const nodeLimits = {
  'node-1': {},
  'node-2': { concurrency: 1, requestsPerSecond: 0 },
  'node-3': { concurrency: 0, requestsPerSecond: 1 },
};

export function configureEgressLimits(node) {
  return {
    egress: {
      ...nodeLimits[node],
      defaultPolicy: 'deny',
      clusterAddresses: ['apiserver:8940'],
      tenants: [{ tenant: tenantName, allowInternal: [origin], concurrency: 0, requestsPerSecond: 0 }],
    },
  };
}

export async function egressLimits(cluster) {
  const peer = `${cluster.project}-egress-origin`;
  await cluster.command('docker', ['run', '--detach', '--rm', '--name', peer, '--network', `${cluster.project}_backend`,
    '--network-alias', 'egress-origin', 'node:22-trixie', 'node', '-e', `
      require('node:http').createServer((req, res) => {
        if (req.url === '/ready') return res.end('ready');
        setTimeout(() => res.end('origin'), 2000);
      }).listen(8080, '0.0.0.0');
    `]);
  try {
    await eventually('private upstream ready', async () => {
      const ready = await cluster.command('docker', ['exec', peer, 'node', '-e', `
        fetch('http://127.0.0.1:8080/ready').then(async response => console.log(await response.text()));
      `]);
      assert.equal(ready, 'ready');
    });

    const tenant = await cluster.tenant(tenantName);
    await cluster.deploy(tenant, 'probe', `export default { async fetch(request) {
      const run = async url => {
        try { return await (await fetch(url)).text(); }
        catch (error) { return error.message; }
      };
      const nodeId = request.cf.nodeId;
      if (new URL(request.url).searchParams.has('blocked')) {
        return Response.json({ nodeId, result: await run('http://127.0.0.2:8080/') });
      }
      const result = await Promise.all(Array.from({ length: 3 }, () => run('http://${origin}/')));
      return Response.json({ nodeId, result });
    } };`);

    const call = async (node, query = '') =>
      json(await request(cluster.urls[node], `/${tenant.name}/probe${query}`, { timeout: 10000 }), 200, 'egress probe');
    const scenarios = [
      ['node-1', ['origin', 'origin', 'origin'], {}],
      ['node-2', ['EGRESS_DENIED: concurrency-limit', 'EGRESS_DENIED: concurrency-limit', 'origin'],
        { 'concurrency-limit': 2 }],
      ['node-3', ['EGRESS_DENIED: rate-limit', 'EGRESS_DENIED: rate-limit', 'origin'], { 'rate-limit': 2 }],
    ];
    for (const [node, expected, denied] of scenarios) {
      const response = await call(node);
      assert.equal(response.nodeId, node);
      assert.deepEqual(response.result.sort(), expected);
      const { egress } = await cluster.state(node);
      assert.equal(egress.concurrency, nodeLimits[node].concurrency ?? 0);
      assert.equal(egress.requestsPerSecond, nodeLimits[node].requestsPerSecond ?? 0);
      const counts = egress.tenants[tenantName];
      assert.equal(counts.maxInFlight, node === 'node-1' ? 3 : 1);
      assert.deepEqual(counts.denied, denied);
    }
    assert.deepEqual(await call('node-1', '?blocked'), { nodeId: 'node-1', result: 'EGRESS_DENIED: loopback' });
  } finally {
    await cluster.command('docker', ['stop', '--time', '2', peer], { cleanup: true });
  }
}

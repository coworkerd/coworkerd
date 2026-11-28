import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [mode, productArg, kernelArg, reportArg, iterationsArg, hotRateArg, option, ...extra] = process.argv.slice(2);
const iterations = Number(iterationsArg);
const hotRate = Number(hotRateArg);
const calibrate = mode === 'calibrate';
const longRun = option === '--long';
assert.ok(['dynamic', 'pinned', 'calibrate'].includes(mode) && productArg && kernelArg && reportArg &&
  Number.isSafeInteger(iterations) && iterations > 0 && iterations <= 500_000_000 && !extra.length &&
  (option === undefined || (!calibrate && longRun)) &&
  (calibrate ? hotRateArg === undefined : Number.isInteger(hotRate) && hotRate > 0 && hotRate <= 100),
  'Usage: node tests/cluster/hot-worker-placement.mjs <dynamic|pinned|calibrate> <product-checkout> ' +
  '<verified-kernel-directory> <new-report.jsonl> <iterations> [hot-requests-per-second [--long]]');

const productRoot = resolve(productArg);
const kernel = resolve(kernelArg);
const report = resolve(reportArg);
const pinned = mode !== 'dynamic';
const home = 'node-3';
const hot = 'hot-placement/compute';
const light = 'hot-neighbor/light';
const clientImage = 'node:22-trixie';
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}

function checksum() {
  let value = 1234;
  for (let i = 0; i < iterations; ++i) value = (Math.imul(value ^ i, 1664525) + 1013904223) | 0;
  return value;
}

await writeFile(report, '', { flag: 'wx' });
const record = value => appendFile(report, JSON.stringify(value) + '\n');
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-hot-placement-')), kernel, controller.signal, {
  productRoot,
  configureNode: () => ({
    ingressPlacement: true,
    // This enables per-tenant slot accounting. The fixture sends only roots and the
    // tenant ceiling for nested calls is the same as the node-wide nested ceiling.
    placement: { rootLimit: 128, nestedLimit: 384, homeProbability: 1, ...(longRun && { tenantNestedLimit: 384 }) },
    ...(pinned && { placements: [{ workerId: hot, nodeId: home }] }),
  }),
});

async function waitForHomes() {
  await eventually('all ingresses agree that both Workers have the same home', async () => {
    for (const node of cluster.nodes) {
      const path = `/__coworkerd/placement?workers=${hot},${light}`;
      const rows = json(await request(cluster.urls[node], path, { token: cluster.management }), 200, 'Worker homes');
      assert.equal(rows.length, 2);
      for (const row of rows) {
        assert.equal(row.home, home);
        assert.deepEqual([...row.candidates].sort(), cluster.nodes);
      }
    }
  });
}

async function measure() {
  const pins = JSON.parse(await readFile(join(productRoot, 'patches/workerd/artifacts.json'), 'utf8'));
  const manifest = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
  const sha256 = async path => createHash('sha256').update(await readFile(path)).digest('hex');
  assert.equal(manifest.patchSeriesSha256, pins.patchSeriesSha256);
  assert.ok(pins.archives[manifest.architecture], 'the product must pin a published archive for this architecture');
  const kernels = ['workerd', 'coworkerd-threaded'];
  const kernelHashes = Object.fromEntries(await Promise.all(kernels.map(async name => [name, await sha256(join(kernel, name))])));
  for (const name of kernels) assert.equal(kernelHashes[name], manifest.files[name]);
  const binaries = resolve(process.env.COWORKERD_CLUSTER_BIN_DIR ?? join(productRoot, 'build'));
  await record({ metric: 'provenance', mode, longRun, iterations, hotRate: calibrate ? null : hotRate, home,
    commit: await cluster.command('git', ['rev-parse', 'HEAD'], { cwd: productRoot }),
    cliVersion: JSON.parse(await readFile(join(productRoot, 'cli/package.json'), 'utf8')).version,
    productBinarySha256: await sha256(join(binaries, 'coworkerd')),
    kernelRelease: pins.releaseTag, archiveSha256: pins.archives[manifest.architecture], kernelHashes,
    architecture: manifest.architecture, patchSeriesSha256: pins.patchSeriesSha256 });
  await cluster.start();
  await waitForHomes();
  const hotTenant = await cluster.tenant('hot-placement');
  const lightTenant = await cluster.tenant('hot-neighbor');
  await cluster.deploy(hotTenant, 'compute', `
    const iterations = ${iterations};
    ${checksum.toString()}
    export default { fetch(request) {
      const id = new URL(request.url).searchParams.get('id');
      return Response.json({ nodeId: request.cf.nodeId, id, checksum: checksum() });
    } };
  `);
  await cluster.deploy(lightTenant, 'light', `export default { fetch(request) {
    return Response.json({ nodeId: request.cf.nodeId });
  } };`);

  const cpu = Object.fromEntries(await Promise.all(cluster.nodes.map(async node => {
    const details = await cluster.compose(['exec', '-T', node, 'sh', '-ec',
      'cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpuset.cpus.effective; grep ^Cpus_allowed_list: /proc/self/status']);
    const [quota, cpuset, affinity] = details.split('\n');
    return [node, { quota, cpuset, affinity: affinity.split(':')[1].trim() }];
  })));
  await record({ metric: 'node-cpu', nodes: cpu });
  const daemonVersion = await cluster.compose(['exec', '-T', home, 'coworkerd', '--version']);
  await record({ metric: 'daemon-version', lines: daemonVersion.split('\n') });
  const input = join(cluster.directory, 'placement-measure.json');
  await writeFile(input, JSON.stringify({ home, hot, light, hotRate: calibrate ? null : hotRate, calibrate, longRun, checksum: checksum(),
    nodes: cluster.nodes, management: cluster.management }), { mode: 0o600 });
  const client = fileURLToPath(new URL('./hot-worker-placement-client.mjs', import.meta.url));
  const result = await cluster.command('docker', ['run', '--rm', '--network', `${cluster.project}_frontend`,
    '-v', `${input}:/measure.json:ro`, '-v', `${client}:/client.mjs:ro`, clientImage, 'node', '/client.mjs'],
  { timeout: 420000, maxBuffer: 8 * 1024 * 1024 });
  for (const line of result.split('\n')) {
    const row = JSON.parse(line);
    await record(row);
    delete row.hot;
    delete row.light;
    console.log(JSON.stringify(row));
  }
}

try {
  await measure();
} catch (error) {
  await cluster.diagnostics('hot-worker-placement', error);
  throw error;
} finally {
  await cluster.close();
}

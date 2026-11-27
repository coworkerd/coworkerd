import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [kernelArg, reportArg, ...extra] = process.argv.slice(2);
assert.ok(kernelArg && reportArg && extra.length === 0,
  'Usage: node tests/cluster/durable-idle-measure.mjs <verified-kernel-directory> <new-report.jsonl>');
assert.equal(process.platform, 'linux');
const root = fileURLToPath(new URL('../../', import.meta.url));
const kernel = resolve(kernelArg);
const report = resolve(reportArg);
const cohortSize = 5000;
const batchSize = 250;
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}
await writeFile(report, '', { flag: 'wx' });
const record = (metric, fields) => appendFile(report, JSON.stringify({ metric, at: new Date().toISOString(), ...fields }) + '\n');
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-durable-idle-')), kernel, controller.signal, {
  singleNode: true, threadedNodes: ['node-1'],
  configureNode: () => ({ durableObjects: { processes: 1, peers: 0, threadedStorage: true } }),
});
const [node] = cluster.nodes;

const worker = `
  import { DurableObject } from 'cloudflare:workers';
  let moduleId;
  export class Record extends DurableObject {
    actorId = crypto.randomUUID();
    async fetch(request) {
      moduleId ??= crypto.randomUUID();
      if (request.method === 'POST') await this.ctx.storage.put('saved', await request.json());
      return Response.json({ actorId: this.actorId, moduleId, value: await this.ctx.storage.get('saved') });
    }
  }
  export default { fetch(request, env) {
    const id = new URL(request.url).pathname.split('/').at(-1);
    return env.RECORDS.get(env.RECORDS.idFromString(id)).fetch(request);
  } };
`;

function summary(rows) {
  const times = rows.filter(row => !row.error).map(row => row.ms).toSorted((a, b) => a - b);
  return { successes: times.length, errors: rows.length - times.length,
    p50Ms: times[Math.ceil(0.50 * times.length) - 1] ?? null,
    p99Ms: times[Math.ceil(0.99 * times.length) - 1] ?? null };
}

async function measure() {
  const pins = JSON.parse(await readFile(join(root, 'patches/workerd/artifacts.json'), 'utf8'));
  const manifest = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
  assert.equal(manifest.patchSeriesSha256, pins.patchSeriesSha256);
  const binaryHash = createHash('sha256').update(await readFile(join(kernel, 'coworkerd-threaded'))).digest('hex');
  assert.equal(binaryHash, manifest.files['coworkerd-threaded']);
  const run = (program, args) => cluster.command(program, args, { cwd: root });
  await record('provenance', { commit: await run('git', ['rev-parse', 'HEAD']),
    kernelRelease: pins.releaseTag, archiveSha256: pins.archives[manifest.architecture],
    architecture: manifest.architecture, binaryHash, os: await run('uname', ['-a']),
    cpu: await run('lscpu', []), node: process.version, cohorts: 2, cohortSize, batchSize, concurrency: 1,
    placements: 'One durable child, one shard, no peers' });
  await cluster.start();
  const tenant = await cluster.tenant('acceptance-idle');
  await cluster.deploy(tenant, 'objects', worker, { wrangler: {
    durable_objects: { bindings: [{ name: 'RECORDS', class_name: 'Record' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record'] }],
  } });
  const objectId = index => index.toString(16).padStart(64, '0');
  const call = async (index, value) => {
    const path = `/${tenant.name}/objects/${objectId(index)}`;
    return json(await request(cluster.urls[node], path, value === undefined
      ? { timeout: 7000 } : { method: 'POST', body: value, timeout: 7000 }), 200, path);
  };
  await eventually('deployed record', () => call(0, 'warm'));
  const state = async () => {
    const { processes } = await cluster.state(node);
    const slots = processes.entries.filter(slot => slot.processClass === 'durable');
    assert.equal(slots.length, 1);
    const { processId, pid, generation, binarySha256, sandbox, lifecycle, state: status } = slots[0];
    assert.ok(pid > 0);
    assert.equal(status, 'ready');
    assert.equal(binarySha256, binaryHash);
    assert.equal(sandbox.durableServer, 'threaded');
    assert.ok(lifecycle?.durableAdmission);
    return { child: { processId, pid, generation }, admission: lifecycle.durableAdmission };
  };
  const initial = await eventually('the baseline actor is resident in the durable process', async () => {
    const reading = await state();
    assert.equal(reading.admission.residentRoots, 1);
    return reading;
  });
  await record('initial', initial);
  const onSameChild = async () => {
    const reading = await state();
    assert.deepEqual(reading.child, initial.child);
    return reading.admission;
  };
  const idle = async () => {
    const admission = await onSameChild();
    assert.equal(admission.residentRoots, 0);
    assert.equal(admission.homeSessions, 0);
    assert.equal(admission.running, 0);
    assert.equal(admission.waiting, 0);
    return admission;
  };
  let retirements = initial.admission.idleRetirements;
  const resources = async phase => {
    const retirement = await eventually(phase + ' retirement', async () => {
      const reading = await idle();
      assert.ok(reading.idleRetirements > retirements);
      return reading;
    }, { timeout: 35000 });
    retirements = retirement.idleRetirements;
    const samples = [];
    for (let i = 0; i < 6; ++i) {
      const before = await idle();
      const status = await cluster.compose(['exec', '-T', '--user', '0', node, 'cat', `/proc/${initial.child.pid}/status`]);
      const fds = await cluster.compose(['exec', '-T', '--user', '0', node, 'ls', '-1', `/proc/${initial.child.pid}/fd`]);
      await idle();
      const threadMatch = status.match(/^Threads:\s+(\d+)$/m);
      const rssMatch = status.match(/^VmRSS:\s+(\d+) kB$/m);
      assert.ok(threadMatch && rssMatch);
      const values = { threads: Number(threadMatch[1]), rssKiB: Number(rssMatch[1]),
        descriptors: fds.split('\n').filter(Boolean).length };
      samples.push(values);
      await record('resources', { phase, index: i, child: initial.child, admission: before, ...values });
      if (i < 5) await delay(2000);
    }
    await record('idle-phase', { phase, retirement, child: initial.child, samples,
      uptime: await run('uptime', []), memory: await run('free', ['-m']) });
    return samples;
  };
  const baseline = await resources('warmed-baseline');
  const finish = [];
  let workloadPassed = true;
  for (const cohort of [1, 2]) {
    const accepted = new Map();
    for (const phase of ['seed', 'reopen']) {
      const samples = [];
      for (let from = 0; from < cohortSize; from += batchSize) {
        controller.signal.throwIfAborted();
        await onSameChild();
        const batch = [];
        for (let offset = from; offset < Math.min(from + batchSize, cohortSize); ++offset) {
          const index = (cohort - 1) * cohortSize + offset + 1;
          const value = 'cohort-' + cohort + '-object-' + index;
          const start = performance.now();
          try {
            const result = await call(index, phase === 'seed' ? value : undefined);
            const ms = performance.now() - start;
            assert.equal(result.value, value);
            assert.match(result.actorId, /^[a-f0-9-]{36}$/);
            if (phase === 'seed') accepted.set(index, result.actorId);
            else {
              assert.ok(accepted.has(index), 'the original write was confirmed');
              assert.notEqual(result.actorId, accepted.get(index), 'the actor was reconstructed');
            }
            batch.push({ index, ms, ...result, ...(phase === 'reopen' && { previousActorId: accepted.get(index) }) });
          } catch (error) {
            batch.push({ index, ms: performance.now() - start, error: error.message });
          }
        }
        samples.push(...batch);
        await record('object-batch', { cohort, phase, child: initial.child, from, samples: batch });
      }
      const result = summary(samples);
      if (result.successes !== cohortSize) workloadPassed = false;
      await record('workload-summary', { cohort, phase, ...result });
      console.log(JSON.stringify({ cohort, phase, ...result }));
      finish.push({ cohort, phase, samples: await resources(`cohort-${cohort}-after-${phase}`) });
    }
  }

  const rss = baseline.map(sample => sample.rssKiB);
  const rssVariationKiB = Math.max(...rss) - Math.min(...rss);
  const baselineStable = baseline.every(sample => sample.threads === baseline[0].threads &&
    sample.descriptors === baseline[0].descriptors);
  const comparisons = finish.map(({ cohort, phase, samples }) => {
    const threadDelta = samples.map(sample => sample.threads - baseline[0].threads);
    const descriptorDelta = samples.map(sample => sample.descriptors - baseline[0].descriptors);
    const rssDeltaFromBaselineMaxKiB = samples.map(sample => sample.rssKiB - Math.max(...rss));
    const pass = baselineStable && threadDelta.every(delta => delta === 0) &&
      descriptorDelta.every(delta => delta === 0) && rssDeltaFromBaselineMaxKiB.every(delta => delta <= 0);
    return { cohort, phase, threadDelta, descriptorDelta, rssDeltaFromBaselineMaxKiB, pass };
  });
  const pass = workloadPassed && comparisons.every(comparison => comparison.pass);
  await record('gate-summary', { child: initial.child, baselineStable, rssVariationKiB,
    workloadPassed, comparisons, pass });
  console.log(JSON.stringify({ baselineStable, rssVariationKiB, workloadPassed, comparisons, pass }));
}

try {
  await measure();
} catch (error) {
  await record('measurement-error', { error: error.stack });
  await cluster.diagnostics('idle-measurement', error);
  throw error;
} finally {
  await cluster.close();
}

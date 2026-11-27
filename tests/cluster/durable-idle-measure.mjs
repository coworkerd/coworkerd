import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, parse, resolve } from 'node:path';
import { setInterval as interval, setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [kernelArg, reportArg, mode, ...extra] = process.argv.slice(2);
assert.ok(kernelArg && reportArg && [undefined, '--sequential', '--residency'].includes(mode) && !extra.length,
  'Usage: node tests/cluster/durable-idle-measure.mjs <verified-kernel-directory> <new-report.jsonl> [--sequential|--residency]');
const residentTarget = 4096;
const residencyConcurrency = 48;
assert.equal(process.platform, 'linux');
const root = fileURLToPath(new URL('../../', import.meta.url));
const kernel = resolve(kernelArg);
const report = resolve(reportArg);
const cohortSize = 5000;
const batchSize = 250;
const residencyPollMs = 50;
const offerPeriodMs = Number(process.env.COWORKERD_ACCEPTANCE_IDLE_PERIOD_MS ?? 0);
assert.ok(Number.isFinite(offerPeriodMs) && offerPeriodMs >= 0);
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}
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

async function measureResidency(cluster, node, record, call, onSameChild, child) {
  const initial = await onSameChild();
  assert.ok(Number.isSafeInteger(initial.objectSqliteConnections),
    'the kernel must publish the ordinary SQLite connection count');
  const samples = [];
  let peak;
  const started = performance.now();
  const sample = async () => {
    const admission = await onSameChild();
    const row = { elapsedMs: performance.now() - started, ...admission };
    samples.push(row);
    const simultaneous = Math.min(row.residentRoots, row.objectSqliteConnections);
    if (!peak || simultaneous > Math.min(peak.residentRoots, peak.objectSqliteConnections)) {
      peak = row;
    }
  };
  await sample();
  const stopSampling = new AbortController();
  const sampling = (async () => {
    try {
      for await (const _ of interval(residencyPollMs, undefined, { signal: stopSampling.signal })) {
        await sample();
      }
    } catch (error) {
      if (error.name !== 'AbortError') throw error;
    }
  })();
  // Mark the rejection handled while the requests settle. The finally block still observes it.
  void sampling.catch(() => {});
  const rounds = [];
  const errors = [];
  const copies = new Set();
  try {
    // A bounded rolling load keeps earlier opens within the normal idle timeout. A successful
    // run needs a single publication with both full counts, regardless of total replies.
    for (let round = 1; round <= 3; ++round) {
      let next = 0;
      let successes = 0;
      const roundStarted = performance.now();
      await Promise.all(Array.from({ length: residencyConcurrency }, async () => {
        for (;;) {
          controller.signal.throwIfAborted();
          const index = next++;
          if (index >= residentTarget) return;
          try {
            const result = await call(index);
            assert.match(result.actorId, /^[a-f0-9-]{36}$/);
            copies.add(result.moduleId);
            ++successes;
          } catch (error) {
            errors.push({ round, index, error: error.message });
          }
        }
      }));
      rounds.push({ round, successes, errors: residentTarget - successes,
        elapsedMs: performance.now() - roundStarted });
      await sample();
      if (peak.residentRoots === residentTarget && peak.objectSqliteConnections === residentTarget) break;
    }
  } finally {
    stopSampling.abort();
    await sampling;
  }
  const limits = await cluster.compose(['exec', '-T', '--user', '0', node, 'cat', `/proc/${child.pid}/limits`]);
  const result = { child, residentTarget, rounds, copies: copies.size, peak, limits, errors };
  await record('simultaneous-residency-samples', { child, samples });
  await record('simultaneous-residency-summary', result);
  console.log(JSON.stringify({ ...result, errors: errors.length }));
  assert.equal(errors.length, 0, 'each offered residency probe succeeded');
  assert.equal(peak.residentRoots, residentTarget, 'simultaneously resident ordinary root trees');
  assert.equal(peak.objectSqliteConnections, residentTarget, 'simultaneously open ordinary SQLite connections');
  assert.ok(peak.sqliteHeapReservedBytes > 0, 'the live SQLite heap was charged');
}

async function measure(cluster, node, record, residencyOnly) {
  const pins = JSON.parse(await readFile(join(root, 'patches/workerd/artifacts.json'), 'utf8'));
  const manifest = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
  assert.equal(manifest.patchSeriesSha256, pins.patchSeriesSha256);
  const binaryHash = createHash('sha256').update(await readFile(join(kernel, 'coworkerd-threaded'))).digest('hex');
  assert.equal(binaryHash, manifest.files['coworkerd-threaded']);
  const run = (program, args) => cluster.command(program, args, { cwd: root });
  await record('provenance', { commit: await run('git', ['rev-parse', 'HEAD']),
    kernelRelease: pins.releaseTag, archiveSha256: pins.archives[manifest.architecture],
    architecture: manifest.architecture, binaryHash, os: await run('uname', ['-a']),
    cpu: await run('lscpu', []), node: process.version,
    ...(residencyOnly ? { residentTarget, concurrency: residencyConcurrency }
      : { cohorts: 2, cohortSize, batchSize, concurrency: 1, offerPeriodMs }),
    residencyPollMs, placements: 'One durable child, one shard, no peers' });
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
  const first = await eventually('deployed record', () => call(0));
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
    assert.ok(reading.admission.workerThreads > 0);
    return reading;
  });
  await record('initial', initial);
  const onSameChild = async () => {
    const reading = await state();
    assert.deepEqual(reading.child, initial.child);
    return reading.admission;
  };
  if (residencyOnly) {
    return measureResidency(cluster, node, record, call, onSameChild, initial.child);
  }
  // Placement packs live trees into an existing copy first. Open read-only
  // warmup IDs outside both measured cohorts to fill every home with this Worker.
  const expectedCopies = initial.admission.workerThreads;
  const warmCopies = new Map([[first.moduleId, 1]]);
  let warmupCalls = 1;
  const warmupBatchSize = Math.min(8, expectedCopies);
  while (warmCopies.size < expectedCopies && warmupCalls < cohortSize) {
    controller.signal.throwIfAborted();
    const size = Math.min(warmupBatchSize, cohortSize - warmupCalls);
    const probes = await Promise.all(Array.from({ length: size }, (_, offset) =>
      call(2 * cohortSize + warmupCalls + offset)));
    for (const { moduleId } of probes) {
      warmCopies.set(moduleId, (warmCopies.get(moduleId) ?? 0) + 1);
    }
    warmupCalls += size;
  }
  assert.equal(warmCopies.size, expectedCopies, 'every home opened a copy of the measured Worker');
  const warmed = await eventually('all copies appear in the same durable child', async () => {
    const admission = await onSameChild();
    assert.equal(admission.homes, expectedCopies);
    assert.ok(admission.residentRoots > 0);
    return admission;
  });
  await record('warmup', { child: initial.child, calls: warmupCalls,
    copies: Object.fromEntries(warmCopies), admission: warmed });

  const idle = async () => {
    const admission = await onSameChild();
    assert.equal(admission.residentRoots, 0);
    assert.equal(admission.homeSessions, 0);
    assert.equal(admission.running, 0);
    assert.equal(admission.waiting, 0);
    return admission;
  };
  let retirements = warmed.idleRetirements;
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
      assert.equal(before.homes, expectedCopies, 'every warmed copy remains present for the RSS sample');
      const status = await cluster.compose(['exec', '-T', '--user', '0', node, 'cat', `/proc/${initial.child.pid}/status`]);
      const fds = await cluster.compose(['exec', '-T', '--user', '0', node, 'ls', '-1', `/proc/${initial.child.pid}/fd`]);
      assert.equal((await idle()).homes, expectedCopies);
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
      const observedCopies = new Set();
      const residency = [];
      const started = performance.now();
      const sampleResidency = async () => {
        const { residentRoots, homes, idleRetirements, admissionRetirements } = await onSameChild();
        residency.push({ elapsedMs: performance.now() - started, residentRoots, homes,
          idleRetirements, admissionRetirements });
      };
      await sampleResidency();
      const stopSampling = new AbortController();
      let samplingError;
      const sampling = (async () => {
        for await (const _ of interval(residencyPollMs, undefined, { signal: stopSampling.signal })) {
          await sampleResidency();
        }
      })().catch(error => {
        if (error.name !== 'AbortError') samplingError = error;
      });
      let nextOffer = performance.now();
      try {
        for (let from = 0; from < cohortSize; from += batchSize) {
          controller.signal.throwIfAborted();
          const batch = [];
          for (let offset = from; offset < Math.min(from + batchSize, cohortSize); ++offset) {
            const index = (cohort - 1) * cohortSize + offset + 1;
            const value = 'cohort-' + cohort + '-object-' + index;
            if (offerPeriodMs > 0) {
              const wait = nextOffer - performance.now();
              if (wait > 0) await delay(Math.ceil(wait));
            }
            const start = performance.now();
            nextOffer = start + offerPeriodMs;
            try {
              const result = await call(index, phase === 'seed' ? value : undefined);
              const ms = performance.now() - start;
              observedCopies.add(result.moduleId);
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
      } finally {
        stopSampling.abort();
        await sampling;
      }
      if (samplingError) throw samplingError;
      await sampleResidency();
      const peakResidentRoots = Math.max(...residency.map(sample => sample.residentRoots));
      const keptWarmCopies = [...observedCopies].every(copy => warmCopies.has(copy)) &&
        residency.every(sample => sample.homes === expectedCopies);
      const result = { ...summary(samples), peakResidentRoots, keptWarmCopies };
      if (result.successes !== cohortSize || !keptWarmCopies) workloadPassed = false;
      await record('pass-residency', { cohort, phase, child: initial.child, observedCopies: [...observedCopies],
        peakResidentRoots, samples: residency });
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
  const gateEligible = offerPeriodMs === 0;
  const pass = gateEligible && workloadPassed && comparisons.every(comparison => comparison.pass);
  await record('gate-summary', { child: initial.child, baselineStable, rssVariationKiB,
    gateEligible, workloadPassed, comparisons, pass });
  console.log(JSON.stringify({ baselineStable, rssVariationKiB, gateEligible, workloadPassed, comparisons, pass }));
}

async function runMeasurement(output, residencyOnly) {
  await writeFile(output, '', { flag: 'wx' });
  const record = (metric, fields) =>
    appendFile(output, JSON.stringify({ metric, at: new Date().toISOString(), ...fields }) + '\n');
  const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-durable-idle-')), kernel, controller.signal, {
    singleNode: true, threadedNodes: ['node-1'],
    configureNode: () => ({ durableObjects: { processes: 1, peers: 0, threadedStorage: true } }),
  });
  const [node] = cluster.nodes;
  try {
    await measure(cluster, node, record, residencyOnly);
  } catch (error) {
    await record('measurement-error', { error: error.stack });
    await cluster.diagnostics(residencyOnly ? 'residency-measurement' : 'idle-measurement', error);
    throw error;
  } finally {
    await cluster.close();
  }
}

if (mode === undefined) {
  // The existing hosted idle job invokes both workloads. Each gets a fresh cluster and report.
  const { dir, name, ext } = parse(report);
  await runMeasurement(report, false);
  await runMeasurement(join(dir, name + '-residency' + ext), true);
} else {
  await runMeasurement(report, mode === '--residency');
}

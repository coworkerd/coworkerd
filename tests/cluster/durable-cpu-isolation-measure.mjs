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
  'Usage: node tests/cluster/durable-cpu-isolation-measure.mjs <verified-kernel-directory> <new-report.jsonl>');
assert.equal(process.platform, 'linux');
const root = fileURLToPath(new URL('../../', import.meta.url));
const kernel = resolve(kernelArg);
const report = resolve(reportArg);
const cpuMs = 1500;
const intervalMs = 10;
const offersPerWindow = 110;
const pairs = 11;
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}
await writeFile(report, '', { flag: 'wx' });
const record = (metric, fields) => appendFile(report, JSON.stringify({ metric, ...fields }) + '\n');
const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-durable-cpu-')), kernel, controller.signal, {
  singleNode: true, durableFixtureNodes: ['node-1'],
  configureNode: () => ({ durableObjects: { processes: 1, peers: 0 } }),
});
const [node] = cluster.nodes;

function source(className) {
  return `
    import { DurableObject } from 'cloudflare:workers';
    let copy;
    export class ${className} extends DurableObject {
      instance = crypto.randomUUID();
      async fetch(request) {
        copy ??= crypto.randomUUID();
        const url = new URL(request.url);
        const phase = url.searchParams.get('phase');
        if (phase === 'warm') this.sentinel = 'preserved ' + this.instance;
        if (phase === 'burn') {
          console.log('cpu-window ' + url.searchParams.get('window') + ' ' + Date.now());
          let checksum = 1;
          for (let i = 0; i < 2000000000; ++i) checksum = Math.imul(checksum, 1664525) + i | 0;
          this.checksum = checksum;
        }
        return Response.json({ instance: this.instance, copy, sentinel: this.sentinel, observedAt: Date.now() });
      }
    }
    export default { fetch(request, env) {
      return env.OBJECT.getByName(new URL(request.url).pathname.split('/').at(-1)).fetch(request);
    } };
  `;
}

function summary(rows) {
  const sent = rows.filter(row => !row.missed);
  const misses = rows.length - sent.length;
  // Skipped offers have no latency. Failures among sent probes occupy ranks above successful replies.
  const times = sent.filter(row => !row.error).map(row => row.scheduledElapsedMs).toSorted((a, b) => a - b);
  return { offers: rows.length, sent: sent.length, successes: times.length, misses,
    skipRatio: rows.length ? misses / rows.length : null, errors: sent.length - times.length,
    p50Ms: times[Math.ceil(sent.length * 0.50) - 1] ?? null,
    p99Ms: times[Math.ceil(sent.length * 0.99) - 1] ?? null };
}

async function measure() {
  const pins = JSON.parse(await readFile(join(root, 'patches/workerd/artifacts.json'), 'utf8'));
  const manifest = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
  const binaryHash = createHash('sha256').update(await readFile(join(kernel, 'coworkerd-threaded'))).digest('hex');
  assert.equal(manifest.patchSeriesSha256, pins.patchSeriesSha256);
  assert.equal(binaryHash, manifest.files['coworkerd-threaded']);
  const run = (program, args) => cluster.command(program, args, { cwd: root });
  await record('provenance', { commit: await run('git', ['rev-parse', 'HEAD']),
    kernelRelease: pins.releaseTag, archiveSha256: pins.archives[manifest.architecture], binaryHash,
    os: await run('uname', ['-a']), cpu: await run('lscpu', []), node: process.version,
    pairs, offersPerWindow, intervalMs, limitMs: cpuMs });
  await cluster.start();
  const first = await cluster.tenant('acceptance-cpu');
  const second = await cluster.tenant('acceptance-neighbor');
  const roles = { sameTenant: 'SameTenantProbe', otherTenant: 'OtherTenantProbe' };
  for (const [tenant, name, className] of [
    [first, 'burner', 'BurnObject'], [first, 'probe', roles.sameTenant], [second, 'probe', roles.otherTenant],
  ]) {
    await cluster.deploy(tenant, name, source(className), { wrangler: {
      durable_objects: { bindings: [{ name: 'OBJECT', class_name: className }] },
      migrations: [{ tag: 'v1', new_sqlite_classes: [className] }],
    } });
  }
  await cluster.api(`${first.path}/workers/burner`, {
    token: first.token, method: 'PATCH', body: { limits: { cpuMs } },
  });
  const path = {
    burner: `/${first.name}/burner/burner`, watcher: `/${first.name}/burner/watcher`,
    sameTenant: `/${first.name}/probe/neighbor`, otherTenant: `/${second.name}/probe/neighbor`,
  };
  const call = name => request(cluster.urls[node], name, { timeout: 9000 });
  const ok = async name => json(await call(name), 200, name);
  const ready = await eventually('worker limits and distinct homes warm', async () => {
    const state = await cluster.state(node);
    const cloud = await cluster.api('/v1/status');
    assert.equal(String(state.storage.appliedTenantsRevision), String(cloud.storage.tenantsRevision));
    const warmed = Object.fromEntries(await Promise.all(Object.keys(path)
      .map(async role => [role, await ok(path[role] + '?phase=warm')])));
    assert.equal(warmed.burner.copy, warmed.watcher.copy);
    assert.equal(new Set([warmed.burner.copy, warmed.sameTenant.copy, warmed.otherTenant.copy]).size, 3);
    const children = state.processes.entries.filter(entry => entry.processClass === 'durable');
    assert.equal(children.length, 1);
    const child = children[0];
    assert.ok(child.pid > 0);
    assert.equal(child.state, 'ready');
    assert.equal(child.binarySha256, binaryHash);
    assert.equal(child.sandbox.durableServer, 'threaded');
    return { warmed, child: { processId: child.processId, pid: child.pid, generation: child.generation } };
  });
  await record('initial', { ...ready, load: await run('uptime', []) });
  const checkGeneration = async () => {
    const { processes } = await cluster.state(node);
    const current = processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(current.state, 'ready');
    assert.deepEqual({ processId: current.processId, pid: current.pid, generation: current.generation }, ready.child);
    return current.lifecycle.durableAdmission;
  };

  async function offers() {
    const mono = performance.now();
    const wall = Date.now();
    const requests = [];
    for (let i = 0; i < offersPerWindow; ++i) {
      const start = mono + i * intervalMs;
      const remaining = start - performance.now();
      if (remaining > 0) await delay(Math.ceil(remaining));
      for (const role of Object.keys(roles)) {
        const scheduledAt = wall + i * intervalMs;
        requests.push((async () => {
          const sentAt = Date.now();
          if (performance.now() - start >= intervalMs) return { role, scheduledAt, sentAt, missed: true };
          try {
            const value = await ok(path[role]);
            const scheduledElapsedMs = performance.now() - start;
            assert.equal(value.instance, ready.warmed[role].instance);
            assert.equal(value.copy, ready.warmed[role].copy);
            return { role, scheduledAt, sentAt, doneAt: Date.now(), scheduledElapsedMs, workerAt: value.observedAt };
          } catch (error) {
            return { role, scheduledAt, sentAt, doneAt: Date.now(), error: error.message };
          }
        })());
      }
    }
    return Promise.all(requests);
  }

  const windows = [];
  let currentBurner = ready.warmed.burner;
  for (let index = 0; index < pairs; ++index) {
    controller.signal.throwIfAborted();
    const quiet = { index, condition: 'quiet', probes: await offers() };
    windows.push(quiet);
    await record('window', quiet);
    const startedAt = Date.now();
    const burning = call(path.burner + '?phase=burn&window=' + index).then(response => ({
      status: response.status, body: response.text, doneAt: Date.now(),
    }), error => ({ error: error.message, doneAt: Date.now() }));
    await delay(180);
    const watcherSentAt = Date.now();
    const watching = ok(path.watcher).then(value => ({ ...value, doneAt: Date.now() }),
      error => ({ error: error.message, doneAt: Date.now() }));
    const probes = await offers();
    const [burn, watcher] = await Promise.all([burning, watching]);
    const resetAttempts = [];
    const reopened = await eventually('the CPU-limited object can reopen', async () => {
      try {
        const value = await ok(path.burner);
        assert.notEqual(value.instance, currentBurner.instance);
        return value;
      } catch (error) {
        resetAttempts.push(error.message);
        throw error;
      }
    }, { timeout: 2500, interval: 25 });
    const busy = { index, condition: 'busy', startedAt, probes, burn, watcherSentAt, watcher, reopened,
      resetAttempts, previousBurner: currentBurner, admission: await checkGeneration() };
    windows.push(busy);
    await record('window', busy);
    currentBurner = reopened;
  }
  const childLog = await cluster.compose(['exec', '-T', node, 'cat', '/var/lib/coworkerd/logs/workerd.log']);
  const starts = new Map(childLog.split('\n').flatMap(line => {
    const match = line.match(/^cpu-window (\d+) (\d+)$/);
    return match ? [[Number(match[1]), Number(match[2])]] : [];
  }));
  const trace = await cluster.compose(['logs', '--no-color', node], { maxBuffer: 16 * 1024 * 1024 });
  const ownerEvents = trace.split('\n').filter(line => line.includes('[do] '))
    .map(line => JSON.parse(line.split('[do] ')[1]))
    .filter(row => row.className === 'BurnObject' || Object.values(roles).includes(row.className));
  await record('owner-placement', { starts: Object.fromEntries(starts), ownerEvents });
  for (const row of ownerEvents) {
    assert.equal(row.nodeId, node);
    assert.equal(row.route, 'local');
    assert.equal(row.processId, ready.child.processId);
    assert.equal(row.processGeneration, ready.child.generation);
  }
  assert.equal(starts.size, pairs);
  for (const window of windows.filter(row => row.condition === 'busy')) {
    const entered = starts.get(window.index);
    const completions = ownerEvents.filter(row => row.className === 'BurnObject' && row.status === 503 &&
      row.code === 'CPU_LIMIT_EXCEEDED' && row.observedAtUnixMs > entered &&
      row.observedAtUnixMs <= window.burn.doneAt + 25);
    assert.equal(completions.length, 1, JSON.stringify(window.burn));
    const [limit] = completions;
    assert.equal(json({ status: window.burn.status, text: window.burn.body }, 503, 'CPU limit').code, 'CPU_LIMIT_EXCEEDED');
    assert.ok(window.watcherSentAt > entered && window.watcherSentAt < limit.observedAtUnixMs);
    assert.ok(window.watcher.observedAt > entered + cpuMs - 40, 'same-copy actor waited for the CPU-bound event');
    assert.equal(window.watcher.instance, ready.warmed.watcher.instance);
    assert.equal(window.watcher.copy, ready.warmed.watcher.copy);
    assert.equal(window.watcher.sentinel, ready.warmed.watcher.sentinel);
    assert.notEqual(window.reopened.instance, window.previousBurner.instance);
    assert.equal(window.reopened.copy, ready.warmed.watcher.copy);
    assert.equal(window.reopened.sentinel, undefined);
    for (const [role, className] of Object.entries(roles)) {
      const successful = window.probes.filter(row => row.role === role && !row.error && !row.missed);
      assert.ok(successful.every(row => row.sentAt > entered && row.doneAt < limit.observedAtUnixMs),
        `client overlap for window ${window.index} ${role}`);
      const firstSend = Math.min(...successful.map(row => row.sentAt));
      const lastReply = Math.max(...successful.map(row => row.doneAt));
      // The owner may finish logging after the response body reaches the client.
      const actual = ownerEvents.filter(row => row.className === className && row.status === 200 &&
        row.observedAtUnixMs >= firstSend && row.observedAtUnixMs <= lastReply + 25);
      assert.equal(actual.length, successful.length, `owner dispatch count for window ${window.index} ${role}`);
      assert.ok(actual.every(row => row.observedAtUnixMs > entered && row.observedAtUnixMs < limit.observedAtUnixMs),
        `owner overlap for window ${window.index} ${role}`);
    }
  }
  const results = Object.fromEntries(Object.keys(roles).map(role => {
    const counts = condition => summary(windows.filter(w => w.condition === condition)
      .flatMap(w => w.probes.filter(row => row.role === role)));
    const quiet = counts('quiet');
    const busy = counts('busy');
    const deltaMs = busy.p99Ms === null || quiet.p99Ms === null ? null : busy.p99Ms - quiet.p99Ms;
    return [role, { quiet, busy, deltaMs, pass: quiet.successes >= 1000 && busy.successes >= 1000 &&
      deltaMs !== null && deltaMs <= 5 && quiet.errors === 0 && busy.errors === 0 &&
      quiet.skipRatio < 0.01 && busy.skipRatio < 0.01 }];
  }));
  await record('gate-summary', { child: ready.child, load: await run('uptime', []), ...results });
  console.log(JSON.stringify(results, null, 2));
}

try {
  await measure();
} catch (error) {
  await record('measurement-error', { error: error.stack });
  await cluster.diagnostics('cpu-isolation-measurement', error);
  throw error;
} finally {
  await cluster.close();
}

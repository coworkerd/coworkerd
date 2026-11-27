import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { availableParallelism, release } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';

const input = JSON.parse(readFileSync('/measure.json', 'utf8'));
const { home, hot, light, nodes, checksum, hotRate, calibrate, longRun, management } = input;
const shortWindowMs = 15_000;
const loadedWindowMs = longRun ? 60_000 : shortWindowMs;
const lightRate = 100;
const pairs = 3;
const url = node => `http://${node}:8930`;

async function call(worker, id) {
  try {
    const response = await fetch(`${url(home)}/${worker}?id=${id}`, { signal: AbortSignal.timeout(45_000) });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      return { code: /^[A-Z_\d]+$/.test(body.code) ? body.code : `HTTP_${response.status}` };
    }
    if (!nodes.includes(body.nodeId) || (!longRun && worker === light && body.nodeId !== home) ||
        (worker === hot && (body.id !== id || body.checksum !== checksum))) {
      return { code: 'INVALID_REPLY' };
    }
    return { nodeId: body.nodeId };
  } catch (error) {
    return { code: error.name === 'TimeoutError' ? 'CLIENT_TIMEOUT' : 'CLIENT_ERROR' };
  }
}

async function at(when) {
  const wait = when - performance.now();
  if (wait > 0) await delay(Math.ceil(wait));
}

async function offer(worker, id, start, due) {
  const sent = performance.now();
  const result = await call(worker, id);
  const completed = performance.now();
  return { id, ...result, sendDelayMs: sent - due, latencyMs: completed - due, completionMs: completed - start };
}

async function paced(worker, label, start, windowMs, rate, offset = 0) {
  const spacing = 1_000 / rate;
  const pending = [];
  for (let slot = 0; slot < windowMs * rate / 1_000; ++slot) {
    const due = start + slot * spacing + offset;
    const id = `${label}-${slot}`;
    await at(due);
    const sendDelayMs = performance.now() - due;
    if (sendDelayMs >= spacing) pending.push({ id, code: 'CLIENT_MISSED_OFFER', sendDelayMs });
    else pending.push(offer(worker, id, start, due));
  }
  return Promise.all(pending);
}

function summarize(rows, windowMs) {
  const complete = rows.filter(row => row.nodeId);
  const inWindow = complete.filter(row => row.completionMs < windowMs);
  const byNode = replies => Object.fromEntries(nodes.map(node => [node, replies.filter(row => row.nodeId === node).length]));
  const codes = {};
  for (const { code } of rows) if (code) codes[code] = (codes[code] ?? 0) + 1;
  const latencies = complete.map(row => row.latencyMs).sort((a, b) => a - b);
  // Rank every failed or missed offer above the validated replies.
  return { offers: rows.length, replies: complete.length, byNode: byNode(complete),
    inWindowByNode: byNode(inWindow), completedPerSecond: inWindow.length * 1_000 / windowMs, codes,
    p99Ms: latencies[Math.ceil(rows.length * 0.99) - 1] ?? null,
    replyP99Ms: latencies[Math.ceil(latencies.length * 0.99) - 1] ?? null };
}

function cpuTicks() {
  const [user, nice, system, idle, wait, irq, softirq, steal] =
    readFileSync('/proc/stat', 'utf8').split('\n', 1)[0].trim().split(/\s+/).slice(1).map(Number);
  return { idle, total: user + nice + system + idle + wait + irq + softirq + steal };
}

async function sampleCpu(start, windowMs) {
  const ticks = [];
  for (let elapsed = 0; elapsed <= windowMs; elapsed += 1_000) {
    await at(start + elapsed);
    ticks.push(cpuTicks());
  }
  const idle = (before, after) => (after.idle - before.idle) / (after.total - before.total);
  return { meanIdleFraction: idle(ticks[0], ticks.at(-1)),
    minOneSecondIdleFraction: Math.min(...ticks.slice(1).map((after, i) => idle(ticks[i], after))) };
}

async function nodePlacement(node) {
  const response = await fetch(`${url(node)}/__coworkerd/state`, {
    headers: { authorization: `Bearer ${management}` }, signal: AbortSignal.timeout(5_000),
  });
  assert.equal(response.status, 200, `placement counters at ${node}`);
  const { placement, processes } = await response.json();
  assert.equal(placement.rootLimit, 128);
  assert.equal(processes.entries.filter(entry => entry.processClass === 'ordinary').length, 1);
  return placement;
}

async function rejectionCounters() {
  return Object.fromEntries(await Promise.all(nodes.map(async node => [node, (await nodePlacement(node)).rejections])));
}

async function sampleHome(start, windowMs) {
  const hotTenant = hot.split('/')[0];
  const lightTenant = light.split('/')[0];
  const samples = [];
  for (let elapsed = 0; elapsed <= windowMs; elapsed += 5_000) {
    await at(start + elapsed);
    const sentAtMs = performance.now() - start;
    const { inFlight, tenantAdmission } = await nodePlacement(home);
    assert.equal(tenantAdmission.rootLimit, 0);
    assert.equal(tenantAdmission.nestedLimit, 384);
    assert.equal(inFlight.nested, 0);
    const { tenants } = tenantAdmission;
    const hotRoots = tenants.find(row => row.tenant === hotTenant)?.inFlight.roots ?? 0;
    const lightRoots = tenants.find(row => row.tenant === lightTenant)?.inFlight.roots ?? 0;
    assert.equal(hotRoots + lightRoots, inFlight.roots);
    samples.push({ elapsed, sentAtMs, receivedAtMs: performance.now() - start, hotRoots, lightRoots });
  }
  return samples;
}

function counterDelta(before, after) {
  return Object.fromEntries(nodes.map(node => {
    const entries = Object.entries(after[node]).map(([code, value]) => [code, value - (before[node][code] ?? 0)]);
    assert.ok(entries.every(([, count]) => count >= 0), `placement counters reset on ${node}`);
    return [node, Object.fromEntries(entries.filter(([, count]) => count > 0))];
  }));
}

const emit = row => console.log(JSON.stringify(row));

for (let i = 0; i < 6; ++i) {
  assert.deepEqual(await call(hot, `warm-${i}`), { nodeId: home });
  assert.deepEqual(await call(light, `warm-${i}`), { nodeId: home });
}
await delay(4_000);
emit({ metric: 'client', node: process.version, linuxRelease: release(), vcpus: availableParallelism(),
  quietWindowMs: shortWindowMs, loadedWindowMs, lightRate,
  cgroupQuota: readFileSync('/sys/fs/cgroup/cpu.max', 'utf8').trim(),
  cgroupCpuset: readFileSync('/sys/fs/cgroup/cpuset.cpus.effective', 'utf8').trim() });

if (calibrate) {
  // Keep a single statically pinned isolate busy with four outstanding requests.
  const start = performance.now() + 100;
  await at(start);
  const [lanes, cpu] = await Promise.all([Promise.all(Array.from({ length: 4 }, async (_, lane) => {
    const rows = [];
    while (performance.now() < start + shortWindowMs) {
      rows.push(await offer(hot, `calibration-${lane}-${rows.length}`, start, performance.now()));
    }
    return rows;
  })), sampleCpu(start, shortWindowMs)]);
  const rows = lanes.flat();
  const summary = summarize(rows, shortWindowMs);
  assert.equal(summary.byNode[home], rows.length, 'calibration must complete on the pinned isolate');
  emit({ metric: 'calibration', cpu, hotSummary: summary, hot: rows });
} else {
  const loadedWindows = longRun ? 1 : pairs;
  for (let index = 0; index <= 2 * loadedWindows; ++index) {
    const loaded = index % 2 === 1;
    const windowMs = loaded ? loadedWindowMs : shortWindowMs;
    const before = await rejectionCounters();
    const start = performance.now() + 100;
    const [hotRows, lightRows, cpu, homeRoots] = await Promise.all([
      loaded ? paced(hot, `hot-${index}`, start, windowMs, hotRate) : [],
      paced(light, `light-${index}`, start, windowMs, lightRate, 5), sampleCpu(start, windowMs),
      longRun ? sampleHome(start, windowMs) : [],
    ]);
    const refusalsByNode = counterDelta(before, await rejectionCounters());
    emit({ metric: 'window', index, loaded, windowMs, cpu, refusalsByNode, ...(longRun && { homeRoots }),
      hotSummary: summarize(hotRows, windowMs), lightSummary: summarize(lightRows, windowMs), hot: hotRows, light: lightRows });
    if (loaded) await delay(4_000);
  }
}

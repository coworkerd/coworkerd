import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, readdirSync } from 'node:fs';
import { Agent, request } from 'node:http';

const [base, input] = process.argv.slice(2);
const { operation, clients, targets, ids, warmMs, measureMs, pids } = JSON.parse(input);
const hz = Number(execFileSync('getconf', ['CLK_TCK'], { encoding: 'utf8' }));
assert.ok(hz > 0);
const agent = new Agent({ keepAlive: true, maxSockets: clients });
const inserting = operation === 'insert';
const expectedBody = inserting ? '1' : '[{"value":1}]';

function send(url) {
  return new Promise((resolve, reject) => {
    const req = request(url, { agent, method: inserting ? 'POST' : 'GET' }, response => {
      response.setEncoding('utf8');
      let body = '';
      response.on('data', chunk => { body += chunk; });
      response.on('error', reject);
      response.on('end', () => {
        if (response.statusCode !== 200 || body !== expectedBody) {
          reject(new Error('HTTP ' + response.statusCode + ' ' + body.slice(0, 300)));
        } else {
          resolve();
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => req.destroy(new Error('request timed out')));
    req.end();
  });
}

function optionalFile(path) {
  try {
    return readFileSync(path, 'utf8').trim();
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    return null;
  }
}

function stat(path) {
  const line = readFileSync(path, 'utf8');
  // The command name is parenthesized and can contain spaces or parentheses.
  const fields = line.slice(line.lastIndexOf(')') + 2).split(/\s+/);
  return { ticks: Number(fields[11]) + Number(fields[12]), start: fields[19] };
}

function cpuSample() {
  const [aggregate, ...cpuLines] = readFileSync('/proc/stat', 'utf8').split('\n');
  const [user, nice, system, idle, iowait, irq, softirq, steal] = aggregate.trim().split(/\s+/).slice(1).map(Number);
  const host = { busy: user + nice + system + irq + softirq, idle, iowait, steal };
  const processes = {};
  for (const [role, pid] of Object.entries({ ...pids, client: process.pid })) {
    const path = '/proc/' + pid;
    const threads = {};
    for (const tid of readdirSync(path + '/task')) {
      try {
        threads[tid] = stat(path + '/task/' + tid + '/stat');
      } catch (error) {
        // A helper thread can exit while the process is being sampled.
        if (error.code !== 'ENOENT' && error.code !== 'ESRCH') throw error;
      }
    }
    processes[role] = { pid, ...stat(path + '/stat'), threads };
  }
  return { host, processes, logicalCpus: cpuLines.filter(line => /^cpu\d+\s/.test(line)).length,
    load: readFileSync('/proc/loadavg', 'utf8').trim(),
    cpuPressure: optionalFile('/proc/pressure/cpu'), ioPressure: optionalFile('/proc/pressure/io') };
}

function cpuDelta(before, after, elapsedMs) {
  assert.equal(before.logicalCpus, after.logicalCpus);
  const percentOfCore = ticks => ticks * 100000 / hz / elapsedMs;
  const usage = ticks => ({ ticks, percentOfCore: percentOfCore(ticks) });
  const processes = {};
  for (const [role, finish] of Object.entries(after.processes)) {
    const start = before.processes[role];
    assert.equal(finish.start, start.start, role + ' process changed during the measurement');
    assert.ok(finish.ticks >= start.ticks);
    const threads = Object.entries(finish.threads)
      .filter(([tid, sample]) => start.threads[tid]?.start === sample.start)
      .map(([tid, sample]) => ({ tid: Number(tid), ...usage(sample.ticks - start.threads[tid].ticks) }));
    const main = threads.find(thread => thread.tid === finish.pid);
    assert.ok(main, role + ' main thread was sampled at both endpoints');
    const busiest = threads.reduce((max, thread) => thread.ticks > max.ticks ? thread : max, main);
    processes[role] = { pid: finish.pid, start: finish.start, ...usage(finish.ticks - start.ticks), main, busiest };
  }
  const host = Object.fromEntries(Object.entries(after.host).map(([key, ticks]) => [key, ticks - before.host[key]]));
  return { hz, processes, host: { ticks: host, logicalCpus: before.logicalCpus,
    busyCores: percentOfCore(host.busy) / 100,
    busyPercent: 100 * host.busy / Object.values(host).reduce((a, b) => a + b, 0) } };
}

async function drive(durationMs, samples, start) {
  const completed = targets.map(() => 0);
  let failures = 0;
  const errorExamples = [];
  await Promise.all(Array.from({ length: clients }, async (_, lane) => {
    const index = lane % targets.length;
    const url = base + '/' + targets[index] + '/' + operation;
    while (performance.now() - start < durationMs) {
      const sent = performance.now();
      try {
        await send(inserting ? url + '?id=' + (++ids[index]) : url);
        completed[index]++;
        samples?.push(performance.now() - sent);
      } catch (error) {
        failures++;
        if (errorExamples.length < 5) errorExamples.push(error.message);
      }
    }
  }));
  return { completed, failures, errorExamples };
}

try {
  const warmup = await drive(warmMs, null, performance.now());
  const clientCpuMax = optionalFile('/sys/fs/cgroup/cpu.max');
  const before = cpuSample();
  const start = performance.now();
  const samples = [];
  const measurement = await drive(measureMs, samples, start);
  const elapsedMs = performance.now() - start;
  const after = cpuSample();
  samples.sort((a, b) => a - b);
  const percentile = fraction => samples[Math.ceil(samples.length * fraction) - 1] ?? null;
  console.log(JSON.stringify({
    clientNode: process.version, clientCpuMax, clients, targets, elapsedMs,
    n: samples.length, perSecond: samples.length * 1000 / elapsedMs,
    p50Ms: percentile(0.5), p99Ms: percentile(0.99),
    warmup, measurement, cpu: cpuDelta(before, after, elapsedMs),
    resources: [before, after].map(({ load, cpuPressure, ioPressure }) => ({ load, cpuPressure, ioPressure })),
  }));
} finally {
  agent.destroy();
}

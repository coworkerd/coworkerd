import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { readFile, readdir } from 'node:fs/promises';
import { Agent, request } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';

const { child, blocking, sameTenant, otherTenant, objectPath, management } =
  JSON.parse(readFileSync('/isolation.json', 'utf8'));
const node = 'http://node-1:8930';
const api = 'http://apiserver:8940';
const agent = new Agent({ keepAlive: true, maxSockets: 16 });
const ticksPerSecond = Number(execFileSync('getconf', ['CLK_TCK'], { encoding: 'utf8' }));
assert.ok(ticksPerSecond > 0);
const read = { sql: 'SELECT value FROM ledger WHERE id = 1' };
const periodMs = 25;
const windowMs = 30000;
// Leave equal room to observe execution at the beginning and end of each busy window.
const marginMs = 350;
const offersPerRole = Math.floor((windowMs - 2 * marginMs) / periodMs);

function send(url, { token, body, timeout = 5000 } = {}) {
  const payload = body === undefined ? undefined : JSON.stringify(body);
  const headers = {
    ...(token && { authorization: `Bearer ${token}` }),
    ...(payload && { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) }),
  };
  return new Promise((resolve, reject) => {
    const req = request(url, { agent, method: payload ? 'POST' : 'GET', headers,
      signal: AbortSignal.timeout(timeout) }, response => {
      response.setEncoding('utf8');
      let text = '';
      response.on('data', chunk => { text += chunk; });
      response.on('error', reject);
      response.on('end', () => resolve({ text, status: response.statusCode }));
    });
    req.on('error', reject);
    req.end(payload);
  });
}

const sql = (db, body, timeout) => send(api + db.path, { token: db.token, body, timeout });
function parse(response, status) {
  assert.equal(response.status, status, `HTTP ${response.status} ${response.text.slice(0, 600)}`);
  return JSON.parse(response.text);
}

async function checkRead(db) {
  const result = parse(await sql(db, read), 200);
  assert.equal(result.success, true);
  assert.deepEqual(result.results, [{ value: db.name }]);
}

const probes = {
  sameTenant: () => checkRead(sameTenant),
  otherTenant: () => checkRead(otherTenant),
  async object() {
    const response = await send(node + objectPath);
    assert.equal(response.status, 200, response.text);
    assert.equal(response.text, 'object');
  },
};

async function snapshot() {
  const { processes, durableLog } = parse(await send(node + '/__coworkerd/state', { token: management }), 200);
  const entries = processes.entries.filter(entry => entry.processClass === 'durable');
  assert.equal(entries.length, 1);
  const [{ pid, generation, processId, state, lifecycle }] = entries;
  assert.deepEqual({ pid, generation, processId }, child);
  assert.equal(state, 'ready');
  assert.equal(lifecycle.durableServer, 'threaded');
  const { running, waiting, residentRoots } = lifecycle.durableAdmission;
  const { confirmations, unconfirmed } = durableLog.confirm;
  return { running, waiting, residentRoots, confirmations, unconfirmed,
    opens: durableLog.threadedHomeIo.openSamples, at: Date.now() };
}

async function waitForRunning(running) {
  const end = performance.now() + 1200;
  do {
    const state = await snapshot();
    if (state.running === running && state.waiting === 0) return state;
    await delay(15);
  } while (performance.now() < end);
  assert.fail(`the durable process did not report ${running} running events`);
}

async function readCpu() {
  const task = `/proc/${child.pid}/task`;
  const client = { at: Date.now(), usage: process.cpuUsage(), eventLoop: performance.eventLoopUtilization() };
  const [tids, schedstat] = await Promise.all([readdir(task), readFile('/proc/self/schedstat', 'utf8')]);
  const threads = await Promise.all(tids.map(async tid => {
    try {
      const stat = await readFile(`${task}/${tid}/stat`, 'utf8');
      const fields = stat.slice(stat.lastIndexOf(')') + 2).split(/\s+/);
      return [tid, Number(fields[11]) + Number(fields[12])];
    } catch (error) {
      if (error.code !== 'ENOENT' && error.code !== 'ESRCH') throw error;
      return null;
    }
  }));
  const [cpuTimeNs, runQueueWaitNs, timeslices] = schedstat.trim().split(/\s+/).map(Number);
  return { at: Date.now(), threads: Object.fromEntries(threads.filter(Boolean)),
    client: { ...client, scheduler: { cpuTimeNs, runQueueWaitNs, timeslices } } };
}

function executionEvidence(cpu, doneAt) {
  const beforeEnd = cpu.filter(sample => sample.at < doneAt);
  const first = beforeEnd[0];
  const last = beforeEnd.at(-1);
  const threads = Object.keys(first.threads).filter(tid => tid !== String(child.pid) && tid in last.threads);
  const ticks = tid => last.threads[tid] - first.threads[tid];
  const tid = threads.reduce((fastest, current) => ticks(current) > ticks(fastest) ? current : fastest);
  const active = beforeEnd.slice(1).filter((sample, index) => sample.threads[tid] > beforeEnd[index].threads[tid]);
  const maximumGap = Math.max(...active.slice(1).map((sample, index) => sample.at - active[index].at));
  // The long statement is the only additional work offered in this condition. Require one non-main
  // thread to consume CPU continuously from before the first probe to after the last reply.
  assert.ok(ticks(tid) >= 20 * ticksPerSecond && maximumGap < 450,
    `SQLite thread ${tid}: ${ticks(tid)} ticks, ${maximumGap} ms gap`);
  return { executionFromAt: active[0].at, executionThroughAt: active.at(-1).at, tid, ticks: ticks(tid), maximumGap };
}

async function at(when) {
  const remaining = when - performance.now();
  if (remaining > 0) await delay(Math.ceil(remaining));
}

async function drive(start, startedAt, offers, offset) {
  const entries = Object.entries(probes);
  return Object.fromEntries(await Promise.all(entries.map(async ([role, probe], phase) => {
    const samples = [];
    const errors = [];
    const pending = [];
    const misses = [];
    for (let slot = 0; slot < offers; slot++) {
      const relative = offset + slot * periodMs + phase * periodMs / entries.length;
      const scheduled = start + relative;
      const intendedAt = startedAt + relative;
      await at(scheduled);
      const sent = performance.now();
      if (sent - scheduled >= periodMs) {
        misses.push({ slot, intendedAt, schedulingDelayMs: sent - scheduled });
        continue;
      }
      const sentAt = Date.now();
      pending.push(probe().then(() => {
        const finished = performance.now();
        samples.push({ slot, intendedAt, sentAt, finishedAt: Date.now(), elapsedMs: finished - sent,
          scheduledElapsedMs: finished - scheduled, schedulingDelayMs: sent - scheduled });
      }, error => errors.push({ slot, intendedAt, sentAt, message: error.message })));
    }
    await Promise.all(pending);
    return [role, { samples, errors, misses, offered: offers }];
  })));
}

async function window(condition, index) {
  const warm = await drive(performance.now(), Date.now(), 60, 0);
  for (const item of Object.values(warm)) assert.equal(item.samples.length, item.offered, JSON.stringify(item.errors));
  await checkRead(blocking);
  const before = await waitForRunning(0);
  assert.equal(before.residentRoots, 4);
  const cpu = [await readCpu()];
  const startedAt = Date.now();
  const start = performance.now();
  const sampler = new AbortController();
  const sampling = (async () => {
    while (!sampler.signal.aborted) {
      await delay(100, undefined, { signal: sampler.signal });
      cpu.push(await readCpu());
    }
  })().catch(error => {
    if (error.name !== 'AbortError') throw error;
  });
  void sampling.catch(() => {});
  let running;
  let deadline;
  let pending;
  try {
    if (condition === 'busy') {
      const long = 'WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n < 1000000000)';
      pending = sql(blocking, [
        { sql: "INSERT INTO ledger VALUES (2, 'must roll back')" },
        { sql: `${long} INSERT INTO ledger SELECT sum(n), 'must roll back' FROM r` },
      ], 40000).then(response => ({ response, doneAt: Date.now(), elapsedMs: performance.now() - start }));
      void pending.catch(() => {});
      running = await waitForRunning(1);
    }
    const result = await drive(start, startedAt, offersPerRole, marginMs);
    await at(start + windowMs);
    let execution;
    if (pending) {
      const { response, doneAt, elapsedMs } = await pending;
      deadline = { ...parse(response, 503), doneAt, elapsedMs };
      assert.equal(deadline.code, 'SQL_LIMIT_EXCEEDED');
      assert.equal(deadline.reason, 'SQL_LIMIT_EXCEEDED: wall limit 30000ms exceeded');
      assert.ok(elapsedMs >= 29500 && elapsedMs < 32000, `deadline elapsed ${elapsedMs} ms`);
      execution = executionEvidence(cpu, doneAt);
    }
    const samples = Object.values(result).flatMap(probe => probe.samples);
    const lastReplyAt = Math.max(...samples.map(sample => sample.finishedAt));
    if (execution) {
      assert.ok(samples.every(sample => sample.sentAt >= Math.max(running.at, execution.executionFromAt) &&
        sample.finishedAt <= execution.executionThroughAt),
      `client overlap: ${running.at}, ${execution.executionFromAt} through ${execution.executionThroughAt}, last reply ${lastReplyAt}`);
      const rows = parse(await sql(blocking, { sql: 'SELECT id, value FROM ledger ORDER BY id' }), 200).results;
      assert.deepEqual(rows, [{ id: 1, value: blocking.name }]);
    }
    const after = await waitForRunning(0);
    assert.equal(after.confirmations, before.confirmations, 'no additional SQL transaction was confirmed');
    assert.equal(after.unconfirmed, 0);
    assert.equal(after.opens, before.opens, 'no database reopened during the window or rollback check');
    return { condition, index, startedAt, windowMs, offersPerSecond: 1000 / periodMs, ticksPerSecond,
      firstScheduledAt: startedAt + marginMs, lastReplyAt,
      before, after, runningAt: running?.at, deadline, ...execution, cpu, probes: result };
  } finally {
    sampler.abort();
    await sampling;
    if (pending) await pending.catch(() => {});
  }
}

try {
  for (const probe of Object.values(probes)) {
    // The worker can become reachable a little after cowctl reports activation.
    const end = performance.now() + 15000;
    for (;;) {
      try {
        await probe();
        break;
      } catch (error) {
        if (performance.now() >= end) throw error;
        await delay(100);
      }
    }
  }
  for (const [index, condition] of ['quiet', 'busy', 'quiet', 'busy', 'quiet', 'busy'].entries()) {
    console.log(JSON.stringify(await window(condition, index + 1)));
  }
} finally {
  agent.destroy();
}

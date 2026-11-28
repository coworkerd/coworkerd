import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, readFile, unlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [kernelArg, reportArg, mode, size, countArg, ...extra] = process.argv.slice(2);
assert.ok(kernelArg && reportArg && ['workers', 'databases', 'reuse', 'idle'].includes(mode) &&
  ['small', 'large'].includes(size) && !extra.length,
  'Usage: node tests/cluster/durable-home-capacity-measure.mjs <kernel> <new-report.jsonl> ' +
  '<workers|databases|reuse|idle> <small|large> [offers or idle ceiling]');
const root = fileURLToPath(new URL('../../', import.meta.url));
const kernel = resolve(kernelArg);
const report = resolve(reportArg);
const capacity = mode === 'workers' || mode === 'databases';
const total = Number(countArg ?? (capacity ? 513 : mode === 'reuse' ? 12 : 16384));
assert.ok(Number.isSafeInteger(total) && total > 0 && total <= (capacity ? 1024 : 16384));
assert.ok(!capacity || total > 4, 'capacity sweep needs at least five offers');
const memoryLimit = (size === 'small' ? 2 : 6) * 1024 ** 3;
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}
await writeFile(report, '', { flag: 'wx' });
const record = (metric, fields) => appendFile(report, JSON.stringify({
  metric, mode, size, at: new Date().toISOString(), ...fields,
}) + '\n');
const directory = await mkdtemp(join(tmpdir(), 'coworkerd-homes-'));
const databases = mode === 'databases';
const cluster = new Cluster(directory, kernel, controller.signal, {
  singleNode: !databases, threadedNodes: ['node-1'], holdPeerAcks: databases,
  memoryLimit: node => node === 'node-1' ? memoryLimit : undefined,
  configureNode: node => node === 'node-1' ? {
    durableObjects: { threadedStorage: true, ...(databases ? {} : { peers: 0 }),
      ...(mode === 'reuse' ? { maxHomes: 1 } : mode === 'idle' ? { maxHomes: total } : {}) },
  } : { durableObjects: { processes: 0 }, processPool: { count: 0 }, quarantineProcesses: 0 },
});
const [owner] = cluster.nodes;

const worker = `
  import { DurableObject } from 'cloudflare:workers';
  let copy;
  export class ObjectUnderTest extends DurableObject {
    async fetch(request) {
      copy ??= crypto.randomUUID();
      const op = new URL(request.url).pathname;
      if (op === '/pin' && !this.release) {
        this.ctx.waitUntil(new Promise(resolve => { this.release = resolve; }));
      } else if (op === '/release') {
        this.release?.();
        this.release = undefined;
      }
      return Response.json({ copy, pinned: Boolean(this.release),
        value: (await this.ctx.storage.get('value')) ?? null });
    }
  }
  export default { fetch(request, env) {
    const op = new URL(request.url).searchParams.get('op') ?? 'read';
    return env.OBJECT.getByName('one').fetch('http://object/' + op);
  } };
`;
const binding = {
  durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'ObjectUnderTest' }] },
  migrations: [{ tag: 'v1', new_sqlite_classes: ['ObjectUnderTest'] }],
};
const callObject = (tenant, op = 'read') =>
  request(cluster.urls[owner], `/${tenant.name}/app?op=${op}`, { timeout: 32000 });
const query = (tenant, body) => request(cluster.urls.apiserver, `${tenant.path}/databases/app/query`, {
  token: tenant.token, method: 'POST', body, timeout: 32000,
});

async function stage(count) {
  const tenants = new Array(count);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(6, count) }, async () => {
    for (;;) {
      controller.signal.throwIfAborted();
      const index = next++;
      if (index >= count) return;
      const tenant = await cluster.tenant('home-' + index);
      if (databases) {
        await cluster.api(`${tenant.path}/databases/app`, { token: tenant.token, method: 'POST' });
      } else {
        await cluster.deploy(tenant, 'app', worker, { wrangler: binding });
      }
      tenants[index] = tenant;
    }
  }));
  const { storage } = await cluster.api('/v1/status');
  await eventually('the owner has applied the staged catalog', async () => {
    const state = await cluster.state(owner);
    assert.ok(BigInt(state.storage.appliedTenantsRevision) >= BigInt(storage.tenantsRevision));
    if (databases) {
      assert.ok(cluster.nodes.slice(1).every(node =>
        state.durableLog.peers.streams.some(peer => peer.open && peer.nodeId === node)));
    }
  });
  return tenants;
}

async function child() {
  const { processes } = await cluster.state(owner);
  const entries = processes.entries.filter(entry => entry.processClass === 'durable');
  assert.equal(entries.length, 1);
  const [entry] = entries;
  assert.equal(entry.state, 'ready');
  assert.equal(entry.sandbox.durableServer, 'threaded');
  assert.ok(entry.lifecycle?.durableAdmission);
  return entry;
}

async function sample(entry) {
  const proc = `/proc/${entry.pid}`;
  const usage = await cluster.compose(['exec', '-T', '--user', '0', owner, 'sh', '-ec', `
    awk '/^(VmRSS|RssAnon|Threads):/ {print $2}' "${proc}/status"
    ls -1 "${proc}/fd" | wc -l
    cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.current
    awk '/^anon / {print $2}' /sys/fs/cgroup/memory.stat
  `]);
  const [rssKiB, anonymousKiB, threads, descriptors, memoryMax, memoryCurrent, cgroupAnonymous] =
    usage.split(/\s+/).map(Number);
  assert.equal(memoryMax, memoryLimit);
  assert.ok(rssKiB > 0 && threads > 0 && descriptors > 0 && memoryCurrent > 0 && cgroupAnonymous > 0);
  return { rssKiB, anonymousKiB, threads, descriptors, memoryMax, memoryCurrent, cgroupAnonymous,
    pid: entry.pid, generation: entry.generation, admission: entry.lifecycle.durableAdmission };
}

function sameChild(before, after) {
  assert.equal(after.pid, before.pid);
  assert.equal(after.generation, before.generation);
}

function checkCapacityRefusal(reply) {
  const refused = json(reply, 503, 'new home refused before execution');
  assert.equal(refused.code, 'OVERLOADED');
  assert.equal(refused.dispatched, false);
  assert.equal(refused.retryable, true);
  return refused;
}

async function empty() {
  const before = await sample(await child());
  assert.equal(before.admission.maxHomes, 16384);
  assert.equal(before.admission.workerThreads, 0);
  await record('empty', before);
  return before;
}

function reservationReached(state, pinned) {
  const resource = state.admission.lastDurableRefusal;
  assert.match(resource, /^(copy-(memory|residents|address-space)|home-(threads|descriptors|memory)|threads|descriptors|memory|address-space)$/);
  assert.ok(state.admission.overloaded > 0);
  assert.ok(pinned > 4);
  return resource;
}

async function measureResidentCost(before, after, pinned) {
  const deltas = Object.fromEntries(['rssKiB', 'anonymousKiB', 'threads', 'descriptors']
    .map(key => [key, (after[key] - before[key]) / pinned]));
  await record('resident-cost', { pinned, deltas, ...after });
  console.log(JSON.stringify({ size, mode, pinned, resource: after.admission.lastDurableRefusal, deltas }));
}

async function visitParticipants(tenants, action) {
  for (let offset = 0; offset < tenants.length; offset += 12) {
    await Promise.all(tenants.slice(offset, offset + 12).map(action));
  }
}

async function fillWorkers() {
  const tenants = await stage(total);
  const before = await empty();
  const participants = new Map();
  let nextRead = performance.now() + 2000;
  let firstRefusal;
  let refusalState;
  let offered = 0;
  try {
    while (offered < tenants.length && !firstRefusal) {
      const batch = tenants.slice(offered, offered + 8);
      const replies = await Promise.all(batch.map(tenant => callObject(tenant, 'pin')));
      for (let index = 0; index < batch.length; ++index) {
        const reply = replies[index];
        if (reply.status === 200) {
          const result = json(reply, 200, batch[index].name);
          assert.equal(result.pinned, true);
          participants.set(batch[index], result.copy);
        } else {
          const refusal = checkCapacityRefusal(reply);
          firstRefusal ??= { firstRefusedOffer: offered + index + 1, refusal };
        }
      }
      offered += batch.length;
      if (performance.now() >= nextRead || firstRefusal) {
        // Background work protects each home without leaving an ordinary Worker
        // invocation in flight. A paced read proves that its same copy is still live.
        await visitParticipants([...participants.keys()], async tenant => {
          const value = json(await callObject(tenant), 200, tenant.name);
          assert.equal(value.copy, participants.get(tenant));
          assert.equal(value.pinned, true);
        });
        nextRead = performance.now() + 2000;
      }
      const current = await eventually('each accepted Worker keeps a home', async () => {
        const current = await child();
        sameChild(before, current);
        const state = current.lifecycle.durableAdmission;
        assert.equal(state.residentRoots, participants.size);
        assert.equal(state.homes, participants.size);
        assert.equal(state.homeRetirements, 0);
        assert.equal(state.idleRetirements, 0);
        return current;
      }, { timeout: 3500, interval: 25 });
      const state = await sample(current);
      await record('fill', { offered, pinned: participants.size, ...state });
      if (firstRefusal) {
        refusalState = state;
        const resource = reservationReached(state, participants.size);
        await record('first-refusal', { pinned: participants.size, ...firstRefusal, resource, ...state });
      }
    }
    assert.ok(refusalState, 'increase offers to reach the first reservation refusal');
  } finally {
    await visitParticipants([...participants.keys()], tenant => callObject(tenant, 'release'));
  }
  await measureResidentCost(before, refusalState, participants.size);
}

async function fillDatabases() {
  const tenants = await stage(total);
  const before = await empty();
  const gate = join(cluster.peerAckGate, 'hold');
  await writeFile(gate, '');
  const attempts = [];
  let firstRefusal;
  let pinned = 0;
  const begin = performance.now();
  // An in-flight event holds every resident home throughout the sweep. An
  // idle gap between keepalive calls would let the incoming definition evict it.
  const batchSize = 8;
  try {
    for (let offset = 0; offset < tenants.length && !firstRefusal; offset += batchSize) {
      const batch = tenants.slice(offset, offset + batchSize);
      for (const tenant of batch) {
        const entry = { tenant, offset: attempts.length, settled: undefined, error: undefined };
        entry.done = query(tenant, [
          { sql: 'CREATE TABLE IF NOT EXISTS ledger (value INTEGER)' },
          { sql: 'INSERT INTO ledger VALUES (1)' },
        ]).then(reply => {
          entry.settled = reply;
          if (reply.status !== 200) firstRefusal ??= { offset: entry.offset, reply };
        }, error => { entry.error = error; });
        attempts.push(entry);
      }
      const until = performance.now() + 3500;
      let full;
      do {
        const current = await child();
        sameChild(before, current);
        const admission = current.lifecycle.durableAdmission;
        const failures = attempts.filter(attempt => attempt.settled?.status === 503).length;
        const earlyCompletions = attempts.filter(attempt => attempt.settled?.status === 200).length;
        assert.equal(earlyCompletions, 0, 'a held call completed before release');
        const error = attempts.find(attempt => attempt.error)?.error;
        if (error) throw error;
        pinned = admission.pendingCommits;
        if (pinned + failures === attempts.length) {
          full = current;
          break;
        }
        await delay(25);
      } while (performance.now() < until);
      assert.ok(full, `only ${pinned} of ${attempts.length} offers settled at the home or refused`);
      const state = await sample(full);
      assert.equal(state.admission.residentRoots, pinned);
      assert.equal(state.admission.homes, pinned);
      assert.equal(state.admission.homeRetirements, 0);
      assert.equal(state.admission.idleRetirements, 0);
      await record('fill', { offered: attempts.length, pinned, elapsedMs: performance.now() - begin, ...state });
      if (firstRefusal) {
        const refusal = checkCapacityRefusal(firstRefusal.reply);
        const resource = reservationReached(state, pinned);
        await record('first-refusal', { pinned, firstRefusedOffer: firstRefusal.offset + 1, refusal, resource, ...state });
      }
    }
    assert.ok(firstRefusal, 'increase offers to reach the first reservation refusal');
  } finally {
    await unlink(gate);
    await Promise.all(attempts.map(attempt => attempt.done));
  }
  const successful = attempts.filter(attempt => attempt.settled?.status === 200);
  assert.equal(successful.length, pinned);
  for (const entry of successful) {
    json(entry.settled, 200, entry.tenant.name);
  }
  const settled = await eventually('all accepted homes have settled and remain resident', async () => {
    const current = await child();
    sameChild(before, current);
    const state = current.lifecycle.durableAdmission;
    assert.equal(state.running, 0);
    assert.equal(state.waiting, 0);
    assert.equal(state.residentRoots, pinned);
    return current;
  }, { timeout: 3500, interval: 25 });
  await measureResidentCost(before, await sample(settled), pinned);
}

async function idle() {
  const initial = await child();
  for (let i = 0; i < 8; ++i) {
    const after = await sample(await child());
    sameChild(initial, after);
    assert.equal(after.admission.homes, 0);
    assert.equal(after.admission.residentRoots, 0);
    if (after.admission.maxHomes !== undefined) {
      assert.equal(after.admission.maxHomes, total);
      assert.equal(after.admission.workerThreads, 0);
      assert.equal(after.admission.homeReservedDescriptors, 0);
      assert.equal(after.admission.homeReservedBytes, 0);
    }
    await record('empty', after);
    await delay(250);
  }
}

async function reuse() {
  const [a, b] = await stage(2);
  for (let index = 0; index < total; ++index) {
    const before = await child();
    assert.equal(before.lifecycle.durableAdmission.workerThreads, 0);
    const timed = async tenant => {
      const start = performance.now();
      const data = json(await callObject(tenant), 200, tenant.name);
      return { ms: performance.now() - start, ...data };
    };
    const [first, second] = index % 2 ? [b, a] : [a, b];
    const free = await timed(first);
    const reused = await timed(second);
    assert.notEqual(free.copy, reused.copy);
    await eventually('one idle home was reused', async () => {
      const current = await child();
      sameChild(before, current);
      assert.equal(current.lifecycle.durableAdmission.homeRetirements, 1);
      assert.equal(current.lifecycle.durableAdmission.workerThreads, 1);
    }, { interval: 25 });
    await record('first-call', { index, freeMs: free.ms, reuseMs: reused.ms, ...await sample(await child()) });
    if (index + 1 === total) break;
    json(await request(cluster.urls[owner], `/__coworkerd/rotate?process=${before.processId}`, {
      method: 'POST', token: cluster.management,
    }), 200, 'rotate the durable child for the next cold pair');
    await eventually('replacement child is ready', async () => {
      const current = await child();
      assert.ok(current.generation > before.generation);
      assert.equal(current.lifecycle.durableAdmission.workerThreads, 0);
    });
  }
}

try {
  const manifest = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
  const digest = createHash('sha256').update(await readFile(join(kernel, 'coworkerd-threaded'))).digest('hex');
  assert.equal(manifest.files['coworkerd-threaded'], digest);
  const run = (command, args) => cluster.command(command, args, { cwd: root });
  await record('provenance', { commit: await run('git', ['rev-parse', 'HEAD']), kernel: manifest,
    os: await run('uname', ['-a']), memoryLimit, node: process.version });
  await cluster.start();
  const linux = await cluster.compose(['exec', '-T', owner, 'uname', '-a']);
  assert.match(linux, /^Linux /);
  await record('node-kernel', { linux });
  const initial = await eventually('the durable process is ready', child);
  assert.equal(initial.binarySha256, digest);
  if (mode === 'workers') await fillWorkers();
  else if (mode === 'databases') await fillDatabases();
  else if (mode === 'reuse') await reuse();
  else await idle();
} catch (error) {
  await record('measurement-error', { error: error.stack });
  await cluster.diagnostics('durable-home-capacity', error);
  throw error;
} finally {
  await cluster.close();
}

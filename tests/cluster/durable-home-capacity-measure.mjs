import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [kernelArg, reportArg, countArg, ...extra] = process.argv.slice(2);
assert.ok(kernelArg && reportArg && !extra.length,
  'Usage: node tests/cluster/durable-home-capacity-measure.mjs <kernel> <new-report.jsonl> [offers]');
const root = fileURLToPath(new URL('../../', import.meta.url));
const kernel = resolve(kernelArg);
const report = resolve(reportArg);
const total = Number(countArg ?? 513);
assert.ok(Number.isSafeInteger(total) && total > 4 && total <= 16385);
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}
await writeFile(report, '', { flag: 'wx' });
const record = (metric, fields) => appendFile(report, JSON.stringify({
  metric, at: new Date().toISOString(), ...fields,
}) + '\n');
const directory = await mkdtemp(join(tmpdir(), 'coworkerd-homes-'));
const cluster = new Cluster(directory, kernel, controller.signal, {
  singleNode: true, threadedNodes: ['node-1'],
  configureNode: () => ({ durableObjects: { threadedStorage: true, peers: 0 } }),
});
const [owner] = cluster.nodes;

const worker = `
  import { DurableObject } from 'cloudflare:workers';
  let copy;
  export class ObjectUnderTest extends DurableObject {
    async visit(op) {
      copy ??= crypto.randomUUID();
      if (op === 'pin' && !this.release) {
        this.ctx.waitUntil(new Promise(resolve => { this.release = resolve; }));
      } else if (op === 'release') {
        this.release?.();
        this.release = undefined;
      }
      return { copy, pinned: Boolean(this.release),
        value: (await this.ctx.storage.get('value')) ?? null };
    }
  }
  export default { async fetch(request, env) {
    const op = new URL(request.url).searchParams.get('op') ?? 'read';
    try {
      return Response.json(await env.OBJECT.getByName('one').visit(op));
    } catch (error) {
      if (error.code !== 'OVERLOADED' || !error.message.startsWith('OVERLOADED: ')) throw error;
      return new Response(error.message.slice('OVERLOADED: '.length), {
        status: 503, headers: { 'content-type': 'application/json' },
      });
    }
  } };
`;
const binding = {
  durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'ObjectUnderTest' }] },
  migrations: [{ tag: 'v1', new_sqlite_classes: ['ObjectUnderTest'] }],
};
const callObject = (tenant, op = 'read') =>
  request(cluster.urls[owner], `/${tenant.name}/app?op=${op}`, { timeout: 32000 });
async function stage(count) {
  const tenants = new Array(count);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(6, count) }, async () => {
    for (;;) {
      controller.signal.throwIfAborted();
      const index = next++;
      if (index >= count) return;
      const tenant = await cluster.tenant('home-' + index);
      await cluster.deploy(tenant, 'app', worker, { wrangler: binding });
      tenants[index] = tenant;
    }
  }));
  const { storage } = await cluster.api('/v1/status');
  await eventually('the owner has applied the staged catalog', async () => {
    const state = await cluster.state(owner);
    assert.ok(BigInt(state.storage.appliedTenantsRevision) >= BigInt(storage.tenantsRevision));
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

function sample(entry) {
  return { rssBytes: entry.residentBytes, pid: entry.pid, generation: entry.generation,
    admission: entry.lifecycle.durableAdmission };
}

function sameChild(before, after) {
  assert.equal(after.pid, before.pid);
  assert.equal(after.generation, before.generation);
}

function checkCapacityRefusal(reply, tenant) {
  const refused = json(reply, 503, tenant.name + ' new home refused before execution');
  assert.equal(refused.code, 'OVERLOADED', JSON.stringify(refused));
  assert.equal(refused.dispatched, false);
  assert.equal(refused.retryable, true);
  return refused;
}

async function empty() {
  const before = sample(await child());
  const { processes: { configuration } } = await cluster.state(owner);
  const defaultCeiling = Math.max(512, Math.min(16384, Math.floor(configuration.threadedMemoryBytes / 1048576)));
  assert.equal(before.admission.maxHomes, defaultCeiling);
  assert.equal(configuration.durableMaxHomes, defaultCeiling);
  assert.equal(before.admission.workerThreads, 0);
  await record('empty', { ...before, configuration });
  return before;
}

async function refusalResources(refusals) {
  const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', '--tail', '500', owner]);
  const rows = tag => logs.split('\n').filter(line => line.startsWith(tag))
    .map(line => JSON.parse(line.slice(tag.length)));
  const resources = new Map(rows('[durable-admission] ')
    .map(row => [row.invocationId, row.resource]));
  const rejected = rows('[execution-rejected] ');
  return refusals.map(({ tenant, ...reply }) => {
    const receipts = rejected.filter(row => row.target === tenant + '/app@v1');
    assert.equal(receipts.length, 1, tenant + ' has one node rejection');
    const [receipt] = receipts;
    assert.equal(receipt.dispatched, false);
    assert.equal(receipt.code, reply.code);
    const resource = resources.get(receipt.invocationId);
    assert.match(resource, /^(copy-(memory|residents|address-space)|home-(threads|descriptors|memory)|threads|descriptors|memory|address-space)$/);
    return { tenant, ...reply, resource, invocationId: receipt.invocationId };
  });
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
  let refusalBatch;
  let refusalState;
  let offered = 0;
  try {
    while (offered < tenants.length && !refusalBatch) {
      const batch = tenants.slice(offered, offered + 8);
      const replies = await Promise.all(batch.map(tenant => callObject(tenant, 'pin')));
      const refusals = [];
      for (let index = 0; index < batch.length; ++index) {
        const reply = replies[index];
        if (reply.status === 200) {
          const result = json(reply, 200, batch[index].name);
          assert.equal(result.pinned, true);
          participants.set(batch[index], result.copy);
        } else {
          refusals.push({ tenant: batch[index].name, ...checkCapacityRefusal(reply, batch[index]) });
        }
      }
      offered += batch.length;
      if (refusals.length !== 0) {
        assert.ok(participants.size > 4);
        const receipts = await refusalResources(refusals);
        const resources = [...new Set(receipts.map(receipt => receipt.resource))];
        refusalBatch = { offered, batchSize: batch.length, accepted: participants.size,
          refusals: receipts, resources };
        await record('refusal-observed', refusalBatch);
      }
      if (performance.now() >= nextRead || refusalBatch) {
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
        assert.equal(state.workerThreads, participants.size);
        // A refused cold event can leave an unused copy to reclaim. Every
        // accepted copy has just been verified by instance at the boundary.
        assert.ok(state.homeRetirements <= (refusalBatch?.refusals.length ?? 0));
        assert.equal(state.idleRetirements, 0);
        return current;
      }, { timeout: 3500, interval: 25 });
      const state = sample(current);
      await record('fill', { offered, pinned: participants.size, ...state });
      if (refusalBatch) {
        refusalState = state;
        await record('refusal-batch', { ...refusalBatch, ...state });
      }
    }
    assert.ok(refusalState, 'increase offers to reach the first reservation refusal');
  } finally {
    await visitParticipants([...participants.keys()], tenant => callObject(tenant, 'release'));
  }
  console.log(JSON.stringify({ homes: participants.size, resources: refusalBatch.resources,
    refused: refusalBatch.refusals.length, generation: refusalState.generation }));
}

try {
  const manifest = JSON.parse(await readFile(join(kernel, 'kernel.json'), 'utf8'));
  const digest = createHash('sha256').update(await readFile(join(kernel, 'coworkerd-threaded'))).digest('hex');
  assert.equal(manifest.files['coworkerd-threaded'], digest);
  const run = (command, args) => cluster.command(command, args, { cwd: root });
  await record('provenance', { commit: await run('git', ['rev-parse', 'HEAD']), kernel: manifest,
    os: await run('uname', ['-a']), node: process.version });
  await cluster.start();
  const linux = await cluster.compose(['exec', '-T', owner, 'uname', '-a']);
  assert.match(linux, /^Linux /);
  await record('node-kernel', { linux });
  const initial = await eventually('the durable process is ready', child);
  assert.equal(initial.binarySha256, digest);
  await fillWorkers();
} catch (error) {
  await record('measurement-error', { error: error.stack });
  await cluster.diagnostics('durable-home-capacity', error);
  throw error;
} finally {
  await cluster.close();
}

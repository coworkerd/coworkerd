import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Cluster, eventually, json, request } from './cluster.mjs';

const [baseline, candidate, ...extra] = process.argv.slice(2);
assert.ok(baseline && candidate && extra.length === 0,
  'Usage: node tests/cluster/durable-object-sql-heap-measure.mjs <baseline-kernel> <candidate-kernel>');

const source = `
  import { DurableObject } from 'cloudflare:workers';
  export class Ledger extends DurableObject {
    instance = crypto.randomUUID();
    fetch(request) {
      const sql = this.ctx.storage.sql;
      if (request.url.endsWith('/seed')) {
        sql.exec('CREATE TABLE ledger (id INTEGER PRIMARY KEY, value INTEGER)');
        sql.exec('INSERT INTO ledger VALUES (1, 0)');
      }
      const write = request.method === 'POST';
      let value;
      if (write) {
        value = sql.exec('UPDATE ledger SET value = value + 1 WHERE id = 1 RETURNING value').one().value;
      } else {
        for (let i = 0; i < 8; i++) {
          value = sql.exec('SELECT value FROM ledger WHERE id = ?', 1).one().value;
        }
      }
      return Response.json({ instance: this.instance, value });
    }
  }
  export default { fetch(request, env) {
    return env.LEDGER.getByName('hot').fetch(request);
  } };
`;
const intervalMs = 10;
const samples = 1200;
const controller = new AbortController();
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}

async function digest(file) {
  const hash = createHash('sha256');
  for await (const bytes of createReadStream(file)) hash.update(bytes);
  return hash.digest('hex');
}

async function measure(label, kernel, round) {
  const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-do-sql-heap-')), kernel,
    controller.signal, { singleNode: true, threadedNodes: ['node-1'],
      configureNode: () => ({ durableObjects: { threadedStorage: true, processes: 1, peers: 0 } }) });
  const [node] = cluster.nodes;
  try {
    await cluster.start();
    const tenant = await cluster.tenant('object-sql-measure');
    await cluster.deploy(tenant, 'ledger', source, { wrangler: {
      durable_objects: { bindings: [{ name: 'LEDGER', class_name: 'Ledger' }] },
      migrations: [{ tag: 'v1', new_sqlite_classes: ['Ledger'] }],
    } });
    const child = async () => {
      const { processes } = await cluster.state(node);
      const known = processes.entries.filter(row => row.processClass === 'durable');
      assert.equal(known.length, 1);
      assert.equal(known[0].state, 'ready');
      return known[0];
    };
    const binarySha256 = await digest(join(kernel, 'coworkerd-threaded'));
    const prior = await eventually('the durable child reports the kernel under test', async () => {
      const current = await child();
      assert.equal(current.binarySha256, binarySha256);
      return current;
    });
    const call = async (operation, suffix = '') => json(await request(cluster.urls[node],
      `/${tenant.name}/ledger${suffix}`, { method: operation === 'write' ? 'POST' : 'GET' }),
    200, operation);
    const initial = await call('read', '/seed');
    for (let i = 0; i < 50; i++) await call('write');
    for (let i = 0; i < 100; i++) await call('read');
    const ticks = async () => {
      const line = await cluster.compose(['exec', '-T', node, 'cat', `/proc/${prior.pid}/stat`]);
      const fields = line.slice(line.lastIndexOf(')') + 2).split(' ');
      return Number(fields[11]) + Number(fields[12]);
    };
    let expected = 50;
    for (const operation of ['read', 'write']) {
      const before = await ticks();
      const started = performance.now();
      const pending = [];
      for (let i = 0; i < samples; i++) {
        controller.signal.throwIfAborted();
        const due = started + i * intervalMs;
        const wait = due - performance.now();
        if (wait > 0) await delay(Math.ceil(wait));
        pending.push((async () => {
          const sent = performance.now();
          try {
            const value = await call(operation);
            assert.equal(value.instance, initial.instance);
            return { value: value.value, elapsedMs: performance.now() - sent, lateMs: sent - due };
          } catch (error) {
            return { error: error.message };
          }
        })());
      }
      const rows = await Promise.all(pending);
      const after = await ticks();
      const current = await child();
      assert.deepEqual([current.pid, current.generation], [prior.pid, prior.generation]);
      const successful = rows.filter(row => row.error === undefined);
      const times = successful.map(row => row.elapsedMs).sort((a, b) => a - b);
      const percentile = (arr, p) => arr[Math.ceil(arr.length * p) - 1];
      console.log(JSON.stringify({ label, round, operation, binarySha256, pid: current.pid,
        generation: current.generation, rate: 1000 / intervalMs, offers: samples,
        successes: successful.length, failures: rows.length - successful.length,
        p50Ms: percentile(times, 0.5), p99Ms: percentile(times, 0.99),
        p99ScheduleDelayMs: percentile(successful.map(row => row.lateMs).sort((a, b) => a - b), 0.99),
        processCpuTicks: after - before }));
      assert.equal(successful.length, samples, JSON.stringify(rows.find(row => row.error)));
      if (operation === 'write') {
        assert.deepEqual(successful.map(row => row.value).sort((a, b) => a - b),
          Array.from({ length: samples }, (_, i) => expected + i + 1));
        expected += samples;
      } else {
        assert.ok(successful.every(row => row.value === expected));
      }
    }
    assert.equal((await call('read')).value, expected);
  } finally {
    await cluster.close();
  }
}

for (const [round, order] of [[1, [['baseline', baseline], ['candidate', candidate]]],
  [2, [['candidate', candidate], ['baseline', baseline]]]]) {
  for (const [label, kernel] of order) await measure(label, resolve(kernel), round);
}

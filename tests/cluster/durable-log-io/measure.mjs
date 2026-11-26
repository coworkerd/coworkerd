import assert from 'node:assert/strict';
import { constants } from 'node:fs';
import { access, appendFile, chmod, mkdir, mkdtemp, readdir, readFile, writeFile } from 'node:fs/promises';
import { Agent, request } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Cluster, eventually } from '../cluster.mjs';

const [kernel, output, volume, probe, objects] = process.argv.slice(2).map(path => resolve(path));
assert.ok(kernel && output && volume && probe && objects,
  'usage: measure.mjs KERNEL REPORT.jsonl LOG_VOLUME TRACE.so OBJECT_VOLUME');
assert.equal(process.platform, 'linux');
await access(volume, constants.R_OK);
assert.deepEqual(await readdir(volume), []);
await writeFile(output, '', { flag: 'wx' });
const directory = await mkdtemp(join(tmpdir(), 'coworkerd-log-io-'));
const trace = join(directory, 'trace');
await mkdir(trace);
await chmod(trace, 0o777);
const bind = (source, target, read_only = false) => ({ type: 'bind', source, target, read_only });
const cluster = new Cluster(directory, kernel, undefined, {
  singleNode: true, threadedNodes: ['node-1'],
  configureNode: () => ({
    memoryBudgetBytes: 16 * 1024 ** 3,
    quarantineProcesses: 1,
    placement: { rootLimit: 512, nestedLimit: 1024 },
    processPool: { count: 1, inFlightLimit: 512, threadedMemoryBytes: 3 * 1024 ** 3 },
    durableObjects: { processes: 1, threadedStorage: true, peers: 0, segmentBytes: 512 * 1024 ** 2, segmentIntervalMs: 3600000 },
  }),
  nodeEnvironment: () => ({
    COWORKERD_DO_TEST_HOME_COUNT: '16',
    COWORKERD_DO_TEST_MAILBOX_EVENTS: '64',
    COWORKERD_DO_TEST_MAILBOX_BYTES: '1048576',
    COWORKERD_LOG_IO_TRACE: '/tmp/log-io-probe/events.bin',
    LD_PRELOAD: '/tmp/log-io-trace.so',
  }),
  nodeVolumes: () => [
    bind(objects, '/var/lib/coworkerd/do'),
    bind(volume, '/var/lib/coworkerd/do/log'),
    bind(probe, '/tmp/log-io-trace.so', true),
    bind(trace, '/tmp/log-io-probe'),
  ],
});
const [owner] = cluster.nodes;
const agent = new Agent({ keepAlive: true, maxSockets: 32 });
const source = `
  import { DurableObject } from 'cloudflare:workers';
  export class Rows extends DurableObject {
    fetch(request) {
      const { pathname, searchParams } = new URL(request.url);
      const sql = this.ctx.storage.sql;
      if (pathname.endsWith('/preflight')) return Response.json(1);
      if (pathname.endsWith('/seed')) {
        sql.exec('CREATE TABLE sample(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)');
        sql.exec('INSERT INTO sample VALUES (1, 1)');
        return Response.json(1);
      }
      if (pathname.endsWith('/insert')) {
        return Response.json(sql.exec('INSERT INTO sample VALUES (?, 1)', Number(searchParams.get('id'))).rowsWritten);
      }
      if (pathname.endsWith('/count')) return Response.json(sql.exec('SELECT count(*) AS n FROM sample').one().n);
      return Response.json(sql.exec('SELECT value FROM sample WHERE id = 1').one().value);
    }
  }
  export default {
    fetch(request, env) { return env.ROWS.getByName('table').fetch(request); },
  };
`;
const clock = () => Number(process.hrtime.bigint());
const ms = ns => ns / 1e6;
const reader = 'log-io-reader';
const writer = 'log-io-writer';
const latency = values => {
  const sorted = values.sort((a, b) => a - b);
  return { p50Ms: ms(sorted[Math.floor(sorted.length * 0.5)]),
    p99Ms: ms(sorted[Math.floor(sorted.length * 0.99)]), maxMs: ms(sorted.at(-1)) };
};

function call(tenant, operation, query = '') {
  return new Promise((accept, reject) => {
    const start = clock();
    const http = request(cluster.urls[owner] + '/' + tenant + '/rows/' + operation + query, {
      agent, method: ['seed', 'insert'].includes(operation) ? 'POST' : 'GET',
    }, response => {
      response.setEncoding('utf8');
      let data = '';
      response.on('data', chunk => { data += chunk; });
      response.once('error', reject);
      response.on('end', () => {
        if (response.statusCode !== 200) return reject(new Error(tenant + ' ' + operation + ' HTTP ' + response.statusCode + ' ' + data));
        accept({ start, end: clock(), value: JSON.parse(data) });
      });
    });
    http.once('error', reject);
    http.setTimeout(10000, () => http.destroy(new Error(tenant + ' ' + operation + ' timed out')));
    http.end();
  });
}

async function listEvents() {
  const raw = await readFile(join(trace, 'events.bin'));
  const events = [];
  for (let offset = 0; offset + 32 <= raw.length; offset += 32) {
    const [kind, start, end, main] = [0, 8, 16, 24].map(i => Number(raw.readBigUInt64LE(offset + i)));
    assert.ok([1, 2, 4].includes(kind), 'the real log fdatasync succeeded');
    events.push({ kind, start, end, main: Boolean(main) });
  }
  return events;
}

async function save(row) {
  const line = JSON.stringify(row);
  await appendFile(output, line + '\n');
  console.log(line);
}

let nextId = 2;
async function exercise(clients, durationMs, withReader) {
  const begin = clock();
  const end = begin + durationMs * 1e6;
  const batches = await Promise.all(Array.from({ length: clients + Number(withReader) }, async (_, index) => {
    const reading = index === clients;
    const samples = [];
    do {
      const sample = reading ? await call(reader, 'read') : await call(writer, 'insert', '?id=' + nextId++);
      assert.equal(sample.value, 1);
      samples.push(sample);
    } while (clock() < end);
    return samples;
  }));
  return { begin, end, reads: withReader ? batches.pop() : [], writes: batches.flat() };
}

function inside(samples, begin, end) {
  return samples.filter(sample => sample.start >= begin && sample.end <= end);
}

async function measure(clients, durationMs, round, withReader = false) {
  await exercise(clients, 1000, withReader);
  const { begin, end, writes, reads } = await exercise(clients, durationMs, withReader);
  await call(reader, 'read'); // Return the main loop to polling after the timed work.
  const events = await listEvents();
  const flushes = events.filter(event => event.kind === 2 && event.end >= begin && event.start <= end);
  const gaps = events.filter(event => event.kind === 1 && event.end >= begin && event.start <= end);
  const appends = events.filter(event => event.kind === 4 && event.end >= begin && event.start <= end);
  const measured = inside(writes, begin, end);
  assert.ok(flushes.length && appends.length && measured.length);
  const overlap = inside(reads, begin, end).filter(read =>
    flushes.some(flush => read.start >= flush.start && read.end <= flush.end));
  await save({ round, clients, durationMs, withReader, completed: measured.length,
    perSecond: measured.length * 1000 / durationMs, insert: latency(measured.map(({ start, end }) => end - start)),
    appends: appends.length, appendOnMain: appends.filter(event => event.main).length,
    flushes: flushes.length, flushOnMain: flushes.filter(event => event.main).length,
    flush: latency(flushes.map(({ start, end }) => end - start)),
    longestMainPollGapMs: Math.max(0.1, ...gaps.map(({ start, end }) => ms(end - start))),
    reads: inside(reads, begin, end).length, readsEntirelyInsideFlush: overlap.length,
    ...(overlap.length && { example: {
      read: overlap[0], flush: flushes.find(flush => overlap[0].start >= flush.start && overlap[0].end <= flush.end),
    } }),
  });
}

try {
  await cluster.start();
  for (const name of [reader, writer]) {
    const tenant = await cluster.tenant(name, { workers: 1, durableObjectNamespaces: 1 });
    await cluster.deploy(tenant, 'rows', source, { wrangler: {
      durable_objects: { bindings: [{ name: 'ROWS', class_name: 'Rows' }] },
      migrations: [{ tag: 'v1', new_sqlite_classes: ['Rows'] }],
    } });
    await eventually('route active for ' + name, async () => {
      const response = await fetch(cluster.urls[owner] + '/' + name + '/rows/preflight');
      assert.equal(response.status, 200);
      assert.equal(await response.json(), 1);
    });
    assert.equal((await call(name, 'seed')).value, 1);
    assert.equal((await call(name, 'read')).value, 1);
  }
  const start = await cluster.state(owner);
  assert.equal(start.membership.members.filter(member => member.state === 'active').length, 1);
  for (let round = 1; round <= 3; ++round) {
    for (const clients of round % 2 ? [1, 16] : [16, 1]) await measure(clients, 5000, round);
  }
  await measure(1, 7000, 1, true);
  assert.equal((await call(writer, 'count')).value, nextId - 1, 'every successful insert is visible');
  await save({ confirmedInserts: nextId - 2, readerTenant: reader, writerTenant: writer });
} catch (error) {
  if (cluster.started) {
    try {
      console.error(await cluster.compose(['logs', '--no-color', '--tail', '60', owner]));
    } catch (logsError) {
      console.error(logsError.message);
    }
  }
  await cluster.diagnostics('durable-log-io', error);
  throw error;
} finally {
  agent.destroy();
  await cluster.close();
}

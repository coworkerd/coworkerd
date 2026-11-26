import assert from 'node:assert/strict';
import { appendFile, chmod, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { Agent, request } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Cluster, eventually } from '../cluster.mjs';

const [kernel, reports, tier, probe, baselineBin, candidateBin, baselineLog, candidateLog, baselineObjects, candidateObjects] =
  process.argv.slice(2);
assert.ok(candidateObjects, 'usage: measure.mjs KERNEL REPORTS TIER TRACE.so BASELINE_BIN CANDIDATE_BIN ' +
  'BASELINE_LOG CANDIDATE_LOG BASELINE_OBJECTS CANDIDATE_OBJECTS');
assert.ok(['normal', 'slow'].includes(tier));
assert.equal(process.platform, 'linux');

const clock = () => Number(process.hrtime.bigint());
const ms = ns => ns / 1e6;
const bind = (source, target, read_only = false) => ({ type: 'bind', source: resolve(source), target, read_only });
const reader = 'log-io-reader';
const writer = 'log-io-writer';
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
    async fetch(request, env) {
      const { pathname, search } = new URL(request.url);
      const result = await env.ROWS.getByName('table').fetch('http://rows' + pathname + search, { method: request.method });
      return Response.json(await result.json(), { status: result.status });
    },
  };
`;

const types = ['', 'daemon', 'poll', 'write', 'writev', 'pwrite', 'fdatasync', 'fsync', 'syncfs'];
async function listEvents(fixture) {
  const raw = await readFile(join(fixture.trace, 'events.bin'));
  const events = [];
  for (let offset = 0; offset + 240 <= raw.length; offset += 240) {
    const [kind, start, end, tid, cpuNs, failed] = [0, 8, 16, 24, 32, 40]
      .map(i => Number(raw.readBigUInt64LE(offset + i)));
    const data = raw.subarray(offset + 48, offset + 240);
    const file = data.subarray(0, data.indexOf(0)).toString();
    assert.ok(types[kind] && !failed, 'traced I/O succeeded');
    events.push({ call: types[kind], start, end, tid, cpuNs, file });
  }
  assert.equal(events[0]?.call, 'daemon', 'daemon trace header');
  const { tid: main } = events.shift();
  return { main, events };
}

const append = event => ['write', 'writev', 'pwrite'].includes(event.call) && event.file.endsWith('.log');
const flush = event => event.call === 'fdatasync' && event.file.endsWith('.log');
const within = (event, { begin, end }) => event.end >= begin && event.start <= end;
const overlaps = (first, second) => first.start < second.end && second.start < first.end;

async function fixture(side, binaries, volume, objects) {
  assert.deepEqual(await readdir(volume), [], 'fresh log directory');
  const output = join(reports, side + '-' + tier + '.jsonl');
  await writeFile(output, '', { flag: 'wx' });
  const directory = await mkdtemp(join(tmpdir(), 'coworkerd-log-io-'));
  const trace = await mkdtemp('/dev/shm/coworkerd-log-io-');
  await chmod(trace, 0o777);
  const cluster = new Cluster(directory, resolve(kernel), undefined, {
    singleNode: true, threadedNodes: ['node-1'], binariesDirectory: resolve(binaries),
    configureNode: () => ({
      memoryBudgetBytes: 16 * 1024 ** 3,
      quarantineProcesses: 1,
      placement: { rootLimit: 512, nestedLimit: 1024 },
      processPool: { count: 1, inFlightLimit: 512, threadedMemoryBytes: 3 * 1024 ** 3 },
      durableObjects: { processes: 1, threadedStorage: true, peers: 0, segmentBytes: 512 * 1024 ** 2,
        segmentIntervalMs: 3600000 },
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
  return { side, cluster, trace, output, owner: cluster.nodes[0], nextId: 2, windows: [] };
}

function call(fixture, tenant, operation, agent, query = '') {
  return new Promise((accept, reject) => {
    const start = clock();
    const http = request(fixture.cluster.urls[fixture.owner] + '/' + tenant + '/rows/' + operation + query, {
      agent, method: ['seed', 'insert'].includes(operation) ? 'POST' : 'GET',
    }, response => {
      response.setEncoding('utf8');
      let data = '';
      response.on('data', chunk => { data += chunk; });
      response.once('error', reject);
      response.on('end', () => {
        if (response.statusCode !== 200) return reject(new Error(tenant + ' ' + operation + ' HTTP ' + response.statusCode + ' ' + data));
        try {
          accept({ start, end: clock(), value: JSON.parse(data) });
        } catch (error) {
          reject(error);
        }
      });
    });
    http.once('error', reject);
    http.setTimeout(10000, () => http.destroy(new Error(tenant + ' ' + operation + ' timed out')));
    http.end();
  });
}

async function setup(fixture) {
  const { cluster, owner } = fixture;
  await cluster.start();
  for (const name of [reader, writer]) {
    const tenant = await cluster.tenant(name, { workers: 1, durableObjectNamespaces: 1 });
    await cluster.deploy(tenant, 'rows', source, { wrangler: {
      durable_objects: { bindings: [{ name: 'ROWS', class_name: 'Rows' }] },
      migrations: [{ tag: 'v1', new_sqlite_classes: ['Rows'] }],
    } });
    await eventually('route active for ' + name, async () => {
      assert.equal((await call(fixture, name, 'preflight', false)).value, 1);
    });
    assert.equal((await call(fixture, name, 'seed', false)).value, 1);
    assert.equal((await call(fixture, name, 'read', false)).value, 1);
  }
  const state = await cluster.state(owner);
  assert.equal(state.membership.members.filter(member => member.state === 'active').length, 1);
  assert.equal((await listEvents(fixture)).main, state.server.pid);
}

function latency(values) {
  const sorted = values.sort((a, b) => a - b);
  const percentile = fraction => ms(sorted[Math.floor(sorted.length * fraction)]);
  return { p50Ms: percentile(0.5), p99Ms: percentile(0.99), maxMs: ms(sorted.at(-1)) };
}

async function save(fixture, row) {
  const line = JSON.stringify(row);
  await appendFile(fixture.output, line + '\n');
  console.log(line);
}

async function exercise(fixture, agent, clients, durationMs, withReader) {
  const begin = clock();
  const end = begin + durationMs * 1e6;
  const batches = await Promise.all(Array.from({ length: clients + Number(withReader) }, async (_, index) => {
    const reading = index === clients;
    const samples = [];
    do {
      const sample = reading ? await call(fixture, reader, 'read', agent) :
        await call(fixture, writer, 'insert', agent, '?id=' + fixture.nextId++);
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

async function measure(fixture, clients, durationMs, round, withReader = false) {
  const agent = new Agent({ keepAlive: true, maxSockets: clients + Number(withReader) });
  let window;
  try {
    await exercise(fixture, agent, clients, 1000, withReader);
    window = await exercise(fixture, agent, clients, durationMs, withReader);
  } finally {
    agent.destroy();
  }
  const { begin, end, writes, reads } = window;
  fixture.windows.push({ clients, round, withReader, begin, end });
  await call(fixture, reader, 'read', false); // Return to polling after the timed work.
  const { main, events } = await listEvents(fixture);
  const timed = events.filter(event => within(event, window));
  const appends = timed.filter(append);
  const flushes = timed.filter(flush);
  const peak = timed.filter(event => event.call === 'poll').reduce((max, event) =>
    event.end - event.start > max.end - max.start ? event : max, { start: 0, end: 100000, cpuNs: 0 });
  const measured = inside(writes, begin, end);
  assert.ok(flushes.length && appends.length && measured.length);
  const overlap = inside(reads, begin, end).filter(read =>
    flushes.some(sync => read.start >= sync.start && read.end <= sync.end));
  await save(fixture, { side: fixture.side, round, clients, durationMs, withReader, begin, end, completed: measured.length,
    perSecond: measured.length * 1000 / durationMs, insert: latency(measured.map(({ start, end }) => end - start)),
    appends: appends.length, appendOnMain: appends.filter(event => event.tid === main).length,
    flushes: flushes.length, flushOnMain: flushes.filter(event => event.tid === main).length,
    flush: latency(flushes.map(({ start, end }) => end - start)),
    longFlushes: flushes.filter(event => event.end - event.start > 40e6).length,
    longestMainPollGapMs: ms(peak.end - peak.start), longestGap: peak,
    reads: inside(reads, begin, end).length, readsEntirelyInsideFlush: overlap.length,
    ...(overlap.length && { example: {
      read: overlap[0], flush: flushes.find(sync => overlap[0].start >= sync.start && overlap[0].end <= sync.end),
    } }),
  });
}

async function saveLongFlushes(fixtures) {
  const files = await Promise.all(fixtures.map(async fixture => {
    const { main, events } = await listEvents(fixture);
    return events.filter(event => event.file).map(event =>
      ({ side: fixture.side, main: event.tid === main, ...event }));
  }));
  const all = files.flat();
  const candidate = fixtures.find(fixture => fixture.side === 'candidate');
  const rows = [];
  for (const event of all) {
    if (event.side !== 'candidate' || !flush(event) || event.end - event.start <= 40e6) continue;
    const window = candidate.windows.find(window => within(event, window));
    if (!window) continue;
    rows.push({ window, flush: event,
      overlapping: all.filter(other => other !== event && overlaps(other, event)) });
  }
  await writeFile(join(reports, tier + '-long-flushes.jsonl'), rows.map(row => JSON.stringify(row) + '\n').join(''), { flag: 'wx' });
  console.log(tier + ': attributed ' + rows.length + ' candidate flushes above 40 ms');
}

const fixtures = [];
try {
  fixtures.push(await fixture('baseline', baselineBin, baselineLog, baselineObjects));
  fixtures.push(await fixture('candidate', candidateBin, candidateLog, candidateObjects));
  // Use the opposite starting build on the other volume.
  if (tier === 'slow') fixtures.reverse();
  for (const version of fixtures) await setup(version);
  // One-client windows all precede the heavier insert load. Alternate builds
  // at every concurrency on this volume.
  for (const [clients, rounds] of [[1, 5], [16, 3]]) {
    for (let round = 1; round <= rounds; ++round) {
      for (const version of fixtures) await measure(version, clients, 5000, round);
    }
  }
  for (const version of fixtures) await measure(version, 1, 7000, 1, true);
  await saveLongFlushes(fixtures);
  for (const version of fixtures) {
    assert.equal((await call(version, writer, 'count', false)).value, version.nextId - 1, 'every successful insert is visible');
    await save(version, { confirmedInserts: version.nextId - 2, readerTenant: reader, writerTenant: writer });
  }
} catch (error) {
  for (const { cluster, owner } of fixtures) {
    if (!cluster.started) continue;
    try {
      console.error(await cluster.compose(['logs', '--no-color', '--tail', '60', owner]));
    } catch (logsError) {
      console.error(logsError.message);
    }
    await cluster.diagnostics('durable-log-io', error);
  }
  throw error;
} finally {
  await Promise.all(fixtures.map(async ({ cluster, trace }) => {
    try {
      await cluster.close();
    } finally {
      await rm(trace, { recursive: true, force: true });
    }
  }));
}

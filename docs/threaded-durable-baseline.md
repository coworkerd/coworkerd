# Baseline for threaded Durable Object processes

This is the comparison run for the [design](threaded-durable-processes.md). It uses product commit
`dfc8803731a33feaaf8796313a0053d403f5ba8a` and the published Linux x86_64 kernel pinned by that checkout.
The kernel release is `kernel-v1.20261114.1-aebfc411b650`. The archive SHA-256 is
`d4c39e0ea39c379df6062fcb66d286119a2fdc887a378bce9cb698a35e7a6fce`.
[Kernel release](https://github.com/coworkerd/coworkerd/releases/tag/kernel-v1.20261114.1-aebfc411b650).

## Conditions

Ubuntu 24.04.3 LTS x86_64, Linux 6.8.0-149-generic, one Intel Xeon E5-2689 at 2.60 GHz with eight
cores and 16 logical CPUs, 62.7 GiB of RAM, Docker 29.9.3, Compose 5.7.1, Node.js 22.17.0.
The release build of the daemons used Clang 19.1.1 and the pinned Cap'n Proto from the product build.
The published kernel came from the checkout's checksum-verifying fetch command. Before the latency run the
host had 69 GiB disk space and 58.4 GiB available RAM. The one-minute load average was 1.56.
Both one-second CPU samples before the run were 97% idle.

The public integration fixture ran fresh volumes, isolated networking and dynamic loopback ports.
The measurement enabled only one durable slot across three nodes to force all test objects and
databases onto the same process. This also leaves an empty selected peer list, so the seed writes
used the local log flush path. Measurements are loopback client round trips through the public
Worker and SQL interfaces. Warm and cold here refer to the tenant Worker isolate and a locally
reopened object. Shard takeover and history replay latency have a separate failure workload in
[#21](https://github.com/coworkerd/coworkerd/issues/21).

The script uses nearest-rank p50 and p99, including response body consumption. With fewer than 100
samples, p99 is the sample maximum. It reports request attempts that fail checks as experiment failures.

## Results

The default resident report on the timed idle run went from 140 objects to zero in 12 s below the
4,096 target. The process generation was unchanged. The reopened HTTP objects had a new constructor
marker and the original module marker and value. The node's history rebuild counter did not increase
during that measurement.

| Local reconstruction | Successful samples | p50 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: |
| HTTP, Worker isolate warm | 120 | 14.877 | 19.088 |
| HTTP, Worker isolate cold | 40 | 69.974 | 74.354 |
| Application message on a timer-hibernated WebSocket | 20 | 6.148 | 7.967 |

Each cold-isolate sample rotated only the durable child and waited for its new ready generation before
starting the clock. The first call reloaded the Worker, observed a new module marker and returned the
saved value. The node history rebuild counter did not increase in this cohort either.

All 20 established sockets remained open, returned a pong with the original protocol ping payload
and returned the configured application auto-response. After these frames, both the resident count
and the wake counter were still zero. The subsequent application message observed a new actor marker
in the original isolate. This measures round-trip wake latency. The threaded gate
adds a constructor counter and requires the node itself to handle the automatic traffic.

### Resident capacity

The capacity runs use explicit IDs with the same leading 64 bits. All IDs route to shard zero, so
the measurement does not spend most of its time acquiring new shards. They read the live generation's
`durableLog.resident` report, stop after a process change, and record the durable PID and generation.

With the default target of 4,096, the fixture held most actors active with a four-minute
`waitUntil`. It made 4,300 attempts in groups of 24 over 115.338 s. The sampled peak and final
resident count were both 3,703. The durable process stayed in generation 1 and used all 11,136
of its available descriptors. The hard limit was 524,288. The test received 595 `DO_UNAVAILABLE`
responses and one `STORAGE_UNAVAILABLE`. Reopening its oldest object returned HTTP 503
`DO_UNAVAILABLE` with SQLite's `SQLITE_CANTOPEN`. This run hit the descriptor limit before
the process had enough resident actors to trigger the LRU target. With the fixture's normal
container limits, the node raises the child's inherited soft limit to a minimum calculated for
dispatch and process generations. [Limit calculation](../node/main.c++#L8999).

Raising only the container's descriptor limit to 65,536 let the same workload reach a sampled peak
of 4,099 with the target still at 4,096. During the next group the old child exited with
`watchdog timeout`, and generation 2 replaced generation 1. The run stopped after 4,129 attempts
over 154.953 s, with 23 `RESULT_UNCERTAIN` and five `STORAGE_UNAVAILABLE` responses. The observed
overshoot demonstrates that the setting is a target. The watchdog exit makes this a useful failure
case for the new process's saturation gate.

With `durableObjects.residentObjects=64`, 160 distinct requests took 6.185 s. The sample reached 64
by request 81 and remained at 64 after all requests, with 96 cumulative evictions. One initial
request returned `STORAGE_UNAVAILABLE`. The first object reopened with a new actor marker in the
same Worker isolate, before the upstream idle timer would have retired it. This demonstrates
replacement at the configured target.

### Long SQL under the current limits

Each of 120 pairs ran one quiet probe of each type, then began a large recursive calculation on
a separate database. The busy probes were sent together 25 ms later with the blocker still pending,
in the same durable generation. All 120 blocker queries returned HTTP 503 `SQL_LIMIT_EXCEEDED`
at the configured 200 ms CPU limit. Their client p50 was 227.807 ms and p99 was 231.280 ms.
The client overlap check uses a pending blocker response, with its start offset controlled by the
script. All 120 probes in each column succeeded.

| Probe | Quiet p50 (ms) | Quiet p99 (ms) | Busy p50 (ms) | Busy p99 (ms) | p99 increase (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| A different DB, same tenant | 25.462 | 31.132 | 206.867 | 211.224 | 180.092 |
| A different DB, other tenant | 24.915 | 31.613 | 206.173 | 210.077 | 178.464 |
| Durable Object, same child | 11.273 | 17.029 | 206.439 | 210.700 | 193.671 |

The default budget interrupted every attempt before 30 seconds. A 30-second overlap is therefore
unavailable on this default baseline. The design's acceptance test runs a 30-second statement
on the native database path and requires actual statement/probe overlap.

## Reproduce

On Linux x86_64 with the build dependencies in [CONTRIBUTING](../CONTRIBUTING.md), Docker, and Node.js
22.17.0, use a disposable checkout. The benchmark uses the same image build, CLI deploy and teardown
as the cluster integration fixture. Install the npm dependencies from the lockfile. Copy the script
below into `tests/cluster/issue16-baseline.mjs` in that checkout.

```sh
git clone https://github.com/coworkerd/coworkerd.git
cd coworkerd
git checkout --detach dfc8803731a33feaaf8796313a0053d403f5ba8a
npm ci --prefix cli --registry=https://registry.npmjs.org
kernel=$(python3 scripts/fetch-kernel.py)
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=4
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD="$kernel/workerd"
node tests/cluster/issue16-baseline.mjs "$kernel"
node tests/cluster/issue16-baseline.mjs "$kernel" capacity
node tests/cluster/issue16-baseline.mjs "$kernel" capacity 64
node tests/cluster/issue16-baseline.mjs "$kernel" capacity 4096 high-fd
```

On a shared host, use its measurement lock if available by setting `ISSUE16_MEASURE_LOCK` to the
lockfile. The optional lock uses shared mode for setup/cleanup and exclusive mode for the measurements.

```js
import assert from 'node:assert/strict';
import { readFile, writeFile, mkdtemp } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { execFileSync, spawn } from 'node:child_process';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { once } from 'node:events';
import { Cluster, eventually, json, request } from './cluster.mjs';

const require = createRequire(new URL('../../cli/package.json', import.meta.url));
const WebSocket = require('ws');
const dir = await mkdtemp(join(resolve('.'), 'build', 'issue16-baseline-'));
const cluster = new Cluster(dir, resolve(process.argv[2]));
const capacity = process.argv[3] === 'capacity';
const target = Number(process.argv[4] ?? 4096);
const highFd = process.argv[5] === 'high-fd';
const compose = cluster.compose.bind(cluster);
cluster.compose = async (args, options) => {
  if (args[0] === 'up') {
    for (const node of cluster.nodes) {
      const path = join(dir, node + '.json');
      const config = JSON.parse(await readFile(path, 'utf8'));
      config.durableObjects = { processes: node === 'node-1' ? 1 : 0 };
      if (capacity && target !== 4096) config.durableObjects.residentObjects = target;
      await writeFile(path, JSON.stringify(config));
    }
    if (highFd) {
      const override = JSON.parse(await readFile(cluster.override, 'utf8'));
      override.services['node-1'].ulimits = { nofile: { soft: 65536, hard: 65536 } };
      await writeFile(cluster.override, JSON.stringify(override));
    }
  }
  return compose(args, options);
};

let release = async () => {};
async function lock(mode) {
  await release();
  release = async () => {};
  if (!process.env.ISSUE16_MEASURE_LOCK) return;
  const holder = spawn('flock', [mode, '-w', '60', process.env.ISSUE16_MEASURE_LOCK,
    'sh', '-c', 'printf ready; cat >/dev/null'], { stdio: ['pipe', 'pipe', 'inherit'] });
  const exited = once(holder, 'exit');
  release = async () => { holder.stdin.end(); await exited; };
  const ready = await Promise.race([once(holder.stdout, 'data').then(([data]) => data.toString()), exited]);
  assert.equal(ready, 'ready', 'measurement lock was acquired');
}

const node = 'node-1';
const sockets = [];
const stats = values => {
  const sorted = [...values].sort((a, b) => a - b);
  const quantile = p => +sorted[Math.ceil(p * sorted.length) - 1].toFixed(3);
  return { n: sorted.length, p50: quantile(.5), p99: quantile(.99), max: quantile(1) };
};
const report = (label, value) => console.log(label, JSON.stringify(value));
const actorIdleMs = 12000;
const time = async fn => {
  const start = performance.now();
  const value = await fn();
  return { value, ms: performance.now() - start };
};
const exchange = (ws, send, event = 'message') => {
  const reply = once(ws, event, { signal: AbortSignal.timeout(5000) });
  send();
  return reply.then(([data]) => data.toString());
};
const source = `
import { DurableObject } from 'cloudflare:workers';
let moduleId;
export class Record extends DurableObject {
  actorId = crypto.randomUUID();
  async fetch(request) {
    if (request.headers.get('upgrade') === 'websocket') {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('auto', 'automatic'));
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    if (new URL(request.url).pathname.split('/').at(-1).startsWith('fill-')) {
      this.ctx.waitUntil(new Promise(resolve => setTimeout(resolve, 60000)));
    }
    if (request.method === 'POST') await this.ctx.storage.put('value', new URL(request.url).pathname);
    return Response.json(await this.info());
  }
  async info() {
    return { actorId: this.actorId, moduleId: moduleId ??= crypto.randomUUID(), value: (await this.ctx.storage.get('value')) ?? null };
  }
  async webSocketMessage(ws) { ws.send(JSON.stringify(await this.info())); }
}
export default { fetch(request, env) {
  const name = new URL(request.url).pathname.split('/').at(-1);
  return env.RECORDS.get(env.RECORDS.idFromName(name)).fetch(request);
} };
`;

try {
  await lock('-s');
  await cluster.start();
  const tenant = await cluster.tenant('baseline-a');
  const other = await cluster.tenant('baseline-b');
  const worker = capacity ? source
    .replace('60000', '240000')
    .replace("startsWith('fill-')", "startsWith('fill-') && !request.url.includes('?idle')")
    .replace('env.RECORDS.idFromName(name)', "name.startsWith('fill-') ? env.RECORDS.idFromString(name.slice(5)) : env.RECORDS.idFromName(name)")
    : source;
  await cluster.deploy(tenant, 'objects', worker, { wrangler: {
    durable_objects: { bindings: [{ name: 'RECORDS', class_name: 'Record' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record'] }],
  } });
  const path = '/' + tenant.name + '/objects/';
  const call = async (name, method = 'GET') =>
    json(await request(cluster.urls[node], path + name, { method, timeout: 35000 }), 200, name);
  const durableSlot = state => state.processes.entries.find(row => row.processClass === 'durable');
  const slot = async () => durableSlot(await cluster.state(node));
  const residency = async () => {
    const state = await cluster.state(node);
    return state.durableLog.resident.find(row => row.processId === 'd0-' + durableSlot(state).generation);
  };
  await eventually('object deployment', () => call('probe'));
  await lock('-x');
  for (const [program, args] of [['uptime', []], ['free', ['-m']], ['df', ['-h', '.']], ['vmstat', ['1', '3']]]) {
    process.stdout.write(execFileSync(program, args));
  }
  const firstSlot = await slot();
  if (capacity) {
    const name = index => 'fill-' + index.toString(16).padStart(64, '0');
    const samples = [];
    let attempted = 1;
    const errors = {};
    const attempts = target === 4096 ? 4300 : 160;
    const group = target === 4096 ? 24 : 8;
    const started = performance.now();
    const first = await call(name(0) + '?idle');
    for (let i = 1; i < attempts; i += group) {
      const end = Math.min(attempts, i + group);
      const answers = await Promise.allSettled(Array.from({ length: end - i }, (_, n) =>
        call(name(i + n) + (target === 4096 && i + n < 4176 ? '' : '?idle'))));
      for (const answer of answers) {
        if (answer.status === 'rejected') {
          const summary = answer.reason.message.match(/"code":"([^"]+)"/)?.[1] ?? answer.reason.name;
          errors[summary] = (errors[summary] ?? 0) + 1;
        }
      }
      attempted = end;
      if ((end - 1) % (group * 10) === 0 || end > 3900 || end === attempts) {
        const process = await slot();
        const row = await residency();
        if (process.generation !== firstSlot.generation || process.pid !== firstSlot.pid || !row) {
          report('capacity-generation-changed', { attempted, process, previousResidency: samples.at(-1), reported: row });
          break;
        }
        samples.push(row.objects);
        report('fill-' + end, row);
      }
    }
    await delay(150);
    const full = await residency();
    const lastSlot = await slot();
    const durationMs = performance.now() - started;
    const sampledPeak = Math.max(full?.objects ?? 0, ...samples);
    if (lastSlot.generation !== firstSlot.generation || lastSlot.pid !== firstSlot.pid) {
      const trace = await cluster.compose(['logs', '--no-color', '--tail', '1000', node]);
      for (const line of trace.split('\n')) {
        if (line.includes('[process]') && line.includes('"processId":"d0"')) console.log(line);
      }
      report('capacity', { target, highFd, attempted, durationMs, sampledPeak, full, errors,
        pidBefore: firstSlot.pid, pidAfter: lastSlot.pid, generationBefore: firstSlot.generation,
        generationAfter: lastSlot.generation });
    } else {
      const proc = '/proc/' + lastSlot.pid;
      const limits = await cluster.compose(['exec', '-T', '--user', '0', node, 'cat', proc + '/limits']);
      const nofile = limits.match(/^Max open files\s+(\d+)\s+(\d+)/m);
      assert.ok(nofile);
      const descriptors = await cluster.compose(['exec', '-T', '--user', '0', node, 'ls', '-1', proc + '/fd']);
      const again = await request(cluster.urls[node], path + name(0) + '?idle', { timeout: 35000 });
      const value = JSON.parse(again.text);
      if (again.status === 200) assert.equal(value.moduleId, first.moduleId);
      report('capacity', { target, highFd, attempted, durationMs, sampledPeak, full, errors,
        pid: lastSlot.pid, generation: lastSlot.generation, softFdLimit: Number(nofile[1]),
        hardFdLimit: Number(nofile[2]), openFds: descriptors.split('\n').length, firstAgainStatus: again.status,
        firstReopened: again.status === 200 ? value.actorId !== first.actorId : null,
        firstAgainError: again.status === 200 ? null : value.code,
        firstAgainReason: again.status === 200 ? null : value.reason });
    }
  } else {

    const saved = [];
    for (let i = 0; i < 120; i++) saved.push(await call('saved-' + i, 'POST'));
    const connected = [];
    for (let i = 0; i < 20; i++) {
      const url = new URL(path + 'socket-' + i, cluster.urls[node]);
      url.protocol = 'ws:';
      const ws = new WebSocket(url);
      sockets.push(ws);
      ws.on('error', () => {});
      await once(ws, 'open', { signal: AbortSignal.timeout(8000) });
      connected.push(JSON.parse(await exchange(ws, () => ws.send('info'))));
    }
    const startIdle = await residency();
    await delay(actorIdleMs);
    const endIdle = await residency();
    report('idle-below-capacity', { milliseconds: actorIdleMs, before: startIdle, after: endIdle });

    for (const ws of sockets) {
      assert.equal(await exchange(ws, () => ws.ping('ping'), 'pong'), 'ping');
      assert.equal(await exchange(ws, () => ws.send('auto')), 'automatic');
    }
    const afterAutomatic = await residency();
    report('after-automatic-frames', afterAutomatic);
    assert.equal(afterAutomatic.objects, 0);
    assert.equal(afterAutomatic.woken, endIdle.woken);
    const wsTimes = [];
    for (let i = 0; i < sockets.length; i++) {
      const ws = sockets[i];
      const sample = await time(() => exchange(ws, () => ws.send('info')));
      const info = JSON.parse(sample.value);
      assert.notEqual(info.actorId, connected[i].actorId);
      assert.equal(info.moduleId, connected[i].moduleId);
      wsTimes.push(sample.ms);
      ws.close();
    }
    report('socket-wake-ms', stats(wsTimes));

    const warm = [];
    const localRebuilds = async () => (await cluster.state(node)).durableLog.objects.rebuilds;
    const warmRebuildsBefore = await localRebuilds();
    for (let i = 0; i < saved.length; i++) {
      const sample = await time(() => call('saved-' + i));
      assert.notEqual(sample.value.actorId, saved[i].actorId);
      assert.equal(sample.value.moduleId, saved[i].moduleId);
      assert.equal(sample.value.value, saved[i].value);
      warm.push(sample.ms);
    }
    report('local-reopen-warm-isolate-ms', { ...stats(warm), nodeImageRebuilds: (await localRebuilds()) - warmRebuildsBefore });

    const quiet = [[], [], []];
    const busy = [[], [], []];
    const rawSql = (who, db, statement) => request(cluster.urls.apiserver, who.path + '/databases/' + db + '/query',
      { method: 'POST', token: who.token, body: { sql: statement }, timeout: 35000 });
    const sql = async (who, db, statement) => json(await rawSql(who, db, statement), 200, db);

    for (const [who, db] of [[tenant, 'blocker'], [tenant, 'same'], [other, 'other']]) {
      await cluster.api(who.path + '/databases/' + db, { method: 'POST', token: who.token });
      await eventually('database ' + db + ' ready', async () => assert.equal((await sql(who, db, 'SELECT 1 AS n')).success, true));
    }
    const probes = [
      async () => assert.equal((await sql(tenant, 'same', 'SELECT 1 AS n')).success, true),
      async () => assert.equal((await sql(other, 'other', 'SELECT 1 AS n')).success, true),
      () => call('probe'),
    ];
    const slow = 'WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n<1000000000) SELECT sum(n) FROM r';
    const costs = [];
    let overlapped = 0;
    let sampleError;
    for (let i = 0; i < 120; i++) {
      const sampleProbes = async target =>
        await Promise.all(probes.map(async (probe, index) => target[index].push((await time(probe)).ms)));
      await sampleProbes(quiet);
      let settled = false;
      const blocker = time(() => rawSql(tenant, 'blocker', slow)).finally(() => { settled = true; });
      await delay(25);
      if (!settled) overlapped++;
      await sampleProbes(busy);
      const sample = await blocker;
      assert.equal(sample.value.status, 503);
      assert.equal(JSON.parse(sample.value.text).code, 'SQL_LIMIT_EXCEEDED');
      sampleError ??= JSON.parse(sample.value.text).reason;
      costs.push(sample.ms);
    }
    assert.equal((await slot()).generation, firstSlot.generation);
    report('sql-ms', { blocker: stats(costs), sampleError, overlapped, quiet: quiet.map(stats), busy: busy.map(stats) });

    const cold = [];
    let previousModuleId = saved[0].moduleId;
    const coldRebuildsBefore = await localRebuilds();
    for (let i = 0; i < 40; i++) {
      const before = await slot();
      const rotated = await request(cluster.urls[node], '/__coworkerd/rotate?process=d0',
        { method: 'POST', token: cluster.management });
      assert.ok(rotated.status >= 200 && rotated.status < 300, rotated.text);
      await eventually('new d0', async () => {
        const running = await slot();
        assert.ok(running.state === 'ready' && running.generation > before.generation);
      }, { interval: 40 });
      const sample = await time(() => call('saved-' + i));
      assert.notEqual(sample.value.moduleId, previousModuleId);
      assert.equal(sample.value.value, saved[i].value);
      previousModuleId = sample.value.moduleId;
      cold.push(sample.ms);
    }
    report('local-reopen-cold-isolate-ms', { ...stats(cold), nodeImageRebuilds: (await localRebuilds()) - coldRebuildsBefore });

  }
} catch (error) {
  await cluster.diagnostics();
  throw error;
} finally {
  await lock('-s');
  for (const ws of sockets) ws.terminate();
  try { await cluster.close(); } finally { await release(); }
}
```

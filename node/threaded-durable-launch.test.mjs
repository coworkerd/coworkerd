/*
Disposable threaded Durable Object fixture

The `cpu-events` mode calibrates finite work from native home-thread CPU samples. Events on
one actor interleave across awaits: their combined CPU must exceed a single event budget while
each stays below it, then one event alone exceeds its budget across repeated awaits. The latter
must reset only its actor tree, preserving stored state, the neighboring object and the ready child.

The `load-cancel` mode holds the first definition's cold load open with the source delay hook.
A TCP reset from its only caller ends the home's session and cancels the load. A second
definition on the same home must answer with its own code, the abandoned load must be counted
as canceled, and the first definition must still load afterwards on the other home.

The positional single-node fixture opts in with `durableObjects.threadedTestStorage: true`.
The node creates a fresh temporary stack for the run and refuses object-store configuration, peers, and
an existing Durable Object storage directory in the fixture stack. Use this setting only with disposable
fixture deployments. Each threaded durable child starts without a persistent log channel.
The `events` mode checks an RPC read, input gating, and an RPC round trip through a neighboring
object that calls back while its caller is suspended. The `rpc-output-gate` mode observes a pending
storage confirmation before the RPC result can arrive, then independently reads the written value.
The `rpc-cpu-events` mode applies the calibrated CPU checks to method calls across awaits.
The fixture derives local namespaces from the active declarations and routes them only within the node.
Each child generation opens a fresh SQLite volume inside the fixture stack.
The `http-output-gate` mode delays the test-storage completion callback by 300 ms and observes its pending
counter before accepting a response to an unawaited write. This default-zero hook is limited to the test
role and at most 1000 ms; it does not simulate peer acknowledgments, a durable log or crash recovery.
The `http 4 64` fixture mode writes and reads two objects, checks unchanged instances after a brief
idle interval, then observes three real idle retirements and wakes. Each first wake request alternates
between a read and a write that verifies the previous stored value before replacing it. Repeated reads
check the new instance and stored value, while the child must retain its ready PID and generation.
It does not send JavaScript RPC calls.
The disposable role defaults to four home loops, capped by the startup thread limit, and 64 roots per
copy. Cold placement closes at 64 MiB or 3000 ms busy time in a four-second window. The per-object
waiting mailbox defaults to 512 events/2 MiB. Reservations keep 256 descriptors plus five per home
outside admission, then charge 16 KiB/four descriptors per root and 4 KiB/one descriptor per event plus
declared waiting bytes. Facets reserve their own metadata and descriptors. The capacity, queue,
pressure and overlap measurements in PR #43 cover these reservation costs. Actual body execution
remains subject to the copy/process memory limits.
The `http-overlap` mode observes 16 fully read and written 16 KiB bodies held across four facets while
same-object and neighboring-object requests continue. It records RSS, threads and descriptors, verifies
facet state and exact event-credit return, and checks the original child remains ready. The ordinary
ingress fixture uses 32 call slots so its concurrency limit does not hide durable overlap.
The `group-refusal` mode injects one empty V8-group factory result only in the disposable role, then
requires returned reservations, successful retry and the original ready child. This does not simulate
physical address-space exhaustion; `COWORKERD_DO_TEST_REFUSE_GROUP` is zero by default.
The fixture can vary the home and mailbox limits with `COWORKERD_DO_TEST_HOME_COUNT`,
`COWORKERD_DO_TEST_TREES_PER_COPY`, `COWORKERD_DO_TEST_MAILBOX_EVENTS` and
`COWORKERD_DO_TEST_MAILBOX_BYTES`. `COWORKERD_DO_TEST_PLACEMENT_BYTES` and
`COWORKERD_DO_TEST_BUSY_MS` control placement, and `COWORKERD_DO_TEST_FD_HEADROOM` sets the
descriptor headroom before accounting for the home loops. These settings apply to disposable runs.

On Linux, run `node node/threaded-durable-launch.test.mjs "$NODE" "$KERNEL_DIR/workerd" runtime/runtime.js measure 4 64`
to fill one durable child with four copies of 64 real objects each. The tool checks the next object's admission after an idle retirement
and prints baseline and saturated thread count, RSS, descriptors, and admission counters.

The `placement` mode alternates bounded synchronous work on one object with requests to new objects.
It checks quiet-copy reuse, a distinct copy after the home becomes busy, pinned existing actors,
returned event credits, and an unchanged ready child without actor resets or idle retirement.
For example, `node node/threaded-durable-launch.test.mjs "$NODE" "$KERNEL_DIR/workerd" runtime/runtime.js placement 4 64 3000`
uses four homes, 64 roots per copy, and a 3000 ms busy threshold in the four-second window.
The last argument can vary the threshold for measurements; output includes every work and cold-request
duration plus the child's thread count, RSS, descriptors, and admission counters. These request durations
include transport and scheduling, so they are not direct samples of the kernel's synchronous busy clock.
With `placement-control 4 64 4000`, the same workload runs 32 rounds and must keep one copy;
the four-second threshold excludes busy-triggered splitting while leaving capacity and memory limits unchanged.

The queue modes set a four-event and 64 KiB mailbox. The `queues 4 64` mode measures admission for
64 objects with four events each, declaring 16 KiB per
event. It checks the four-event count limit separately using one-byte declarations, with another object
still admitted. This is a measurement before request bodies are read, not an actor execution or payload
allocation workload. It records reservations, RSS, threads and descriptors, cancels the queued calls,
and checks that reservations and descriptors return to baseline. The final argument varies the object count.
The `queue-limit 4 64` mode sets a descriptor limit of 512 on its durable child. It asserts refusal
at the reservation ceiling, continued control access,
256 descriptors of headroom and full credit return. `queue-memory 4 256` sets a 128 MiB process budget
and checks refusal at its 16 MiB admission memory ceiling. These queue modes verify the original ready
PID and generation after release. They do not execute Worker events or exercise V8 allocation failure.
*/

import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmod, copyFile, mkdir, mkdtemp, readFile, readdir, rm, symlink, unlink, writeFile } from 'node:fs/promises';
import { get } from 'node:http';
import { createConnection, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';

const [nodeBinary, workerd, runtime, scenario, homeArg = '8', treeArg = '64', busyArg = '3000'] = process.argv.slice(2);
const measure = scenario === 'measure';
const httpLifecycle = scenario === 'http';
const homeReservation = scenario === 'home-reservation';
const warmCopies = scenario === 'warm-copies';
const events = scenario === 'events';
const rpcCpu = scenario === 'rpc-cpu-events';
const cpuEvents = scenario === 'cpu-events' || rpcCpu;
const loadCancel = scenario === 'load-cancel';
const httpIsolation = scenario === 'http-isolation';
const rpcOutputGate = scenario === 'rpc-output-gate';
const outputGate = scenario === 'http-output-gate' || rpcOutputGate;
const httpMemory = scenario === 'http-memory';
const groupRefusal = scenario === 'group-refusal';
const overlap = scenario === 'http-overlap';
const memoryPlacementMiB = httpMemory ? Number(busyArg) : 64;
if (httpMemory) assert.ok(Number.isInteger(memoryPlacementMiB) && memoryPlacementMiB >= 8 && memoryPlacementMiB <= 64);
const boundedEvents = events || httpIsolation;
const queueLimit = scenario === 'queue-limit';
const queueMemory = scenario === 'queue-memory';
const queues = scenario === 'queues' || queueLimit || queueMemory;
const mailboxBytes = queues ? 64 * 1024 : 2 * 1024 * 1024;
const placementControl = scenario === 'placement-control';
const placement = scenario === 'placement' || placementControl;
const exerciseEvents = loadCancel || cpuEvents || warmCopies || boundedEvents || measure || placement || httpLifecycle || outputGate || httpMemory || groupRefusal || overlap;
const homeCount = Number(homeArg);
const treesPerHome = Number(treeArg);
if (measure || placement) {
  assert.ok(Number.isInteger(homeCount) && homeCount >= 1 && homeCount <= 32);
  assert.ok(Number.isInteger(Number(busyArg)) && Number(busyArg) > 0 && Number(busyArg) <= 4000);
  assert.ok(Number.isInteger(treesPerHome) && treesPerHome >= 2 && treesPerHome <= 128);
}
if (placementControl) assert.equal(Number(busyArg), 4000);
if (queues) assert.ok(Number.isInteger(treesPerHome) && treesPerHome >= 1 && treesPerHome <= 4096);
const exec = promisify(execFile);
const scratch = await mkdtemp(join(tmpdir(), 'cwkd-durable-'));
const env = {
  ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(COWORKERD_|WORKERD_)/.test(key))),
  TMPDIR: scratch,
  ...(outputGate ? { COWORKERD_DO_TEST_COMMIT_DELAY_MS: '300' } : {}),
  ...(groupRefusal ? { COWORKERD_DO_TEST_REFUSE_GROUP: '1' } : {}),
  ...(overlap ? { COWORKERD_DO_TEST_HOME_COUNT: '4' } : {}),
  ...(homeReservation || warmCopies ? { COWORKERD_DO_TEST_HOME_COUNT: '2' } : {}),
  ...(loadCancel ? { COWORKERD_DO_TEST_HOME_COUNT: '2', COWORKERD_THREADED_SOURCE_DELAY_FOR_TEST: '2500' } : {}),
  ...(httpMemory ? { COWORKERD_DO_TEST_PLACEMENT_BYTES: String(memoryPlacementMiB * 1024 * 1024) } : {}),
  ...(queues ? { COWORKERD_DO_TEST_HOME_COUNT: homeArg, COWORKERD_DO_TEST_MAILBOX_EVENTS: '4',
    COWORKERD_DO_TEST_MAILBOX_BYTES: String(mailboxBytes) } : {}),
  ...(queueMemory ? { COWORKERD_THREADED_MEMORY_BYTES: String(128 * 1024 * 1024) } : {}),
  ...(cpuEvents ? { COWORKERD_CPU_LIMIT_MS: '250', COWORKERD_THREADED_TRACE_FOR_TEST: '1',
    COWORKERD_DO_TEST_HOME_COUNT: '1' } : {}),
  ...(boundedEvents ? {
    COWORKERD_CPU_LIMIT_MS: '250',
    COWORKERD_THREADED_TRACE_FOR_TEST: '1',
  } : {}),
  ...(measure || placement || httpLifecycle || httpMemory ? {
    COWORKERD_DO_TEST_HOME_COUNT: homeArg,
    COWORKERD_DO_TEST_TREES_PER_COPY: treeArg,
    ...(placement ? { COWORKERD_DO_TEST_BUSY_MS: busyArg } : {}),
  } : {}),
};
const fixture = join(scratch, 'fixture');
const configPath = join(fixture, 'node.json');
const manifestPath = join(fixture, 'workers', 'manifest.json');
const threaded = resolve(dirname(workerd), 'coworkerd-threaded');
const descriptorLimitedThreaded = join(fixture, 'threaded-fd-limit');
const workerId = 'counter';
const token = 'fixture-management-placeholder';
const config = {
  nodeId: 'fixture',
  ...(httpMemory ? { memoryLimitMiB: 128 } : {}),
  durableObjects: {
    processes: 1, threadedTestStorage: true, ...(measure || placement || queues || httpLifecycle || httpMemory ? { residentObjects: 0 } : {}),
  },
  // Keep enough node slots for each Worker root and its overlapping durable calls.
  ...(overlap ? { placement: { rootLimit: 128, nestedLimit: 384 } } : {}),
  processPool: { count: exerciseEvents ? 1 : 0, inFlightLimit: overlap ? 32 : 8,
    ...(queueLimit ? { threadedExecutable: descriptorLimitedThreaded } : {}) },
  quarantineProcesses: 0,
  sandbox: { mode: 'off', syscalls: { mode: 'off' } },
  peerListenAddress: '127.0.0.1:0',
  memoryWatermarkEnabled: false,
  hostCheck: { intervalMs: 0 },
  management: { token },
};

const args = port => [manifestPath, workerd, `127.0.0.1:${port}`, '127.0.0.1:0', '127.0.0.1:0', configPath];
const writeConfig = value => writeFile(configPath, JSON.stringify(value));

async function refuseStart(pattern) {
  await assert.rejects(exec(nodeBinary, args(0), { env, timeout: 3000 }), error => {
    assert.equal(error.code, 1);
    assert.match(error.stderr, pattern);
    return true;
  });
  assert.deepEqual(await readdir(scratch), ['fixture']);
}

function reservePort() {
  return new Promise((resolve, reject) => {
    const socket = createServer();
    socket.on('error', reject);
    socket.listen(0, '127.0.0.1', () => {
      const { port } = socket.address();
      socket.close(error => error ? reject(error) : resolve(port));
    });
  });
}

function sendPreface(path, preface) {
  return new Promise((resolve, reject) => {
    const socket = createConnection(path, () => socket.write(preface));
    socket.setTimeout(1000, () => socket.destroy(new Error('admission response timed out')));
    socket.once('error', reject);
    socket.once('data', bytes => {
      socket.setTimeout(0);
      resolve({ socket, receipt: bytes.toString() });
    });
    socket.once('end', () => reject(new Error('admission closed without a response')));
  });
}

function readThreaded(socketPath) {
  return new Promise((resolve, reject) => {
    const socket = createConnection(socketPath, () => {
      socket.write('GET /__coworkerd/threaded HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n');
    });
    const chunks = [];
    socket.on('data', chunk => chunks.push(chunk));
    socket.once('error', reject);
    // The test also counts the child's open descriptors. The response body can
    // arrive before the child closes the probe's own descriptor.
    socket.once('end', () => {
      try {
        const response = Buffer.concat(chunks).toString();
        assert.match(response, /^HTTP\/1\.1 200 /);
        const headersEnd = response.indexOf('\r\n\r\n');
        assert.ok(headersEnd > 0);
        resolve(JSON.parse(response.slice(headersEnd + 4)));
      } catch (error) {
        reject(error);
      }
    });
  });
}

async function readAdmission(socketPath) {
  return (await readThreaded(socketPath)).durableAdmission;
}

async function readProcessResources(pid) {
  const status = await readFile(`/proc/${pid}/status`, 'utf8');
  return {
    threads: Number(status.match(/^Threads:\s+(\d+)/m)[1]),
    rssKiB: Number(status.match(/^VmRSS:\s+(\d+) kB/m)[1]),
    descriptors: (await readdir(`/proc/${pid}/fd`)).length,
  };
}

function durablePreface(objectId, invocationId, queuedBytes, expiry = process.hrtime.bigint() + 5_000_000_000n, definition = 'fixture') {
  return 'D' + JSON.stringify({
    namespaceId: 'a'.repeat(64), objectId, invocationId, className: 'Counter', definition,
    uniqueKey: 'coworkerd-do-root-v1-' + 'a'.repeat(64), storageId: objectId,
    generation: '1', dataEpoch: '1', shardEpoch: '1', openId: '1',
    queuedBytes: String(queuedBytes), expiresMonotonicNs: String(expiry),
  }) + '\n';
}

async function waitForAdmission(path, waiting, homeSessions = 0, { running } = {}) {
  let state;
  const until = performance.now() + 2000;
  do {
    state = await readAdmission(path);
    if (state.waiting === waiting && state.homeSessions === homeSessions &&
        (running === undefined || state.running === running)) return state;
    await delay(20);
  } while (performance.now() < until);
  const expected = JSON.stringify({ waiting, homeSessions, running });
  assert.fail(`Admission counters did not reach ${expected}: ${JSON.stringify(state)}`);
}

let child, exited;
let diagnostics = '';
try {
  const modules = join(fixture, 'workers', workerId, 'v1');
  await mkdir(modules, { recursive: true });
  if (queueLimit) {
    await copyFile(new URL('./threaded-durable-fd-limit.test.sh', import.meta.url), descriptorLimitedThreaded);
    await chmod(descriptorLimitedThreaded, 0o755);
    await symlink(threaded, `${descriptorLimitedThreaded}.target`);
  }
  await mkdir(join(fixture, 'runtime'));
  await copyFile(runtime, join(fixture, 'runtime', 'runtime.js'));
  const source = exerciseEvents ? `
    import { DurableObject } from "cloudflare:workers";
    const DEFINITION = "first";
    let isolate;
    let inputGate = "idle";
    export class Counter extends DurableObject {
      instance = crypto.randomUUID();
      latch;
      async fetch(request) {
        isolate ??= crypto.randomUUID();
        const query = new URL(request.url).searchParams;
        if (query.has("definition")) return Response.json({definition: DEFINITION, isolate});
        if (query.has("sliceWaiting")) return Response.json(this.slicePart ?? 0);
        if (query.has("sliceRelease")) {
          this.sliceLatch.resolve();
          return Response.json("released");
        }
        if (query.has("sliceWork")) {
          return Response.json(await this.sliceWork(Number(query.get("sliceWork")), Number(query.get("parts"))));
        }
        if (query.has("work")) return Response.json(this.work(Number(query.get("work"))));
        if (query.has("allocateMiB")) {
          const size = Number(query.get("allocateMiB"));
          if (!Number.isInteger(size) || size < 1 || size > 120) {
            return new Response("invalid allocation size", {status: 400});
          }
          this.allocation = new Uint8Array(size * 1024 * 1024);
          this.allocation.fill(1);
        }
        if (query.has("hold")) {
          this.latch = Promise.withResolvers();
          if (query.has("cancelable")) {
            request.signal.addEventListener("abort", () => {
              this.aborts = (this.aborts ?? 0) + 1;
              this.latch.resolve();
            }, {once: true});
          }
          await this.latch.promise;
          this.latch = undefined;
        }
        if (query.has("waiting")) return Response.json(this.latch !== undefined);
        if (query.has("aborts")) return Response.json(this.aborts ?? 0);
        if (query.has("release")) this.latch.resolve();
        if (query.has("nested")) {
          const next = this.env.COUNTERS.get(this.env.COUNTERS.idFromName("/other"));
          return Response.json(await next.value());
        }
        if (query.has("facet")) {
          const facet = this.ctx.facets.get("facet", () => ({class: this.ctx.exports.Counter}));
          return facet.fetch(query.has("overlapHold")
            ? "https://fixture.invalid/?overlapHold"
            : query.has("overlapState") ? "https://fixture.invalid/?overlapState"
            : "https://fixture.invalid/", request.method === "POST"
            ? { method: "POST", body: await request.text() } : {});
        }
        if (query.has("overlapState")) return Response.json(this.heldPayloads ?? 0);
        if (query.has("overlapHold")) {
          const payload = await request.text();
          await this.ctx.storage.put("overlap", payload);
          this.heldPayloads = (this.heldPayloads ?? 0) + 1;
          try { await new Promise(resolve => setTimeout(resolve, 2500)); }
          finally { --this.heldPayloads; }
          return Response.json({instance: this.instance, isolate, bytes: payload.length});
        }
        if (query.has("gateState")) return Response.json(inputGate);
        if (query.has("releaseGate")) {
          inputGate = "released";
          return Response.json(inputGate);
        }
        if (query.has("inputGate")) {
          await this.ctx.blockConcurrencyWhile(async () => {
            inputGate = "held";
            try {
              while (inputGate === "held") await scheduler.wait(5);
              await this.ctx.storage.put("counter", "after input gate");
            } finally { inputGate = "idle"; }
          });
        }
        if (query.has("outputGate")) {
          this.ctx.storage.put("outputGate", "http write");
          return Response.json("http committed");
        }
        if (query.has("readOutputGate")) {
          return Response.json(await this.ctx.storage.get("outputGate"));
        }
        if (query.has("cpu")) {
          await Promise.resolve();
          for (;;) {}
        }
        if (query.has("sqlCpu")) {
          this.ctx.storage.sql.exec("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000000000) SELECT count(*) FROM n").one();
        }
        if (query.has("expect") && await this.ctx.storage.get("counter") !== query.get("expect")) {
          return new Response("previous value mismatch", {status: 409});
        }
        if (request.method === "POST") await this.ctx.storage.put("counter", await request.text());
        return Response.json({instance: this.instance, isolate, counter: await this.ctx.storage.get("counter")});
      }
      work(iterations) {
        if (!Number.isInteger(iterations) || iterations < 1 || iterations > 200000000) {
          throw new Error("invalid work size");
        }
        let checksum = 1;
        for (let i = 0; i < iterations; ++i) checksum = Math.imul(checksum, 1664525) + i | 0;
        return {instance: this.instance, isolate, checksum};
      }
      async sliceWork(iterations, parts) {
        if (![2, 4].includes(parts)) throw new Error("invalid slices");
        for (let part = 1; part <= parts; ++part) {
          this.sliceChecksum = this.work(iterations).checksum;
          this.slicePart = part;
          if (part < parts) {
            this.sliceLatch = Promise.withResolvers();
            await this.sliceLatch.promise;
          }
        }
        this.slicePart = 0;
        return {instance: this.instance, isolate, counter: await this.ctx.storage.get("counter")};
      }
      writeThroughRpc() {
        this.ctx.storage.put("outputGate", "rpc write");
        return "rpc committed";
      }
      async roundTrip() {
        this.order = ["begin"];
        const peer = this.env.COUNTERS.get(this.env.COUNTERS.idFromName("/other"));
        const instance = await peer.callBack();
        this.order.push("end");
        return {order: this.order, instance};
      }
      async callBack() {
        const caller = this.env.COUNTERS.get(this.env.COUNTERS.idFromName("/first"));
        return await caller.recordNested();
      }
      recordNested() { this.order.push("nested"); return this.instance; }
      async value() { return this.ctx.storage.get("counter"); }
    }
    export default { async fetch(request, env) {
      const url = new URL(request.url);
      const counter = env.COUNTERS.get(env.COUNTERS.idFromName(url.pathname));
      try {
        const query = url.searchParams;
        if (query.has("rpcWrite")) return Response.json(await counter.writeThroughRpc());
        if (query.has("roundTrip")) return Response.json(await counter.roundTrip());
        if (query.has("rpcCpu")) {
          return Response.json(query.has("work")
            ? await counter.work(Number(query.get("work")))
            : await counter.sliceWork(Number(query.get("sliceWork")), Number(query.get("parts"))));
        }
        return query.has("rpc")
          ? Response.json(await counter.value())
          : await counter.fetch(request);
      } catch (error) {
        return Response.json({error: String(error), code: error.code},
          {status: error.code === "CPU_LIMIT_EXCEEDED" ? 503 : 502});
      }
    } };
  ` : 'export default { fetch() { return new Response("fixture"); } };';
  const hash = value => createHash('sha256').update(value).digest('hex');
  const version = { versionId: 'v1', mainModule: 'index.js', contentHash: hash(source), bindings: [] };
  await writeFile(join(modules, 'index.js'), source);
  if (exerciseEvents) {
    const packageManifest = JSON.stringify({ modules: [
      { bytes: Buffer.byteLength(source), name: 'index.js', sha256: hash(source), type: 'esm' },
    ] });
    await writeFile(join(modules, 'package.json'), packageManifest);
    Object.assign(version, {
      format: 'v2', compatibilityDate: '2026-08-27', contentHash: hash(packageManifest),
      // Incoming cancellation and forwarding its signal are opt-ins for this date.
      ...(httpIsolation ? {compatibilityFlags: ['enable_request_signal', 'request_signal_passthrough']} : {}),
      durableObjects: [{ binding: 'COUNTERS', className: 'Counter' }], classes: { declared: ['Counter'] },
    });
  }
  const routes = [{ host: '*', pathPrefix: '/', workerId }];
  const workers = [{ workerId, activeVersion: 'v1', versions: [version] }];
  if (warmCopies || loadCancel) {
    const secondId = 'second-counter';
    const secondModules = join(fixture, 'workers', secondId, 'v1');
    await mkdir(secondModules, {recursive: true});
    const secondSource = source.replace('DEFINITION = "first"', 'DEFINITION = "second"') +
      '\n// A distinct fixture definition.\n';
    assert.ok(secondSource.includes('DEFINITION = "second"'));
    const secondPackage = JSON.stringify({modules: [
      {bytes: Buffer.byteLength(secondSource), name: 'index.js', sha256: hash(secondSource), type: 'esm'},
    ]});
    await writeFile(join(secondModules, 'index.js'), secondSource);
    await writeFile(join(secondModules, 'package.json'), secondPackage);
    routes.unshift({host: '*', pathPrefix: '/warm-second', workerId: secondId});
    workers.push({workerId: secondId, activeVersion: 'v1',
      versions: [{...version, contentHash: hash(secondPackage)}]});
  }
  await writeFile(manifestPath, JSON.stringify({compatibilityDate: '2026-08-27', routes, workers}));

  await writeConfig({ ...config, storage: { endpoint: 'http://127.0.0.1:1' } });
  await refuseStart(/requires the positional single-node fixture and disposable local storage/);
  await writeConfig(config);
  const productionDirectory = join(fixture, 'do');
  await mkdir(productionDirectory);
  await writeFile(join(productionDirectory, 'format'), 'existing state');
  await refuseStart(/refuses an existing Durable Object storage directory/);
  assert.equal(await readFile(join(productionDirectory, 'format'), 'utf8'), 'existing state');
  await rm(productionDirectory, { recursive: true });
  await symlink('missing-storage', productionDirectory);
  await refuseStart(/refuses an existing Durable Object storage directory/);
  await unlink(productionDirectory);

  await assert.rejects(exec(threaded, ['4', 'unused', 'd0:1', '1', '--durable-test-storage'], {
    env: { ...env, COWORKERD_DO_WAL_FD: '9' },
    timeout: 3000,
  }), error => {
    assert.equal(error.code, 78);
    assert.match(error.stderr, /refuses a persistent durable log channel/);
    return true;
  });

  const port = await reservePort();
  child = spawn(nodeBinary, args(port), { env, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
  for (const stream of [child.stdout, child.stderr]) {
    stream.on('data', bytes => { diagnostics = (diagnostics + bytes).slice(-5000); });
  }
  let done = false;
  exited = new Promise(resolve => {
    child.on('error', error => { diagnostics += error.message; });
    child.on('close', () => { done = true; resolve(); });
  });
  let ready;
  for (const until = performance.now() + 8000; !done && performance.now() < until;) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
        headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(500),
      });
      const state = await response.json();
      if (response.ok && state.processes.entries.every(entry => entry.state === 'ready')) {
        ready = state.processes.entries;
        break;
      }
    } catch {}
    await delay(30);
  }
  assert.ok(ready, diagnostics);
  assert.equal(ready.length, exerciseEvents ? 2 : 1);
  const processInfo = ready.find(entry => entry.processClass === 'durable');
  assert.equal(processInfo.processClass, 'durable');
  assert.equal(processInfo.generation, 1);
  assert.equal(processInfo.sandbox.durableServer, 'threaded-test');
  assert.equal(processInfo.sandbox.threadedProtocol, 3);
  assert.equal(processInfo.sandbox.defaultCpuLimitMs, boundedEvents || cpuEvents ? 250 : 30000);
  const environment = (await readFile(`/proc/${processInfo.pid}/environ`, 'utf8')).split('\0');
  assert.ok(environment.some(value => value.startsWith('COWORKERD_PARENT_FD=')));
  assert.ok(environment.every(value => !value.startsWith('COWORKERD_DO_WAL_FD=')));
  const paths = (await readdir(scratch)).filter(name => name.startsWith('coworkerd-durable-test-'));
  assert.equal(paths.length, 1);
  const stack = join(scratch, paths[0]);
  const socketPath = join(stack, 'run', processInfo.socket);
  assert.equal((await readAdmission(socketPath)).workerThreads,
    Number(env.COWORKERD_DO_TEST_HOME_COUNT ?? Math.min(4, config.processPool.inFlightLimit)));
  const ordinary = await sendPreface(socketPath, 'T');
  ordinary.socket.destroy();
  assert.equal(ordinary.receipt, 'R');
  const objectId = 'b'.repeat(64);
  const held = [];
  try {
    for (const [id, sequence, bytes, expected] of [
      [objectId, 'first', mailboxBytes / 2, 'A'],
      [objectId, 'second', mailboxBytes / 2, 'A'],
      [objectId, 'overflow', 1, 'R'],
      ['c'.repeat(64), 'unrelated', 1024, 'A'],
    ]) {
      const { socket, receipt } = await sendPreface(socketPath, durablePreface(id, sequence, bytes));
      held.push(socket);
      assert.equal(receipt, expected);
    }
    const loaded = await readAdmission(socketPath);
    assert.equal(loaded.trees, 2);
    assert.equal(loaded.waiting, 3);
    assert.equal(loaded.waitingBytes, mailboxBytes + 1024);
    assert.equal(loaded.overloaded, 1);
    assert.ok(loaded.reservedDescriptors > 0);
    assert.ok(loaded.reservedBytes > loaded.waitingBytes);
  } finally {
    for (const socket of held) socket.destroy();
  }
  let released = await waitForAdmission(socketPath, 0);
  assert.equal(released.trees, 0);
  assert.equal(released.reservedDescriptors, 0);
  assert.equal(released.reservedBytes, 0);
  const routed = await sendPreface(socketPath, durablePreface(objectId, 'home-cancellation', 32));
  try {
    assert.equal(routed.receipt, 'A');
    // Begin RPC framing, then cancel it while the home holds the connection.
    routed.socket.write(Buffer.alloc(1));
    await waitForAdmission(socketPath, 1, 1);
  } finally {
    routed.socket.destroy();
  }
  released = await waitForAdmission(socketPath, 0);
  assert.equal(released.reservedBytes, 0);
  const expiring = await sendPreface(socketPath,
    durablePreface(objectId, 'expiry', 8, process.hrtime.bigint() + 500_000_000n));
  try {
    assert.equal(expiring.receipt, 'A');
    released = await waitForAdmission(socketPath, 0);
    assert.equal(released.expired, 1);
    assert.equal(released.reservedBytes, 0);
  } finally {
    expiring.socket.destroy();
  }
  const live = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
    headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(500),
  });
  const output = await live.json();
  const durable = output.processes.entries.find(entry => entry.processClass === 'durable');
  assert.equal(durable.pid, processInfo.pid);
  assert.equal(durable.generation, processInfo.generation);
  assert.equal(durable.state, 'ready');
  if (homeReservation) {
    const homeThreads = async () => {
      const tasks = join('/proc', String(processInfo.pid), 'task');
      const rows = await Promise.all((await readdir(tasks)).map(async tid =>
        [(await readFile(join(tasks, tid, 'comm'), 'utf8')).trim(), Number(tid)]));
      return rows.filter(([name]) => name === 'cwkd-call').map(([, tid]) => tid).sort((a, b) => a - b);
    };
    // Observe actual Linux home loops, not just the reservation table's thread dimension.
    const originalThreads = await homeThreads();
    assert.equal(originalThreads.length, 2);
    const keep = [];
    const claim = async (id, definition) => {
      const connection = await sendPreface(socketPath,
        durablePreface(id.repeat(64), `home-${id}`, 0, undefined, definition));
      keep.push(connection.socket);
      return connection;
    };
    try {
      for (const [id, definition] of [['1', 'definition-one'], ['2', 'definition-two']]) {
        const connection = await claim(id, definition);
        assert.equal(connection.receipt, 'A');
        connection.socket.write(Buffer.alloc(1));
      }
      const occupied = await waitForAdmission(socketPath, 2, 2);
      assert.equal(occupied.homes, 2);
      assert.deepEqual(await homeThreads(), originalThreads);
      const refused = await claim('3', 'definition-three');
      assert.equal(refused.receipt, 'R',
        'a definition without a reservable home must be refused before acknowledgement');
      const bounded = await readAdmission(socketPath);
      assert.equal(bounded.trees, 2);
      assert.equal(bounded.waiting, 2);
      assert.equal(bounded.homes, 2);
      assert.equal(bounded.reservedBytes, occupied.reservedBytes);
      assert.equal(bounded.reservedDescriptors, occupied.reservedDescriptors);
      assert.deepEqual(await homeThreads(), originalThreads, 'refusal must not create another home thread');
    } finally {
      for (const socket of keep) socket.destroy();
    }
    const drained = await waitForAdmission(socketPath, 0);
    assert.equal(drained.trees, 0);
    assert.equal(drained.reservedBytes, 0);
    assert.equal(drained.reservedDescriptors, 0);
    const retry = await claim('3', 'definition-three');
    try {
      assert.equal(retry.receipt, 'A');
      const reserved = await readAdmission(socketPath);
      assert.equal(reserved.homes, 1, 'the home must already be reserved when acknowledgement arrives');
    } finally { retry.socket.destroy(); }
    await waitForAdmission(socketPath, 0);
    const state = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: {Authorization: `Bearer ${token}`}, signal: AbortSignal.timeout(1000),
    }).then(response => response.json());
    const live = state.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(live.pid, processInfo.pid);
    assert.equal(live.generation, processInfo.generation);
    assert.equal(live.state, 'ready');
    assert.deepEqual(await homeThreads(), originalThreads);
    console.log(JSON.stringify({event: 'home-reserved-before-ack', homes: 2,
      homeThreadIds: originalThreads, refusedDefinitions: 1, creditsReturned: true}));
  } else if (cpuEvents) {
    const request = async (path, options, accepted = [200]) => {
      if (rpcCpu && /[?&](work|sliceWork)=/.test(path)) path += '&rpcCpu';
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        ...options, signal: AbortSignal.timeout(5000),
      });
      const body = await response.text();
      assert.ok(accepted.includes(response.status), `${path}: ${response.status} ${body}\n${diagnostics}`);
      return {status: response.status, value: JSON.parse(body)};
    };
    const original = (await request('/event-owner', {method: 'POST', body: 'saved owner'})).value;
    const neighbor = (await request('/event-neighbor', {method: 'POST', body: 'saved neighbor'})).value;
    assert.equal(original.isolate, neighbor.isolate);
    const tasks = join('/proc', String(processInfo.pid), 'task');
    const tids = new Set();
    for (const tid of await readdir(tasks)) {
      if ((await readFile(join(tasks, tid, 'comm'), 'utf8')).trim() === 'cwkd-call') tids.add(Number(tid));
    }
    assert.equal(tids.size, 1);
    const samples = [];
    const cpuTurns = async () => Array.from(
      (await readFile(join(stack, 'logs', 'workerd.log'), 'utf8')).matchAll(/\[threaded-js\] (\{[^\n]*\})/g),
      match => JSON.parse(match[1]));
    const measure = async (iterations) => {
      await delay(20);
      const start = (await cpuTurns()).length;
      await request(`/event-owner?work=${iterations}`);
      await waitForAdmission(socketPath, 0);
      await delay(50);
      const cpuMs = (await cpuTurns()).slice(start).filter(row => tids.has(row.tid))
        .reduce((total, row) => total + row.cpuNs / 1e6, 0);
      assert.ok(cpuMs > 0, 'calibration requires native home-thread CPU samples');
      samples.push({iterations, cpuMs});
      return cpuMs;
    };
    await measure(1000000);
    let iterations = 10000000, sliceCpuMs;
    for (let attempt = 0; attempt < 5; ++attempt) {
      sliceCpuMs = await measure(iterations);
      if (sliceCpuMs >= 75 && sliceCpuMs <= 100) break;
      iterations = Math.min(200000000, Math.max(100000, Math.round(iterations * 85 / sliceCpuMs)));
    }
    assert.ok(sliceCpuMs >= 75 && sliceCpuMs <= 100, JSON.stringify(samples));
    const waiting = async (part, ended = () => false) => {
      for (const until = performance.now() + 2000; performance.now() < until;) {
        if (ended()) return false;
        const response = await request('/event-owner?sliceWaiting', undefined, [200, 502, 503]);
        if (response.status !== 200) return false;
        if (response.value === part) return true;
        await delay(10);
      }
      assert.fail('the sliced event did not suspend at part ' + part);
    };
    // Four calibrated slices in total exceed the actor's 250 ms budget, but no event does.
    const cpuStart = (await cpuTurns()).length;
    const within = request(`/event-owner?sliceWork=${iterations}&parts=2`);
    within.catch(() => {});
    assert.ok(await waiting(1));
    await request(`/event-owner?work=${iterations}`);
    await request(`/event-owner?work=${iterations}`);
    await request('/event-owner?sliceRelease');
    assert.deepEqual((await within).value, original);
    assert.deepEqual((await request('/event-owner')).value, original);
    assert.equal((await waitForAdmission(socketPath, 0)).actorResets, 0);
    await delay(50);
    const interleavedCpuMs = (await cpuTurns()).slice(cpuStart).filter(row => tids.has(row.tid))
      .reduce((total, row) => total + row.cpuNs / 1e6, 0);
    assert.ok(interleavedCpuMs > 250, 'the interleaved total must exceed one event budget');
    // The same event must retain its own CPU charge after repeated yields and other events.
    let ended = false;
    const over = request(`/event-owner?sliceWork=${iterations}&parts=4`, undefined, [503])
      .then(result => { ended = true; return result; });
    over.catch(() => {});
    for (let part = 1; part <= 3; ++part) {
      if (!(await waiting(part, () => ended))) break;
      await request('/event-owner?work=1');
      await request('/event-owner?sliceRelease', undefined, [200, 502, 503]);
    }
    assert.equal((await over).value.code, 'CPU_LIMIT_EXCEEDED');
    await waitForAdmission(socketPath, 0);
    await delay(300);
    const rebuilt = (await request('/event-owner')).value;
    assert.notEqual(rebuilt.instance, original.instance);
    assert.equal(rebuilt.counter, original.counter);
    assert.equal(rebuilt.isolate, original.isolate);
    assert.deepEqual((await request('/event-neighbor')).value, neighbor);
    const after = await waitForAdmission(socketPath, 0);
    assert.equal(after.actorResets, 1);
    assert.equal(after.isolateResets, 0);
    const state = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: {Authorization: `Bearer ${token}`}, signal: AbortSignal.timeout(1000),
    }).then(response => response.json());
    const sameChild = state.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    console.log(JSON.stringify({scenario, samples, interleavedCpuMs, isolatedEventCpu: true,
      cumulativeCpuAcrossAwaits: true, neighborPreserved: true, admission: after}));
  } else if (loadCancel) {
    const request = async path => {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {signal: AbortSignal.timeout(8000)});
      const body = await response.text();
      assert.equal(response.status, 200, `${path}: ${body}\n${diagnostics}`);
      return JSON.parse(body);
    };
    const before = await readThreaded(socketPath);
    assert.equal(before.durableAdmission.trees, 0);
    let caller;
    const aborted = new Promise((resolve, reject) => {
      caller = get(`http://127.0.0.1:${port}/first?definition`, {agent: false}, response => {
        response.resume();
        reject(new Error('the cold request returned a response before the client reset'));
      });
      caller.once('error', resolve);
    });
    aborted.catch(() => {});
    let resetAt;
    try {
      // The ordinary child spends its source delay first. Wait for the durable
      // home to receive the event, while its first definition is still loading.
      const until = performance.now() + 7000;
      let loading;
      do {
        loading = await readThreaded(socketPath);
        if (loading.durableAdmission.homeSessions === 1 && loading.durableAdmission.running === 1) break;
        await delay(10);
      } while (performance.now() < until);
      assert.equal(loading.durableAdmission.homeSessions, 1, 'the call reached the durable home');
      assert.equal(loading.durableAdmission.running, 1, 'the durable event started');
      assert.equal(loading.durableAdmission.residentRoots, 0);
      assert.equal(loading.compilations, before.compilations, 'the cold definition is still loading');
      resetAt = performance.now();
      caller.socket.resetAndDestroy();
      assert.equal((await aborted).code, 'ECONNRESET');
    } finally {
      caller.destroy();
    }
    let drained;
    for (const until = performance.now() + 3000; performance.now() < until;) {
      drained = await readAdmission(socketPath);
      if (drained.trees === 0 && drained.running === 0) break;
      await delay(10);
    }
    const canceledAfterMs = Math.round(performance.now() - resetAt);
    assert.equal(drained.trees, 0, `the disconnected caller must release its tree: ${JSON.stringify(drained)}`);
    assert.equal(drained.running, 0);
    assert.equal(drained.residentRoots, 0);
    // The second definition is placed on the home whose cold load lost its only waiter.
    const second = await request('/warm-second?definition');
    assert.equal(second.definition, 'second',
      'a tree must never receive the isolate of a cold load canceled for another definition');
    assert.equal((await readThreaded(socketPath)).canceledLoads, before.canceledLoads + 1,
      'a cold load must end with its last waiter');
    const first = await request('/first?definition');
    assert.equal(first.definition, 'first');
    assert.notEqual(first.isolate, second.isolate);
    assert.deepEqual(await request('/warm-second?definition'), second);
    const after = await waitForAdmission(socketPath, 0);
    assert.equal(after.homes, 2);
    assert.equal(after.actorResets, 0);
    assert.equal(after.isolateResets, 0);
    const state = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: {Authorization: `Bearer ${token}`}, signal: AbortSignal.timeout(1000),
    }).then(response => response.json());
    const sameChild = state.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    console.log(JSON.stringify({scenario, canceledColdLoad: true, definitionsKeptApart: true,
      canceledAfterMs, admission: after}));
  } else if (warmCopies) {
    const request = async (path, options) => {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        ...options, signal: AbortSignal.timeout(5000),
      });
      const body = await response.text();
      assert.equal(response.status, 200, `${path}: ${body}\n${diagnostics}`);
      return JSON.parse(body);
    };
    const paths = ['/warm-first', '/warm-second'];
    const initial = [];
    for (const path of paths) initial.push(await request(path, {method: 'POST', body: path}));
    assert.notEqual(initial[0].isolate, initial[1].isolate);
    assert.equal((await waitForAdmission(socketPath, 0)).homes, 2);
    let idle;
    for (const until = performance.now() + 15000; performance.now() < until;) {
      idle = await readAdmission(socketPath);
      if (idle.residentRoots === 0 && idle.idleRetirements >= 2) break;
      await delay(50);
    }
    assert.equal(idle.residentRoots, 0, JSON.stringify(idle));
    assert.equal(idle.trees, 0);
    assert.equal(idle.reservedBytes, 0);
    // Reverse creation order: choosing the lowest empty home would destroy both warm copies.
    for (const index of [1, 0]) {
      const woken = await request(paths[index]);
      assert.equal(woken.counter, paths[index]);
      assert.notEqual(woken.instance, initial[index].instance);
      assert.equal(woken.isolate, initial[index].isolate,
        'an empty matching warm copy must be preferred over another definition’s empty home');
      assert.deepEqual(await request(paths[index]), woken);
    }
    const current = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: {Authorization: `Bearer ${token}`}, signal: AbortSignal.timeout(1000),
    }).then(response => response.json());
    const sameChild = current.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    const after = await waitForAdmission(socketPath, 0);
    assert.equal(after.actorResets, 0);
    assert.equal(after.isolateResets, 0);
    console.log(JSON.stringify({scenario, definitions: 2, warmCopiesPreserved: true,
      statePreserved: true, admission: after}));
  } else if (httpLifecycle) {
    const request = async (path, options) => {
      const started = performance.now();
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        ...options, signal: AbortSignal.timeout(5000),
      });
      const body = await response.text();
      assert.equal(response.status, 200, `${path}: ${body}\n${diagnostics}`);
      return {value: JSON.parse(body), elapsedMs: performance.now() - started};
    };
    const paths = ['/http-first', '/http-second'];
    const records = [];
    for (const path of paths) {
      const value = `saved at startup: ${path}`;
      const initial = await request(path, {method: 'POST', body: value});
      assert.equal(initial.value.counter, value);
      assert.deepEqual((await request(path)).value, initial.value);
      records.push(initial.value);
    }
    assert.equal(records[0].isolate, records[1].isolate);
    assert.notEqual(records[0].instance, records[1].instance);
    for (let i = 0; i < 10; ++i) {
      for (const [index, path] of paths.entries()) {
        assert.deepEqual((await request(path)).value, records[index]);
      }
    }
    console.log(JSON.stringify({event: 'http-writes-reads-passed', objects: paths.length}));
    // A brief idle interval must preserve the still-resident actor's instance.
    await delay(4200);
    for (const [index, path] of paths.entries()) {
      assert.deepEqual((await request(path)).value, records[index]);
    }
    console.log(JSON.stringify({event: 'http-brief-idle-passed'}));
    const wakes = [];
    for (let round = 0; round < 3; ++round) {
      const before = await waitForAdmission(socketPath, 0);
      let idle;
      for (const until = performance.now() + 20000; performance.now() < until;) {
        idle = await readAdmission(socketPath);
        if (idle.residentRoots === 0 && idle.idleRetirements >= before.idleRetirements + paths.length) break;
        await delay(100);
      }
      console.log(JSON.stringify({event: 'http-idle-observed', round, admission: idle}));
      assert.equal(idle.residentRoots, 0, JSON.stringify(idle));
      assert.equal(idle.trees, 0);
      assert.equal(idle.reservedBytes, 0);
      assert.ok(idle.idleRetirements >= before.idleRetirements + paths.length);
      for (const [index, path] of paths.entries()) {
        const previous = records[index];
        const writing = (round + index) % 2 === 1;
        const value = writing ? `saved at wake ${round}: ${path}` : previous.counter;
        const first = await request(writing ? `${path}?expect=${encodeURIComponent(previous.counter)}` : path,
          writing ? {method: 'POST', body: value} : undefined);
        assert.notEqual(first.value.instance, previous.instance);
        assert.equal(first.value.counter, value);
        assert.deepEqual((await request(path)).value, first.value);
        records[index] = first.value;
        wakes.push({round, method: writing ? 'POST' : 'GET', elapsedMs: first.elapsedMs,
          oldInstance: previous.instance, instance: first.value.instance, statePreserved: true});
      }
      const current = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
        headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(1000),
      }).then(reply => reply.json());
      const sameChild = current.processes.entries.find(entry => entry.processClass === 'durable');
      assert.equal(sameChild.pid, processInfo.pid);
      assert.equal(sameChild.generation, processInfo.generation);
      assert.equal(sameChild.state, 'ready');
      const after = await waitForAdmission(socketPath, 0);
      assert.equal(after.actorResets, before.actorResets);
      assert.equal(after.isolateResets, before.isolateResets);
    }
    console.log(JSON.stringify({scenario, pid: processInfo.pid, generation: processInfo.generation,
      wakes, admission: await readAdmission(socketPath)}));
  } else if (overlap) {
    const baseline = await readProcessResources(processInfo.pid);
    const request = async (path, options, status = 200) => {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        ...options, signal: AbortSignal.timeout(6000),
      });
      const body = await response.text();
      assert.equal(response.status, status, body + '\n' + diagnostics);
      return JSON.parse(body);
    };
    const facets = [];
    for (let index = 0; index < 4; ++index) {
      facets.push(await request(`/overlap-${index}?facet`, {method: 'POST', body: `facet ${index}`}));
    }
    const quiet = await waitForAdmission(socketPath, 0);
    const warm = await readProcessResources(processInfo.pid);
    const held = [];
    const bodyBytes = 16 * 1024;
    for (let index = 0; index < 4; ++index) {
      for (let event = 0; event < 4; ++event) {
        const promise = request(`/overlap-${index}?facet&overlapHold`, {
          method: 'POST', body: 'x'.repeat(bodyBytes),
        });
        promise.catch(() => {});
        held.push(promise);
      }
    }
    let loaded, counts;
    for (const until = performance.now() + 2000; performance.now() < until;) {
      loaded = await readAdmission(socketPath);
      // Wait until the held calls have reached their handlers before adding observation calls.
      if (loaded.running === held.length) {
        counts = await Promise.all(facets.map((_, index) => request(`/overlap-${index}?facet&overlapState`)));
        if (counts.every(count => count === 4)) break;
      }
      await delay(10);
    }
    assert.deepEqual(counts, [4, 4, 4, 4], 'all bodies must be read and written before sampling');
    for (const until = performance.now() + 500; performance.now() < until;) {
      loaded = await readAdmission(socketPath);
      if (loaded.running === held.length) break;
      await delay(10);
    }
    assert.equal(loaded.running, 16, JSON.stringify(loaded));
    assert.equal(loaded.residentRoots, 4);
    const full = await readProcessResources(processInfo.pid);
    // Mailbox limits apply to waiting events; these delivered events are suspended at await.
    const interleaved = await request('/overlap-0');
    assert.equal(interleaved.isolate, facets[0].isolate);
    const neighbor = await request('/overlap-neighbor', {method: 'POST', body: 'still available'});
    const replies = await Promise.all(held);
    for (const [index, reply] of replies.entries()) {
      assert.equal(reply.instance, facets[Math.floor(index / 4)].instance);
      assert.equal(reply.bytes, bodyBytes);
    }
    for (let index = 0; index < 4; ++index) {
      assert.deepEqual(await request(`/overlap-${index}?facet`), facets[index]);
    }
    assert.deepEqual(await request('/overlap-neighbor'), neighbor);
    const drained = await waitForAdmission(socketPath, 0);
    assert.equal(drained.running, 0);
    assert.equal(drained.reservedBytes, quiet.reservedBytes + 16 * 1024);
    assert.equal(drained.reservedDescriptors, quiet.reservedDescriptors + 4);
    assert.equal(drained.actorResets, 0);
    assert.equal(drained.isolateResets, 0);
    const current = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(1000),
    }).then(reply => reply.json());
    const sameChild = current.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    console.log(JSON.stringify({scenario, roots: 4, facets: 4, events: held.length, bodyBytes,
      confirmedBodies: counts.reduce((sum, count) => sum + count, 0), baseline, warm, full, after: await readProcessResources(processInfo.pid),
      quiet, loaded, drained, pid: sameChild.pid, generation: sameChild.generation}));
  } else if (groupRefusal) {
    const refused = await fetch(`http://127.0.0.1:${port}/group-refused`, {
      method: 'POST', body: 'must not reach the actor', signal: AbortSignal.timeout(5000),
    });
    const refusal = await refused.text();
    assert.equal(refused.status, 502, refusal + '\n' + diagnostics);
    assert.match(JSON.parse(refusal).error, /OVERLOADED/);
    const empty = await waitForAdmission(socketPath, 0);
    assert.equal(empty.groupCreationRefusals, 1);
    for (const field of ['trees', 'residentRoots', 'homes', 'running', 'reservedBytes',
      'reservedDescriptors']) assert.equal(empty[field], 0, field);
    const request = async (path, options) => {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        ...options, signal: AbortSignal.timeout(5000),
      });
      const body = await response.text();
      assert.equal(response.status, 200, body + '\n' + diagnostics);
      return JSON.parse(body);
    };
    const retry = await request('/group-refused');
    assert.equal(retry.counter, undefined, 'the refused request entered the actor');
    const saved = await request('/group-refused', {method: 'POST', body: 'after group refusal'});
    assert.deepEqual(await request('/group-refused'), saved);
    const neighbor = await request('/group-neighbor', {method: 'POST', body: 'neighbor'});
    assert.equal(neighbor.isolate, saved.isolate);
    const current = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(1000),
    }).then(reply => reply.json());
    const sameChild = current.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    const final = await waitForAdmission(socketPath, 0);
    assert.equal(final.groupCreationRefusals, 1);
    assert.equal(final.actorResets, 0);
    assert.equal(final.isolateResets, 0);
    console.log(JSON.stringify({scenario, injectedFactoryRefusal: true, refusedBeforeActor: true,
      retried: true, pid: sameChild.pid, generation: sameChild.generation, admission: final}));
  } else if (httpMemory) {
    const request = async (path, options, status = 200) => {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        ...options, signal: AbortSignal.timeout(5000),
      });
      const body = await response.text();
      assert.equal(response.status, status, `${path}: ${body}\n${diagnostics}`);
      return JSON.parse(body);
    };
    const untilState = async (condition, label) => {
      let state;
      for (const until = performance.now() + 4000; performance.now() < until;) {
        state = await readAdmission(socketPath);
        if (condition(state)) return state;
        await delay(10);
      }
      assert.fail(`${label}: ${JSON.stringify(state)}`);
    };
    const resources = {baseline: await readProcessResources(processInfo.pid)};
    const initial = await request('/memory-idle', {method: 'POST', body: 'idle value'});
    const moderateMiB = memoryPlacementMiB + 8;
    await request(`/memory-idle?allocateMiB=${moderateMiB}`);
    const evicted = await untilState(state => state.pressureRetirements > 0 && state.trees === 0 &&
      state.maxHomeMemoryBytes < memoryPlacementMiB * 1024 * 1024, 'idle-first memory eviction');
    assert.equal(evicted.isolateResets, 0);
    assert.equal(evicted.reservedBytes, 0);
    resources.afterIdleEviction = await readProcessResources(processInfo.pid);
    const reopened = await request('/memory-idle');
    assert.notEqual(reopened.instance, initial.instance);
    assert.equal(reopened.counter, initial.counter);
    console.log(JSON.stringify({event: 'http-idle-first-memory-eviction-passed', admission: evicted}));

    const heldInitial = await request('/memory-held', {method: 'POST', body: 'held value'});
    const held = request(`/memory-held?allocateMiB=${moderateMiB}&hold`);
    held.catch(() => {});
    const closed = await untilState(state => state.placementClosedHomes > 0, 'closed memory placement');
    assert.equal(closed.isolateResets, 0);
    assert.ok(closed.maxHomeMemoryBytes >= memoryPlacementMiB * 1024 * 1024);
    assert.equal(await request('/memory-held?waiting'), true);
    const cold = await request('/memory-cold', {method: 'POST', body: 'other copy'});
    assert.notEqual(cold.isolate, heldInitial.isolate);
    resources.closedPlacement = await readProcessResources(processInfo.pid);
    await request('/memory-held?release');
    assert.deepEqual(await held, heldInitial);
    const reclaimed = await untilState(state => state.pressureRetirements > closed.pressureRetirements &&
      state.placementClosedHomes === 0, 'reclaim released memory');
    assert.equal(reclaimed.isolateResets, 0);
    assert.deepEqual(await request('/memory-cold'), cold);
    console.log(JSON.stringify({event: 'http-memory-placement-passed', admission: closed,
      coldIsolate: cold.isolate, heldIsolate: heldInitial.isolate}));

    const hardInitial = await request('/memory-hard', {method: 'POST', body: 'hard value'});
    const resetting = request('/memory-hard?allocateMiB=118&hold', undefined, 503);
    resetting.catch(() => {});
    let hardPeakBytes = 0;
    const hard = await untilState(state => {
      hardPeakBytes = Math.max(hardPeakBytes, state.maxHomeMemoryBytes);
      return state.isolateResets === 1;
    }, 'hard isolate reset');
    // This is a copy-pressure reset below the 128 MiB allocation ceiling. An already
    // connected call interrupted by that close has the existing uncertain-result contract.
    assert.ok(hardPeakBytes >= 128 * 1024 * 1024 * 0.9, String(hardPeakBytes));
    assert.ok(hardPeakBytes < 128 * 1024 * 1024, String(hardPeakBytes));
    assert.equal((await resetting).code, 'RESULT_UNCERTAIN');
    assert.equal(hard.actorResets, 0);
    const recovered = await request('/memory-hard');
    assert.notEqual(recovered.instance, hardInitial.instance);
    assert.notEqual(recovered.isolate, hardInitial.isolate);
    assert.equal(recovered.counter, hardInitial.counter);
    resources.afterReset = await readProcessResources(processInfo.pid);
    const current = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: {Authorization: `Bearer ${token}`}, signal: AbortSignal.timeout(1000),
    }).then(reply => reply.json());
    const sameChild = current.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    const final = await waitForAdmission(socketPath, 0);
    assert.equal(final.isolateResets, 1);
    console.log(JSON.stringify({scenario, memoryPlacementMiB, moderateMiB, resources,
      idleFirst: evicted, closed, hardPeakBytes, hard, final, pid: sameChild.pid, generation: sameChild.generation}));
  } else if (outputGate) {
    // Warm the actor and drain initialization before observing the unawaited write.
    const initial = await fetch(`http://127.0.0.1:${port}/output`, {
      method: 'POST', body: 'initial value', signal: AbortSignal.timeout(5000),
    });
    assert.equal(initial.status, 200);
    assert.equal((await initial.json()).counter, 'initial value');
    assert.equal((await waitForAdmission(socketPath, 0)).pendingCommits, 0);
    let replied = false;
    const method = rpcOutputGate ? 'rpc' : 'http';
    const query = rpcOutputGate ? 'rpcWrite' : 'outputGate';
    const pending = fetch(`http://127.0.0.1:${port}/output?${query}`, {
      signal: AbortSignal.timeout(5000),
    }).then(async response => {
      replied = true;
      assert.equal(response.status, 200);
      assert.equal((await readAdmission(socketPath)).pendingCommits, 0,
        method + ' result arrived before test storage confirmation');
      return response.json();
    });
    pending.catch(() => {});
    let confirmationObserved = false;
    for (const until = performance.now() + 2000; performance.now() < until;) {
      if ((await readAdmission(socketPath)).pendingCommits > 0) {
        confirmationObserved = true;
        break;
      }
      if (replied) break;
      await delay(10);
    }
    assert.ok(confirmationObserved, 'no pending test storage confirmation was observed');
    assert.equal(await pending, method + ' committed');
    const saved = await fetch(`http://127.0.0.1:${port}/output?readOutputGate`, {
      signal: AbortSignal.timeout(5000),
    });
    assert.equal(saved.status, 200);
    const savedValue = await saved.json();
    assert.equal(savedValue, method + ' write');
    const counters = await waitForAdmission(socketPath, 0);
    assert.equal(counters.pendingCommits, 0);
    assert.equal(counters.actorResets, 0);
    assert.equal(counters.isolateResets, 0);
    const state = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(1000),
    }).then(reply => reply.json());
    const sameChild = state.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    console.log(JSON.stringify({scenario, confirmationObserved, responseWaited: true,
      savedValue, pid: sameChild.pid, generation: sameChild.generation, admission: counters}));
  } else if (queues) {
    const deadline = process.hrtime.bigint() + 30_000_000_000n;
    const keep = [];
    const queue = async (id, sequence, bytes, expected) => {
      const connection = await sendPreface(socketPath, durablePreface(id, sequence, bytes, deadline));
      keep.push(connection.socket);
      assert.equal(connection.receipt, expected);
    };
    const depthId = 'd'.repeat(64);
    try {
      for (let event = 0; event < 4; ++event) await queue(depthId, `depth-${event}`, 1, 'A');
      await queue(depthId, 'depth-overflow', 1, 'R');
      await queue('e'.repeat(64), 'depth-unrelated', 1, 'A');
      const depth = await readAdmission(socketPath);
      assert.equal(depth.waiting, 5);
      assert.equal(depth.waitingBytes, 5);
    } finally {
      for (const socket of keep.splice(0)) socket.destroy();
    }
    await waitForAdmission(socketPath, 0);
    const baseline = await readProcessResources(processInfo.pid);
    const start = performance.now();
    let full, reserved;
    let accepted = 0, refused = false;
    try {
      filling: for (let object = 0; object < treesPerHome; ++object) {
        const id = (object + 1000).toString(16).padStart(64, '0');
        for (let event = 0; event < 4; ++event) {
          const connection = await sendPreface(socketPath,
            durablePreface(id, `mailbox-${object}-${event}`, 16384, deadline));
          keep.push(connection.socket);
          if ((queueLimit || queueMemory) && connection.receipt === 'R') { refused = true; break filling; }
          assert.equal(connection.receipt, 'A');
          ++accepted;
        }
      }
      reserved = await readAdmission(socketPath);
      assert.equal(reserved.trees, Math.ceil(accepted / 4));
      assert.equal(reserved.waiting, accepted);
      assert.equal(reserved.waitingBytes, accepted * 16384);
      assert.equal(reserved.reservedDescriptors, reserved.trees * 4 + accepted);
      assert.equal(reserved.reservedBytes, reserved.trees * 16384 + accepted * 20480);
      full = await readProcessResources(processInfo.pid);
      if (queueLimit) {
        assert.ok(refused, 'descriptor reservation ceiling was not reached');
        const limits = await readFile(`/proc/${processInfo.pid}/limits`, 'utf8');
        const [, soft, hard] = limits.match(/^Max open files\s+(\d+)\s+(\d+)/m);
        const maxFiles = Number(soft);
        assert.equal(maxFiles, 512);
        assert.equal(Number(hard), 512);
        const remaining = maxFiles - 256 - homeCount * 5 - reserved.reservedDescriptors;
        assert.ok(remaining >= 0 && remaining < (accepted % 4 === 0 ? 5 : 1));
        assert.ok(full.descriptors <= maxFiles - 256, 'fixed descriptor headroom consumed');
      } else if (queueMemory) {
        assert.ok(refused, 'memory reservation ceiling was not reached');
        const remaining = 16 * 1024 * 1024 - reserved.reservedBytes;
        assert.ok(remaining >= 0 && remaining < (accepted % 4 === 0 ? 36864 : 20480));
      } else {
        assert.equal(accepted, treesPerHome * 4);
      }
    } finally {
      for (const socket of keep) socket.destroy();
    }
    const released = await waitForAdmission(socketPath, 0);
    assert.equal(released.trees, 0);
    assert.equal(released.reservedDescriptors, 0);
    assert.equal(released.reservedBytes, 0);
    let drained;
    for (const until = performance.now() + 2000; performance.now() < until;) {
      drained = await readProcessResources(processInfo.pid);
      if (drained.descriptors === baseline.descriptors) break;
      await delay(20);
    }
    assert.equal(drained.descriptors, baseline.descriptors);
    assert.equal(drained.threads, baseline.threads);
    const current = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(1000),
    }).then(reply => reply.json());
    const sameChild = current.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    console.log(JSON.stringify({scenario, objects: reserved.trees, events: accepted, refused,
      declaredBytesPerEvent: 16384, elapsedMs: performance.now() - start,
      baseline, full, drained, reserved, released}));
  } else if (placement) {
    const request = async path => {
      const start = performance.now();
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        signal: AbortSignal.timeout(8000),
      });
      const body = await response.text();
      assert.equal(response.status, 200, body + '\n' + diagnostics);
      return { value: JSON.parse(body), elapsedMs: performance.now() - start };
    };
    const root = (await request('/busy')).value;
    const quiet = (await request('/quiet')).value;
    assert.equal(quiet.isolate, root.isolate);
    // Adjust bounded work for host speed while keeping each event short.
    let iterations = 15000000;
    const baseline = await readProcessResources(processInfo.pid);
    const before = await waitForAdmission(socketPath, 0);
    const start = performance.now();
    const workMs = [], workIterations = [], coldMs = [];
    let split;
    for (let i = 0; performance.now() - start < 10000 && (!placementControl || i < 32); ++i) {
      const work = await request(`/busy?work=${iterations}`);
      assert.equal(work.value.instance, root.instance);
      assert.equal(work.value.isolate, root.isolate);
      workMs.push(work.elapsedMs);
      workIterations.push(iterations);
      iterations = Math.min(200000000, Math.max(1000000,
        Math.round(iterations * 120 / Math.max(1, work.elapsedMs))));
      const cold = await request(`/probe-${i}`);
      coldMs.push(cold.elapsedMs);
      if (placementControl) {
        assert.equal(cold.value.isolate, root.isolate, 'control unexpectedly split');
        split = cold.value;
      } else if (cold.value.isolate !== root.isolate) { split = cold.value; break; }
      assert.ok(i < treesPerHome - 3, 'capacity reached before busy threshold');
    }
    if (placementControl) assert.equal(coldMs.length, 32);
    else assert.ok(split, 'busy home did not divert a cold tree');
    const elapsedMs = performance.now() - start;
    const after = await waitForAdmission(socketPath, 0);
    assert.equal(after.homes, before.homes + (placementControl ? 0 : 1));
    assert.equal(after.idleRetirements, before.idleRetirements);
    assert.equal(after.actorResets, before.actorResets);
    assert.equal(after.isolateResets, before.isolateResets);
    assert.ok(after.residentRoots < treesPerHome);
    const pinned = (await request('/busy')).value;
    assert.equal(pinned.instance, root.instance);
    assert.equal(pinned.isolate, root.isolate);
    assert.equal((await request(`/probe-${coldMs.length - 1}`)).value.instance, split.instance);
    const finalState = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(1000),
    }).then(reply => reply.json());
    const finalChild = finalState.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(finalChild.pid, processInfo.pid);
    assert.equal(finalChild.generation, processInfo.generation);
    assert.equal(finalChild.state, 'ready');
    console.log(JSON.stringify({scenario, busyThresholdMs: Number(busyArg),
      elapsedMs, workMs, workIterations, coldMs, baseline,
      full: await readProcessResources(processInfo.pid), before, after}));
  } else if (measure) {
    const query = (index, body) => fetch(`http://127.0.0.1:${port}/occupy-${index}`, {
      method: 'POST', body, signal: AbortSignal.timeout(5000),
    });
    const baseline = await readProcessResources(processInfo.pid);
    const isolates = new Set();
    const body = 'x'.repeat(4096);
    const count = homeCount * treesPerHome;
    const start = performance.now();
    for (let i = 0; i < count; ++i) {
      const reply = await query(i, body);
      const payload = await reply.text();
      assert.equal(reply.status, 200, payload + '\n' + diagnostics);
      isolates.add(JSON.parse(payload).isolate);
    }
    assert.equal(isolates.size, homeCount);
    const loaded = await readAdmission(socketPath);
    assert.equal(loaded.homes, homeCount);
    assert.equal(loaded.residentRoots, count);
    const full = await readProcessResources(processInfo.pid);
    const next = await query(count, body);
    assert.equal(next.status, 200, await next.text() + '\n' + diagnostics);
    const after = await readAdmission(socketPath);
    assert.equal(after.admissionRetirements, loaded.admissionRetirements + 1);
    assert.equal((await query(0, 'still ready')).status, 200);
    console.log(JSON.stringify({ homes: homeCount, treesPerHome, residentRoots: count,
      bodyBytes: body.length, warmupMs: performance.now() - start, baseline, full, admission: loaded }));
  } else if (exerciseEvents) {
    const request = async (path, options, status = 200) => {
      const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        ...options, signal: AbortSignal.timeout(5000),
      });
      const body = await response.text();
      assert.equal(response.status, status, body + '\n' + diagnostics);
      return JSON.parse(body);
    };
    const initial = await request('/first', { method: 'POST', body: 'confirmed in fixture SQLite' });
    assert.deepEqual(await request('/first'), initial);
    assert.equal(await request('/first?rpc'), 'confirmed in fixture SQLite');
    const other = await request('/other', { method: 'POST', body: 'other tree' });
    assert.notEqual(other.instance, initial.instance);
    assert.equal(other.isolate, initial.isolate);
    const firstFacet = await request('/first?facet', { method: 'POST', body: 'fixture facet' });
    assert.notEqual(firstFacet.instance, initial.instance);
    assert.equal(firstFacet.isolate, initial.isolate);
    assert.equal(firstFacet.counter, 'fixture facet');
    assert.deepEqual(await request('/first?facet'), firstFacet);
    const waiting = request('/first?hold');
    waiting.catch(() => {});
    // Observe the first event in its await before issuing another event to the same actor.
    let held = false;
    for (const until = performance.now() + 2000; performance.now() < until;) {
      if (await request('/first?waiting')) {
        held = true;
        break;
      }
      await delay(10);
    }
    assert.ok(held);
    if (httpIsolation) {
      assert.deepEqual(await request('/first'), initial);
      assert.deepEqual(await request('/other'), other);
    } else {
      assert.equal(await request('/first?rpc'), initial.counter);
      assert.equal(await request('/first?nested'), other.counter);
    }
    await request('/first?release');
    assert.deepEqual(await waiting, initial);
    if (events) {
      assert.deepEqual(await request('/first?roundTrip'),
        {order: ['begin', 'nested', 'end'], instance: initial.instance});
      const drained = await waitForAdmission(socketPath, 0);
      assert.equal(drained.running, 0);
      assert.equal(drained.waitingBytes, 0);
    }

    if (httpIsolation) {
      for (let attempt = 0; attempt < 3; ++attempt) {
        // The HTTP response can arrive before its event finishes retiring.
        const before = await waitForAdmission(socketPath, 0, 0, { running: 0 });
        let caller;
        const aborted = new Promise((resolve, reject) => {
          caller = get(`http://127.0.0.1:${port}/first?hold&cancelable`, response => {
            response.resume();
            reject(new Error('canceled request unexpectedly produced a response'));
          });
          caller.once('error', resolve);
        });
        aborted.catch(() => {});
        let observed = false;
        for (const until = performance.now() + 2000; performance.now() < until;) {
          if (await request('/first?waiting')) {
            observed = true;
            break;
          }
          await delay(10);
        }
        assert.ok(observed, 'the canceled request never reached the actor');
        assert.ok((await readAdmission(socketPath)).running > 0);
        // Send a TCP reset explicitly while response headers are pending.
        caller.socket.resetAndDestroy();
        assert.equal((await aborted).code, 'ECONNRESET');
        const drained = await waitForAdmission(socketPath, 0, 0, { running: 0 });
        for (const field of ['reservedBytes', 'reservedDescriptors', 'residentRoots',
          'actorResets', 'isolateResets']) assert.equal(drained[field], before[field], field);
        assert.equal(await request('/first?aborts'), attempt + 1);
        assert.deepEqual(await request('/first'), initial);
        assert.deepEqual(await request('/other'), other);
      }
      console.log(JSON.stringify({event: 'http-cancellation-passed', attempts: 3}));
    }

    const gated = request('/first?inputGate');
    gated.catch(() => {});
    const observe = async condition => {
      for (const until = performance.now() + 2000; performance.now() < until;) {
        if (await condition()) return;
        await delay(5);
      }
      assert.fail('the input gate or the independent call was never observed');
    };
    await observe(async () => (await request('/other?gateState')) === 'held');
    let replied = false;
    const read = request(httpIsolation ? '/first' : '/first?rpc').then(value => {
      replied = true;
      return value;
    });
    read.catch(() => {});
    try {
      await observe(async () => replied || (await readAdmission(socketPath)).running >= 2);
      assert.deepEqual(await request('/other'), other);
      assert.equal(replied, false, 'independent call returned while the input gate was closed');
    } finally {
      await request('/other?releaseGate');
    }
    const [write, gatedRead] = await Promise.all([gated, read]);
    assert.equal(write.counter, 'after input gate');
    assert.equal(httpIsolation ? gatedRead.counter : gatedRead, write.counter);

    console.log(JSON.stringify({event: 'http-input-gate-and-interleaving-passed', scenario}));
    let previous = write;
    let resets = (await readAdmission(socketPath)).actorResets;
    for (const cause of ['cpu', 'sqlCpu']) {
      const sibling = request('/other?hold');
      sibling.catch(() => {});
      let siblingHeld = false;
      for (const until = performance.now() + 2000; performance.now() < until;) {
        if (await request('/other?waiting')) {
          siblingHeld = true;
          break;
        }
        await delay(10);
      }
      assert.ok(siblingHeld, 'the neighboring event must be suspended before the CPU reset');
      const refused = await request('/first?' + cause, undefined, 503);
      assert.equal(refused.code, 'CPU_LIMIT_EXCEEDED');
      let resetObserved = false;
      for (const until = performance.now() + 2000; performance.now() < until;) {
        if ((await readAdmission(socketPath)).actorResets === resets + 1) {
          resetObserved = true;
          break;
        }
        await delay(10);
      }
      assert.ok(resetObserved, diagnostics);
      ++resets;
      assert.deepEqual(await request('/other'), other);
      const rebuilt = await request('/first');
      assert.notEqual(rebuilt.instance, previous.instance);
      assert.equal(rebuilt.isolate, previous.isolate);
      assert.equal(rebuilt.counter, previous.counter);
      previous = rebuilt;
      await request('/other?release');
      assert.deepEqual(await sibling, other);
      console.log(JSON.stringify({event: 'http-actor-cpu-reset-passed', cause, resets,
        suspendedNeighborSurvived: true}));
    }
    await delay(11500);
    const reopened = await request('/first');
    assert.notEqual(reopened.instance, previous.instance);
    assert.equal(reopened.isolate, previous.isolate);
    assert.equal(reopened.counter, previous.counter);
    const reopenedFacet = await request('/first?facet');
    assert.notEqual(reopenedFacet.instance, firstFacet.instance);
    assert.equal(reopenedFacet.isolate, reopened.isolate);
    assert.equal(reopenedFacet.counter, firstFacet.counter);

    const log = join(stack, 'logs', 'workerd.log');
    let completeTrace = false;
    for (const until = performance.now() + 2000; performance.now() < until;) {
      const lines = await readFile(log, 'utf8');
      const records = Array.from(lines.matchAll(/\[threaded-durable-actor\] (\{[^\n]*\})/g),
        match => JSON.parse(match[1]));
      const closedFacet = records.find(record => record.facet !== 0 && record.phase === 'destroyed');
      if (closedFacet) {
        const root = records.find(record => record.tree === closedFacet.tree && record.facet === 0);
        const actors = new Set(records.filter(record => record.tree === closedFacet.tree).map(record => record.actor));
        for (const actor of actors) {
          const turns = records.filter(record => record.actor === actor);
          assert.ok(turns.every(record => record.home === root.home && record.tid === root.tid));
          if (turns.some(record => record.phase === 'destroyed')) {
            for (const phase of ['created', 'executed', 'delivered', 'destroyed']) {
              assert.ok(turns.some(record => record.phase === phase), phase);
            }
          }
        }
        const rootEnd = records.findIndex(record => record.actor === root.actor && record.phase === 'destroyed');
        if (rootEnd !== -1) {
          assert.ok(rootEnd > records.indexOf(closedFacet));
          completeTrace = true;
          break;
        }
      }
      await delay(25);
    }
    assert.ok(completeTrace, 'missing same-home facet and root destruction evidence');
    const finalState = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
      headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(1000),
    }).then(reply => reply.json());
    const sameChild = finalState.processes.entries.find(entry => entry.processClass === 'durable');
    assert.equal(sameChild.pid, processInfo.pid);
    assert.equal(sameChild.generation, processInfo.generation);
    assert.equal(sameChild.state, 'ready');
    const counters = await waitForAdmission(socketPath, 0);
    assert.equal(counters.isolateResets, 0);
    assert.equal(counters.actorResets, 2);
    console.log(JSON.stringify({scenario, inputGate: true, interleavedHttp: httpIsolation,
      httpCancellation: httpIsolation, facetStatePreserved: true, sameHomeLifecycle: true, pid: sameChild.pid,
      generation: sameChild.generation, admission: counters}));
  }
  assert.deepEqual((await readdir(fixture)).sort(),
    ['node.json', 'runtime', ...(queueLimit ? ['threaded-fd-limit', 'threaded-fd-limit.target'] : []), 'workers']);
  child.kill('SIGTERM');
  await exited;
  assert.equal(child.exitCode, 0);
  assert.deepEqual(await readdir(scratch), ['fixture']);
} catch (error) {
  console.error(diagnostics);
  throw error;
} finally {
  if (child?.pid && child.exitCode === null && child.signalCode === null) {
    try { process.kill(-child.pid, 'SIGTERM'); } catch {}
    const timer = setTimeout(() => { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }, 1500);
    await exited;
    clearTimeout(timer);
  }
  await rm(scratch, { recursive: true, force: true });
}

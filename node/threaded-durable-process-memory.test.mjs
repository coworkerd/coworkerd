// Process-budget refusal must return without blocking a durable home or losing stored state.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const [nodeBinary, workerd, kind = 'buffer'] = process.argv.slice(2);
assert.ok(['buffer', 'heap'].includes(kind));
assert.equal(process.platform, 'linux');
const scratch = await mkdtemp(join(tmpdir(), 'cwkd-process-memory-'));
const fixture = join(scratch, 'fixture');
const configPath = join(fixture, 'node.json');
const manifestPath = join(fixture, 'workers', 'manifest.json');
const env = {
  ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(COWORKERD_|WORKERD_)/.test(key))),
  TMPDIR: scratch, COWORKERD_DO_TEST_HOME_COUNT: '1', COWORKERD_CPU_LIMIT_MS: '1000',
  COWORKERD_THREADED_MEMORY_BYTES: String(128 * 1024 * 1024),
};
const token = 'fixture-management-placeholder';
const hash = value => createHash('sha256').update(value).digest('hex');
let child, closed, diagnostics = '';
const server = createServer();
const port = await new Promise((resolve, reject) => {
  server.once('error', reject);
  server.listen(0, '127.0.0.1', () => {
    const value = server.address().port;
    server.close(error => error ? reject(error) : resolve(value));
  });
});
const args = [manifestPath, workerd, `127.0.0.1:${port}`, '127.0.0.1:0', '127.0.0.1:0', configPath];

async function state() {
  const response = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {
    headers: {Authorization: `Bearer ${token}`}, signal: AbortSignal.timeout(500),
  });
  assert.equal(response.status, 200);
  return (await response.json()).processes.entries;
}

async function ready(previous) {
  for (const until = performance.now() + 10000; performance.now() < until;) {
    assert.equal(child.exitCode, null, diagnostics);
    assert.equal(child.signalCode, null, diagnostics);
    try {
      const entries = await state();
      const durable = entries.find(entry => entry.processClass === 'durable');
      if (entries.length === 2 && entries.every(entry => entry.state === 'ready') &&
          durable && (!previous || durable.generation > previous.generation)) {
        assert.equal(durable.sandbox.durableServer, 'threaded-test');
        return durable;
      }
    } catch {}
    await delay(30);
  }
  assert.fail('the replacement durable child did not become ready: ' + diagnostics);
}

async function start() {
  child = spawn(nodeBinary, args, {env, detached: true, stdio: ['ignore', 'pipe', 'pipe']});
  for (const stream of [child.stdout, child.stderr]) {
    stream.on('data', bytes => { diagnostics = (diagnostics + bytes).slice(-5000); });
  }
  closed = new Promise(resolve => {
    child.on('error', error => { diagnostics += error.message; });
    child.on('close', resolve);
  });
  return ready();
}

async function stop() {
  child.kill('SIGTERM');
  const timer = setTimeout(() => { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }, 3000);
  await closed;
  clearTimeout(timer);
  assert.equal(child.exitCode, 0, diagnostics);
  assert.deepEqual(await readdir(scratch), ['fixture']);
}

async function request(path, value) {
  const response = await fetch(`http://127.0.0.1:${port}${path}`, {
    ...(value === undefined ? {} : {method: 'POST', body: value}), signal: AbortSignal.timeout(1500),
  });
  const text = await response.text();
  let body;
  try { body = JSON.parse(text); } catch { body = text; }
  return {status: response.status, body};
}

async function successful(path, value) {
  const result = await request(path, value);
  assert.equal(result.status, 200, JSON.stringify(result) + '\n' + diagnostics);
  return result.body;
}

try {
  const modules = join(fixture, 'workers', 'counter', 'v1');
  await mkdir(modules, {recursive: true});
  const source = `
    import { DurableObject } from "cloudflare:workers";
    let isolate;
    export class Counter extends DurableObject {
      instance = crypto.randomUUID();
      async fetch(request) {
        isolate ??= crypto.randomUUID();
        const query = new URL(request.url).searchParams;
        if (query.has("buffer")) {
          this.held = new Uint8Array(64 * 1024 * 1024);
          this.held.fill(7);
        }
        if (query.has("heap")) this.held = new Array(12 * 1024 * 1024).fill(7);
        if (request.method === "POST") await this.ctx.storage.put("value", await request.text());
        return Response.json({id: this.ctx.id.toString(), instance: this.instance, isolate,
          value: await this.ctx.storage.get("value") ?? null});
      }
    }
    export default {fetch(request, env) {
      return env.COUNTERS.get(env.COUNTERS.idFromName(new URL(request.url).pathname)).fetch(request);
    }};
  `;
  const packageManifest = JSON.stringify({modules: [
    {bytes: Buffer.byteLength(source), name: 'index.js', sha256: hash(source), type: 'esm'},
  ]});
  await writeFile(join(modules, 'index.js'), source);
  await writeFile(join(modules, 'package.json'), packageManifest);
  const manifest = JSON.stringify({compatibilityDate: '2026-08-27',
    routes: [{host: '*', pathPrefix: '/', workerId: 'counter'}],
    workers: [{workerId: 'counter', activeVersion: 'v1', versions: [{
      versionId: 'v1', mainModule: 'index.js', contentHash: hash(packageManifest), bindings: [],
      format: 'v2', compatibilityDate: '2026-08-27',
      durableObjects: [{binding: 'COUNTERS', className: 'Counter'}], classes: {declared: ['Counter']},
    }]}],
  });
  const config = JSON.stringify({nodeId: 'fixture',
    durableObjects: {processes: 1, threadedTestStorage: true},
    processPool: {count: 1, inFlightLimit: 4}, quarantineProcesses: 0,
    sandbox: {mode: 'off', syscalls: {mode: 'off'}}, peerListenAddress: '127.0.0.1:0',
    memoryWatermarkEnabled: false, hostCheck: {intervalMs: 0}, management: {token},
  });
  await writeFile(manifestPath, manifest);
  await writeFile(configPath, config);

  const originalChild = await start();
  const initial = await successful('/pressure', 'value before process pressure');
  const neighbor = await successful('/neighbor', 'neighbor value');
  assert.equal(initial.value, 'value before process pressure');
  assert.equal(neighbor.value, 'neighbor value');
  const started = performance.now();
  let refused;
  try { refused = await request('/pressure?' + kind); }
  catch (error) {
    console.log(JSON.stringify({event: 'process-memory-response', kind,
      elapsedMs: performance.now() - started, error: error.name}));
    throw error;
  }
  console.log(JSON.stringify({event: 'process-memory-response', kind,
    elapsedMs: performance.now() - started, status: refused.status, body: refused.body}));
  assert.equal(refused.status, 503, JSON.stringify(refused));
  assert.match(JSON.stringify(refused.body), /PROCESS_MEMORY_PRESSURE|MEMORY_LIMIT_EXCEEDED/);
  let recovered;
  for (const until = performance.now() + 5000; performance.now() < until;) {
    const result = await request('/pressure');
    if (result.status === 200) { recovered = result.body; break; }
    assert.ok([502, 503].includes(result.status), JSON.stringify(result));
    await delay(10);
  }
  assert.ok(recovered, 'process memory pressure left its home blocked');
  assert.equal(recovered.value, initial.value);
  assert.equal(recovered.id, initial.id);
  let restoredNeighbor;
  for (const until = performance.now() + 5000; performance.now() < until;) {
    const result = await request('/neighbor');
    if (result.status === 200) { restoredNeighbor = result.body; break; }
    assert.ok([502, 503].includes(result.status), JSON.stringify(result));
    await delay(10);
  }
  assert.ok(restoredNeighbor, 'the neighboring object did not become available again');
  assert.equal(restoredNeighbor.id, neighbor.id);
  assert.equal(restoredNeighbor.value, neighbor.value);
  const current = (await state()).find(entry => entry.processClass === 'durable');
  assert.equal(current.pid, originalChild.pid);
  assert.equal(current.generation, originalChild.generation);
  assert.equal(current.state, 'ready');
  await stop();

} finally {
  if (child?.pid && child.exitCode === null && child.signalCode === null) {
    try { process.kill(-child.pid, 'SIGTERM'); } catch {}
    const timer = setTimeout(() => { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }, 1000);
    await closed;
    clearTimeout(timer);
  }
  await rm(scratch, {recursive: true, force: true});
}

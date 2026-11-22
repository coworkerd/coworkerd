// An application abort must retire its broken tree promptly even while ordinary HTTP callers retry.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const [nodeBinary, workerd, runtime] = process.argv.slice(2);
assert.equal(process.platform, 'linux');
const scratch = await mkdtemp(join(tmpdir(), 'cwkd-reset-progress-'));
const fixture = join(scratch, 'fixture');
const configPath = join(fixture, 'node.json');
const manifestPath = join(fixture, 'workers', 'manifest.json');
const env = {
  ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(COWORKERD_|WORKERD_)/.test(key))),
  TMPDIR: scratch, COWORKERD_DO_TEST_HOME_COUNT: '1', COWORKERD_CPU_LIMIT_MS: '250',
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
  await mkdir(join(fixture, 'runtime'));
  await copyFile(runtime, join(fixture, 'runtime', 'runtime.js'));
  const source = `
    import { DurableObject } from "cloudflare:workers";
    let isolate;
    export class Counter extends DurableObject {
      instance = crypto.randomUUID();
      async fetch(request) {
        isolate ??= crypto.randomUUID();
        const query = new URL(request.url).searchParams;
        if (query.has("abort")) this.ctx.abort("fixture application reset");
        if (query.has("cpu")) {
          let checksum = 1;
          for (let i = 0; i < 200000000; ++i) checksum = Math.imul(checksum, 1664525) + i | 0;
          this.checksum = checksum;
        }
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
  let initial = await successful('/reset', 'retained after abort');
  const neighbor = await successful('/neighbor', 'neighbor retained');
  assert.equal(initial.isolate, neighbor.isolate);
  for (const cause of ['abort', 'cpu']) {
    const aborted = await request('/reset?' + cause);
    assert.ok([502, 503].includes(aborted.status), JSON.stringify(aborted));
    if (cause === 'abort') assert.match(JSON.stringify(aborted.body), /fixture application reset/);
    else {
      assert.equal(aborted.status, 503);
      assert.equal(aborted.body.code, 'CPU_LIMIT_EXCEEDED');
    }
    const started = performance.now();
    const statuses = {};
    let recovered;
    // The two-second deadline is below the ten-second idle retirement delay.
    // Failed sessions must not postpone abort cleanup when requests keep arriving.
    while (performance.now() - started < 2000) {
      const result = await request('/reset');
      statuses[result.status] = (statuses[result.status] ?? 0) + 1;
      if (result.status === 200) { recovered = result.body; break; }
      assert.ok([502, 503].includes(result.status), JSON.stringify(result));
      await delay(10);
    }
    const currentChild = (await state()).find(entry => entry.processClass === 'durable');
    assert.equal(currentChild.pid, originalChild.pid);
    assert.equal(currentChild.generation, originalChild.generation);
    assert.equal(currentChild.state, 'ready');
    assert.deepEqual(await successful('/neighbor'), neighbor);
    const elapsedMs = performance.now() - started;
    console.log(JSON.stringify({event: 'durable-reset-retries', cause, elapsedMs, statuses,
      recovered: Boolean(recovered), sameChild: true, neighborPreserved: true}));
    assert.ok(recovered, 'an aborted tree did not recover under steady retries within two seconds');
    assert.notEqual(recovered.instance, initial.instance);
    assert.equal(recovered.isolate, initial.isolate);
    assert.equal(recovered.id, initial.id);
    assert.equal(recovered.value, initial.value);
    assert.deepEqual(await successful('/reset'), recovered);
    initial = recovered;
  }
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

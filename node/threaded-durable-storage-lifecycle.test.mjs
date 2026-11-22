// Real restarts with unchanged configuration must discard disposable storage, not actor state on reads.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const [nodeBinary, workerd, runtime] = process.argv.slice(2);
assert.equal(process.platform, 'linux');
const scratch = await mkdtemp(join(tmpdir(), 'cwkd-storage-lifecycle-'));
const fixture = join(scratch, 'fixture');
const configPath = join(fixture, 'node.json');
const manifestPath = join(fixture, 'workers', 'manifest.json');
const env = {...Object.fromEntries(Object.entries(process.env)
  .filter(([key]) => !/^(COWORKERD_|WORKERD_)/.test(key))), TMPDIR: scratch};
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

async function request(value) {
  const response = await fetch(`http://127.0.0.1:${port}/same-object`, {
    ...(value === undefined ? {} : {method: 'POST', body: value}), signal: AbortSignal.timeout(5000),
  });
  const body = await response.text();
  assert.equal(response.status, 200, body + '\n' + diagnostics);
  return JSON.parse(body);
}

try {
  const modules = join(fixture, 'workers', 'counter', 'v1');
  await mkdir(modules, {recursive: true});
  await mkdir(join(fixture, 'runtime'));
  await copyFile(runtime, join(fixture, 'runtime', 'runtime.js'));
  const source = `
    import { DurableObject } from "cloudflare:workers";
    export class Counter extends DurableObject {
      instance = crypto.randomUUID();
      async fetch(request) {
        if (request.method === "POST") await this.ctx.storage.put("value", await request.text());
        return Response.json({id: this.ctx.id.toString(), instance: this.instance,
          value: await this.ctx.storage.get("value") ?? null});
      }
    }
    export default {fetch(request, env) {
      return env.COUNTERS.get(env.COUNTERS.idFromName("same-object")).fetch(request);
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

  const firstChild = await start();
  const firstNode = child.pid;
  const first = await request('before child restart');
  assert.equal(first.value, 'before child restart');
  assert.deepEqual(await request(), first);
  // Replace the actual child in the same running node and fixture stack.
  process.kill(firstChild.pid, 'SIGTERM');
  const replacement = await ready(firstChild);
  assert.notEqual(replacement.pid, firstChild.pid);
  assert.equal(child.pid, firstNode);
  const emptyChild = await request();
  assert.equal(emptyChild.id, first.id);
  assert.notEqual(emptyChild.instance, first.instance);
  assert.equal(emptyChild.value, null, 'a new child generation must not recover disposable data');
  const second = await request('before node restart');
  assert.deepEqual(await request(), second);
  await stop();

  // Reuse the exact source, manifest, node config, arguments and object identity.
  assert.equal(await readFile(configPath, 'utf8'), config);
  assert.equal(await readFile(manifestPath, 'utf8'), manifest);
  const restarted = await start();
  assert.notEqual(child.pid, firstNode);
  assert.notEqual(restarted.pid, replacement.pid);
  const emptyNode = await request();
  assert.equal(emptyNode.id, first.id);
  assert.notEqual(emptyNode.instance, second.instance);
  assert.equal(emptyNode.value, null, 'restarting the same fixture must start with empty test storage');
  const third = await request('after node restart');
  assert.equal(third.value, 'after node restart');
  assert.deepEqual(await request(), third);
  await stop();
  console.log(JSON.stringify({event: 'disposable-storage-restarts', sameObject: first.id,
    configSha256: hash(config), manifestSha256: hash(manifest),
    childGenerations: [firstChild.generation, replacement.generation, restarted.generation],
    childRestartEmpty: true, nodeRestartEmpty: true, writesAndReads: true}));
} finally {
  if (child?.pid && child.exitCode === null && child.signalCode === null) {
    try { process.kill(-child.pid, 'SIGTERM'); } catch {}
    const timer = setTimeout(() => { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }, 1000);
    await closed;
    clearTimeout(timer);
  }
  await rm(scratch, {recursive: true, force: true});
}

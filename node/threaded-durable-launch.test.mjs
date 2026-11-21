import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, symlink, unlink, writeFile } from 'node:fs/promises';
import { get } from 'node:http';
import { createConnection, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';

const [nodeBinary, workerd, runtime] = process.argv.slice(2);
const exec = promisify(execFile);
const scratch = await mkdtemp(join(tmpdir(), 'cwkd-durable-'));
const env = {
  ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(COWORKERD_|WORKERD_)/.test(key))),
  TMPDIR: scratch,
};
const fixture = join(scratch, 'fixture');
const configPath = join(fixture, 'node.json');
const manifestPath = join(fixture, 'workers', 'manifest.json');
const workerId = 'fixture/counter';
const token = 'fixture-management-placeholder';
const config = {
  nodeId: 'fixture',
  durableObjects: { processes: 1, threadedTestStorage: true },
  processPool: { count: 0, inFlightLimit: 8 },
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

function readAdmission(socketPath) {
  return new Promise((resolve, reject) => {
    get({ socketPath, path: '/__coworkerd/threaded' }, async response => {
      try {
        assert.equal(response.statusCode, 200);
        const chunks = [];
        for await (const chunk of response) chunks.push(chunk);
        resolve(JSON.parse(Buffer.concat(chunks).toString()).durableAdmission);
      } catch (error) {
        reject(error);
      }
    }).once('error', reject);
  });
}

function durablePreface(objectId, invocationId, queuedBytes, expiry = process.hrtime.bigint() + 5_000_000_000n) {
  return 'D' + JSON.stringify({
    namespaceId: 'a'.repeat(64), objectId, invocationId, className: 'Counter', definition: 'fixture',
    generation: '1', dataEpoch: '1', shardEpoch: '1', openId: '1',
    queuedBytes: String(queuedBytes), expiresMonotonicNs: String(expiry),
  }) + '\n';
}

async function waitForAdmission(path, waiting, homeSessions = 0) {
  let state;
  const until = performance.now() + 2000;
  do {
    state = await readAdmission(path);
    if (state.waiting === waiting && state.homeSessions === homeSessions) return state;
    await delay(20);
  } while (performance.now() < until);
  assert.fail(`Admission counters did not reach ${waiting} waiting and ${homeSessions} home sessions: ${JSON.stringify(state)}`);
}

let child, exited;
try {
  const modules = join(fixture, 'workers', workerId, 'v1');
  await mkdir(modules, { recursive: true });
  await mkdir(join(fixture, 'runtime'));
  await copyFile(runtime, join(fixture, 'runtime', 'runtime.js'));
  const source = 'export default { fetch() { return new Response("fixture"); } };';
  const contentHash = createHash('sha256').update(source).digest('hex');
  await writeFile(join(modules, 'index.js'), source);
  await writeFile(manifestPath, JSON.stringify({
    compatibilityDate: '2026-08-27',
    routes: [{ host: '*', pathPrefix: '/', workerId }],
    workers: [{ workerId, activeVersion: 'v1', versions: [
      { versionId: 'v1', mainModule: 'index.js', contentHash, bindings: [] },
    ] }],
  }));

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

  const threaded = join(dirname(workerd), 'coworkerd-threaded');
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
  let diagnostics = '';
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
      if (response.ok && state.processes.entries.some(entry => entry.state === 'ready')) {
        ready = state.processes.entries;
        break;
      }
    } catch {}
    await delay(30);
  }
  assert.ok(ready, diagnostics);
  assert.equal(ready.length, 1);
  const [processInfo] = ready;
  assert.equal(processInfo.processClass, 'durable');
  assert.equal(processInfo.generation, 1);
  assert.equal(processInfo.sandbox.durableServer, 'threaded-test');
  assert.equal(processInfo.sandbox.threadedProtocol, 3);
  assert.equal(processInfo.sandbox.defaultCpuLimitMs, 30000);
  const environment = (await readFile(`/proc/${processInfo.pid}/environ`, 'utf8')).split('\0');
  assert.ok(environment.some(value => value.startsWith('COWORKERD_PARENT_FD=')));
  assert.ok(environment.every(value => !value.startsWith('COWORKERD_DO_WAL_FD=')));
  const paths = (await readdir(scratch)).filter(name => name.startsWith('coworkerd-durable-test-'));
  assert.equal(paths.length, 1);
  const socketPath = join(scratch, paths[0], 'run', processInfo.socket);
  const ordinary = await sendPreface(socketPath, 'T');
  ordinary.socket.destroy();
  assert.equal(ordinary.receipt, 'R');
  const objectId = 'b'.repeat(64);
  const held = [];
  try {
    for (const [id, sequence, bytes, expected] of [
      [objectId, 'first', 32768, 'A'],
      [objectId, 'second', 32768, 'A'],
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
    assert.equal(loaded.waitingBytes, 66560);
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
  assert.equal(output.processes.entries[0].pid, processInfo.pid);
  assert.equal(output.processes.entries[0].generation, processInfo.generation);
  assert.equal(output.processes.entries[0].state, 'ready');
  assert.deepEqual((await readdir(fixture)).sort(), ['node.json', 'runtime', 'workers']);
  child.kill('SIGTERM');
  await exited;
  assert.equal(child.exitCode, 0);
  assert.deepEqual(await readdir(scratch), ['fixture']);
} finally {
  if (child?.pid && child.exitCode === null && child.signalCode === null) {
    try { process.kill(-child.pid, 'SIGTERM'); } catch {}
    const timer = setTimeout(() => { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }, 1500);
    await exited;
    clearTimeout(timer);
  }
  await rm(scratch, { recursive: true, force: true });
}

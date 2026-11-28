// Exercise the real Linux process launcher and inspect only the effective limit in each ready workerd child.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const [nodeBinary, threaded] = process.argv.slice(2);
assert.equal(process.platform, 'linux');
const reservePort = () => new Promise((resolve, reject) => {
  const socket = createServer();
  socket.on('error', reject);
  socket.listen(0, '127.0.0.1', () => {
    const { port } = socket.address();
    socket.close(error => error ? reject(error) : resolve(port));
  });
});
const cases = [
  ['child-memory-default-128-mib', {}, {}, 128],
  ['child-memory-configured-64-mib', { memoryLimitMiB: 64 }, {}, 64],
  ['child-memory-disabled-by-environment', { memoryLimitMiB: 64 }, { COWORKERD_MEMORY_LIMIT_MIB: '0' }, 0],
];
const environment = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(?:COWORKERD_|WORKERD_)/.test(key)));
const limitKey = 'COWORKERD_MEMORY_LIMIT_MIB=';
for (const [label, options, override, expected] of cases) {
  const stack = await mkdtemp(join(tmpdir(), 'node-child-memory-'));
  let child, closed, exited = false, diagnostic = '';
  try {
    await mkdir(join(stack, 'workers'));
    const manifest = join(stack, 'workers/manifest.json');
    const workerId = 'memory-component';
    const source = 'export default { fetch(){ return new Response("ready") } };';
    const contentHash = createHash('sha256').update(source).digest('hex');
    await mkdir(join(stack, 'workers', workerId, 'v1'), {recursive: true});
    await writeFile(join(stack, 'workers', workerId, 'v1/index.js'), source);
    await writeFile(manifest, JSON.stringify({ compatibilityDate: '2026-08-27',
      routes: [{host:'*', pathPrefix:'/', workerId}],
      workers: [{workerId, activeVersion:'v1', versions:[{versionId:'v1', mainModule:'index.js', contentHash, bindings:[]}]}] }));
    const port = await reservePort();
    const config = join(stack, 'node.json');
    await writeFile(config, JSON.stringify({ nodeId: 'memory-component', processPool: { count: 1 },
      quarantineProcesses: 0, sandbox: { mode: 'off', syscalls: { mode: 'off' } }, management: { allowWithoutToken: true }, memoryWatermarkEnabled: false,
      peerListenAddress: '127.0.0.1:0', ...options }));
    child = spawn(nodeBinary, [manifest, threaded, `127.0.0.1:${port}`, '127.0.0.1:0', '127.0.0.1:0', config],
      { env: { ...environment, ...override }, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stderr.on('data', chunk => diagnostic = (diagnostic + chunk.toString()).slice(-3000));
    child.stdout.resume();
    closed = new Promise(resolve => {
      child.on('error', error => { diagnostic += error.message; });
      child.on('close', () => { exited = true; resolve(); });
    });
    let view;
    for (const until = performance.now() + 3500; !exited && performance.now() < until;) {
      try {
        const response = await fetch(`http://127.0.0.1:${port}/__coworkerd/state`, {signal: AbortSignal.timeout(250)});
        if (response.ok) {
          const state = await response.json();
          if (state.processes.entries.some(p => p.state === 'ready' && p.pid > 0)) { view = state; break; }
        }
      } catch {}
      await delay(20);
    }
    assert.ok(view, `${label}: no ready workerd child: ${diagnostic}`);
    assert.equal(view.memoryLimitMiB, expected, label);
    assert.equal(view.processes.configuration.memoryLimitMiB, expected, label);
    assert.equal(view.processes.configuration.durableProcesses, 0, 'file membership has no persistent durable children');
    const regular = view.processes.entries.filter(p => p.state === 'ready' && p.pid > 0);
    assert.ok(regular.length > 0, label);
    for (const {pid} of regular) {
      const status = await readFile(`/proc/${pid}/status`, 'utf8');
      assert.match(status, new RegExp(`^PPid:\\s+${child.pid}\\s*$`, 'm'), label);
      const actual = (await readFile(`/proc/${pid}/environ`, 'utf8')).split('\0').filter(e => e.startsWith(limitKey));
      assert.deepEqual(actual, [`${limitKey}${expected}`], label);
    }
    assert.deepEqual(view.processes.suspects.entries, [], label);
    console.log(`${label}: ready children received ${expected} MiB (${regular.length} checked)`);
  } finally {
    if (child?.pid && !exited) {
      try { process.kill(-child.pid, 'SIGTERM'); } catch {}
      const kill = setTimeout(() => { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }, 700);
      await closed;
      clearTimeout(kill);
    }
    await rm(stack, { recursive: true, force: true });
  }
}

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import { checksumFor } from './hot-worker-checksum.mjs';

const [binary, option, ...extra] = process.argv.slice(2);
assert.ok(binary && (option === undefined || option === '--no-maglev-inlining') && !extra.length,
  'Usage: node tests/cluster/hot-worker-v8-control.mjs <Linux-workerd> [--no-maglev-inlining]');

const iterations = 150_000_000;
const checksum = checksumFor(iterations);

const tenant = `const iterations = ${iterations};
${checksum.toString()}
export default { fetch(request) {
  const id = new URL(request.url).searchParams.get('id');
  return Response.json({ id, checksum: checksum() });
} };`;
const entry = `export default { async fetch(request, env) {
  const worker = env.LOADER.get('hot-worker', async () => ({
    compatibilityDate: '2026-08-27', mainModule: 'index.js', modules: { 'index.js': { js: ${JSON.stringify(tenant)} } },
  }));
  return worker.getEntrypoint().fetch(request);
} };`;

const flags = ['--trace-opt', '--trace-deopt', '--trace-osr', '--trace-gc'];
if (option) flags.push(option);
const fixture = await mkdtemp(join(tmpdir(), 'coworkerd-worker-v8-control-'));
let child;
let exited;
let completed = 0;
const emit = record => console.log(JSON.stringify(record));
try {
  await writeFile(join(fixture, 'entry.js'), entry);
  await writeFile(join(fixture, 'config.capnp'), `using W = import "/workerd/workerd.capnp";
const config: W.Config = (
  v8Flags = ${JSON.stringify(flags)},
  services = [(name = "test", worker = (compatibilityDate = "2026-08-27",
    modules = [(name = "index.js", esModule = embed "entry.js")],
    bindings = [(name = "LOADER", workerLoader = (id = "hot-worker"))]))],
  sockets = [(name = "http", address = "127.0.0.1:0", http = (), service = "test")]);`);
  const expected = checksum();
  child = spawn('stdbuf', ['-oL', '-eL', resolve(binary), 'serve', '--experimental', '--control-fd=3', 'config.capnp'],
    { cwd: fixture, stdio: ['ignore', 'pipe', 'inherit', 'pipe'] });
  exited = new Promise(resolve => child.once('close', resolve));
  createInterface({ input: child.stdout }).on('line', text => emit({ metric: 'v8', completed, text }));
  const port = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('workerd did not publish a listening port')), 10_000);
    child.once('error', error => {
      clearTimeout(timeout);
      reject(error);
    });
    child.once('exit', code => {
      clearTimeout(timeout);
      reject(new Error(`workerd exited before readiness (${code})`));
    });
    createInterface({ input: child.stdio[3] }).on('line', line => {
      const { event, socket, port } = JSON.parse(line);
      if (event === 'listen' && socket === 'http') {
        clearTimeout(timeout);
        resolve(port);
      }
    });
  });

  emit({ metric: 'control', iterations, flags });
  const started = performance.now();
  for (let ordinal = 1; ordinal <= 650; ++ordinal) {
    if (ordinal === 7) await delay(4_000);
    const start = performance.now();
    const id = String(ordinal);
    const response = await fetch(`http://127.0.0.1:${port}/?id=${id}`, { signal: AbortSignal.timeout(30_000) });
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { id, checksum: expected });
    completed = ordinal;
    const now = performance.now();
    emit({ metric: 'reply', ordinal, latencyMs: now - start, elapsedMs: now - started });
  }
} finally {
  if (child && child.exitCode === null && child.signalCode === null) child.kill();
  await exited;
  await rm(fixture, { recursive: true, force: true });
}

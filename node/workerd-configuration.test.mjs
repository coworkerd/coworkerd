// Replay environment and memory-limit contracts on one real Workerd and a dynamic Worker.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const [workerd] = process.argv.slice(2);
assert.ok(workerd, 'supply the patched Workerd from the candidate build');
const fixture = await mkdtemp(join(tmpdir(), 'coworkerd-configuration-'));
const config = join(fixture, 'config.capnp');
const tenant = `const held=[]; export default {fetch(request){
  const url = new URL(request.url);
  if (url.pathname === '/memory') {
    const bytes = new Uint8Array(new ArrayBuffer(Number(url.searchParams.get('mib')) * 1024 * 1024));
    for (let i=0; i<bytes.length; i+=4096) bytes[i]=7;
    held.push(bytes);
    return new Response('retained:'+held.length+':'+bytes.length);
  }
  let sum=0;for(let i=0;i<1000000000;i++)sum+=Math.imul(i,i);return new Response('completed:'+sum);
}};`;
const entry = `export default {async fetch(request,env){
  if (new URL(request.url).pathname === '/ready') return new Response('ready');
  const worker=env.LOADER.get('configuration-dynamic',async()=>({compatibilityDate:'2026-08-27',mainModule:'index.js',
    modules:{'index.js':{js:${JSON.stringify(tenant)}}}}),{coworkerdWorker:{workerId:'configuration-dynamic',versionId:'v1',generation:0}});
  try {return await worker.getEntrypoint().fetch(request)} catch(error) {return new Response(String(error),{status:503})}
}};`;
const rows = [
  ['unset', undefined, undefined, 200], ['zero', '0', undefined, 200],
  ['valid', '200', undefined, 503],
  ['negative', '-1'], ['fraction', '1.5'], ['text', 'invalid'],
  ['overflow', '4294967296'], ['empty', ''],
  ['event-fd-empty', '200', ''], ['event-fd-text', '200', '3a'], ['event-fd-unopened', '200', '987'],
];
const memoryRows = [
  ['memory-default-effective-128-mib', '128', 96, 503],
  ['memory-explicit-64-mib', '64', 48, 503],
  ['memory-disabled', '0', 96, 200],
];
const request = (port, path) => fetch(`http://127.0.0.1:${port}${path}`, { signal: AbortSignal.timeout(4000) })
  .then(async response => ({ status: response.status, body: await response.text() }))
  .catch(error => ({ status: 0, body: '', error: String(error) }));

async function trial([name, limit, eventFd, expected], memory) {
  const env = { ...process.env };
  for (const key of ['WORKERD_ALL_AUTOGATES', 'COWORKERD_CPU_LIMIT_MS', 'COWORKERD_CPU_STATE', 'COWORKERD_EVENT_FD', 'COWORKERD_MEMORY_LIMIT_MIB']) delete env[key];
  if (limit !== undefined) env.COWORKERD_CPU_LIMIT_MS = limit;
  if (name === 'valid') env.COWORKERD_CPU_STATE = join(fixture, 'state');
  if (eventFd !== undefined) env.COWORKERD_EVENT_FD = eventFd;
  if (memory) env.COWORKERD_MEMORY_LIMIT_MIB = memory[1];
  const child = spawn(workerd, ['serve', '--experimental', '--control-fd=3', config],
    { env, stdio: ['ignore', 'ignore', 'pipe', 'pipe'] });
  let stderr = '', control = '', exited = false;
  child.stderr.on('data', chunk => stderr += chunk);
  child.stdio[3].on('data', chunk => control += chunk);
  const closed = new Promise(resolve => {
    child.once('error', error => { stderr += String(error); });
    child.once('close', (code, signal) => { exited = true; resolve({ code, signal }); });
  });
  let port = 0, ready, after, replies = [];
  try {
    for (const until = performance.now() + 4000; !port && !exited && performance.now() < until;) {
      for (const line of control.split('\n').slice(0,-1)) {
        const record = JSON.parse(line);
        if (record.event === 'listen' && record.socket === 'http') port = record.port;
      }
      if (!port) await delay(10);
    }
    assert.ok(port || exited, `${name}: no listener and no startup refusal: ${stderr}`);
    if (port && !exited) {
      ready = await request(port, '/ready');
      if (!exited) assert.deepEqual(ready, { status: 200, body: 'ready' }, name);
      const count = eventFd === undefined && !memory ? 1 : 2;
      for (let i = 0; i < count && !exited; ++i) {
        replies.push(await request(port, memory ? `/memory?mib=${memory[2]}` : '/'));
      }
      if (memory && !exited) after = await request(port, '/ready');
    }
  } finally {
    if (!exited) child.kill('SIGTERM');
    await closed;
  }
  const diagnostic = stderr + replies.map(reply => reply.body).join('\n');
  if (memory) {
    const [, , mib, secondStatus] = memory;
    const allocated = mib * 1024 * 1024;
    assert.deepEqual(replies[0], { status: 200, body: `retained:1:${allocated}` }, name);
    assert.equal(replies[1]?.status, secondStatus, `${name}: ${diagnostic}`);
    assert.match(replies[1].body, secondStatus === 200 ? new RegExp(`^retained:2:${allocated}$`) : /MEMORY_LIMIT_EXCEEDED/, name);
    assert.deepEqual(after, { status: 200, body: 'ready' }, `${name}: original child still responding`);
    console.log(`${name}: ${replies.map(reply => reply.status).join(' → ')}`);
    return;
  }
  if (expected !== undefined) {
    assert.equal(replies[0]?.status, expected, `${name}: ${diagnostic}`);
    assert.match(stderr, /\[coworkerd-platform\]/, name);
    assert.match(replies[0].body, expected === 200 ? /^completed:/ : /CPU_LIMIT_EXCEEDED/, name);
  } else {
    assert.ok(replies.every(reply => reply.status !== 200 && !/^completed:/.test(reply.body)), name);
    assert.match(diagnostic, eventFd !== undefined ? /COWORKERD_EVENT_FD/ : name === 'empty' ?
      /COWORKERD_CPU_LIMIT_MS|coworkerd-cpu-limit\.c\+\+:\d+: failed: expected \*text != / : /COWORKERD_CPU_LIMIT_MS/, name);
  }
  console.log(`${name}: ${expected ?? (replies.length ? 'refused at first tenant load' : 'refused at startup')}`);
}
try {
  await writeFile(join(fixture, 'entry.js'), entry);
  await writeFile(config, `using W=import "/workerd/workerd.capnp";
const config:W.Config=(services=[(name="test",worker=(compatibilityDate="2026-08-27",modules=[(name="index.js",esModule=embed "entry.js")],bindings=[(name="LOADER",workerLoader=(id="configuration"))]))],sockets=[(name="http",address="127.0.0.1:0",http=(),service="test")]);`);
  for (const row of rows) await trial(row);
  for (const row of memoryRows) await trial([row[0], '0'], row);
} finally {
  await rm(fixture, { recursive: true, force: true });
}

import assert from 'node:assert/strict';
import { lookup } from 'node:dns/promises';
import { createServer, get } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';

const [role, planArg, rampArg] = process.argv.slice(2);
assert.ok(role === 'client' || role === 'upstream');
const plan = planArg.split(',').map(Number);
const rampMs = Number(rampArg);
const waitMs = 2000;
const target = role === 'client' ? (await lookup('node-1-peer', { family: 4 })).address : undefined;
// Linux CLOCK_MONOTONIC in both containers, on the same Docker VM.
const now = () => Number(process.hrtime.bigint()) / 1e6;
let nextBatch = 0;
let batch;
let active = 0;

function invoke(id) {
  return new Promise((resolve, reject) => {
    const request = get('http://' + target + ':8930/waiting-capacity/wait?call=' + id,
      { agent: false, signal: AbortSignal.timeout(12000) }, response => {
        response.setEncoding('utf8');
        let text = '';
        response.on('data', chunk => { text += chunk; });
        response.on('error', reject);
        response.on('end', () => {
          let body;
          try { body = JSON.parse(text); } catch { body = {}; }
          resolve({ status: response.statusCode, nodeId: body.nodeId, upstream: body.upstream,
            code: body.error?.code ?? body.code, reason: body.error?.reason ?? body.reason });
        });
      });
    request.on('error', reject);
  });
}

function arm() {
  const number = nextBatch++;
  const calls = Array.from({ length: plan[number] }, (_, i) => ({ id: number + '-' + i }));
  batch = { number, calls, completed: 0, entered: 0, released: 0 };
  if (role === 'client') {
    const start = now();
    for (const [index, call] of calls.entries()) {
      const offer = () => {
        call.sentMs = now();
        ++active;
        invoke(call.id).then(
          result => { call.result = result; },
          error => { call.result = { transportError: error.code ?? error.name }; },
        ).finally(() => {
          call.finishedMs = now();
          --active;
          ++batch.completed;
        });
      };
      if (rampMs === 0) offer();
      else setTimeout(offer, Math.max(0, start + rampMs * index / calls.length - now()));
    }
  }
}

if (role === 'upstream') {
  createServer(async (request, response) => {
    response.setHeader('connection', 'close');
    if (request.method === 'GET' && request.url === '/warm') return response.end('warm');
    const match = request.method === 'GET' && /^\/hold\/(\d+)-(\d+)$/.exec(request.url);
    const call = match && Number(match[1]) === batch?.number && batch.calls[Number(match[2])];
    if (!call || call.enteredMs !== undefined) {
      response.writeHead(404).end();
      return;
    }
    call.enteredMs = now();
    ++active;
    ++batch.entered;
    const controller = new AbortController();
    response.once('close', () => controller.abort());
    try {
      let remaining = waitMs;
      do {
        await delay(Math.ceil(remaining), undefined, { signal: controller.signal });
        remaining = call.enteredMs + waitMs - now();
      } while (remaining > 0);
      call.releasedMs = now();
      ++batch.released;
      response.end(call.id);
    } catch (error) {
      if (error.name !== 'AbortError') throw error;
      call.abortedMs = now();
    } finally {
      --active;
    }
  }).listen({ port: 8080, host: '0.0.0.0', backlog: 16384 });
}

createServer(async (request, response) => {
  const reply = value => response.end(JSON.stringify(value));
  const status = () => ({ atMs: now(), number: batch?.number, active,
    entered: batch?.entered, released: batch?.released, completed: batch?.completed });
  if (request.method === 'GET' && request.url === '/status') {
    return reply({ ...status(), waitMs, nodeVersion: process.version });
  }
  if (request.method === 'GET' && request.url === '/result' && batch) {
    return reply({ ...status(), calls: batch.calls });
  }
  if (request.method === 'POST' && request.url === '/batch' && nextBatch < plan.length && active === 0 &&
      (role === 'upstream' || !batch || batch.completed === batch.calls.length)) {
    arm();
    return reply({ number: batch.number, offered: batch.calls.length });
  }
  if (request.method === 'POST' && request.url === '/warm' && role === 'client' && active === 0) {
    try {
      return reply(await invoke('warm'));
    } catch (error) {
      return reply({ transportError: error.code ?? error.name });
    }
  }
  response.writeHead(404).end();
}).listen(8081, '127.0.0.1');

import { createServer, get } from 'node:http';

// This container shares the fixture's private backend. The client and upstream
// use one clock and report their combined process resources.
const plan = [8, 16, 24, 16, 24, 8, 24, 8, 16];
const waitMs = 2000;
const now = () => performance.now();
let nextBatch = 0;
let batch;

function invoke(id) {
  return new Promise((resolve, reject) => {
    const request = get(`http://node-1-peer:8930/waiting-capacity/wait?call=${id}`, { agent: false }, response => {
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
    request.setTimeout(8000, () => request.destroy(new Error('client socket timed out')));
  });
}

const upstream = createServer((request, response) => {
  response.setHeader('connection', 'close');
  if (request.method === 'GET' && request.url === '/warm') return response.end('warm');
  const call = request.method === 'GET' && batch?.byPath.get(request.url);
  if (!call || call.enteredMs !== undefined) {
    response.writeHead(404).end();
    return;
  }
  call.enteredMs = now();
  setTimeout(() => {
    call.releasedMs = now();
    response.end(call.id);
  }, waitMs);
});

function observation() {
  const calls = batch?.calls ?? [];
  const entered = calls.filter(call => call.enteredMs !== undefined).length;
  const released = calls.filter(call => call.releasedMs !== undefined).length;
  const completed = calls.filter(call => call.finishedMs !== undefined).length;
  return { atMs: now(), entered, released, completed, active: entered - released };
}

function startBatch() {
  const number = nextBatch++;
  const calls = Array.from({ length: plan[number] }, (_, index) => ({ id: `${number}-${index}` }));
  batch = { number, calls, byPath: new Map(calls.map(call => [`/hold/${call.id}`, call])) };
  for (const call of calls) {
    call.sentMs = now();
    invoke(call.id).then(
      result => { call.result = result; },
      error => { call.result = { transportError: error.message }; },
    ).finally(() => { call.finishedMs = now(); });
  }
}

upstream.listen(8080, '0.0.0.0', () => {
  createServer(async (request, response) => {
    const reply = value => response.end(JSON.stringify(value));
    if (request.method === 'GET' && request.url === '/status') {
      return reply({ ...observation(), number: batch?.number, plan, waitMs, nodeVersion: process.version });
    }
    if (request.method === 'GET' && request.url === '/result' && batch) {
      return reply({ ...observation(), calls: batch.calls });
    }
    if (request.method === 'POST' && request.url === '/batch' && nextBatch < plan.length &&
        (!batch || observation().completed === batch.calls.length)) {
      startBatch();
      return reply({ number: batch.number, offered: batch.calls.length });
    }
    if (request.method === 'POST' && request.url === '/warm' && (!batch || observation().completed === batch.calls.length)) {
      try {
        return reply(await invoke('warm'));
      } catch (error) {
        return reply({ transportError: error.message });
      }
    }
    response.writeHead(404).end();
  }).listen(8081, '127.0.0.1');
});

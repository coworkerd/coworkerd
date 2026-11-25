import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { rename, unlink, writeFile } from 'node:fs/promises';
import { get } from 'node:http';
import { createRequire } from 'node:module';
import { createConnection } from 'node:net';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { eventually, json, request } from './cluster.mjs';

function respondWith(value) {
  return `export default { fetch() { return new Response(${JSON.stringify(value)}); } };`;
}

async function deployment(cluster) {
  const tenant = await cluster.tenant('deploy-test');
  const path = `/${tenant.name}/hello`;
  for (const version of ['v1', 'v2']) {
    const expected = `hello from ${version}`;
    await cluster.deploy(tenant, 'hello', respondWith(expected), { version });
    await eventually(`${version} serves on all three ingresses`, async () => {
      const observed = await Promise.all(cluster.nodes.map(node => request(cluster.urls[node], path)));
      assert.deepEqual(Object.fromEntries(cluster.nodes.map((node, i) => [node, observed[i]])),
        Object.fromEntries(cluster.nodes.map(node => [node, { status: 200, text: expected }])));
    });
  }
}

async function webSocketAdmission(cluster) {
  const [node, otherNode] = cluster.nodes;
  const { WebSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const source = `export default { async fetch(request) {
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') return new Response('ready');
    const path = new URL(request.url).pathname;
    if (path.endsWith('/pending')) {
      await new Promise(resolve => setTimeout(resolve, 6000));
      return new Response(null, { status: 403 });
    }
    if (path.endsWith('/decline')) {
      const body = new ReadableStream({ start(controller) {
        controller.enqueue(new TextEncoder().encode('declined'));
        setTimeout(() => controller.close(), 15000);
      } });
      return new Response(body, { status: 403 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.addEventListener('close', event => server.close(event.code, event.reason));
    return new Response(null, { status: 101, webSocket: client });
  } };`;
  const first = await cluster.tenant('socket-first');
  const second = await cluster.tenant('socket-second');
  for (const tenant of [first, second]) {
    await cluster.deploy(tenant, 'socket', source);
    await eventually('Worker ready on both ingresses', async () => {
      for (const ingress of [node, otherNode]) {
        assert.deepEqual(await workerCall(cluster, ingress, tenant, 'socket'), { status: 200, text: 'ready' });
      }
    });
  }
  const sockets = [];
  const url = (ingress, tenant, suffix = '') => new URL(`/${tenant.name}/socket${suffix}`, cluster.urls[ingress]);
  const waitFor = (socket, event) => once(socket, event, { signal: AbortSignal.timeout(5000) });
  function connect(ingress, tenant, suffix) {
    const target = url(ingress, tenant, suffix);
    target.protocol = 'ws:';
    const socket = new WebSocket(target);
    socket.on('error', () => {});
    sockets.push(socket);
    return socket;
  }
  async function open(ingress, tenant) {
    const socket = connect(ingress, tenant);
    await waitFor(socket, 'open');
    return socket;
  }
  const decline = () => new Promise((resolve, reject) => {
    const request = get(url(node, second, '/decline'), {
      headers: { Connection: 'Upgrade', Upgrade: 'websocket', 'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': randomBytes(16).toString('base64') },
    });
    const timeout = setTimeout(() => request.destroy(new Error('upgrade response headers timed out')), 5000);
    request.once('response', response => {
      clearTimeout(timeout);
      resolve(response);
    }).once('error', error => {
      clearTimeout(timeout);
      reject(error);
    }).once('upgrade', (_response, socket) => {
      clearTimeout(timeout);
      socket.destroy();
      reject(new Error('the Worker should refuse the upgrade'));
    });
  });
  const state = async () => (await cluster.state(node)).webSockets;
  const inUse = (count, timeout = 5000) => eventually(`node has ${count} WebSocket reservations`, async () => {
    assert.equal((await state()).inUse, count);
  }, { timeout });
  try {
    assert.equal((await state()).limitPerNode, 2);
    const otherState = await cluster.state(otherNode);
    assert.equal(otherState.webSockets.limitPerNode, 2048);
    const descriptors = await cluster.compose(['exec', '-T', otherNode, 'cat', `/proc/${otherState.server.pid}/limits`]);
    assert.match(descriptors, /^Max open files\s+15488\s+524288\s/m);
    const a = await open(node, first);
    const b = await open(node, second);
    const full = await state();
    assert.equal(full.inUse, 2);
    assert.deepEqual(full.places, { [first.name]: 1, [second.name]: 1 });
    const denied = await decline();
    assert.equal(denied.statusCode, 503);
    let body = '';
    for await (const chunk of denied) body += chunk;
    const { code, limit, inFlight, dispatched, retryable } = JSON.parse(body);
    assert.deepEqual({ code, limit, inFlight, dispatched, retryable },
      { code: 'WEBSOCKET_NODE_CAPACITY', limit: 2, inFlight: 2, dispatched: false, retryable: true });
    assert.deepEqual(await workerCall(cluster, node, second, 'socket'), { status: 200, text: 'ready' });
    await open(otherNode, second);

    const closed = waitFor(a, 'close');
    a.close(1000, 'done');
    assert.equal((await closed)[0], 1000);
    await inUse(1);

    const target = url(node, first, '/pending');
    const pending = createConnection({ host: target.hostname, port: Number(target.port), allowHalfOpen: true });
    pending.on('error', () => {});
    try {
      await waitFor(pending, 'connect');
      pending.write([
        `GET ${target.pathname} HTTP/1.1`, `Host: ${target.host}`,
        'Connection: Upgrade', 'Upgrade: websocket', 'Sec-WebSocket-Version: 13',
        `Sec-WebSocket-Key: ${randomBytes(16).toString('base64')}`, '', '',
      ].join('\r\n'));
      await inUse(2);
      // A FIN from a pending upgrade cannot yield a bidirectional relay.
      const disconnectedAt = performance.now();
      pending.end();
      await inUse(1, 1000);
      const releasedIn = performance.now() - disconnectedAt;
      assert.ok(releasedIn < 1000, `pending WebSocket place took ${releasedIn.toFixed(1)} ms to return`);
      console.log(`Pending WebSocket place returned in ${releasedIn.toFixed(1)} ms`);
    } finally {
      pending.destroy();
    }

    const declined = await decline();
    declined.on('error', () => {});
    try {
      assert.equal(declined.statusCode, 403);
      await inUse(1);
      assert.equal(declined.complete, false, 'the upgrade place is free while the HTTP body is still streaming');
      assert.equal(declined.destroyed, false);
      await open(node, first);
      await inUse(2);
    } finally {
      declined.destroy();
    }

    b.terminate();
    await inUse(1);
    await open(node, second);
    await inUse(2);
  } finally {
    for (const socket of sockets) socket.terminate();
  }
}

async function sqlAtomicity(cluster) {
  const { path, token } = await cluster.tenant('sql-test');
  const database = `${path}/databases/ledger`;
  await cluster.api(database, { method: 'POST', token });
  const query = (body) => cluster.api(`${database}/query`, { method: 'POST', token, body, timeout: 35000 });
  // The database's namespace can take time to become visible on the nodes.
  await eventually('database serves SELECT', async () => {
    const probe = await query({ sql: 'SELECT 1 AS ready' });
    assert.deepEqual(probe.results, [{ ready: 1 }]);
  });

  const insert = (id, value) => ({ sql: 'INSERT INTO ledger VALUES (?, ?)', params: [id, value] });
  const read = async () => (await query({ sql: 'SELECT * FROM ledger ORDER BY id' })).results;
  assert.equal((await query({ sql: 'CREATE TABLE ledger(id INTEGER PRIMARY KEY, value TEXT)' })).success, true);
  const committed = await query([insert(1, 'one'), insert(2, 'two')]);
  assert.deepEqual(committed.map(result => result.success), [true, true]);
  const rows = [{ id: 1, value: 'one' }, { id: 2, value: 'two' }];
  assert.deepEqual(await read(), rows, 'both committed statements are visible');

  const refused = await query([insert(3, 'partial'), insert(1, 'duplicate'), insert(4, 'after error')]);
  assert.equal(refused[0]?.success, false, 'the duplicate primary key must reject the transaction');
  assert.match(refused[0].error, /UNIQUE constraint failed/);
  assert.deepEqual(await read(), rows, 'the failed transaction left the committed rows intact');
}

async function invocationBudgets(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant('invocation-budget');
  const bindings = [{ binding: 'CHILD', service: 'budget-child' }];
  const runner = `export default { async fetch(request, env) {
    const params = new URL(request.url).searchParams;
    const count = Number(params.get('count') ?? '1');
    const binding = params.has('lowerChild') ? env.LOWER_CHILD : env.CHILD;
    const target = new URL('https://binding.test/');
    if (params.has('childCalls')) target.searchParams.set('count', params.get('childCalls'));
    const limit = request.cf.lease.subrequestLimit;
    let result;
    for (let completed = 0; completed < count; ++completed) {
      const reply = await binding.fetch(target);
      const value = await reply.json();
      if (!reply.ok) return Response.json({ limit, completed, refusal: { status: reply.status, code: value.code } });
      result = value;
    }
    return Response.json({ limit, completed: count, result });
  } };`;
  const leaf = [{ binding: 'CHILD', service: 'budget-leaf' }];
  await cluster.deploy(tenant, 'budget-leaf', 'export default { fetch() { return Response.json("ok"); } };');
  await cluster.deploy(tenant, 'budget-child', runner, { wrangler: { services: leaf } });
  await cluster.deploy(tenant, 'budget-small-child', runner,
    { wrangler: { services: leaf, limits: { subrequests: 2 } } });
  await cluster.deploy(tenant, 'budget-root', runner,
    { wrangler: { services: [...bindings, { binding: 'LOWER_CHILD', service: 'budget-small-child' }] } });
  await cluster.deploy(tenant, 'budget-one', runner,
    { wrangler: { services: bindings, limits: { subrequests: 1 } } });
  const call = async (worker, query) => json(
    await workerCall(cluster, node, tenant, `${worker}?${query}`),
    200, 'the budget test Worker handled its request');
  const normal = await call('budget-root', 'count=100');
  assert.deepEqual(normal, { limit: 10_000, completed: 100, result: { limit: 10_000, completed: 1, result: 'ok' } });
  const nested = await call('budget-one', 'childCalls=200');
  assert.deepEqual(nested, { limit: 1, completed: 1, result: { limit: 10_000, completed: 200, result: 'ok' } });
  const exhausted = { status: 429, code: 'SUBREQUEST_BUDGET_EXHAUSTED' };
  const lowerChild = await call('budget-root', 'childCalls=3&lowerChild');
  assert.deepEqual(lowerChild, { limit: 10_000, completed: 1, result: { limit: 2, completed: 2, refusal: exhausted } });

  await cluster.deploy(tenant, 'budget-object', `
    import { DurableObject } from 'cloudflare:workers';
    export class BudgetActor extends DurableObject {
      async fetch(request) {
        if (new URL(request.url).pathname === '/leaf') return new Response('ok');
        const leaf = this.env.OBJECT.getByName('leaf');
        for (let completed = 0; completed < 200; ++completed) {
          const reply = await leaf.fetch('https://object.test/leaf');
          if (!reply.ok) return reply;
          await reply.text();
        }
        return Response.json({ completed: 200 });
      }
    }
    export default {
      fetch(_request, env) { return env.OBJECT.getByName('parent').fetch('https://object.test/parent'); }
    };
  `, { wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['BudgetActor'] }],
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'BudgetActor' }] } } });
  assert.deepEqual(json(await workerCall(cluster, node, tenant, 'budget-object'), 200, 'Durable Object subrequest budget'),
    { completed: 200 });

  await cluster.api(tenant.path, { method: 'PATCH', body: { defaultLimits: { subrequests: 5, d1Queries: 2 } } });
  const lower = await eventually('tenant budget is visible', async () => {
    const value = await call('budget-root', 'count=6');
    assert.equal(value.limit, 5);
    return value;
  });
  assert.deepEqual(lower, { limit: 5, completed: 5, refusal: exhausted });

  const wrangler = { services: bindings, limits: { subrequests: 10_000_000 } };
  await cluster.deploy(tenant, 'budget-wide', runner, { wrangler });
  const wide = await call('budget-wide', 'count=10');
  assert.equal(wide.limit, 10_000_000);
  assert.equal(wide.completed, 10);
  assert.equal(wide.refusal, undefined);

  const loop = 'budget-loop';
  await cluster.deploy(tenant, loop, 'export default { fetch(request, env) { return env.SELF.fetch(request); } };', {
    wrangler: { services: [{ binding: 'SELF', service: loop }], limits: { subrequests: 10_000 } },
  });
  const loopReply = await workerCall(cluster, node, tenant, loop);
  assert.equal(json(loopReply, 508, 'recursive binding hop limit').code, 'BINDING_HOP_LIMIT');

  await cluster.deploy(tenant, 'budget-egress', `export default { async fetch(request, env) {
    const reply = await env.CHILD.fetch('https://binding.test/');
    if (!reply.ok) return reply;
    await reply.text();
    try {
      await fetch('http://127.0.0.1/');
      return new Response('egress unexpectedly ran');
    } catch (error) {
      return new Response(error.message);
    }
  } };`, { wrangler: { services: bindings, limits: { subrequests: 1 } } });
  const egress = await workerCall(cluster, node, tenant, 'budget-egress');
  assert.equal(egress.status, 200);
  assert.match(egress.text, /SUBREQUEST_BUDGET_EXHAUSTED/);
}

async function d1InvocationBudgets(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant('d1-budget');
  await cluster.api(tenant.path, { method: 'PATCH', body: { defaultLimits: { subrequests: 5, d1Queries: 2 } } });
  const { storage: { tenantsRevision } } = await cluster.api('/v1/status');
  await eventually('the D1 budget is visible on every node', async () => {
    const states = await Promise.all(cluster.nodes.map(id => cluster.state(id)));
    assert.ok(states.every(state => BigInt(state.storage.appliedTenantsRevision) >= BigInt(tenantsRevision)));
  });
  await cluster.api(`${tenant.path}/databases/budget-db`, { method: 'POST', token: tenant.token });
  await cluster.deploy(tenant, 'budget-child', respondWith('ok'));
  await cluster.deploy(tenant, 'd1-edge', `
    async function exercise(env) {
      const statement = n => env.DB.prepare('SELECT ? AS value').bind(n);
      const batch = await env.DB.batch([statement(1), statement(2)]);
      const query = await statement(3).first('value');
      let refusal;
      try { await statement(4).run(); } catch (error) { refusal = error.message; }
      const bindings = [];
      for (let i = 0; i < 4; i++) {
        const reply = await env.CHILD.fetch('https://binding.test/');
        bindings.push(reply.ok ? await reply.text() : (await reply.json()).code);
      }
      return { batch: batch.map(result => result.results[0].value), query, refusal, bindings };
    }
    export default { async fetch(request, env) {
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') return Response.json(await exercise(env));
      const [client, socket] = Object.values(new WebSocketPair());
      socket.accept();
      socket.addEventListener('message', async () => {
        try { socket.send(JSON.stringify(await exercise(env))); }
        catch (error) { socket.send(JSON.stringify({ unexpected: error.message })); }
      });
      return new Response(null, { status: 101, webSocket: client });
    } };
  `, { wrangler: { services: [{ binding: 'CHILD', service: 'budget-child' }],
    d1_databases: [{ binding: 'DB', database_name: 'budget-db' }] } });
  const verify = ({ batch, query, refusal, bindings, unexpected }) => {
    assert.equal(unexpected, undefined);
    assert.deepEqual(batch, [1, 2]);
    assert.equal(query, 3);
    assert.match(refusal, /^D1_ERROR: D1_QUERY_BUDGET_EXHAUSTED:/);
    assert.deepEqual(bindings, ['ok', 'ok', 'ok', 'SUBREQUEST_BUDGET_EXHAUSTED']);
  };
  verify(json(await workerCall(cluster, node, tenant, 'd1-edge'), 200, 'D1 budget over HTTP'));
  const { WebSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const url = new URL(`/${tenant.name}/d1-edge`, cluster.urls[node]);
  url.protocol = 'ws:';
  const socket = new WebSocket(url);
  try {
    const waitFor = event => once(socket, event, { signal: AbortSignal.timeout(10000) });
    await waitFor('open');
    for (const message of ['first', 'second']) {
      const reply = waitFor('message');
      socket.send(message);
      verify(JSON.parse((await reply)[0]));
    }
  } finally {
    socket.terminate();
  }

  await cluster.deploy(tenant, 'd1-actor', `
    import { DurableObject } from 'cloudflare:workers';
    export class BudgetActor extends DurableObject {
      async fetch(request) {
        if (new URL(request.url).pathname === '/child') return new Response('child');
        const statement = n => this.env.DB.prepare('SELECT ? AS value').bind(n);
        const batch = await this.env.DB.batch([statement(1), statement(2)]);
        const query = await statement(3).first('value');
        let refusal;
        try { await statement(4).run(); } catch (error) { refusal = error.message; }
        const child = this.env.OBJECT.getByName('child');
        const ran = await child.fetch('https://object.test/child');
        const denied = await child.fetch('https://object.test/child');
        return Response.json({ batch: batch.map(result => result.results[0].value), query, refusal,
          child: await ran.text(), status: denied.status, code: (await denied.json()).code });
      }
    }
    export default {
      fetch(_request, env) {
        return env.OBJECT.getByName('parent').fetch('https://object.test/parent');
      }
    };
  `, { wrangler: { limits: { subrequests: 3 },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['BudgetActor'] }],
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'BudgetActor' }] },
    d1_databases: [{ binding: 'DB', database_name: 'budget-db' }] } });
  const object = json(await workerCall(cluster, node, tenant, 'd1-actor'), 200, 'D1 budget from a Durable Object');
  assert.deepEqual(object.batch, [1, 2]);
  assert.equal(object.query, 3);
  assert.match(object.refusal, /^D1_ERROR: D1_QUERY_BUDGET_EXHAUSTED:/);
  assert.deepEqual([object.child, object.status, object.code], ['child', 429, 'SUBREQUEST_BUDGET_EXHAUSTED']);
}

const budgetLifetime = { tenant: 'budget-lifetime', leaseMs: 5_000 };

async function invocationBudgetLifetime(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant(budgetLifetime.tenant);
  const withD1 = cluster.threadedNodes.size === 0;
  if (withD1) {
    await cluster.api(tenant.path, { method: 'PATCH', body: { defaultLimits: { d1Queries: 1 } } });
    const { storage: { tenantsRevision } } = await cluster.api('/v1/status');
    await eventually('the lifetime fixture has the new D1 limit', async () => {
      const states = await Promise.all(cluster.nodes.map(id => cluster.state(id)));
      assert.ok(states.every(state => BigInt(state.storage.appliedTenantsRevision) >= BigInt(tenantsRevision)));
    });
    await cluster.api(`${tenant.path}/databases/lifetime-db`, { method: 'POST', token: tenant.token });
  }
  await cluster.deploy(tenant, 'budget-lifetime', `
    import { DurableObject } from 'cloudflare:workers';
    const waitMs = ${budgetLifetime.leaseMs + 500};
    const origin = 'http://budget-origin:8080/';
    async function spend(env) {
      const first = await (await fetch(origin)).text();
      const second = env.DB ? await env.DB.prepare('SELECT 41 AS value').first('value') : await (await fetch(origin)).text();
      let extraFetch, extraQuery;
      try { await fetch(origin); } catch (error) { extraFetch = error.message; }
      if (env.DB) {
        try { await env.DB.prepare('SELECT 42').run(); } catch (error) { extraQuery = error.message; }
      }
      return { first, second, extraFetch, extraQuery };
    }
    export class BudgetLifetime extends DurableObject {
      async fetch(request) {
        const action = new URL(request.url).searchParams.get('action');
        if (action === 'schedule') {
          await this.ctx.storage.setAlarm(Date.now() + 50);
          return Response.json('scheduled');
        }
        if (action === 'result') return Response.json((await this.ctx.storage.get('result')) ?? null);
        if (action === 'start') {
          const first = await spend(this.env);
          const resumed = new Promise(resolve => { this.resumeBackground = resolve; });
          this.background = resumed.then(() => spend(this.env)).catch(error => ({ unexpected: error.message }));
          this.ctx.waitUntil(this.background);
          return Response.json(first);
        }
        await scheduler.wait(50);
        this.resumeBackground();
        return Response.json(await this.background);
      }
      async alarm() {
        await scheduler.wait(waitMs);
        const result = await spend(this.env).catch(error => ({ unexpected: error.message }));
        await this.ctx.storage.put('result', result);
      }
    }
    export default { async fetch(request, env) {
      const action = new URL(request.url).searchParams.get('action');
      if (action === 'late-fetch') {
        await scheduler.wait(waitMs);
        const reply = await env.OBJECT.getByName('past-deadline').fetch('https://object.test/?action=result');
        const lateObject = { status: reply.status, value: await reply.json() };
        const first = await (await fetch(origin)).text();
        let extraFetch;
        try { await fetch(origin); } catch (error) { extraFetch = error.message; }
        return Response.json({ first, extraFetch, invocationId: request.cf.invocationId, lateObject });
      }
      const actor = action === 'schedule' || action === 'result' ? 'alarm' : 'background';
      return env.OBJECT.getByName(actor).fetch(request);
    } };
  `, { wrangler: { limits: { subrequests: 2 }, migrations: [{ tag: 'v1', new_sqlite_classes: ['BudgetLifetime'] }],
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'BudgetLifetime' }] },
    ...(withD1 ? { d1_databases: [{ binding: 'DB', database_name: 'lifetime-db' }] } : {}) } });

  const call = async action => json(await workerCall(cluster, node, tenant, `budget-lifetime?action=${action}`, { timeout: 20_000 }),
    200, `invocation budget lifetime ${action}`);
  const verify = (value, checksD1 = withD1) => {
    assert.equal(value.unexpected, undefined);
    assert.equal(value.first, 'origin');
    assert.equal(value.second, checksD1 ? 41 : 'origin');
    assert.match(value.extraFetch, /^SUBREQUEST_BUDGET_EXHAUSTED:/);
    if (checksD1) assert.match(value.extraQuery, /^D1_ERROR: D1_QUERY_BUDGET_EXHAUSTED:/);
  };
  assert.equal(await call('schedule'), 'scheduled');
  const late = await call('late-fetch');
  assert.deepEqual(late.lateObject, { status: 200, value: null });
  assert.equal(late.first, 'origin');
  assert.match(late.extraFetch, /^SUBREQUEST_BUDGET_EXHAUSTED:/);
  assert.ok(late.invocationId);
  await eventually('the completed invocation releases its lease record', async () => {
    assert.ok((await cluster.state(node)).leases.every(lease => lease.invocationId !== late.invocationId));
  });
  await eventually('the alarm uses its budget after the HTTP preparation interval', async () => {
    const result = await call('result');
    assert.ok(result);
    verify(result);
  });
  // The original event spends both subrequests. Its resumed callback uses the newer request's balance.
  verify(await call('start'));
  verify(await call('resume'));
}

async function ordinaryProcess(cluster, node) {
  const { processes } = await cluster.state(node);
  const ordinary = processes.entries.filter(entry => entry.processClass === 'ordinary');
  assert.equal(ordinary.length, 1, `${node} must have exactly one ordinary slot`);
  return ordinary[0];
}

function workerCall(cluster, node, tenant, worker, options) {
  return request(cluster.urls[node], `/${tenant.name}/${worker}`, options);
}

async function cpuIsolation(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant('cpu-test');
  const burner = `export default { fetch(request) {
    if (new URL(request.url).pathname.endsWith('/burn')) {
      for (;;) { /* let the per-request CPU watchdog terminate this invocation */ }
    }
    return new Response('warm');
  } };`;
  await cluster.deploy(tenant, 'burner', burner);
  await cluster.deploy(tenant, 'neighbor', respondWith('neighbor'));
  await cluster.api(`${tenant.path}/workers/burner`, {
    token: tenant.token, method: 'PATCH', body: { limits: { cpuMs: 1500 } },
  });
  await eventually('both Workers warm in the target node', async () => {
    const state = await cluster.state(node);
    const status = await cluster.api('/v1/status');
    assert.equal(String(state.storage.appliedTenantsRevision), String(status.storage.tenantsRevision));
    assert.deepEqual(await workerCall(cluster, node, tenant, 'burner'), { status: 200, text: 'warm' });
    assert.deepEqual(await workerCall(cluster, node, tenant, 'neighbor'), { status: 200, text: 'neighbor' });
  });

  const before = await eventually('the warmed slot is idle', async () => {
    const slot = await ordinaryProcess(cluster, node);
    assert.equal(slot.inFlight, 0);
    return slot;
  });
  let burnFinished = false;
  const burn = workerCall(cluster, node, tenant, 'burner/burn', { timeout: 12000 })
    .then(response => ({ response }), error => ({ error }))
    .finally(() => { burnFinished = true; });
  try {
    await eventually('CPU-bound invocation is in the ordinary process', async () => {
      const current = await ordinaryProcess(cluster, node);
      assert.equal(current.pid, before.pid);
      assert.equal(current.inFlight, 1);
    }, { timeout: 1100, interval: 20 });
    assert.equal(burnFinished, false, 'the burner is still running when the neighbor starts');
    assert.deepEqual(await workerCall(cluster, node, tenant, 'neighbor', { timeout: 1000 }),
      { status: 200, text: 'neighbor' }, 'the neighbor answers within one second');
    assert.equal(burnFinished, false, 'the neighbor answers before the burner gets its limit response');
  } finally {
    await burn;
  }
  const { response, error } = await burn;
  if (error) throw error;
  assert.equal(json(response, 503, 'CPU-bound Worker').code, 'CPU_LIMIT_EXCEEDED');
  const after = await ordinaryProcess(cluster, node);
  assert.equal(after.pid, before.pid, 'both invocations used the same live process generation');
  assert.equal(after.generation, before.generation);
  assert.equal(after.dispatchedCount, before.dispatchedCount + 2, 'both invocations were dispatched to that process');
}

async function processReplacement(cluster) {
  const [node] = cluster.nodes;
  const tenant = await cluster.tenant('restart-test');
  await cluster.deploy(tenant, 'hello', respondWith('alive'));
  await eventually('worker runs before the fault', async () => {
    assert.deepEqual(await workerCall(cluster, node, tenant, 'hello'), { status: 200, text: 'alive' });
  });

  const before = await ordinaryProcess(cluster, node);
  assert.ok(Number.isInteger(before.pid) && before.pid > 1);
  await cluster.compose(['exec', '-T', node, '/bin/sh', '-c', 'kill -KILL "$1"', 'sh', String(before.pid)]);
  await eventually(`${node} replaces ${before.processId} generation ${before.generation}`, async () => {
    const after = await ordinaryProcess(cluster, node);
    assert.equal(after.processId, before.processId);
    assert.notEqual(after.pid, before.pid);
    assert.ok(after.generation > before.generation, `generation is still ${after.generation}`);
    assert.equal(after.state, 'ready');
    assert.equal((await request(cluster.urls[node], '/__coworkerd/ready')).status, 200);
  });
  const { processes } = await cluster.state(node);
  const exit = processes.recentFatalExits.find(entry => entry.pid === before.pid && entry.generation === before.generation);
  assert.equal(exit?.culpritStatus, 'undetermined', 'a process exit without evidence leaves the tenant unmarked');
  assert.equal(exit.culprit, null);
  assert.ok(!processes.suspects.entries.some(entry => entry.tenant === tenant.name));
  assert.deepEqual(await workerCall(cluster, node, tenant, 'hello'), { status: 200, text: 'alive' },
    'the first Worker call after the replacement is ready succeeds');
}

export async function fatalAttribution(cluster) {
  const [node, peer] = cluster.nodes;
  const tenant = await cluster.tenant('fatal-test');
  const neighbor = await cluster.tenant('fatal-neighbor');
  const target = `${tenant.name}/hello`;
  await cluster.deploy(tenant, 'hello', respondWith('hello'));
  await cluster.deploy(neighbor, 'hello', `export default { async fetch(request) {
    if (new URL(request.url).pathname.endsWith('/held')) {
      await new Promise(resolve => setTimeout(resolve, 15000));
    }
    return new Response('neighbor');
  } };`);
  const hello = (at = node) => workerCall(cluster, at, tenant, 'hello');
  const near = (worker = 'hello') => workerCall(cluster, node, neighbor, worker, { timeout: 18000 });
  const pool = async (at = node) => (await cluster.state(at)).processes;
  const sameGeneration = (entry, process) => entry.processId === process.processId && entry.generation === process.generation;
  const assertUncertain = (result, description) => assert.equal(json(result, 503, description).code, 'RESULT_UNCERTAIN');
  const assertQuarantineRoute = async (at, process) => {
    const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', at]);
    const prefix = '[execution] ';
    const call = logs.split('\n').filter(line => line.startsWith(prefix))
      .map(line => JSON.parse(line.slice(prefix.length)))
      .findLast(entry => entry.kind === 'ingress' && entry.target === `${target}@v1`);
    assert.equal(call?.processId, process.processId);
    assert.equal(call.processGeneration, process.generation);
  };
  const recovered = (process) => eventually('a new ordinary process is ready', async () => {
    const current = await ordinaryProcess(cluster, node);
    assert.equal(current.processId, process.processId);
    assert.ok(current.generation > process.generation);
    assert.equal(current.state, 'ready');
    return current;
  });
  const replacedQuarantine = (process) => eventually('a new quarantine process is ready', async () => {
    const current = (await pool()).entries.find(entry => entry.processId === process.processId);
    assert.ok(current?.generation > process.generation);
    assert.equal(current.state, 'ready');
    assert.ok(current.pid > 1);
    return current;
  });
  // What V8 itself said before the process with `pid` exited, from the node's log.
  const diagnosticOf = async (pid) => {
    const prefix = '[v8-fatal-diagnostic] ';
    const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', node]);
    return logs.split('\n').filter(line => line.startsWith(prefix))
      .map(line => JSON.parse(line.slice(prefix.length))).find(entry => entry.pid === pid);
  };
  const syntheticExit = async (process, event, check = result => assertUncertain(result, `synthetic ${event} from ${process.processClass}`)) => {
    const command = join(cluster.fatalControl, `command.${process.pid}`);
    const pending = `${command}.pending`;
    await writeFile(pending, `${event}\n${target}`, { flag: 'wx', mode: 0o644 });
    await rename(pending, command);
    try {
      check(await hello());
    } finally {
      await unlink(command);
    }
  };
  await eventually('both tenants run before the fault', async () => {
    assert.deepEqual(await hello(), { status: 200, text: 'hello' });
    assert.deepEqual(await near(), { status: 200, text: 'neighbor' });
    assert.deepEqual(await hello(peer), { status: 200, text: 'hello' });
  });

  const initial = await ordinaryProcess(cluster, node);
  await syntheticExit(initial, 'shared-oom');
  const before = await recovered(initial);
  const oom = (await pool()).recentMemoryCrashes.find(entry => sameGeneration(entry, initial));
  assert.equal(oom?.culpritStatus, 'undetermined');
  assert.equal(oom.culprit, null);
  assert.ok(!(await pool()).suspects.entries.some(entry => entry.tenant === tenant.name));
  assert.deepEqual(await hello(), { status: 200, text: 'hello' });

  const held = near('hello/held').then(response => ({ response }), error => ({ error }));
  let firstRoute;
  try {
    await eventually('the other tenant is in flight in the ordinary process', async () => {
      const current = await ordinaryProcess(cluster, node);
      assert.equal(current.pid, before.pid);
      assert.equal(current.inFlight, 1);
    }, { timeout: 5000, interval: 30 });
    // V8 reports this one itself, through the fatal handler the isolate ended up with.
    await syntheticExit(before, 'api-check');
    // Send the first call before polling state or waiting for an ordinary replacement.
    assert.deepEqual(await hello(), { status: 200, text: 'hello' });
    firstRoute = await pool();
  } finally {
    await held;
  }
  const { response, error } = await held;
  if (error) throw error;
  assertUncertain(response, 'neighbor affected by the ordinary exit');
  const first = firstRoute.recentFatalExits.find(entry => sameGeneration(entry, before));
  assert.equal(first?.culpritStatus, 'confirmed');
  assert.equal(first.culprit.workerId, target);
  assert.ok(first.inFlight.some(call => call.workerId === `${neighbor.name}/hello`));
  assert.deepEqual(await diagnosticOf(before.pid),
    { pid: before.pid, location: 'v8::ToLocalChecked', line: 0, message: 'Empty MaybeLocal' });
  const quarantine = firstRoute.entries.find(entry => entry.processClass === 'quarantine');
  assert.equal(quarantine?.state, 'ready');
  await assertQuarantineRoute(node, quarantine);
  const replacement = await recovered(before);
  assert.deepEqual(await near(), { status: 200, text: 'neighbor' });

  const adopted = await eventually('another node learns which tenant to quarantine', async () => {
    const view = await pool(peer);
    assert.ok(view.suspects.entries.some(entry => entry.tenant === tenant.name && entry.workerId === target));
    return view;
  });
  assert.deepEqual(await hello(peer), { status: 200, text: 'hello' });
  await assertQuarantineRoute(peer, adopted.entries.find(entry => entry.processClass === 'quarantine'));

  await syntheticExit(quarantine, 'fatal');
  const second = (await pool()).recentFatalExits.find(entry => sameGeneration(entry, quarantine));
  assert.equal(second?.culpritStatus, 'confirmed');
  assert.equal(second.culprit.workerId, target);
  assert.equal(second.processClass, 'quarantine');
  assert.deepEqual(await near(), { status: 200, text: 'neighbor' });
  assert.equal((await ordinaryProcess(cluster, node)).pid, replacement.pid);

  // V8's API check while the Worker loads cold in the replaced quarantine process, after its top level
  // ran (handler validation), where only the load's own scope names the Worker.
  const reloaded = await replacedQuarantine(quarantine);
  let loadResult;
  await syntheticExit(reloaded, 'load-api-check', result => { loadResult = result; });
  const third = (await pool()).recentFatalExits.find(entry => sameGeneration(entry, reloaded));
  assert.equal(third?.culpritStatus, 'confirmed', JSON.stringify(third));
  // The call that started the load is answered as a load failure.
  assert.equal(json(loadResult, 503, 'the call whose load ended the process').code, 'LOAD_FAILED');
  assert.equal(third.culprit.workerId, target);
  assert.equal((await diagnosticOf(reloaded.pid))?.message, 'Empty MaybeLocal');
  assert.deepEqual(await near(), { status: 200, text: 'neighbor' });
}

async function logQuota(cluster) {
  const [node] = cluster.nodes;
  const rate = 2;
  const buffer = 8192;
  const burst = 24;
  const limited = await cluster.tenant('log-limited', { logLinesPerSecond: rate, logBufferBytes: buffer });
  const other = await cluster.tenant('log-other');
  await cluster.deploy(limited, 'logger', `export default { fetch() {
    console.log('long-line:' + 'x'.repeat(8192));
    for (let i = 1; i < ${burst}; ++i) console.log('burst-' + i);
    return new Response('logged');
  } };`);
  await cluster.deploy(limited, 'latest', `export default { fetch() {
    console.log('latest-one:' + 'y'.repeat(3000));
    console.log('latest-two:' + 'z'.repeat(3000));
    return new Response('logged');
  } };`);
  await cluster.deploy(other, 'logger', `export default { fetch() {
    for (let i = 0; i < 4; ++i) console.log('other-' + i);
    return new Response('logged');
  } };`);
  const logs = (tenant, worker) => cluster.api(`${tenant.path}/workers/${worker}/logs`, { token: tenant.token });
  const onNode = (answer) => {
    const report = answer.nodes.find(entry => entry.nodeId === node);
    assert.ok(report, `logs must include ${node}`);
    assert.equal(report.problem, undefined);
    return report;
  };
  const log = { status: 200, text: 'logged' };
  assert.deepEqual(await Promise.all([
    workerCall(cluster, node, limited, 'logger'), workerCall(cluster, node, other, 'logger'),
  ]), [log, log]);

  const first = await eventually('all burst logs were processed and drops reported', async () => {
    const answer = await logs(limited, 'logger');
    const report = onNode(answer);
    assert.equal(report.keptLines + report.droppedLines, burst);
    assert.ok(answer.lines.some(line => line.reason === 'rate' && line.dropped > 0), 'missing rate-drop record');
    return answer;
  });
  const kept = first.lines.filter(line => typeof line.message === 'string');
  const windows = Map.groupBy(kept, line => Math.floor(line.atUnixMs / 1000));
  for (const lines of windows.values()) assert.ok(lines.length <= rate, `kept ${lines.length} lines in one second`);
  const long = kept.find(line => line.message.startsWith('long-line:'));
  assert.ok(long, 'the first line of the burst is visible');
  assert.equal(long.nodeId, node);
  assert.equal(long.truncated, true);
  assert.ok(Buffer.byteLength(long.message) <= 4096 && long.messageBytes > Buffer.byteLength(long.message),
    'the long line is marked and its retained message fits the documented 4 KiB limit');

  await eventually('the other tenant keeps all its lines on the same node', async () => {
    const answer = await logs(other, 'logger');
    assert.deepEqual(answer.lines.map(line => line.message), ['other-0', 'other-1', 'other-2', 'other-3']);
    assert.equal(onNode(answer).droppedLines, 0);
  });

  // The burst can straddle a second boundary. Let its last rate window expire before testing eviction.
  await delay(1000);
  assert.deepEqual(await workerCall(cluster, node, limited, 'latest'), log);
  const latest = await eventually('new log entries reach the tenant buffer', async () => {
    const answer = await logs(limited, 'latest');
    assert.equal(answer.lines.filter(line => line.message?.startsWith('latest-')).length, 2);
    return answer;
  });
  assert.ok(onNode(latest).bufferBytes <= buffer);
  assert.ok(onNode(latest).evictedLines > 0);
  assert.ok((await logs(limited, 'logger')).lines.every(line => !line.message?.startsWith('long-line:')),
    'new entries evict the oldest line across Workers of the same tenant');
}

async function heldPeerWrite(cluster, peers, url, value, readNeighbor) {
  await eventually('both peers finished releasing earlier acknowledgements', async () => {
    const observations = await Promise.all(peers.map(node => cluster.state(node)));
    assert.ok(observations.every(state => state.durableLog.peers.heldAcksForTest === 0));
  });
  const gate = join(cluster.peerAckGate, 'hold');
  await writeFile(gate, '');
  let receivedHeaders = false;
  const write = fetch(url, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify(value), signal: AbortSignal.timeout(10000),
  }).then(async response => {
    receivedHeaders = true;
    return { status: response.status, text: await response.text() };
  }).catch(error => ({ error }));
  let result;
  let early;
  try {
    await eventually(`both peers hold positive append acknowledgements for the ${value} write`, async () => {
      const observations = await Promise.all(peers.map(node => cluster.state(node)));
      assert.ok(observations.every(state => state.durableLog.peers.heldAcksForTest > 0));
    }, { timeout: 3000, interval: 25 });
    await readNeighbor();
    await delay(250);
    early = receivedHeaders;
  } finally {
    await unlink(gate);
    result = await write;
  }
  if (result.error) throw result.error;
  assert.equal(json(result, 200, `${value} write after acknowledgement release`), value);
  return early;
}

async function durablePeerAck(cluster) {
  const tenant = await cluster.tenant('durable-ack-test');
  const worker = 'object';
  await cluster.deploy(tenant, worker, `
    import { DurableObject } from 'cloudflare:workers';
    export class Record extends DurableObject {
      write(value) {
        void this.ctx.storage.put('value', value);
        return value;
      }
      read() { return this.ctx.storage.get('value'); }
      async fetch(request) {
        if (request.method === 'POST') return Response.json(this.write(await request.json()));
        return Response.json(await this.read() ?? null);
      }
    }
    export class Neighbor extends DurableObject {
      fetch() { return new Response('neighbor'); }
    }
    export default {
      async fetch(request, env) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/neighbor')) return env.NEIGHBORS.getByName('neighbor').fetch(request);
        const record = env.RECORDS.getByName('record');
        if (path.endsWith('/rpc')) return Response.json(await record.write(await request.json()));
        return record.fetch(request);
      },
    };
  `, { wrangler: {
    durable_objects: { bindings: [
      { name: 'RECORDS', class_name: 'Record' },
      { name: 'NEIGHBORS', class_name: 'Neighbor' },
    ] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record', 'Neighbor'] }],
  } });
  const [ingress] = cluster.nodes;
  const path = `/${tenant.name}/${worker}`;
  const read = suffix => request(cluster.urls[ingress], path + suffix);
  await eventually('both objects serve reads', async () => {
    assert.equal(json(await read(''), 200, 'warm record read'), null);
    assert.deepEqual(await read('/neighbor'), { status: 200, text: 'neighbor' });
  });
  const owner = await eventually('record has a live owner', async () => {
    const { owners } = await cluster.api(`${tenant.path}/workers/${worker}/namespaces/Record`, { token: tenant.token });
    assert.equal(owners.length, 1);
    assert.ok(!owners[0].released && cluster.nodes.includes(owners[0].nodeId));
    return owners[0].nodeId;
  });
  const peers = cluster.nodes.filter(node => node !== owner);
  const warm = await workerCall(cluster, ingress, tenant, worker, { method: 'POST', body: 'warm' });
  assert.equal(json(warm, 200, 'warm write'), 'warm');
  const before = await cluster.state(owner);
  assert.equal(before.durableLog.peers.want, peers.length);
  const earlyResponses = [];
  for (const [suffix, value] of [['', 'http'], ['/rpc', 'rpc']]) {
    const early = await heldPeerWrite(cluster, peers, cluster.urls[ingress] + path + suffix, value, async () => {
      assert.deepEqual(await read('/neighbor'), { status: 200, text: 'neighbor' });
    });
    if (early) earlyResponses.push(value);
    assert.equal(json(await read(''), 200, `${value} stored value`), value);
  }
  assert.deepEqual(earlyResponses, [], 'responses arrived while both peers held their acknowledgements');
}

async function takeoverWriter(cluster, tenantName) {
  const tenant = await cluster.tenant(tenantName);
  await cluster.deploy(tenant, 'object', `
    import { DurableObject } from 'cloudflare:workers';
    export class Record extends DurableObject {
      async fetch(request) {
        if (request.method === 'POST') {
          const id = await request.json();
          await this.ctx.storage.put(String(id), true);
          return Response.json(id);
        }
        return Response.json(Array.from((await this.ctx.storage.list()).keys(), Number));
      }
    }
    export default { fetch(request, env) { return env.RECORDS.getByName('takeover').fetch(request); } };
  `, { wrangler: {
    durable_objects: { bindings: [{ name: 'RECORDS', class_name: 'Record' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record'] }],
  } });
  return tenant;
}

async function durableTakeover(cluster) {
  const tenant = await takeoverWriter(cluster, 'durable-test');
  const [node] = cluster.nodes;
  const namespace = () => cluster.api(`${tenant.path}/workers/object/namespaces/Record`, { token: tenant.token });
  await eventually('object and its shard are ready', async () => {
    assert.deepEqual(json(await workerCall(cluster, node, tenant, 'object'), 200, 'initial object read'), []);
  });
  const original = await eventually('public namespace state identifies the owner', async () => {
    const { owners } = await namespace();
    assert.equal(owners.length, 1);
    assert.ok(cluster.nodes.includes(owners[0].nodeId) && !owners[0].released);
    return owners[0];
  });
  await eventually('the owner sees both surviving members', async () => {
    const { membership } = await cluster.state(original.nodeId);
    const peers = membership.members.filter(member => member.durableProcesses > 0).map(member => member.nodeId);
    for (const peer of cluster.nodes.filter(id => id !== original.nodeId)) assert.ok(peers.includes(peer), `missing ${peer}`);
  });

  const survivor = cluster.nodes.find(id => id !== original.nodeId);
  const concurrency = 16;
  const warm = Promise.withResolvers();
  const acknowledged = new Set();
  let sent = 0;
  let pending = 0;
  let interrupted = 0;
  let writing = true;
  let killing = false;
  const writers = Promise.allSettled(Array.from({ length: concurrency }, async () => {
    try {
      while (writing) {
        const id = ++sent;
        ++pending;
        let response;
        try {
          response = await workerCall(cluster, survivor, tenant, 'object', { method: 'POST', body: id, timeout: 10000 });
        } catch (error) {
          if (!killing) throw error;
        } finally {
          --pending;
        }
        if (killing && (!response || response.status >= 500)) {
          ++interrupted;
          writing = false;
          return;
        }
        assert.equal(json(response, 200, `object write ${id}`), id);
        acknowledged.add(id);
        if (acknowledged.size === concurrency) warm.resolve();
      }
    } catch (error) {
      writing = false;
      warm.reject(error);
      throw error;
    }
  }));
  let inFlightAtKill;
  let results;
  let gate;
  try {
    await warm.promise;
    if (cluster.threadedNodes.size) {
      assert.equal((await durableGeneration(cluster, original.nodeId)).lifecycle?.durableServer, 'threaded');
      gate = join(cluster.peerAckGate, 'hold');
      await writeFile(gate, '');
      await eventually('the surviving peers withhold positive confirmations during active writes', async () => {
        const peers = cluster.nodes.filter(node => node !== original.nodeId);
        const states = await Promise.all(peers.map(node => cluster.state(node)));
        assert.ok(states.every(state => state.durableLog.peers.heldAcksForTest > 0));
      }, { timeout: 3000, interval: 25 });
    }
    inFlightAtKill = pending;
    assert.ok(inFlightAtKill > 0, 'writes are in flight when the kill is requested');
    killing = true;
    await cluster.compose(['kill', '-s', 'SIGKILL', original.nodeId]);
  } finally {
    writing = false;
    if (gate) await unlink(gate);
    results = await writers;
  }
  for (const result of results) {
    if (result.status === 'rejected') throw result.reason;
  }
  console.log(`Durable writes through ${survivor} to ${original.nodeId}: ${acknowledged.size}/${sent} acknowledged, ` +
    `${inFlightAtKill} pending at kill request, ${interrupted} interrupted`);
  assert.ok(interrupted > 0, 'the owner loss interrupted at least one write');

  const recovered = await eventually('a surviving node serves a read after owner loss', async () => {
    const read = await workerCall(cluster, survivor, tenant, 'object', { timeout: 35000 });
    return json(read, 200, 'takeover read');
  }, { timeout: 100000, interval: 1000 });
  const stored = new Set(recovered);
  const missing = [...acknowledged].filter(id => !stored.has(id));
  assert.equal(missing.length, 0, `takeover lost acknowledged writes: ${missing.join(', ')}`);
  assert.ok(recovered.every(id => Number.isSafeInteger(id) && id > 0 && id <= sent), 'takeover returned an unsent write');
  await eventually('the shard has a new live owner and a later epoch', async () => {
    const { owners } = await namespace();
    const owner = owners.find(entry => entry.shard === original.shard);
    assert.ok(owner && !owner.released && owner.nodeId !== original.nodeId && cluster.nodes.includes(owner.nodeId));
    assert.ok(BigInt(owner.epoch) > BigInt(original.epoch));
  });
}

async function durableSlowHolder(cluster) {
  const tenant = await takeoverWriter(cluster, 'slow-holder-test');
  const [first] = cluster.nodes;
  const namespace = () => cluster.api(`${tenant.path}/workers/object/namespaces/Record`, { token: tenant.token });
  await eventually('the original holder confirms the value', async () => {
    const response = await workerCall(cluster, first, tenant, 'object', { method: 'POST', body: 19 });
    assert.equal(json(response, 200, 'confirmed value'), 19);
  });
  const { namespaceId, owners: [original] } = await namespace();
  assert.ok(original && !original.released);
  const originalState = await cluster.state(original.nodeId);
  const ingress = cluster.nodes.find(node => node !== first && node !== original.nodeId);
  const id = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update('takeover').digest('hex');
  const gate = join(cluster.recoveryRelayGate, original.nodeId);
  await writeFile(gate, '');
  try {
    // Inject the normal handoff marker while keeping the previous node and its
    // heartbeat alive. The relay withholds Cap'n Proto replies from that node.
    await cluster.releaseShardForRecoveryTest(namespaceId, { ...original, nodeEpoch: originalState.durableLog.nodeEpoch });
    let answered = false;
    const read = workerCall(cluster, ingress, tenant, 'object', { timeout: 34000 })
      .then(value => { answered = value.status === 200; return { value }; }, error => ({ error }));
    const takeover = await eventually('a successor has acquired the released shard', async () => {
      const { owners } = await namespace();
      const newOwner = owners.find(owner => owner.shard === original.shard);
      assert.ok(newOwner && !newOwner.released && newOwner.nodeId !== original.nodeId &&
        BigInt(newOwner.epoch) > BigInt(original.epoch));
      return newOwner.nodeId;
    });
    await eventually('the successor is waiting past the deadline for the live holder', async () => {
      const state = await cluster.state(takeover);
      const entry = state.durableLog.recovery.scheduler.entries.find(row => row.object.startsWith(`${namespaceId}/${id}/`));
      assert.ok(state.durableLog.recovery.cutsHeldOwnerLive > 0 && entry);
      assert.equal(entry.owner, original.nodeId);
      assert.equal(entry.ownerEpoch, originalState.durableLog.nodeEpoch);
      assert.ok(entry.waitingMs >= 8500);
      assert.equal(state.durableLog.objects.activeRebuilds, 0);
      assert.ok(state.peerConnections[original.nodeId].tailFailures > 0, 'a real page timeout marked its connection');
    }, { timeout: 22000, interval: 75 });
    assert.equal(answered, false, 'the successor cannot serve the object while the holder is slow');
    const stillAlive = await cluster.state(original.nodeId);
    assert.equal(stillAlive.membership.selfExpired, false);
    assert.equal(stillAlive.membership.leaseFences, originalState.membership.leaseFences);
    await unlink(gate);
    const { value, error } = await read;
    if (error) throw error;
    assert.deepEqual(json(value, 200, 'the successor read after the holder replied'), [19]);
    const settled = await cluster.state(takeover);
    assert.equal(settled.durableLog.recovery.ownerSilent, 0);
    assert.equal(settled.durableLog.recovery.bounded, 0);
  } finally {
    await unlink(gate).catch(error => { if (error.code !== 'ENOENT') throw error; });
  }
}

async function durableThreadedFence(cluster) {
  const tenant = await takeoverWriter(cluster, 'fenced-durable-test');
  const [first] = cluster.nodes;
  const namespace = () => cluster.api(`${tenant.path}/workers/object/namespaces/Record`, { token: tenant.token });
  await eventually('the object serves a confirmed write', async () => {
    assert.equal(json(await workerCall(cluster, first, tenant, 'object', { method: 'POST', body: 1 }),
      200, 'baseline write'), 1);
  });
  const { namespaceId, owners: [old] } = await namespace();
  assert.ok(old && cluster.nodes.includes(old.nodeId));
  const objectId = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update('takeover').digest('hex');
  const peers = cluster.nodes.filter(node => node !== old.nodeId);
  const [ingress] = peers;
  await eventually('both peers are reachable before the fence', async () => {
    const { durableLog } = await cluster.state(old.nodeId);
    assert.equal(durableLog.peers.want, 2);
    assert.ok(peers.every(peer => durableLog.peers.streams.some(stream => stream.nodeId === peer && stream.open)));
  });
  const child = await durableGeneration(cluster, old.nodeId);
  assert.equal(child.lifecycle?.durableServer, 'threaded');
  const { durableLog: before } = await cluster.state(old.nodeId);
  const sequence = (BigInt(before.log.nextSeq) + 1n).toString();
  const gate = join(cluster.peerAckGate, 'hold');
  await writeFile(gate, '');
  const sentAt = performance.now();
  const staleWrite = workerCall(cluster, ingress, tenant, 'object', { method: 'POST', body: 2, timeout: 30000 })
    .catch(error => ({ error }));
  let paused = false;
  let recovery;
  try {
    await eventually('the old generation has a write waiting on two peers', async () => {
      const states = await Promise.all(peers.map(peer => cluster.state(peer)));
      assert.ok(states.every(state => state.durableLog.peers.heldAcksForTest > 0));
      const { durableLog } = await cluster.state(old.nodeId);
      assert.equal(durableLog.log.nextSeq, sequence);
      assert.equal(durableLog.confirm.unconfirmed, 1);
    }, { timeout: 3000, interval: 25 });
    await cluster.compose(['pause', old.nodeId]);
    paused = true;
    recovery = eventually('the survivor can read after acquiring the shard', async () =>
      json(await workerCall(cluster, ingress, tenant, 'object', { timeout: 10000 }), 200, 'fenced takeover read'),
    { timeout: 45000, interval: 100 }).catch(error => ({ error }));
    await eventually('a different node acquires a higher epoch while the old child stays frozen', async () => {
      const { owners } = await namespace();
      const owner = owners.find(entry => entry.shard === old.shard);
      assert.ok(owner && peers.includes(owner.nodeId) && BigInt(owner.epoch) > BigInt(old.epoch));
    }, { timeout: 20000, interval: 100 });
    assert.ok(performance.now() - sentAt < cluster.confirmTimeoutMs - 1000,
      'the old write has time left on its confirmation deadline when the new epoch is known');
  } finally {
    await unlink(gate);
    if (paused) await cluster.compose(['unpause', old.nodeId]);
  }
  const result = await staleWrite;
  assert.ok(result.error || result.status >= 400, 'a write from the fenced generation never returned success');
  await eventually('the old owner excludes the pending write because it lost ownership', async () => {
    const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', old.nodeId]);
    const prefix = '[do-log] ';
    const events = logs.split('\n').filter(line => line.startsWith(prefix))
      .map(line => JSON.parse(line.slice(prefix.length)));
    const fence = events.find(row => row.event === 'owner-fenced' && row.namespaceId === namespaceId &&
      row.shard === old.shard && BigInt(row.throughShardEpoch) >= BigInt(old.epoch));
    assert.ok(fence, `no fence for shard ${old.shard} epoch ${old.epoch} on ${old.nodeId}`);
    const excluded = events.find(row => row.event === 'isolated' && row.object.startsWith(`${namespaceId}/${objectId}/`) &&
      row.records === 1 && row.from === sequence);
    assert.ok(excluded, `no isolation for held sequence ${sequence} on ${old.nodeId}`);
    assert.ok(excluded.reason === fence.reason || excluded.reason === 'the node\'s membership lease ended before confirmation',
      `unexpected reason for isolating sequence ${sequence} on ${old.nodeId}: ${excluded.reason}`);
  });
  const saved = await recovery;
  if (saved.error) throw saved.error;
  assert.ok(saved.includes(1), 'the later owner keeps the earlier confirmed write');
}

async function threadedAlarmTakeover(cluster) {
  const tenant = await cluster.tenant('threaded-alarm-fence');
  const [entry] = cluster.nodes;
  const namespace = () => cluster.api(`${tenant.path}/workers/alarms/namespaces/Alarm`,
    { token: tenant.token });
  const deploy = (version, brokenConstructor = '') => cluster.deploy(tenant, 'alarms', `
    import { DurableObject } from 'cloudflare:workers';
    export class Alarm extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        if (ctx.id.toString() === ${JSON.stringify(brokenConstructor)}) {
          throw new Error('alarm actor constructor failed');
        }
      }
      async fetch(request) {
        if (request.method === 'POST') {
          const at = await request.json();
          if (new URL(request.url).searchParams.has('stop')) await this.ctx.storage.put('stop', true);
          await this.ctx.storage.setAlarm(at);
          return Response.json(at);
        }
        if (request.method === 'DELETE') {
          await this.ctx.storage.deleteAlarm();
          return Response.json(await this.ctx.storage.getAlarm());
        }
        return Response.json({
          at: await this.ctx.storage.getAlarm(),
          fired: (await this.ctx.storage.get('fired')) ?? 0,
        });
      }
      async alarm() {
        if (await this.ctx.storage.get('stop')) this.ctx.abort('stop this alarm', { retryAlarm: false });
        await this.ctx.storage.put('fired', ((await this.ctx.storage.get('fired')) ?? 0) + 1);
      }
    }
    export default { fetch(request, env) {
      const name = new URL(request.url).searchParams.get('object');
      return env.ALARMS.getByName(name).fetch(request);
    } };
  `, { version, wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['Alarm'] }],
    durable_objects: { bindings: [{ name: 'ALARMS', class_name: 'Alarm' }] } } });
  await deploy('v1');
  const { namespaceId, shards } = await namespace();
  const idOf = name => createHash('sha256').update(Buffer.from(namespaceId, 'hex'))
    .update('name').update(name).digest('hex');
  const shardOf = name => {
    const id = idOf(name);
    return Number(BigInt(`0x${id.slice(0, 16)}`) % BigInt(shards));
  };
  const confirmed = 'confirmed';
  const shard = shardOf(confirmed);
  const sameShard = label => {
    for (let i = 0; i < shards * 16; ++i) {
      const name = `${label}-${i}`;
      if (shardOf(name) === shard) return name;
    }
    assert.fail(`no ${label} object maps to shard ${shard}`);
  };
  const unconfirmed = sameShard('unconfirmed');
  const rescheduled = sameShard('rescheduled');
  const ownerOfShard = async () => (await namespace()).owners.find(owner => owner.shard === shard && !owner.released);
  const call = (node, object, options) => workerCall(cluster, node, tenant, `alarms?object=${object}`, options);
  const alarmEvents = async (object, ...nodes) => {
    const logs = await cluster.compose(['logs', '--no-color', ...nodes]);
    const prefix = '[do-log] ';
    return logs.split('\n').filter(line => line.includes(prefix)).map(line => JSON.parse(line.split(prefix)[1]))
      .filter(row => row.namespaceId === namespaceId && row.objectId === idOf(object).slice(0, 12) &&
        row.event.startsWith('alarm-'));
  };

  // Both behaviors are exercised while the three nodes can still confirm writes normally.
  const constructorFailure = 'constructor-failure';
  const noRetry = 'no-retry';
  const reviewAt = Date.now() + 12000;
  await eventually('schedule the two reviewed alarm outcomes', async () => {
    const set = { method: 'POST', body: reviewAt };
    assert.equal(json(await call(entry, constructorFailure, set), 200, 'constructor schedule'), reviewAt);
    assert.equal(json(await call(entry, noRetry + '&stop', set), 200, 'nonretrying schedule'), reviewAt);
  });
  await deploy('v2', idOf(constructorFailure));
  await eventually('abort without retry clears the schedule and constructor failures are counted', async () => {
    const stopped = await alarmEvents(noRetry);
    assert.equal(stopped.filter(row => row.event === 'alarm-no-retry').length, 1);
    assert.deepEqual(json(await call(entry, noRetry), 200, 'nonretrying alarm after clear'), { at: null, fired: 0 });
    const failed = (await alarmEvents(constructorFailure)).filter(row => row.event === 'alarm-failed');
    assert.deepEqual(failed.slice(0, 2).map(row => row.retries), [1, 2]);
    assert.ok(failed[0].afterMs >= 1750 && failed[0].afterMs < 2250);
    assert.ok(failed[1].afterMs >= 3500 && failed[1].afterMs < 4500);
  }, { timeout: 45000, interval: 500 });

  const at = Date.now() + 65000;
  await eventually('both original schedules are confirmed under the same shard owner', async () => {
    for (const object of [confirmed, unconfirmed, rescheduled]) {
      assert.equal(json(await call(entry, object, { method: 'POST', body: at }), 200, object), at);
    }
  });
  const original = await ownerOfShard();
  assert.ok(original && cluster.nodes.includes(original.nodeId));
  const peers = cluster.nodes.filter(node => node !== original.nodeId);
  const [survivor] = peers;
  const clear = { method: 'DELETE', timeout: 30000 };
  assert.equal(json(await call(entry, confirmed, clear), 200, 'confirmed clear'), null);
  const gate = join(cluster.peerAckGate, 'hold');
  await writeFile(gate, '');
  const sentAt = performance.now();
  const pending = call(survivor, unconfirmed, clear).catch(error => ({ error }));
  let paused = false;
  let recovery;
  try {
    await eventually('the old owner has started a clear that cannot be confirmed', async () => {
      const states = await Promise.all(peers.map(node => cluster.state(node)));
      assert.ok(states.every(state => state.durableLog.peers.heldAcksForTest > 0));
      assert.equal((await cluster.state(original.nodeId)).durableLog.confirm.unconfirmed, 1);
    }, { timeout: 3000, interval: 25 });
    await cluster.compose(['pause', original.nodeId]);
    paused = true;
    recovery = eventually('the new owner reopens the confirmed clear', async () =>
      json(await call(survivor, confirmed, { timeout: 10000 }), 200, 'fenced alarm takeover'),
    { timeout: 45000, interval: 100 }).catch(error => ({ error }));
    await eventually('a survivor acquires the alarm shard at a higher epoch while the old owner is frozen', async () => {
      const owner = await ownerOfShard();
      assert.ok(owner && peers.includes(owner.nodeId) && BigInt(owner.epoch) > BigInt(original.epoch));
    }, { timeout: 20000, interval: 100 });
    assert.ok(performance.now() - sentAt < cluster.confirmTimeoutMs - 1000,
      'the old clear still has time on its confirmation deadline when the new epoch is known');
  } finally {
    await unlink(gate);
    if (paused) await cluster.compose(['unpause', original.nodeId]);
  }
  const result = await pending;
  assert.ok(result.error || result.status >= 400, 'the fenced clear never returns success');
  const saved = await recovery;
  if (saved.error) throw saved.error;
  assert.deepEqual(saved, { at: null, fired: 0 }, 'the previously confirmed clear survives recovery');
  const newTime = at + 120000;
  await eventually('the successor replaces the inherited schedule', async () => {
    assert.equal(json(await call(survivor, rescheduled, { method: 'POST', body: newTime, timeout: 10000 }),
      200, 'replace the inherited schedule'), newTime);
  });
  assert.ok(Date.now() < at, 'the successor replaces the alarm before its former due time');
  await eventually('the surviving owner delivers the alarm whose clear was unconfirmed', async () => {
    assert.deepEqual(json(await call(survivor, unconfirmed), 200, 'unconfirmed clear recovery'),
      { at: null, fired: 1 });
    assert.deepEqual(json(await call(survivor, confirmed), 200, 'confirmed clear recovery'),
      { at: null, fired: 0 });
    assert.deepEqual(json(await call(survivor, rescheduled), 200, 'superseded alarm hint'),
      { at: newTime, fired: 0 }, 'the previous due time cannot fire the replacement schedule');
  }, { timeout: 85000, interval: 250 });
  const previouslyConfirmed = (await alarmEvents(confirmed, ...peers)).length;
  assert.ok(Date.now() >= at, 'observe across the previous scheduled time');
  await delay(2500);
  assert.equal((await alarmEvents(confirmed, ...peers)).length, previouslyConfirmed,
    'the successor stops dispatching for the confirmed clear');
  assert.deepEqual(json(await call(survivor, rescheduled), 200, 'replacement after previous due time'),
    { at: newTime, fired: 0 });
}

async function durableGeneration(cluster, node) {
  const { processes } = await cluster.state(node);
  const durable = processes.entries.filter(entry => entry.processClass === 'durable');
  assert.equal(durable.length, 1, `${node} must have one durable slot`);
  assert.equal(durable[0].state, 'ready', `${node}'s durable slot is ready`);
  return durable[0];
}

async function rotationRequest(cluster, node, slot) {
  const { requests: [rotation] } = json(await request(cluster.urls[node], `/__coworkerd/rotate?process=${slot}`, {
    method: 'POST', token: cluster.management,
  }), 200, 'durable rotation request');
  return rotation.requestId;
}

async function rotationStatus(cluster, node, requestId) {
  return json(await request(cluster.urls[node], `/__coworkerd/rotation?requests=${requestId}`, {
    token: cluster.management,
  }), 200, 'durable rotation status');
}

async function waitForDurableRotation(cluster, node, before, requestId, drainOutcome) {
  await eventually(`durable generation ${before.generation} rotates after ${drainOutcome}`, async () => {
    const { requests: [status], records: [record] } =
      await rotationStatus(cluster, node, requestId);
    assert.equal(status.status, 'completed');
    assert.equal(record.oldGeneration, before.generation);
    assert.equal(record.drainOutcome, drainOutcome);
    const after = await durableGeneration(cluster, node);
    assert.ok(after.generation > before.generation && after.pid !== before.pid);
  }, { timeout: 45000 });
}

async function mixedDurableStorage(cluster) {
  const [threaded, ingress, other] = cluster.nodes;
  const peers = [ingress, other];
  const tenant = await cluster.tenant('mixed-durable-test');
  const worker = `
    import { DurableObject } from 'cloudflare:workers';
    export class Record extends DurableObject {
      write(value) {
        void this.ctx.storage.put('value', value);
        return value;
      }
      async fetch(request) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/checkpoint')) {
          const payload = await request.json();
          await this.ctx.storage.put('blob', payload);
          return Response.json(payload.length);
        }
        if (path.endsWith('/alarm')) {
          const at = await request.json();
          await this.ctx.storage.put('fired', false);
          await this.ctx.storage.setAlarm(at);
          return Response.json(at);
        }
        if (path.endsWith('/clone-facet')) {
          try {
            this.ctx.facets.clone('source', 'destination');
            return Response.json({ cloned: true });
          } catch (error) {
            return Response.json({ error: String(error) });
          }
        }
        if (request.method === 'POST') return Response.json(this.write(await request.json()));
        const value = await this.ctx.storage.get('value');
        const fired = await this.ctx.storage.get('fired');
        return Response.json({ id: this.ctx.id.toString(), value: value ?? null, fired: fired ?? false });
      }
      async alarm() { await this.ctx.storage.put('fired', true); }
    }
    export default {
      async fetch(request, env) {
        const url = new URL(request.url);
        const record = env.RECORDS.getByName(url.searchParams.get('object'));
        if (url.pathname.endsWith('/rpc')) return Response.json(await record.write(await request.json()));
        return record.fetch(request);
      },
    };
  `;
  const wrangler = {
    durable_objects: { bindings: [{ name: 'RECORDS', class_name: 'Record' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Record'] }],
  };
  for (const name of ['writer', 'reader']) await cluster.deploy(tenant, name, worker, { wrangler });
  const { workerId, versions: [{ versionId, definitionHash, contentHash }] } =
    await cluster.api(`${tenant.path}/workers/writer`, { token: tenant.token });
  const trial = json(await request(cluster.urls[threaded], '/__coworkerd/trial-load', {
    method: 'POST', token: cluster.management, timeout: 10000,
    body: { workerId, versionId, definitionHash, contentHash, routed: true },
  }), 200, 'trial load on a threaded node');
  assert.equal(trial.ok, true, trial.message);
  assert.equal(trial.dispatched, true);
  assert.match(trial.processId, /^q\d+$/);
  const findObject = async name => {
    const namespace = () => cluster.api(`${tenant.path}/workers/${name}/namespaces/Record`, { token: tenant.token });
    const { namespaceId, shards } = await namespace();
    for (let i = 0; i < 32; ++i) {
      const object = `object-${i}`;
      const id = createHash('sha256').update(Buffer.from(namespaceId, 'hex')).update('name').update(object).digest('hex');
      const shard = Number(BigInt(`0x${id.slice(0, 16)}`) % BigInt(shards));
      const path = action => `/${tenant.name}/${name}${action}?object=${object}`;
      const read = async (node = ingress) =>
        json(await request(cluster.urls[node], path(''), { timeout: 35000 }), 200, name + ' read');
      const owner = async () => (await namespace()).owners.find(entry => entry.shard === shard && !entry.released);
      await eventually(`${name} candidate serves on its owner`, async () => assert.equal((await read()).id, id));
      if ((await owner())?.nodeId === threaded) return { id, path, read, owner };
    }
    assert.fail(`no ${name} candidate acquired on the threaded node`);
  };
  const target = await findObject('writer');
  const neighbor = await findObject('reader');
  const write = async (suffix, value, node = ingress) => json(await request(cluster.urls[node], target.path(suffix), {
    method: 'POST', body: value,
  }), 200, 'mixed durable write');
  assert.equal(await write('', 'warm'), 'warm');
  assert.match((await write('/clone-facet', null)).error, /NOT_SUPPORTED/);
  const first = await eventually('separate definitions occupy two homes in the same threaded generation', async () => {
    const generation = await durableGeneration(cluster, threaded);
    assert.equal(generation.lifecycle?.durableServer, 'threaded');
    assert.ok(generation.lifecycle.durableAdmission.homes >= 2);
    assert.ok(generation.lifecycle.durableAdmission.residentRoots >= 2);
    return generation;
  });
  const early = [];
  for (const [suffix, value] of [['', 'threaded-http'], ['/rpc', 'threaded-rpc']]) {
    if (await heldPeerWrite(cluster, peers, cluster.urls[ingress] + target.path(suffix), value, async () => {
      assert.equal((await neighbor.read()).id, neighbor.id);
      assert.equal((await durableGeneration(cluster, threaded)).pid, first.pid);
    })) early.push(value);
  }
  assert.deepEqual(early, [], 'threaded responses escaped while both upstream peers held acknowledgements');
  assert.equal((await target.read()).value, 'threaded-rpc');
  assert.equal(await write('/checkpoint', 'x'.repeat(65536)), 65536);
  const io = await eventually('the threaded SQLite open and checkpoint are measured', async () => {
    const { durableLog } = await cluster.state(threaded);
    const io = durableLog.threadedHomeIo;
    assert.ok(io.openSamples > 0 && io.maxOpenNs > 0);
    assert.ok(io.checkpointSamples > 0 && io.maxCheckpointNs > 0 && io.maxCheckpointWalBytes >= 65536);
    return io;
  });
  console.log(`Threaded home I/O for the isolated workload ${JSON.stringify(io)}`);
  let rotation;
  const confirmed = 'committed-during-drain';
  assert.equal(await heldPeerWrite(cluster, peers, cluster.urls[ingress] + target.path(''), confirmed, async () => {
    rotation = await rotationRequest(cluster, threaded, first.processId);
    await eventually('rotation waits in its drain phase while the write is unconfirmed', async () => {
      const { requests: [status], records: [record] } = await rotationStatus(cluster, threaded, rotation);
      assert.equal(status.status, 'active');
      assert.equal(record.phase, 'draining');
    }, { timeout: 1500, interval: 25 });
  }), false);
  await waitForDurableRotation(cluster, threaded, first, rotation, 'drained');
  assert.equal((await target.read()).value, confirmed, 'clean child rotation reopens the value committed during drain');

  const beforeLoss = await durableGeneration(cluster, threaded);
  const loss = join(cluster.peerAckGate, `loss-${threaded}`);
  const channelId = `${beforeLoss.processId}-${beforeLoss.generation}`;
  await writeFile(loss, channelId);
  try {
    await eventually('the daemon cuts the WAL socket of the live child', async () => {
      const logs = await cluster.compose(['logs', '--no-color', '--tail', '120', threaded]);
      assert.ok(logs.split('\n').some(line => line.includes('"event":"channel-cut-for-test"') &&
        line.endsWith(`"processId":"${channelId}","pid":${beforeLoss.pid}}`)));
    });
  } finally {
    await unlink(loss);
  }
  await eventually('channel loss recovery reopens the confirmed value after replacing the child', async () => {
    assert.equal((await target.read()).value, confirmed);
    const afterLoss = await durableGeneration(cluster, threaded);
    assert.ok(afterLoss.generation > beforeLoss.generation && afterLoss.pid !== beforeLoss.pid);
  }, { timeout: 45000 });

  const stalled = await durableGeneration(cluster, threaded);
  await cluster.compose(['exec', '-T', threaded, '/bin/sh', '-c', 'kill -STOP "$1"', 'sh', String(stalled.pid)]);
  const stalledRotation = await rotationRequest(cluster, threaded, stalled.processId);
  await waitForDurableRotation(cluster, threaded, stalled, stalledRotation, 'timeout');
  assert.equal((await target.read()).value, confirmed, 'unresponsive generation exits before the image reopens');

  const localAlarm = Date.now() + 500;
  assert.equal(await write('/alarm', localAlarm), localAlarm);
  await eventually('the threaded owner delivers a due alarm', async () => {
    assert.equal((await target.read()).fired, true);
    assert.equal((await target.owner()).nodeId, threaded);
  });

  const at = Date.now() + 25000;
  assert.equal(await write('/alarm', at), at);
  assert.equal((await target.read()).fired, false, 'the replacement alarm is still pending');
  const startingOwner = await target.owner();
  await cluster.compose(['stop', '--timeout', '45', threaded], { timeout: 70000 });
  const drained = await cluster.compose(['ps', '-a', '-q', threaded]);
  assert.equal(await cluster.command('docker', ['inspect', '--format', '{{.State.ExitCode}}', drained]), '0',
    'the threaded node completed its drain before Compose stopped it');
  const upstreamOwner = await eventually('an upstream owner recovers the threaded write and fires the due alarm', async () => {
    const saved = await target.read();
    assert.equal(saved.value, confirmed);
    assert.equal(saved.fired, true);
    const owner = await target.owner();
    assert.ok(peers.includes(owner.nodeId) && BigInt(owner.epoch) > BigInt(startingOwner.epoch));
    const logs = await cluster.compose(['logs', '--no-color', owner.nodeId]);
    assert.ok(logs.split('\n').some(line => line.includes('[do-log]') &&
      line.includes('"event":"alarm-fired"') && line.includes(`"objectId":"${target.id.slice(0, 12)}"`) &&
      line.includes(`"atUnixMs":${at},`)), 'the new upstream owner dispatched this exact alarm');
    return owner;
  }, { timeout: 120000, interval: 1000 });

  await cluster.compose(['start', threaded]);
  await cluster.refreshUrl(threaded);
  await eventually('the restarted threaded node rejoins as a live peer', async () => {
    await durableGeneration(cluster, threaded);
    const { membership } = await cluster.state(upstreamOwner.nodeId);
    assert.ok(membership.members.some(member => member.nodeId === threaded && member.durableProcesses > 0));
  }, { timeout: 45000 });
  const secondPeer = peers.find(node => node !== upstreamOwner.nodeId);
  const upstreamEarly = await heldPeerWrite(cluster, [threaded, secondPeer],
    cluster.urls[upstreamOwner.nodeId] + target.path(''), 'upstream-write', async () => {});
  assert.equal(upstreamEarly, false, 'upstream waits for the restarted threaded peer');
  await cluster.compose(['stop', '--timeout', '45', secondPeer], { timeout: 70000 });
  await cluster.compose(['kill', '-s', 'SIGKILL', upstreamOwner.nodeId]);
  await eventually('the restarted threaded owner replays confirmed upstream history', async () => {
    const saved = await target.read(threaded);
    assert.equal(saved.value, 'upstream-write');
    assert.equal(saved.fired, true);
    const owner = await target.owner();
    assert.equal(owner.nodeId, threaded);
    assert.ok(BigInt(owner.epoch) > BigInt(upstreamOwner.epoch));
  }, { timeout: 140000, interval: 1000 });
}

async function threadedClassMigration(cluster) {
  const { WebSocket: TestSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const [ingress] = cluster.nodes;
  const tenant = await cluster.tenant('threaded-migration');
  const name = 'records';
  const url = `/${tenant.name}/${name}`;
  const call = (path, options) => request(cluster.urls[ingress], url + path, options);
  const namespace = className => cluster.api(`${tenant.path}/workers/${name}/namespaces/${className}`,
    { token: tenant.token });
  const initial = { tag: 'v1', new_sqlite_classes: ['Record'] };
  const renamed = { tag: 'v2', renamed_classes: [{ from: 'Record', to: 'RenamedRecord' }] };
  const deleted = { tag: 'v3', deleted_classes: ['RenamedRecord'] };
  const source = (className, version) => `
    import { DurableObject, RpcTarget } from 'cloudflare:workers';
    class Retained extends RpcTarget {
      ping() { return '${version}'; }
    }
    export class MigrationFacet extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        this.constructedWith = ctx.getWebSockets().length;
      }
      fetch(request) {
        if (!request.headers.has('upgrade')) {
          return Response.json({ constructedWith: this.constructedWith, version: this.ctx.props.version });
        }
        const [client, socket] = Object.values(new WebSocketPair());
        this.ctx.acceptWebSocket(socket);
        return new Response(null, { status: 101, webSocket: client });
      }
      webSocketMessage(socket, message) { socket.send(this.ctx.props.version + ':' + message); }
    }
    export class ${className} extends DurableObject {
      retain() { return new Retained(); }
      async fetch(request) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/facet-socket') || path.endsWith('/facet-status')) {
          return this.ctx.facets.get('migration', () => ({
            class: this.ctx.exports.MigrationFacet({ props: { version: '${version}' } }),
          })).fetch(request);
        }
        if (path.endsWith('/alarm')) {
          const at = await request.json();
          await this.ctx.storage.setAlarm(at);
          return Response.json(at);
        }
        if (path.endsWith('/socket') || path.endsWith('/hibernatable')) {
          const [client, socket] = Object.values(new WebSocketPair());
          if (path.endsWith('/hibernatable')) {
            this.ctx.acceptWebSocket(socket, ['old']);
          } else {
            socket.accept();
            socket.addEventListener('message', event => socket.send('${version}:' + event.data));
          }
          return new Response(null, { status: 101, webSocket: client });
        }
        if (path.endsWith('/waiting')) return Response.json(await this.ctx.storage.get('waiting') ?? false);
        if (path.endsWith('/wait-starts')) return Response.json(await this.ctx.storage.get('wait-starts') ?? 0);
        if (path.endsWith('/wait-for-headers')) {
          await this.ctx.storage.put('wait-starts', (await this.ctx.storage.get('wait-starts') ?? 0) + 1);
          await this.ctx.storage.put('waiting', true);
          await scheduler.wait(90000);
          return new Response('old handler completed');
        }
        if (path.endsWith('/held')) {
          const encoder = new TextEncoder();
          return new Response(new ReadableStream({
            async start(controller) {
              controller.enqueue(encoder.encode('entered:'));
              for (let i = 0; i < 45; i++) {
                await scheduler.wait(2000);
                controller.enqueue(encoder.encode('${version}'));
              }
              controller.close();
            },
          }));
        }
        if (request.method === 'POST') await this.ctx.storage.put('value', await request.json());
        return Response.json({
          id: this.ctx.id.toString(),
          value: await this.ctx.storage.get('value'),
          alarmVersion: (await this.ctx.storage.get('alarm-version')) ?? null,
          version: '${version}',
        });
      }
      webSocketMessage(socket, message) { socket.send('${version}:' + message); }
      async alarm() { await this.ctx.storage.put('alarm-version', '${version}'); }
    }
    export default { async fetch(request, env) {
      const path = new URL(request.url).pathname;
      const stub = env.OBJECT.getByName(path.endsWith('/facet-socket') || path.endsWith('/facet-status') ?
        'parked' : 'stable');
      if (!path.endsWith('/retain')) return stub.fetch(request);
      const retained = await stub.retain();
      const encoder = new TextEncoder();
      return new Response(new ReadableStream({
        async start(controller) {
          controller.enqueue(encoder.encode('retained:' + await retained.ping()));
          await scheduler.wait(18000);
          try {
            controller.enqueue(encoder.encode('still:' + await retained.ping()));
          } catch {
            controller.enqueue(encoder.encode('released'));
          }
          controller.close();
        },
      }));
    } };
  `;
  const deploy = (version, className, migrations) => cluster.deploy(tenant, name, source(className, version), {
    version, wrangler: { migrations, durable_objects: { bindings: [{ name: 'OBJECT', class_name: className }] } },
  });
  await deploy('v1', 'Record', [initial]);
  const first = await eventually('v1 creates a threaded record', async () => {
    const value = json(await call('', { method: 'POST', body: 'retained' }), 200, 'write v1');
    assert.equal(value.version, 'v1');
    return value;
  });
  const { namespaceId: identity, owners: [owner] } = await namespace('Record');
  const generation = (await durableGeneration(cluster, owner.nodeId)).generation;
  const openSocket = async mode => {
    const address = new URL(url + mode, cluster.urls[ingress]);
    address.protocol = 'ws:';
    const socket = new TestSocket(address);
    await once(socket, 'open', { signal: AbortSignal.timeout(5000) });
    const echoed = once(socket, 'message', { signal: AbortSignal.timeout(5000) });
    socket.send('probe');
    assert.equal((await echoed)[0].toString(), 'v1:probe');
    return socket;
  };
  const sockets = await Promise.all(['/socket', '/hibernatable'].map(openSocket));
  const facetSocket = await openSocket('/facet-socket');
  const facetClosed = once(facetSocket, 'close', { signal: AbortSignal.timeout(65000) });
  void facetClosed.catch(() => {});
  await eventually('the old definition has a parked facet WebSocket', async () => {
    const statistics = await Promise.all(cluster.nodes.map(node => cluster.state(node)));
    assert.ok(statistics.some(state => state.webSockets.parked > 0));
    assert.equal(facetSocket.readyState, TestSocket.OPEN);
  }, { timeout: 25000 });
  const socketsClosed = sockets.map(socket => once(socket, 'close', { signal: AbortSignal.timeout(35000) }));
  for (const closing of socketsClosed) void closing.catch(() => {});
  const heldRpc = await fetch(cluster.urls[ingress] + url + '/retain', { signal: AbortSignal.timeout(45000) });
  assert.equal(heldRpc.status, 200);
  const rpcReader = heldRpc.body.getReader();
  assert.equal(Buffer.from((await rpcReader.read()).value).toString(), 'retained:v1');
  const due = Date.now() + 8000;
  assert.equal(json(await call('/alarm', { method: 'POST', body: due }), 200, 'schedule v1 alarm'), due);
  const waiting = call('/wait-for-headers', { timeout: 60000 }).then(
    value => ({ value }), error => ({ error }),
  );
  await eventually('the second v1 request is waiting before response headers', async () => {
    assert.equal(json(await call('/waiting'), 200, 'v1 handler entered'), true);
  });
  const held = await fetch(cluster.urls[ingress] + url + '/held', { signal: AbortSignal.timeout(30000) });
  assert.equal(held.status, 200);
  const reader = held.body.getReader();
  try {
    const firstChunk = await reader.read();
    assert.equal(Buffer.from(firstChunk.value).toString(), 'entered:');
    await deploy('v2', 'RenamedRecord', [initial, renamed]);
    await eventually('the first v2 request is refused while v1 still holds the tree', async () => {
      const response = await call('');
      assert.equal(response.status, 503);
      assert.match(response.text, /DO_VERSION_DRAINING/);
    });
    const oldChunk = await reader.read();
    assert.match(Buffer.from(oldChunk.value).toString(), /^(v1)+$/, 'an open stream keeps its code pin');
    await assert.rejects(async () => {
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) return;
      }
    }, /terminated|closed|reset|aborted/i, 'the new definition cuts the old stream at the drain bound');
    const pending = await waiting;
    if (pending.error) throw pending.error;
    assert.equal(pending.value.status, 503, 'an old call without response headers has an uncertain result');
    assert.match(pending.value.text, /DO_CODE_UPDATED/);
    assert.match(pending.value.text, /Durable Object reset because its code was updated/);
    assert.match(pending.value.text, /execution result uncertain/);
  } finally {
    await reader.cancel().catch(() => {});
  }
  const freshFacet = await eventually('the new definition opens without the old facet socket or props', async () => {
    const response = await call('/facet-status');
    assert.equal(response.status, 200, response.text);
    return json(response, 200, 'new facet');
  });
  assert.deepEqual(freshFacet, { constructedWith: 0, version: 'v2' });
  const [facetCode, facetReason] = await facetClosed;
  assert.equal(facetCode, 1001);
  assert.match(facetReason.toString(), /definition changed/i);

  await eventually('renamed class uses the existing threaded storage', async () => {
    assert.equal((await namespace('RenamedRecord')).namespaceId, identity);
    const entry = json(await call(''), 200, 'read v2');
    assert.deepEqual(entry, { id: first.id, value: 'retained', version: 'v2', alarmVersion: 'v2' });
  });
  assert.equal((await durableGeneration(cluster, owner.nodeId)).generation, generation,
    'the target tree releases its sessions without replacing the process');
  assert.equal(json(await call('/wait-starts'), 200, 'confirmed call count'), 1,
    'the platform does not replay an old call with an uncertain result');
  assert.equal(Buffer.from((await rpcReader.read()).value).toString(), 'released',
    'a retained RPC capability from the old tree is revoked');
  await rpcReader.cancel();
  for (const closed of socketsClosed) assert.equal((await closed)[0], 1001, 'version drain closes both socket kinds');
  await cluster.deploy(tenant, name, respondWith('v3'), {
    version: 'v3', wrangler: { migrations: [initial, renamed, deleted] },
  });
  await eventually('deletion removes admission to the renamed namespace', async () => {
    assert.deepEqual(await call(''), { status: 200, text: 'v3' });
    const gone = await request(cluster.urls.apiserver,
      `${tenant.path}/workers/${name}/namespaces/RenamedRecord`, { token: tenant.token });
    if (gone.status === 200) {
      const record = JSON.parse(gone.text);
      assert.equal(record.namespaceId, identity);
      assert.ok(record.status === 'deleting' || record.status === 'deleted');
    } else {
      assert.equal(gone.status, 404);
    }
  });
}

async function threadedSocketHibernation(cluster) {
  const { WebSocket: PingSocket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const tenant = await cluster.tenant('threaded-hibernation');
  const [ingress] = cluster.nodes;
  const path = `/${tenant.name}/sleeper`;
  const namespace = () => cluster.api(`${tenant.path}/workers/sleeper/namespaces/Sleeper`,
    { token: tenant.token });
  await cluster.deploy(tenant, 'sleeper', `
    import { DurableObject, WorkerEntrypoint } from 'cloudflare:workers';
    export class ServiceProbe extends WorkerEntrypoint {
      async fetch() { return Response.json({ target: 'service', label: this.ctx.props.label }); }
    }
    export class ObjectProbe extends DurableObject {
      async fetch() { return Response.json({ target: 'object', id: this.ctx.id.toString() }); }
    }
    class SocketActor extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        this.role = ctx.props?.role ?? 'root';
        this.attachment = { field: 42, role: this.role };
        this.autoRequest = this.role === 'root' ? 'tick' : 'facet-tick';
        this.autoReply = this.role === 'root' ? 'tock' : 'facet-tock';
        const retained = ctx.getWebSockets('retained');
        const automatic = ctx.getWebSocketAutoResponse();
        this.constructedWith = retained.length;
        this.restored = retained.every(socket =>
          ctx.getTags(socket).join(',') === 'retained,' + this.role &&
          JSON.stringify(socket.deserializeAttachment()) === JSON.stringify(this.attachment)) &&
          (retained.length === 0 || (automatic?.request === this.autoRequest &&
            automatic.response === this.autoReply));
        this.sequence = 0;
        ctx.blockConcurrencyWhile(async () => {
          this.instance = (await ctx.storage.get('constructors') ?? 0) + 1;
          await ctx.storage.put('constructors', this.instance);
        });
      }
      async fetch(request) {
        const route = new URL(request.url).pathname;
        if (route.endsWith('/terminal')) {
          return Response.json(await this.ctx.storage.get('terminal') ?? null);
        }
        if (route.endsWith('/hold')) {
          return new Response(new ReadableStream({
            async start(controller) {
              controller.enqueue(new TextEncoder().encode('entered'));
              await scheduler.wait(20000);
              controller.close();
            },
          }));
        }
        if (route.endsWith('/discarded')) {
          return Response.json({ sockets: this.ctx.getWebSockets('retained').length,
            state: this.lastSocket?.readyState });
        }
        if (request.headers.get('upgrade')?.toLowerCase() === 'websocket') {
          const [client, socket] = Object.values(new WebSocketPair());
          this.ctx.acceptWebSocket(socket, ['retained', this.role]);
          this.lastSocket = socket;
          socket.serializeAttachment(this.attachment);
          this.ctx.setWebSocketAutoResponse(
            new WebSocketRequestResponsePair(this.autoRequest, this.autoReply));
          return new Response(null, { status: 101, webSocket: client });
        }
        return Response.json({ instance: this.instance, sockets: this.ctx.getWebSockets('retained').length,
          constructedWith: this.constructedWith, restored: this.restored, role: this.role });
      }
      async webSocketMessage(socket, message) {
        if (message === 'binding targets') {
          const { service, object } = this.ctx.props.bindings;
          socket.send(JSON.stringify(await Promise.all([service, object].map(async binding =>
            (await binding.fetch('http://binding/probe')).json()))));
          return;
        }
        socket.send(JSON.stringify({
          instance: this.instance, constructedWith: this.constructedWith, restored: this.restored,
          role: this.role, sequence: ++this.sequence, id: this.ctx.id.toString(),
          message: typeof message === 'string' ? message : [...new Uint8Array(message)],
          attachment: socket.deserializeAttachment(),
          retained: this.ctx.getWebSockets('retained').length,
          automatic: this.ctx.getWebSocketAutoResponseTimestamp(socket)?.getTime(),
        }));
      }
      async webSocketClose(socket, code, reason, wasClean) {
        await this.ctx.storage.put('terminal', { code, reason, wasClean });
        if (wasClean) socket.close(code, reason);
      }
      async webSocketError(socket, error) {
        await this.ctx.storage.put('terminal', { error: String(error) });
      }
    }
    export class SocketFacet extends SocketActor {}
    export class Parent extends DurableObject {
      fetch(request) {
        return this.ctx.facets.get('leaf', () => {
          if (new URL(request.url).pathname.endsWith('/status')) throw new Error('The saved facet must reopen.');
          return {
            class: this.ctx.exports.SocketFacet({ props: { role: 'leaf', bindings: {
              service: this.ctx.exports.ServiceProbe({ props: { label: 'original' } }),
              object: this.env.PROBE.getByName('original-object'),
            } } }),
            id: 'original-facet',
          };
        }).fetch(request);
      }
    }
    export class Sleeper extends SocketActor {
      async fetch(request) {
        const route = new URL(request.url).pathname;
        if (route.endsWith('/delete-facet')) {
          this.ctx.facets.delete('parent');
          return new Response(null, { status: 204 });
        }
        const parts = route.split('/');
        const actionIndex = parts.findIndex(part => ['delete-facet', 'abort-facet', 'probe-facet'].includes(part));
        const [action, child] = actionIndex === -1 ? [] : parts.slice(actionIndex);
        if (action === 'delete-facet' || action === 'abort-facet') {
          if (action === 'delete-facet') this.ctx.facets.delete(child);
          else this.ctx.facets.abort(child, new Error('test abort'));
          return new Response(null, { status: 204 });
        }
        if (action === 'probe-facet') {
          const facet = this.ctx.facets.get(child, () => ({
            id: child,
            class: this.ctx.exports.SocketFacet({ props: {
              role: 'leaf',
              ...(child === 'too-large' || child.startsWith('large-') ?
                { payload: 'x'.repeat(child === 'too-large' ? 70000 : 63000) } : {}),
            } }),
          }));
          try {
            const response = await facet.fetch(request);
            if (child === 'discard' && response.status === 101) {
              try { response.webSocket.accept(); } catch (error) {
                if (route.endsWith('/throw')) throw new Error('Discarding the upgrade after a throw.');
                return Response.json({ name: error.name });
              }
              throw new Error('Accepted an unforwarded facet peer.');
            }
            return response;
          } catch (error) {
            if (child === 'discard' && route.endsWith('/throw')) throw error;
            return Response.json({ name: error.name, message: error.message }, { status: 422 });
          }
        }
        if (route.includes('/facet/')) {
          return this.ctx.facets.get('parent', () => ({ class: this.ctx.exports.Parent })).fetch(request);
        }
        return super.fetch(request);
      }
    }
    export default {
      fetch(request, env) { return env.TREE.getByName('parked').fetch(request); }
    };
  `, { wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['Sleeper', 'ObjectProbe'] }],
    durable_objects: { bindings: [
      { name: 'TREE', class_name: 'Sleeper' }, { name: 'PROBE', class_name: 'ObjectProbe' },
    ] } } });

  await eventually('the threaded object answers successfully', async () => {
    assert.deepEqual(json(await request(cluster.urls[ingress], path), 200, 'open sleeper'),
      { instance: 1, sockets: 0, constructedWith: 0, restored: true, role: 'root' });
  });
  const owner = (await namespace()).owners[0];
  assert.ok(owner && cluster.nodes.includes(owner.nodeId));
  const activity = async () => (await durableGeneration(cluster, owner.nodeId)).lifecycle.durableAdmission;
  const retired = after => eventually('the tree and its live sessions leave the home', async () => {
    const state = await activity();
    assert.ok(state.idleRetirements > after, JSON.stringify(state));
    assert.equal(state.residentRoots, 0, JSON.stringify(state));
    assert.equal(state.homeSessions, 0, JSON.stringify(state));
    assert.equal(state.running, 0);
    return state.idleRetirements;
  }, { timeout: 25000 });

  const socketUrl = new URL(path, cluster.urls[owner.nodeId]);
  socketUrl.protocol = 'ws:';
  const first = new PingSocket(socketUrl);
  const second = new PingSocket(socketUrl);
  const facet = new PingSocket(new URL(socketUrl.pathname + '/facet/socket', socketUrl));
  const sockets = [first, second, facet];
  const next = async (socket, event, timeout = 7000) => {
    const abort = new AbortController();
    const deadline = setTimeout(() => abort.abort(new Error(`WebSocket ${event} timed out`)), timeout);
    try {
      return await once(socket, event, { signal: abort.signal });
    } catch (error) {
      if (abort.signal.aborted) throw abort.signal.reason;
      throw error;
    } finally {
      clearTimeout(deadline);
    }
  };
  const ping = async (socket, text) => {
    const pong = next(socket, 'pong');
    socket.ping(text);
    assert.equal((await pong)[0].toString(), text);
  };
  const send = async (socket, text) => {
    const answer = next(socket, 'message');
    socket.send(text);
    return (await answer)[0].toString();
  };
  const facetPath = (name, operation = 'socket') => `/probe-facet/${name}/${operation}`;
  const openFacet = async name => {
    const socket = new PingSocket(new URL(path + facetPath(name), socketUrl));
    sockets.push(socket);
    await next(socket, 'open');
    return socket;
  };
  const upgradeResponse = async (name, operation = 'socket') => {
    const socket = new PingSocket(new URL(path + facetPath(name, operation), socketUrl));
    sockets.push(socket);
    const [, response] = await next(socket, 'unexpected-response');
    try {
      const chunks = [];
      for await (const chunk of response) chunks.push(chunk);
      const text = Buffer.concat(chunks).toString();
      const body = response.headers['content-type']?.startsWith('application/json') ? JSON.parse(text) : text;
      return { status: response.statusCode, body };
    } finally {
      socket.on('error', () => {});
      socket.terminate();
    }
  };
  const lookup = suffix => request(cluster.urls[owner.nodeId], path + suffix);
  const closeFacet = async (socket, action, name) => {
    const closed = next(socket, 'close');
    void closed.catch(() => {});
    assert.equal((await lookup(`/${action}-facet/${name}`)).status, 204);
    const [code, reason] = await closed;
    assert.equal(code, 1001);
    assert.match(reason.toString(), new RegExp(action === 'abort' ? 'facet was aborted' : 'facet was deleted', 'i'));
  };
  let paused = false;
  try {
    await Promise.all(sockets.map(socket => next(socket, 'open')));
    await ping(first, 'before');
    assert.equal(await send(first, 'tick'), 'tock');
    const firstEvent = JSON.parse(await send(first, 'warm'));
    assert.equal(firstEvent.instance, 1);
    assert.equal(firstEvent.retained, 2);
    assert.deepEqual(firstEvent.attachment, { field: 42, role: 'root' });
    assert.ok(Number.isSafeInteger(firstEvent.automatic));
    await ping(facet, 'facet before retirement');
    assert.equal(await send(facet, 'facet-tick'), 'facet-tock');
    const warmFacet = JSON.parse(await send(facet, 'warm facet'));
    assert.equal(warmFacet.instance, 1);
    assert.equal(warmFacet.id, 'original-facet');
    assert.ok(Number.isSafeInteger(warmFacet.automatic));
    const warmBindings = JSON.parse(await send(facet, 'binding targets'));
    assert.deepEqual(warmBindings[0], { target: 'service', label: 'original' });
    assert.equal(warmBindings[1].target, 'object');
    assert.match(warmBindings[1].id, /^[0-9a-f]{64}$/);

    for (const operation of ['socket', 'throw']) {
      const discarded = await upgradeResponse('discard', operation);
      if (operation === 'throw') assert.ok(discarded.status >= 500, JSON.stringify(discarded));
      else assert.deepEqual(discarded, { status: 200, body: { name: 'NotSupportedError' } });
      await eventually(`the unforwarded socket closes and leaves its manager after ${operation}`, async () => {
        assert.deepEqual(json(await lookup(facetPath('discard', 'discarded')), 200, 'discarded socket'),
          { sockets: 0, state: PingSocket.CLOSED });
      }, { timeout: 7000 });
    }

    assert.equal((await lookup(facetPath('too-large', 'status'))).status, 200);
    const rejected = await upgradeResponse('too-large');
    assert.equal(rejected.status, 422, JSON.stringify(rejected.body));
    assert.equal(rejected.body.name, 'NotSupportedError');
    assert.match(rejected.body.message, /startup options cannot be restored/i);
    assert.deepEqual(json(await lookup(facetPath('too-large', 'discarded')), 200, 'rejected socket'),
      { sockets: 0, state: PingSocket.CLOSED });

    const largeSockets = [];
    for (let i = 0; i < 16; ++i) largeSockets.push(await openFacet(`large-${i}`));
    const treeLimit = await upgradeResponse('large-over-limit');
    assert.equal(treeLimit.status, 422, JSON.stringify(treeLimit.body));
    assert.equal(treeLimit.body.name, 'NotSupportedError');
    assert.match(treeLimit.body.message, /1 MiB/);
    await Promise.all(largeSockets.map((socket, i) => closeFacet(socket, 'delete', `large-${i}`)));

    await closeFacet(await openFacet('idle-abort'), 'abort', 'idle-abort');
    const busy = await openFacet('busy-abort');
    const signal = AbortSignal.timeout(15000);
    const streaming = await fetch(cluster.urls[owner.nodeId] + path + facetPath('busy-abort', 'hold'),
      { signal });
    assert.equal(streaming.status, 200);
    const reader = streaming.body.getReader();
    try {
      assert.equal(Buffer.from((await reader.read()).value).toString(), 'entered');
      await closeFacet(busy, 'abort', 'busy-abort');
      const end = await reader.read().catch(() => ({ done: true }));
      assert.equal(end.done, true);
      assert.equal(signal.aborted, false, 'the facet abort cancels its running request');
    } finally {
      await reader.cancel().catch(() => {});
    }

    const firstRetirement = await retired(0);
    const parked = (await cluster.state(owner.nodeId)).webSockets;
    assert.equal(parked.parked, 3);
    await ping(second, 'while parked');
    assert.equal(await send(second, 'tick'), 'tock');
    await ping(facet, 'facet while parked');
    assert.equal(await send(facet, 'facet-tick'), 'facet-tock');
    const afterAutomatic = await activity();
    assert.equal(afterAutomatic.residentRoots, 0);
    assert.equal(afterAutomatic.homeSessions, 0);
    assert.equal(afterAutomatic.idleRetirements, firstRetirement);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.socketWakes, parked.socketWakes);
    assert.deepEqual(json(await request(cluster.urls[owner.nodeId], path), 200, 'normal HTTP wake'),
      { instance: 2, sockets: 2, constructedWith: 2, restored: true, role: 'root' },
      'a normal reopen restores the root sockets before the constructor runs');
    const secondRetirement = await retired(firstRetirement);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 3,
      'the facet socket parks again even though only the root opened');
    assert.equal(await send(facet, 'facet-tick'), 'facet-tock');
    const burst = ['facet wake', Buffer.from([0, 128, 255]), 'last'];
    const replies = [];
    const onMessage = body => replies.push(JSON.parse(body.toString()));
    facet.on('message', onMessage);
    try {
      const last = eventually('the sleeping facet receives all queued frames', () => {
        assert.equal(replies.length, burst.length);
      }, { timeout: 7000 });
      for (const frame of burst) facet.send(frame);
      await last;
      const marker = next(facet, 'message');
      facet.send('marker');
      assert.equal(JSON.parse((await marker)[0].toString()).message, 'marker');
    } finally {
      facet.off('message', onMessage);
    }
    assert.deepEqual(replies.map(reply => [reply.sequence, reply.message]), [
      [1, 'facet wake'], [2, [0, 128, 255]], [3, 'last'], [4, 'marker'],
    ]);
    for (const reply of replies) {
      assert.equal(reply.id, 'original-facet');
      assert.equal(reply.role, 'leaf');
      assert.equal(reply.instance, 2);
      assert.equal(reply.constructedWith, 1);
      assert.equal(reply.retained, 1);
      assert.equal(reply.restored, true);
      assert.deepEqual(reply.attachment, { field: 42, role: 'leaf' });
      assert.ok(reply.automatic > warmFacet.automatic);
    }
    assert.deepEqual(JSON.parse(await send(facet, 'binding targets')), warmBindings,
      'the reopened facet reaches the original service binding and Durable Object stub');

    assert.equal(await send(first, 'tick'), 'tock');
    const wake = JSON.parse(await send(first, 'wake'));
    assert.equal(wake.instance, 3);
    assert.equal(wake.message, 'wake');
    assert.equal(wake.retained, 2);
    assert.equal(wake.constructedWith, 2);
    assert.deepEqual(wake.attachment, { field: 42, role: 'root' });
    assert.ok(wake.automatic > firstEvent.automatic);
    assert.deepEqual(JSON.parse(await send(second, Buffer.from([0, 128, 255]))).message, [0, 128, 255]);

    const thirdRetirement = await retired(secondRetirement);
    assert.deepEqual(json(await lookup('/facet/status'), 200, 'facet HTTP wake'),
      { instance: 3, sockets: 1, constructedWith: 1, restored: true, role: 'leaf' },
      'a normal reopen restores the facet socket, tags, attachment and automatic reply before its constructor');

    const readTerminal = async () => json(await request(cluster.urls[owner.nodeId], path + '/terminal'),
      200, 'socket termination');
    const clean = new PingSocket(socketUrl);
    await next(clean, 'open');
    const cleanClose = next(clean, 'close');
    clean.close(1000, 'finished');
    assert.equal((await cleanClose)[0], 1000);
    await eventually('the actor receives the clean close code and its reason', async () => {
      assert.deepEqual(await readTerminal(), { code: 1000, reason: 'finished', wasClean: true });
    }, { timeout: 7000 });
    const interrupted = new PingSocket(socketUrl);
    await next(interrupted, 'open');
    assert.equal(JSON.parse(await send(interrupted, 'armed')).message, 'armed');
    interrupted.terminate();
    await eventually('the actor receives one abnormal close', async () => {
      const terminal = await readTerminal();
      assert.equal(terminal?.code, 1006);
      assert.equal(terminal.wasClean, false);
    }, { timeout: 7000 });
    const fourthRetirement = await retired(thirdRetirement);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 3);
    const facetClosed = next(facet, 'close');
    void facetClosed.catch(() => {});
    assert.equal((await request(cluster.urls[owner.nodeId], path + '/delete-facet')).status, 204);
    const [facetCloseCode, facetCloseReason] = await facetClosed;
    assert.equal(facetCloseCode, 1001);
    assert.match(facetCloseReason.toString(), /facet was deleted/i);
    assert.equal(await send(first, 'tick'), 'tock');
    await retired(fourthRetirement);

    const survivor = cluster.nodes.find(node => node !== owner.nodeId);
    const closed = [first, second].map(socket => next(socket, 'close', 45000));
    closed.forEach(promise => void promise.catch(() => {}));
    await cluster.compose(['pause', owner.nodeId]);
    paused = true;
    await eventually('a surviving node takes the parked socket shard', async () => {
      const read = json(await request(cluster.urls[survivor], path, { timeout: 7000 }), 200, 'takeover wake');
      assert.deepEqual(read, { instance: 6, sockets: 0, constructedWith: 0, restored: true, role: 'root' });
      const nextOwner = (await namespace()).owners[0];
      assert.notEqual(nextOwner.nodeId, owner.nodeId);
      assert.ok(BigInt(nextOwner.epoch) > BigInt(owner.epoch));
    }, { timeout: 35000 });
    await cluster.compose(['unpause', owner.nodeId]);
    paused = false;
    for (const close of closed) assert.equal((await close)[0], 1001, 'loss of shard fences the parked socket');
  } finally {
    if (paused) await cluster.compose(['unpause', owner.nodeId]);
    for (const socket of sockets) socket.terminate();
  }
}

async function threadedSocketRetirement(cluster) {
  const { WebSocket: Socket } = createRequire(new URL('../../cli/package.json', import.meta.url))('ws');
  const tenant = await cluster.tenant('threaded-retirement');
  const [ingress] = cluster.nodes;
  const path = `/${tenant.name}/sleeper`;
  const call = (route, options) => request(cluster.urls[ingress], path + route, options);
  await cluster.deploy(tenant, 'sleeper', `
    import { DurableObject } from 'cloudflare:workers';
    export class Sleeper extends DurableObject {
      constructor(ctx, env) {
        super(ctx, env);
        ctx.blockConcurrencyWhile(async () => {
          this.instance = (await ctx.storage.get('constructors') ?? 0) + 1;
          await ctx.storage.put('constructors', this.instance);
        });
      }
      async fetch(request) {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/ordinary') || path.endsWith('/managed')) {
          const [client, socket] = Object.values(new WebSocketPair());
          if (path.endsWith('/managed')) {
            this.ctx.acceptWebSocket(socket, ['managed']);
            socket.serializeAttachment({ token: 'preserved' });
            this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('tick', 'tock'));
            socket.send('welcome');
          } else {
            socket.accept();
            socket.addEventListener('message', event => {
              if (event.data === 'reset') this.ctx.abort('reset the old home after transfer');
              const managed = this.ctx.getWebSockets('managed')[0];
              const changes = [
                () => managed.serializeAttachment({ token: 'lost' }),
                () => this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('tick', 'lost')),
                () => managed.send('lost'),
                () => managed.close(1000, 'lost'),
              ];
              socket.send(JSON.stringify(changes.map(change => {
                try { change(); return 'accepted'; } catch (error) { return error.name; }
              })));
            });
          }
          return new Response(null, { status: 101, webSocket: client });
        }
        if (path.endsWith('/local')) {
          const [peer, socket] = Object.values(new WebSocketPair());
          this.ctx.acceptWebSocket(socket, ['local']);
          try { peer.accept(); } catch (error) {
            return Response.json({ name: error.name, message: error.message });
          }
          return Response.json({ name: 'accepted' });
        }
        if (path.endsWith('/hold')) this.ctx.waitUntil(scheduler.wait(120000));
        if (path.endsWith('/write')) this.ctx.getWebSockets('managed')[0].send('write-finished');
        if (path.endsWith('/reset')) this.ctx.abort('reset the listening socket');
        return Response.json(this.instance);
      }
      webSocketMessage(socket, message) {
        socket.send(this.instance + ':' + socket.deserializeAttachment().token);
      }
      webSocketClose(socket, code, reason) { if (code !== 1006) socket.close(code, reason); }
    }
    export default { fetch(request, env) { return env.TREE.getByName('stable').fetch(request); } };
  `, { wrangler: { migrations: [{ tag: 'v1', new_sqlite_classes: ['Sleeper'] }],
    durable_objects: { bindings: [{ name: 'TREE', class_name: 'Sleeper' }] } } });
  await eventually('a hibernatable pair cannot be used without an HTTP upgrade', async () => {
    const local = json(await call('/local'), 200, 'local pair');
    assert.equal(local.name, 'NotSupportedError');
    assert.match(local.message, /requires an HTTP upgrade/);
  });
  const { owners: [owner] } = await cluster.api(`${tenant.path}/workers/sleeper/namespaces/Sleeper`, { token: tenant.token });
  const activity = async () => (await durableGeneration(cluster, owner.nodeId)).lifecycle.durableAdmission;
  const generation = (await durableGeneration(cluster, owner.nodeId)).generation;
  const control = name => join(cluster.durableLifecycleGate, name);
  const advance = async elapsed => {
    const next = control('idle-offset-next');
    await writeFile(next, String(elapsed), { mode: 0o644 });
    await rename(next, control('idle-offset-ms'));
    await delay(550);
  };
  const open = route => {
    const address = new URL(path + route, cluster.urls[owner.nodeId]);
    address.protocol = 'ws:';
    return new Socket(address);
  };
  const next = (socket, event, timeout = 7000) => once(socket, event, { signal: AbortSignal.timeout(timeout) })
    .catch(error => { throw new Error(`WebSocket ${socket.url} did not deliver ${event}`, { cause: error }); });
  const send = async (socket, message) => {
    const answer = next(socket, 'message');
    socket.send(message);
    return (await answer)[0].toString();
  };
  const retired = () => eventually('the ordinary socket or expired work releases the tree with its managed socket parked', async () => {
    const current = await activity();
    assert.equal(current.homeSessions, 0);
    assert.equal(current.residentRoots, 0);
    assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 1);
  });
  const ordinary = open('/ordinary'), managed = open('/managed');
  const welcome = next(managed, 'message');
  try {
    await Promise.all([next(ordinary, 'open'), next(managed, 'open')]);
    assert.equal((await welcome)[0].toString(), 'welcome', 'writes queued before 101 move to the upgraded transport');
    const before = json(await call('/status'), 200, 'read before retirement');
    const onlyOrdinarySession = () => eventually('only the ordinary transport still holds an event ticket', async () => {
      const state = await activity();
      assert.equal(state.homeSessions, 1);
      assert.equal(state.running, 1);
    });
    await onlyOrdinarySession();
    await advance(65000);
    assert.equal(ordinary.readyState, Socket.OPEN);
    await writeFile(control('hold-socket-park'), '');
    await advance(150000);
    assert.equal(ordinary.readyState, Socket.OPEN, 'the ordinary socket remains open until the managed socket is parked');
    assert.deepEqual(JSON.parse(await send(ordinary, 'mutate')), Array(4).fill('InvalidStateError'),
      'an ordinary handler cannot change the managed transport after the snapshot');
    let settled = false;
    const racingRequest = call('/status', { timeout: 7000 })
      .then(response => { settled = true; return response; });
    void racingRequest.catch(() => {});
    await delay(400);
    assert.equal(settled, false, 'new requests wait at the retiring tree fence');
    const raced = await racingRequest;
    if (raced.status === 200) assert.equal(JSON.parse(raced.text), before);
    else assert.equal(raced.status, 503, raced.text);
    await eventually('a timed out park resumes admission to the same tree', async () => {
      assert.equal(json(await call('/status'), 200, 'park refused'), before);
    });
    await onlyOrdinarySession();
    await unlink(control('hold-socket-park'));
    await writeFile(control('hold-socket-park-reply'), '');
    const ordinaryClosed = next(ordinary, 'close', 13000);
    await advance(305000);
    await eventually('the node parks the socket while the original reply is held', async () => {
      assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 1);
    });
    await ordinaryClosed.catch(async error => {
      const state = await cluster.state(owner.nodeId);
      throw new Error(`Ordinary socket stayed open. ${JSON.stringify({ activity: await activity(), websockets: state.webSockets })}`,
        { cause: error });
    });
    await unlink(control('hold-socket-park-reply'));
    await retired();
    assert.equal(await send(managed, 'tick'), 'tock');
    assert.equal((await activity()).homeSessions, 0);

    await writeFile(control('hold-socket-adopt'), '');
    const stalled = await call('/status', { timeout: 7500 }).catch(() => null);
    assert.ok(stalled == null || stalled.status === 503, 'no request starts before adoption succeeds');
    assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 1);
    await unlink(control('hold-socket-adopt'));
    await writeFile(control('hold-socket-adopt-reply'), '');
    const cancelled = call('/status', { timeout: 2200 }).catch(() => null);
    await eventually('the node adopts while the reply is held', async () => {
      assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 0);
    });
    assert.equal(await cancelled, null, 'the first caller disconnects while waiting for adoption');
    await delay(5400);
    await advance(322000);
    await retired();
    await unlink(control('hold-socket-adopt-reply'));
    const restored = await eventually('a new open restores the parked socket after adoption lost its reply', async () => {
      const instance = json(await call('/status'), 200, 'adopt after timeout');
      // The interrupted ingress may finish its submitted event when adoption
      // resolves. Either way a single subsequent open restores the socket.
      assert.ok(instance === before + 1 || instance === before + 2, String(instance));
      return instance;
    });
    assert.equal(await send(managed, 'resumed'), restored + ':preserved');

    const oldHome = open('/ordinary');
    try {
      await next(oldHome, 'open');
      await onlyOrdinarySession();
      await writeFile(control('hold-socket-park-reply'), '');
      await advance(482000);
      await eventually('the node has parked the managed socket before the old home resets', async () => {
        assert.equal((await cluster.state(owner.nodeId)).webSockets.parked, 1);
      });
      const oldHomeClosed = next(oldHome, 'close', 3500);
      oldHome.send('reset');
      await oldHomeClosed;
      await retired();
      await unlink(control('hold-socket-park-reply'));
      assert.equal(await send(managed, 'tick'), 'tock', 'a reset of the old home leaves the parked socket live');
      assert.equal(await send(managed, 'after old home reset'), (restored + 1) + ':preserved');
    } finally {
      oldHome.terminate();
    }

    assert.equal(json(await call('/hold'), 200, 'start pending work'), restored + 1);
    await eventually('the pending-work request releases its session', async () => {
      assert.equal((await activity()).homeSessions, 0);
    });
    await advance(482000 + 14 * 60 * 1000);
    assert.equal((await activity()).residentRoots, 1, 'pending work protects the tree before 15 minutes');
    const afterPendingExpiry = 482000 + 15 * 60 * 1000 + 1000;
    await advance(afterPendingExpiry);
    await retired();
    assert.equal(await send(managed, 'after the deadline'), (restored + 2) + ':preserved',
      'expiry parks the attachment before aborting pending work');
    const resetClosed = next(managed, 'close');
    await call('/reset', { timeout: 7000 }).catch(() => null);
    const [code, reason] = await resetClosed;
    assert.equal(code, 1011);
    assert.equal(reason.toString(), 'Durable Object reset');
    await eventually('the forced reset releases its home before the next upgrade', async () => {
      const current = await activity();
      assert.equal(current.homeSessions, 0);
      assert.equal(current.residentRoots, 0);
    });
    await eventually('a fresh request opens on the surviving generation', async () => {
      json(await call('/status'), 200, 'read after reset');
    });

    const slow = open('/managed');
    const greeting = next(slow, 'message');
    void greeting.catch(() => {});
    const nodeLogs = () => cluster.compose(['logs', '--no-color', '--tail', '100', owner.nodeId]);
    const waitForHeldWrite = count => eventually('the managed write reaches the node test gate', async () => {
      assert.equal((await nodeLogs()).match(/\[websocket-test\] managed write held/g)?.length ?? 0, count);
    });
    try {
      await next(slow, 'open');
      assert.equal((await greeting)[0].toString(), 'welcome');
      await writeFile(control('hold-socket-write'), '');
      const delivered = next(slow, 'message', 10000);
      json(await call('/write'), 200, 'start a slow write');
      await waitForHeldWrite(1);
      await delay(5300);
      assert.equal(slow.readyState, Socket.OPEN, 'a write may take more than five seconds outside parking');
      await unlink(control('hold-socket-write'));
      assert.equal((await delivered)[0].toString(), 'write-finished');
      await writeFile(control('hold-socket-write'), '');
      json(await call('/write'), 200, 'start a write that will block parking');
      await waitForHeldWrite(2);
      const stalledClosed = next(slow, 'close', 12000);
      await advance(afterPendingExpiry + 15 * 60 * 1000 + 3000);
      await stalledClosed;
      const logs = await nodeLogs();
      assert.ok(logs.includes('"cause":"park-write-timeout"'), 'the node closed the write that prevented parking');
      await unlink(control('hold-socket-write'));
    } finally {
      slow.terminate();
    }
    assert.equal((await durableGeneration(cluster, owner.nodeId)).generation, generation);
  } finally {
    ordinary.terminate();
    managed.terminate();
  }
}

// The profiler uses the standard cluster. Its node-level fault runs last.
export const cases = { deployment, 'sql-atomicity': sqlAtomicity,
  'invocation-budgets': invocationBudgets, 'd1-invocation-budgets': d1InvocationBudgets, 'cpu-isolation': cpuIsolation,
  'process-replacement': processReplacement, 'log-quota': logQuota,
  'durable-takeover': durableTakeover };

export const webSocketCases = { 'websocket-admission': webSocketAdmission };

export const durableGatedCases = {
  'invocation-budget-lifetime': { test: invocationBudgetLifetime, budgetLifetime },
  'invocation-budget-threaded-background': {
    test: invocationBudgetLifetime, budgetLifetime, nodes: ['node-1', 'node-2', 'node-3'],
  },
  'durable-peer-ack': { test: durablePeerAck },
  'durable-slow-holder': {
    test: durableSlowHolder, delayRecoveryPeers: true,
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
  },
  'durable-threaded-mixed': { test: mixedDurableStorage, nodes: ['node-1'] },
  'durable-threaded-takeover': { test: durableTakeover, nodes: ['node-1', 'node-2', 'node-3'] },
  'durable-threaded-fence': {
    test: durableThreadedFence, nodes: ['node-1', 'node-2', 'node-3'],
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
    confirmTimeoutMs: 25000,
  },
  'durable-threaded-alarm-fence': {
    test: threadedAlarmTakeover, nodes: ['node-1', 'node-2', 'node-3'],
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
    confirmTimeoutMs: 25000,
  },
  'durable-threaded-migration': {
    test: threadedClassMigration, nodes: ['node-1', 'node-2', 'node-3'], versionDrainTimeoutMs: 10000,
  },
  'durable-threaded-hibernation': {
    test: threadedSocketHibernation, nodes: ['node-1', 'node-2', 'node-3'],
    membership: { ttlSeconds: 6, heartbeatIntervalMs: 1000, livenessGraceMs: 3000, listIntervalMs: 200 },
  },
  'durable-threaded-retirement': {
    test: threadedSocketRetirement, nodes: ['node-1', 'node-2', 'node-3'], controlDurableLifecycle: true,
  },
};

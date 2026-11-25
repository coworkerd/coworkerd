import assert from 'node:assert/strict';
import { get } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { eventually, json, request } from './cluster.mjs';

const serviceWorker = `
  import { RpcTarget, WorkerEntrypoint } from 'cloudflare:workers';

  class Handle extends RpcTarget {
    value() { return 'rpc'; }
  }

  export default class LeaseService extends WorkerEntrypoint {
    fetch() { return new Response('service'); }
    openHandle() { return new Handle(); }
  }
`;

const worker = `
  import { DurableObject } from 'cloudflare:workers';

  export class LeaseObject extends DurableObject {
    async fetch(request) {
      const path = new URL(request.url).pathname;
      if (path === '/relay') {
        return this.env.OBJECT.getByName('leaf').fetch('https://object.test/leaf');
      }
      if (path === '/markers') return Response.json(Array.from((await this.ctx.storage.list()).keys()));
      if (path === '/timer') {
        setTimeout(async () => {
          const leaf = await this.env.OBJECT.getByName('leaf').fetch('https://object.test/leaf');
          await this.ctx.storage.put('timer', await leaf.text());
        }, 32_000);
        return new Response('scheduled');
      }
      if (path === '/take-over') {
        await scheduler.wait(20_000);
        return new Response(await this.ctx.storage.get('timer'));
      }
      if (path === '/timer-result') return new Response(await this.ctx.storage.get('timer'));
      if (request.method === 'PUT') {
        await this.ctx.storage.put(path.slice(1), true);
        return new Response(null, { status: 204 });
      }
      return new Response('object');
    }
  }

  function disconnect(env, ctx, mode) {
    const ledger = env.OBJECT.getByName('disconnect-markers');
    const record = async marker => {
      const result = await ledger.fetch('https://object.test/' + mode + '/' + marker, { method: 'PUT' });
      if (result.status !== 204) throw new Error('Could not record ' + marker + ': ' + result.status);
    };
    const registered = record('registered');
    ctx.waitUntil((async () => {
      await registered;
      await scheduler.wait(32_000);
      await record('background');
      await scheduler.wait(8_000);
      await record('after-drain');
    })());

    const foreground = async () => {
      await registered;
      await scheduler.wait(8_000);
      await record('foreground');
    };
    if (mode === 'before-headers') return foreground().then(() => new Response('finished'));
    return new Response(new ReadableStream({
      async start(controller) {
        await registered;
        controller.enqueue(new TextEncoder().encode('started'));
        await foreground();
        controller.close();
      },
    }));
  }

  export default {
    fetch(request, env, ctx) {
      const path = new URL(request.url).pathname.split('/').at(-1);
      if (path === 'before-headers' || path === 'during-body') return disconnect(env, ctx, path);
      if (path === 'markers') return env.OBJECT.getByName('disconnect-markers').fetch('https://object.test/markers');
      if (path === 'timer' || path === 'take-over' || path === 'timer-result') {
        const actor = new URL(request.url).searchParams.get('actor');
        return env.OBJECT.getByName(actor).fetch('https://object.test/' + path);
      }
      if (path === 'silent') {
        return new Response(new ReadableStream({ async start(controller) {
          controller.enqueue(new TextEncoder().encode('started'));
          await scheduler.wait(60_000);
          controller.close();
        } }));
      }
      if (path !== 'stream') return new Response('ready');
      const encoder = new TextEncoder();
      return new Response(new ReadableStream({
        async start(controller) {
          const handle = await env.SERVICE.openHandle();
          controller.enqueue(encoder.encode('started\\n'));
          await scheduler.wait(32_000);
          const service = await env.SERVICE.fetch('https://service.test/');
          if (!service.ok) throw new Error('Service Binding returned ' + service.status);
          controller.enqueue(encoder.encode(await service.text() + '\\n'));
          const object = await env.OBJECT.getByName('relay').fetch('https://object.test/relay');
          if (!object.ok) throw new Error('Durable Object returned ' + object.status);
          controller.enqueue(encoder.encode(await object.text() + '\\n'));
          controller.enqueue(encoder.encode(await handle.value() + '\\n'));
          await scheduler.wait(88_000);
          controller.enqueue(encoder.encode('finished\\n'));
          controller.close();
        },
      }), { headers: { 'content-type': 'text/plain; charset=utf-8' } });
    },
  };
`;

async function prepareLeaseWorker(cluster, tenantName) {
  const tenant = await cluster.tenant(tenantName);
  await cluster.deploy(tenant, 'lease-service', serviceWorker);
  await cluster.deploy(tenant, 'lease-stream', worker, { wrangler: {
    services: [{ binding: 'SERVICE', service: 'lease-service' }],
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'LeaseObject' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['LeaseObject'] }],
  } });
  await cluster.api(`${tenant.path}/workers/lease-stream`, {
    token: tenant.token, method: 'PATCH', body: { limits: { cpuMs: 1500 } },
  });

  const [ingress] = cluster.nodes;
  const base = cluster.urls[ingress];
  const path = `/${tenant.name}/lease-stream`;
  // In this fixture both lease Workers have a home other than the first node.
  assert.equal((await cluster.state(ingress)).placement.ingressPlacement, true);
  await eventually('the version and CPU limit are visible at ingress', async () => {
    const state = await cluster.state(ingress);
    const status = await cluster.api('/v1/status');
    assert.equal(String(state.storage.appliedTenantsRevision), String(status.storage.tenantsRevision));
    assert.deepEqual(await request(base, path), { status: 200, text: 'ready' });
  });
  return { ingress, base, path };
}

export async function connectedRequestLease(cluster) {
  const { ingress, base, path } = await prepareLeaseWorker(cluster, 'live-request-lease');
  const controller = new AbortController();
  const signal = AbortSignal.any([cluster.signal, controller.signal, AbortSignal.timeout(145_000)]);
  try {
    const sentAt = performance.now();
    let halfClosed = false;
    const response = await new Promise((resolve, reject) => {
      const client = get(base + path + '/stream', { agent: false, signal }, resolve);
      client.once('socket', socket => client.once('finish', () => socket.end(() => { halfClosed = true; })));
      client.once('error', reject);
    });
    assert.equal(response.statusCode, 200);
    const expected = ['started', 'service', 'object', 'rpc', 'finished'];
    const observed = [];
    let pending = '';
    let startedAt;
    for await (const chunk of response) {
      pending += chunk.toString();
      const lines = pending.split('\n');
      pending = lines.pop();
      for (const line of lines) {
        assert.equal(line, expected[observed.length]);
        observed.push(line);
        if (line === 'started') {
          assert.ok(halfClosed, 'the client ended its write side before reading the response');
          startedAt = performance.now();
          const { placement } = await cluster.state(ingress);
          assert.equal(placement.proxyingIngress, 1, 'the streaming invocation runs on a peer');
        }
        if (line === 'service' || line === 'object' || line === 'rpc') {
          assert.ok(performance.now() - startedAt >= 30_000, `${line} ran after the old lease`);
        }
      }
    }
    assert.equal(pending, '', 'every response frame ended at a line boundary');
    assert.deepEqual(observed, expected);
    const elapsed = performance.now() - sentAt;
    assert.ok(elapsed >= 120_000, 'the connected stream completed after 120 seconds');
    console.log(`A half-closed HTTP client received the complete ${(elapsed / 1000).toFixed(1)} s stream`);
  } finally {
    controller.abort();
  }
}

export async function disconnectedRequestLease(cluster) {
  const { ingress, base, path } = await prepareLeaseWorker(cluster, 'live-request-disconnect');
  for (const mode of ['before-headers', 'during-body']) {
    const markers = async () => json(await request(base, path + '/markers'), 200, 'read disconnect markers')
      .filter(marker => marker.startsWith(mode + '/')).map(marker => marker.slice(mode.length + 1));
    let receivedHeaders = false;
    const first = Promise.withResolvers();
    const client = get(base + path + '/' + mode, { agent: false, signal: cluster.signal }, response => {
      receivedHeaders = true;
      response.on('error', () => {});
      response.once('data', value => first.resolve({ status: response.statusCode, text: value.toString() }));
    });
    client.on('error', () => {});
    try {
      await eventually(`${mode} registered background work`, async () => {
        assert.deepEqual(await markers(), ['registered']);
      }, { timeout: 8000, interval: 100 });
      const registeredAt = performance.now();
      if (mode === 'during-body') {
        const frame = await Promise.race([first.promise, delay(5000)]);
        assert.deepEqual(frame, { status: 200, text: 'started' });
      }
      // The background marker is scheduled after the original 30 s lease.
      // Give the request four seconds before starting the disconnect drain.
      await delay(4000, undefined, { signal: cluster.signal });
      assert.equal(receivedHeaders, mode === 'during-body');
      assert.ok(client.socket && !client.socket.destroyed);
      assert.equal((await cluster.state(ingress)).placement.proxyingIngress, 1);
      const resetAt = performance.now();
      client.socket.resetAndDestroy();
      await eventually(`${mode} released the forwarding slot after reset`, async () => {
        assert.equal((await cluster.state(ingress)).placement.proxyingIngress, 0);
      }, { timeout: 1000, interval: 25 });
      console.log(`HTTP reset ${mode} stopped ingress forwarding in ${(performance.now() - resetAt).toFixed(1)} ms`);
      await eventually(`${mode} left only the registered work alive`, async () => {
        assert.deepEqual(await markers(), ['background', 'registered']);
      }, { timeout: 31_000, interval: 250 });
      const untilAfterDrain = registeredAt + 42_000 - performance.now();
      if (untilAfterDrain > 0) await delay(untilAfterDrain, undefined, { signal: cluster.signal });
      assert.deepEqual(await markers(), ['background', 'registered'], `${mode} ended all work after its one drain`);
    } finally {
      client.destroy();
    }
  }
}

export async function silentRequestLease(cluster) {
  const { ingress, path } = await prepareLeaseWorker(cluster, 'live-request-silent');
  const client = `${cluster.project}-silent-client`;
  const network = `${cluster.project}_backend`;
  const script = `
    const http = require('node:http');
    http.get(process.argv[1], response => {
      response.once('data', chunk => {
        if (response.statusCode !== 200 || chunk.toString() !== 'started') process.exit(1);
        console.log('connected');
      });
      response.on('error', () => {});
    }).on('error', () => {});
    setInterval(() => {}, 1000);
  `;
  await cluster.command('docker', ['run', '--rm', '--detach', '--network', network, '--name', client,
    'node:22-trixie', 'node', '-e', script, `http://${ingress}-peer:8930${path}/silent`]);
  try {
    await eventually('the client received the start of its silent response', async () => {
      assert.equal(await cluster.command('docker', ['logs', client]), 'connected');
      assert.equal((await cluster.state(ingress)).placement.proxyingIngress, 1);
    });
    const lostAt = performance.now();
    await cluster.command('docker', ['network', 'disconnect', network, client]);
    await eventually('TCP keepalive closes a silent request after the client loses its network', async () => {
      assert.equal((await cluster.state(ingress)).placement.proxyingIngress, 0);
    }, { timeout: 7000, interval: 50 });
    console.log(`Silent HTTP client detected in ${(performance.now() - lostAt).toFixed(1)} ms`);
  } finally {
    await cluster.command('docker', ['stop', '-t', '1', client], { cleanup: true });
  }
}

export async function actorRequestLease(cluster) {
  const { base, path } = await prepareLeaseWorker(cluster, 'live-request-timers');
  const call = (event, actor, timeout = 5000) => request(base, `${path}/${event}?actor=${actor}`, { timeout });
  for (const actor of ['idle', 'taken-over']) {
    assert.deepEqual(await call('timer', actor), { status: 200, text: 'scheduled' });
  }
  await delay(16_000, undefined, { signal: cluster.signal });
  const takeover = call('take-over', 'taken-over', 30_000);
  assert.deepEqual(await takeover, { status: 200, text: 'object' }, 'the newer event can hold the timer work');
  assert.deepEqual(await call('timer-result', 'idle'), { status: 200, text: 'object' },
    'an actor timer can make a bounded call with no live ingress event');
}

export async function alarmRequestLease(cluster) {
  const tenant = await cluster.tenant('alarm-wall-time');
  await cluster.deploy(tenant, 'alarm', `
    import { DurableObject } from 'cloudflare:workers';
    export class Clock extends DurableObject {
      async alarm(info) {
        if (info.retryCount) return;
        await this.ctx.storage.put('started', true);
        const wait = await this.ctx.storage.get('wait');
        await scheduler.wait(wait);
        await this.ctx.storage.put('finished', true);
      }
      async fetch(request) {
        if (request.method === 'POST') {
          await this.ctx.storage.put('wait', await request.json());
          await this.ctx.storage.setAlarm(Date.now() + 500);
          return new Response('scheduled');
        }
        return Response.json({
          started: await this.ctx.storage.get('started') ?? false,
          finished: await this.ctx.storage.get('finished') ?? false,
        });
      }
    }
    export default { fetch(request, env) {
      return env.CLOCK.getByName(new URL(request.url).pathname.split('/').at(-1)).fetch(request);
    } };
  `, { wrangler: {
    durable_objects: { bindings: [{ name: 'CLOCK', class_name: 'Clock' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['Clock'] }],
  } });
  const base = cluster.urls[cluster.nodes[0]];
  const path = `/${tenant.name}/alarm`;
  const read = async key => json(await request(base, `${path}/${key}`), 200, `read ${key} alarm`);
  await eventually('the alarm Worker is ready', async () => {
    assert.deepEqual(await read('short'), { started: false, finished: false });
  });
  for (const [key, duration] of [['short', 150], ['long', 3500]]) {
    assert.deepEqual(await request(base, `${path}/${key}`, { method: 'POST', body: duration }),
      { status: 200, text: 'scheduled' });
  }
  await eventually('both alarm handlers start', async () => {
    assert.deepEqual(await read('short'), { started: true, finished: true });
    assert.equal((await read('long')).started, true);
  });
  await delay(4000, undefined, { signal: cluster.signal });
  assert.deepEqual(await read('long'), { started: true, finished: false }, 'the wall limit cancelled the long handler');
}

import assert from 'node:assert/strict';
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
    const response = await fetch(base + path + '/stream', { signal });
    assert.equal(response.status, 200);
    const expected = ['started', 'service', 'object', 'rpc', 'finished'];
    const observed = [];
    let pending = '';
    let startedAt;
    for await (const chunk of response.body.pipeThrough(new TextDecoderStream())) {
      pending += chunk;
      const lines = pending.split('\n');
      pending = lines.pop();
      for (const line of lines) {
        assert.equal(line, expected[observed.length]);
        observed.push(line);
        if (line === 'started') {
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
    console.log(`Connected stream completed in ${(elapsed / 1000).toFixed(1)} s with a 1500 ms CPU limit`);
  } finally {
    controller.abort();
  }
}

export async function disconnectedRequestLease(cluster) {
  const { base, path } = await prepareLeaseWorker(cluster, 'live-request-disconnect');
  for (const mode of ['before-headers', 'during-body']) {
    const markers = async () => json(await request(base, path + '/markers'), 200, 'read disconnect markers')
      .filter(marker => marker.startsWith(mode + '/')).map(marker => marker.slice(mode.length + 1));
    const client = new AbortController();
    let receivedHeaders = false;
    const result = fetch(base + path + '/' + mode, { signal: AbortSignal.any([client.signal, cluster.signal]) })
      .then(response => { receivedHeaders = true; return response; })
      .catch(error => ({ error }));
    try {
      await eventually(`${mode} registered background work`, async () => {
        assert.deepEqual(await markers(), ['registered']);
      }, { timeout: 8000, interval: 100 });
      const registeredAt = performance.now();
      let reader;
      if (mode === 'during-body') {
        const response = await result;
        assert.equal(response.status, 200);
        reader = response.body.getReader();
        const { value } = await reader.read();
        assert.equal(new TextDecoder().decode(value), 'started');
      }
      // The background marker is scheduled after the original 30 s lease.
      // Give the request four seconds before starting the disconnect drain.
      await delay(4000, undefined, { signal: cluster.signal });
      assert.equal(receivedHeaders, mode === 'during-body');
      client.abort();
      if (reader) {
        await assert.rejects(reader.read(), { name: 'AbortError' });
        reader.releaseLock();
      } else {
        assert.equal((await result).error?.name, 'AbortError');
      }
      await eventually(`${mode} left only the registered work alive`, async () => {
        assert.deepEqual(await markers(), ['background', 'registered']);
      }, { timeout: 31_000, interval: 250 });
      const untilAfterDrain = registeredAt + 42_000 - performance.now();
      if (untilAfterDrain > 0) await delay(untilAfterDrain, undefined, { signal: cluster.signal });
      assert.deepEqual(await markers(), ['background', 'registered'], `${mode} ended all work after its one drain`);
    } finally {
      client.abort();
    }
  }
}

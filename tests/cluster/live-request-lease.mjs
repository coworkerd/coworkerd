import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { get } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { eventually, json, request } from './cluster.mjs';

const serviceWorker = `
  import { DurableObject, RpcTarget, WorkerEntrypoint } from 'cloudflare:workers';

  class Handle extends RpcTarget {
    value() { return 'rpc'; }
  }

  export class DrainMarkers extends DurableObject {
    async fetch(request) {
      const marker = new URL(request.url).pathname.slice(1);
      if (request.method === 'PUT') {
        await this.ctx.storage.put(marker, Date.now());
        return new Response(null, { status: 204 });
      }
      return Response.json(Object.fromEntries(await this.ctx.storage.list()));
    }
  }

  export default class LeaseService extends WorkerEntrypoint {
    async fetch(request) {
      const url = new URL(request.url);
      const path = url.pathname.split('/').at(-1);
      if (!['drain-child', 'drain-markers', 'drain-record'].includes(path)) return new Response('service');
      const ledger = this.env.DRAIN_MARKERS.getByName('response');
      const record = async marker => {
        const response = await ledger.fetch('https://object.test/' + marker, { method: 'PUT' });
        if (response.status !== 204) throw new Error('Could not record ' + marker + ': ' + response.status);
      };
      if (path === 'drain-markers') return ledger.fetch('https://object.test/');
      if (path === 'drain-record') {
        await record(url.searchParams.get('marker'));
        return new Response(null, { status: 204 });
      }
      await record('child-started');
      this.ctx.waitUntil((async () => {
        await scheduler.wait(8_000);
        await record('child-within-drain');
        await scheduler.wait(8_000);
        await record('child-after-drain');
      })());
      return new Response('child-response');
    }
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
          let result;
          try {
            const response = await fetch('http://budget-origin:8080/');
            if (await response.text() !== 'origin') throw new Error('unexpected timer egress response');
            if (this.env.DB && await this.env.DB.prepare('SELECT 41 AS value').first('value') !== 41)
              throw new Error('unexpected timer query result');
            const leaf = await this.env.OBJECT.getByName('leaf').fetch('https://object.test/leaf');
            result = await leaf.text();
          } catch (error) {
            result = error.message;
          }
          await this.ctx.storage.put('timer', result);
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

  function disconnect(request, env, ctx, mode) {
    const ledger = env.OBJECT.getByName('disconnect-markers');
    const record = async marker => {
      const result = await ledger.fetch('https://object.test/' + mode + '/' + marker, { method: 'PUT' });
      if (result.status !== 204) throw new Error('Could not record ' + marker + ': ' + result.status);
    };
    const registered = record('registered');
    const checksSignal = mode === 'checks-signal' || mode === 'normal-close';
    if (mode !== 'no-wait-until' && mode !== 'peer-partition') {
      ctx.waitUntil((async () => {
        await registered;
        await scheduler.wait(32_000);
        await record('background');
        await scheduler.wait(8_000);
        await record('after-drain');
      })());
    }
    if (checksSignal) {
      const aborted = new Promise(resolve => request.signal.addEventListener('abort', resolve, { once: true }));
      ctx.waitUntil(aborted.then(() => record('aborted')));
    }

    const foreground = async () => {
      await registered;
      await scheduler.wait(8_000, checksSignal ? { signal: request.signal } : {});
      await record('foreground');
    };
    if (mode !== 'during-body') return foreground().then(() => new Response('finished'));
    return new Response(new ReadableStream({
      async start(controller) {
        await registered;
        controller.enqueue(new TextEncoder().encode('started'));
        await foreground();
        controller.close();
      },
      cancel() { return record('body-canceled'); },
    }));
  }

  export default {
    fetch(request, env, ctx) {
      const path = new URL(request.url).pathname.split('/').at(-1);
      if (['no-wait-until', 'checks-signal', 'during-body', 'normal-close', 'peer-partition'].includes(path)) {
        return disconnect(request, env, ctx, path);
      }
      if (path === 'markers') return env.OBJECT.getByName('disconnect-markers').fetch('https://object.test/markers');
      if (path === 'timer' || path === 'take-over' || path === 'timer-result') {
        const actor = new URL(request.url).searchParams.get('actor');
        return env.OBJECT.getByName(actor).fetch('https://object.test/' + path);
      }
      if (path === 'many-children') {
        return new Response(new ReadableStream({ async start(controller) {
          const encoder = new TextEncoder();
          const send = message => controller.enqueue(encoder.encode(message + '\\n'));
          send('started');
          for (let batch = 1; batch <= 3; ++batch) {
            for (let i = 0; i < 32; ++i) {
              const response = await env.SERVICE.fetch('https://service.test/');
              if (await response.text() !== 'service') throw new Error('Unexpected child response');
            }
            send(String(batch * 32));
            await scheduler.wait(11_000);
          }
          send('finished');
          controller.close();
        } }), { headers: { 'x-invocation-id': request.cf.invocationId } });
      }
      if (path === 'response-drain') {
        const record = async marker => {
          const response = await env.SERVICE.fetch('https://service.test/drain-record?marker=' + marker);
          if (response.status !== 204) throw new Error('Could not record ' + marker + ': ' + response.status);
        };
        ctx.waitUntil((async () => {
          await scheduler.wait(23_000);
          const child = await env.SERVICE.fetch('https://service.test/drain-child');
          if (await child.text() !== 'child-response') throw new Error('Unexpected nested response');
          await record('root-child-response');
        })());
        ctx.waitUntil((async () => {
          await scheduler.wait(32_000);
          await record('root-within-drain');
          await scheduler.wait(7_000);
          await record('root-after-drain');
        })());
        return new Response(new ReadableStream({ async start(controller) {
          const encoder = new TextEncoder();
          controller.enqueue(encoder.encode('started\\n'));
          await scheduler.wait(5_000);
          controller.enqueue(encoder.encode('finished\\n'));
          controller.close();
        } }), { headers: { 'x-drain-invocation': request.cf.invocationId } });
      }
      if (path === 'decline-upgrade') {
        return scheduler.wait(32_000).then(async () => {
          const response = await env.SERVICE.fetch('https://service.test/');
          return new Response(await response.text(), { status: response.ok ? 403 : response.status });
        });
      }
      if (path === 'socket') {
        const [client, server] = Object.values(new WebSocketPair());
        server.accept();
        server.addEventListener('message', async () => {
          const response = await env.SERVICE.fetch('https://service.test/');
          server.send(await response.text());
        });
        return new Response(null, { status: 101, webSocket: client });
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

async function prepareLeaseWorker(cluster, tenantName, withD1 = false) {
  const tenant = await cluster.tenant(tenantName);
  if (withD1) await cluster.api(`${tenant.path}/databases/timer-db`, { method: 'POST', token: tenant.token });
  const drainMarkers = tenantName === 'live-request-drain' ? {
    wrangler: {
      durable_objects: { bindings: [{ name: 'DRAIN_MARKERS', class_name: 'DrainMarkers' }] },
      migrations: [{ tag: 'v1', new_sqlite_classes: ['DrainMarkers'] }],
    },
  } : {};
  await cluster.deploy(tenant, 'lease-service', serviceWorker, drainMarkers);
  await cluster.deploy(tenant, 'lease-stream', worker, { wrangler: {
    compatibility_flags: ['enable_request_signal'],
    services: [{ binding: 'SERVICE', service: 'lease-service' }],
    durable_objects: { bindings: [{ name: 'OBJECT', class_name: 'LeaseObject' }] },
    migrations: [{ tag: 'v1', new_sqlite_classes: ['LeaseObject'] }],
    ...(withD1 ? { d1_databases: [{ binding: 'DB', database_name: 'timer-db' }] } : {}),
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
  const tenantName = 'live-request-lease';
  const { ingress, base, path } = await prepareLeaseWorker(cluster, tenantName);
  const tenantCpuMs = async () => {
    const states = await Promise.all(cluster.nodes.map(node => cluster.state(node)));
    return states.flatMap(state => Object.values(state.usage.hours))
      .reduce((total, hour) => total + (hour[tenantName]?.cpuMs ?? 0), 0);
  };
  const cpuBefore = await tenantCpuMs();
  const controller = new AbortController();
  const signal = AbortSignal.any([cluster.signal, controller.signal, AbortSignal.timeout(145_000)]);
  try {
    const sentAt = performance.now();
    const response = await new Promise((resolve, reject) => {
      get(base + path + '/stream', { agent: false, signal }, resolve).once('error', reject);
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
    const { usage: { sampleIntervalMs } } = await cluster.state(ingress);
    await delay(2 * sampleIntervalMs, undefined, { signal });
    const cpuMs = await tenantCpuMs() - cpuBefore;
    assert.ok(cpuMs > 0 && cpuMs < 150, `the stream and its children used ${cpuMs.toFixed(2)} ms CPU`);
    console.log(`A connected HTTP client received the complete ${(elapsed / 1000).toFixed(1)} s stream, ` +
      `with ${cpuMs.toFixed(2)} ms of measured tenant CPU across the cluster`);
  } finally {
    controller.abort();
  }
}

export async function responseDrainRequestLease(cluster) {
  const tenantName = 'live-request-drain';
  const { ingress, base, path } = await prepareLeaseWorker(cluster, tenantName);
  const started = performance.now();
  const response = await fetch(base + path + '/response-drain', { signal: AbortSignal.timeout(15_000) });
  assert.equal(response.status, 200);
  assert.equal(await response.text(), 'started\nfinished\n');
  const ended = performance.now();
  const endedAt = Date.now();
  assert.ok(ended - started >= 4_900, 'the client received the delayed end of the body');
  const markers = async () => json(await request(base, `/${tenantName}/lease-service/drain-markers`),
    200, 'read response drain markers');
  const expected = ['child-started', 'child-within-drain', 'root-child-response', 'root-within-drain'];
  const observed = await eventually('both events can make calls late in the shared drain', async () => {
    const result = await markers();
    assert.deepEqual(Object.keys(result).sort(), expected);
    return result;
  }, { timeout: 31_000, interval: 250 });
  const rootMs = observed['root-within-drain'] - endedAt;
  const childMs = observed['child-within-drain'] - endedAt;
  assert.ok(rootMs >= 25_000 && rootMs < 30_000, 'root work ran near the end of the drain');
  assert.ok(childMs >= 24_000 && childMs < 30_000, 'a nested event survived into the same drain');

  const states = () => Promise.all(cluster.nodes.map(node => cluster.state(node)));
  const active = (snapshots, invocation) => snapshots.some(snapshot =>
    snapshot.leases.some(lease => lease.invocationId === invocation.invocationId));
  const snapshots = await states();
  const executions = snapshots.flatMap(state => state.executions);
  const rootId = response.headers.get('x-drain-invocation');
  const executionNode = snapshots.find(node => node.executions.some(event => event.invocationId === rootId));
  assert.ok(executionNode && executionNode.nodeId !== ingress, 'the root ran on a peer');
  const root = executionNode.executions.find(event => event.invocationId === rootId);
  const child = executions.filter(event => event.parentInvocationId === rootId)
    .sort((left, right) => left.callSequence - right.callSequence)[0];
  assert.ok(child && child.treeId === root.treeId, 'the late Service Binding inherited the root tree');
  assert.ok(active(snapshots, root) && active(snapshots, child), 'both native events were still draining');
  const released = await eventually('both native events settled at the ingress cutoff', async () => {
    const snapshots = await states();
    assert.ok(!active(snapshots, root) && !active(snapshots, child));
    return performance.now() - ended;
  }, { timeout: 6000, interval: 100 });
  assert.ok(released >= 29_500 && released <= 32_000, `both events had settled by ${released.toFixed(1)} ms after EOF`);

  await delay(Math.max(0, ended + 37_000 - performance.now()), undefined, { signal: cluster.signal });
  assert.deepEqual(await markers(), observed, 'neither event recorded work after the shared cutoff');
  console.log(`HTTP body ended after ${((ended - started) / 1000).toFixed(1)} s. Root and child recorded work ` +
    `${rootMs} ms and ${childMs} ms after EOF and had both settled by ${released.toFixed(1)} ms after EOF. ` +
    'Both beyond-cutoff writes were absent at +37 s.');
}

export async function upgradeRequestLease(cluster) {
  const { base, path } = await prepareLeaseWorker(cluster, 'live-request-upgrade');
  const signal = AbortSignal.any([cluster.signal, AbortSignal.timeout(50_000)]);
  const url = new URL(base + path + '/socket');
  url.protocol = 'ws:';
  const socket = new WebSocket(url);
  try {
    await once(socket, 'open', { signal });
    const pending = await new Promise((resolve, reject) => {
      const client = get(base + path + '/decline-upgrade', {
        signal, headers: { Connection: 'Upgrade', Upgrade: 'websocket', 'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': randomBytes(16).toString('base64') },
      }, resolve);
      client.once('error', reject);
      client.once('upgrade', (_response, connection) => {
        connection.destroy();
        reject(new Error('the worker should decline the upgrade'));
      });
    });
    let body = '';
    for await (const chunk of pending) body += chunk;
    assert.equal(pending.statusCode, 403, 'a pending upgrade kept its HTTP lease beyond 30 seconds');
    assert.equal(body, 'service');
    const message = once(socket, 'message', { signal });
    socket.send('fetch');
    assert.equal((await message)[0].data, 'service', '101 hands off to the WebSocket message lease');
  } finally {
    socket.close();
  }
}

export async function disconnectedRequestLease(cluster) {
  await checkDisconnects(cluster, 'live-request-disconnect', ['no-wait-until', 'checks-signal', 'during-body']);
}

export async function gracefulCloseRequestLease(cluster) {
  await checkDisconnects(cluster, 'live-request-graceful-close', ['normal-close']);
}

async function checkDisconnects(cluster, tenant, modes) {
  const { ingress, base, path } = await prepareLeaseWorker(cluster, tenant);
  // Use a client on the same network so a Docker port proxy cannot turn a reset into a FIN.
  const network = `${cluster.project}_backend`;
  const script = `
    const client = require('node:http').get(process.argv[1], response => {
      console.log('headers ' + response.statusCode);
      response.on('error', () => {});
      response.once('data', data => console.log(data.toString()));
    });
    client.on('error', () => {});
    process.once('SIGUSR2', () => {
      if (!client.socket || client.socket.destroyed) process.exit(1);
      if (process.argv[2] === 'FIN') client.socket.end();
      else client.socket.resetAndDestroy();
    });
    setInterval(() => {}, 1000);
  `;
  for (const mode of modes) {
    const transport = mode === 'normal-close' ? 'FIN' : 'RST';
    const checksSignal = mode === 'checks-signal' || mode === 'normal-close';
    const markers = async () => json(await request(base, path + '/markers'), 200, 'read disconnect markers')
      .filter(marker => marker.startsWith(mode + '/')).map(marker => marker.slice(mode.length + 1));
    const client = `${cluster.project}-${mode}`;
    await cluster.command('docker', ['run', '--rm', '--detach', '--network', network, '--name', client,
      'node:22-trixie', 'node', '-e', script, `http://${ingress}-peer:8930${path}/${mode}`, transport]);
    try {
      await eventually(`${mode} began execution`, async () => {
        assert.deepEqual(await markers(), ['registered']);
      }, { timeout: 8000, interval: 100 });
      const registeredAt = performance.now();
      if (mode === 'during-body') {
        await eventually('the client received the start of the response', async () => {
          assert.equal(await cluster.command('docker', ['logs', client]), 'headers 200\nstarted');
        }, { timeout: 5000, interval: 100 });
      }
      // The background marker is scheduled after the original 30 s lease.
      // Give the request four seconds before starting the disconnect drain.
      await delay(4000, undefined, { signal: cluster.signal });
      if (mode !== 'during-body') assert.equal(await cluster.command('docker', ['logs', client]), '');
      assert.equal((await cluster.state(ingress)).placement.proxyingIngress, 1);
      const resetAt = performance.now();
      await cluster.command('docker', ['kill', '--signal=SIGUSR2', client]);
      await eventually(`${mode} released the forwarding slot after reset`, async () => {
        assert.equal((await cluster.state(ingress)).placement.proxyingIngress, 0);
      }, { timeout: 1000, interval: 25 });
      console.log(`HTTP ${transport} ${mode} stopped ingress forwarding in ${(performance.now() - resetAt).toFixed(1)} ms`);
      if (mode === 'no-wait-until') {
        await delay(6000, undefined, { signal: cluster.signal });
        assert.deepEqual(await markers(), ['registered'], 'the context stopped with no waitUntil work');
        continue;
      }
      const notification = checksSignal ? 'aborted' : 'body-canceled';
      await eventually(`${mode} notified the Worker of the reset`, async () => {
        assert.deepEqual(await markers(), [notification, 'registered']);
      }, { timeout: 1000, interval: 50 });
      const reachedMs = performance.now() - resetAt;
      assert.ok(reachedMs < 1000, 'workerd was notified promptly');
      console.log(`HTTP ${transport} ${mode} reached workerd in ${reachedMs.toFixed(1)} ms`);
      const withinDrain = checksSignal ? ['aborted', 'background', 'registered'] :
        ['background', 'body-canceled', 'foreground', 'registered'];
      await eventually(`${mode} drained with workerd's cancellation semantics`, async () => {
        assert.deepEqual(await markers(), withinDrain);
      }, { timeout: 31_000, interval: 250 });
      const backgroundMs = performance.now() - resetAt;
      assert.ok(backgroundMs < 30_000, `${mode} recorded background work before the reset drain ended`);
      const untilAfterDrain = registeredAt + 42_000 - performance.now();
      if (untilAfterDrain > 0) await delay(untilAfterDrain, undefined, { signal: cluster.signal });
      assert.deepEqual(await markers(), withinDrain, `${mode} ended all work after its one drain`);
      console.log(`HTTP ${transport} ${mode} recorded waitUntil work by ${backgroundMs.toFixed(1)} ms. ` +
        `The beyond-cutoff write was absent at ${(performance.now() - resetAt).toFixed(1)} ms.`);
    } finally {
      await cluster.command('docker', ['stop', '-t', '1', client], { cleanup: true });
    }
  }
}

export async function boundedChildRequestLeases(cluster) {
  // These two Workers have different homes under the test's placement policy.
  const { base, path } = await prepareLeaseWorker(cluster, 'live-request-drain');
  const state = () => Promise.all(cluster.nodes.map(node => cluster.state(node)));
  const started = performance.now();
  const signal = AbortSignal.any([cluster.signal, AbortSignal.timeout(60_000)]);
  const response = await fetch(base + path + '/many-children', { signal });
  assert.equal(response.status, 200);
  const rootId = response.headers.get('x-invocation-id');
  const executionNode = (await state()).find(node => node.executions.some(event => event.invocationId === rootId));
  assert.ok(executionNode, 'found the connected root');
  const root = executionNode.executions.find(event => event.invocationId === rootId);
  const expected = ['started', '32', '64', '96', 'finished'];
  let index = 0;
  let pending = '';
  for await (const chunk of response.body) {
    const lines = (pending + Buffer.from(chunk).toString()).split('\n');
    pending = lines.pop();
    for (const line of lines) {
      assert.equal(line, expected[index++]);
      if (line === 'started' || line === 'finished') continue;
      await eventually(`leases from the first ${line} completed child calls are gone`, async () => {
        const snapshots = await state();
        const leases = snapshots.flatMap(node => node.leases).filter(lease => lease.treeId === root.treeId);
        assert.deepEqual(leases.map(lease => lease.invocationId), [rootId]);
        const children = snapshots.flatMap(node => node.executions).filter(event => event.parentInvocationId === rootId);
        assert.equal(children.length, Number(line));
        assert.ok(snapshots.some(node => node.nodeId !== executionNode.nodeId &&
          node.executions.some(event => event.parentInvocationId === rootId)), 'a child ran on a second execution node');
      }, { timeout: 3000, interval: 100 });
    }
  }
  assert.equal(pending, '');
  assert.equal(index, expected.length);
  assert.ok(performance.now() - started >= 30_000, 'the root stayed live through the old deadline');
  console.log('Each of three batches returned to one live lease across the cluster while the root remained connected.');
}

export async function peerPartitionRequestLease(cluster) {
  const { ingress, base, path } = await prepareLeaseWorker(cluster, 'live-request-disconnect');
  const waiting = request(base, path + '/peer-partition', { timeout: 15_000 });
  waiting.catch(() => {});
  const markers = async () => json(await request(base, path + '/markers'), 200, 'read peer partition markers')
    .filter(marker => marker.startsWith('peer-partition/'));
  await eventually('the callee ran before its peer session stopped responding', async () => {
    assert.deepEqual(await markers(), ['peer-partition/registered']);
    assert.equal((await cluster.state(ingress)).placement.proxyingIngress, 1);
  });
  const registeredAt = performance.now();
  const snapshots = await Promise.all(cluster.nodes.map(node => cluster.state(node)));
  const executing = snapshots.find(node => node.nodeId !== ingress && node.leases.some(lease =>
    node.executions.some(event => event.invocationId === lease.invocationId && event.kind === 'ingress' &&
      event.target === 'live-request-disconnect/lease-stream@v1')));
  assert.ok(executing, 'the open call is executing on a peer');
  const root = executing.executions.findLast(event => event.kind === 'ingress' &&
    event.target === 'live-request-disconnect/lease-stream@v1' &&
    executing.leases.some(lease => lease.invocationId === event.invocationId));
  const pausedAt = performance.now();
  await cluster.compose(['pause', executing.nodeId]);
  try {
    assert.equal(json(await waiting, 503, 'the open call became uncertain').code, 'RESULT_UNCERTAIN');
    const uncertainMs = performance.now() - pausedAt;
    assert.ok(uncertainMs < 6000, `the unresponsive peer was detected in ${uncertainMs.toFixed(1)} ms`);
    console.log(`A paused peer with an open TCP session returned RESULT_UNCERTAIN in ${uncertainMs.toFixed(1)} ms.`);
  } finally {
    await cluster.compose(['unpause', executing.nodeId], { cleanup: true });
  }
  await eventually('the callee dropped the old invocation after it resumed', async () => {
    assert.ok(!(await cluster.state(executing.nodeId)).leases.some(lease => lease.invocationId === root.invocationId));
    assert.equal((await cluster.state(ingress)).placement.proxyingIngress, 0);
  }, { timeout: 3000, interval: 50 });
  await delay(Math.max(0, registeredAt + 9000 - performance.now()), undefined, { signal: cluster.signal });
  assert.deepEqual(await markers(), ['peer-partition/registered'], 'the canceled request made no further write');
  assert.deepEqual(await request(base, path), { status: 200, text: 'ready' });
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
  const { base, path } = await prepareLeaseWorker(cluster, 'live-request-timers', cluster.threadedNodes.size === 0);
  const call = (event, actor, timeout = 5000) => request(base, `${path}/${event}?actor=${actor}`, { timeout });
  const executions = async () => (await Promise.all(cluster.nodes.map(node => cluster.state(node))))
    .flatMap(state => state.executions).filter(event => event.target === 'live-request-timers/lease-stream@v1');
  for (const actor of ['idle', 'taken-over']) {
    assert.deepEqual(await call('timer', actor), { status: 200, text: 'scheduled' });
  }
  await delay(16_000, undefined, { signal: cluster.signal });
  const previous = new Set((await executions()).map(event => event.invocationId));
  const takeover = call('take-over', 'taken-over', 30_000);
  assert.deepEqual(await takeover, { status: 200, text: 'object' }, 'the newer event can hold the timer work');
  const next = (await executions()).filter(event => !previous.has(event.invocationId));
  const root = next.find(event => event.kind === 'ingress');
  assert.ok(root, 'the newer request has an ingress invocation');
  const actor = next.find(event => event.kind === 'durable' && event.parentInvocationId === root.invocationId);
  assert.ok(actor, 'the newer request invokes the object');
  assert.ok(next.some(event => event.kind === 'durable' && event.parentInvocationId === actor.invocationId &&
    event.treeId === root.treeId), 'the timer inherits the newer event’s live lease');
  assert.deepEqual(await call('timer-result', 'idle'), { status: 200, text: 'object' },
    'an actor timer can make a bounded call with no live ingress event');
  assert.ok(next.some(event => event.kind === 'durable' && event.parentInvocationId === '' &&
    event.treeId !== root.treeId), 'the idle timer opens a fresh tree');
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

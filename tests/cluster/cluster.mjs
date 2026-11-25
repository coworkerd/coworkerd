import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { copyFile, mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const exec = promisify(execFile);
const root = fileURLToPath(new URL('../../', import.meta.url));
const nodes = ['node-1', 'node-2', 'node-3'];
const services = [...nodes, 'apiserver'];

export async function request(base, path, { method = 'GET', token, body, timeout = 5000 } = {}) {
  const headers = token ? { authorization: `Bearer ${token}` } : {};
  if (body !== undefined) headers['content-type'] = 'application/json';
  const response = await fetch(base + path, {
    method, headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeout),
  });
  const text = await response.text();
  return { status: response.status, text };
}

export function json(response, status, description) {
  assert.equal(response.status, status, `${description}: HTTP ${response.status} ${response.text.slice(0, 500)}`);
  return JSON.parse(response.text);
}

// Poll only observations which can legitimately lag. Assertions outside this helper fail immediately.
export async function eventually(description, probe, { timeout = 30000, interval = 200 } = {}) {
  const deadline = performance.now() + timeout;
  let lastError;
  do {
    try {
      return await probe();
    } catch (error) {
      lastError = error;
    }
    await delay(interval);
  } while (performance.now() < deadline);
  throw new Error(`${description} within ${timeout} ms: ${lastError.message}`, { cause: lastError });
}

export class Cluster {
  constructor(directory, kernel, signal, {
    holdPeerAcks = false, threadedNodes = [], membership, confirmTimeoutMs, versionDrainTimeoutMs,
    controlDurableLifecycle = false, delayRecoveryPeers = false, fatalTestBinary, webSocketsPerNode,
  } = {}) {
    this.directory = directory;
    this.kernel = kernel;
    this.signal = signal;
    this.project = `coworkerd-test-${randomBytes(6).toString('hex')}`;
    this.urls = {};
    this.nodes = nodes;
    this.env = {
      ...Object.fromEntries(Object.entries(process.env).filter(([name]) => !/^(COMPOSE_|COWORKERD_|COWCTL_)/.test(name))),
      COWORKERD_VERSION: this.project,
      COWORKERD_HTTP_PORT: '0', COWORKERD_HTTP_PORT_2: '0', COWORKERD_HTTP_PORT_3: '0', COWORKERD_CONTROL_PORT: '0',
      WRANGLER_SEND_METRICS: 'false',
      TMPDIR: directory,
    };
    this.image = `coworkerd:${this.project}`;
    this.override = join(directory, 'compose.json');
    this.peerAckGate = holdPeerAcks ? join(directory, 'peer-acks') : null;
    this.fatalTestBinary = fatalTestBinary;
    this.fatalControl = fatalTestBinary ? join(directory, 'fatal-control') : null;
    this.durableLifecycleGate = controlDurableLifecycle ? join(directory, 'durable-lifecycle') : null;
    this.recoveryRelayGate = delayRecoveryPeers ? join(directory, 'recovery-relay') : null;
    this.threadedNodes = new Set(threadedNodes);
    this.membership = membership;
    this.confirmTimeoutMs = confirmTimeoutMs;
    this.versionDrainTimeoutMs = versionDrainTimeoutMs;
    this.webSocketsPerNode = webSocketsPerNode;
  }

  async command(program, args, { cwd = this.directory, token, cleanup = false, timeout = 60000 } = {}) {
    try {
      return (await exec(program, args, {
        cwd, env: token ? { ...this.env, COWCTL_TOKEN: token } : this.env,
        timeout, signal: cleanup ? undefined : this.signal, maxBuffer: 4 * 1024 * 1024,
      })).stdout.trim();
    } catch (error) {
      throw new Error(`${program} ${args.join(' ')} failed (${error.signal ?? error.code})\n${error.stderr ?? ''}${error.stdout ?? ''}`);
    }
  }

  compose(args, options) {
    return this.command('docker', [
      'compose', '--env-file', '/dev/null', '--project-name', this.project, '--profile', 'cluster',
      '-f', join(root, 'docker/docker-compose.yml'), '-f', this.override, ...args,
    ], options);
  }

  async start() {
    const context = join(this.directory, 'image');
    await Promise.all(['bin', 'docker', 'runtime'].map(dir => mkdir(join(context, dir), { recursive: true })));
    const binaries = process.env.COWORKERD_CLUSTER_BIN_DIR ?? join(root, 'build');
    const files = Object.fromEntries(['coworkerd', 'coworkerd-apiserver', 'coworkerd-launcher']
      .map(file => [`bin/${file}`, join(binaries, file)]));
    for (const file of ['workerd', 'coworkerd-threaded']) files[`bin/${file}`] = join(this.kernel, file);
    if (this.fatalTestBinary) files['bin/coworkerd-threaded'] = this.fatalTestBinary;
    for (const phase of ['startup', 'steady']) {
      const file = `coworkerd-syscalls-${phase}.txt`;
      files[`bin/${file}`] = join(this.kernel, 'allowlist', file);
    }
    for (const file of ['docker/Dockerfile', 'docker/entrypoint.sh', 'runtime/runtime.js']) files[file] = join(root, file);
    await Promise.all(Object.entries(files).map(([file, source]) => copyFile(source, join(context, file))));
    if (this.peerAckGate) await mkdir(this.peerAckGate, { mode: 0o755 });
    if (this.fatalControl) await mkdir(this.fatalControl, { mode: 0o755 });
    if (this.durableLifecycleGate) await mkdir(this.durableLifecycleGate, { mode: 0o755 });
    const overrides = {};
    if (this.recoveryRelayGate) {
      await mkdir(this.recoveryRelayGate, { mode: 0o755 });
      overrides['recovery-relay'] = {
        image: 'node:22-trixie', command: ['node', '/relay.mjs'], networks: ['backend'],
        depends_on: { secrets: { condition: 'service_completed_successfully' } },
        volumes: [
          { type: 'bind', source: join(root, 'tests/cluster/recovery-relay.mjs'), target: '/relay.mjs', read_only: true },
          { type: 'bind', source: this.recoveryRelayGate, target: '/gates', read_only: true },
          { type: 'volume', source: 'secrets', target: '/store-secrets', read_only: true, volume: { subpath: 'apiserver' } },
        ],
      };
    }
    for (const node of nodes) {
      const peer = `${node}-peer:8934`;
      const secrets = '/var/lib/coworkerd-secrets';
      const config = join(this.directory, `${node}.json`);
      const target = '/tmp/cluster-test-node.json';
      await writeFile(config, JSON.stringify({
        nodeId: node, stackDirectory: '/var/lib/coworkerd', listenAddress: '0.0.0.0:8930',
        advertisedAddress: this.recoveryRelayGate ? `recovery-relay:${18001 + nodes.indexOf(node)}` : peer,
        peerListenAddress: peer,
        secrets: { keyFile: `${secrets}/secrets-key.json` },
        peerAuth: { keyFile: `${secrets}/cluster-key.json` },
        storage: { endpoint: 'http://storage:8333', credentialsFile: `${secrets}/server.json` },
        egress: { clusterAddresses: ['apiserver:8940'] },
        // Pin stateless ingress to the receiving node and its sole ordinary process.
        ingressPlacement: false, processPool: { count: 1 },
        ...(node === nodes[0] && this.webSocketsPerNode ? { ingress: { webSocketsPerNode: this.webSocketsPerNode } } : {}),
        ...(this.fatalControl ? { quarantineProcesses: 1 } : {}),
        membership: this.membership,
        ...(this.threadedNodes.has(node) ? {
          durableObjects: {
            threadedStorage: true, checkpointBytes: 65536, confirmTimeoutMs: this.confirmTimeoutMs,
            versionDrainTimeoutMs: this.versionDrainTimeoutMs,
          },
          rotation: { drainTimeoutMs: 3000, termGraceMs: 1000 },
        } : {}),
        ...(this.recoveryRelayGate ? {
          durableObjects: { peerBufferBytes: 1048576, confirmTimeoutMs: 1000, recoverParallelism: 1 },
        } : {}),
      }));
      overrides[node] = {
        ...(this.webSocketsPerNode ? { ulimits: { nofile: { soft: 65536, hard: 65536 } } } : {}),
        // Keep a killed owner down so that only a different node can recover the object.
        restart: 'no',
        environment: { COWORKERD_CONFIG: target },
        volumes: [{ type: 'bind', source: config, target, read_only: true }],
      };
      if (this.threadedNodes.has(node)) overrides[node].environment.COWORKERD_DO_TEST_MAILBOX_EVENTS = '32';
      if (this.durableLifecycleGate && this.threadedNodes.has(node)) {
        const gate = '/tmp/cluster-test-durable-lifecycle';
        overrides[node].environment.COWORKERD_DO_TEST_LIFECYCLE_DIR = gate;
        overrides[node].volumes.push({ type: 'bind', source: this.durableLifecycleGate, target: gate, read_only: true });
      }
      if (this.peerAckGate) {
        const gate = '/tmp/cluster-test-peer-acks';
        overrides[node].environment.COWORKERD_HOLD_PEER_ACK_FOR_TEST = `${gate}/hold`;
        if (this.threadedNodes.has(node)) {
          overrides[node].environment.COWORKERD_CUT_DURABLE_CHANNEL_FOR_TEST = `${gate}/loss-${node}`;
        }
        overrides[node].volumes.push({ type: 'bind', source: this.peerAckGate, target: gate, read_only: true });
      }
      if (this.fatalControl && node === nodes[0]) {
        overrides[node].volumes.push({ type: 'bind', source: this.fatalControl,
          target: '/tmp/coworkerd-fatal-test', read_only: true });
      }
    }
    await writeFile(this.override, JSON.stringify({ services: overrides }));
    console.log(`Building ${this.image}`);
    await this.command('docker', ['build', '--tag', this.image, '-f', join(context, 'docker/Dockerfile'), context], { timeout: 240000 });
    this.built = true;
    console.log(`Starting Compose project ${this.project}`);
    this.started = true;
    await this.compose(['up', '-d', '--wait', '--wait-timeout', '120'], { timeout: 180000 });
    await Promise.all(services.map(service => this.refreshUrl(service)));
    this.admin = await this.compose(['exec', '-T', 'apiserver', 'coworkerd-entrypoint', 'tool', 'admin-credential']);
    this.management = await this.compose(['exec', '-T', 'apiserver', 'coworkerd-entrypoint', 'tool', 'token']);
    await eventually('three registered nodes', async () => {
      const status = await this.api('/v1/status');
      assert.deepEqual(status.nodes.map(node => node.nodeId).sort(), nodes);
    });
  }

  async refreshUrl(service) {
    const address = await this.compose(['port', service, service === 'apiserver' ? '8940' : '8930']);
    assert.match(address, /^127\.0\.0\.1:\d+$/);
    this.urls[service] = `http://${address}`;
  }

  async api(path, options = {}) {
    const { token = this.admin, status = 200, ...requestOptions } = options;
    return json(await request(this.urls.apiserver, path, { ...requestOptions, token }), status, path);
  }

  async state(node) {
    return json(await request(this.urls[node], '/__coworkerd/state', { token: this.management }), 200, `${node} state`);
  }

  async releaseShardForRecoveryTest(namespaceId, { nodeId, nodeEpoch, shard, epoch }) {
    assert.ok(this.recoveryRelayGate);
    assert.match(namespaceId, /^[a-f0-9]{64}$/);
    await this.compose(['exec', '-T', 'recovery-relay', 'node', '/relay.mjs', '--release',
      namespaceId, nodeId, String(nodeEpoch), String(shard), String(epoch)]);
  }

  async tenant(name, quotas) {
    await this.api('/v1/tenants', { method: 'POST', body: { name, quotas } });
    const path = `/v1/tenants/${name}`;
    const { credential: token } = await this.api(`${path}/credentials`, { method: 'POST', body: {} });
    assert.ok(token, 'tenant credential was issued');
    return { name, path, token };
  }

  async deploy(tenant, name, source, { version = 'v1', wrangler = {} } = {}) {
    const cwd = join(this.directory, `${tenant.name}-${name}`);
    await mkdir(cwd, { recursive: true });
    await Promise.all([
      writeFile(join(cwd, 'index.js'), source),
      writeFile(join(cwd, 'wrangler.json'), JSON.stringify({
        name, main: 'index.js', compatibility_date: '2026-10-01', ...wrangler,
      })),
      writeFile(join(cwd, 'cowctl.json'), JSON.stringify({
        tenant: tenant.name, controlPlane: this.urls.apiserver, ingress: this.urls[nodes[0]], worker: name,
      })),
    ]);
    console.log(await this.command(process.execPath, [join(root, 'cli/bin/cowctl.mjs'), 'deploy', '--version', version], {
      cwd, token: tenant.token,
    }));
  }

  async diagnostics() {
    if (!this.started) return;
    for (const args of [['ps', '-a'], ['logs', '--no-color', '--tail', '40']]) {
      try {
        console.error(await this.compose(args, { cleanup: true, timeout: 10000 }));
      } catch (error) {
        console.error(error.message);
      }
    }
  }

  async reportMemory() {
    try {
      const running = await this.compose(['ps', '-q', '--status', 'running']);
      const usage = await this.command('docker', [
        'stats', '--no-stream', '--format', '{{.Name}} {{.MemUsage}}', ...running.split(/\s+/).filter(Boolean),
      ], { timeout: 10000 });
      console.log(`Docker memory before cases:\n${usage}`);
    } catch (error) {
      console.warn(`Docker memory sample unavailable: ${error.message}`);
    }
  }

  async close() {
    try {
      if (this.started) await this.compose(['down', '--volumes', '--remove-orphans', '--timeout', '5'], { cleanup: true });
    } finally {
      try {
        if (this.built) await this.command('docker', ['image', 'rm', this.image], { cleanup: true });
      } finally {
        await rm(this.directory, { recursive: true, force: true });
      }
    }
  }
}

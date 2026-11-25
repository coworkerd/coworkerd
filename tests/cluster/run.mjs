import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { cases, durableGatedCases, fatalAttribution, webSocketCases } from './cases.mjs';
import { Cluster } from './cluster.mjs';
import { alarmRequestLease, actorRequestLease, connectedRequestLease, disconnectedRequestLease, silentRequestLease }
  from './live-request-lease.mjs';
import { liveRequestCapacity } from './live-request-load.mjs';

const leaseCases = {
  'live-request-lease': connectedRequestLease,
  'disconnected-request-lease': disconnectedRequestLease,
  'silent-request-lease': silentRequestLease,
};
const actorLeaseCases = { 'actor-request-lease': [], 'threaded-actor-request-lease': ['node-1', 'node-2', 'node-3'] };
const alarmCases = { 'alarm-wall-time': [], 'threaded-alarm-wall-time': ['node-1', 'node-2', 'node-3'] };
const requestLeaseNode = () => ({
  ingressPlacement: true, placement: { homeProbability: 1 },
  ingress: { tcpKeepalive: { idleSeconds: 1, intervalSeconds: 1, probes: 2 } },
});

async function runSuite(selected, kernel, signal, options) {
  const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-cluster-')), kernel, signal, options);
  let failed = 0;
  try {
    await cluster.start();
    await cluster.reportMemory();
    for (const [name, test] of selected) {
      signal.throwIfAborted();
      const started = performance.now();
      console.log(`RUN ${name}`);
      try {
        await test(cluster);
        console.log(`PASS ${name} (${((performance.now() - started) / 1000).toFixed(1)} s)`);
      } catch (error) {
        ++failed;
        console.error(`FAIL ${name}\n${error.stack}`);
      }
    }
    if (failed) await cluster.diagnostics();
    signal.throwIfAborted();
    return failed;
  } catch (error) {
    await cluster.diagnostics();
    throw error;
  } finally {
    await cluster.close();
  }
}

async function main(args) {
  const all = { ...cases, ...webSocketCases, ...durableGatedCases, ...leaseCases,
    'actor-request-lease': actorRequestLease, 'threaded-actor-request-lease': actorRequestLease,
    'alarm-wall-time': alarmRequestLease, 'threaded-alarm-wall-time': alarmRequestLease,
    'live-request-lease-capacity': liveRequestCapacity, 'fatal-attribution': fatalAttribution };
  const help = `Usage: node tests/cluster/run.mjs <kernel-directory> [${Object.keys(all).join('|')}]`;
  if (args[0] === '--help' && args.length === 1) return console.log(help);
  if (args.length !== 1 && (args.length !== 2 || !Object.hasOwn(all, args[1]))) {
    console.error(help);
    process.exitCode = 2;
    return;
  }
  const controller = new AbortController();
  for (const signal of ['SIGTERM', 'SIGINT']) {
    process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
  }
  const start = performance.now();
  const kernel = resolve(args[0]);
  const fatalTest = args[1] === 'fatal-attribution';
  const testBinary = process.env.COWORKERD_CLUSTER_FATAL_TEST_BINARY;
  if (fatalTest && !testBinary) {
    throw new Error('fatal-attribution requires COWORKERD_CLUSTER_FATAL_TEST_BINARY');
  }
  const regular = fatalTest ? [['fatal-attribution', fatalAttribution]]
    : Object.entries(cases).filter(([name]) => !args[1] || args[1] === name);
  const webSockets = Object.entries(webSocketCases).filter(([name]) => !args[1] || args[1] === name);
  const requestLeases = Object.entries(leaseCases).filter(([name]) => !args[1] || args[1] === name);
  const actorLeases = Object.entries(actorLeaseCases).filter(([name]) => !args[1] || args[1] === name);
  const capacity = args[1] === 'live-request-lease-capacity';
  const alarms = Object.entries(alarmCases).filter(([name]) => !args[1] || args[1] === name);
  const gated = Object.entries(durableGatedCases).filter(([name]) => !args[1] || args[1] === name);
  let failed = 0;
  try {
    if (regular.length) {
      failed += await runSuite(regular, kernel, controller.signal, {
        fatalTestBinary: fatalTest ? resolve(testBinary) : undefined,
      });
    }
    if (webSockets.length) {
      failed += await runSuite(webSockets, kernel, controller.signal, { webSocketsPerNode: 2 });
    }
    if (requestLeases.length) {
      failed += await runSuite(requestLeases, kernel, controller.signal, {
        configureNode: requestLeaseNode,
      });
    }
    for (const [name, threadedNodes] of actorLeases) {
      failed += await runSuite([[name, actorRequestLease]], kernel, controller.signal, {
        threadedNodes, configureNode: requestLeaseNode,
        budgetLifetime: { tenant: 'live-request-timers', leaseMs: 30_000 },
      });
    }
    if (capacity) {
      failed += await runSuite([['live-request-lease-capacity', liveRequestCapacity]], kernel, controller.signal, {
        configureNode: node => node === 'node-3' ? {} : ({
          ingressPlacement: true,
          placement: { homeProbability: 1, rootLimit: 5400, nestedLimit: 5500 },
          processPool: { count: 1, inFlightLimit: 5400 },
          quarantineProcesses: 0, durableObjects: { processes: 0 },
        }),
      });
    }
    for (const [name, threadedNodes] of alarms) {
      failed += await runSuite([[name, alarmRequestLease]], kernel, controller.signal,
        { threadedNodes, nodeEnvironment: () => ({ COWORKERD_TEST_ALARM_WALL_MS: '1000' }) });
    }
    for (const [name, { test, nodes, membership, confirmTimeoutMs, versionDrainTimeoutMs, controlDurableLifecycle,
      delayRecoveryPeers, budgetLifetime }] of gated) {
      failed += await runSuite([[name, test]], kernel, controller.signal, {
        threadedNodes: nodes, holdPeerAcks: true, membership, confirmTimeoutMs, versionDrainTimeoutMs,
        controlDurableLifecycle, delayRecoveryPeers, budgetLifetime,
      });
    }
    const executed = regular.length + webSockets.length + requestLeases.length + actorLeases.length + gated.length +
      alarms.length + Number(capacity);
    console.log(`${executed} executed, ${failed} failed`);
  } finally {
    console.log(`Cluster run including setup and cleanup: ${((performance.now() - start) / 1000).toFixed(1)} s`);
  }
  if (failed) process.exitCode = 1;
}

main(process.argv.slice(2)).catch(error => {
  console.error(error.stack);
  process.exitCode = 1;
});

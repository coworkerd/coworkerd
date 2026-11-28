import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { cases, durableGatedCases, fatalAttribution, webSocketCases } from './cases.mjs';
import { Cluster } from './cluster.mjs';
import { configureEgressLimits, egressLimits } from './egress-limits.mjs';
import { ingressPlacementDefault, ingressPlacementEnabled, ingressPlacementSaturation, ingressPlacementAllBusy,
  ingressPlacementUnrelatedStreams, ingressPlacementLiveSaturation, placementStreamWorker } from './ingress-placement.mjs';
import { isolateBusy } from './isolate-busy.mjs';
import { alarmRequestLease, actorRequestLease, boundedChildRequestLeases, connectedRequestLease,
  disconnectedRequestLease, gracefulCloseRequestLease, peerPartitionRequestLease, responseDrainRequestLease,
  silentRequestLease, upgradeRequestLease }
  from './live-request-lease.mjs';
import { liveRequestCapacity } from './live-request-load.mjs';

const leaseCases = {
  'live-request-lease': connectedRequestLease,
  'upgrade-request-lease': upgradeRequestLease,
  'disconnected-request-lease': disconnectedRequestLease,
  'response-drain-request-lease': responseDrainRequestLease,
  'silent-request-lease': silentRequestLease,
  'graceful-close-request-lease': gracefulCloseRequestLease,
  'bounded-child-request-leases': boundedChildRequestLeases,
  'peer-partition-request-lease': peerPartitionRequestLease,
};
const actorLeaseCases = { 'actor-request-lease': [], 'threaded-actor-request-lease': ['node-1', 'node-2', 'node-3'] };
const alarmCases = { 'alarm-wall-time': [], 'threaded-alarm-wall-time': ['node-1', 'node-2', 'node-3'] };
const requestLeaseNode = () => ({
  ingressPlacement: true, placement: { homeProbability: 1, subcalls: 'home' },
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
        await cluster.diagnostics(name, error);
      }
    }
    signal.throwIfAborted();
    return failed;
  } catch (error) {
    await cluster.diagnostics('suite', error);
    throw error;
  } finally {
    await cluster.close();
  }
}

function defaultSuites() {
  const regularCases = { 'ingress-placement-default': ingressPlacementDefault, ...cases };
  const placementCases = {
    'ingress-placement': { test: ingressPlacementEnabled },
    'isolate-busy': { test: isolateBusy },
    'ingress-placement-saturation': { test: ingressPlacementSaturation, isolatePermilleOnThird: 899, rootLimit: 1 },
    'ingress-placement-all-busy': { test: ingressPlacementAllBusy, isolatePermilleOnThird: 900, rootLimit: 2 },
    'ingress-placement-unrelated-streams': {
      test: ingressPlacementUnrelatedStreams, isolatePermilleOnThird: 900, rootLimit: 40,
      placements: [{ workerId: placementStreamWorker, nodeId: 'node-3' }],
    },
    'ingress-placement-live-saturation': { test: ingressPlacementLiveSaturation },
  };
  return [
    // Keep the shared projects together, including the owner kill at the end of the regular group.
    { selected: Object.entries(regularCases) },
    { selected: Object.entries(webSocketCases), options: { webSocketsPerNode: 2 } },
    ...Object.entries(durableGatedCases).map(([name, { test, nodes, membership, confirmTimeoutMs, versionDrainTimeoutMs,
      controlDurableLifecycle, delayRecoveryPeers, budgetLifetime, platformFixture, databaseAlarmFixture,
      publishPeerPorts, configureNode, nodeEnvironment, singleNode, holdPeerAcks = true }]) => {
      const previousProduct = name === 'durable-threaded-database-placement' && process.env.COWORKERD_CLUSTER_PREVIOUS_PRODUCT;
      const previousKernel = previousProduct && process.env.COWORKERD_CLUSTER_PREVIOUS_KERNEL;
      return {
        selected: [[name, test]],
        options: {
          threadedNodes: nodes, holdPeerAcks, membership, confirmTimeoutMs, versionDrainTimeoutMs,
          controlDurableLifecycle, delayRecoveryPeers, budgetLifetime, platformFixture,
          databaseAlarmFixture, publishPeerPorts, configureNode, nodeEnvironment, singleNode,
          previousProduct: previousProduct ? resolve(previousProduct) : undefined,
          previousKernel: previousKernel ? resolve(previousKernel) : undefined,
        },
      };
    }),
    ...Object.entries(placementCases).map(([name, { test, isolatePermilleOnThird, rootLimit, placements }]) => ({
      selected: [[name, test]],
      options: {
        configureNode: () => ({
          ingressPlacement: true,
          ...(placements && { placements }),
          ...(isolatePermilleOnThird && { initialPlacementHops: 3, peerTimeoutMs: 1200 }),
          placement: { homeProbability: 1, ...(rootLimit && { rootLimit }) },
        }),
        nodeEnvironment: isolatePermilleOnThird ? node => ({
          COWORKERD_WORKER_BUSY_PERMILLE_FOR_TEST: { 'node-1': 900, 'node-2': 899, 'node-3': isolatePermilleOnThird }[node],
          COWORKERD_NODE_CPU_PERMILLE_FOR_TEST: node === 'node-2' ? 950 : 949,
        }) : undefined,
      },
    })),
    { selected: [['egress-limits', egressLimits]], options: { configureNode: configureEgressLimits } },
    ...Object.entries(leaseCases).map(([name, test]) => ({
      selected: [[name, test]], options: {
        configureNode: requestLeaseNode,
        ...(name === 'bounded-child-request-leases' && {
          platformFixture: () => [],
          nodeEnvironment: () => ({ COWORKERD_TEST_FAIL_CHILD_OPEN: 'live-request-drain/lease-open-failure' }),
        }),
      },
    })),
    ...Object.entries(actorLeaseCases).map(([name, threadedNodes]) => ({
      selected: [[name, actorRequestLease]],
      options: {
        threadedNodes, configureNode: requestLeaseNode,
        budgetLifetime: { tenant: 'live-request-timers', leaseMs: 30_000 },
      },
    })),
    ...Object.entries(alarmCases).map(([name, threadedNodes]) => ({
      selected: [[name, alarmRequestLease]],
      options: { threadedNodes, alarmWallMs: 1000 },
    })),
  ];
}

async function main(args) {
  let suites = defaultSuites();
  const names = suites.flatMap(({ selected }) => selected.map(([name]) => name));
  const help = [
    'Usage: node tests/cluster/run.mjs <kernel-directory> [case-name | --shard INDEX/COUNT]',
    '       node tests/cluster/run.mjs --list [--shard INDEX/COUNT]',
    `Cases: ${[...names, 'fatal-attribution', 'live-request-lease-capacity'].join('|')}`,
  ].join('\n');
  if (args[0] === '--help' && args.length === 1) return console.log(help);
  const invalid = () => {
    console.error(help);
    process.exitCode = 2;
  };
  const [kernelDirectory, selection, shard] = args;
  const list = kernelDirectory === '--list';
  if (!kernelDirectory) return invalid();
  if (selection === '--shard' && args.length === 3) {
    const match = /^([1-9]\d*)\/([1-9]\d*)$/.exec(shard);
    if (!match) return invalid();
    const index = Number(match[1]);
    const count = Number(match[2]);
    if (index > count || count > suites.length) return invalid();
    // Each project belongs to exactly one shard. Preserve the sequence within each project.
    suites = suites.filter((_, i) => i % count === index - 1);
  } else if (args.length === 2 && !list && names.includes(selection)) {
    const suite = suites.find(({ selected }) => selected.some(([name]) => name === selection));
    suites = [{ ...suite, selected: suite.selected.filter(([name]) => name === selection) }];
  } else if (args.length === 2 && !list && selection === 'live-request-lease-capacity') {
    suites = [{
      selected: [[selection, liveRequestCapacity]],
      options: {
        configureNode: node => node === 'node-3' ? {} : ({
          ingressPlacement: true,
          placement: { homeProbability: 1, rootLimit: 5400, nestedLimit: 5500 },
          processPool: { count: 1, inFlightLimit: 5400 },
          quarantineProcesses: 0, durableObjects: { processes: 0 },
        }),
      },
    }];
  } else if (args.length === 2 && !list && selection === 'fatal-attribution') {
    const testBinary = process.env.COWORKERD_CLUSTER_FATAL_TEST_BINARY;
    if (!testBinary) throw new Error('fatal-attribution requires COWORKERD_CLUSTER_FATAL_TEST_BINARY');
    suites = [{ selected: [['fatal-attribution', fatalAttribution]], options: { fatalTestBinary: resolve(testBinary) } }];
  } else if (args.length !== 1) {
    return invalid();
  }
  const selectedNames = suites.flatMap(({ selected }) => selected.map(([name]) => name));
  if (list) return console.log(selectedNames.join('\n'));

  const controller = new AbortController();
  for (const signal of ['SIGTERM', 'SIGINT']) {
    process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
  }
  const start = performance.now();
  const kernel = resolve(kernelDirectory);
  let failed = 0;
  try {
    if (shard) console.log(`Cluster shard ${shard}: ${selectedNames.join(', ')}`);
    for (const { selected, options } of suites) {
      controller.signal.throwIfAborted();
      failed += await runSuite(selected, kernel, controller.signal, options);
    }
    console.log(`${selectedNames.length} executed, ${failed} failed`);
  } finally {
    console.log(`Cluster run including setup and cleanup: ${((performance.now() - start) / 1000).toFixed(1)} s`);
  }
  if (failed) process.exitCode = 1;
}

main(process.argv.slice(2)).catch(error => {
  console.error(error.stack);
  process.exitCode = 1;
});

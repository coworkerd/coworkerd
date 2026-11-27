import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { cases, durableGatedCases, fatalAttribution, webSocketCases } from './cases.mjs';
import { Cluster } from './cluster.mjs';
import { ingressPlacementDefault, ingressPlacementEnabled, ingressPlacementSaturation, ingressPlacementAllBusy } from './ingress-placement.mjs';

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

async function main(args) {
  const regularCases = { 'ingress-placement-default': ingressPlacementDefault, ...cases };
  const placementCases = {
    'ingress-placement': { test: ingressPlacementEnabled },
    'ingress-placement-saturation': { test: ingressPlacementSaturation, isolatePermilleOnThird: 899 },
    'ingress-placement-all-busy': { test: ingressPlacementAllBusy, isolatePermilleOnThird: 900 },
  };
  const all = { ...regularCases, ...placementCases, ...webSocketCases, ...durableGatedCases,
    'fatal-attribution': fatalAttribution };
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
    : Object.entries(regularCases).filter(([name]) => !args[1] || args[1] === name);
  const placement = Object.entries(placementCases).filter(([name]) => !args[1] || args[1] === name);
  const webSockets = Object.entries(webSocketCases).filter(([name]) => !args[1] || args[1] === name);
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
    for (const [name, { test, nodes, membership, confirmTimeoutMs, versionDrainTimeoutMs, controlDurableLifecycle,
      delayRecoveryPeers, budgetLifetime, platformFixture, databaseAlarmFixture,
      publishPeerPorts, configureNode, nodeEnvironment, singleNode, holdPeerAcks = true }] of gated) {
      const previousProduct = name === 'durable-threaded-database-placement' && process.env.COWORKERD_CLUSTER_PREVIOUS_PRODUCT;
      const previousKernel = previousProduct && process.env.COWORKERD_CLUSTER_PREVIOUS_KERNEL;
      failed += await runSuite([[name, test]], kernel, controller.signal, {
        threadedNodes: nodes, holdPeerAcks, membership, confirmTimeoutMs, versionDrainTimeoutMs,
        controlDurableLifecycle, delayRecoveryPeers, budgetLifetime, platformFixture,
        databaseAlarmFixture, publishPeerPorts, configureNode, nodeEnvironment, singleNode,
        previousProduct: previousProduct ? resolve(previousProduct) : undefined,
        previousKernel: previousKernel ? resolve(previousKernel) : undefined,
      });
    }
    for (const [name, { test, isolatePermilleOnThird }] of placement) {
      failed += await runSuite([[name, test]], kernel, controller.signal, {
        configureNode: () => ({
          ingressPlacement: true,
          ...(isolatePermilleOnThird && { initialPlacementHops: 3, peerTimeoutMs: 1200 }),
          placement: { homeProbability: 1, ...(isolatePermilleOnThird && { rootLimit: 1 }) },
        }),
        nodeEnvironment: node => ({
          COWORKERD_WORKER_BUSY_PERMILLE_FOR_TEST: isolatePermilleOnThird
            ? { 'node-1': 900, 'node-2': 899, 'node-3': isolatePermilleOnThird }[node] : 0,
          COWORKERD_NODE_CPU_PERMILLE_FOR_TEST: isolatePermilleOnThird && node === 'node-2' ? 950 : 949,
        }),
      });
    }
    console.log(`${regular.length + webSockets.length + gated.length + placement.length} executed, ${failed} failed`);
  } finally {
    console.log(`Cluster run including setup and cleanup: ${((performance.now() - start) / 1000).toFixed(1)} s`);
  }
  if (failed) process.exitCode = 1;
}

main(process.argv.slice(2)).catch(error => {
  console.error(error.stack);
  process.exitCode = 1;
});

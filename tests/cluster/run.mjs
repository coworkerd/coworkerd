import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { cases, durableGatedCases } from './cases.mjs';
import { Cluster } from './cluster.mjs';

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
  const all = { ...cases, ...durableGatedCases };
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
  const regular = Object.entries(cases).filter(([name]) => !args[1] || args[1] === name);
  const gated = Object.entries(durableGatedCases).filter(([name]) => !args[1] || args[1] === name);
  let failed = 0;
  try {
    if (regular.length) {
      failed += await runSuite(regular, kernel, controller.signal);
    }
    for (const [name, { test, nodes, membership, confirmTimeoutMs }] of gated) {
      failed += await runSuite([[name, test]], kernel, controller.signal, {
        threadedNodes: nodes, holdPeerAcks: true, membership, confirmTimeoutMs,
      });
    }
    console.log(`${regular.length + gated.length} executed, ${failed} failed`);
  } finally {
    console.log(`Cluster run including setup and cleanup: ${((performance.now() - start) / 1000).toFixed(1)} s`);
  }
  if (failed) process.exitCode = 1;
}

main(process.argv.slice(2)).catch(error => {
  console.error(error.stack);
  process.exitCode = 1;
});

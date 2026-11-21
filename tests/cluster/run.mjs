import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { cases } from './cases.mjs';
import { Cluster } from './cluster.mjs';

async function main(args) {
  const help = `Usage: node tests/cluster/run.mjs <kernel-directory> [${Object.keys(cases).join('|')}]`;
  if (args[0] === '--help' && args.length === 1) return console.log(help);
  if (args.length !== 1 && (args.length !== 2 || !Object.hasOwn(cases, args[1]))) {
    console.error(help);
    process.exitCode = 2;
    return;
  }
  const selected = args[1] ? [[args[1], cases[args[1]]]] : Object.entries(cases);
  const controller = new AbortController();
  for (const signal of ['SIGTERM', 'SIGINT']) {
    process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
  }
  const start = performance.now();
  const cluster = new Cluster(await mkdtemp(join(tmpdir(), 'coworkerd-cluster-')), resolve(args[0]), controller.signal);
  let failed = 0;
  try {
    await cluster.start();
    await cluster.reportMemory();
    for (const [name, test] of selected) {
      controller.signal.throwIfAborted();
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
    controller.signal.throwIfAborted();
    console.log(`${selected.length} executed, ${failed} failed`);
  } catch (error) {
    await cluster.diagnostics();
    throw error;
  } finally {
    await cluster.close();
    console.log(`Cluster run including setup and cleanup: ${((performance.now() - start) / 1000).toFixed(1)} s`);
  }
  if (failed) process.exitCode = 1;
}

main(process.argv.slice(2)).catch(error => {
  console.error(error.stack);
  process.exitCode = 1;
});

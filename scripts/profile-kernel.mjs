// Run the cluster workloads on the instrumented threaded executable, the only one a node starts. Keep the strace
// process separate so the node still reports the executable's PID in its lifecycle log.
import { appendFile, chmod, mkdir, mkdtemp, readFile, rename, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const [kernel, output] = process.argv.slice(2).map(path => resolve(path));
const { Cluster } = await import(pathToFileURL(resolve('tests/cluster/cluster.mjs')));
const { cases } = await import(pathToFileURL(resolve('tests/cluster/cases.mjs')));
const programs = ['coworkerd-threaded'];
const wrapper = `#!/bin/sh
set -eu
binary=$(basename "$0")
target=/opt/coworkerd/bin/profile-target/$binary
if [ "\${1:-}" = --sandbox-facts ]; then
  exec "$target" "$@"
fi
exec strace -D -f -ttt -qq -yy -s 200 -o "/profiles/$binary-$$.trace" "$target" "$@"
`;

class ProfiledCluster extends Cluster {
  async command(program, args, options) {
    if (program === 'docker' && args[0] === 'build') {
      const context = args.at(-1);
      const bin = join(context, 'bin');
      const targets = join(bin, 'profile-target');
      await mkdir(targets);
      for (const executable of programs) {
        await rename(join(bin, executable), join(targets, executable));
        await writeFile(join(bin, executable), wrapper, { mode: 0o755 });
      }
      await appendFile(join(context, 'docker/Dockerfile'), `
USER root
RUN apt-get update && apt-get install -y --no-install-recommends strace && rm -rf /var/lib/apt/lists/*
COPY bin/profile-target/ /opt/coworkerd/bin/profile-target/
USER coworkerd:coworkerd
`);
    }
    return super.command(program, args, options);
  }

  async compose(args, options) {
    if (args[0] === 'up') {
      const override = JSON.parse(await readFile(this.override, 'utf8'));
      for (const node of this.nodes) {
        const directory = join(output, node);
        await mkdir(directory);
        await chmod(directory, 0o777);
        const service = override.services[node];
        const configPath = service.volumes[0].source;
        const config = JSON.parse(await readFile(configPath, 'utf8'));
        config.sandbox = { mode: 'off' };
        // The default call ceiling follows the runner's memory, and a process starts one thread for each
        // slot. Under strace that takes longer than the readiness check allows. The calls a process makes
        // do not depend on the number of slots, so the profile names the ceiling unless a case does.
        if (config.processPool?.inFlightLimit === undefined) {
          config.processPool = { ...config.processPool, inFlightLimit: 512 };
        }
        await writeFile(configPath, JSON.stringify(config));
        service.volumes.push({ type: 'bind', source: directory, target: '/profiles' });
      }
      await writeFile(this.override, JSON.stringify(override));
    }
    return super.compose(args, options);
  }
}

await mkdir(output, { recursive: true });
const controller = new AbortController();
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.once(signal, () => controller.abort(new Error(`Interrupted by ${signal}`)));
}
const cluster = new ProfiledCluster(await mkdtemp(join(tmpdir(), 'coworkerd-profile-')), kernel, controller.signal);
const windows = [];
try {
  await cluster.start();
  for (const [name, test] of Object.entries(cases)) {
    controller.signal.throwIfAborted();
    const startUnixMs = Date.now();
    await test(cluster);
    windows.push({ name, startUnixMs, endUnixMs: Date.now() });
    console.log(`PASS profile workload ${name}`);
  }
  for (const node of cluster.nodes) {
    const logs = await cluster.compose(['logs', '--no-color', '--no-log-prefix', node], { cleanup: true });
    await writeFile(join(output, node, 'node.log'), logs);
  }
  await writeFile(join(output, 'windows.json'), JSON.stringify(windows));
} catch (error) {
  await cluster.diagnostics();
  throw error;
} finally {
  await cluster.close();
}

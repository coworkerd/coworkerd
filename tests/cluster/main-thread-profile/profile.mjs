import assert from 'node:assert/strict';
import { appendFile, copyFile, readFile, rm, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { eventually } from '../cluster.mjs';
import { recordPerf, summarizeStacks } from './perf.mjs';

export const traceInContainer = '/tmp/coworkerd-benchmark-trace.bin';

function weightedStats(durationByCount, total) {
  const order = [...durationByCount.entries()].sort(([a], [b]) => a - b);
  const average = order.reduce((sum, [count, duration]) => sum + count * duration, 0) / total;
  function percentile(fraction) {
    let cumulative = 0;
    for (const [count, duration] of order) {
      cumulative += duration;
      if (cumulative >= total * fraction) return count;
    }
    throw new Error('occupancy trace did not cover the window');
  }
  return { mean: average, p95: percentile(0.95), p99: percentile(0.99), max: order.at(-1)[0] };
}

export function summarizeTrace(bytes, windowNs) {
  const [begin, end] = windowNs.map(BigInt);
  const read = offset => Number(bytes.readBigUInt64LE(offset));
  const count = read(8);
  const capacity = read(16);
  assert.equal(read(0), 1, 'trace format version');
  assert.equal(bytes.length, 24 + capacity * 32, 'trace file size');
  const durations = [new Map(), new Map(), new Map()];
  const flush = { calls: 0, elapsedNs: 0, cpuNs: 0, maxElapsedNs: 0 };
  let previous;
  let peak = { roots: 0, nested: 0, total: 0 };
  const rootArrivals = new Map();
  let arrivals = 0;
  function addInterval(until) {
    assert.ok(previous, 'trace wrapped within the timed window');
    const [timestamp, roots, nested] = previous;
    const start = timestamp < begin ? begin : timestamp;
    const finish = until > end ? end : until;
    const ns = Number(finish - start);
    if (ns <= 0) return;
    const total = roots + nested;
    if (total > peak.total) peak = { roots, nested, total };
    for (const [index, value] of [roots, nested, total].entries()) {
      const distribution = durations[index];
      distribution.set(value, (distribution.get(value) ?? 0) + ns);
    }
  }
  for (let event = Math.max(0, count - capacity); event < count; ++event) {
    const offset = 24 + (event % capacity) * 32;
    const timestamp = bytes.readBigUInt64LE(offset);
    const kind = read(offset + 8);
    const first = read(offset + 16);
    const second = read(offset + 24);
    if (kind === 0) {
      if (timestamp >= end) break;
      if (timestamp >= begin) addInterval(timestamp);
      if (previous) {
        const [previousTime, roots, nested] = previous;
        assert.ok(timestamp >= previousTime, 'admission timestamps must be ordered');
        if (timestamp >= begin && first === roots + 1) {
          assert.equal(second, nested, 'only one admission slot changes at a time');
          const active = roots + nested;
          rootArrivals.set(active, (rootArrivals.get(active) ?? 0) + 1);
          ++arrivals;
        }
      }
      previous = [timestamp, first, second];
    } else {
      assert.equal(kind, 1, 'trace event kind');
      if (timestamp <= end && timestamp - BigInt(first) >= begin) {
        ++flush.calls;
        flush.elapsedNs += first;
        flush.cpuNs += second;
        flush.maxElapsedNs = Math.max(flush.maxElapsedNs, first);
      }
    }
  }
  addInterval(end);
  const window = Number(end - begin);
  for (const distribution of durations)
    assert.equal([...distribution.values()].reduce((a, b) => a + b, 0), window);
  const [roots, nested, total] = durations.map(distribution => weightedStats(distribution, window));
  const atRootAdmission = { count: arrivals, ...weightedStats(rootArrivals, arrivals) };
  return { occupancy: { roots, nested, total, peak, atRootAdmission }, flush };
}

const operations = ['worker-only', 'object-noop', 'read', 'insert'];
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const median = values => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];

export async function runProfile({ cluster, container, owner, binaries, runCell, record, smoke }) {
  const proxy = cluster.project + '-proxy';
  const forwarder = join(binaries, 'coworkerd-bench-http-forwarder');
  const observePerf = process.env.COWORKERD_PROFILE_PERF !== '0';
  const stackOutput = process.env.COWORKERD_PROFILE_STACKS;
  async function nodeHostPid() {
    // Compose starts the daemon below its PID 1 init process.
    const processes = await cluster.command('docker', ['top', container, '-eo', 'pid,comm']);
    const pids = processes.split('\n').map(line => line.trim().split(/\s+/))
      .filter(([, command]) => command === 'coworkerd').map(([pid]) => Number(pid));
    assert.equal(pids.length, 1, 'one coworkerd daemon in the Docker container');
    assert.ok(Number.isInteger(pids[0]) && pids[0] > 0);
    return pids[0];
  }
  const trace = join(cluster.directory, 'node-trace.bin');
  const perfData = join(cluster.directory, 'perf.data');
  const perfScript = join(cluster.directory, 'perf.txt');
  if (stackOutput) await writeFile(resolve(stackOutput), '', { flag: 'wx' });
  if (observePerf) {
    assert.equal(process.platform, 'linux', 'perf must run on the Linux Docker host');
    // Seed the host cache with the exact daemon ELF used in the container.
    await cluster.command('sudo', ['-n', 'perf', 'buildid-cache', '--add', join(binaries, 'coworkerd')]);
  }
  await record('profile-tools', { perf: observePerf ? await cluster.command('sudo', ['-n', 'perf', '--version']) : null,
    perfHz: observePerf ? 499 : null, forwarderSha256: sha256(await readFile(forwarder)) });

  await cluster.command('docker', ['run', '-d', '--rm', '--name', proxy,
    '--network', cluster.project + '_frontend', '--pid', 'container:' + container,
    '--mount', 'type=bind,src=' + forwarder + ',dst=/http-forwarder,readonly',
    '--entrypoint', '/http-forwarder', cluster.image, '0.0.0.0:8930', owner + ':8930']);
  try {
    await eventually('KJ forwarder is listening', () => cluster.command('docker', [
      'exec', proxy, 'bash', '-ec', 'exec 3<>/dev/tcp/127.0.0.1/8930',
    ]));
    const proxyPid = Number(await cluster.command('docker', ['exec', proxy, 'bash', '-ec',
      'for candidate in /proc/[0-9]*/comm; do if read -r name < "$candidate" && [[ "$name" = http-forwarder ]]; then dir=${candidate%/comm}; echo "${dir##*/}"; fi; done',
    ]));
    assert.ok(Number.isInteger(proxyPid) && proxyPid > 0, 'forwarder PID in the node namespace');
    const pid = observePerf ? await nodeHostPid() : 0;

    async function measure(operation, clients, profileMode, round) {
      const result = await runCell({ operation, clients, profileMode, round,
        ...(profileMode === 'forwarded' && { site: proxy, extraPids: { forwarder: proxyPid } }),
        observe: async drive => {
          const profile = profileMode === 'profiled' && observePerf;
          const row = profile ? await recordPerf({ pid, data: perfData, script: perfScript, drive, seconds: smoke ? 5 : 14 }) : await drive();
          await cluster.command('docker', ['cp', container + ':' + traceInContainer, trace]);
          const counters = summarizeTrace(await readFile(trace), row.windowNs);
          assert.ok(counters.occupancy.roots.max > 0, 'window admitted at least one root');
          assert.equal(counters.occupancy.nested.max > 0, operation !== 'worker-only');
          if (operation === 'insert') assert.ok(counters.flush.calls > 0, 'counted local fdatasync calls');
          let perf;
          if (profile) {
            if (smoke && stackOutput) await copyFile(perfScript, resolve(stackOutput + '.' + operation + '.perf.txt'));
            const { stacks, ...summary } = await summarizeStacks(perfScript, row.windowNs);
            assert.ok(summary.histogram.other.samples + summary.histogram.unresolved.samples +
              summary.histogram.allocation.samples < summary.samples,
              'no named node or protocol frames were found in the profile');
            if (stackOutput) await appendFile(resolve(stackOutput),
              JSON.stringify({ operation, clients, round, ...summary, stacks }) + '\n');
            perf = summary;
          }
          return { ...row, ...counters, ...(perf && { perf }) };
        },
      });
      return result.perSecond;
    }

    const rounds = smoke ? 1 : 3;
    const offered = smoke ? [2] : [1, 4, 8, 16, 32];
    const rates = Object.fromEntries(operations.map(operation => [operation,
      Object.fromEntries(offered.map(clients => [clients, []]))]));
    for (let round = 0; round < rounds; ++round) {
      for (const clients of round % 2 ? [...offered].reverse() : offered) {
        for (const operation of operations) {
          rates[operation][clients].push(await measure(operation, clients, 'sweep', round + 1));
        }
      }
    }

    const selected = {};
    for (const operation of operations) {
      const medians = offered.map(clients => median(rates[operation][clients]));
      const ceiling = Math.max(...medians);
      const clients = offered[medians.findIndex(rate => rate >= 0.95 * ceiling)];
      selected[operation] = clients;
      await record('profile-ceiling', { operation, clients, ceiling, offered, medians });
    }
    for (let round = 0; round < rounds; ++round) {
      for (const operation of operations) {
        const modes = observePerf ? ['direct', 'forwarded', 'profiled'] : ['direct', 'forwarded'];
        for (const mode of [...modes.slice(round % modes.length), ...modes.slice(0, round % modes.length)]) {
          await measure(operation, selected[operation], mode, round + 1);
        }
      }
    }
  } finally {
    await cluster.command('docker', ['rm', '-f', proxy], { cleanup: true });
    await Promise.all([trace, perfData, perfScript].map(path => rm(path, { force: true })));
  }
}

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

const reports = process.argv[2];
const rows = async name => (await readFile(join(reports, name + '.jsonl'), 'utf8'))
  .split('\n').filter(Boolean).map(line => JSON.parse(line));
const measurements = {};
for (const tier of ['normal', 'slow']) {
  const sequence = [];
  for (const side of ['baseline', 'candidate']) {
    const data = await rows(side + '-' + tier);
    const mixed = data.find(row => row.withReader);
    assert.ok(mixed && data.at(-1).confirmedInserts > 0);
    const rates = Object.fromEntries([1, 16].map(clients => {
      const pairs = data.filter(row => !row.withReader && row.clients === clients);
      assert.equal(pairs.length, clients === 1 ? 5 : 3);
      return [clients, pairs.map(row => row.perSecond).sort((a, b) => a - b)];
    }));
    const timing = data.filter(row => row.flushes);
    sequence.push(...timing);
    for (const row of timing) {
      assert.equal(row.appendOnMain, side === 'baseline' ? row.appends : 0);
      assert.equal(row.flushOnMain, side === 'baseline' ? row.flushes : 0);
      if (tier === 'slow') assert.ok(row.flush.p50Ms >= 18, 'device flush really delayed fdatasync');
    }
    const peak = timing.reduce((max, row) => row.longestMainPollGapMs > max.longestMainPollGapMs ? row : max);
    measurements[side + '-' + tier] = {
      rates, mixed, peak: { clients: peak.clients, round: peak.round, withReader: peak.withReader,
        wallMs: peak.longestMainPollGapMs, cpuMs: peak.longestGap.cpuNs / 1e6 },
      longFlushes: timing.reduce((sum, row) => sum + row.longFlushes, 0),
      confirmedInserts: data.at(-1).confirmedInserts,
    };
  }
  sequence.sort((first, second) => first.begin - second.begin);
  for (let i = 1; i < sequence.length; ++i) {
    const previous = sequence[i - 1], current = sequence[i];
    assert.notEqual(previous.side, current.side, 'the two builds alternate at every concurrency');
    assert.ok(previous.end <= current.begin, 'the timed windows do not overlap');
  }
  const attribution = await rows(tier + '-long-flushes');
  assert.equal(attribution.length, measurements['candidate-' + tier].longFlushes);
  measurements['candidate-' + tier].withOverlappingIo = attribution.filter(row => row.overlapping.length).length;
}
console.log(JSON.stringify(measurements, null, 2));
assert.equal(measurements['baseline-slow'].mixed.readsEntirelyInsideFlush, 0);
assert.ok(measurements['candidate-slow'].mixed.readsEntirelyInsideFlush > 0);
assert.ok(measurements['candidate-slow'].peak.wallMs < 5, '5 ms issue gate on slow volume');

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

const reports = process.argv[2];
const rows = async (side, tier) => (await readFile(join(reports, side + '-' + tier + '.jsonl'), 'utf8'))
  .trim().split('\n').map(line => JSON.parse(line));
const measurements = {};
for (const tier of ['normal', 'slow']) {
  for (const side of ['baseline', 'candidate']) {
    const data = await rows(side, tier);
    const mixed = data.find(row => row.withReader);
    assert.ok(mixed && data.at(-1).confirmedInserts > 0);
    const rates = Object.fromEntries([1, 16].map(clients => {
      const pairs = data.filter(row => !row.withReader && row.clients === clients);
      assert.equal(pairs.length, 3);
      return [clients, pairs.map(row => row.perSecond).sort((a, b) => a - b)];
    }));
    const timing = data.filter(row => row.flushes);
    for (const row of timing) {
      assert.equal(row.appendOnMain, side === 'baseline' ? row.appends : 0);
      assert.equal(row.flushOnMain, side === 'baseline' ? row.flushes : 0);
      if (tier === 'slow') assert.ok(row.flush.p50Ms >= 18, 'device flush really delayed fdatasync');
    }
    measurements[side + '-' + tier] = {
      rates, mixed: { ...mixed }, longestMainPollGapMs: Math.max(...timing.map(row => row.longestMainPollGapMs)),
      confirmedInserts: data.at(-1).confirmedInserts,
    };
  }
}
console.log(JSON.stringify(measurements, null, 2));
assert.equal(measurements['baseline-slow'].mixed.readsEntirelyInsideFlush, 0);
assert.ok(measurements['candidate-slow'].mixed.readsEntirelyInsideFlush > 0);
assert.ok(measurements['candidate-slow'].longestMainPollGapMs < 5, '5 ms issue gate on slow volume');

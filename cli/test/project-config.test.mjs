import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { adapt } from '../adapter.mjs';

test('project TOML is parsed before unsupported deployment configuration is refused', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'coworkerd-config-'));
  try {
    const config = join(directory, 'wrangler.toml');
    await writeFile(config, 'name = "test-worker"\nmain = "index.js"\ncompatibility_date = "2026-01-01"\n[build]\ncommand = "echo never"\n[vars]\nHELLO = "there"\n[unsafe]\nmetadata = { flags = ["custom"] }\n');
    await assert.rejects(adapt({ config, output: join(directory, 'out'), workerId: 'test-worker' }),
      /Unsupported Wrangler configuration: unsafe/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('Wrangler subrequest limits travel with the uploaded version and are checked before building', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'coworkerd-config-'));
  try {
    const config = join(directory, 'wrangler.toml');
    const output = join(directory, 'out');
    const base = 'name = "test-worker"\nmain = "index.js"\ncompatibility_date = "2026-01-01"\n';
    const writeLimit = value => writeFile(config, `${base}[limits]\nsubrequests = ${value}\n`);
    for (const limit of ['0', '-1', '2.5', '"50"', '10_000_001']) {
      await writeLimit(limit);
      await assert.rejects(adapt({ config, output }), /limits\.subrequests must be a whole number between 1 and 10,000,000/);
    }
    await writeFile(join(directory, 'index.js'), 'export default { fetch: () => new Response("ready") };');
    await writeLimit('10_000_000');
    const { deployment } = await adapt({ config, output });
    assert.deepEqual(deployment.workers[0].versions[0].limits, { subrequests: 10_000_000 });
    await writeFile(config, base);
    const legacy = await adapt({ config, output: join(directory, 'legacy') });
    assert.equal(legacy.deployment.workers[0].versions[0].limits, undefined);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

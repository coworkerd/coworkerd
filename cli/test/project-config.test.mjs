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

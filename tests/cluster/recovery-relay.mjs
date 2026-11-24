import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import { once } from 'node:events';
import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { createConnection, createServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';

async function releaseShard(namespaceId, nodeId, nodeEpoch, shardText, epoch) {
  const host = 'storage:8333';
  const shard = Number(shardText);
  const { accessKey, secretKey } = JSON.parse(await readFile('/store-secrets/deploy.json', 'utf8'));
  const prefix = `/coworkerd/do/${namespaceId}/shards/${shard}`;
  const record = { namespaceId, nodeId, nodeEpoch: String(nodeEpoch), shard, epoch: String(epoch) };
  const put = async (path, value) => {
    const sha = data => createHash('sha256').update(data).digest('hex');
    const hmac = (key, data) => createHmac('sha256', key).update(data).digest();
    const body = JSON.stringify(value);
    const date = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
    const hash = sha(body);
    const signed = 'host;x-amz-content-sha256;x-amz-date';
    const headers = `host:${host}\nx-amz-content-sha256:${hash}\nx-amz-date:${date}\n`;
    const canonical = ['PUT', path, '', headers, signed, hash].join('\n');
    const day = date.slice(0, 8), region = 'us-east-1';
    const scope = `${day}/${region}/s3/aws4_request`;
    const key = hmac(hmac(hmac(hmac(`AWS4${secretKey}`, day), region), 's3'), 'aws4_request');
    const signature = hmac(key, `AWS4-HMAC-SHA256\n${date}\n${scope}\n${sha(canonical)}`).toString('hex');
    const response = await fetch(`http://${host}${path}`, { method: 'PUT', body, headers: {
      'x-amz-date': date, 'x-amz-content-sha256': hash,
      authorization: `AWS4-HMAC-SHA256 Credential=${accessKey}/${scope}, SignedHeaders=${signed}, Signature=${signature}`,
    }, signal: AbortSignal.timeout(5000) });
    assert.equal(response.status, 200, `test shard release write: ${await response.text()}`);
  };
  await put(`${prefix}/epochs/${String(epoch).padStart(20, '0')}.released`,
    { ...record, reason: 'drain', releasedAtUnixMs: Date.now() });
  await put(`${prefix}/owner`, { ...record, released: true, writtenAtUnixMs: Date.now() });
}

if (process.argv[2] === '--release') {
  await releaseShard(...process.argv.slice(3));
} else {
  relayPeers();
}

function relayPeers() {
  // Keep CONNECT and traffic to the holder flowing. A file pauses traffic
  // from that holder after the HTTP handshake. The store and its heartbeats
  // are on a different connection.
  for (const index of [1, 2, 3]) {
    createServer(client => {
      const upstream = createConnection(8934, `node-${index}-peer`);
      const hold = `/gates/node-${index}`;
      client.on('error', () => upstream.destroy());
      client.on('close', () => upstream.destroy());
      upstream.on('error', () => client.destroy());
      client.pipe(upstream);
      (async () => {
        let handshake = Buffer.alloc(0);
        for await (let bytes of upstream) {
          if (handshake) {
            handshake = Buffer.concat([handshake, bytes]);
            const end = handshake.indexOf('\r\n\r\n');
            if (end < 0) continue;
            client.write(handshake.subarray(0, end + 4));
            bytes = handshake.subarray(end + 4);
            handshake = null;
          }
          if (!bytes.length) continue;
          while (!client.destroyed && existsSync(hold)) await delay(20);
          if (client.destroyed) return;
          if (!client.write(bytes)) await once(client, 'drain');
        }
        client.end();
      })().catch(() => client.destroy());
    }).listen(18000 + index, '0.0.0.0');
  }
}

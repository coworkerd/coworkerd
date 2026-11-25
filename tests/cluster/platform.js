import { DurableObject } from 'cloudflare:workers';

let copy;

export class PlatformStore extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.instance = crypto.randomUUID();
    this.sql = ctx.storage.sql;
    this.sql.exec('CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY)');
    this.sql.exec('CREATE TABLE IF NOT EXISTS alarm_status (fired INTEGER)');
  }

  async fetch(request) {
    const action = new URL(request.url).searchParams.get('action');
    if (action === 'identity') {
      copy ??= crypto.randomUUID();
      return Response.json({ copy, instance: this.instance, build: fixtureBuild, env: Object.keys(this.env) });
    }
    if (action === 'alarm') {
      await this.ctx.storage.setAlarm(Date.now() + 1000);
      return Response.json(true);
    }
    if (action === 'alarmed') {
      return Response.json(this.sql.exec('SELECT COUNT(*) AS count FROM alarm_status').one().count);
    }
    if (request.method === 'POST') {
      const id = await request.json();
      this.sql.exec('INSERT OR IGNORE INTO records VALUES (?)', id);
      return Response.json(id);
    }
    return Response.json(this.sql.exec('SELECT id FROM records ORDER BY id').toArray().map(row => row.id));
  }

  alarm() {
    this.sql.exec('INSERT INTO alarm_status VALUES (1)');
  }
}

export class PlatformEcho extends DurableObject {
  fetch() {
    return Response.json({ service: 'echo', build: fixtureBuild });
  }
}

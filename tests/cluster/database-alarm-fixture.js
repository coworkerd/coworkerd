// Appended to the database module by the test executable. Inherit the real SQL and alarm request handling.
SqlDatabase = class extends SqlDatabase {
  constructor(ctx, env) {
    super(ctx, env);
    // workerd allows a constructor to seed an alarm before it checks for an alarm() handler.
    // The first SQL request creates records, so later constructors leave the alarm alone.
    const seeded = ctx.storage.sql.exec("SELECT name FROM sqlite_schema WHERE name = 'records'").toArray();
    if (seeded.length === 0) {
      const scheduled = ctx.storage.setAlarm(Date.now() + 5000);
      ctx.blockConcurrencyWhile(() => scheduled);
    }
  }

  async fetch(request) {
    if (request.method === 'POST' && new URL(request.url).pathname === '/query') {
      const body = await request.clone().json().catch(() => null);
      if (body?.alarmForTest === true) {
        return Response.json({ success: true, results: [{ alarm: await this.ctx.storage.getAlarm() }] });
      }
    }
    return super.fetch(request);
  }
};

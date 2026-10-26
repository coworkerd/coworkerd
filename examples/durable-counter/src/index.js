import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  async value() {
    return (await this.ctx.storage.get("count")) ?? 0;
  }

  async fetch(request) {
    if (request.method === "POST") {
      await this.ctx.storage.put("count", (await this.value()) + 1);
    } else if (request.method !== "GET") {
      return new Response("Use GET or POST", { status: 405, headers: { Allow: "GET, POST" } });
    }
    return Response.json({ count: await this.value() });
  }
}

export default {
  async fetch(request, env) {
    const counter = env.COUNTERS.getByName("shared");
    if (new URL(request.url).pathname.endsWith("/rpc")) {
      const count = await counter.value();
      const row = await env.DB.prepare("SELECT 42 AS answer").first();
      return Response.json({ count, database: row.answer });
    }
    return counter.fetch(request);
  },
};

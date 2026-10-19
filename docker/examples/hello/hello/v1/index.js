// The smallest Worker: answers every request and says which Server ran it.
export default {
  async fetch(request) {
    return new Response(`Hello from Coworkerd (${request.cf?.serverId ?? "unknown"})\n`, {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  },
};

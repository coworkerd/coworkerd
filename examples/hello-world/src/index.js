// The quickstart's Worker: plain JavaScript, no dependencies and no build step.
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    // Shown by `coworkerd logs`.
    console.log(`${request.method} ${url.pathname}`);
    if (url.pathname.endsWith("/secret")) {
      // A secret is a string in env; never send its value back, only whether it is there.
      return new Response(env.API_KEY === undefined ? "API_KEY is not set\n" : `API_KEY has ${env.API_KEY.length} characters\n`);
    }
    return new Response("Hello from Coworkerd!\n");
  },
};

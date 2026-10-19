# email-routing-template (static assets)

Cloudflare's official template [`email-routing-template`](https://github.com/cloudflare/templates/tree/dfdf14099a81a08cfe1ffc78f8463605670ae513/email-routing-template),
as it is (pinned at commit `dfdf1409`, MIT License, see `LICENSE`; the template's own description is in `README.cloudflare.md`). It uses:

- **static assets**: the `public/` directory (`assets.directory`), served by the Server directly at the ingress;
- **the `ASSETS` Binding**: `/` goes to the Worker first (`run_worker_first: ["/"]`), and the Worker takes `index.html` with `env.ASSETS.fetch()`, then uses
  `HTMLRewriter` to fill the addresses from `vars` into the page;
- `vars` (three environment variables).

The email handler (`email()`) is never called on Coworkerd; `observability` is a Cloudflare platform setting, ignored with a notice at conversion. No `npm install` is needed:
it has no runtime dependencies, and the wrangler that comes with `coworkerd` can build it.

Its asset paths start at `/`, so give it a hostname of its own as its route (`*.localhost` resolves to 127.0.0.1 locally):

```bash
cowctl init --route email-routing.localhost/
cowctl deploy
curl http://email-routing.localhost:18080/                   # the settings page: support@company.example
curl -I http://email-routing.localhost:18080/index.html      # static assets answered directly by the Server (ETag and so on)
```

A new version: change `ROUTE_ADDRESS` in `wrangler.jsonc` to another address and `cowctl deploy` again; the address on the page changes with it; `cowctl rollback v1` goes back to the original.
`--route /` can also give it `/` (only one Worker on a cluster can take `/`).

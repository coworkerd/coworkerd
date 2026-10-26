# email-routing-template (static assets)

Cloudflare's official template [`email-routing-template`](https://github.com/cloudflare/templates/tree/dfdf14099a81a08cfe1ffc78f8463605670ae513/email-routing-template),
as it is (pinned at commit `dfdf1409`, MIT License, see `LICENSE`; the template's own description is in `README.cloudflare.md`). It uses:

- **static assets**: the `public/` directory (`assets.directory`), served by the node directly at the ingress;
- **the `ASSETS` Binding**: `/` goes to the Worker first (`run_worker_first: ["/"]`), and the Worker takes `index.html` with `env.ASSETS.fetch()`, then uses
  `HTMLRewriter` to fill the addresses from `vars` into the page;
- `vars` (three environment variables).

The email handler (`email()`) is never called on Coworkerd; `observability` is a Cloudflare platform setting, ignored with a notice at conversion. No `npm install` is needed:
it has no runtime dependencies, and the wrangler that comes with `coworkerd` can build it.

Its asset paths start at `/`, so it takes a hostname of its own as its route (the curl examples explicitly resolve it to 127.0.0.1): the administrator registers the hostname
to the tenant first. With a tenant and its credential as the quickstart's section 2 makes them (the tenant `acme`, the admin credential in `~/.cowctl/admin`, the tenant's in `~/.cowctl/token`):

```bash
COWCTL_TOKEN=$(cat ~/.cowctl/admin) cowctl tenant update acme --hostname email-routing.localhost   # the list replaces acme's: name every hostname it keeps
cowctl init --tenant acme --route email-routing.localhost/
cowctl deploy
curl --resolve email-routing.localhost:18080:127.0.0.1 http://email-routing.localhost:18080/                   # the settings page: support@company.example
curl --resolve email-routing.localhost:18080:127.0.0.1 -I http://email-routing.localhost:18080/index.html      # static assets answered directly by the node (ETag and so on)
```

A new version: change `ROUTE_ADDRESS` in `wrangler.jsonc` to another address and `cowctl deploy` again; the address on the page changes with it; `cowctl rollback v1` goes back to the original.
A route must be on a hostname registered to the tenant, or a path under `/<tenant>/` of the cluster's default host, which this template's paths starting at `/` do not fit.

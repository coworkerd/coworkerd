# hello-world

The Worker of the quickstart ([docs/guide/quickstart.md](../../docs/guide/quickstart.md); `docs/quickstart.md` in the release package): plain JavaScript,
no dependencies, no build step.

| Request | Answer |
| --- | --- |
| `GET /<tenant>/hello-world` | `Hello from Coworkerd!`, with a line from `console.log` (visible in `cowctl logs`) |
| `GET /<tenant>/hello-world/secret` | Whether the secret `API_KEY` exists and how many characters it has (the value is never echoed) |

With a tenant and its credential as the quickstart's section 2 makes them (the tenant `acme`, its credential in `~/.cowctl/token`):

```bash
cowctl init --tenant acme   # writes cowctl.json: the tenant, the control plane, the ingress, the credential file; the Worker's name comes from wrangler.jsonc's name
cowctl deploy               # builds, uploads as a new version, trial-loads, switches, waits for the nodes; route */acme/hello-world
curl http://127.0.0.1:18080/acme/hello-world
cowctl logs
printf 'my-first-secret' | cowctl secret put API_KEY
cowctl deploy --secret API_KEY
curl http://127.0.0.1:18080/acme/hello-world/secret   # API_KEY has 15 characters
```

This directory is also an ordinary wrangler project (`wrangler.jsonc`), which `cowctl deploy` builds with wrangler.

# hello-world

The Worker of the quickstart ([docs/guide/quickstart.md](../../docs/guide/quickstart.md); `docs/quickstart.md` in the release package): plain JavaScript,
no dependencies, no build step.

| Request | Answer |
| --- | --- |
| `GET /hello-world` | `Hello from Coworkerd!`, with a line from `console.log` (visible in `cowctl logs`) |
| `GET /hello-world/secret` | Whether the secret `API_KEY` exists and how many characters it has (the value is never echoed) |

```bash
cowctl init          # writes cowctl.json: the control plane, the ingress, the token file; the Worker's name comes from wrangler.jsonc's name
cowctl deploy        # builds, uploads as a new version, trial-loads, switches; route */hello-world
curl http://127.0.0.1:18080/hello-world
cowctl logs
printf 'my-first-secret' | cowctl secret put API_KEY
cowctl deploy --secret API_KEY
curl http://127.0.0.1:18080/hello-world/secret   # API_KEY has 15 characters
```

This directory is also an ordinary wrangler project: with wrangler installed, `npx wrangler dev` works.

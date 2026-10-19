# Coworkerd quickstart

> For release 0.3.0.
> From a wrangler project to the first request in about 10 minutes: start the cluster -> the administrator creates a tenant and a credential -> `cowctl init --tenant` -> `cowctl deploy` -> `curl` ->
> `cowctl logs` -> a new version -> `cowctl rollback` -> `cowctl secret put` -> `cowctl usage`.
> Installation details, bare-metal deployment and configuration are in [installing and running](install.md); which wrangler features work is in the [compatibility scope](compat.md); errors are in [error codes](errors.md).

Coworkerd's resources have two levels: **tenant -> Worker**. One cluster serves one platform, the platform is the administrator, and each of the platform's users (or teams) is a tenant;
a Worker's full name is `tenant/Worker`, so two tenants can each have a Worker called `hello-world` without affecting each other.

## 0. Preparation

- **Docker Engine 28 or newer** and Docker Compose v2 (Docker Desktop on the Mac has both; on Linux it is enough that `docker compose version` prints a version);
- **Node.js 20 or newer**: the command line `cowctl` and the wrangler build it runs both use it (`node --version`);
- `curl`;
- your architecture's two files from the release and the checksums: `coworkerd-0.3.0-linux-<arch>.tar.gz`, `coworkerd-0.3.0-linux-<arch>-image.tar.gz`,
  `SHA256SUMS`. A Mac (Apple silicon) uses `arm64`; most Linux servers use `amd64`.

## 1. Start the cluster (about 30 seconds)

```bash
shasum -a 256 -c --ignore-missing SHA256SUMS        # on Linux: sha256sum -c --ignore-missing SHA256SUMS
tar -xzf coworkerd-0.3.0-linux-arm64.tar.gz
docker load -i coworkerd-0.3.0-linux-arm64-image.tar.gz
cd coworkerd-0.3.0-linux-arm64
export COWORKERD_PACKAGE="$PWD" PATH="$PWD/bin:$PATH"
(cd docker && docker compose --profile cluster up -d --wait)
```

This starts three nodes (compose services `node-1`, `node-2`, `node-3`, with their ingresses on local ports `18080`, `18081`, `18082`), etcd, object storage and the **control plane**
(service `apiserver`, the API `cowctl` connects to, local port `18090`). For a single node, leave out `--profile cluster`. `bin/cowctl` is the command line `cowctl` (`cowctl --help` lists all subcommands).

## 2. Administrator: create a tenant and its credential

```bash
mkdir -p ~/.cowctl
(cd "$COWORKERD_PACKAGE/docker" && docker compose run --rm tool admin-credential) > ~/.cowctl/admin
chmod 600 ~/.cowctl/admin
COWCTL_TOKEN=$(cat ~/.cowctl/admin) cowctl tenant create acme
COWCTL_TOKEN=$(cat ~/.cowctl/admin) cowctl credential create --tenant acme > ~/.cowctl/token
chmod 600 ~/.cowctl/token
```

The cluster generated an **admin credential** on its first start (for the platform itself: it can create tenants, issue credentials, and see all nodes and all usage). `cowctl credential create` issues the tenant `acme`
a **tenant credential**: it can act only on `acme`'s own Workers, its value appears only in this one output, and the control plane stores only its hash -- if you lose it, issue a new one and revoke the old one.
`cowctl` reads the credential only from the environment variable `COWCTL_TOKEN` or from the file `cowctl.json` points to (default `~/.cowctl/token`), never from a command line argument.
Tenant names and Worker names are lowercase letters, digits and `-`, at most 63 characters.

## 3. Deploy the first Worker

```bash
cp -R "$COWORKERD_PACKAGE/examples/hello-world" ~/hello-world     # or go into your own wrangler project's directory
cd ~/hello-world
cowctl init --tenant acme
cowctl deploy
curl http://127.0.0.1:18080/acme/hello-world
```

`cowctl init` writes `cowctl.json` in the current directory (the tenant `acme`, the control plane `http://127.0.0.1:18090`, the ingress `http://127.0.0.1:18080`, the credential file, and the
Worker name taken from `name` in the wrangler configuration) and connects to the control plane once to confirm it works. `cowctl deploy` builds the project with wrangler, converts it into a Coworkerd code bundle,
uploads it as the new version `v1`, and **first trial-loads it in a node's quarantine process**; only if it loads does it exist and get switched to:

```text
deployed acme/hello-world v1 (active)
  package: 3 files, 4608 bytes; built in 2.1 s, deployed in 2.2 s
  trial load: node-2 (its quarantine process), 15 ms
  route */acme/hello-world  ->  http://127.0.0.1:18080/acme/hello-world
```

```text
$ curl http://127.0.0.1:18080/acme/hello-world
Hello from Coworkerd!
```

All three ingresses work: a request can enter at any node and runs on this Worker's home (the node computed from `tenant/Worker`). By default the route is
`/<tenant>/<Worker name>` under the cluster's default host; a tenant's own hostnames are registered to it by the administrator, then used with `cowctl deploy --route <hostname>/` (see section 9).

## 4. Logs

```bash
cowctl logs
cowctl logs --follow          # keeps printing; Ctrl-C ends it
```

```text
2026-10-18 12:47:39.982 node-2 log   v1 GET /acme/hello-world
```

Each line is one `console.log`/`info`/`debug`/`warn`/`error` in the Worker: the time, the node that ran it, the level, the version, the content. The control plane fetches this Worker's logs from each node and
merges them by time. `--since 10m` (or `2h`, or an ISO time) shows only the recent ones; `--json` prints one JSON object per line. Log quotas are per **tenant**: each node keeps the latest 1 MiB in memory for each tenant,
and each tenant may record at most 100 lines per second (the administrator can adjust both per tenant); what exceeds them is not recorded, and a `[dropped]` line counts it; a line over 4 KB is truncated
(none of this affects the requests themselves or other tenants).

## 5. A new version, and a rollback

```bash
sed -i.orig 's/Hello from Coworkerd!/Hello again!/' src/index.js && rm src/index.js.orig
cowctl deploy
curl http://127.0.0.1:18080/acme/hello-world          # Hello again!
cowctl versions
cowctl rollback v1
curl http://127.0.0.1:18080/acme/hello-world          # Hello from Coworkerd!
```

Each `deploy` is a new version (`v1`, `v2`, ...; `--version` names it yourself), and old versions are kept (the number of versions kept per Worker is a tenant quota; when it is full, delete old ones first);
`rollback <version>` makes it the active version again (again trial-loading first). `deploy --no-activate` only uploads and trial-loads, without switching; switch to it later with `rollback`.

```text
$ cowctl versions
acme/hello-world: active v2
  v1       uploaded at revision 7        package 73014de13478
* v2       uploaded at revision 9        package 0a20a12702fa
  route */acme/hello-world  ->  http://127.0.0.1:18080/acme/hello-world
```

## 6. Add a secret

```bash
printf 'my-first-secret' | cowctl secret put API_KEY
cowctl deploy --secret API_KEY
curl http://127.0.0.1:18080/acme/hello-world/secret   # API_KEY has 15 characters
```

A secret's value is read only from standard input (run directly in a terminal, it prompts for it without echoing) or from `--from-file`, with trailing whitespace removed; every output, log and state shows only its name and length.
A version has to **declare** which secrets it reads: wrangler's convention is to list them in the project's `.dev.vars.example` (only the names are taken; `.dev.vars` and `.env` are never read),
or `deploy --secret NAME`. A version that declares a secret without a value is refused at deployment, with a hint to `secret put` first. `cowctl secret list` lists only names and lengths;
`cowctl secret delete <name>` deletes one (the deletion is refused while an active version still uses it). After a value changes, newly loaded isolates read the new value, and already loaded ones keep the old value until they are evicted.

## 7. Usage

```bash
cowctl usage
```

Each node records, per hour and per tenant: requests, CPU milliseconds, egress bytes, log lines, isolate time; it writes them once a minute (so a request you just sent takes a little while to appear).
`--since 7d` shows earlier ones; the administrator sees all tenants with `cowctl usage --all`.

## 8. A second tenant

```bash
COWCTL_TOKEN=$(cat ~/.cowctl/admin) cowctl tenant create beta
COWCTL_TOKEN=$(cat ~/.cowctl/admin) cowctl credential create --tenant beta > ~/.cowctl/beta-token
chmod 600 ~/.cowctl/beta-token
cp -R "$COWORKERD_PACKAGE/examples/hello-world" ~/hello-world-beta
cd ~/hello-world-beta
cowctl init --tenant beta --token-file ~/.cowctl/beta-token
cowctl deploy
curl http://127.0.0.1:18080/beta/hello-world
curl http://127.0.0.1:18080/acme/hello-world/secret   # acme's Worker is unchanged
cd ~/hello-world
cowctl versions --tenant beta                          # acme's credential: FORBIDDEN
```

The two tenants each have a `hello-world` with the same name, without affecting each other: routes, versions, secrets, logs and usage are each their own. A tenant credential acts only on its own tenant;
touching another tenant gets `403 FORBIDDEN`.

## 9. Your own project

In the directory of a wrangler project, `cowctl init --tenant <tenant>`, then `cowctl deploy`:

- if the project has its own wrangler installed (`npm install` was run), it builds with that; otherwise with the wrangler 4.136.1 `cowctl` brings along. For a project with dependencies, run `npm install` first;
- a project that needs its own build first (Vite, React Router and the like): run `npm run build` first;
- unsupported configuration (KV, Durable Objects, D1 and others) is listed in full at conversion and refused; platform-type settings (`observability` and others) are ignored with a notice, see the [compatibility scope](compat.md);
- routes: `/<tenant>/<Worker name>` by default. A **static assets site** has its asset paths starting at `/` and needs a whole host: the administrator first registers the hostname to the tenant
  (`cowctl tenant update acme --hostname site.acme.localhost`; a hostname belongs to one tenant only), then `cowctl deploy --route site.acme.localhost/`
  (then `curl http://site.acme.localhost:18080/`). An unregistered hostname, or a path under the default host that does not start with `/<tenant>/`, gets `HOSTNAME_NOT_OWNED` at deployment;
- quotas: the number of Workers, versions per Worker, bundle size, the number and total size of static assets, the number of secrets, the number of hostnames and the log volume are all tenant quotas (set by the administrator, shown by `cowctl tenant get`);
  over one gets `QUOTA_EXCEEDED`, and the reason says which.

`examples/` also has two official templates as they are: `email-routing-template` (static assets and the `ASSETS` Binding) and `x402-proxy-template` (secrets);
each one's `README.md` says how to deploy it.

## 10. Cleaning up

```bash
cowctl status                                                       # each node, its processes and versions
(cd "$COWORKERD_PACKAGE/docker" && docker compose --profile cluster down -v)   # stop and delete all data
```

To stop without deleting: `down` without `-v`; at the next `up`, the tenants, credentials, deployments and secrets are all still there.

## 11. Commands

| Command | Purpose |
| --- | --- |
| `cowctl init --tenant T [--url URL] [--ingress URL] [--token-file FILE] [--route PREFIX] [--force]` | Write `cowctl.json` |
| `cowctl deploy [--no-activate] [--version ID] [--route PREFIX] [--secret NAME]...` | Build, upload a new version, trial-load, switch |
| `cowctl versions` | The versions; `*` marks the active one |
| `cowctl rollback <version>` | Make a version the active one |
| `cowctl remove [--yes]` | Stop serving this Worker (its versions are kept; `deploy` again restores it) |
| `cowctl secret put <NAME> [--from-file FILE]`, `secret list`, `secret delete <NAME>` | Secrets |
| `cowctl status` | The cluster's nodes, processes and versions |
| `cowctl logs [--follow] [--since 10m] [--limit N] [--json]` | This Worker's logs |
| `cowctl usage [--since 24h] [--until TIME] [--all]` | Hourly usage |
| `cowctl tenant create\|list\|get\|update\|delete` (administrator) | Tenants: hostnames (`--hostname`), quotas (`--quota name=value`), default limits |
| `cowctl credential create\|list\|revoke` (administrator; a tenant can revoke its own) | Tenant credentials |
| `cowctl node list` (administrator) | Each node's processes, sandbox, rotation, suspects |

`--tenant T` names the tenant (default from `cowctl.json`), `--worker NAME` the Worker (default from `cowctl.json`, then `name` in the wrangler configuration);
`COWCTL_CONTROL_PLANE` overrides the control plane's address. On failure it prints one sentence plus the reason the control plane gave (cleaned: no secret values or absolute paths) and exits with 1; a usage error exits with 2.

## 12. Troubleshooting

| Symptom | Remedy |
| --- | --- |
| `cannot reach the control plane at http://127.0.0.1:18090` | The cluster is not up, or the control plane's port differs: `docker compose ps`; after changing `COWORKERD_CONTROL_PORT` in `.env`, `cowctl init --url ... --force` |
| `no credential` / `refused the credential` | Step 2: put the tenant credential into `~/.cowctl/token`, or `export COWCTL_TOKEN=...`; a revoked credential no longer works |
| `FORBIDDEN` | A tenant credential acts only on its own tenant; `tenant`, `credential create`, `node` and `usage --all` need the admin credential |
| `TENANT_NOT_FOUND` | The tenant has not been created yet (`cowctl tenant create`), or the name is misspelled |
| `the project does not convert: Unsupported Wrangler configuration: ...` | The listed settings are not supported by this platform, see the [compatibility scope](compat.md) |
| `deploy refused: version vN does not load (SYNTAX_ERROR ...)` | The new version cannot be loaded and the active version did not change; reasons and categories are in [error codes](errors.md) |
| `deploy refused: ... secret ... without a value` | `cowctl secret put <name>` first |
| `deploy refused: tenant ...'s quota` / `HOSTNAME_NOT_OWNED` | Over the tenant's quota, or the route's hostname does not belong to this tenant: see section 9 |
| `curl` gets 404 `ROUTE_NOT_FOUND` | The path is not under any route: see the routes at the end of `cowctl versions` (by default `/<tenant>/<Worker>`) |
| `curl` gets 503 | The code failed or exceeded a limit while running, see [error codes](errors.md); `cowctl logs` shows what this request printed |

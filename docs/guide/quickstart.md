# Coworkerd quickstart

> For release 0.4.0.
> From a wrangler project to the first request: start the cluster -> the administrator creates a tenant and a credential -> `cowctl init --tenant` -> `cowctl deploy` -> `curl` ->
> `cowctl logs` -> a new version -> `cowctl rollback` -> `cowctl secret put` -> `cowctl usage`.
> Installation details, bare-metal deployment and configuration are in [installing and running](install.md); which wrangler features work is in the [compatibility scope](compat.md); errors are in [error codes](errors.md).

Coworkerd's resources have two levels: **tenant -> Worker**. One cluster serves one platform, the platform is the administrator, and each of the platform's users (or teams) is a tenant;
a Worker's full name is `tenant/Worker`, so two tenants can each have a Worker called `hello-world` without affecting each other.

## 0. Preparation

- **Docker Engine 28 or newer** and the Docker Compose plugin (Docker Desktop on the Mac has both; on Linux take a Compose that supports volume `subpath` and the healthcheck's `start_interval`, which the compose file uses; this page was run with Docker 29.2 and Compose 5.1);
- **Node.js 22.5 or newer** (`node --version`): the wrangler that `cowctl deploy` runs requires Node.js 22, and `cowctl`'s adapter uses `path.matchesGlob`, which Node.js has since 22.5;
- `curl`;
- your architecture's two files from the release and the checksums: `coworkerd-0.4.0-linux-<arch>.tar.gz`, `coworkerd-0.4.0-linux-<arch>-image.tar.gz`,
  `SHA256SUMS`. A Mac (Apple silicon) uses `arm64`; most Linux servers use `amd64`.

## 1. Start the cluster (well under a minute)

```bash
shasum -a 256 -c --ignore-missing SHA256SUMS        # on Linux: sha256sum -c --ignore-missing SHA256SUMS
tar -xzf coworkerd-0.4.0-linux-arm64.tar.gz
docker load -i coworkerd-0.4.0-linux-arm64-image.tar.gz
cd coworkerd-0.4.0-linux-arm64
export COWORKERD_PACKAGE="$PWD" PATH="$PWD/bin:$PATH"
(cd docker && docker compose --profile cluster up -d --wait)
```

This starts three nodes (compose services `node-1`, `node-2`, `node-3`, with their ingresses on local ports `18080`, `18081`, `18082`), the object store that holds the cluster's whole state, and the **control plane**
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
a **tenant credential**: it can act only within tenant `acme` (its Workers, its usage and record, and revoking its credentials) and, like every credential, read the cluster's status (`cowctl status`), its value appears only in this one output, and the control plane stores only its hash -- if you lose it, issue a new one and revoke the old one.
`cowctl` reads the credential only from the environment variable `COWCTL_TOKEN` or from the file `cowctl.json` points to (default `~/.cowctl/token`), never from a command line argument.
Tenant names and Worker names are lowercase letters, digits and `-`, start with a letter or a digit, and have at most 63 characters; `routes` and `generation` are not tenant names.

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
  package: 3 files, 4608 bytes; built in 2.2 s, deployed in 2.2 s
  trial load: node-3 (its quarantine process), 17 ms
  route */acme/hello-world  ->  http://127.0.0.1:18080/acme/hello-world
  served by 3 nodes (tenant index revision 3) in 1.9 s
```

The nodes read the cluster's desired state from the object store every 5 seconds, so a deployment reaches them within about 5 seconds; until then a node answers with what it had before --
here `404 ROUTE_NOT_FOUND`, after a new version the previous one. So `cowctl deploy` waits until every node that is not draining reports that it has read the change (the last line), at most 15 seconds:
past that it names the nodes that have not yet and succeeds all the same, since the change is made. `cowctl rollback` and `cowctl secret put` wait the same way, and `--no-wait` returns at once.

```text
$ curl http://127.0.0.1:18080/acme/hello-world
Hello from Coworkerd!
```

All three ingresses work: a request can enter at any node and normally runs on this Worker's home node (computed from `tenant/Worker`); a node that ran the version within the last 30 s may serve it itself. By default the route is
`/<tenant>/<Worker name>` under the cluster's default host; a tenant's own hostnames are registered to it by the administrator, then used with `cowctl deploy --route <hostname>/` (see section 9).

## 4. Logs

```bash
cowctl logs
cowctl logs --follow          # keeps printing; Ctrl-C ends it
```

```text
2026-10-24 06:44:11.514 node-3 log   v1 GET /acme/hello-world
```

Each line is one `console.log`/`info`/`debug`/`warn`/`error` in the Worker: the time, the node that ran it, the level, the version, the content. The control plane fetches this Worker's logs from each node and
merges them by time. `--since 10m` (or `2h`, or an ISO time) shows only the recent ones; `--json` prints one JSON object per line. Log quotas are per **tenant** and per node: each node keeps the latest 1 MiB in memory for each tenant,
and each tenant may record at most 100 lines per second on each node (the administrator can adjust both per tenant); lines over the per-second rate are not recorded and a `[dropped]` line counts them, the buffer keeps the newest 1 MiB, and a line over 4 KB is cut;
all tenants' lines on a node together are kept up to 64 MiB, past which the oldest line of any tenant goes first (none of this affects the requests themselves).

## 5. A new version, and a rollback

```bash
sed -i.orig 's/Hello from Coworkerd!/Hello again!/' src/index.js && rm src/index.js.orig
cowctl deploy
curl http://127.0.0.1:18080/acme/hello-world          # Hello again!
cowctl versions
cowctl rollback v1
curl http://127.0.0.1:18080/acme/hello-world          # Hello from Coworkerd!
```

Each `deploy` is a new version (`v1`, `v2`, ...; `--version` names it yourself), and old versions are kept (the number of versions kept per Worker is a tenant quota; when it is full, delete old ones first with the API's `DELETE /v1/tenants/{t}/workers/{w}/versions/{v}`: `cowctl` has no command for it);
`rollback <version>` makes it the active version again (again trial-loading first). `deploy --no-activate` only uploads, without switching and without a trial load (the version is trial-loaded when it is activated) and without a route (it refuses `--route`); switch to it later with `rollback`.

```text
$ cowctl versions
acme/hello-world: active v2
  v1       uploaded at revision 2        package 73014de13478
* v2       uploaded at revision 3        package 1df563644dea
  route */acme/hello-world  ->  http://127.0.0.1:18080/acme/hello-world
```

## 6. Add a secret

```bash
printf 'my-first-secret' | cowctl secret put API_KEY
cowctl deploy --secret API_KEY
curl http://127.0.0.1:18080/acme/hello-world/secret   # API_KEY has 15 characters
```

A secret's value is read only from standard input (run directly in a terminal, it prompts for it without echoing) or from `--from-file`, with trailing whitespace removed; every output, log and state shows only its name, its length and when it was set.
In the cluster it is stored sealed with the cluster's secrets key: the control plane seals it, and opens it only to check that it opens, each time it evaluates or lists the tenant's Workers (a deployment, a rollback, `versions`, `secret list`); the nodes open it to hand it to the Worker.
A version has to **declare** which secrets it reads: wrangler's convention is to list them in the project's `.dev.vars.example` (only the names are taken; `.dev.vars` and `.env` are never read),
or `deploy --secret NAME`. A version that declares a secret without a value is refused at deployment, with a hint to `secret put` first (with `--no-activate` the upload is accepted, and its activation is refused, naming the secret, until the value is set). `cowctl secret list` lists only names, lengths and when each was set;
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
  (`cowctl tenant update acme --hostname site.acme.localhost`, which sets the tenant's whole list: name every hostname it keeps; a hostname belongs to one tenant only), then `cowctl deploy --route site.acme.localhost/`
  (then `curl -H 'Host: site.acme.localhost' http://127.0.0.1:18080/`). An unregistered hostname is refused at deployment with `deploy refused: hostname ... is not one of tenant acme's hostnames`, and a path under the default host that does not start with `/<tenant>/` with `deploy refused: the routes of tenant acme under the default host are */acme/...: route */... is outside them` (the API's code is `HOSTNAME_NOT_OWNED` for both);
- quotas: the number of Workers, versions per Worker, bundle size, the number and total size of static assets, the number of secrets, the number of hostnames and the log volume are all tenant quotas (set by the administrator, shown by `cowctl tenant get`);
  over one, the deployment, the secret or -- for the number of hostnames -- the administrator's tenant update is refused with a reason that names the quota (`deploy refused: tenant acme's quota: ...`, the API's code `QUOTA_EXCEEDED`), except the two log quotas, over which lines are dropped (section 4).
  The conversion itself refuses more than 20000 static assets, 100 MiB of them, or one over 25 MiB, and a package of 160 MiB or more -- both the default quota and the control plane's upload limit -- gets `PACKAGE_TOO_LARGE`.

`examples/` also has two official templates as they are: `email-routing-template` (static assets and the `ASSETS` Binding) and `x402-proxy-template` (secrets).
Each serves at `/` of a host of its own: register a hostname to the tenant as above, then in the template's directory `cowctl init --tenant acme --route <hostname>/` and `cowctl deploy`; `x402-proxy-template` also needs `npm ci` first and its secret, `cowctl secret put JWT_SECRET` after `init`;
each example's own `README.md` gives these commands for it.

## 10. Cleaning up

```bash
cowctl status                                                       # the control plane, the desired state's revisions, the nodes
(cd "$COWORKERD_PACKAGE/docker" && docker compose --profile cluster down -v)   # stop and delete all data
```

```text
$ cowctl status
control plane http://127.0.0.1:18090: coworkerd-apiserver 0.4.0 (source 9d8a0003edd8), tenants revision 9, credentials revision 3; tenant acme
node-1  active  coworkerd 0.4.0
node-2  active  coworkerd 0.4.0
node-3  active  coworkerd 0.4.0
```

`cowctl status` names the control plane, the revisions of the tenant index and the credentials index it holds (each change of the desired state is a new revision in the object store), and the nodes;
with the admin credential it also shows each node's processes and the revision of the deployment state the node serves. To stop without deleting: `down` without `-v`; at the next `up`, the tenants, credentials, deployments and secrets are all still there.

## 11. Commands

| Command | Purpose |
| --- | --- |
| `cowctl init --tenant T [--url URL] [--ingress URL] [--token-file FILE] [--route PREFIX] [--force]` | Write `cowctl.json` |
| `cowctl deploy [--no-activate] [--version ID] [--route PREFIX] [--secret NAME]... [--no-wait]` | Build, upload a new version, trial-load, switch, wait for the nodes |
| `cowctl versions` | The versions; `*` marks the active one |
| `cowctl rollback <version> [--no-wait]` | Make a version the active one |
| `cowctl remove [--yes]` | Stop serving this Worker (its versions are kept; `deploy` again restores it) |
| `cowctl secret put <NAME> [--from-file FILE] [--no-wait]`, `secret list`, `secret delete <NAME>` | Secrets |
| `cowctl status` | The control plane, the desired state's revisions, the cluster's nodes and their versions (with the admin credential also their processes and the deployment revision they serve) |
| `cowctl logs [--follow] [--since 10m] [--limit N] [--json]` | This Worker's logs |
| `cowctl usage [--since 24h] [--until TIME] [--all]` | Hourly usage |
| `cowctl tenant create\|list\|get\|update\|delete` (administrator) | Tenants: hostnames (`--hostname`), quotas (`--quota name=value`), default limits |
| `cowctl credential create\|list\|revoke` (administrator; a tenant can revoke its own) | Tenant credentials |
| `cowctl node list` (administrator) | Each node's processes, sandbox, rotation, suspects |

`--tenant T` names the tenant (default from `cowctl.json`), `--worker NAME` the Worker (default from `cowctl.json`, then `name` in the wrangler configuration);
`COWCTL_CONTROL_PLANE` overrides the control plane's address. On failure it prints one sentence plus the reason the control plane gave (cleaned: no secret values or absolute paths) and exits with 1; a usage error exits with 2, an interrupted command with 130.

## 12. Troubleshooting

| Symptom | Remedy |
| --- | --- |
| `cannot reach the control plane at http://127.0.0.1:18090` | The cluster is not up, or the control plane's port differs: `docker compose ps`; after changing `COWORKERD_CONTROL_PORT` in `.env`, `cowctl init --tenant T --url http://127.0.0.1:<port> --force` (it writes the whole `cowctl.json` again), or set `COWCTL_CONTROL_PLANE` |
| `no credential` / `refused the credential` | Step 2: put the tenant credential into `~/.cowctl/token`, or `export COWCTL_TOKEN=...`; a revoked credential no longer works |
| `FORBIDDEN` | A tenant credential acts only on its own tenant; `tenant` (except reading its own record), `credential create` and `credential list`, `node` and `usage --all` need the admin credential |
| `TENANT_NOT_FOUND` | The tenant has not been created yet (`cowctl tenant create`), or the name is misspelled |
| `the project does not convert: Unsupported Wrangler configuration: ...` | The listed settings are not supported by this platform, see the [compatibility scope](compat.md) |
| `deploy refused: version vN does not load (SYNTAX_ERROR ...)` | The new version cannot be loaded and the active version did not change; reasons and categories are in [error codes](errors.md) |
| `deploy refused: Worker ... would be invalid after upsert: missing secret: NAME` | `cowctl secret put NAME` first |
| `deploy refused: tenant ...'s quota` / `deploy refused: hostname ... is not one of tenant ...'s hostnames` | Over the tenant's quota, or the route's hostname does not belong to this tenant: see section 9 |
| `curl` right after `cowctl deploy --no-wait` (or after a deploy that said `not served yet by ...`) gets 404 `ROUTE_NOT_FOUND`, or the previous version | The nodes have not read the deployment yet: they do every 5 seconds. Wait a few seconds and try again |
| `curl` gets 404 `ROUTE_NOT_FOUND` | The path is not under any route: see the routes at the end of `cowctl versions` (by default `/<tenant>/<Worker>`) |
| `curl` gets 503 | The request was refused or did not finish: the JSON answer's `code` says why -- a limit exceeded while running, but also a refusal before anything ran (`WORKER_INVALID`, `TENANT_UNAVAILABLE`, `SERVER_DRAINING`, `PROCESS_UNAVAILABLE` and others), see [error codes](errors.md); if it ran, `cowctl logs` shows what it printed |

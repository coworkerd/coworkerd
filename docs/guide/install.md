# Installing and running Coworkerd

> For release 0.3.1.
> For platform operators meeting Coworkerd for the first time: follow this page to start a cluster on your own machine, create a tenant, deploy a Worker and send the first request.
> Tenants, credentials, deployments, logs, new versions, rollbacks, secrets and usage are done with the command line `cowctl`, see the [quickstart](quickstart.md); this page covers installation, startup, configuration and operations.

## 1. What it is, and what the release contains

Coworkerd is a Workers infrastructure layer for platforms, based on Cloudflare `workerd`: one cluster runs all the Workers of one platform, the platform is the only administrator,
the platform's users are **tenants**, and Workers belong to tenants (full name `tenant/Worker`). Each **node** is one `coworkerd` process (the node daemon) and the
`workerd` processes it manages; nodes find each other through etcd, Workers' code bundles live in object storage (S3 API), and a request enters through any node's ingress
and runs on that node or another one according to the placement rules. The **control plane** `coworkerd-apiserver` is the only writer of the desired state (tenants, credentials, deployments), and `cowctl` talks only to it;
nodes only read the desired state and only write their own membership, suspects and usage (section 4, "etcd roles, users and authentication").

Each version comes in two architectures (`linux/amd64`, `linux/arm64`), two files each, plus one checksum file:

| File | Contents |
| --- | --- |
| `coworkerd-0.3.1-linux-<arch>.tar.gz` | `bin/` (`coworkerd`: the node daemon (`coworkerd check` is its self-check, including the object storage check `check storage`); `coworkerd-apiserver`: the control plane (also the internal tool `tool`); `coworkerd-launcher`: the launcher that starts workerd in a least-privilege environment (see "Sandbox" in section 4); `cowctl`: the command line; `workerd`; `coworkerd-syscalls-startup.txt`, `coworkerd-syscalls-steady.txt`, `coworkerd-syscalls.json`: the system call allowlist of this workerd and the record of how it was generated, see "System call allowlist" in section 4), `lib/` (`cowctl` and the wrangler adapter with their dependencies), `runtime/runtime.js`, `docker/` (the compose file, the entry point script, the Dockerfile, `.env.example`, `examples/hello/`: the example project the compose `tool` service mounts by default), `examples/` (hello-world and two official templates), `docs/` (this document, the quickstart, the compatibility scope, the error codes), `INSTALL.md` (a copy of this document; read it first after unpacking), `VERSION`, `SHA256SUMS` of the files in the package |
| `coworkerd-0.3.1-linux-<arch>-image.tar.gz` | The image `coworkerd:0.3.1`, for `docker load` |
| `coworkerd-0.3.1-linux-<arch>.json` | The release record: which sources and binaries this architecture's package was built from, each with its SHA-256 |
| `SHA256SUMS` | The SHA-256 of each file above (both architectures) |

A Mac (Apple silicon) uses `arm64`; most Linux servers use `amd64` (`uname -m` is `x86_64`).

Requirements: **Docker Engine 28 or newer** (compose's internal network uses `gateway_mode_ipv4: isolated`, which only exists from 28.0; 27.x rejects it) and Docker Compose v2 (tested with v5.1 and v5.6; Docker Desktop on the Mac includes it; on Linux it is enough that `docker compose version`
prints a version, otherwise install `docker-compose-plugin`). The image download is about 75 MB, about 320 MB once loaded; an idle cluster takes about 110 MiB of memory (one node: the node 47, object storage 46, etcd 15) up to 200 MiB (three nodes) (measured on a pre-release build, not measured again on 0.3.0).

## 2. Option one: compose, one node (one command)

```bash
# 1. Verify, unpack, load the image (amd64 as the example)
sha256sum -c --ignore-missing SHA256SUMS      # skips the other architecture's lines when you downloaded only one; on a Mac: shasum -a 256 -c --ignore-missing SHA256SUMS
tar -xzf coworkerd-0.3.1-linux-amd64.tar.gz
docker load -i coworkerd-0.3.1-linux-amd64-image.tar.gz
cd coworkerd-0.3.1-linux-amd64/docker

# 2. Start the cluster: etcd, object storage, initialization, one node and the control plane; --wait waits until the ingress is ready (usually 20-30 seconds)
docker compose up -d --wait

# 3. The administrator creates a tenant and a credential, deploys hello-world and sends the first request: with cowctl, see the quickstart (quickstart.md), sections 2-3
```

On the first start, the `secrets` container (no network, exits once it has generated everything) generates all of the cluster's credentials in the data volume named `secrets`: the two object storage identities
(the read-write identity for deployments, the read-only identity for nodes), the **management token** (used by the control plane, the operations tool and operators calling a node; nodes sign their own sessions with the cluster key), the **admin credential** (the administrator of `/v1`, used by `cowctl tenant`
and `cowctl credential`), the **cluster key** nodes sign with, and the etcd users (root, one for the control plane, one per node), and puts them into each service's own
directory on a "whoever reads it gets it" basis -- each service mounts only its own directory: nodes have only the read-only storage identity, their own etcd user, the cluster key and the management token; the control plane (and `tool`) has only the deployment identity, its etcd
user, the management token and the admin credential; etcd's root exists only in the initialization container. The initialization container `init` then creates the bucket and checks it (the deployment identity writes, the node identity reads back, unsigned
requests are refused), creates the **roles and users** in etcd for the v2 layout and **turns on etcd authentication** (the control plane writes `tenants/`, `credentials/` and `deploy/`, and each node writes only its own
`nodes/<nodeId>/`; from then on anonymous requests are refused, see "etcd roles, users and authentication" in section 4), and then writes an empty deployment state. Every later `up` runs the checks again and leaves existing credentials,
users and state untouched.

Common operations:

| What | Command |
| --- | --- |
| Show the admin credential | `docker compose run --rm tool admin-credential` (for `cowctl`'s administrator subcommands: `COWCTL_TOKEN=$(...) cowctl tenant list`) |
| Show the management token | `docker compose run --rm tool token` |
| Show the nodes' state | `COWCTL_TOKEN=<admin credential> cowctl node list`; or read a node directly: `curl -H "Authorization: Bearer $(docker compose run --rm tool token)" http://127.0.0.1:18080/__coworkerd/state` |
| Show logs | One Worker's: `cowctl logs` (see section 4 of the quickstart); the node's own: `docker compose logs -f node-1`; all of workerd's output is in `logs/workerd.log` of the node's data volume (tenant `console.*` output appears there as plain lines without attribution; the lines `cowctl logs` shows reach the node separately over workerd's per-Worker tail channel, attributed to the Worker that wrote them), rotated at 256 MiB x 4 |
| Stop (keep the data) | `docker compose down` |
| Stop and delete all data | `docker compose down -v` |
| Show the version | `docker run --rm coworkerd:0.3.1 version` |

To change ports, tokens or the project directory, copy `.env.example` to `.env` and edit it (every entry may be left out):

| Variable | Default | Effect |
| --- | --- | --- |
| `COWORKERD_BIND` | `127.0.0.1` | The local address the ingress ports bind to; set `0.0.0.0` to let other machines reach them |
| `COWORKERD_HTTP_PORT` (`_2`, `_3`) | `18080` (`18081`, `18082`) | The local port of node-1's (node-2's, node-3's) ingress |
| `COWORKERD_CONTROL_PORT` | `18090` | The local port of the control plane (the API `cowctl` connects to) |
| `COWORKERD_MANAGEMENT_TOKEN` | empty: the one initialization generated | Your own management token (16-4096 visible characters, no spaces); used from the control plane to the nodes and by the nodes' management endpoints |
| `COWORKERD_ADMIN_CREDENTIAL` | empty: the one initialization generated | Your own admin credential for `/v1` (same rules); used by `cowctl`'s administrator subcommands, do not give it to tenants |
| `COWORKERD_PROCESS_COUNT` | `1` | Ordinary workerd processes per node (plus 2 quarantine processes, see `quarantineProcesses` in section 5) |
| `COWORKERD_PROJECT_DIR` | `./examples/hello` | The local directory mounted at `/work` in the `tool` container (the project to deploy) |
| `COWORKERD_PROJECT` | `coworkerd` | The compose project name (to tell several clusters on one machine apart) |

## 3. Option two: compose, three nodes

```bash
docker compose --profile cluster up -d --wait
for port in 18080 18081 18082; do curl -s http://127.0.0.1:$port/acme/hello-world; done
# All three ingresses take requests; a Worker runs on its home node (computed from the full name tenant/Worker) unless the receiving node ran that version within the last 30 s; otherwise the request is forwarded
docker compose --profile cluster down -v      # stopping needs --profile cluster too
```

The three nodes find each other through etcd (a membership table of three, keys `/coworkerd/v2/nodes/<nodeId>`, `nodeId` being `node-1..3`) and connect to each other by container name on the internal network
(`node-N-peer:8934`). One deployment takes effect on all three nodes. The `secrets` container prepares, for each node in `COWORKERD_NODE_IDS` (default `node-1,node-2,node-3`), the directory
`nodes/<nodeId>/` in the `secrets` volume (its etcd user, the read-only storage identity, the cluster key and the management token), and the initialization container creates a user in etcd for each of them;
to add a node yourself, add the new `nodeId` to this list of `secrets`, mount the `nodes/<nodeId>` directory into the new node, and run `up` once more for `secrets` and `init`.

**The sandbox under compose**: the nodes' `sandbox.mode` defaults to `auto` -- each workerd starts through `coworkerd-launcher` in a least-privilege environment (see "Sandbox" in section 4),
and when the conditions are not met workerd starts directly with a warning. Docker's default seccomp profile does not let a process without `CAP_SYS_ADMIN` create namespaces, so **in a cluster started with the commands above the sandbox is off**:
each node's log has a line `[sandbox] {"event":"warning",...}`, the state endpoint's `processes.sandbox.active` is `false`, and everything else works as usual.
To turn it on, first meet the host prerequisites of section 4, then add the package's `docker-compose.sandbox.yml` (it only lifts the default seccomp and AppArmor profiles for `node-*`, adds
`no-new-privileges`, adds no capability, and sets `sandbox.mode` to `launcher`: a node that cannot set up the environment refuses to start instead of running without the sandbox):

```bash
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml up -d --wait
# or write COMPOSE_FILE=docker-compose.yml:docker-compose.sandbox.yml into .env, and every later compose command takes it
```

## 4. Option three: bare metal (Linux, systemd)

For running the nodes and the control plane directly on Linux hosts, with your own etcd and object storage.

**Preparation**

- etcd 3.5 or newer, with its HTTP gateway reachable (for example `http://10.0.0.5:2379`).
- S3-compatible object storage and a bucket (we tested SeaweedFS 3.97; **create the bucket first**, then deploy), and two credentials: one for deployments (read-write, for the control plane) and one for the nodes (read-only is enough).
  You can confirm them with the package's self-check:
  `AWS_ACCESS_KEY_ID=<deployment> AWS_SECRET_ACCESS_KEY=<deployment> COWORKERD_READ_ACCESS_KEY=<read-only> COWORKERD_READ_SECRET_KEY=<read-only> bin/coworkerd check storage http://storage:8333 coworkerd`
  (it creates the bucket, writes a probe object and reads it back; an unsigned request must be refused).
- An ordinary user, for example `useradd --system --home-dir /var/lib/coworkerd coworkerd`; the programs go into `/opt/coworkerd` (the unpacked `bin/` and `runtime/`).
- **The sandbox's prerequisites** (see "Sandbox" below): Linux allows unprivileged user namespaces; `kernel.yama.ptrace_scope` is 1 (Ubuntu's default) or stricter.

**Sandbox**: the node starts each workerd process through `bin/coworkerd-launcher`; the process runs in a read-only root directory holding only the few files it needs (a new mount namespace), without network interfaces
(a new network namespace with only `lo`, not brought up), as 65534 in a new user namespace, without any capability, with `no_new_privs`; when the node is killed, its workerd processes end with it.
Once ready, workerd marks itself non-dumpable (the bit the launcher sets is reset by `execve`, so workerd sets it again itself); a node's workerd processes share one outside uid, so keeping them from
attaching to each other or reading each other's memory also takes Yama's `kernel.yama.ptrace_scope=1` (attach only to descendants; Ubuntu's default, 2 and 3 are stricter) -- both layers are needed.
The launcher has to create user namespaces as an ordinary user and mount inside them. To check (run the launcher's own check as the user that runs the node; `ok` means the prerequisites are met; on failure it prints a line
`[coworkerd-launcher] {"event":"failed","step":...}` saying which step failed):

```bash
sysctl kernel.apparmor_restrict_unprivileged_userns     # no such key (Ubuntu 22.04 and others) or 0: not restricted by this
cat /proc/sys/user/max_user_namespaces                  # greater than 0
cat /proc/sys/kernel/yama/ptrace_scope                  # 1 or greater; when 0: echo 'kernel.yama.ptrace_scope = 1' | sudo tee /etc/sysctl.d/60-coworkerd-ptrace.conf && sudo sysctl --system
sudo -u coworkerd /opt/coworkerd/bin/coworkerd-launcher --user --net --ipc --uts --root /tmp --drop-caps --no-new-privs --check && echo ok
```

**Ubuntu 23.10 and later (including 24.04)** default to `kernel.apparmor_restrict_unprivileged_userns=1` (the two approaches below have not been tested; they will be finalized after verification on real 24.04 machines): a user namespace created by a process without an AppArmor permission has no capabilities in it,
and the check above fails (the `[coworkerd-launcher]` line it prints says where it stopped). Pick one:

- Permit only the launcher (recommended): `/etc/apparmor.d/coworkerd-launcher`

  ```
  abi <abi/4.0>,
  include <tunables/global>
  profile coworkerd-launcher /opt/coworkerd/bin/coworkerd-launcher flags=(unconfined) {
    userns,
  }
  ```

  then `sudo apparmor_parser -r /etc/apparmor.d/coworkerd-launcher` (loaded automatically after a reboot);
- or lift the restriction for the whole machine: `echo 'kernel.apparmor_restrict_unprivileged_userns = 0' | sudo tee /etc/sysctl.d/60-coworkerd-userns.conf && sudo sysctl --system`.

At startup the node prints a line `[sandbox] {...}`: `active` being `true` means it is in effect; the state endpoint's `processes.sandbox` says the same, and each process's `sandbox.observed.layers` are the layers the node
read from `/proc`. `sandbox.mode` defaults to `auto`: when the prerequisites are not met, workerd starts without the sandbox and a separate warning line is printed; `launcher` refuses to start when the prerequisites are not met (recommended for production);
`off` turns it off.

**System call allowlist**: the last layer of the sandbox. Before handing the process to workerd, the launcher installs a system call allowlist, and the allowlist **is generated by the build from profiles of this workerd**
(`bin/coworkerd-syscalls-startup.txt` and `coworkerd-syscalls-steady.txt`, shipped in the package; the `syscalls` section of the release record `.json` holds their hashes and the hash of the
workerd they belong to), not written into the code -- following upstream, changing the compiler or changing build options can all change the set. The comment at the top of the file says which workerd it was generated for; when the launcher finds that it does not match the binary
it is about to run, it refuses to start that generation (the state endpoint's `lastExitCategory` is `launch-failed`, with both hashes in the reason), and when `sandbox.mode` is `launcher` the server
refuses to start altogether.

Three modes (`sandbox.syscalls.mode`, default `observe`):

| Mode | Behaviour | When to use it |
| --- | --- | --- |
| `off` | No filter is installed | Temporarily, while investigating a sandbox problem |
| `observe` | A call outside the allowlist **is first reported to the server, then executed as usual**; the process behaves the same (the kernel's "notify the supervisor, then allow", from 5.5; the supervisor is the local node process) | The default. Run a new allowlist this way first |
| `enforce` | A call outside the allowlist **kills the whole process**; the server classifies such an exit as `syscall-denied` and counts it into that slot's restart backoff, and the tenant requests in flight at the time get 503 | After the observations are confirmed to be zero |

**Before switching from `observe` to `enforce`**, do two things:

1. run a complete regression (not just a core subset), with zero observations;
2. soak with real traffic for a while (for example 2 hours of mixed load), still with zero observations.

Both look at the state endpoint's `processes.sandbox.syscalls`:

```bash
# The management endpoints need the token (section 8; for a compose cluster get it with docker compose run --rm tool token)
curl -s -H "Authorization: Bearer $TOKEN" localhost:8930/__coworkerd/state \
  | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["processes"]["sandbox"]["syscalls"], indent=1, ensure_ascii=False))'
```

| Field | Meaning |
| --- | --- |
| `mode`, `active`, `reason` | The configured mode; whether it is really installed at every start; why it was not installed |
| `allowlist.path`, `sha256`, `calls`, `workerdSha256s`, `workerdSha256Running`, `workerdSha256Matched` | Which file was read, its hash, how many calls, the workerd binaries it covers, the one actually running on this machine, and which one matched (the running one must be in the list) |
| `notifications`, `observed[]` | The calls reported in observe mode: the total count, and the list deduplicated by call number (call number, name, count, first and last time, thread names, which processes) |
| `denied`, `recentDenials[]` | The number of processes the filter killed in enforce mode, and the latest records |
| `interrupted`, `errors` | How often the thread was already interrupted by a signal while a notification was being produced; how often the supervision itself failed (should be 0) |

**What to do about an observation**: each record is a log line `[syscall-observed] {...}` (rate-limited to one line per call number per 10 seconds; the counts are not affected), with the fields of `observed[]` in the table above.
Steps:

1. Tell from `name`/`call` and `thread` who made it (thread names tell V8's worker threads, our watchdog thread and the main thread apart);
2. confirm whether it is **really needed** (for example a path only some load reaches, a connection only some generation opens) or a sampling artifact;
3. if it is really needed, generate the allowlist again with it as an "always allowed" entry and the reason -- that is what `--relax <call name>` of the generator `scripts/syscall-allowlist.py` does (the reason is recorded in
   `evidence` of `allowlist.json`);
4. after putting the new file in place, restart the server (the allowlist is read and fixed at startup), then repeat the two steps above to confirm the count is back to zero.

**Replacing workerd: the allowlist first, then the binary**. The allowlist file says which workerd binaries it belongs to (one `# workerd-sha256` line each), and the launcher **refuses** to start bytes the allowlist does not cover
-- exactly the check that prevents "a new binary with the old allowlist". So, to upgrade workerd:

1. first put in place an allowlist that **covers both the old and the new bytes** (`--workerd` of the generator `scripts/syscall-allowlist.py` can be given several times, for example
   `--workerd old/workerd --workerd new/workerd`; the one in the release package covers only the package's own binary);
2. then replace the workerd file -- the server then rotates slot by slot as usual ("the binary changed, so rotate"), and the new generation's launcher reads an allowlist that already covers it;
3. once done, replace the allowlist with the one that covers only the new bytes (the one in the release package); it takes effect at the server's next restart.

Two details: the **launcher reads** the allowlist **at every start**, so step 1 takes effect for every later start as soon as the file is in place; the server reads it only **once, at startup**, for reporting and for checking the pair, so
`allowlist.workerdSha256s` in the state endpoint changes only after the server restarts. An upgrade of the whole package (the node and workerd together) does not need this order: stop and replace everything as in section 9.

The launcher installs the startup table, which has to contain the calls used only during startup (the filter stays in force after `execve`). The narrower steady-state table is installed by workerd itself once it is ready, with its own mode:
`sandbox.syscalls.steady.mode` (`off`/`observe`/`enforce`, default `off`; environment variable `COWORKERD_SANDBOX_SYSCALLS_STEADY`); once it is installed workerd writes a line `[coworkerd-seccomp]`, and the state endpoint's
`sandbox.syscalls.steady` of each process reports the mode and the table's hash. Switch it in the same order as the startup table: `observe` first, `enforce` after zero records.

**etcd roles, users and authentication**: only the control plane can write the desired state and each node writes only the keys under its own name; etcd's own role permissions enforce this, not a convention;
with authentication on, anonymous requests are always refused. The layout and the permissions (under `/coworkerd/v2/`):

| Role | Permissions | For |
| --- | --- | --- |
| `coworkerd-apiserver` | `tenants/`, `credentials/`, `deploy/` read-write; `nodes/` read-only | The user `coworkerd-apiserver` (the control plane; also what the internal tool `tool` uses) |
| `coworkerd-node` | `deploy/`, `tenants/`, `nodes/` read-only (`credentials/` is not readable) | Every node's user |
| `coworkerd-node-<nodeId>` | The key `nodes/<nodeId>` and the prefix `nodes/<nodeId>/` read-write (membership lease, suspects, usage roll-ups) | The user `coworkerd-node-<nodeId>` |
| `coworkerd-health` | No permission at all | The user `coworkerd-health`: compose's etcd health check uses it (etcd counts a refused read as healthy); its password is its user name and is no secret |
| `root` (built into etcd) | Everything | The user `root`: used only to create users and turn authentication on |

etcd's authorization is a union of key ranges without "except", so "read-write on all of v2 but `nodes/` read-only" is written as three read-write prefixes plus `nodes/` read-only; one node's own range is two entries
(the membership key itself and the prefix below it), so that `node-1` cannot write `node-10`'s keys.

First prepare a password file `{"user":"...","password":"..."}` each for root, the control plane and every node (mode 0600, random passwords, for example `openssl rand -hex 24`),
then create everything and turn authentication on with one command (it can be run again: each time it resets the passwords from the files and revokes permissions outside the plan; once authentication is on it logs in as root):

```bash
coworkerd-apiserver tool etcd-auth --endpoint http://10.0.0.5:2379 --root /etc/coworkerd/etcd/root.json \
  --apiserver /etc/coworkerd/etcd/apiserver.json --node node-1=/etc/coworkerd/etcd/node-1.json --node node-2=... --node node-3=...
# the user in each file: root.json is root, apiserver.json is coworkerd-apiserver, node-1.json is coworkerd-node-node-1 (give it to that node)
```

The equivalent by hand with `etcdctl` (for `node-1`, turning authentication on last):

```bash
etcdctl user add root && etcdctl user grant-role root root
etcdctl role add coworkerd-apiserver
for p in tenants credentials deploy; do etcdctl role grant-permission coworkerd-apiserver readwrite --prefix=true /coworkerd/v2/$p/; done
etcdctl role grant-permission coworkerd-apiserver read --prefix=true /coworkerd/v2/nodes/
etcdctl role add coworkerd-node
for p in deploy tenants nodes; do etcdctl role grant-permission coworkerd-node read --prefix=true /coworkerd/v2/$p/; done
etcdctl role add coworkerd-node-node-1
etcdctl role grant-permission coworkerd-node-node-1 readwrite /coworkerd/v2/nodes/node-1
etcdctl role grant-permission coworkerd-node-node-1 readwrite --prefix=true /coworkerd/v2/nodes/node-1/
etcdctl user add coworkerd-apiserver && etcdctl user grant-role coworkerd-apiserver coworkerd-apiserver
etcdctl user add coworkerd-node-node-1 && etcdctl user grant-role coworkerd-node-node-1 coworkerd-node && etcdctl user grant-role coworkerd-node-node-1 coworkerd-node-node-1
etcdctl auth enable
```

The nodes and the control plane log in to etcd as their own users: the configuration file says `"etcdCredentialsFile": "/etc/coworkerd/etcd.json"` (each its own file, owned by coworkerd, 0600), and the deployment tool reads the
environment variable `COWORKERD_ETCD_CREDENTIALS_FILE`; when etcd refuses the token they authenticate again and read the file again (also when a watch is the first to be refused), so changing a password needs no restart (section 12). compose's `init`
runs the same `tool etcd-auth` at every start with the files `secrets` generated.

**The configuration file** `/etc/coworkerd/config.json` (at minimum the nodeId, etcd and the storage endpoint; the rest in section 5):

```json
{
  "nodeId": "node-1",
  "etcdEndpoint": "http://10.0.0.5:2379",
  "advertisedAddress": "10.0.0.11:8934",
  "storage": {
    "endpoint": "http://10.0.0.6:8333",
    "credentialsFile": "/etc/coworkerd/storage.json"
  }
}
```

`/etc/coworkerd/storage.json` (owned by coworkerd, mode 0600): `{"accessKey": "...", "secretKey": "..."}` (the node's read-only identity).
The management token goes into the environment file `/etc/coworkerd/env` (mode 0600): `COWORKERD_MANAGEMENT_TOKEN=...`; do not put it on the command line.
`nodeId` is this node's name in the cluster and also part of its etcd user name and key prefix: do not change it once set (a changed one is a new node, which needs its etcd user first).

**The control plane** `coworkerd-apiserver --config /etc/coworkerd/control.json` reads the same kind of configuration file; it uses `etcdEndpoint`, `storage` (the deployment **read-write** identity)
and the `control` section: `control.listenAddress` (default `0.0.0.0:8940`), `control.adminCredential` (the admin credential, 16-4096 visible characters; better put into the environment file as
`COWORKERD_ADMIN_CREDENTIAL`), `control.pathRouting` (default `true`: tenants may route `/<tenant>/...` under the cluster's default host; with `false` only the hostnames registered to them),
`control.maxUploadBytes`. It also needs `COWORKERD_MANAGEMENT_TOKEN` (to call the nodes' trial load and read their state and logs). The admin credential is the platform's own;
tenant credentials are created with it through `/v1` (`cowctl credential create --tenant T`), and etcd stores only their hashes.

**The systemd unit** `/etc/systemd/system/coworkerd.service`:

```ini
[Unit]
Description=Coworkerd node
After=network-online.target
Wants=network-online.target

[Service]
User=coworkerd
Group=coworkerd
EnvironmentFile=/etc/coworkerd/env
ExecStart=/opt/coworkerd/bin/coworkerd --config /etc/coworkerd/config.json
# SIGTERM starts the drain (at most 30 seconds by default); the node exits once drained
KillSignal=SIGTERM
KillMode=mixed
TimeoutStopSec=45
Restart=on-failure
StateDirectory=coworkerd
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload && systemctl enable --now coworkerd
curl http://127.0.0.1:8930/__coworkerd/ready        # ready
```

At startup the node prints one line with the effective configuration (`[server] {"event":"configuration",...}`; credentials and tokens show only whether they are set); without a token it prints a reminder as well.
Before its first deployment, a brand-new cluster needs a deployment state in etcd (a node does not start on an empty state, so that it does not serve as usual after connecting to the wrong etcd): write an empty initial state with the internal tool,
with the manifest `{"compatibilityDate":"2026-10-01","routes":[],"workers":[]}`:
`AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... coworkerd-apiserver tool upsert --endpoint http://10.0.0.5:2379 --storage-endpoint http://10.0.0.6:8333 --manifest empty.json --no-trial`
(this is the step compose's initialization container performs). From then on, tenants, credentials and deployments go through the control plane (`cowctl`).

## 5. Configuration reference

The configuration file is a JSON object (unknown fields are refused). The table lists the common entries; `coworkerd --config FILE` reads it (the control plane `coworkerd-apiserver --config FILE` reads the same kind of file).

| Field | Default | Description |
| --- | --- | --- |
| `nodeId` | required | This node's name in the cluster (letters, digits, `-_.`); also its etcd user `coworkerd-node-<nodeId>` and its key prefix `nodes/<nodeId>/` |
| `etcdEndpoint` | required | etcd's HTTP gateway; the membership table, the tenants and the deployment state are all there (`/coworkerd/v2/`) |
| `storage.endpoint` | required | The object storage's S3 endpoint |
| `storage.bucket` / `storage.region` | `coworkerd` / `us-east-1` | The bucket and the region |
| `storage.credentialsFile` | none: the environment variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | The node's read-only credential file `{"accessKey","secretKey"}` (the control plane uses the deployment read-write identity) |
| `storage.cacheDirectory` / `storage.cacheMaxBytes` | `<stack directory>/cache` / 64 MiB | The local code bundle cache |
| `stackDirectory` | `/var/lib/coworkerd` | The stack directory: `logs/`, `run/` (the Unix sockets shared with workerd, mode 0700), `cache/`, `processes/`, and on Linux with the sandbox not `off` `sandbox/` (0700) |
| `workerdPath` | `workerd` in the directory of `coworkerd` | The workerd executable |
| `runtimePath` | `../runtime/runtime.js` (relative to the directory of `coworkerd`) | The runtime script |
| `listenAddress` | `0.0.0.0:8930` | The ingress (also the management endpoints) |
| `peerListenAddress` | `0.0.0.0:8934` | The peer listener (calls from other nodes, trial loads during deployment) |
| `advertisedAddress` | `<hostname>:<peer port>` | The address other nodes use to reach this node; they must be able to connect to it |
| `management.token` | none | The management token (see section 8); the environment variable `COWORKERD_MANAGEMENT_TOKEN` takes precedence |
| `processPool.count` | `1` | The number of ordinary workerd processes |
| `quarantineProcesses` | `2` | The number of quarantine processes. A **tenant** identified as the culprit (all its Workers) is kept here for an observation period, assigned to one of them by the tenant's hash, so that a tenant crashing a quarantine process does not affect the tenants in another shard; deployment **trial loads** run here too (in the target tenant's shard), so that a new version crashing a process does not take the in-flight requests of the ordinary processes with it. `0`: no quarantine processes; suspects and trial loads both fall back to the ordinary processes (not recommended) |
| `cpuLimitMs` / `memoryLimitMiB` | `200` / `128` | The CPU limit per request and the memory limit per isolate (the node's defaults; a limit the Worker declares takes precedence, see the end of this section) |
| `idleEvictMs` | `300000` | How long a Worker may stay idle before it is evicted |
| `drainTimeoutMs` | `30000` | How long to wait for in-flight calls when exiting; `0` turns the drain off |
| `rotation.*` | see the schema | Process rotation: `binaryCheckMs` (rotate when the workerd file changes, default 1000), `maxAgeMs` (default `3600000`: every process is replaced by a new generation every hour, with in-flight requests completing in the old process; `0` turns it off), `maxRequests`, `drainTimeoutMs` and others |
| `sandbox.mode` | `auto` | The least-privilege environment of workerd ("Sandbox" in section 4): `auto` turns it on when the prerequisites are met, otherwise runs without it and warns; `launcher` refuses to start without them; `off` turns it off |
| `sandbox.launcherPath` | `coworkerd-launcher` in the directory of `coworkerd` | The launcher. The node checks and fixes it at startup: replacing the file while running triggers no rotation, and the state endpoint's `processes.sandbox.pendingRestart` says the new one is used only after a restart |
| `sandbox.syscalls.mode` | `observe` | The system call allowlist ("System call allowlist" in section 4): `off` installs nothing; `observe` reports calls outside the allowlist and executes them as usual; `enforce` kills the process on a call outside the allowlist (exit category `syscall-denied`). The environment variable `COWORKERD_SANDBOX_SYSCALLS` overrides it |
| `sandbox.syscalls.allowlistPath` | `coworkerd-syscalls-startup.txt` in the directory of `coworkerd` | The allowlist file (shipped in the release package, generated by the build for this workerd). The environment variable `COWORKERD_SANDBOX_ALLOWLIST` overrides it |
| `loadCheck` | `true` | Check that a Worker loads before its first dispatch, and refuse explicitly when it does not (`LOAD_FAILED`) |
| `logs.tenantBufferBytes` / `logs.linesPerSecond` | `1048576` / `100` | How much log each tenant keeps on this node and how many lines per second are recorded; `logBufferBytes` and `logLinesPerSecond` of the tenant's quota take precedence |
| `usage.flushIntervalMs` / `usage.retentionHours` / `usage.sampleIntervalMs` | `60000` / `744` / `1000` | Per-tenant usage: how often the current hour's counts are written to etcd (a stopped node loses at most that much), how long its own hourly rows are kept (`0`: never deleted), the sampling interval of isolate time |
| `egress.mode` | `on` | Outgoing requests of tenant code (section 11): `on` goes through this node's egress service; `off` refuses every outgoing request at the policy point (`EGRESS_DENIED: egress-off`). The environment variable `COWORKERD_EGRESS_MODE` overrides it |
| `egress.defaultPolicy` / `egress.publicPorts` | `public` / `[80, 443]` | Where tenants without their own rules may go: `public`, these ports of public addresses; `deny`, nowhere. The environment variable `COWORKERD_EGRESS_DEFAULT_POLICY` overrides it |
| `egress.tenants[]` | none | Each tenant's rules: `tenant` (the tenant name), `allow`, `deny`, `allowInternal` (internal destinations the administrator allows), `concurrency`, `requestsPerSecond` (section 11) |
| `egress.clusterAddresses` | none | Cluster addresses the node cannot see itself (the control plane, load balancers, the nodes' other networks): `HOST[:PORT]`, an address or a CIDR; like etcd, object storage and the members, tenants may not reach them |
| `egress.timeoutMs` / `concurrency` / `requestsPerSecond` | `30000` / `32` / `100` | The timeout of each outgoing request; each tenant's outgoing concurrency and requests per second on each node |
| `egress.dnsCacheMs` / `caFile` / `systemTrustStore` | `10000` / none / `true` | How long a resolution result may be reused at most (`0`: resolve every time); extra PEM certificates trusted for HTTPS destinations; whether the system's root certificates are trusted |
| `peerAuth.keyFile` / `peerAuth.windowMs` | none / `30000` | The cluster key file (section 12): when set, peer connections are signed and only signed ones accepted; the time window. The environment variable `COWORKERD_PEER_KEY_FILE` overrides it |
| `etcdCredentialsFile` | none | etcd's user and password file `{"user","password"}` (mode 0600, "etcd roles, users and authentication" in section 4); the environment variable `COWORKERD_ETCD_CREDENTIALS_FILE` overrides it. The control plane reads the same field |
| `placement.*`, `membership.*`, `desiredState.*` | see the schema | Detailed parameters of placement, membership and the deployment state; `etcdEndpoint` and `storage` are their shorthands, and when both are written they must agree. Entries in `placements` and `peers` use `nodeId`, and Workers their full name `tenant/Worker` |
| `control.*` | see "The control plane" in section 4 | Used only by `coworkerd-apiserver` (`listenAddress`, `adminCredential`, `pathRouting`, `maxUploadBytes`, `serverTimeoutMs`), but every daemon validates the section: a malformed `control` section stops a node too |

All fields are in `ServerConfig` of the source `common/schema/coworkerd.capnp`. Fields with `ForTest` are for tests only; do not set them in production.

**Per-Worker limits**: the CPU and memory limits are also properties of the Worker (`limits` of `PATCH /v1/tenants/{t}/workers/{w}`, at most the tenant's
`maxWorkerCpuMs` and `maxWorkerMemoryMiB`; when absent, the tenant's default limits, and when those are absent, the node's `cpuLimitMs` and `memoryLimitMiB`), recorded in etcd with the active version.
**Nodes apply them per Worker**: each isolate in the same workerd process uses its own Worker's limits (each Worker's `applied` in the state endpoint is the value in effect); a Worker that declares no limits uses the node's configuration. The process-wide WebAssembly memory limit of a generation is the largest memory limit any Worker of the deployment declares, or the node's `memoryLimitMiB` if that is larger; a newly started generation starts with it.

**Environment variable overrides** (take precedence over the configuration file): `COWORKERD_MANAGEMENT_TOKEN`, `COWORKERD_ADMIN_CREDENTIAL` (control plane), `COWORKERD_TENANT_LOG_BUFFER_BYTES`, `COWORKERD_DRAIN_TIMEOUT_MS`, `COWORKERD_LOAD_CHECK` (`0`/`1`),
`COWORKERD_CPU_LIMIT_MS`, `COWORKERD_MEMORY_LIMIT_MIB`, `COWORKERD_QUARANTINE_PROCESSES`, `COWORKERD_IDLE_EVICT_MS`,
`COWORKERD_MEMORY_WATERMARK_ENABLED`, `COWORKERD_ROTATION_*`, `COWORKERD_SANDBOX_MODE`, `COWORKERD_LEASE_TTL_MS`, `COWORKERD_RUNTIME_FILE`,
`COWORKERD_EGRESS_MODE`, `COWORKERD_EGRESS_DEFAULT_POLICY`, `COWORKERD_PEER_KEY_FILE`, `COWORKERD_ETCD_CREDENTIALS_FILE`.

**The image's entry point script** writes a configuration file from environment variables (to `/var/lib/coworkerd/config.json`) and then starts: `COWORKERD_NODE_ID`,
`COWORKERD_ETCD_ENDPOINT`, `COWORKERD_STORAGE_ENDPOINT` (these three are required), `COWORKERD_STORAGE_BUCKET`, `COWORKERD_STORAGE_REGION`,
`COWORKERD_STORAGE_CREDENTIALS_FILE`, `COWORKERD_PEER_ADDRESS`, `COWORKERD_LISTEN_ADDRESS`, `COWORKERD_PEER_LISTEN_ADDRESS`,
`COWORKERD_PROCESS_COUNT`, `COWORKERD_STACK_DIR`, as well as `COWORKERD_ETCD_CREDENTIALS_FILE` and `COWORKERD_PEER_KEY_FILE` (by default the
`etcd.json` and `cluster-key.json` in the secrets directory, used when present) and `COWORKERD_CONTROL_ADDRESS` (the control plane's address, written into `egress.clusterAddresses`, default `apiserver:8940`);
with `COWORKERD_CONFIG=<file>` set, that file is used directly. Both ways give the same configuration.

## 6. Tenants and deployments

**Tenants** are the platform's users: the administrator (the platform) creates a tenant with the admin credential, registers hostnames for it, sets its quota, and then issues it a tenant credential; the tenant deploys its own Workers with its own credential.
`cowctl tenant create|list|get|update|delete` and `cowctl credential create|list|revoke` are the administrator's subcommands (see sections 2 and 11 of the [quickstart](quickstart.md));
the API is `/v1/tenants/...` (the control plane, compose's `apiserver` service, local port `18090`), and each request carries `Authorization: Bearer <credential>`.

| Tenant quota (`--quota name=value`) | Default | Where it is enforced |
| --- | --- | --- |
| `workers`, `versionsPerWorker` | 100, 50 | In the control plane when a new version is uploaded (when full, delete old versions first: the first version does no automatic cleanup) |
| `packageBytes`, `assetFiles`, `assetBytes` | 160 MiB, 20000, 100 MiB | In the control plane on upload |
| `secrets`, `hostnames` | 1000, 100 | In the control plane when a secret is set or a hostname registered |
| `logLinesPerSecond`, `logBufferBytes` | 100, 1 MiB | On each node, per tenant (lines over the limit are counted in `[dropped]`; requests are not affected) |
| `maxWorkerCpuMs`, `maxWorkerMemoryMiB` | 30000, 512 | In the control plane when Worker-level limits are set (the limits themselves: see the end of section 5) |

Over a quota gives `422 QUOTA_EXCEEDED`, with a `reason` that starts with the quota's name ([error codes](errors.md)). **Hostnames**: a hostname belongs to one tenant only (`cowctl tenant update T --hostname H`);
a tenant's routes may use only the hostnames registered to it, or paths starting with `/<tenant>/` under the cluster's default host `*` (not the latter when `control.pathRouting` is `false`), otherwise `403 HOSTNAME_NOT_OWNED`. A request whose hostname is registered to a tenant is matched against that tenant's routes only, and a path prefix matches whole path segments (`/acme` does not take `/acme-corp/...`).
**Deleting a tenant** (`cowctl tenant delete T`): it is first marked `deleting` (every node stops serving it immediately and requests get `503 TENANT_UNAVAILABLE`), then its routes, active pointers, secrets,
definitions, bundles and assets in object storage and credentials are deleted, and finally the tenant itself; a failure on the way answers `503 DELETION_INCOMPLETE`, and deleting again continues from where it stopped.

**Deploying with `cowctl`**: in the directory of a wrangler project, `cowctl init --tenant T` and `cowctl deploy`, then `logs`, `versions`, `rollback`, `secret put` and `usage` --
see the [quickstart](quickstart.md). It talks only to the control plane and needs neither the addresses nor the credentials of etcd and object storage, only the tenant credential; which wrangler features work is in the [compatibility scope](compat.md).

**The internal tool** (for operations and tests): `coworkerd-apiserver tool <apply|upsert|remove|show|compile|secret|rotate|drain|etcd-auth> ...`
writes etcd and object storage directly with the control plane's identity. Under compose it is `docker compose run --rm tool upsert --manifest /work/manifest.json`: it reads a
manifest (`docker/examples/hello/manifest.json`) and the code directory `<workerId>/<versionId>/<main module>`; `contentHash` is the main module's SHA-256.
Without `--tenant` it acts on the **`default` tenant** (the Worker names in the manifest are names within the tenant, and the full name is `default/<name>`); the platform's tenants go through `cowctl`.
It writes the same deployment state as the control plane (the same write protocol) and likewise asks a node for a trial load before deploying.

## 7. Ports and networks

| Port | Used by | Under compose |
| --- | --- | --- |
| 8930 | Ingress: user requests and the management endpoints | Published locally as `18080` (`18081`, `18082`) |
| 8934 | Peer: calls between nodes, trial loads during deployment | Only on the internal network (`node-N-peer`) |
| 8940 | Control plane: `cowctl`'s API `/v1` (with credentials) | Published locally as `18090` |
| 2379 | etcd | Only on the internal network |
| 8333 | Object storage, S3 | Only on the internal network |

compose has two networks: `backend` (internal, without a gateway: etcd, object storage, the nodes' peer listeners, the control plane, the initialization and tool containers; the host cannot reach it) and
`frontend` (the nodes and the control plane: their published ports, and the nodes' egress). All containers run as a non-root user (uid 10001).

On bare metal: the ingress is for users and the front load balancer; the control plane only for the platform's own systems and `cowctl`; the peer port only between nodes; etcd and object storage not exposed.
Each of these channels has credentials: peer connections are signed with the cluster key (section 12), etcd has authentication on with one user per node ("etcd roles, users and authentication" in section 4),
object storage requests are signed and nodes have only a read-only identity; the management endpoints use the management token, the control plane the admin and tenant credentials. Network isolation still has to be done -- apart from the credentials, tenant code itself cannot reach
these ports either (section 11).

## 8. Management endpoints and the token

| Path | Purpose | Token |
| --- | --- | --- |
| `GET /__coworkerd/ready` | Readiness (503 while draining; load balancers take the node out on it) | Not needed |
| `GET /__coworkerd/probe` | Liveness (200 as long as the process is there) | Not needed |
| `GET /__coworkerd/state` | State: `nodeId`, members, tenants (`tenants`), per-tenant usage (`usage`), processes (including the quarantine shards and the per-tenant suspects), deployment, version (`version`), effective configuration (`configuration`), counters | Needed |
| `GET /__coworkerd/deployment`, `/placement?workers=`, `/process-placement?workers=` | The deployment view, placement | Needed |
| `POST /__coworkerd/drain`, `GET /__coworkerd/drain` | Start exiting (like SIGTERM), see the drain's progress | Needed |
| `POST /__coworkerd/rotate?all=1`, `GET /__coworkerd/rotation` | Process rotation | Needed |
| `POST /__coworkerd/trial-load` | Trial load (used by the deployment tool; on both the ingress and the peer port) | Needed |
| `POST /__coworkerd/load-band`, `POST /__coworkerd/reload` | For tests and the file backend | Needed |

With a token set (`management.token` or `COWORKERD_MANAGEMENT_TOKEN`), everything under `/__coworkerd/` except `ready` and `probe` needs
`Authorization: Bearer <token>`, otherwise 401; each refusal is logged as a line `[management] {"event":"unauthorized",...}` (without the token).
Without a token these endpoints stay open, and the startup log reminds you of it. The deployment tool, `tool drain` and `tool rotate` read the environment variable `COWORKERD_MANAGEMENT_TOKEN`
(or `--token`, but others can see command line arguments; prefer the environment variable).

Changing the token: edit `.env` (or the environment file), then restart the nodes and the control plane one by one; the internal tool uses the new token. Changing the admin credential likewise: edit it and restart the control plane; tenant credentials are replaced through `cowctl credential` (issue a new one, revoke the old one), without restarts.

## 9. Upgrades and draining

- **One node exits**: `SIGTERM` (`docker compose stop`, `systemctl stop`) or `coworkerd-apiserver tool drain --server http://<ingress> --wait`
  (with the token). The node first announces its exit (readiness turns 503, and other nodes stop placing new calls on it), waits for the in-flight calls to complete (at most `drainTimeoutMs`), closes workerd,
  writes out the current hour's usage, revokes its membership lease, then exits. A second SIGTERM exits immediately. compose allows 45 seconds (`stop_grace_period`).
- **Upgrading nodes**: one at a time: load the new image, `docker compose up -d node-1` (recreates that one), wait until it is ready, then the next. As long as the membership table always holds at least two nodes,
  calls fail zero times.
- **Replacing workerd**: replace the file `workerdPath` points to; the node notices within a second and rotates process by process, with in-flight requests completing in the old process;
  `coworkerd-apiserver tool rotate --server ... --all` rotates by hand as well.
- **Replacing workerd** (only workerd, not the whole package): first put in an allowlist covering both the old and the new bytes, then replace the binary, finally switch to the allowlist covering only the new bytes -- the order and the reasons are in "System call allowlist" in section 4.
- **Replacing the launcher** (`bin/coworkerd-launcher`, upgraded together with the node): the node checks and fixes it at startup, so replacing the file while running triggers no rotation, and the state endpoint's
  `processes.sandbox.pendingRestart` says the new one is used only after a restart; so replace it together with the node, then restart the node. When the prerequisites are met through an AppArmor profile (section 4),
  the profile attaches by path, and the replaced old file is no longer at that path: workerd processes started after the replacement and before the restart fail to start; stop the node first, then replace.
- **Version**: `coworkerd --version` (`coworkerd-apiserver --version` in the same format) prints the version, the source it was built from, and the workerd it expects: the upstream version and the SHA-256 of the patch series; the state endpoint's `version` says the same.

## 10. Troubleshooting

| Symptom | Cause and remedy |
| --- | --- |
| `up --wait` reports `init` failed | `docker compose logs init`: object storage did not pass the self-check within two minutes (full disk, port conflict); `down -v` and start again |
| The control plane refuses to start with `set control.adminCredential or COWORKERD_ADMIN_CREDENTIAL` | No admin credential: compose's `secrets` step generates it; on bare metal see "The control plane" in section 4 |
| `init` reports `etcd did not take its users and roles` | `docker compose logs init` has the output of `tool etcd-auth` and etcd's answers; when etcd already has authentication on and root's password file is not the one used when it was turned on (the `secrets` volume was replaced), it cannot log in |
| `cowctl` reports `UNAUTHORIZED` / `FORBIDDEN` | The credential is wrong, revoked or expired / its scope is not enough (a tenant credential acts only on its own tenant; `tenant` (except reading its own record), `credential create` and `node` need the admin credential), see [error codes](errors.md) |
| The node restarts over and over, logging `deployment state is empty` | No deployment state in etcd: compose's init writes it; on bare metal see the end of section 4 |
| The internal tool reports `HTTP 401: the Server requires the management token` | The internal tool has no token: set `COWORKERD_MANAGEMENT_TOKEN` (compose's `tool` reads the one `secrets` generated by itself) |
| Deployment reports `trial load ... failed` | The new version cannot be loaded; the reason is in the error (cleaned: no secret values or paths); the pointer was not switched |
| A request gets 503 `RESULT_UNCERTAIN` | The Worker failed while executing (for example it threw); the reason is cleaned, the full story is in the node's log |
| Startup reports `stack directory's path is too long` | The stack directory's path is too long for a Unix socket: use a shorter path |
| Startup reports `another Server runs in this stack` | Another node already runs in the same stack directory |
| The log has `[sandbox] {"event":"warning",...}` | `sandbox.mode` is `auto` and the prerequisites are not met, so workerd runs without the sandbox; `reason` says which step (for example `unshare: Operation not permitted`); see "Sandbox" in section 4 |
| Startup reports `sandbox.mode is launcher, and the launcher cannot set up ...` | As above, refused in `launcher` mode |
| A process's `lastExitCategory` in the state endpoint is `launch-failed` | The launcher failed before handing over to workerd (`lastLaunchFailure.reason`, with the matching `[coworkerd-launcher]` line in `workerd.log`); retried with the usual backoff |
| The log has `[syscall-observed] {...}` | In observe mode a call is outside the allowlist; see "What to do about an observation" in section 4; the process's behaviour is not affected |
| A process's `lastExitCategory` in the state endpoint is `syscall-denied` | In enforce mode the process made a call outside the allowlist and the filter killed it (`processes.sandbox.syscalls.recentDenials` has the record, and the kernel log a seccomp audit record with `sig=31`); the slot restarts with the usual backoff. Switch back to `observe` first, attribute the call and add it to the allowlist |
| A startup failure reason contains `allowlist ... was generated for workerd ...` | The allowlist and this machine's workerd are not a pair (workerd was replaced without the allowlist, or the other way round). Generate it again as in section 4, or take both files from the same release package |
| `fetch` throws `EGRESS_DENIED: <rule>` / `EGRESS_FAILED: <category>` | The outgoing request was refused by policy or could not connect, see section 11 and section 4 of the [error codes](errors.md); the node log's `[egress]` lines have the destination and the reason |
| The log has `[peer-auth] {"event":"refused",...}` and cross-node calls get `PEER_UNAVAILABLE` | The peer connection is unsigned or its signature is not accepted: `reason` `unknown-key` (the two key files differ; a rotation is not finished), `expired` (the two clocks differ by more than 30 seconds: install NTP), `missing` (the other side has no cluster key configured). See section 12 |
| Startup reports `etcd authentication failed` | This node's (or the control plane's) etcd user or password is wrong: under compose run `init` again (it sets each user's password from the files); on bare metal see "etcd roles, users and authentication" in section 4 |
| etcd reports `user name is empty` / `permission denied` | Authentication is on and the request carries no credentials / the credential's role does not allow the operation (for example a node writing the deployment state) |

## 11. Egress: tenant code reaching the network

Tenant code's `fetch()` does not connect to the network from workerd itself -- with the sandbox active the workerd processes have no network interfaces ("Sandbox" in section 4), and without it they reach the network only through this egress configuration -- but goes to **the egress service of the node it runs on**
(`<stack directory>/run/egress.sock`): the node checks the destination against the policy, resolves names itself, opens the connection itself (it also does the TLS of HTTPS, verified against the system's root certificates),
counts the bytes, and hands the answer back. When refused or unable to connect, `fetch` throws `EGRESS_DENIED: <rule>` or `EGRESS_FAILED: <category>` (section 4 of the [error codes](errors.md)).
Request and response headers are forwarded as they are (the node adds no identifying headers for the tenant); redirects are followed by `fetch` and each hop is checked again; HTTP, HTTPS and WebSocket are supported.

**Refusals no rule can open**: loopback, link-local, the clouds' metadata addresses (169.254.169.254 and others), `0.0.0.0`, reserved and multicast addresses, and **the cluster's own addresses** --
all of this node's interface addresses, the advertised address of every node in the membership table, the endpoints of etcd and object storage, and those listed in `egress.clusterAddresses` (the control plane, load balancers and others the node cannot see itself),
on any port. When nodes have several networks (for example a public address and an internal one), write all the networks the cluster's nodes are on into `egress.clusterAddresses` (CIDR), because the membership table has only each node's advertised address.

**The default policy** (`egress.defaultPolicy`): `public` (the default) allows ports 80 and 443 of public addresses and refuses internal ones; `deny` allows nothing except what the tenant's rules allow.
**Each tenant's rules** (`egress.tenants`, written in the node configuration in 0.3.0, later delivered by the control plane with the tenant):

```json
"egress": {
  "defaultPolicy": "public",
  "clusterAddresses": ["10.0.0.0/24", "apiserver.internal:8940"],
  "tenants": [
    { "tenant": "shop", "allow": ["api.stripe.com", "*.amazonaws.com:443", "203.0.113.0/24:8443"], "deny": ["*.example.net"],
      "allowInternal": ["db.internal:5432"], "concurrency": 64, "requestsPerSecond": 200 }
  ]
}
```

Each entry is a name (`api.example.com`), a wildcard (`*.example.com`), an address or a CIDR (`203.0.113.7`, `198.51.100.0/24`, `[2001:db8::/32]`), optionally with `:port`
(none means any port). `deny` is looked at first; `allow` can only open public addresses; `allowInternal` is the administrator's permission for internal destinations (the platform's own database, its authentication service), which the tenant should not be able to change.
`tenant` is the tenant name (the first part of the Worker's full name `tenant/Worker`): all Workers of one tenant share these rules, limits and accounting.

**Names are resolved on the node**: **every** address a resolution yields has to pass the check, and connections go only to the checked addresses; a result is reused for at most `egress.dnsCacheMs` (10 seconds by default; the system resolver
reports no TTL, so this is the bound), so after a name is pointed at an internal address the next resolution is refused, and requests within the reuse period still go only to the addresses checked back then.

**Limits and accounting**: each tenant has an outgoing concurrency limit (`concurrency`, default 32) and a requests-per-second limit (`requestsPerSecond`, default 100, a token bucket) on each node; beyond them it gets
`EGRESS_DENIED: concurrency-limit` / `rate-limit`, without affecting other tenants; each request has a timeout (`egress.timeoutMs`, until the response headers arrive, and between two reads of the body).
The bytes of request and response bodies (for WebSocket, the bytes of the messages) count into the tenant's usage: the state endpoint's `egress.tenants.<tenant>` has `requestBytes`, `responseBytes` and their sum
`egressBytes` (the same bytes count hourly into the tenant's usage rows, the egress bytes of `cowctl usage`), plus `requests`, `allowed`, `denied` (by rule), `failed` (by category) and `inFlight`.
Each request has a line `[egress] {...}` in the node log (not counted against the tenant's log quota); the state endpoint's `egress.cluster` lists the addresses currently treated as the cluster's own.

**HTTPS certificates**: the node verifies destinations against the system's root certificates (the image has `ca-certificates` installed); add an internal CA with `egress.caFile` (PEM, several certificates allowed).
To turn egress off: set `egress.mode` to `off` (or `COWORKERD_EGRESS_MODE=off`), and tenant code's `fetch` fails.

## 12. The cluster key and etcd credentials

**The cluster key** (peer calls between nodes): each node signs the peer connections it opens (HMAC-SHA256 over a timestamp, a nonce, the target node, the method, the path and the body's hash);
the peer listener accepts only connections with a valid signature, a time within a 30-second window and an unused nonce, answers the rest with 401 and logs a line `[peer-auth] {"event":"refused","reason":...}`;
the state endpoint's `peerAuth` has the key ids, the accepted count per key and the refused count per reason. The key file (owned by coworkerd, mode 0600, otherwise the node refuses to start):

```json
{"keys": [{"id": "k2", "secret": "<64 hexadecimal characters>"}, {"id": "k1", "secret": "..."}]}
```

The first key signs; both are accepted. The node checks the file every second and reads it again when it changed (when it cannot read it, it keeps using the old one and logs a line `reload-failed`); when the signing key changes, later new connections use the new key;
once a key is removed from the file, the sessions established with it are closed after 30 seconds. All nodes' clocks have to agree within 30 seconds (NTP).

**Rotation** (three steps; each step done on all nodes before the next; calls are not interrupted):

| Step | compose | Bare metal (each node's key file) |
| --- | --- | --- |
| 1. Add the new key as the second | `docker compose run --rm secrets rotate-cluster-key add` | `{"keys":[old,new]}` |
| 2. Make the new key the first (start signing with it) | `docker compose run --rm secrets rotate-cluster-key promote` | `{"keys":[new,old]}` |
| 3. Remove the old key | `docker compose run --rm secrets rotate-cluster-key retire` | `{"keys":[new]}` |

To generate a key: `openssl rand -hex 32`. compose's `secrets` distributes the new file into each node's directory at every step (replaced atomically), and the nodes read it within a second.

**etcd authentication**: users, roles and turning authentication on are in "etcd roles, users and authentication" in section 4; compose's `init` runs it at every start (idempotent).

**Changing an etcd password**: under compose, `docker compose run --rm secrets rotate-etcd-password nodes/node-1` (or `apiserver`) writes a new password, then
`docker compose run --rm init` sets it in etcd; a running node, when etcd refuses its token, reads the file again and authenticates with the new password, without a restart. Bare metal likewise: change the file first,
then the password in etcd (`tool etcd-auth` or `etcdctl user passwd`). root's password is changed with `etcdctl --user root user passwd root`.

**Object storage**: requests are always signed; the deployment identity (read-write) is only for the control plane and the deployment tool, and the nodes have only the read-only identity (compose's directory layout does exactly that; writing with a node's identity gets 403).

# Installing and running Coworkerd

> For release 0.5.0.
> For platform operators meeting Coworkerd for the first time: follow this page to start a cluster on your own machine, create a tenant, deploy a Worker and send the first request.
> Tenants, credentials, deployments, logs, new versions, rollbacks, secrets and usage are done with the command line `cowctl`, see the [quickstart](quickstart.md); this page covers installation, startup, configuration and operations.

## 1. What it is, and what the release contains

Coworkerd is a Workers infrastructure layer for platforms, based on Cloudflare `workerd`: one cluster runs all the Workers of one platform, the platform is the only administrator,
the platform's users are **tenants**, and Workers belong to tenants (full name `tenant/Worker`). Each **node** is one `coworkerd` process (the node daemon) and the
`workerd` processes it manages. Membership, desired state, suspects, usage, code and uploaded durable history are in one bucket of an S3-compatible object store; the latest confirmed object tail may still be on nodes;
nodes keep recovered SQLite files and an append log, while the control plane mirrors desired state. The bucket address, marker and credentials identify the cluster a node joins. A request enters through any node's ingress
and runs on that node or another one according to the placement rules. The **control plane** `coworkerd-apiserver` and its operations tool, with the same identity, are the only writers of the desired state (tenants, credentials, deployments), and `cowctl` talks only to the control plane;
nodes read desired state and write their own membership, suspects, usage and WAL segments, plus shared Durable Object ownership and history, each with an object storage identity of its own (section 4, "Object storage identities").

Each version comes in two architectures (`linux/amd64`, `linux/arm64`), three files each, plus one checksum file:

| File | Contents |
| --- | --- |
| `coworkerd-0.5.0-linux-<arch>.tar.gz` | The package, unpacked into `coworkerd-0.5.0-linux-<arch>/` (the next table) |
| `coworkerd-0.5.0-linux-<arch>-image.tar.gz` | The image `coworkerd:0.5.0`, for `docker load` |
| `coworkerd-0.5.0-linux-<arch>.json` | The release record: which sources and binaries this architecture's package was built from, each with its SHA-256 |
| `SHA256SUMS` | The SHA-256 of each file above (both architectures) |

The package:

| Entry | Contents |
| --- | --- |
| `bin/` | `coworkerd` (the node daemon; `coworkerd check` is its self-check, `coworkerd check storage` the object store's), `coworkerd-apiserver` (the control plane, and the operations tool `coworkerd-apiserver tool`), `coworkerd-launcher` (starts workerd in a least-privilege environment, see "Sandbox" in section 4), `workerd`, `cowctl` (the command line; it runs `lib/cowctl` with the machine's Node.js), and `coworkerd-syscalls-startup.txt`, `coworkerd-syscalls-steady.txt` and `coworkerd-syscalls.json` (the system call allowlist of this workerd and the record of how it was generated, see "System call allowlist" in section 4) |
| `lib/` | `lib/cowctl/`: `cowctl` and the wrangler adapter with their dependencies |
| `runtime/` | `runtime.js`, the runtime bridge every workerd process loads |
| `docker/` | The compose file `docker-compose.yml`, `docker-compose.sandbox.yml` (section 3), the entry point script `entrypoint.sh`, the `Dockerfile`, `.env.example`, and `examples/hello/`: the example project the compose `tool` service mounts by default |
| `examples/` | `hello-world` and two official Cloudflare templates |
| `docs/` | This document, the quickstart, the compatibility scope and the error codes |
| `INSTALL.md` | A copy of this document; read it first after unpacking |
| `VERSION` | What `coworkerd --version` prints |
| `SHA256SUMS` | The SHA-256 of every file in the package |

A Mac (Apple silicon) uses `arm64`; most Linux servers use `amd64` (`uname -m` is `x86_64`).

Requirements: **Docker Engine 28 or newer** (compose's internal network uses `gateway_mode_ipv4: isolated`, which only exists from 28.0; 27.x rejects it) and the Docker Compose plugin (Docker Desktop on the Mac includes it; the compose file uses volume `subpath` and the healthcheck's
`start_interval`; use a Compose version that supports both). Budget memory for the ordinary and quarantine slots, the durable processes and their resident isolates, plus the control plane and object store; the durable process count and resident-object target are configured below.

## 2. Option one: compose, one node (one command)

```bash
# 1. Verify, unpack, load the image (amd64 as the example)
sha256sum -c --ignore-missing SHA256SUMS      # skips the other architecture's lines when you downloaded only one; on a Mac: shasum -a 256 -c --ignore-missing SHA256SUMS
tar -xzf coworkerd-0.5.0-linux-amd64.tar.gz
docker load -i coworkerd-0.5.0-linux-amd64-image.tar.gz
cd coworkerd-0.5.0-linux-amd64/docker

# 2. Start the cluster: the object store, initialization, one node and the control plane; --wait waits until the ingress is ready (usually 10-30 seconds)
docker compose up -d --wait

# 3. The administrator creates a tenant and a credential, deploys hello-world and sends the first request: with cowctl, see the quickstart (quickstart.md), sections 2-3
```

The services: `secrets` and `init` run at every start and exit, `storage` is the object store (SeaweedFS 4.48), `node-1` the node and `apiserver` the control plane (the API `cowctl` connects to);
`node-2` and `node-3` are in the profile `cluster` (section 3), and `tool`, the operations tool, in the profile `tools`, run on demand. The store's bucket `coworkerd` holds desired state and uploaded durable history. Node volumes also hold SQLite files and the local append log; retain them because they may contain the latest confirmed tail.

**What `secrets` generates.** On the first start the `secrets` container (no network, exits once it is done) generates every credential of the cluster into the data volume named `secrets`, and on every later start only what is missing:

| Credential | Used by |
| --- | --- |
| The object store identity `deploy` | The control plane and `tool`: it reads, writes and lists the whole bucket, as the store's administrator (`init` creates the bucket with it) |
| An object store identity `node-<nodeId>` for every node | That node: it reads what a node reads -- every node's `nodes/` prefix among it, where the Durable Object log's segments are -- and writes only its own `members/<nodeId>/` and `nodes/<nodeId>/` and the objects' own prefixes (section 4, "Object storage identities") |
| The **secrets key** `secrets-key.json` | The control plane seals secrets' values with it before they are stored, and every node opens them with it |
| The **cluster key** | The nodes sign their peer connections with it (section 12) |
| The **management token** (unless `COWORKERD_MANAGEMENT_TOKEN` is set) | The control plane, the operations tool and operators calling a node's management endpoints (section 8); nodes sign their own sessions with the cluster key |
| The **admin credential** (unless `COWORKERD_ADMIN_CREDENTIAL` is set) | The administrator of `/v1`, used by `cowctl`'s administrator subcommands (`tenant`, `credential`, `node list`, `usage --all`) |

From the identities it writes the store's identity file `s3.json`, which holds the deploy identity, and a file per node, `storage/hotload/<id>.weed`, which the store's container puts into the store's filer (again at every start, from the identities in the volume), and it writes the configuration that closes the store's own gRPC interface to everyone. It puts each credential into the
directory of every service that reads it, on a "whoever reads it gets it" basis, and the store, the nodes, the control plane and `tool` each mount only their own directory, read-only (`secrets` and `init` mount the whole volume): a node has its own identity, the cluster key, the secrets key and the management token;
the control plane (and `tool`) has the deploy identity, the secrets key, the management token and the admin credential; the store has `s3.json`, its gRPC configuration and the nodes' identities (`hotload/`). The nodes it prepares are `COWORKERD_NODE_IDS` of the `secrets` service in `docker-compose.yml`: `node-1,node-2,node-3`.

**What `init` does.** It creates the bucket and checks the store with `coworkerd check storage`: the deploy identity writes, the first node's identity reads back, unsigned requests must be refused, and the contract the cluster
relies on must hold -- create-if-absent, a listing's `Date` and `LastModified`, and this machine's clock within 2 s of the store's. It tries for up to two minutes while the store starts, and logs the check's result as one line,
`[init] {"contract":{...}}`, which also reports `conditionalPutAtomic` (whether create-if-absent holds under concurrency) and `policy` (whether the first node's identity is held to its own prefixes).
Then `coworkerd-apiserver tool init` writes the cluster's marker `cluster/format/1` and empty tenant and credentials indexes into the bucket when they are not there (`[init] {"operation":"init",...,"format":"created",...}`, and `"present"` on a later start);
nodes and the control plane refuse to start on a bucket without the marker. Every later `up` runs `secrets` and `init` again and leaves existing credentials and state untouched.

Common operations:

| What | Command |
| --- | --- |
| Show the admin credential | `docker compose run --rm tool admin-credential` (for `cowctl`'s administrator subcommands: `COWCTL_TOKEN=$(...) cowctl tenant list`) |
| Show the management token | `docker compose run --rm tool token` |
| Show the nodes' state | `COWCTL_TOKEN=<admin credential> cowctl node list`; or read a node directly: `curl -H "Authorization: Bearer $(docker compose run --rm tool token)" http://127.0.0.1:18080/__coworkerd/state` |
| Show the store's check | `docker compose logs init` |
| Show logs | One Worker's: `cowctl logs` (see section 4 of the quickstart); the node's own: `docker compose logs -f node-1`; all of workerd's output is in `logs/workerd.log` of the node's data volume (tenant `console.*` output appears there as plain lines without attribution; the lines `cowctl logs` shows reach the node separately over workerd's per-Worker tail channel, attributed to the Worker that wrote them), rotated at 256 MiB x 4 |
| Add a node | `docker compose run --rm secrets add-node <nodeId>`, then start it: "Adding a node" below |
| Rotate the cluster key | `docker compose run --rm secrets rotate-cluster-key add`, then `promote`, then `retire` (section 12) |
| Stop (keep the data) | `docker compose down` |
| Stop and delete all data | `docker compose down -v` |
| Show the version | `docker run --rm coworkerd:0.5.0 version` |

To change ports, tokens or the project directory, copy `.env.example` to `.env` and edit it (every entry may be left out):

| Variable | Default | Effect |
| --- | --- | --- |
| `COWORKERD_VERSION` | `0.5.0` | The tag of the image the services start |
| `COWORKERD_BIND` | `127.0.0.1` | The local address the ingress ports and the control plane's port bind to; set `0.0.0.0` to let other machines reach them |
| `COWORKERD_HTTP_PORT` (`_2`, `_3`) | `18080` (`18081`, `18082`) | The local port of node-1's (node-2's, node-3's) ingress |
| `COWORKERD_CONTROL_PORT` | `18090` | The local port of the control plane (the API `cowctl` connects to) |
| `COWORKERD_MANAGEMENT_TOKEN` | empty: the one `secrets` generated | Your own management token (16-4096 visible characters, no spaces); used from the control plane to the nodes and by the nodes' management endpoints |
| `COWORKERD_ADMIN_CREDENTIAL` | empty: the one `secrets` generated | Your own admin credential for `/v1` (same rules); used by `cowctl`'s administrator subcommands, do not give it to tenants |
| `COWORKERD_PROCESS_COUNT` | `1` | Ordinary workerd processes per node (plus 2 quarantine processes, see `quarantineProcesses` in section 5) |
| `COWORKERD_PROJECT_DIR` | `./examples/hello` | The local directory mounted at `/work` in the `tool` container (the project to deploy) |
| `COWORKERD_PROJECT` | `coworkerd` | The compose project name (to tell several clusters on one machine apart) |

**The store's other interfaces.** Only SeaweedFS's S3 gateway listens on the store container's network; its master, volume and filer servers listen on the container's loopback, and the gateway's own gRPC interface takes only client certificates of a certificate authority that has signed none and whose key is not kept; the gateway's own certificate, whose key the volume holds under `storage/`, comes from another authority, so it opens nothing either.
The filer also serves an identity service without authentication, which the loopback keeps closed: do not publish the filer's ports. The initialization scripts discard `weed shell`'s `s3.configure` output. Treat identity-management output and `docker compose logs storage` as potentially containing credentials; redact them before sharing.

### Adding a node

A node joins a running cluster with an identity of its own, which the store takes without a restart. For a fourth node `node-4`:

```bash
docker compose run --rm secrets add-node node-4
cat > docker-compose.node-4.yml <<'YAML'
services:
  node-4:
    image: coworkerd:${COWORKERD_VERSION:-0.5.0}
    init: true
    restart: unless-stopped
    stop_grace_period: 45s
    environment:
      COWORKERD_STORAGE_ENDPOINT: http://storage:8333
      COWORKERD_MANAGEMENT_TOKEN: ${COWORKERD_MANAGEMENT_TOKEN:-}
      COWORKERD_PROCESS_COUNT: ${COWORKERD_PROCESS_COUNT:-1}
      COWORKERD_NODE_ID: node-4
      COWORKERD_PEER_ADDRESS: node-4-peer:8934
      COWORKERD_PEER_LISTEN_ADDRESS: node-4-peer:8934
    healthcheck:
      test: ["CMD", "coworkerd-entrypoint", "healthcheck"]
      interval: 5s
      timeout: 3s
      retries: 3
      start_period: 60s
      start_interval: 1s
    networks:
      frontend: {}
      backend:
        aliases: [node-4-peer]
    ports: ["${COWORKERD_BIND:-127.0.0.1}:18083:8930"]
    volumes:
      - node-4-data:/var/lib/coworkerd
      - {type: volume, source: secrets, target: /var/lib/coworkerd-secrets, read_only: true, volume: {subpath: nodes/node-4}}
volumes:
  node-4-data:
YAML
docker compose -f docker-compose.yml -f docker-compose.node-4.yml up -d --wait node-4
```

`secrets add-node node-4` (one line per step, never a value) creates the directory `nodes/node-4/` with the node's own identity and the cluster's keys, and writes
`storage/hotload/node-4.weed`, which the store's container applies within about 2 seconds with `weed shell`'s `s3.configure` -- kept in the store's filer and in force at once, so the store is not restarted.
The file defines `node-4` as `docker-compose.yml` defines `node-3` -- except that it has no profile and does not wait for `init`, being for a cluster that runs -- with that directory and the ingress on local port `18083`; once it is ready, it is a member like the others (`cowctl node list`).
Measured on a three-node cluster: `node-4` was ready about 4 seconds after `add-node` returned, the store still running since its start.
Pass both files to every later compose command for this cluster (or write `COMPOSE_FILE=docker-compose.yml:docker-compose.node-4.yml` into `.env`), for example to stop and delete it:
`docker compose -f docker-compose.yml -f docker-compose.node-4.yml --profile cluster down -v`.

Adding the id to `COWORKERD_NODE_IDS` of the `secrets` service and running `up` again goes the same way. Restarting the store (`docker compose restart storage`) applies every hotload file again, with a period of storage unavailability: the nodes keep serving what they have, and the control plane answers reads from its copy -- except the usage, and a first read of Workers whose definitions it has not fetched, which answer `503 STORAGE_UNAVAILABLE` too -- and changes can answer `503 STORAGE_UNAVAILABLE`. A failed answer can follow a committed write; inspect the resulting state before retrying a mutation.
Do not edit `s3.json` or the hotload files by hand: `secrets` writes them again from the volume's identities at every start, a hotload file for every `nodes/<id>/` directory.

**Removing a node** for good: stop it, then `docker compose run --rm secrets remove-node node-4` removes its directory `nodes/node-4/` and its `storage/hotload/node-4.weed`, and writes `storage/hotload/node-4.remove.weed`, which the store's container applies within about 2 seconds with `s3.configure`'s deletion: the identity leaves the store's filer, and requests signed with it are refused from then on, without a restart of the store.
This holds for every node, `node-1` to `node-3` included: `s3.json`, which the store reads at its start, holds only the deploy identity, and every node's identity is a `storage/hotload/<id>.weed` that the store's container puts into the filer at each of its starts and whenever one appears. Take an id that is in `COWORKERD_NODE_IDS` out of it as well (the line `remove-node` prints says `"inNodeIds":true`), or the next `up` gives that node a new identity.

## 3. Option two: compose, three nodes

```bash
docker compose --profile cluster up -d --wait
for port in 18080 18081 18082; do curl -s http://127.0.0.1:$port/acme/hello-world; done
# All three ingresses take requests; a Worker runs on its home node (computed from the full name tenant/Worker) unless the receiving node ran that version within the last 30 s (even then about one request in twenty goes to the home node); otherwise the request is forwarded
docker compose --profile cluster down -v      # stops all three nodes and deletes the data
```

The three nodes find each other in the object store: each registers an epoch below `members/<nodeId>/` (`nodeId` being `node-1..3`), writes its heartbeat there every 5 seconds and lists `members/` every 10 seconds,
and they connect to each other by their alias on the internal network (`node-N-peer:8934`). One deployment takes effect on all three nodes within about 5 seconds: each node reads the desired state that often, and `cowctl deploy` waits up to 15 seconds for non-draining nodes to report the new revision, then warns and returns successfully if some remain behind; `--no-wait` skips that wait.
A node that stops is dropped by the others at their next listing, one that crashed about half a minute after its last heartbeat; a fourth node joins as "Adding a node" in section 2 describes.

**The sandbox under compose**: the nodes' `sandbox.mode` defaults to `auto` -- each workerd starts through `coworkerd-launcher` in a least-privilege environment (see "Sandbox" in section 4),
and when the conditions are not met workerd starts directly with a warning. Docker's default seccomp profile does not let a process without `CAP_SYS_ADMIN` create namespaces, so **in a cluster started with the commands above the sandbox is off**:
each node's log has a line `[sandbox] {"event":"warning",...}`, the state endpoint's `processes.sandbox.active` is `false`; the node is running without launcher isolation.
To turn it on, first meet the host prerequisites of section 4, then add the package's `docker-compose.sandbox.yml` (it only lifts the default seccomp and AppArmor profiles for `node-1`, `node-2` and `node-3` -- a node added as in section 2 needs the same settings in its own file --, adds
`no-new-privileges`, adds no capability, and sets `sandbox.mode` to `launcher`: a node that cannot set up the environment refuses to start instead of running without the sandbox):

```bash
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml up -d --wait
# or write COMPOSE_FILE=docker-compose.yml:docker-compose.sandbox.yml into .env, and every later compose command takes it
```

## 4. Option three: bare metal (Linux, systemd)

For running the nodes and the control plane directly on Linux hosts, with your own object storage.

**Preparation**

- S3-compatible object storage and a bucket. The compose files pin **SeaweedFS 4.48**, whose concurrent create-if-absent behavior was verified. In the tested 3.97 version, concurrent creates of one key can all succeed, which more than one writer of the desired state at a time cannot work with (the nodes' membership reads its registrations back and does not rely on it: a node only warns), and
  `coworkerd check storage` reports `conditionalPutAtomic: false` and still exits 0. What the cluster needs from a store: create-if-absent (`If-None-Match: *`) -- one that also holds under concurrency when more than one writer of the desired state uses the bucket --, a read after a write that returns the write,
  listings that name each object's `LastModified` and ETag with the answer's `Date`, and overwrites; reads are signed as standard presigned URLs (the host only, an unsigned payload) and writes in the `Authorization` header, so the store must take both. Keep versioning off on the bucket, and **create the bucket first**, then deploy.
- One identity of the store for the control plane (`deploy`: it creates the bucket -- the self-check's first request -- and reads, writes and lists it; compose gives it SeaweedFS's `Admin`, `Read`, `Write` and `List`) and one for each node, held to the policy in "Object storage identities" below.
  Confirm them with the package's self-check, here with node-1's identity:
  `AWS_ACCESS_KEY_ID=<deploy> AWS_SECRET_ACCESS_KEY=<deploy> COWORKERD_READ_ACCESS_KEY=<node-1's> COWORKERD_READ_SECRET_KEY=<node-1's> COWORKERD_READ_NODE_ID=node-1 bin/coworkerd check storage http://storage:8333 coworkerd`
  (it creates the bucket, writes a probe object and reads it back with the node's identity, expects unsigned requests to be refused, and checks the contract: create-if-absent, a listing's `Date` and `LastModified`, this machine's clock within 2 s of the store's;
  it prints one line for each request of the signature probe -- the bucket, the probe object's write, its two signed reads and the three unsigned requests -- and then `{"contract":{...}}`, exits 0 when the contract holds, and reports without requiring them `conditionalPutAtomic`, the node identity's `policy` and whether the bucket holds the cluster's marker).
- The cluster's **secrets key**, one file for the control plane and every node: `printf '{"key":"%s"}\n' "$(openssl rand -hex 32)" > secrets-key.json`, then copy it to each of them as `/etc/coworkerd/secrets-key.json`, owned by coworkerd, mode 0600.
- An ordinary user, for example `useradd --system --home-dir /var/lib/coworkerd coworkerd`; the programs go into `/opt/coworkerd` (the unpacked `bin/` and `runtime/`).
- **The sandbox's prerequisites** (see "Sandbox" below): Linux allows unprivileged user namespaces; `kernel.yama.ptrace_scope` is 1 (Ubuntu's default) or stricter.
- Clocks within 2 seconds of the store's (NTP): a node refuses to start beyond that.

**Sandbox**: the node starts each workerd process through `bin/coworkerd-launcher`; the process runs in a read-only root directory holding only the few files it needs (a new mount namespace), without a usable network
(a new network namespace: `lo`, down -- on a kernel with tunnel drivers loaded also their inert fallback devices, down -- and no route), as 65534 in a new user namespace, without any capability, with `no_new_privs`; when the node is killed, its workerd processes end with it.
At its first request -- the node's readiness probe -- workerd marks itself non-dumpable (the bit the launcher sets is reset by `execve`, so workerd sets it again itself); a node's workerd processes share one outside uid, so the host also needs
Yama's `kernel.yama.ptrace_scope=1` (attach only to descendants; Ubuntu's default, 2 and 3 are stricter) to keep them from attaching to each other.
The launcher has to create user namespaces as an ordinary user and mount inside them. To check (run the launcher's own check as the user that runs the node; `ok` means the prerequisites are met; on failure it prints a line
`[coworkerd-launcher] {"event":"failed",...,"step":...}` saying which step failed):

```bash
sysctl kernel.apparmor_restrict_unprivileged_userns     # no such key or 0: not restricted by this
cat /proc/sys/user/max_user_namespaces                  # greater than 0
cat /proc/sys/kernel/yama/ptrace_scope                  # 1 or greater; when 0: echo 'kernel.yama.ptrace_scope = 1' | sudo tee /etc/sysctl.d/60-coworkerd-ptrace.conf && sudo sysctl --system
sudo -u coworkerd /opt/coworkerd/bin/coworkerd-launcher --user --net --ipc --uts --root /tmp --drop-caps --no-new-privs --check && echo ok
```

**AppArmor restrictions:** the tested Ubuntu 24.04 machine had `kernel.apparmor_restrict_unprivileged_userns=1` (the AppArmor profile below has not been run; lifting the restriction for the whole machine let the launcher's check pass on an Ubuntu 24.04 machine, with compose's `docker-compose.sandbox.yml`): a user namespace created by a process without an AppArmor permission has no capabilities in it,
and the check above fails (the `[coworkerd-launcher]` line it prints says where it stopped). Pick one:

- Permit only the launcher (recommended): `/etc/apparmor.d/coworkerd-launcher`

  ```
  abi <abi/4.0>,
  include <tunables/global>
  profile coworkerd-launcher /opt/coworkerd/bin/coworkerd-launcher flags=(unconfined) {
    userns,
  }
  ```

  then `sudo apparmor_parser -r /etc/apparmor.d/coworkerd-launcher`; verify the profile is loaded again after a reboot;
- or lift the restriction for the whole machine: `echo 'kernel.apparmor_restrict_unprivileged_userns = 0' | sudo tee /etc/sysctl.d/60-coworkerd-userns.conf && sudo sysctl --system`.

At startup the node prints a line `[sandbox] {...}`: `active` being `true` means it is in effect; the state endpoint's `processes.sandbox` says the same, and each process's `sandbox.observed.layers` are the layers the node
read from `/proc`. `sandbox.mode` defaults to `auto`: when the prerequisites are not met, workerd starts without the sandbox and a separate warning line is printed; `launcher` refuses to start when the prerequisites are not met (recommended for production);
`off` turns it off.

**System call allowlist**: the last layer of the sandbox. Before handing the process to workerd, the launcher installs a system call allowlist, and the allowlist **is generated by the build from profiles of this workerd**
(`bin/coworkerd-syscalls-startup.txt` and `coworkerd-syscalls-steady.txt`, shipped in the package; the `syscalls` section of the release record `.json` holds their hashes and the hash of the
workerd they belong to), not written into the code -- following upstream, changing the compiler or changing build options can all change the set. The comment at the top of the file says which workerd it was generated for, and the node checks the pair itself when it starts: with a list that does not name its workerd, `sandbox.mode` `launcher` refuses to start,
and `auto` runs workerd without the filter (every other layer in effect, a `[sandbox]` warning line, `processes.sandbox.syscalls.active` false with the reason); a binary replaced while the node runs that the list does not cover
is refused by the launcher at its start (the state endpoint's `lastExitCategory` is `launch-failed`, with both hashes in the reason).

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
| `allowlist.path`, `sha256`, `calls`, `workerdSha256s`, `workerdSha256Running`, `workerdSha256Matched` | Which file was read, its hash, how many calls, the workerd binaries it covers, the workerd file this node hashed at its start (it changes only when the node restarts; each process's `binarySha256` follows a replaced file), and which one matched |
| `notifications`, `observed[]` | The calls reported in observe mode: the total count, and the list deduplicated by call number (call number, name, count, first and last time, thread names, which processes) |
| `denied`, `recentDenials[]` | The number of processes the filter killed in enforce mode, and the latest records |
| `interrupted`, `errors` | How often the thread was already interrupted by a signal while a notification was being produced; how often the supervision itself failed (should be 0) |

**What to do about an observation**: each record is a log line `[syscall-observed] {...}` (rate-limited to one line per call number per 10 seconds; the counts are not affected) with the call (`call`, `name`), its total `count`, and this observation's `processId`, `processClass` (`ordinary`, `quarantine` or `durable`), `generation`, `pid`, `tid`, `thread`, `countThisProcess`, `mode`, `allowlistSha256` and `observedAtUnixMs`.
Steps:

1. Tell from `name`/`call` and `thread` who made it (thread names tell V8's worker threads, our watchdog thread and the main thread apart);
2. confirm whether it is **really needed** (for example a path only some load reaches, a connection only some generation opens) or a sampling artifact;
3. if it is really needed, generate the allowlist again with `--relax <call name>` of the generator `scripts/syscall-allowlist.py`, which adds the call to both tables and records it in `allowlist.json` under `relaxed` and in its
   `evidence` as "added by the operator (--relax)" -- the reason itself is not recorded, so keep it yourself; the generator needs the source tree (the package ships no `scripts/`), the workerd file (`--workerd`), the profile summaries it takes with `--profile`
   (the release record keeps only their digests), and a Linux machine of the package's architecture with a C compiler and the kernel's system call headers: it writes the call numbers of the machine it runs on;
4. after putting the new file in place, restart the server (processes started before keep the filter they started with, and the state endpoint reports the file read at the server's start and counts since then; the launcher reads the file at every start), then repeat the two steps above to confirm the count is back to zero.

**Replacing workerd: the allowlist first, then the binary**. The allowlist file says which workerd binaries it belongs to (one `# workerd-sha256` line each), and the launcher **refuses** to start bytes the allowlist does not cover
-- exactly the check that prevents "a new binary with the old allowlist". So, to upgrade workerd:

1. first put in place an allowlist that **covers both the old and the new bytes** (`--workerd` of the generator `scripts/syscall-allowlist.py` can be given several times, for example
   `--workerd old/workerd --workerd new/workerd`; inspect the package allowlist's `# workerd-sha256` lines rather than assuming it covers the old binary);
2. then replace the workerd file -- the server then rotates slot by slot as usual ("the binary changed, so rotate"), and the new generation's launcher reads an allowlist that already covers it;
3. once done, replace the allowlist with the one that covers only the new bytes (generate it with only the new binary); the launcher uses it from the next start of a process on, and the state endpoint shows it after the server's next restart.

Two details: the **launcher reads** the allowlist **at every start**, so step 1 takes effect for every later start as soon as the file is in place; the server reads it only **once, at startup**, for reporting and for checking the pair, so
`allowlist.workerdSha256s` in the state endpoint changes only after the server restarts. An upgrade of the whole package (the node and workerd together) does not need this order: stop and replace everything as in section 9.

The launcher installs the startup table, which has to contain the calls used only during startup (the filter stays in force after `execve`). The narrower steady-state table is installed by workerd itself at its first request, with its own mode:
`sandbox.syscalls.steady.mode` (`off`/`observe`/`enforce`, default `off`; environment variable `COWORKERD_SANDBOX_SYSCALLS_STEADY`); workerd writes a line `[coworkerd-seccomp]` into `logs/workerd.log` saying what it installed; the state endpoint's
`processes.sandbox.syscalls.steady` reports the mode and the table's hash, each process's `sandbox.syscalls.steadyMode` its mode and `sandbox.observed.layers.steadySyscalls` whether the second filter is on. Switch it in the same order as the startup table: `observe` first, `enforce` after zero records.
It is part of a workerd upgrade too: workerd reads only the table's call numbers, without a digest check, and the node checks it against the binary only at its own start, so replace it in the same order as the startup table.

**Object storage identities**: only the deploy identity -- the control plane's and the operations tool's -- can write the desired state, and each node writes its own `members/<nodeId>/` and `nodes/<nodeId>/` prefixes plus shared `do/*/shards/*` and `do/*/objects/*`; the store's own policies enforce this, not a convention.

| Identity | Actions | For |
| --- | --- | --- |
| `deploy` | Create the bucket (the self-check's first request), and read, write and list it -- compose gives it SeaweedFS's `Admin`, `Read`, `Write` and `List` | The control plane and the operations tool |
| `node-<nodeId>` | Read `cluster/format/`, `cluster/tenants/`, `tenants/`, `definitions/`, `packages/`, `modules/`, `bundles/`, `members/`, `do/` and `nodes/`; list `members/`, `cluster/tenants/`, `tenants/`, `do/` and `nodes/`; read, write and list `members/<nodeId>/` and `nodes/<nodeId>/`; write `do/*/shards/*` (the Durable Object shards' epochs, release markers and owner hints: any node may take a shard over) and `do/*/objects/*` (an object's manifests and snapshots, written by whichever node owns its shard). Reading and listing every node's `nodes/` prefix is what lets a node rebuild an object from the segments of the node that owned its shard before (`nodes/<id>/wal/`) | That node |

Every prefix ends with `/`, so that `node-1`'s policy does not cover `node-10`; a node's identity cannot read the credentials index `cluster/credentials/`, and cannot write another node's objects, `cluster/`, `tenants/` or the code.
In the pinned SeaweedFS 4.48 these are the actions of the identity's entry in its identities (`s3.json`, or the filer's, which `s3.configure` writes); for the bucket `coworkerd` and the node `node-1`:

```json
{"name": "node-node-1", "credentials": [{"accessKey": "...", "secretKey": "..."}],
 "actions": ["Read:coworkerd/cluster/format/*", "Read:coworkerd/cluster/tenants/*", "Read:coworkerd/tenants/*", "Read:coworkerd/definitions/*",
   "Read:coworkerd/packages/*", "Read:coworkerd/modules/*", "Read:coworkerd/bundles/*", "Read:coworkerd/members/*",
   "Read:coworkerd/do/*", "Read:coworkerd/nodes/*", "List:coworkerd/members/*", "List:coworkerd/cluster/tenants/*",
   "List:coworkerd/tenants/*", "List:coworkerd/do/*", "List:coworkerd/nodes/*", "Read:coworkerd/members/node-1/*",
   "Write:coworkerd/members/node-1/*", "List:coworkerd/members/node-1/*", "Read:coworkerd/nodes/node-1/*",
   "Write:coworkerd/nodes/node-1/*", "List:coworkerd/nodes/node-1/*", "Write:coworkerd/do/*/shards/*",
   "Write:coworkerd/do/*/objects/*"]}
```

A running SeaweedFS takes a new identity at once with `weed shell`'s `s3.configure -user=... -access_key=... -secret_key=... -actions=... -apply` (what compose's `secrets` hands its store for every node), kept in its filer, and withdraws one the same way with `-delete`; it reads `s3.json` again only when it starts, and an identity read from it stays in force until then -- so compose keeps only the deploy identity there.
`coworkerd check storage` with `COWORKERD_READ_NODE_ID` reports whether a node's identity is held to the policy: the `policy` part of its last line, where writes below another node's `members/` and `nodes/` and below `cluster/`,
and a read of `cluster/credentials/current`, must be refused with `AccessDenied`, while the node's own prefixes take a write and a removal, and a read of `cluster/format/1` and a listing of `members/` succeed. The items try no write below `tenants/` or the code's prefixes, so an identity that may write those still reports `policy.ok: true`.

**The configuration file** `/etc/coworkerd/config.json` (at minimum the nodeId, the object store and the secrets key; the rest in section 5):

```json
{
  "nodeId": "node-1",
  "advertisedAddress": "10.0.0.11:8934",
  "storage": {
    "endpoint": "http://10.0.0.6:8333",
    "credentialsFile": "/etc/coworkerd/storage.json"
  },
  "secrets": {"keyFile": "/etc/coworkerd/secrets-key.json"}
}
```

`/etc/coworkerd/storage.json` (owned by coworkerd, mode 0600): `{"accessKey": "...", "secretKey": "..."}` (this node's own identity). The secrets key file must be readable by its owner only, otherwise the node refuses to start.
A node needs a management token and refuses to start without one: it goes into the environment file `/etc/coworkerd/env` (mode 0600): `COWORKERD_MANAGEMENT_TOKEN=...` (16 to 4096 visible characters, no spaces; the same for every node and the control plane); do not put it on the command line. Only a development node may start without one, with `"management": {"allowWithoutToken": true}` in its configuration: its management endpoints are then open to whoever reaches it (section 8).
`nodeId` is this node's name in the cluster and the name of its prefixes `members/<nodeId>/` and `nodes/<nodeId>/`: do not change it once set (a changed one is a new node, which needs its identity first).
`advertisedAddress` is what the other nodes dial; without it, the host's name and the peer port.

**The control plane** `coworkerd-apiserver --config /etc/coworkerd/control.json` reads the same kind of configuration file; it uses `storage` (the **deploy** identity), `secrets.keyFile`, like the nodes `management.token`, `usage.retentionHours` (its daily removal of every node's old usage objects), `membership.ttlSeconds` and `membership.livenessGraceMs`
(which members are live) and `desiredState.startupTimeoutMs`, and the `control` section: `control.listenAddress` (default `0.0.0.0:8940`), `control.adminCredential` (the admin credential, 16-4096 visible characters; better put into the environment file as
`COWORKERD_ADMIN_CREDENTIAL`), `control.pathRouting` (default `true`: tenants may route `/<tenant>/...` under the cluster's default host; with `false` only the hostnames registered to them),
`control.maxUploadBytes`, `control.serverTimeoutMs` (default 2000: how long it waits for each node's state or logs), `control.pollIntervalMs` (default 5000: how often it reads what another control plane on the same bucket wrote, and completes what a writer left half done),
`control.cleanupGraceMs` (default 3600000, at most 604800000: how old by the store's clock an object must be before a deletion's cleanup removes it, an upload having perhaps just found it; a younger one is listed below `cluster/cleanup/`, and the control plane's sweep, every `cleanupGraceMs` but at least every minute and at most every second, removes it once it is old enough and nothing references it)
and `control.temporaryDirectory` (default `$TMPDIR`, else `/tmp`: where an upload is unpacked; it must exist). It also needs `COWORKERD_MANAGEMENT_TOKEN` (to call the nodes' trial load and read their state and logs). It refuses to start without the admin credential or the secrets key. The admin credential is the platform's own;
tenant credentials are created with it through `/v1` (`cowctl credential create --tenant T`), and the store keeps only their hashes. More than one writer of the desired state -- several control planes, or the operations tool beside one -- may use the same bucket only when the store's create-if-absent holds under concurrency (`conditionalPutAtomic: true`).

**A new cluster's bucket** needs the cluster's marker before the first node or control plane starts; they refuse to start on a bucket without it, so that one connected to the wrong bucket does not serve as if nothing were deployed. Write it once with the operations tool:

```bash
COWORKERD_STORAGE_ENDPOINT=http://10.0.0.6:8333 AWS_ACCESS_KEY_ID=<deploy> AWS_SECRET_ACCESS_KEY=<deploy> /opt/coworkerd/bin/coworkerd-apiserver tool init
```

It writes `cluster/format/1` and empty tenant and credentials indexes, prints `{"operation":"init",...,"format":"created",...}` (`"present"` when the marker was there) and can run again (this is the step compose's `init` performs).
From then on, tenants, credentials and deployments go through the control plane (`cowctl`).

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

At startup the node prints one line with the effective configuration (`[server] {"event":"configuration",...}`; credentials and tokens show only whether they are set); without a token it does not start, unless `management.allowWithoutToken` is `true`, and then it prints a warning (`[server] {"event":"management",...}`).
`coworkerd check --config /etc/coworkerd/config.json` checks a node without starting it -- the configuration, the management token (a node without one would not start), egress, the peer key, the object store with this node's identity, the desired state (the secrets key opens, the bucket holds the marker),
the membership (a listing of `members/`, this machine's clock against the store's `Date`, a write and a removal below `members/<nodeId>/`) and the sandbox's prerequisites --, one `[check]` line each, then the result; it exits 0 when the node would start. With `sandbox.mode` `auto` its `sandbox` line is ok even when the prerequisites are not met; the `[sandbox]` line it prints says whether the sandbox would be in effect.

## 5. Configuration reference

The configuration file is a JSON object (unknown fields are refused). The table lists the common entries; `coworkerd --config FILE` reads it (the control plane `coworkerd-apiserver --config FILE` reads the same kind of file).

| Field | Default | Description |
| --- | --- | --- |
| `nodeId` | required | This node's name in the cluster (letters, digits, `-_.`, at most 255 characters, not `.` or `..`); also the segment of its objects `members/<nodeId>/` and `nodes/<nodeId>/`, which no other node's identity writes (the control plane's deploy identity, which may write the whole bucket, also removes expired usage objects there) |
| `storage.endpoint` | required | The object storage's S3 endpoint (`http://`); desired state, code and uploaded durable history are there |
| `storage.bucket` / `storage.region` | `coworkerd` / `us-east-1` | The bucket and the region |
| `storage.credentialsFile` | none: the environment variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | The node's own identity `{"accessKey","secretKey"}` (the control plane: the deploy identity) |
| `storage.connectTimeoutMs` / `storage.readTimeoutMs` / `storage.createTimeoutMs` | `2000` / `5000` / `30000` | Each request's connection; its answer, and every read or write after the one before; a create-if-absent's answer. A GET, HEAD, listing or DELETE that fails before its whole answer arrives is sent once more on a new connection; a create whose answer is lost is decided by reading the key back, and sent once more when the key is absent |
| `storage.timeoutMs` | `30000` | How long a prefetch waits for one object |
| `secrets.keyFile` | required | The cluster's secrets key `{"key":"<64 lowercase hexadecimal characters>"}` (that field only), readable by its owner only; the same file on the control plane and every node. The environment variable `COWORKERD_SECRETS_KEY_FILE` overrides it |
| `storage.cacheDirectory` / `storage.cacheMaxBytes` | `<stack directory>/cache` / 64 MiB | The local caches of code (bundles, `packages/`, `modules/`), each of the three bounded by `cacheMaxBytes` |
| `stackDirectory` | `/var/lib/coworkerd` | The stack directory: `logs/`, `run/` (the Unix sockets shared with workerd, mode 0700; a stack directory whose path is too long for those sockets stops the start), `cache/`, `processes/`, and on Linux with the sandbox not `off` `sandbox/` (0700) |
| `workerdPath` | `workerd` in the directory of `coworkerd` | The workerd executable |
| `runtimePath` | `../runtime/runtime.js` (relative to the directory of `coworkerd`) | The runtime script |
| `listenAddress` | `0.0.0.0:8930` | The ingress (also the management endpoints) |
| `peerListenAddress` | `0.0.0.0:8934` | The peer listener (calls from other nodes, trial loads during deployment) |
| `advertisedAddress` | `<hostname>:<peer port>` | The address other nodes use to reach this node; they must be able to connect to it |
| `management.token` | required | The management token (see section 8); the environment variable `COWORKERD_MANAGEMENT_TOKEN` takes precedence. A node without one refuses to start |
| `management.allowWithoutToken` | `false` | `true` lets a development node start without a token: its management endpoints are then open to whoever reaches its ingress or peer port, and it warns at startup |
| `processPool.count` | `1` | The number of ordinary workerd processes |
| `quarantineProcesses` | `2` | The number of quarantine processes. A **tenant** identified as the culprit (all its Workers) is kept here for an observation period, assigned to one of them by the tenant's hash, so that a tenant crashing a quarantine process does not affect the tenants in another shard; deployment **trial loads** run here too (in the target tenant's shard), so that a new version crashing a process does not take the in-flight requests of the ordinary processes with it. `0`: no quarantine processes; suspects and trial loads both fall back to the ordinary processes (not recommended) |
| `cpuLimitMs` / `memoryLimitMiB` | `200` / `128` | The CPU limit per request and the memory limit per isolate (the node's defaults; a limit the Worker declares takes precedence, see the end of this section) |
| `idleEvictMs` | `300000` | How long a Worker may stay idle before it is evicted |
| `drainTimeoutMs` | `30000` | How long to wait for in-flight calls when exiting; `0` turns the drain off (a drain closes the node's WebSocket connections with `1001` at its start) |
| `ingress.webSocketsPerTenant` | `1000` | The open WebSocket connections this node's ingress holds for one tenant: an upgrade beyond them is refused before the Worker runs, `429 WEBSOCKET_LIMIT`, and other requests are not affected; `0`: no limit |
| `rotation.*` | see the schema | Process rotation, which replaces a process by a new generation with in-flight requests completing in the old one: `binaryCheckMs` (rotate when the workerd file changes, default 1000), `residentBytes` (rotate a process whose resident memory reaches it; left out, 70% of the node's memory -- its container's `memory.max`, else `memoryBudgetBytes`, else the machine's), `maxAgeMs` (default `0`, off; a value replaces every process after that long, as releases before 0.4.1 did every hour), `maxRequests`, `drainTimeoutMs` and others |
| `gc.*` | see the schema | Garbage collection in the workerd processes ([process isolation](../process-isolation.md), section 4): `budgetMs` / `refillMsPerSecond` (`2000` / `100`: each Worker's budget of full-collection CPU the CPU limit does not charge to the request it runs in; `0` charges every collection), `longPauseMs` (`100`: a collection that pauses a process that long is named in `logs/workerd.log`; `0`: none), `idleCollectAfterMs` / `idleCollectMinGrowthMiB` (`50` / `4`: an idle Worker's isolate is collected between its requests; `0` turns it off), `initialOldSpaceMiB` (`8`: V8's initial old generation), `runtimeHeapMiB` (`128`: the runtime's heap; `0` leaves V8's own) |
| `sandbox.mode` | `auto` | The least-privilege environment of workerd ("Sandbox" in section 4): `auto` turns it on when the prerequisites are met, otherwise runs without it and warns; `launcher` refuses to start without them; `off` turns it off |
| `sandbox.launcherPath` | `coworkerd-launcher` in the directory of `coworkerd` | The launcher. The node checks and fixes it at startup: replacing the file while running triggers no rotation, and the state endpoint's `processes.sandbox.pendingRestart` says the new one is used only after a restart |
| `sandbox.syscalls.mode` | `observe` | The system call allowlist ("System call allowlist" in section 4): `off` installs nothing; `observe` reports calls outside the allowlist and executes them as usual; `enforce` kills the process on a call outside the allowlist (exit category `syscall-denied`). The environment variable `COWORKERD_SANDBOX_SYSCALLS` overrides it |
| `sandbox.syscalls.allowlistPath` | `coworkerd-syscalls-startup.txt` in the directory of `coworkerd` | The allowlist file (shipped in the release package, generated by the build for this workerd). The environment variable `COWORKERD_SANDBOX_ALLOWLIST` overrides it |
| `loadCheck` | `true` | Check that a Worker loads before its first dispatch, and refuse explicitly when it does not (`LOAD_FAILED`) |
| `logs.tenantBufferBytes` / `logs.linesPerSecond` | `1048576` / `100` | How much log each tenant keeps on this node and how many lines per second are recorded; `logBufferBytes` and `logLinesPerSecond` of the tenant's quota take precedence |
| `usage.flushIntervalMs` / `usage.retentionHours` / `usage.sampleIntervalMs` | `60000` / `744` / `1000` | Per-tenant usage: how often the hours whose counts changed are written to the object store (the interval is a scheduling delay, not a hard bound on lost usage; a startup read failure defers that hour's writes until its existing counts can be read), how long the hourly objects are kept (by their node, and by the control plane for every node; `0`: never deleted), the sampling interval of isolate time |
| `egress.mode` | `on` | Outgoing requests of tenant code (section 11): `on` goes through this node's egress service; `off` refuses every outgoing request at the policy point (`EGRESS_DENIED: egress-off`). The environment variable `COWORKERD_EGRESS_MODE` overrides it |
| `egress.defaultPolicy` / `egress.publicPorts` | `public` / `[80, 443]` | What a tenant may reach of the public addresses its own `allow` rules do not name: `public`, these ports of them; `deny`, none. The environment variable `COWORKERD_EGRESS_DEFAULT_POLICY` overrides it |
| `egress.tenants[]` | none | Each tenant's rules: `tenant` (the tenant name), `allow`, `deny`, `allowInternal` (internal destinations the administrator allows), `concurrency`, `requestsPerSecond` (section 11) |
| `egress.clusterAddresses` | none | Cluster addresses the node cannot see itself (load balancers, the nodes' other networks): `HOST[:PORT]`, an address or a CIDR; like object storage, the members and the control planes that announce themselves, tenants may not reach them |
| `egress.timeoutMs` / `concurrency` / `requestsPerSecond` | `30000` / `32` / `100` | The timeout of each outgoing request; each tenant's outgoing concurrency and requests per second on each node |
| `egress.dnsCacheMs` / `caFile` / `systemTrustStore` | `10000` / none / `true` | How long a resolution result may be reused at most (`0`: resolve every time); extra PEM certificates trusted for HTTPS destinations; whether the system's root certificates are trusted |
| `peerAuth.keyFile` / `peerAuth.windowMs` | none / `30000` | The cluster key file (section 12): when set, peer connections are signed and only signed ones accepted; the time window. The environment variable `COWORKERD_PEER_KEY_FILE` overrides it |
| `membership.ttlSeconds` / `membership.heartbeatIntervalMs` / `membership.listIntervalMs` / `membership.livenessGraceMs` | `15` / `5000` / `10000` / `10000` | Membership on the object store: a heartbeat every interval (less than half the TTL), a listing of the members every interval, and a member whose heartbeat is older than the TTL plus the grace (at least one heartbeat interval) is dropped; a node that wrote none for the TTL considers its epoch expired; after this node could not list the store for longer than the TTL, the members whose heartbeat objects still exist are kept for the TTL plus the grace from its first listing again |
| `membership.clockSkewToleranceMs` / `membership.createSettleMs` | `2000` / `1000` | How far this machine's clock may be from the store's `Date` (more refuses the start, and is only warned of while running); how long a new epoch's registration settles before it is read back |
| `membership.backend` / `desiredState.backend` | `objectStore` with the `storage` block | Where the members and the desired state are kept: a node started with `--config` requires `objectStore` for both, and `file` is only the command line's test form (a 0.3 `etcd` backend is refused, section 9) |
| `desiredState.pollIntervalMs` | `5000` | How often the node reads the tenant index's hint, and what changed (50 to 3600000) |
| `placement.subcalls` | `home` | Stateless Service Binding fetch / method placement: `home` uses the normal warm/home policy; `local` tries the caller's node first and falls back to that policy. Object calls follow their shard owner |
| `placement.*`, `membership.*`, `desiredState.*` | see the schema | Detailed parameters of placement, membership and the desired state; `storage` is the shorthand of `desiredState.objectStore` (they cannot both be given) and puts the desired state and the membership on the object store, which a node requires. Entries in `placements` use `nodeId`, and Workers their full name `tenant/Worker` |
| `control.*` | see "The control plane" in section 4 | Used only by `coworkerd-apiserver` (`listenAddress`, `adminCredential`, `pathRouting`, `maxUploadBytes`, `serverTimeoutMs`, `pollIntervalMs`, `temporaryDirectory`), but every daemon validates the section: a malformed `control` section stops a node too |

All fields are in `ServerConfig` of the source `common/schema/coworkerd.capnp`. Fields with `ForTest` are for tests only; do not set them in production. A configuration that names `etcdEndpoint` or `etcdCredentialsFile`, or `etcd` as a backend, is refused with a sentence that says 0.4 has no etcd (section 9).

**Per-Worker limits**: the CPU and memory limits are also properties of the Worker (`limits` of `PATCH /v1/tenants/{t}/workers/{w}`, at most the tenant's
`maxWorkerCpuMs` and `maxWorkerMemoryMiB`; when absent, the tenant's default limits, and when those are absent, the node's `cpuLimitMs` and `memoryLimitMiB`), recorded in the tenant's manifest with the active version.
**Nodes apply them per Worker**: each isolate in the same workerd process uses its own Worker's limits (the state endpoint's `workerLimits` shows the value in effect as `applied`, for each Worker whose own or tenant's limits apply, at most 256 per tenant); a Worker for which neither it nor its tenant names a limit uses the node's configuration. The process-wide WebAssembly memory limit of a generation is the largest memory limit named for any Worker of the deployment (its own, else its tenant's default), or the node's `memoryLimitMiB` if that is larger; a newly started generation starts with it.

**Environment variable overrides** (take precedence over the configuration file): `COWORKERD_MANAGEMENT_TOKEN`, `COWORKERD_ADMIN_CREDENTIAL` (control plane), `COWORKERD_SECRETS_KEY_FILE`, `COWORKERD_PEER_KEY_FILE`, `COWORKERD_DRAIN_TIMEOUT_MS`, `COWORKERD_LOAD_CHECK` (`0`/`1`),
`COWORKERD_CPU_LIMIT_MS`, `COWORKERD_MEMORY_LIMIT_MIB`, `COWORKERD_QUARANTINE_PROCESSES`, `COWORKERD_IDLE_EVICT_MS`, `COWORKERD_IDLE_DESTROY_INTERVAL_MS`, `COWORKERD_MEMORY_WATERMARK_ENABLED`, `COWORKERD_ROTATION_*`, `COWORKERD_RUNTIME_FILE`,
`COWORKERD_GC_BUDGET_MS`, `COWORKERD_GC_REFILL_MS_PER_S`, `COWORKERD_GC_LONG_PAUSE_MS`, `COWORKERD_IDLE_GC_MS`, `COWORKERD_IDLE_GC_MIN_MIB`, `COWORKERD_GC_INITIAL_OLD_SPACE_MIB`, `COWORKERD_STATIC_HEAP_MIB` (the `gc` fields, in that order),
`COWORKERD_SANDBOX_MODE`, `COWORKERD_SANDBOX_SYSCALLS`, `COWORKERD_SANDBOX_ALLOWLIST`, `COWORKERD_SANDBOX_SYSCALLS_STEADY`, `COWORKERD_SANDBOX_STEADY_ALLOWLIST`, `COWORKERD_EGRESS_MODE`, `COWORKERD_EGRESS_DEFAULT_POLICY`,
and for the Workers' logs `COWORKERD_TENANT_LOG_BUFFER_BYTES`, `COWORKERD_WORKER_LOG_TOTAL_BYTES`, `COWORKERD_WORKER_LOG_LINES_PER_SECOND`, `COWORKERD_WORKER_LOG_MAX_LINE_BYTES`, `COWORKERD_WORKERD_LOG_MAX_BYTES`, `COWORKERD_WORKERD_LOG_FILES`.
Some settings have no field in the file and are set only this way: the call lease's `COWORKERD_LEASE_TTL_MS` (default `30000`), `COWORKERD_SUBREQUEST_BUDGET` (`8`) and `COWORKERD_SUBREQUEST_GRANT` (`1`);
the limits on a package's assets, `COWORKERD_ASSET_MAX_COUNT` (`20000`), `COWORKERD_ASSET_MAX_FILE_BYTES` (25 MiB) and `COWORKERD_ASSET_MAX_TOTAL_BYTES` (100 MiB); and the log buffers `COWORKERD_LOG_BUFFER_BYTES` and `COWORKERD_WORKERD_LOG_BUFFER_BYTES` (4 MiB each).
Names ending in `_FOR_TEST` are for tests only.

**The image's entry point script** writes a configuration file from environment variables (to `config.json` in the stack directory `COWORKERD_STACK_DIR`, by default `/var/lib/coworkerd`) and then starts: `COWORKERD_NODE_ID` and
`COWORKERD_STORAGE_ENDPOINT` (these two are required), `COWORKERD_STORAGE_BUCKET`, `COWORKERD_STORAGE_REGION`,
`COWORKERD_STORAGE_CREDENTIALS_FILE` (by default `server.json`, the node's own identity, in the secrets directory), `COWORKERD_SECRETS_KEY_FILE` (by default `secrets-key.json` there; the node does not start without it),
`COWORKERD_PEER_KEY_FILE` (by default `cluster-key.json` there, used when present), `COWORKERD_PEER_ADDRESS`, `COWORKERD_LISTEN_ADDRESS`, `COWORKERD_PEER_LISTEN_ADDRESS`,
`COWORKERD_PROCESS_COUNT`, `COWORKERD_STACK_DIR`, and `COWORKERD_CONTROL_ADDRESS` (the control plane's address, written into `egress.clusterAddresses`, default `apiserver:8940`);
with `COWORKERD_CONFIG=<file>` set, that file is used directly. Both ways give the same configuration. The secrets directory is `COWORKERD_SECRETS_DIR` (default `/var/lib/coworkerd-secrets`); either way the management token, `COWORKERD_MANAGEMENT_TOKEN` or else the secrets directory's `management-token`, reaches the node through its environment, so it takes precedence over a file's `management.token`.

## 6. Tenants and deployments

### Durable Objects and local storage

`durableObjects.processes` defaults to `1` (`d0`), accepts `0`–`64`, and is independent of `processPool.count`. At `0` a node still receives and forwards object / database calls but acquires no shards. Its membership advertises that count. The durable settings below belong in the JSON configuration (`COWORKERD_CONFIG` uses that file directly).

| `durableObjects` key | Default | Meaning |
| --- | --- | --- |
| `processes` | `1` | Number of durable workerd slots, not per tenant |
| `peers` | `2` | Reachable peers that hold committed frames in memory; at most 8; fewer reachable peers reduce K; an empty selected list uses a local flush, but an entirely unreachable nonempty list waits / times out |
| `confirmTimeoutMs` | `5000` | Confirmation deadline; range 100–29000 ms |
| `recoverTimeoutMs` | `30000` | How long an object request waits for recovery; at least 1000 ms |
| `segmentIntervalMs` / `segmentBytes` | `30000` / `16777216` | Seal the node's log every 30 s or 16 MiB; minima 100 ms and 64 KiB |
| `snapshotBytes` / `snapshotIntervalMs` | `67108864` / `86400000` | Snapshot after 64 MiB of history or one day |
| `snapshotPartBytes` | `16777216` | 16 MiB parts; range 64 KiB–256 MiB |
| `diskBytes` | `0` | `0` chooses half the capacity of the stack's filesystem at startup; not half its free space |
| `peerBufferBytes` | `268435456` | 256 MiB total memory for other owners' tails; refusal beyond it |
| `objectBytes` | `10737418240` | 10 GiB per object including its facet databases, based on reported checkpoint / open / close sizes |
| `checkpointBytes` / `idleCheckpointMs` | `4194304` / `10000` | Checkpoint threshold 4 MiB and idle delay 10 s |
| `uploadRetryMs` / `uploadRetryMaxMs` | `500` / `30000` | Upload retry backoff, doubling to 30 s |
| `recoverParallelism` | `4` | Downloads in flight per recovery; range 1–16 |
| `residentObjects` | `4096` | Target idle object-tree eviction per durable process; `0` unlimited; active trees can exceed the target |
| `usagePublishMs` | `3600000` | Publish this node's estimated tenant object bytes every hour |

The stack directory holds `cache/` for verified code and assets, `do/` for recovered object databases and facet indexes, and `do/log/` for the local append log and recovery metadata, alongside `processes/`, `run/` and `logs/`. Object databases live under `do/coworkerd-do-host-v1/` with hashed Host ids, `.sqlite` / facet files, not paths named by the application's object name. Use a persistent writable volume and leave headroom for SQLite WAL, logs and snapshots; `diskBytes` bounds evictable recovered files, not all disk use. Do not remove a single-node cache or append log while it can contain the only confirmed tail. Files are reused only when boot / clean-exit markers and confirmed positions permit it; an unclean machine reboot rebuilds from uploaded history and surviving peers. On macOS the boot identifier is unavailable, so an unclean process exit also drops local durable files. On Linux a same-boot restart with the volume intact reuses the append log and its confirmed WAL positions. See [cluster state](../cluster-state.md), section 12.

**Tenants** are the platform's users: the administrator (the platform) creates a tenant with the admin credential, registers hostnames for it, sets its quota, and then issues it a tenant credential; the tenant deploys its own Workers with its own credential.
`cowctl tenant create|list|get|update|delete` and `cowctl credential create|list|revoke` are the administrator's subcommands (see sections 2 and 11 of the [quickstart](quickstart.md));
the API is `/v1/tenants/...` (the control plane, compose's `apiserver` service, local port `18090`), and each request carries `Authorization: Bearer <credential>`.

| Tenant quota (`--quota name=value`) | Default | Where it is enforced |
| --- | --- | --- |
| `workers`, `versionsPerWorker` | 100, 50 | In the control plane when a new version is uploaded, and `workers` also when a secret is set for a Worker the tenant does not have yet (when full, delete old versions first: the first version does no automatic cleanup) |
| `packageBytes`, `assetFiles`, `assetBytes` | 160 MiB, 20000, 100 MiB | In the control plane on upload |
| `secrets`, `hostnames` | 1000, 100 | In the control plane when a secret is set or a hostname registered |
| `logLinesPerSecond`, `logBufferBytes` | 100, 1 MiB | On each node, per tenant (lines over the limit are counted in `[dropped]`; requests are not affected) |
| `maxWorkerCpuMs`, `maxWorkerMemoryMiB` | 30000, 512 | In the control plane when Worker-level limits are set (the limits themselves: see the end of section 5) |

`durableObjectNamespaces` defaults to 16, counting class namespaces, databases and retained deleting namespaces. `durableObjectBytes` defaults to 10 GiB (10,737,418,240 bytes), enforced at durable confirmation using lagging node summaries; it can overshoot. Deleting namespaces / databases stop counting bytes, but 0.5 retains their history and names. Over the object cap a write gets `413 DO_OBJECT_TOO_LARGE`; over the tenant byte estimate it gets `422 QUOTA_EXCEEDED`. Reads continue; a refused transaction can remain locally visible until reopen repairs it. [Durable Objects](durable-objects.md) and [databases](databases.md) give the usage model.

Over a quota gives `422 QUOTA_EXCEEDED`, with a `reason` that starts with the quota's name (`defaultLimits` when a tenant's default limits exceed its ceilings; [error codes](errors.md)). A package may be as large as the smaller of `packageBytes` and the control plane's `control.maxUploadBytes`: a larger one gets `422 QUOTA_EXCEEDED` while `packageBytes` is the smaller, otherwise -- as with the defaults, both 160 MiB -- `413 PACKAGE_TOO_LARGE`. **Hostnames**: a hostname belongs to one tenant only (`cowctl tenant update T --hostname H` sets the tenant's whole list: repeat `--hostname` for every one it keeps);
a tenant's routes may use only the hostnames registered to it, or paths starting with `/<tenant>/` under the cluster's default host `*` (not the latter when `control.pathRouting` is `false`), otherwise `403 HOSTNAME_NOT_OWNED`. A request whose hostname is registered to a tenant is matched against that tenant's routes only, and a path prefix matches whole path segments (`/acme` does not take `/acme-corp/...`).
**Deleting a tenant** (`cowctl tenant delete T`, which asks for confirmation, or without a terminal needs `--yes`): it is first marked `deleting` -- every node stops serving it within a few seconds, and requests get `503 TENANT_UNAVAILABLE` --, then eligible code / asset objects are cleaned up, credentials removed, and finally the tenant record with its routes, active pointers and secrets. Young code objects remain deferred by the cleanup grace, and Durable Object / D1 history is retained in this release. A step that cannot finish -- an object the store does not remove, for example -- leaves the tenant `deleting` with `503 DELETION_INCOMPLETE`, and `cowctl tenant delete T` again resumes that deletion workflow;
[control plane API](../control-plane-api.md), section 6, describes each step and what a failure answers.

**Deploying with `cowctl`**: in the directory of a wrangler project, `cowctl init --tenant T` and `cowctl deploy`, then `logs`, `versions`, `rollback`, `secret put` and `usage` --
see the [quickstart](quickstart.md). It talks only to the control plane and needs neither the address nor the credentials of object storage, only the tenant credential; which wrangler features work is in the [compatibility scope](compat.md).

**The internal tool** (for operations and tests): `coworkerd-apiserver tool <init|apply|upsert|remove|show|compile|secret|rotate|drain> ...`
writes the object store directly with the deploy identity, which it takes from the environment variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (it has no option for them; the image's entry point sets them from `deploy.json`). Under compose it is `docker compose run --rm tool upsert --manifest /work/manifest.json`: it reads a
manifest (`docker/examples/hello/manifest.json`) and the code directory `<workerId>/<versionId>/<main module>`; `contentHash` is the main module's SHA-256.
Without `--tenant` it acts on the **`default` tenant** (the Worker names in the manifest are names within the tenant, and the full name is `default/<name>`); the platform's tenants go through `cowctl`.
It writes the same desired state as the control plane (the same write protocol: each tenant's manifest) and likewise asks a node for a trial load before deploying; it creates the cluster's marker when the bucket has none.

## 7. Ports and networks

| Port | Used by | Under compose |
| --- | --- | --- |
| 8930 | Ingress: user requests and the management endpoints | Published locally as `18080` (`18081`, `18082`) |
| 8934 | Peer: sessions between nodes, which carry every call between them (the CONNECT that opens each is signed with the cluster key); from the control plane and the tool, trial loads during deployment, the reads of each node's state and logs, and the control plane's announcements (with the management token) | Only on the internal network (`node-N-peer`) |
| 8940 | Control plane: `cowctl`'s API `/v1` (with credentials) | Published locally as `18090` |
| 8333 | Object storage, S3 | Only on the internal network |

compose has two networks: `backend` (internal, without a gateway: object storage, the nodes' peer listeners, the control plane, the initialization and tool containers; the host cannot reach it) and
`frontend` (the nodes and the control plane: their published ports, and the nodes' egress). All containers run as a non-root user (uid 10001).

On bare metal: the ingress is for users and the front load balancer; the control plane only for the platform's own systems and `cowctl`; the peer port only between the nodes and from the control plane and the operations tool; object storage not exposed.
Each of these channels has credentials: peer connections are signed with the cluster key (section 12),
object storage requests are signed and each node has an identity of its own that writes its own membership / usage and shared Durable Object prefixes ("Object storage identities" in section 4); the management endpoints use the management token, the control plane the admin and tenant credentials. Network isolation still has to be done -- apart from the credentials, tenant code itself cannot reach
these ports either (section 11).

## 8. Management endpoints and the token

| Path | Purpose | Token |
| --- | --- | --- |
| `GET /__coworkerd/ready` | Readiness (503 while draining, and while an ordinary process slot is not ready -- the quarantine shards, where trial loads and suspect tenants run, do not count, and `state` shows them under `readiness`; load balancers take the node out on it) | Not needed |
| `GET /__coworkerd/probe` | Liveness (200 as long as the process is there) | Not needed |
| `GET /__coworkerd/state` | State: `nodeId`, members (`membership`), the object store (`storage`: its create-if-absent measurement, the clock check and `appliedTenantsRevision`, the tenant index revision it serves), tenants (`tenants`), per-tenant usage (`usage`), processes (including the quarantine shards and the per-tenant suspects), deployment, version (`version`), effective configuration (`configuration`), the Durable Object shards it holds, its takeovers, releases and refusals and its owner cache (`durableShards`), the objects' log -- the confirmation latency, what is not confirmed and not sealed, the segments and uploads, the peers, the rebuilds, this machine's files, the resident objects of each durable process with its evictions and wakes, the alarms it has to fire, the namespaces it holds objects of and what each tenant's objects hold (`durableLog`) --, counters; on both the ingress and the peer port | Needed |
| `GET /__coworkerd/logs?worker=<tenant>/<worker>` | A Worker's recent log lines on this node, what `/v1`'s logs gather (on both the ingress and the peer port) | Needed |
| `POST /__coworkerd/control-plane` | A control plane announces its addresses (on the peer port) | Needed |
| `GET /__coworkerd/deployment`, `/placement?workers=`, `/process-placement?workers=` | The deployment view, placement | Needed |
| `POST /__coworkerd/drain`, `GET /__coworkerd/drain` | Start exiting (like SIGTERM), see the drain's progress | Needed |
| `POST /__coworkerd/rotate?all=1`, `GET /__coworkerd/rotation` | Process rotation | Needed |
| `POST /__coworkerd/trial-load` | Trial load (used by the deployment tool; on both the ingress and the peer port) | Needed |
| `POST /__coworkerd/load-band`, `POST /__coworkerd/reload` | For tests and the file backend | Needed |

A node needs a token (`management.token` or `COWORKERD_MANAGEMENT_TOKEN`) and refuses to start without one; everything under `/__coworkerd/` except `ready` and `probe` needs
`Authorization: Bearer <token>`, otherwise 401; each refusal is logged as a line `[management] {"event":"unauthorized",...}` (without the token).
A development node whose configuration says `management.allowWithoutToken: true` starts without one, and its endpoints are then open -- on the ingress too, so whoever reaches it can read the state and any Worker's log lines, drain, rotate and trial-load --, which its startup log says. The deployment tool, `tool drain` and `tool rotate` read the environment variable `COWORKERD_MANAGEMENT_TOKEN`
(or `--token`, but others can see command line arguments; prefer the environment variable).

Changing the token: edit `.env` (or the environment file), then start the nodes and the control plane again one by one with the new value -- each daemon reads the token from its environment only when it starts; the internal tool uses the new token. While a node's token differs from the control plane's, the control plane's trial loads, state and log reads and announcements to that node are refused with 401 -- a trial load then moves on to another node, and a deployment fails, with `503 TRIAL_UNAVAILABLE` naming `HTTP 401: the node requires the management token`, only when no node takes it; a refused announcement also lets the control plane's announced addresses drop out of the destinations that node keeps tenant code from, ten minutes after the last one it accepted (at once on a node started after the change), so then only what `egress.clusterAddresses` names stays refused there --, so do not leave them apart for long. Changing the admin credential likewise: edit it and restart the control plane; tenant credentials are replaced through `cowctl credential` (issue a new one, revoke the old one), without restarts.

## 9. Upgrades and draining

- **One node exits**: `SIGTERM` (`docker compose stop`, `systemctl stop`) or `coworkerd-apiserver tool drain --server http://<ingress> --wait`
  (with the token). The node first announces its exit (readiness turns 503, its drain marker appears in the object store, and the other nodes stop placing new calls on it at their next listing), waits for the in-flight calls to complete (at most `drainTimeoutMs`), closes workerd,
  writes out the current hour's usage, removes its heartbeat and drain marker, then exits. A second SIGTERM exits immediately. compose allows 45 seconds (`stop_grace_period`).
- **Upgrading nodes**: one at a time: load the new image, `docker compose up -d node-1` (recreates that one), wait until it is ready, then the next. Keep at least two nodes in the membership table and have the clients' front load balancer take a node out when its readiness turns 503. Clients must still handle refusals and connection failures: a request that still reaches the leaving node gets `503 SERVER_DRAINING`, to be sent to another node, and its port refuses connections once it has exited.
- **Replacing workerd**: replace the file `workerdPath` points to; the node notices within a second and rotates process by process, with in-flight requests completing in the old process;
  `coworkerd-apiserver tool rotate --server ... --all` rotates by hand as well.
- **Replacing workerd** (only workerd, not the whole package): first put in an allowlist covering both the old and the new bytes, then replace the binary, finally switch to the allowlist covering only the new bytes -- the order and the reasons are in "System call allowlist" in section 4.
- **Replacing the launcher** (`bin/coworkerd-launcher`, upgraded together with the node): the node checks and fixes it at startup, so replacing the file while running triggers no rotation, and the state endpoint's
  `processes.sandbox.pendingRestart` says the new one is used only after a restart; so replace it together with the node, then restart the node. When the prerequisites are met through an AppArmor profile (section 4),
  which attaches by path, stop the node first, then replace the file: a running node keeps executing the launcher it opened at startup, which after a replacement is no longer at the profile's path (how the profile then applies has not been tested).
- **Version**: `coworkerd --version` (`coworkerd-apiserver --version` in the same format) prints the version, the source it was built from, and the workerd it expects: the upstream version and the SHA-256 of the patch series; the state endpoint's `version` says the same.

**Upgrading 0.4.x to 0.5.0 is in place.** Keep the bucket, cluster marker, secrets and peer keys, node ids and persistent volumes. The object layout adds `do/` and node WAL / usage objects without rewriting the 0.4 desired-state layout. Back up the bucket and configuration as for any upgrade, load the new image, update the compose / entry point files from the new package, and rerun `docker compose run --rm secrets` so each node's storage policy can read `do/` and `nodes/` and write the shared DO shard / object prefixes. On bare metal, update those policies yourself (section 4).

Upgrade the control plane and then nodes one at a time, waiting for each readiness check before continuing; retain the old volumes. Keep stateless traffic during this sequence, with ordinary retry handling for a node that drains or is unavailable. Finish upgrading all nodes and writers before deploying DO / D1 definitions or adding the new configuration keys: a 0.4 daemon does not understand the new runtime / ownership protocol or those keys. `/v1` keeps its fields and adds new fields and endpoints; clients must also handle the new errors in [errors](errors.md).

**Upgrading from 0.3.x** is not an upgrade in place: 0.3.x kept the cluster's state in etcd, 0.4 keeps all of it in the object store and reads nothing of 0.3's, and nothing is migrated. Build the cluster anew:
deploy tenants, credentials, Workers and secrets again through `/v1` (`cowctl`) on a new 0.4 cluster. Under compose that means a new project or `down -v` first -- the `secrets` volume as well:
a 0.4 node needs an identity of the store of its own, and a 0.3 `secrets` volume holds in each node's directory the one read identity 0.3 gave every node, which a 0.4 `secrets` step would keep. On bare metal, give every node its own identity and the secrets key, and remove `etcdEndpoint` and `etcdCredentialsFile`
from every configuration: a 0.4 daemon refuses a configuration that names them, or an `etcd` backend, with `0.4 has no etcd: ... (the members, the desired state, the suspects and the usage are on the object store: the storage block)`.
Use the pinned SeaweedFS 4.48 or another store verified to have the properties in section 4; on the 0.3.x compose files' SeaweedFS 3.97 create-if-absent does not hold under concurrency: a 0.4 node starts there with a warning, and only one writer of the desired state may run at a time.

## 10. Troubleshooting

| Symptom | Cause and remedy |
| --- | --- |
| `up --wait` reports `init` failed | `docker compose logs init`: object storage did not pass the self-check within two minutes (for example a full disk, or this machine's clock against the store's `Date`), or the cluster's marker could not be written; `down -v` and start again |
| The control plane refuses to start with `set control.adminCredential or COWORKERD_ADMIN_CREDENTIAL` | No admin credential: compose's `secrets` step generates it; on bare metal see "The control plane" in section 4 |
| `cowctl` reports `UNAUTHORIZED` / `FORBIDDEN` | The credential is wrong, revoked or expired / its scope is not enough (a tenant credential acts only on its own tenant; `tenant` (except reading its own record), `credential create` and `credential list`, `node` and `usage --all` need the admin credential), see [error codes](errors.md) |
| A node or the control plane refuses to start: `the bucket ... holds no cluster/format/1` | The bucket is not a cluster's yet, or it is the wrong bucket: compose's `init` writes the marker; on bare metal `coworkerd-apiserver tool init` ("A new cluster's bucket" in section 4) |
| `init`'s contract line or `check storage` reports `"conditionalPutAtomic":false` | The store's create-if-absent does not hold under concurrency (observed on SeaweedFS 3.97). Use a verified store, such as the pinned 4.48. Until then run one desired-state writer only; Durable Objects are unavailable |
| A node refuses to start: `membership refuses to start: clock skew` | This machine's clock is more than `membership.clockSkewToleranceMs` (2 s) from the store's `Date`; the reason says by how much and which store. Install NTP |
| A node refuses to start: `membership nodeId conflict` | Another process holds this `nodeId`: its heartbeat was written again while this one waited. Stop one of them; a node that finds its id taken while it runs exits with 3 |
| A node refuses to start: `no management token: ...` | Set `COWORKERD_MANAGEMENT_TOKEN` (or `management.token`); compose's `secrets` step generates one. Only a development node may run without, with `management.allowWithoutToken: true` (section 8) |
| A node or the control plane refuses to start: `0.4 has no etcd: ...` | A configuration from 0.3.x: see "Upgrading from 0.3.x" in section 9 |
| A node or the control plane refuses to start: `... secrets key ...` | `secrets.keyFile` (or `COWORKERD_SECRETS_KEY_FILE`) is not set or not readable, the file has group or other permissions (it must be readable by its owner only, for example mode 0600), or it is not `{"key":"<64 lowercase hexadecimal characters>"}` |
| The internal tool reports `HTTP 401: the node requires the management token` (a deployment's trial load) or `the Server did not start draining (HTTP 401): it requires the management token` (`tool drain`), or `tool rotate` prints the node's `UNAUTHORIZED` answer | The internal tool has no token, or another one than the node's: set `COWORKERD_MANAGEMENT_TOKEN` to the nodes' token (compose's `tool` reads the one `secrets` generated by itself) |
| Deployment reports `trial load ... failed` | The new version cannot be loaded; the reason is in the error (cleaned: no secret values or paths); the pointer was not switched |
| A request gets 503 `RESULT_UNCERTAIN` | The call was dispatched and did not finish with an answer: the Worker failed while executing (for example it threw), its workerd process exited, the connection to the node running it broke, or the drain deadline cut it. The reason is cleaned, and the node logs nothing fuller (a call that ran on another node gets a `[server] RESULT_UNCERTAIN:` line with the same text); what the Worker printed is in `cowctl logs` |
| Startup reports `stack directory's path is too long` | The stack directory's path is too long for a Unix socket: use a shorter path |
| Startup reports `another Server runs in this stack` | Another node already runs in the same stack directory |
| The log has `[sandbox] {"event":"warning",...}` | `sandbox.mode` is `auto` and the launcher's startup check failed, so workerd runs without the sandbox; `reason` says which step, as `<step>: <error>: <errno text>` (for example `unshare: unshare(0x5c020000): Operation not permitted`). The same prefix with `sandbox.syscalls.mode is ...` (or `sandbox.syscalls.steady.mode is ...`) means only that system call table could not be installed, every other layer in effect, and `cannot resolve the workerd executable's libraries` that a start kept the previous list; see "Sandbox" in section 4 |
| Startup reports `sandbox.mode is launcher, and the launcher cannot set up ...` | As above, refused in `launcher` mode |
| A process's `lastExitCategory` in the state endpoint is `launch-failed` | The launcher failed before handing over to workerd (`lastLaunchFailure.reason`, with the matching `[coworkerd-launcher]` line in `workerd.log`); retried with the usual backoff |
| The log has `[syscall-observed] {...}` | In observe mode a call is outside the allowlist; see "What to do about an observation" in section 4; the process's behaviour is not affected |
| A process's `lastExitCategory` in the state endpoint is `syscall-denied` | In enforce mode the process made a call outside the allowlist and the filter killed it (`processes.sandbox.syscalls.recentDenials` has the record, and the kernel log a seccomp audit record with `sig=31`); the slot restarts with the usual backoff. Switch back to `observe` first, attribute the call and add it to the allowlist |
| A startup failure reason contains `allowlist ... was generated for workerd ...` | The allowlist and this machine's workerd are not a pair (workerd was replaced without the allowlist, or the other way round). Generate it again as in section 4, or take both files from the same release package |
| `fetch` throws `EGRESS_DENIED: <rule>` / `EGRESS_FAILED: <category>` | The outgoing request was refused by policy or could not connect, see section 11 and section 4 of the [error codes](errors.md); the node log's `[egress]` lines have the destination and the reason |
| The log has `[peer-auth] {...,"event":"refused",...}` and cross-node calls get `PEER_UNAVAILABLE` | The peer connection is unsigned or its signature is not accepted: `reason` `unknown-key` (the two key files differ; a rotation is not finished), `expired` (the two clocks differ by more than 30 seconds: install NTP), `missing` (the other side has no cluster key configured), `bad-signature` (the same key id with another secret: the two files differ in a secret), `wrong-target`, `malformed`, `replayed` or `replay-cache-full`. See section 12 |
| The object store answers `403 AccessDenied` to a node | The node's identity is not allowed what it did -- its policy ("Object storage identities" in section 4); a node writing another node's objects is refused by design |
| `cowctl` or `/v1` answers `503 STORAGE_UNAVAILABLE` | The control plane cannot reach the object store: a write can fail before or after committing its manifest revision (inspect state before repeating a non-idempotent operation); reads are answered from its mirror and the members from its last listing, except the usage, which is read from the store, and a first read of Workers whose definitions it has not fetched yet; the nodes keep serving |

## 11. Egress: tenant code reaching the network

Tenant code's `fetch()` does not connect to the network from workerd itself -- with the sandbox active the workerd processes have no network interfaces ("Sandbox" in section 4), and without it they reach the network only through this egress configuration -- but goes to **the egress service of the node it runs on**
(`<stack directory>/run/egress.sock`): the node checks the destination against the policy, resolves names itself, opens the connection itself (it also does the TLS of HTTPS, verified against the system's root certificates),
counts the bytes, and hands the answer back. When refused or unable to connect, `fetch` throws `EGRESS_DENIED: <rule>` or `EGRESS_FAILED: <category>` (section 4 of the [error codes](errors.md)).
The egress service strips connection headers and `x-coworkerd-egress*`, sets `Host` to the destination, and regenerates WebSocket handshake headers (preserving the subprotocol); it adds no tenant-identifying header; redirects are followed by `fetch` and each hop is checked again; HTTP, HTTPS and WebSocket are supported.

**Refusals no rule can open**: loopback, link-local, the clouds' metadata addresses (169.254.169.254 and others), `0.0.0.0`, reserved and multicast addresses, and **the cluster's own addresses** --
all of this node's interface addresses, the advertised address of every node in the membership table, the object storage endpoint, the addresses every control plane announces to the nodes, and those listed in `egress.clusterAddresses` (load balancers and others the node cannot see itself),
on any port. When nodes have several networks (for example a public address and an internal one), write all the networks the cluster's nodes are on into `egress.clusterAddresses` (CIDR), because the membership table has only each node's advertised address.

**The default policy** (`egress.defaultPolicy`): `public` (the default) allows the ports `egress.publicPorts` names (80 and 443 by default) of the public addresses no `allow` rule of the tenant names, and refuses internal ones; `deny` allows nothing except what the tenant's rules allow.
**Each tenant's rules** (`egress.tenants`, in the node configuration):

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
Each request has a line `[egress] {...}` in the node log (not counted against the tenant's log quota), except that limit refusals are logged at most once a second per tenant, with the count of the lines left out; the state endpoint's `egress.cluster` lists the single addresses currently treated as the cluster's own (the first 64) and counts the CIDR ranges.

**HTTPS certificates**: the node verifies destinations against the system's root certificates (the image has `ca-certificates` installed); add an internal CA with `egress.caFile` (PEM, several certificates allowed).
To turn egress off: set `egress.mode` to `off` (or `COWORKERD_EGRESS_MODE=off`), and tenant code's `fetch` fails.

## 12. The cluster key and the other credentials

**The cluster key** (peer calls between nodes): each node signs the peer connections it opens (HMAC-SHA256 over a timestamp, a nonce, the target node, the method, the path and the body's hash);
the peer listener opens a peer session -- the Cap'n Proto CONNECT that carries every call between nodes -- only with a valid signature, a time within a 30-second window and an unused nonce, answers the rest with 401 and logs a line `[peer-auth] {...,"event":"refused",...,"reason":...}`; its HTTP paths (trial loads, state, logs, the control plane's announcements) take the management token instead;
the state endpoint's `peerAuth` has the key ids, the accepted count per key and the refused count per reason. The key file (readable by its owner only -- for example owned by coworkerd with mode 0600 --, otherwise the node refuses to start):

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

**The secrets key** cannot be replaced on a running cluster: a value sealed with one key does not open with another, and a node that cannot open a value treats the secret as missing, so the Workers that declare it are not served until the value is set again.
Keep `secrets-key.json` with the other credentials of the cluster; under compose it is in the `secrets` volume.

**Object storage**: requests are always signed; the deploy identity is only for the control plane and the operations tool, and each node has an identity of its own that writes its own membership / usage and shared Durable Object prefixes (compose's directory layout does exactly that; writing another node's objects with a node's identity gets 403).

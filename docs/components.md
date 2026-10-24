# Components and boundaries

This document is for operators and contributors who need to know what runs where: the components and what each is responsible for, the processes of a node, the two deployment forms, the channels between components and how each is authenticated, where configuration comes from, and how the repository and the release package are laid out. Installation steps are in the [installation guide](guide/install.md).

## 1. The components

| Component | Form | Responsible for | Not responsible for |
| --- | --- | --- | --- |
| `coworkerd` | The node daemon, one per stack directory | The ingress and the management endpoints; placement per call and forwarding to peers; the pool of workerd processes with its watchdog, limits, suspects, quarantine, eviction and rotation; per-tenant logs and usage; its membership, suspects and usage in the object store; following the desired state | Writing tenants, credentials or deployments; talking to `cowctl` |
| `coworkerd-apiserver` | The control plane, an executable of its own | The `/v1` API; tenants and credentials; validation and trial loads of new versions; publishing code packages to object storage; writing the desired state; collecting state and logs from the nodes and summing usage from the object store; the operations tool `coworkerd-apiserver tool` | Running tenant code; serving tenant requests |
| `coworkerd-launcher` | A small C program, built only for Linux | Setting up the least-privilege environment of one workerd process and installing its system call filter, then executing workerd | Anything else |
| `workerd` | Upstream workerd `v1.20261012.1` with the patch series in `patches/workerd/` | Executing tenant code; the workerd side of the limits, idle eviction, the memory watermark and the steady-state system call filter | Placement, state, policy |
| The runtime bridge | `runtime/runtime.js`, copied into every workerd generation | Loading each version into an isolate of its own, building its `env` and dispatching each call to its entry point | -- |
| `cowctl` | A Node.js command line, with the wrangler adapter `cli/adapter.mjs` | Converting wrangler projects and deploying them; the developer's and the administrator's commands, all through `/v1` | Talking to object storage or the nodes |
| Object storage | External, an S3-compatible service reached over plain HTTP with path-style requests | The cluster's whole state: membership, the desired state (tenants, credentials, deployments, sealed secrets), version definitions, code packages, modules and static assets, suspects and usage ([cluster state](cluster-state.md), section 2) | -- |

**One executable per role.** The node and the control plane are the desired state's consumer and producer, run in different places and need different permissions. They are separate executables: `coworkerd` is built from `node/`, `coworkerd-apiserver` from `apiserver/`, and what they share is the static library `common`; neither has the other's directory on its include path, so code that crosses the boundary does not compile, the executable's name is its role, and the node carries no control-plane code. The launcher is a program of its own because it has to be small and do one thing before workerd starts.

**Names.** The daemons carry the project's name (`coworkerd`, `coworkerd-apiserver`, `coworkerd-launcher`), as containerd does; only the command line typed many times a day is short, `cowctl`. Files and environment variables follow the program that reads them: the daemons read `COWORKERD_*` -- and, without a storage credentials file, the object store identity from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` -- and the file given with `--config`, which has no default path; `cowctl` reads `cowctl.json` in the project and its credential from `COWCTL_TOKEN` or `~/.cowctl/token`.

## 2. The processes of a node

| Process | Started by | What it does |
| --- | --- | --- |
| `coworkerd` | A service manager on bare metal (the [installation guide](guide/install.md), section 4, gives a systemd unit), or the image's entry point | The event loop with every listener -- the ingress (8930), the peer listener (8934), the Unix sockets for its workerd processes -- the process pool, membership, the egress service and the management endpoints |
| `coworkerd-log` | `coworkerd`, forked before the event loop starts | Writes the node's own log: the node's lines go into a shared-memory ring (4 MiB by default), and this process copies them to the original output -- the terminal, journald or the container log -- so a slow reader never holds up the event loop; a line that does not fit is dropped and counted. Only what the node writes before this process starts -- the ring's own warning, a refusal to start -- and the launcher's startup check reach that output directly |
| `coworkerd-wlog` | `coworkerd`, forked right after the log writer | Receives the output pipes of every workerd process, writes them to `logs/workerd.log` (rotated at 256 MiB, four files by default) and keeps each tenant's recent lines for its logs |
| workerd processes | `coworkerd`, through `coworkerd-launcher` when the sandbox is active | `processPool.count` ordinary processes `p<n>` (default 1) and `quarantineProcesses` quarantine processes `q<n>` (default 2); each listens on an inherited Unix socket and has no TCP port |

On Linux, when the sandbox is not `off` and its preparation succeeds -- the launcher is an executable file, the sandbox root can be created and workerd opened --, the node also runs short helpers at startup: the dynamic loader's `--list` on a dynamically linked workerd, to learn which shared objects to bind into the sandbox, and then the launcher's own `--check`, which decides whether the sandbox can be used. Beside the event loop it runs threads that hash executables, resolve egress names and, while the system call filter is in `observe`, receive its reports.

The node's working state is its stack directory (`stackDirectory`, default `/var/lib/coworkerd`):

- `logs/` holds `workerd.log` and its rotations, and the workerd processes' pid files (`<slot>.pid`, and `workerd.pid` for the first); the node's own log is not there, it goes to standard output and error.
- `run/` (mode 0700) holds the Unix sockets the workerd processes call back on and each process's own socket; it is the one directory a sandboxed workerd can write.
- `cache/` is the verified cache of code from object storage.
- `processes/` holds each slot's workerd configuration and its copy of the runtime bridge, written again for every new generation.
- `sandbox/` (mode 0700) is the launcher's root, created on Linux when the sandbox is not `off` and the launcher is an executable file.

None of the cluster's state is in the stack directory: a node started on an empty one fetches what it serves from the store. A second node started on the same stack directory finds the first one's sockets live and refuses to start, changing nothing.

## 3. Deployment forms

**Compose.** One image, `coworkerd:<version>`, serves every role of Coworkerd; its entry point chooses the role by its first argument (`node`, `apiserver`, `secrets`, `init`, `tool`, `healthcheck`, `version`). Object storage runs from its own image, SeaweedFS 4.48 pinned by digest.

| Service | Role |
| --- | --- |
| `secrets` | Runs at every start, without a network, and exits: generates every credential of the cluster that does not exist yet into the `secrets` volume -- the object store's identities (`deploy`, and one per node), the management token and the admin credential (unless `COWORKERD_MANAGEMENT_TOKEN` or `COWORKERD_ADMIN_CREDENTIAL` is set: the services then take it from their environment), the cluster key and the secrets key -- and gives the store, the control plane and each node a directory of its own (the tool mounts the control plane's; `init`, like `secrets`, mounts the whole volume); `secrets add-node <id>` adds a node's identity to a running cluster |
| `storage` | An S3-compatible store (SeaweedFS): only its S3 gateway listens on the container's network, its other servers on the container's loopback; it applies an identity `secrets add-node` added within seconds, without a restart |
| `init` | Runs at every start and exits: creates the bucket and checks it -- the deploy identity writes, the first node's identity reads back, unsigned requests are refused, the store's contract and that identity's prefix policy are reported -- then writes the cluster's marker and empty indexes when there are none |
| `node-1`, and `node-2`, `node-3` in the profile `cluster` | Nodes |
| `apiserver` | The control plane |
| `tool` | The operations tool, in the profile `tools`, run on demand |

- **Two networks.** `backend` is isolated: compose gives it no gateway, so it has no route out and none in from the host; it carries object storage, the nodes, the control plane, `init` and `tool`; `secrets` has no network at all. `frontend` is an ordinary bridge carrying the nodes and the control plane: their published ports and the nodes' route out for tenant egress.
- **What each network reaches.** A node's peer listener binds a name that exists only on `backend` (`node-N-peer:8934`); its ingress listens on every address, so it is reachable on both networks.
- **Users.** The Coworkerd containers run as the user `coworkerd`, uid 10001, and object storage as 10001:10001.
- **Health checks and stopping.** A node is healthy while `/__coworkerd/ready` answers 200, so a draining node is not; the control plane's check expects a 401 from `/v1/status`, which shows that it answers. Nodes get 45 seconds to stop, for their 30-second drain; the control plane has Docker's default and exits on SIGTERM without a drain.
- **The sandbox.** Under Docker's default seccomp profile a process without `CAP_SYS_ADMIN` cannot create namespaces, so under the default compose file the nodes run without the sandbox, each saying so in a `[sandbox]` warning; `docker-compose.sandbox.yml` lifts the default seccomp and AppArmor profiles for the nodes, adds no capability and sets the sandbox mode to `launcher`, so that a node which cannot set it up refuses to start.

**Bare metal.** The node and the control plane run as services of their own with your own object storage -- the [installation guide](guide/install.md), section 4, gives a systemd unit for `coworkerd --config /etc/coworkerd/config.json` and runs the control plane as `coworkerd-apiserver --config /etc/coworkerd/control.json`, from the programs unpacked into `/opt/coworkerd`. Nothing else separates the channels there, so each credential in section 4, and each node's identity in the store, has to be configured by hand.

## 4. Channels and trust

| Channel | Between | Authentication |
| --- | --- | --- |
| Ingress, port 8930 | Clients and the platform's load balancer -> nodes | None for tenant traffic: the platform's own gateway is in front |
| Management endpoints, `/__coworkerd/` on the ingress port (the state, the logs and trial loads also on the peer port, and the control plane's announcements only there) | The control plane, the operations tool and operators -> nodes | The management token, except for `ready` and `probe` |
| Peer sessions, port 8934 | Node -> node: one Cap'n Proto session per peer, opened by an HTTP CONNECT | The CONNECT is signed with the cluster key |
| Peer HTTP, port 8934 | The control plane and the operations tool -> nodes, at their advertised addresses: state, logs, trial loads, and the control plane's announcements of its own addresses | The management token |
| `/v1`, port 8940 | The platform's own systems and `cowctl` -> the control plane | The admin credential or a tenant credential |
| Object storage | Nodes and the control plane with its tool -> the store | Signed requests: each node with an identity of its own that writes only its own prefixes, the control plane and its tool with the deploy identity |
| Unix sockets in `run/` and inherited descriptors | A node and its own workerd processes | Not reachable from outside the node |

- **Each credential is in force only when configured.** The cluster key needs `peerAuth.keyFile` and the management token has to be set; without them the channel is open, and the node warns at startup. Object storage always needs credentials, and every node and the control plane require the secrets key; the operations tool needs it only to set a secret's value. Compose sets up all of them.
- **Object storage credentials are the cluster's root.** Whoever holds the deploy identity can change everything the cluster runs; a node's identity can change only that node's own objects, but it reads the whole deployment state, code included. The secrets' values in the manifests are sealed, so reading the bucket does not reveal them without the secrets key.
- **Nothing is encrypted on these links.** Object storage is reached over plain `http://`, and the control plane calls the nodes over plain HTTP.
- **Tenant code reaches none of these channels itself.** Its outgoing requests leave through the node's egress service, which refuses the cluster's own addresses whatever a rule says -- those the node knows: its own interfaces and endpoints, the object storage endpoint, every member's advertised address, the addresses the control planes announce and `egress.clusterAddresses`, where compose lists the control plane -- and its Binding calls go back to its node, which places them. Any other address of a cluster machine, such as another node's `frontend` address under compose, is judged like any destination: a private one is refused unless `allowInternal` opens it.

[Egress and authentication](egress-and-auth.md) describes each credential.

## 5. Where configuration comes from

- **One configuration file format** serves both daemons, given with `--config FILE`: a JSON object decoded into `ServerConfig` of `common/schema/coworkerd.capnp`, whose field defaults are the defaults -- except that with the `storage` block `membership.backend` and `desiredState.backend` default to `objectStore`; unknown fields are refused. A node needs `nodeId`, the `storage` block and the secrets key (`secrets.keyFile`); the control plane needs the `storage` block with the deploy identity, the secrets key and an admin credential, and reads its own section `control`. Every daemon validates the whole file, so a malformed `control` section stops a node too.
- **Environment variables** override single fields, so secrets need not be written into the file: a non-empty `COWORKERD_MANAGEMENT_TOKEN`, `COWORKERD_ADMIN_CREDENTIAL`, `COWORKERD_SECRETS_KEY_FILE` or `COWORKERD_PEER_KEY_FILE` takes precedence over the file, and others set further fields -- the sandbox, the process pool, the drain, the load check, the memory watermark, the runtime file, the egress mode and default policy, and the Workers' log limits -- or settings the file does not have: the call lease and its subrequest budget, the log buffers and the limits on a package's assets ([installing and running](guide/install.md), section 5, lists them all). The node prints its configuration after applying them, so the printed configuration is the one in effect (it leaves out the log buffers and the asset limits, which the file does not have, and the secrets key file).
- **Credential files** are named by the configuration: the object storage identity, the cluster key and the secrets key. The cluster key file and the secrets key file must have no group or other permission bits; the object storage file is not checked.
- **The image's entry point** writes the configuration file from `COWORKERD_*` variables on every start, checking each value against a pattern first, and starts the role; with `COWORKERD_CONFIG` it uses that file as it is, so the variables only the entry point reads, such as `COWORKERD_NODE_ID` or `COWORKERD_PROCESS_COUNT`, then have no effect -- but the management token (and, for the control plane, the admin credential) from the secrets directory still reaches the daemon through its environment and takes precedence over the file.
- **The object store** holds what changes at run time: tenants with their quotas and log limits, credentials, deployments and secrets. A node reads its configuration file once at startup and follows the desired state by reading the store (every 5 s, [cluster state](cluster-state.md), section 4).
- **`cowctl`** reads `cowctl.json`, found in the current directory or a parent (tenant, control plane URL, ingress URL, credential file, Worker, and the route `cowctl init --route` gave), and takes its credential only from `COWCTL_TOKEN` or the credential file, never from the command line.

## 6. Boundary rules

1. **The writers of the desired state.** Only the control plane and the operations tool write tenants, credentials and deployments, with the deploy identity; a node's identity reads the deployment state and the tenants, not the credentials, and writes only its own prefixes `members/<nodeId>/` and `nodes/<nodeId>/`: its membership, its suspects and its usage. The store's policies enforce this. The operations tool of the same executable writes with the deploy identity directly, so it is not held to `/v1`'s authentication, tenant quotas or hostname checks at write time -- the nodes still serve a hostname registered to a tenant only from that tenant's routes -- and its `apply`, `upsert`, `remove` and `secret put`/`delete`, like its `init`, create the cluster's marker on a bucket that has none.
2. **The data plane only consumes state.** Nodes read the desired state from the store, and no node code calls the control plane. The control plane calls nodes only to read their state and logs and for trial loads, announces its own addresses to them, and reads usage from the store.
3. **The control plane keeps no state of its own.** It holds a mirror of the desired state read from the store, and every write is a revision created only if absent; a write that meets another writer's revision is read and computed again ([cluster state](cluster-state.md), section 8). Nothing in it elects a leader or coordinates instances: several control planes may share a bucket where the store's create-if-absent holds under concurrency, and compose runs one.
4. **The client knows only the API.** `cowctl` and the platform's back ends use `/v1` and nothing else -- only the platform's gateway or load balancer talks to the nodes, at their ingress; `/v1` changes only by adding fields and endpoints until 1.0.
5. **Every node is an ingress.** Any node takes any request and places it; the platform puts its own gateway or load balancer in front, and traffic splitting and edge features stay out of the core.

## 7. The repository

| Directory | Contents |
| --- | --- |
| `node/` | The node daemon: the ingress, placement and peers (`main.c++`), the process pool, the asynchronous logs, membership on the object store, following the desired state, the code cache, tenants, usage, suspects, the memory watermark, egress, peer authentication and the storage check |
| `apiserver/` | The control plane: `/v1`, tenants and credentials, the mirror of the desired state and its write protocol, the deployment tool, the rotation tool |
| `common/` | Shared by both: the object storage client and the state on it (revision sequences, the membership's objects, the tenant manifests and indexes, sealed secrets), the deployment state, code packages, tenancy, configuration, log cleaning and the log writer, JSON helpers and the Cap'n Proto schema |
| `launcher/` | `coworkerd-launcher` and the description of its protocol with the node |
| `cli/` | `cowctl` and the wrangler adapter |
| `runtime/` | The runtime bridge, and a static example workerd configuration the node does not use |
| `patches/` | The workerd patch series with its upstream tag, and the Cap'n Proto change the build applies |
| `docker/`, `examples/` | The image, the compose files and the example projects of the release package |
| `scripts/` | Build, release, patch, allowlist and compatibility-flag tools, and the Linux toolchain image |
| `docs/` | The user guide and these design documents |

- **Building.** CMake (3.24 or newer) builds the static library `common` and the executables `coworkerd` and `coworkerd-apiserver`, plus two libraries from Cap'n Proto's own sources: `capnp-http-over`, which `common` links, and `kj-tls-local`, KJ's TLS layer against the system OpenSSL, which only `coworkerd` links, for egress. On Linux it also builds `coworkerd-launcher`, which `coworkerd` depends on. `scripts/build.sh` builds the two daemons, and through that dependency the launcher.
- **Cap'n Proto** is not in the repository: CMake fetches it from GitHub at a pinned commit, which needs network access and Git, and applies `patches/kj-http-abort.diff` to it. `CMakeLists.txt` names the pin as aligned with workerd `v1.20260827.1`, not the `v1.20261012.1` the product ships.
- **workerd** is not built here: it comes from the fork's branch, upstream's tag plus the series, and the release takes a prebuilt binary and refuses one whose SHA-256 is not the one pinned for its architecture. [patches/README.md](../patches/README.md) describes the series and how it follows upstream.

## 8. The release package

Each architecture (`linux/amd64`, `linux/arm64`) has a package and an image.

- The package's `bin/` holds `coworkerd`, `coworkerd-apiserver`, `coworkerd-launcher`, `workerd`, the startup and steady-state system call tables of this workerd with `coworkerd-syscalls.json`, the record of how they were made, and the `cowctl` wrapper, which runs it with the `node` on `PATH`; the wrangler `cowctl` pins declares Node.js 22 or newer.
- `lib/cowctl/` holds `cowctl` and the adapter with their dependencies; `runtime/` holds `runtime.js`; `docker/`, `examples/`, the four guide pages in `docs/`, `INSTALL.md`, `VERSION` (the `--version` output) and `SHA256SUMS` of every file complete it. The tarball is reproducible: sorted entries, the source commit's time, no owners, normalised modes.
- The image is built from the unpacked package, compiles nothing, and holds the four executables and the two tables, without `cowctl`, which needs Node.js.
- A release record per architecture lists the version, the source, the workerd build with its patch series, the allowlist and the hashes of the package and the image.

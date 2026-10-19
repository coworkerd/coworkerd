# Components and boundaries

> Positioning: **Coworkerd is a Workers infrastructure layer for platforms.** One cluster runs all the untrusted Workers of one platform and provides strong isolation, per-tenant quotas and metering, a declarative API and a thin command line; accounts, billing, user interfaces and stateful services are provided by the platform above.
> Shaped like k8s: declarative state, multi-tenant primitives, a thin client. The core is not like k8s: placement per call, density counted in isolates, cold loads measured in milliseconds, no system of controllers.

## 1. Components

| Component | Form | Responsibilities | Not responsible for |
| --- | --- | --- | --- |
| `coworkerd` | The node daemon, one process per machine; its only subcommand is `check`: `check --config FILE` is the node-side self-check (etcd, object storage, the launcher's `--check`), `check storage ENDPOINT BUCKET` is the object storage check | The process pool and the lifecycle of workerd, CPU and memory limits, culprits and quarantine processes, per-Worker logs and quotas, process rotation, ingress and peer forwarding, placement per call, the management endpoints, per-tenant usage counting; reads the desired state, writes its own runtime state (membership, suspects, usage) | Does not write the desired state; is not accessed directly by the command line |
| `coworkerd-apiserver` | The control plane, a separate binary and process, stateless, may run as several instances | The /v1 API; credentials and tenants; admission, meaning validation and trial loads; the only writer of the desired state; uploads of code bundles and static assets; aggregation of logs, state and usage | Runs no tenant code; is not on the request path |
| `coworkerd-launcher` | A separate small C program | Sets up a least-privilege environment for each workerd and installs the system call allowlist, then hands over | Any business logic |
| Runtime | `workerd` (upstream + the patch series) + the bridge module `runtime/runtime.js` | Executes tenant code; the workerd side of limits and event reporting | -- |
| `cowctl` | A Node command line, including the wrangler adapter | The API's client: the developer loop (init, deploy, logs, rollback, secret) and administration (tenants, credentials, quotas) | Does not talk to etcd, storage or the nodes directly |
| etcd | External | Desired state, membership, suspects, runtime state | -- |
| S3-compatible object storage | External | Code bundles and static assets | -- |

**Why one binary per role**: the node and the API service are a consumer and a producer of etcd, with different placement and permissions; splitting them into two executable targets lets the linker guard the boundary (both depend only on `common` and cannot see each other, so crossing the boundary does not compile), the artifact's name is the role, and the node binary has no control-plane code it does not use. The cost is just one static library target in CMake. The launcher is separate because it has to be as small and static as possible and do only one thing before entering the sandbox.

**Naming**: the daemons keep full names (`coworkerd`, `coworkerd-apiserver`, `coworkerd-launcher`), like etcd and containerd, where the project name is the daemon's name; only the command line typed dozens of times a day is shortened to `cowctl`, the ctl suffix saying that it is a control tool rather than the product. Files and environment variables follow the program that reads them: the daemons read `COWORKERD_*` and `/etc/coworkerd/`; the command line's project configuration is `cowctl.json` and its token lives in `~/.cowctl/`; the image is `coworkerd:<version>`. A cow mascot is fine: friendly and harmless.

## 2. Boundary rules

1. **A single writer**: the desired state is written only by `apiserver`, enforced with etcd's role permissions: `apiserver`'s credentials may write the desired-state prefixes; `node`'s credentials may only read the desired state and write only its own runtime-state prefixes (membership, suspects, usage). The deployment tool's subcommands are part of `apiserver`.
2. **The data plane only consumes state**: `node` watches etcd and does not call `apiserver`; `apiserver` contacts `node` only to pull logs, state and usage (through the management endpoints and the token).
3. **The client knows only the API**: `cowctl` talks only to `apiserver`. The platform's back end likewise goes only through /v1, which only changes by adding fields until 1.0.
4. **Ingress stays in the node**: every node is an ingress, and the platform puts its own gateway or load balancer in front; complex traffic splitting does not go into the core. A separate ingress component can be split out later if needed.
5. **Trust between nodes needs credentials**: peer calls, the management endpoints, etcd and object storage are all authenticated; 0.3.0 signs with a cluster-wide shared key, while mTLS and a node join procedure go into 0.4.

## 3. Resource model and terminology

- Two levels: **tenant -> Worker**; versions, routes and secrets hang under the Worker. A Worker's full name is `tenant/Worker`.
- A tenant carries: optional tenant-level credentials; quotas (the number of Workers and versions, bundle size, isolate concurrency and total memory, log volume; a Worker's CPU and memory limits are the Worker's properties, with defaults from the tenant); ownership of routes and hostnames; the scope of suspect marking; usage counters (requests, CPU milliseconds, egress bytes, log lines); cascading deletion.
- Credentials have two scopes: administrator, and limited to one tenant. There is no "owner" level; the multi-platform case is simply one credential managing several tenants.
- Terminology: a machine is called a **node** and runs the node daemon; some documents call it a "Server". Its configuration key is `nodeId`; `request.cf.serverId` carries the same value as `nodeId` and remains through 0.3.x.

## 4. Source directories

```text
apiserver/   the API service: /v1 handling, credentials and tenants, admission, desired-state writes, uploads, aggregation
node/        the node daemon: process-pool, async-log, membership, placement, ingress and peer, management endpoints
common/      shared by both: etcd client, object storage client, deployment state model, capnp schema, log cleaning
launcher/    coworkerd-launcher
cli/         cowctl and its wrangler adapter
runtime/     the bridge module; patches/ the patch series; workerd/ the upstream checkout (a working directory, not in the repository)
docker/ docs/ examples/   the image and compose files, design documents and the user guide, example projects
scripts/     build, release, patches, allowlist
```

CMake targets: the static library `common`, the executables `coworkerd` and `coworkerd-apiserver`, and the C program `coworkerd-launcher`.

## 5. Release artifacts and the image

- The release package's `bin/`: `coworkerd`, `coworkerd-apiserver`, `coworkerd-launcher`, `workerd`, `cowctl` (the image has only the first four: `cowctl` needs Node.js, which the image does not have); `lib/` the command line's dependencies; the allowlist's three files (the startup table, the steady-state table, the generation record) are also in `bin/`, beside workerd (the table in section 1 of the install guide is authoritative); `runtime/`; `docker/`; `docs/`; `examples/`.
- One image; compose services correspond one to one to the components: `node-1..3`, `apiserver`, `etcd`, `storage`; the entry point script's mode names match them.
- Bare metal: two systemd units, `coworkerd --config` and `coworkerd-apiserver --config`.

## 6. Open, and later

- A separate ingress component; mTLS and a node join procedure; an extension point for the platform to implement KV-like bindings with its own services; a binding for "calling by name dynamically within a tenant" (we only make sure the API's wording does not rule it out).

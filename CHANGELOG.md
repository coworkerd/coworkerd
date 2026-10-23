# Changelog

## 0.3.1

Isolation and consistency fixes found while the design documents were checked against the code, in the tenant
boundaries and in the writes below a tenant. Upgrading is replacing the binaries and the image; nothing in etcd or
object storage changes. Two changes to be aware of before you upgrade: a path route under the default host `*` now
serves only the tenant that owns the request's hostname, and a path prefix matches whole path segments; and a node
refuses to start when its configuration names a CPU or memory limit of `0`.

- **A tenant's routes stay its own**: a request whose hostname belongs to a tenant is matched against that tenant's
  routes only -- its hostname routes first, then its paths under the default host -- so no other tenant's route can
  take a request to it. A path prefix matches at path-segment boundaries (`/acme` no longer takes `/acme-corp/...`),
  the control plane writes a tenant's `/<tenant>` under `*` as `/<tenant>/`, and a hostname is compared in lower case
  without a trailing dot.
- **A Worker's log lines are its own**: they reach the node over the tail channel workerd gives each loaded Worker
  under that Worker's own identity, so nothing tenant code can read or write decides whose lines they are. (Before, a
  per-process marker was part of every tenant isolate.)
- **With `egress.mode` `off`**, a tenant's `fetch()` is refused by the policy point (`EGRESS_DENIED: egress-off`)
  instead of leaving the isolate without an outbound, so a throw after `passThroughOnException` fails that request
  only, rather than ending the workerd process the tenants of that node share.
- **Writes below a tenant and its deletion**: every write compares the tenant's record as it read it, a pointer flip
  compares the version definition it names, and a tenant's deletion removes its routes and every key below its prefix
  in one conditional transaction -- a write that began before the deletion can no longer leave a route, a pointer or a
  Worker behind, and a node serves no tenant whose record is gone.
- **An etcd password change** reaches a running component: every authentication reads the credentials file again when
  it changed, and one that fails reads it again whatever its timestamp, so a watch that is the first to meet the new
  password recovers without a restart.
- **Egress policy**: `allowInternal` opens internal destinations only; a tenant's rules also apply to a URL that names
  an IP address (by the addresses a name resolves to), and an IPv6 form carrying an IPv4 address is judged by that
  address -- the local-use NAT64 prefix at every position RFC 6052 allows; the metadata class covers the other clouds'
  IPv6 addresses and the IPv6 documentation range is no longer public; the `[egress]` log line's `host` field holds a
  host name only; and every control plane announces its own addresses to every node, which refuse them like the rest
  of the cluster's own (`egress.clusterAddresses` remains for what nobody announces).
- **Packages**: a manifest's `packagePath` must be a relative path inside the package, and the asset quotas are read
  from the package file it names.
- **`cowctl`**: a project's local value files (`.env`, `.dev.vars` and their variants, at any depth) are never
  published as static assets.
- **workerd's environment**: a workerd process is given the variables it needs and nothing else of the node's own
  environment -- the management token and object storage keys stay with the node.
- **A limit of `0`** is refused in a node's configuration, in a tenant's default limits and in a Worker's own (it
  turned culprit attribution, and with it the quarantine processes, off). `COWORKERD_CPU_LIMIT_MS` and
  `COWORKERD_MEMORY_LIMIT_MIB` still turn a limit off deliberately, and the node says so at startup.
- **compose**: the object store answers on its S3 interface only; its master, volume and filer servers listen on the
  container's loopback, and the S3 gateway's own gRPC interface requires a client certificate nobody holds.
- **Membership**: a member value a node cannot read is skipped and logged, and the other members -- and a node's
  startup -- are unaffected.

## 0.3.0

The first release, for Linux `amd64` and `arm64`, each as a package and a container image. One cluster serves one
platform, which runs its tenants' Workers on it: create tenants, issue credentials, deploy, call, read logs and usage;
nodes and backends authenticate each other; every workerd runs in a least-privilege environment with a system call
allowlist.

**Components** (`bin/`): `coworkerd` (the node daemon; `coworkerd check` is its self-check), `coworkerd-apiserver` (the
control plane, the only writer of desired state), `coworkerd-launcher` (starts each workerd in a least-privilege
environment), `cowctl` (the command line; needs Node.js 22 or newer for the bundled wrangler), `workerd` (upstream `v1.20261012.1` with this
project's patch series, see [coworkerd/workerd](https://github.com/coworkerd/workerd)), and the system call allowlists
of this workerd build (startup and steady-state tables).

**Capabilities**

- **Isolation and placement**: a pool of workerd processes per node with a watchdog, per-isolate CPU and memory
  limits, culprit identification and quarantine processes; any node is an ingress, calls (including `env.B` Service
  Bindings) are placed on a Worker's home node by rendezvous hashing and rerouted only when they certainly did not
  execute; one call tree always sees one deployment state.
- **Tenant → Worker model** and the **`/v1` API**: the administrator's credential creates tenants, sets quotas and
  hostnames and issues tenant credentials; tenants deploy, roll back, set secrets and routes, read logs and usage with
  their own credential. `/v1` changes only by adding fields and endpoints, until 1.0.
- **`cowctl`**: developer commands (`init`, `deploy`, `versions`, `rollback`, `remove`, `secret`, `status`, `logs`,
  `usage`) and administrator commands (`tenant`, `credential`, `node`).
- **Per-tenant usage**: hourly requests, CPU milliseconds, egress bytes and log lines; a draining node flushes its
  last minute.
- **Egress policy**: tenant `fetch()` leaves through a policy point on the node, which refuses loopback, private,
  cloud-metadata and cluster addresses by default, checks the resolved address and pins the resolution; per-tenant
  allow and deny lists, concurrency and byte limits, byte metering.
- **Authentication**: peer calls between nodes are signed with a cluster key (two-key rotation); etcd requires client
  authentication, with separate users and roles for the apiserver and each node (nodes cannot read credentials or
  write desired state); nodes hold read-only object-storage credentials; the management endpoint takes a token.
- **Sandbox**: each workerd runs under the launcher in its own mount, network, IPC, UTS and user namespaces as an
  unprivileged user, and exits with its parent; the launcher installs the system call allowlist generated by the build
  (default `observe`: calls outside the table are reported and allowed, visible in the state endpoint and the logs);
  after readiness workerd installs the narrower steady-state table itself (default `off`) and makes itself
  non-dumpable. Prerequisites are in the install guide, section 4.
- **Per-Worker CPU and memory limits** taken from the Worker's configuration, bounded by the tenant's quota;
  `request.cf.nodeId` (`request.cf.serverId` carries the same value through 0.3.x).
- **Node side**: trial loads run in the quarantine processes (two by default, sharded by tenant); workerd processes are
  rotated hourly by default.

**Getting started**: unpack and read `docs/quickstart.md` (about ten minutes); installation, configuration and
operations are in `docs/install.md`. Requires Docker Engine 28 or newer and Node.js 22 or newer; the sandbox requires
Linux with unprivileged user namespaces (Ubuntu 23.10 and later: see the install guide, section 4).

**Known limitations** (planned for 0.4): nodes trust each other through a shared signing key -- no mTLS and no
node-join flow yet; Worker secrets are stored unencrypted in etcd (access is authenticated and role-restricted); the
control plane is a single process; the allowlist defaults to `observe`, and switching to `enforce` should follow a
period of observation under real traffic; `cowctl logs --follow` polls; some error reasons the node returns are in
Chinese, with English reasons and codes planned.

# Changelog

## 0.5.0 (unreleased)

Inbound WebSocket connections for stateless Workers. A Worker that answers an upgrade with `101` and a WebSocket now gets
its connection: the node that received the upgrade relays text and binary messages and close frames, with their code and
reason, both ways -- also when the Worker runs on another node, through the signed peer session, which the connection
keeps for its life. Outgoing WebSockets (a Worker's `fetch()` upgrade, through the egress service) do not change.

- **A connection is one request, and its messages are the request's events**: each message the Worker receives starts
  with the request's CPU topped up -- workerd's patch 0012; upstream does that for Durable Objects only -- and renews
  the call tree's lease as a new root's (the deadline to the lease time from then, the budget of subrequests to the
  root's), so a connection's handlers are limited and leased message by message however long it lasts: a message's
  Service Binding calls and theirs share the root's budget (four calls by default), as a request's do. A message over
  the CPU or memory limit closes the connection with `1011` and `CPU_LIMIT_EXCEEDED` or `MEMORY_LIMIT_EXCEEDED`, the
  codes an HTTP answer carries, and the isolate is replaced as for a request. A message is at most 32 MiB (workerd's
  default); there is no idle timeout and no compression.
- **Admission and a limit per tenant**: an upgrade's admission slot is given back at its `101` -- between two messages a
  connection is not executing --, and each node holds at most `ingress.webSocketsPerTenant` (1000) open connections of
  one tenant: an upgrade beyond them gets `429 WEBSOCKET_LIMIT` before the Worker runs, and other requests are not
  affected. The state endpoint's `webSockets` counts the open connections per tenant, and each relay of a connection
  writes a `[websocket]` line when it opens and one when it closes (tenant, Worker, which side closed, the code, the
  cause).
- **Draining and process rotation close connections with `1001`**: a draining node closes its connections at once --
  those it runs for another node's ingress through that ingress -- and its drain record counts them; a process
  generation being replaced closes its connections at the rotation's switch, and the rotation's record counts them. A
  client whose ingress lost the node its connection ran on sees an abnormal closure (`1006`).
- **Usage and logs**: a connection counts as one request; its CPU is metered while its messages run (workerd reports a
  lasting request's CPU at most once a second), not only when it closes; the lines its handlers write reach the tenant's
  logs only when it closes, as a call's do when it ends -- a known limitation.
- **Configuration**: `ingress.webSocketsPerTenant` (1000; `0`: no limit).

## 0.4.1

A full garbage collection is no longer taken for a CPU overrun, and processes are no longer replaced every hour. V8
collects an isolate on the thread that runs its requests, and a long-lived isolate's full collection -- mostly the
destruction of the wrappers of objects it let go, in one atomic pause -- was charged to whichever request it ran in: on
a busy node it terminated well-behaved requests with `CPU_LIMIT_EXCEEDED`. With a tree of Workers at 15 requests a
second per root, four processes and no rotation, 0.4.0 terminated 50 requests in an hour on an amd64 machine, each right
after a full collection; 0.4.1, beside it on the same machine, none. A node also no longer expires, and takes a new
epoch, because one store write of its heartbeat was answered late: the heartbeats keep their cadence, and a node survives
two failed ones in a row. The object store's layout and `/v1` do not change; the node's configuration gains `gc`, and two
of rotation's defaults change.

- **Collections are left out of the CPU limit, within a budget**: the thread CPU of a full collection that runs during a
  request -- also while it is under way -- is subtracted from the request's CPU as far as its Worker's budget goes, a
  token bucket of `gc.budgetMs` (2,000 ms) refilled at `gc.refillMsPerSecond` (100 ms a second), one per Worker in each
  process, which the Worker's next isolate takes over; beyond it, collections are charged as before. A tenant that
  allocates on purpose so gets at most a tenth of a core of uncharged collections (87 ms a second measured over 45 s of
  back-to-back collections), and a request that only runs JavaScript is terminated at the limit as before. The rows of
  completed requests carry the CPU left out (`gcExcludedNs`).
- **Long pauses are named**: a collection that pauses its process for `gc.longPauseMs` (100 ms) or more writes a
  `gc-pause` row -- the isolate, the pause, the request it ran in or none, the kind of collection, the CPU left out and
  the budget left -- to `logs/workerd.log` after `[cpu-limit] ` and to its node, whose state endpoint keeps the latest
  rows (`processes.recentGcPauses`) and counts collections, long pauses and the longest per process
  (`processes.entries[].gc`).
- **Idle isolates are collected between requests**: a Worker's isolate idle for `gc.idleCollectAfterMs` (50 ms) whose
  old generation grew `gc.idleCollectMinGrowthMiB` (4 MiB) since its last full collection is collected outside any
  request, never while its process has work waiting; and every isolate starts with an 8 MiB old generation
  (`gc.initialOldSpaceMiB`, V8's `--initial-old-space-size`), so its first full collection comes while it holds little.
  Measured on an arm64 machine, every first full collection of the tree's isolates was such an idle one and paused its
  process less than 21 ms.
- **The runtime's heap is bounded**: the runtime bridge's isolate, which every call passes through, gets a heap of
  `gc.runtimeHeapMiB` (128 MiB) instead of one V8 sizes from the machine (four times the most it was measured to hold
  after a full collection, 7.7 MiB under load, and at least 128 MiB). Its own full collections, outside any request, are
  now the longest pauses left, and are named like the others (isolate `runtime`).
- **Rotation on demand**: `rotation.maxAgeMs` defaults to `0`, so a process is no longer replaced every hour, and
  `rotation.residentBytes` left out of the configuration is 70% of the node's memory (its container's `memory.max`, else
  `memoryBudgetBytes`, else the machine's; the state endpoint names which): a generation is replaced when its workerd
  file changes, by hand, or at that threshold. With the collections above a process's memory stays level -- in the same
  hour without rotation the node's processes together rose to 891 MiB in the first 40 minutes and to 894 MiB by the end
  (0.4.0's, beside them, to 3,205 MiB) -- and an hourly rotation only rebuilt every isolate, its cold loads and its
  first, largest collection, and cut the requests longer than the drain. Set `rotation.maxAgeMs` to keep replacing
  processes by age.
- **An actor's CPU limit is topped up with each event** (`topUpActor()`, workerd's own contract): the requests to one
  Durable Object shared one CPU limit for the object's whole life. Coworkerd runs no Durable Objects before 0.5; this is
  one of their prerequisites.
- **The system call allowlists** are generated anew for the patched workerd: the same calls as 0.4.0's on both
  architectures (arm64: 59 at startup, 43 in the steady state; amd64: 63 and 42), only the binary they name is new.
- **One slow heartbeat no longer expires a node**: the membership's heartbeats go out on a fixed cadence, one every
  `membership.heartbeatIntervalMs` from the epoch's first -- at startup while the node still lists the members --,
  whether the one before has finished or not, each still bounded by the interval. In 0.4.0 a heartbeat waited an
  interval after the previous one had finished, so one that hung to its bound sent the next exactly at the TTL after the
  last one the store took: a single store write answered late (7 s, say) expired the node and gave it a new epoch,
  although no stateless call failed. Now one failed heartbeat leaves a full interval to spare; after two in a row the
  third goes out at the deadline, and a node waits for its answer before it expires -- a third failure expires it one
  interval past the TTL, still before the other nodes drop it (the cluster's state page, section 3). A node registering a
  new epoch after its own expired no longer takes a heartbeat of its own that lands late, while it waits for its id, for
  another process's, which drained the node and ended it with exit code 3. The TTL, the interval and the grace keep
  their defaults.

## 0.4.0

The object store becomes the cluster's only state, and etcd leaves Coworkerd: membership, the desired state (tenants,
credentials, deployments, secrets), the suspects and the usage are all in one bucket of an S3-compatible store, beside
the code that was there already. The control plane keeps nothing of its own, and a node only its Workers' recent log
lines and the usage counts it has not written yet (a minute's worth while the store takes them), so a stateless node and
a bucket make a cluster. **This release does
not upgrade a 0.3.x cluster in place**: nothing is migrated -- build the cluster anew and deploy again (the install
guide, section 9, "Upgrading from 0.3.x"); a configuration that names `etcdEndpoint`, `etcdCredentialsFile` or an
`etcd` backend is refused, saying why.

- **Membership on the object store**: every node registers an epoch it creates only if absent (and reads back), writes
  a heartbeat every 5 s and lists the members every 10 s; liveness is judged by the store's own clock (`LastModified`
  against the answer's `Date`), and a node that cannot write its heartbeat for the TTL considers its epoch expired. A
  node joined the others' tables within 6.6 s, and one that was killed dropped out of them 28 to 33 s later. A node
  refuses to start when its clock is more than 2 s from the store's, and a node whose identity another process takes
  drains and exits with 3.
- **Store outages fail no stateless call whose code and static assets its node holds**: a node whose epoch expired
  keeps serving the stateless calls addressed to any epoch it held, and a node that could not list the store for
  longer than the TTL keeps its members, whose heartbeats are still there, until the TTL and the grace have passed
  after it recovers. Through a 40 s outage of one node's store connection, 3,330 requests at the ingress succeeded.
- **The desired state on the object store**: one immutable manifest per tenant, the tenant index and the credentials
  index, each change the next revision created only if absent; the control plane holds a mirror of all of it and
  writes it -- as does the operations tool, with the same protocol -- and several control planes can share a bucket.
  Nodes read the index's hint every 5 s and apply what changed: a deployment reached three nodes in 3.8 to 4.7 s (in
  0.3, etcd pushed it within milliseconds), and ten concurrent deployments to one tenant through two control planes
  all committed in 305 ms, none lost. A tenant's whole state is one write: there is no limit of 128 operations any
  more. A change is answered as done only once it is visible -- its revision, the hint and the tenant index naming it;
  a failure after the revision is `503 STORAGE_UNAVAILABLE` -- and the control plane's poll and start complete what a
  writer left half done. The hostnames of different tenants are checked once more in the tenant index's own turn.
- **Secrets sealed at rest**: values are sealed with AES-256-GCM under the cluster's secrets key (`secrets.keyFile`),
  which compose's `secrets` step generates for the control plane and every node; the store never holds a value in the
  clear.
- **An object storage identity for every node**: each node's identity writes only its own `members/<nodeId>/` and
  `nodes/<nodeId>/` and cannot read the credentials index; the control plane and its tool keep the deploy identity.
  `docker compose run --rm secrets add-node <nodeId>` issues a node's identity and its secrets directory to a running
  cluster, and the node is then started with that directory (compose defines `node-1` to `node-3`; the install guide
  gives a file for a fourth): the store took the new identity 1.3 s later without a restart, and the fourth node was a
  member 5.6 s after `add-node`.
- **Suspects and usage on the object store**: a node's suspects are one object beside its heartbeat, read by the other
  nodes in their membership listing when it changed -- adopted within one listing cycle (10 s by default; 6.0 s
  measured), one GET per change and none in between -- and its usage one object per hour
  (`nodes/<nodeId>/usage/<hour>`), summed by the control plane.
- **SeaweedFS 4.48** in the compose files (pinned by digest): before 4.29 its create-if-absent does not hold under
  concurrency. Reads are presigned the standard way (only the host signed), which 4.48 requires -- the 0.3.1 binaries
  cannot read from it, so the pin and this release's binaries go together.
- **Checks**: `coworkerd check storage` reports the contract the cluster relies on (create-if-absent, listings with
  `Date` and `LastModified`, the clock), `conditionalPutAtomic` (create-if-absent under concurrency), whether the bucket
  holds the cluster's marker, and with `COWORKERD_READ_NODE_ID` a node identity's prefix policy;
  `coworkerd check --config` checks the management token, the desired state (the secrets key, the marker) and the
  membership (a listing, the clock, a write below the node's own prefix).
- **The cluster's marker**: `coworkerd-apiserver tool init` writes `cluster/format/1` and empty indexes into a new
  bucket (compose's `init` runs it); nodes and the control plane refuse to start on a bucket without it, and the tool's
  other commands refuse to write to one.
- **`/v1`**: `STORAGE_UNAVAILABLE` (503) answers a change while the store cannot be reached, and the usage endpoints;
  `ETCD_UNAVAILABLE` is not answered any more. The status answer gains `storage` (the revisions of the tenant index and
  the credentials index, and the store client's counters), `/v1/nodes` gains `backend` and `lastHeartbeatAgeMs` per
  node, and `etcdRevision` and the administrator view's `apiserver.etcdEndpoint` stay, always `null`. `cowctl status`
  shows the two revisions.
- **compose**: the services are `secrets`, `storage`, `init`, `node-1` to `node-3`, `apiserver` and `tool`; there is no
  etcd service, volume or credential.
- **The object store client**: separate connect (2 s), read (5 s) and create-if-absent (30 s) timeouts, a second try on
  a new connection for idempotent requests that failed, and a create whose answer was lost decided by reading the key
  back.
- **Deletions**: a tenant's deletion answers `503 DELETION_INCOMPLETE` until nothing of the tenant remains, and goes on
  when run again; deleting a version removes its objects first and leaves the version in place (503) when one cannot
  be removed. A deletion removes an object only once it is older than `control.cleanupGraceMs` (1 hour) by the store's
  clock and leaves younger ones to a sweep of the control plane, so an upload that meets a cleanup keeps its objects.
- **A management token is required**: a node refuses to start without `management.token` (or
  `COWORKERD_MANAGEMENT_TOKEN`) unless its configuration says `management.allowWithoutToken: true` (development); compose
  always sets one. 0.3 started without one and only warned.
- **`cowctl` waits for the nodes**: `cowctl deploy`, `rollback` and `secret put` return once every node that is not
  draining reports the change (`storage.appliedTenantsRevision`, new in `/v1/nodes` and the status answer), at most
  15 s; `--no-wait` returns at once. `cowctl` needs Node.js 22.5 or newer.
- **`secrets remove-node`** withdraws a node's object storage identity within about 2 s, without a restart of the store:
  `s3.json` keeps only the deploy identity, and every node's identity is in the store's filer.
- **Answers**: `SECRET_TOO_LARGE` is retired -- a secret value over 5 KiB is `422 SECRET_REFUSED` whatever its size --;
  a package of exactly `control.maxUploadBytes` or `packageBytes` is accepted; an upload without activation is not
  trial-loaded (its activation is) and lists no routes (`400 INVALID_REQUEST`); a store failure is 503 and a write other
  writers kept winning 409 by the failure's kind, never by its reason's words; the ingress's own refusals carry `error`
  like every call refusal; a call whose lease runs out during placement ends with `410 LEASE_EXPIRED`.
  `request.cf.serverId` stays a deprecated alias of `nodeId`, and is removed in 1.0.
- **Supported versions**: 0.4.x only; 0.3.x is no longer fixed (SECURITY.md).

**Still open**: there is no mTLS between nodes; the system call allowlist still defaults to `observe`; the stream of
`cowctl logs --follow` is filled by asking the nodes every 500 ms; some reasons the nodes and the control plane give
are still in Chinese; and an isolate already loaded keeps a secret's old value until it is replaced: an idle one after
5 minutes, one in use at the latest when its process is rotated, every hour by default.

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

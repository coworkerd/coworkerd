# Control plane API

This document is for platform engineers who integrate with Coworkerd and for contributors to the control plane: the resource model, the `/v1` endpoints, credentials and scopes, quotas and limits and where each is enforced, what uploading, activating, rolling back and deleting mean, routes and hostnames, how secrets are stored, how usage and logs are gathered, and what the version promise is. The error codes are listed in [error codes](guide/errors.md), section 2; the command line that uses this API is in the [quickstart](guide/quickstart.md).

## 1. Resources

One cluster serves one platform, and the platform is its only administrator. Resources have two levels, **tenant -> Worker**; versions, routes and secrets belong to a Worker, and a Worker's full name is `tenant/Worker`.

| Resource | Identifier | Holds | Changed by |
| --- | --- | --- | --- |
| Tenant | 1 to 63 lowercase letters, digits and `-`, starting with a letter or digit, neither `routes` nor `generation`, unique in the cluster | State (`active`, `deleting`), quotas, hostnames, default Worker limits | The administrator |
| Tenant credential | `keyId`, 16 hexadecimal characters | Its scope, one tenant; an optional expiry; whether it is revoked | Created by the administrator; revoked by the administrator, or by the tenant itself |
| Worker | `<tenant>/<worker>`, a name of the same characters and length as a tenant's | Versions, the active version, routes, secrets, the Worker's CPU and memory limits | The tenant and the administrator |
| Version | `<tenant>/<worker>/<version>` | The package in object storage and the definition (compatibility date and flags, variables, Bindings, declared secret names), identified by the definition's hash | Uploaded and deleted, never changed |
| Route | Its host and path prefix | The Worker it points to | The tenant and the administrator |
| Secret | `<tenant>/<worker>/<name>` | A value | The tenant and the administrator |

There is no level above the tenant: a platform that serves other platforms gives each of them tenants of its own. A Worker's Service Bindings may point only to Workers of the same tenant.

## 2. The `/v1` API

Every request carries `Authorization: Bearer <credential>`. Answers are JSON, apart from the log stream (section 10); an error is `{"code","reason"}` with a cleaned reason.

| Method and path | Scope | Purpose |
| --- | --- | --- |
| `GET /v1/status` | Any credential | The answering control plane, the caller, and every node, reduced for tenants |
| `GET /v1/nodes`, `GET /v1/nodes/{node}` | Administrator | Each node's state: processes, sandbox, rotation, suspects, usage |
| `GET /v1/usage` | Administrator | Hourly usage of every tenant |
| `POST /v1/tenants`, `GET /v1/tenants` | Administrator | Create, list tenants |
| `GET /v1/tenants/{t}` | The tenant or the administrator | The tenant's record, its counts and its usage of the last 24 hours |
| `PATCH /v1/tenants/{t}`, `DELETE /v1/tenants/{t}` | Administrator | Change hostnames, quotas and default limits; delete the tenant with everything it owns |
| `POST /v1/tenants/{t}/credentials`, `GET /v1/tenants/{t}/credentials` | Administrator | Issue, list the tenant's credentials |
| `DELETE /v1/tenants/{t}/credentials/{keyId}` | The tenant or the administrator | Revoke a credential |
| `GET /v1/tenants/{t}/usage` | The tenant or the administrator | The tenant's hourly usage |
| `GET /v1/tenants/{t}/workers`, `GET .../workers/{w}` | The tenant or the administrator | The Workers: versions, active version, validity, limits, routes, secret names |
| `PATCH /v1/tenants/{t}/workers/{w}` | The tenant or the administrator | The Worker's CPU and memory limits, within the tenant's maxima |
| `DELETE /v1/tenants/{t}/workers/{w}` | The tenant or the administrator | Remove the Worker: its active version and routes go, its versions stay |
| `PUT .../workers/{w}/versions/{v}` | The tenant or the administrator | Upload a package and trial-load it; `?activate=true` also makes it active |
| `POST .../workers/{w}/versions/{v}:activate` | The tenant or the administrator | Make an uploaded version active; a rollback is the same |
| `DELETE .../workers/{w}/versions/{v}` | The tenant or the administrator | Delete an inactive version |
| `PUT .../workers/{w}/routes` | The tenant or the administrator | Replace the Worker's routes |
| `GET .../workers/{w}/secrets`, `GET`, `PUT`, `DELETE .../secrets/{name}` | The tenant or the administrator | List names and lengths; show one; set, delete a value |
| `GET .../workers/{w}/logs` | The tenant or the administrator | The Worker's recent log lines from every node, or a stream with `follow` |

Below `/v1/tenants/{t}/`, a tenant that is being deleted accepts only reads.

**The control plane keeps no state of its own**: everything it answers is read from etcd, object storage and the nodes when it is asked, so it can be stopped at any time -- an interrupted deployment leaves at most published immutable parts, never a half-flipped one. It is not on the request path: a cluster serves calls while the control plane is down, and only changes wait.

## 3. Credentials and scopes

- **The admin credential** is the platform's own: configured for the control plane (`control.adminCredential`, or `COWORKERD_ADMIN_CREDENTIAL`), 16 to 4096 visible ASCII characters without spaces; compose's `secrets` step generates one. It is not stored in etcd and has no expiry or revocation: changing it means configuring a new one and restarting the control plane. It can do everything and is never given to tenants.
- **Tenant credentials** are issued through `/v1` for one tenant. The text is `cwk_<keyId>_<secret>`, shown only in the answer that creates it; etcd keeps only the SHA-256 of the secret part. A credential can carry an expiry, can be revoked (the record stays, marked revoked), and acts only on its own tenant.
- Every request reads the credential's record, so a revocation takes effect at once, and secrets are compared in constant time.
- `401 UNAUTHORIZED` answers a missing, malformed, unknown, revoked or expired credential, always with the same reason; only the control plane's log line says which. `403 FORBIDDEN` answers a credential whose scope is not enough: another tenant, or an administrator's endpoint.
- `cowctl` reads a credential only from `COWCTL_TOKEN` or the credential file its `cowctl.json` names (default `~/.cowctl/token`), never from its command line, where other users could see it.

## 4. One writer

The control plane is the only writer of the desired state -- `tenants/`, `credentials/` and `deploy/` under `/coworkerd/v2/` in etcd -- and etcd's roles enforce it: the nodes can read the deployment state and the tenants but not the credentials, and they write only their own keys. Every write but a tenant's deletion goes through etcd transactions that commit only if the keys they compare are as the write read them -- among them every key it writes except the deployment generation and the Binding index, and for an activated version the creation of its secrets and Binding targets --, so concurrent requests on Workers that share no route or Binding do not conflict; a request whose keys keep changing is tried up to 9 times, with jittered pauses, before it gives up with `409 CONFLICT` (a secret's write whose failure reason would contain the value answers `422 SECRET_REFUSED`, the reason withheld). A tenant's deletion is the exception: once the tenant is marked `deleting`, its routes, pointers, deployment keys and credentials are removed without such conditions, and only its record on the condition that it did not change (section 6). What a write reads but does not compare can change before it commits. A version deleted while it is being activated can leave the Worker naming a definition that is gone, and invalid until that changes -- for example when the same version is uploaded again with the same content, or another version is activated -- or the Worker is removed. A deployment that passed the tenant's status check before a deletion marked the tenant, and commits after the deletion read the tenant's keys, can leave its route behind -- routes are not below the tenant's prefix --, owned by a Worker of the deleted tenant, so that no other tenant's deployment can take it over; if it commits after the deletion's range deletion, its pointer stays as well, and a tenant created again under the same name starts with that Worker. Other writes below the tenant that passed the check can land after the range deletion the same way -- a version's definition, a secret's value --, so the Worker left behind can even be valid; nodes refuse only a tenant whose record says `deleting`, so once the record is gone such a Worker keeps serving its route, and `/v1` reaches it again only through a tenant created again under that name, which starts with these keys. The object cleanup of a version's or a tenant's deletion reads the remaining definitions after its own transaction, so an upload -- of any tenant -- that found shared objects present and skipped them, but publishes its definition only after that read, can be left with a definition whose objects are gone. [Cluster state](cluster-state.md) describes the layout, the roles and the write protocol.

## 5. Quotas and limits

| Quota of a tenant | Default | Enforced |
| --- | --- | --- |
| `workers`, `versionsPerWorker` | 100, 50 | By the control plane when a version of a new Worker, or a new version id, is uploaded |
| `packageBytes` | 160 MiB | By the control plane, on the size of the uploaded package, which `control.maxUploadBytes` (also 160 MiB) caps for every tenant |
| `assetFiles`, `assetBytes` | 20,000, 100 MiB | By the control plane on upload |
| `secrets`, `hostnames` | 1,000, 100 | By the control plane when a new secret is set or the hostnames change |
| `logLinesPerSecond`, `logBufferBytes` | 100, 1 MiB | By every node, for the tenant's log lines |
| `maxWorkerCpuMs`, `maxWorkerMemoryMiB` | 30,000, 512 | By the control plane when a Worker's limits or the tenant's default limits are set |

A change over a quota is refused with `422 QUOTA_EXCEEDED` and the quota's name at the start of the reason (`defaultLimits:` for default limits above the maxima), and nothing is written; a package over the limit is `413 PACKAGE_TOO_LARGE` instead unless the tenant's `packageBytes` is below `control.maxUploadBytes` -- so at the defaults, both 160 MiB, it is 413. The control plane checks these quotas when it admits a change; the log quotas refuse nothing: every node enforces them on the tenant's lines, dropping lines over the rate and the oldest lines over the buffer -- except that a node keeps the tenant's previous values when the buffer quota is larger than its own `logs.totalBufferBytes`. An upload's and a secret's quotas are checked against one read before the write, outside its transaction, so concurrent requests can together take a tenant over `workers`, `versionsPerWorker` or `secrets`. `workers` counts every Worker the tenant's state names, a removed one whose versions remain included; lowering a maximum leaves Workers' own limits above it as they are, and is refused below the tenant's default limits.

**Worker limits.** A Worker's CPU milliseconds per request and memory MiB per isolate are properties of the Worker (`PATCH .../workers/{w}` with `{"limits":{"cpuMs","memoryMiB"}}`, which replaces both), at most the tenant's maxima. They are stored with the Worker's active version, survive activations and new versions, and go when the Worker is removed. A Worker without its own uses the tenant's default limits, then the node's configuration; nodes apply them per isolate.

**Not limited per tenant**: isolate concurrency and total memory across the cluster, and requests per second at the ingress. Both would need coordination across nodes.

## 6. Versions and deployments

- **Upload** (`PUT .../versions/{v}`): the package is a tar file of a manifest and the version's files. The control plane validates it (format, names, sizes, compatibility date and flags, quotas), stores its modules and assets in object storage by hash, and has a node **trial-load** the version in a quarantine process before the definition is written: a version exists only if it loads (one with only static assets has nothing to load and is not tried). A failed trial load answers `422 LOAD_FAILED` with a category; when no node can try, the answer is `503 TRIAL_UNAVAILABLE`.
- **Versions are immutable.** A version id that already has a different definition is refused (`409 VERSION_EXISTS`); uploading the same content again changes nothing. `cowctl deploy` names versions `v1`, `v2`, ... by itself and takes the next number on a conflict.
- **Upload and activate** (`?activate=true`) is checked before anything is published: the Worker must be valid afterwards -- every declared secret has a value, every Binding target is valid and has Worker code, the closure has at most 64 Workers -- and no other valid Worker may become invalid; otherwise `422 DEPLOYMENT_REFUSED` -- the answer's code is chosen by the words of the refusal's reason, so a reason that mentions `etcd`, in a Worker's name for example, is answered `503 ETCD_UNAVAILABLE` instead. An upload without it publishes the version after its trial load, if it has code, and checks none of this -- but a version with code whose declared secrets have no values cannot be trial-loaded, so its upload is refused with `503 TRIAL_UNAVAILABLE`.
- **Activate** (`:activate`) moves the active pointer to a version uploaded before and keeps the Worker's routes; it is validated like an activating upload and trial-loads the version again unless it is already active or has only static assets. A rollback is activating an older version.
- **Delete a version**: only an inactive one. Its definition goes, then the objects in object storage that no remaining definition of any tenant references -- as far as it goes: a failed object deletion is counted in the answer and not tried again, modules stay when more than 1,000 packages remain, and objects left by an interrupted request are not collected later.
- **Remove a Worker** (`DELETE .../workers/{w}`): its active pointer and routes go and nobody can call it; its versions and secrets stay. It is refused while another valid Worker binds it.
- **Delete a tenant**: the tenant is first marked `deleting`, so every node refuses new requests to its Workers (`TENANT_UNAVAILABLE`); calls of trees already running are not refused for the deletion, but can fail once their secrets or code are gone; then its routes and active pointers, the objects only it uses -- as far as their deletion succeeds, as for a version --, its definitions, secrets and credentials are deleted, and finally its record. A failure on the way leaves it `deleting`; deleting again continues where it stopped, and the progress is in `GET /v1/tenants/{t}`.
- **Retention** is the platform's: there is no automatic cleanup of old versions.

## 7. Routes and hostnames

A route maps a host and a path prefix to a Worker. `PUT .../routes` replaces a Worker's routes as a whole (at most 64); an activating upload whose manifest lists routes replaces them too, one that lists none keeps them, and `cowctl deploy` always lists one. A tenant may route:

- **hostnames registered to it** by the administrator; a hostname belongs to one tenant only, and the tenant's list is replaced as a whole when it is changed;
- **paths under the cluster's default host `*`** that are `/<tenant>` or start with `/<tenant>/` -- `cowctl deploy`'s default route is `*` with `/<tenant>/<worker>` -- unless the administrator turns path routing off (`control.pathRouting`).

A request whose hostname is registered to a tenant is matched against that tenant's routes only -- its routes on that hostname first, then its paths under `*` -- and a path route under `*` serves only requests whose hostname belongs to no tenant ([cluster state](cluster-state.md), section 4). Prefixes match whole path segments, and the control plane writes a tenant's `/<tenant>` under `*` as `/<tenant>/`. Anything else is `403 HOSTNAME_NOT_OWNED`. A route that belongs to another Worker is refused rather than taken over, and a hostname still routed by one of the tenant's Workers cannot be removed from it. How a request finds its route is in [cluster state](cluster-state.md), section 4.

## 8. Secrets

- **Storage.** A secret's value is stored in etcd under its Worker as `{"value","setAtUnixMs"}`, **unencrypted**. Access is limited by etcd's authentication and roles: besides etcd's `root`, only the control plane's and the nodes' users can read the deployment state, and tenants never reach etcd. Values also stay in etcd's history until it is compacted, which nothing does by itself: compose starts etcd without automatic compaction. Encryption at rest is not done.
- **API.** `PUT .../secrets/{name}` sets the request body as the value; a name is a JavaScript identifier, and a value is at most 5 KiB of UTF-8 without NUL. `DELETE` is refused while it would make a valid Worker invalid. `GET .../secrets` lists names, lengths and when they were set; none of the platform's own answers, log lines or state endpoints shows a value.
- **Use.** A version declares the names it reads, at most 64; a Worker whose active version declares a secret without a value is invalid and not served. Values reach a Worker's `env` when its isolate loads on the executing node, which takes them from its own view of etcd and hands them to its own workerd process with each dispatch; they never travel with a call to another node. An isolate keeps the values it loaded with until it is replaced.

## 9. Usage

Every node counts, per tenant and UTC hour: requests (each execution of one of the tenant's Workers, Binding calls included), CPU milliseconds, egress bytes, log lines kept and isolate time (resident isolates sampled every second). It writes the counts that changed under its own prefix (`nodes/<nodeId>/usage/<hour>/<tenant>`) every `usage.flushIntervalMs` (one minute), so a node that stops loses at most that much; a draining node writes its last counts before it leaves. Each node keeps its own rows for `usage.retentionHours` (31 days). The usage endpoints sum the rows of every node, departed ones included, over whole UTC hours of a range of at most a year (the last 24 hours by default), so the history does not depend on the control plane.

## 10. Logs

Each node keeps the latest log lines of every tenant in memory, within the tenant's log quota ([process isolation](process-isolation.md), section 10). `GET .../logs` asks every node for the Worker's lines at once, each within `control.serverTimeoutMs` (2 s), and merges them by time; a node that does not answer is listed as unreachable. With `follow`, the answer is a stream of server-sent events (`text/event-stream`) that the control plane fills by asking every node for new lines every 500 ms.

## 11. The version promise

From 0.3.0, `/v1` changes only by adding fields and endpoints until 1.0, and error codes keep their meanings; removing something or changing its meaning goes into a new prefix.

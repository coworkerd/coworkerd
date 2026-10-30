# Coworkerd error codes

> For release 0.5.2. The control plane's API is `/v1`, and resources have two levels: tenant -> Worker.
> Error answers are JSON as a rule: a call's answer `{"error","reason","code",...}` (an admission refusal adds `loadBand`, `inFlight`, `limit`, `avoidMs` and `status`),
> the control plane's and the management endpoints' answers `{"code","reason"}`, some with more fields (a `LOAD_FAILED` of the control plane adds `category`, `message`, `nodeId` and the trial's `record`).
> A few answers are shaped otherwise, for example the plain-text 500 `Coworkerd execution failed` a node answers for an unexpected failure, `/__coworkerd/ready`'s plain-text 503, a static asset's empty 404 or 405, the trial-load endpoint's 400 and 413 `{"error","reason"}`, and the control plane's announcement's 400 and 413 `{"ok":false,"reason"}` on a node's peer port.
> Node-generated call errors and control-plane error responses use the reason cleaner: it replaces known secret values with `[redacted]` and absolute file paths with `<path>`, removes stack frames, and cuts the body at 400 bytes before appending `…`. The trial-load and rotation endpoints' 400 reasons are passed on as written. Durable Object fetch errors use the same redaction scope in the runtime: known secrets, absolute paths and stack lines are removed, and the complete reason is at most 400 UTF-8 bytes. Method exceptions keep workerd's RPC transport.
> The control plane logs each request it answers in full as a `[control] {"event":"request",...}` line with its status and code, and an unexpected failure as a `[control] {"event":"failure",...}` line with the reason, in its own log (`docker compose logs apiserver`); a node logs its own in the node's log (`docker compose logs node-1`); `cowctl logs` shows what the Worker itself printed.
> From 0.3.0, `/v1` only gains fields and endpoints and never changes the meaning of an existing code; 0.4.0 retires `ETCD_UNAVAILABLE`, which 0.3 answered on every endpoint while etcd could not be reached: 0.4 answers most reads from the control plane's copy, and the writes, the usage endpoints and a read of Workers whose definitions the control plane has not fetched yet answer `STORAGE_UNAVAILABLE` while object storage cannot be reached; it also retires `SECRET_TOO_LARGE`: a secret value over 5 KiB is `422 SECRET_REFUSED` whatever its size. The project promises that the codes below keep the meanings written here until 1.0.

## 1. When calling a Worker (the ingress's answers)

"Not executed" means the Worker's code certainly did not start, so it is safe to retry; "result uncertain" means part of it may have executed.

| HTTP | `code` | Meaning | Executed? | What to do |
| --- | --- | --- | --- | --- |
| 404 | `ROUTE_NOT_FOUND` | No route matches this host and path (`cowctl deploy` routes `/<tenant>/<Worker>` under the default host unless `--route`, or `route` in `cowctl.json`, names another: `/<prefix>` under `/<tenant>/` on the default host, or `<host>/<prefix>` on a hostname of the tenant); right after a deployment that did not wait for the nodes (`--no-wait`, or `/v1` directly), the node may not have read it yet (within 5 seconds) | Not executed | See the routes with `cowctl versions`; after such a deployment, retry a few seconds later |
| 503 | `TENANT_UNAVAILABLE` | The tenant the route belongs to is being deleted or its record is gone; none of its Workers are served any more | Not executed | Once the deletion completes the route disappears with it (404 from then on) |
| 503 | `ROUTE_INVALID` | The matching route is invalid in the deployment state (for example written badly bypassing the control plane) | Not executed | `cowctl deploy` again |
| 503 | `WORKER_INVALID` | The Worker is invalid in the deployment state: a secret the active version declares has no value, a Binding's target is invalid, and so on; `reason` says which entry (names only); on a node whose secrets key does not open a value, that secret counts as missing too (`missing secret: NAME`) | Not executed | Supply what the reason names (for example `cowctl secret put`) or switch to another version; a secret missing on one node only means that node's `secrets-key.json` is not the cluster's |
| 503 | `LOAD_FAILED` | The version cannot be loaded in the load check before dispatch, `category`: `package-unavailable` (no candidate node could fetch or verify the code package), `evaluation` (syntax, imports, throwing at the top level or a top-level `await` that never settles, a module that cannot be compiled or decoded, a missing entry point, compatibility flags), `limit` (exceeding the CPU or memory limit while loading), `secret-missing` | Not executed | `evaluation`, `limit`: fix the code and deploy again; `package-unavailable`: recovers by itself once object storage is back |
| 503 | `CPU_LIMIT_EXCEEDED` | This request's CPU exceeded the limit (the Worker's own, else its tenant's default, else the node's: 200 ms by default) and it was terminated | Partly executed | Do less computation per request |
| 503 | `MEMORY_LIMIT_EXCEEDED` | The isolate's memory exceeded the limit (the Worker's own, else its tenant's default, else the node's: 128 MiB by default) and it was terminated | Partly executed | Use less memory |
| 503 | `RESULT_UNCERTAIN` | Failed after dispatch, before the response started: the Worker threw, the process exited, the connection to the node running it broke, the node's drain deadline passed (or, during the load check, a process exited without being its culprit or could not be reached); once the response has started, a failure cuts it instead and the client gets an incomplete body | Result uncertain | See `cowctl logs`; idempotent requests can be retried |
| 503 | `SECRET_UNAVAILABLE` | The node running it has no usable value of a secret the version declares: it has not read the value yet (it was just set; nodes read the desired state every 5 seconds), or the value does not open with that node's secrets key | Not executed | Retry a little later; when it persists on one node, that node's `secrets-key.json` is not the cluster's |
| 409 | `VERSION_UNAVAILABLE`, `VERSION_PIN_MISMATCH` | The deployment changed during the call, and the node it runs on does not know the version this call is locked to yet; or the node could not fetch or verify the version's code bundle before execution: the next candidate node is tried, and when none can, the call answers `503 PLACEMENT_EXHAUSTED` with `VERSION_UNAVAILABLE` in each attempt (409 itself only for a Worker with a static placement or with ingress placement off). A static asset whose bytes cannot be fetched or verified answers `503 ASSET_UNAVAILABLE` at the node that received the call, without another node being tried | Not executed | Retry |
| 410 | `LEASE_EXPIRED` | The deadline of this call (or of its parent call) has passed | Not executed | Retry |
| 429 | `SUBREQUEST_BUDGET_EXHAUSTED` | The subrequest budget of one call tree is used up -- on a WebSocket connection, of one message: each message starts it again | Not executed | Make fewer Binding calls |
| 429 | `WEBSOCKET_LIMIT` | A WebSocket upgrade beyond the open connections this node holds for the tenant (`ingress.webSocketsPerTenant`, 1000 by default); other requests are not affected | Not executed | Close a connection, or connect through another node; the administrator can raise the limit |
| 503 | `SERVER_DRAINING`, `PROCESS_UNAVAILABLE`, `QUARANTINE_UNAVAILABLE`, `PLACEMENT_EXHAUSTED`, `PEER_UNAVAILABLE`, `OVERLOADED`, `LOAD_BAND_REJECTED` | This node or all candidate nodes cannot take it for now (exiting, a process not ready, a concurrency limit, memory pressure) | Not executed | Retry, or use another ingress |
| 508 | `PLACEMENT_HOPS_EXHAUSTED` | A Binding call relayed along static placements ran out of placement hops | Not executed | Check the nodes' `placements` entries |
| 503 | `ASSET_UNAVAILABLE` | A static asset's bytes cannot be fetched or do not verify; while a Worker's asset index cannot be read, every request to that Worker answers it, also for paths its code would serve | Not executed | Recovers by itself once object storage is back |

Once the Worker accepted a WebSocket (`101`), the connection ends with a close code instead of an answer: the Worker's own codes pass as they are; `1001` (going away) when the node holding or running the connection drains, or the process it runs in is replaced -- reconnect, and another node or process takes it --; `1011` with the reason `CPU_LIMIT_EXCEEDED` or `MEMORY_LIMIT_EXCEEDED` when a message went over a limit, `RESULT_UNCERTAIN` when its process ended otherwise; no close frame, which a client reports as `1006`, when the node holding the client's connection lost the node running it.

**Durable Object calls**: what a stub's `fetch()` answers when the object's request fails -- the Worker decides what its own client gets. The refusals of the table above apply as well (`WORKER_INVALID`, `TENANT_UNAVAILABLE`, `VERSION_UNAVAILABLE`, `SECRET_UNAVAILABLE`, `OVERLOADED`, and for a stateless Worker's call `SUBREQUEST_BUDGET_EXHAUSTED` and `LEASE_EXPIRED`). A call to an object goes to the node that owns the object's shard ([cluster state](../cluster-state.md), section 11); when that node refuses it, or cannot be reached, the node of the caller looks for the owner once more and sends it once more, and answers the second refusal: `PEER_UNAVAILABLE` (`503`) when the owner could not be reached, the owner's own code otherwise; once sent, a call whose answer did not come back is `RESULT_UNCERTAIN`. While a shard has no owner a node can reach, its objects' calls fail this way until another node takes the shard over.

| HTTP | `code` | Meaning | Executed? | What to do |
| --- | --- | --- | --- | --- |
| 500 | `EXECUTION_FAILED` | The object's class threw; `reason` has the exception's message. The object stays as it was: the same instance takes the next request | Partly executed | Fix the class; retry what is safe to retry |
| 503 | `CPU_LIMIT_EXCEEDED`, `MEMORY_LIMIT_EXCEEDED` | The object's request went over a limit, which ends the version isolate in that durable process: the object -- and every other object of the version there -- is constructed again at its next request, with its stored data (`EXECUTION_FAILED` with 503 when workerd reset the object otherwise) | Partly executed | Do less per request; retry what is safe to retry |
| 508 | `DO_HOP_LIMIT` | More than 8 Durable Object calls in a row (an object calling an object calling an object, ...) | Not executed | Shorten the chain |
| 409 | `DO_NAMESPACE_UNAVAILABLE` | The Worker's active version does not declare the object's class, or the class's namespace is being deleted (`deleted_classes`); a namespace Binding that cannot be used throws an error starting with this code from `idFromName`, `newUniqueId`, `idFromString` and `get` | Not executed | Deploy a version that declares the class and binds it |
| 503 | `DO_UNAVAILABLE` | The node's durable process for this object is not ready -- starting, or being replaced -- and did not become ready within 5 seconds; or no node that runs durable processes could take the object's shard | Not executed | Retry |
| 409 | `DO_SHARD_EPOCH_MISMATCH` | The call reached a node that does not hold the object's shard at the epoch the caller's node expected (that node's view of the owner was stale); answered to the stub only when the caller's node met it again after looking for the owner once more | Not executed | Retry |
| 503 | `DO_OWNER_EXPIRED` | The node that owns the object's shard -- or the node of the caller -- could not write its membership heartbeat for the TTL and gave up its shards; another node takes them over a TTL plus the grace after its last heartbeat | Not executed | Retry (after about 30 seconds) |
| 503 | `DO_OWNER_DRAINING` | The node that owns the object's shard is draining: it finishes the object's requests in flight and then releases the shard, which another node takes at once | Not executed | Retry |
| 503 | `DO_UNSUPPORTED_STORE` | Durable Objects are not supported on this cluster's object store: its create-if-absent did not hold under concurrency (`conditionalPutAtomic: false`), so a shard's single owner cannot be guaranteed | Not executed | Use a store where it holds (the verified, pinned SeaweedFS 4.48) |
| 503 | `STORAGE_UNAVAILABLE` | Either the object's shard has no owner this node knows and the node could not read or write the shard's epochs on the object store within its timeouts (nothing was executed), or a write of the object could not be made durable: no peer node confirmed its frames within `durableObjects.confirmTimeoutMs` (5 s) -- the peers cannot be reached, or what they hold for this node is full because the object store has been out of reach -- or the local log flush failed with no selected peers. **The result of such a write is uncertain**: it was committed on the owner, and the node then isolates the frames nobody confirmed and rebuilds the object from its replicated history, and persists exclusions at the next seal even without new confirmed records. Until that manifest succeeds, a peer that was out of reach may still hand the tail to a later rebuild. Reads and other objects are not affected | Uncertain | Retry with an idempotency key (below); check the cluster's object store |
| 503 | `DO_RECOVERING` | The object is being rebuilt from its replicated history -- its shard was taken over, this machine's files were lost, or a write of it was not confirmed -- and the request waited `durableObjects.recoverTimeoutMs` (30 s) without it finishing. A large object takes longer than a small one | Not executed | Retry |
| 413 | `DO_OBJECT_TOO_LARGE` | A write would take the object past `durableObjects.objectBytes` (10 GiB), as its databases last reported their size. Growing transactions are refused; the object is marked for repair immediately, and the next request waits for recovery before reads or non-growing transactions resume | Not durable: no peer holds the refused transaction, no segment and no manifest name it, so it is gone from the object's history. The node immediately marks the object for repair; the next request rebuilds from confirmed history before serving, so it cannot read the refused transaction | Delete rows, or ask the administrator to raise `durableObjects.objectBytes`; transactions that do not grow the database remain permitted |
| 422 | `QUOTA_EXCEEDED` | A write while the tenant's Durable Objects already hold more than its `durableObjectBytes` quota (10 GiB by default). What the tenant holds is what the nodes publish of their own objects every `durableObjects.usagePublishMs` (an hour), so enforcement lags; failed publication or reads can delay it beyond an hour. Only growing transactions are refused | Not durable; immediate repair and recovery before the next request, as above | Delete data, or ask the administrator to raise the quota (`cowctl tenant update T --quota durableObjectBytes=N`); transactions that do not grow their database are permitted |

**A write's result can be uncertain.** A Durable Object's write waits for peer confirmation, or requires a successful local flush when the selected peer list is empty. A failed flush answers `STORAGE_UNAVAILABLE` and is not acknowledged. Uploaded history and a surviving confirmed tail can be recovered; simultaneous loss of the owner and all confirming peers can lose the unuploaded tail. The other way round
does not hold: when the owner dies between the moment its peers hold a write and the moment its answer reaches the
caller, the caller sees a failure (`RESULT_UNCERTAIN`, or a broken connection) for a write that did happen -- about
45% of the writes in flight when a node is killed, as the experiments behind this design measured. `STORAGE_UNAVAILABLE`
on a write is the same kind of answer. So a client that retries must either be able to repeat the write (`INSERT OR
REPLACE` on a key it chose) or send an **idempotency key**: a value the client makes once per intent, which the object
stores with the result and answers with the stored result when it sees the key again. Codes marked "Not executed"
above never need one.

For a Worker without a static placement -- an ingress request while ingress placement is on (the default), and every Binding call --, a refusal another node may not share (every code but `RESULT_UNCERTAIN`, `LEASE_EXPIRED` -- the deadline is the call's own, so the call ends with it, and no node is avoided for it -- and a `LOAD_FAILED` other than `package-unavailable`) sends the call to the next candidate node, at most `initialPlacementHops` (8) of them; when every candidate tried refuses, the answer is `503 PLACEMENT_EXHAUSTED` (or `503 LOAD_FAILED` with `package-unavailable` when none could fetch or verify the code package), listing each candidate's `nodeId` and refusal under `attempts`, so codes such as `SECRET_UNAVAILABLE`, `OVERLOADED`, `PROCESS_UNAVAILABLE` or the code bundle's `VERSION_UNAVAILABLE` then appear only there.

The fields in the call answers that refer to a node are called `nodeId`; `/v1`'s deployment answers name the node that ran the trial load in the trial record's `server` fields (`versions[].server`, `attempts[].server`) and as `nodeId` in its verdict, and a `LOAD_FAILED` answer also as a top-level `nodeId`.

## 2. The control plane (`/v1` and `cowctl`)

When `cowctl` fails it prints one sentence plus the reason the control plane gave, and exits with 1 (2 for a usage mistake, 130 when interrupted); for a refused credential it prints the code and where to check: `COWCTL_TOKEN`, or the token file `cowctl.json` names. Every `/v1/` request needs `Authorization: Bearer <credential>`:
the **admin credential** (the platform's own, generated by compose's `secrets` step or given in the configuration) can do everything; a **tenant credential** (starting with `cwk_`, created by the administrator for one tenant) can act only on that tenant and, like every credential, read the cluster's status (`GET /v1/status`).

**Authentication and scope**

| HTTP | `code` | Meaning | What to do |
| --- | --- | --- | --- |
| 401 | `UNAUTHORIZED` | No credential, a malformed one, an unknown one, or one that was revoked or has expired | `COWCTL_TOKEN`, or the credential file `cowctl.json` points to (default `~/.cowctl/token`); for a revoked credential, ask the administrator for a new one |
| 403 | `FORBIDDEN` | The scope is not enough: a tenant credential accessing another tenant, or the administrator's endpoints (creating, listing, updating and deleting tenants, issuing and listing credentials, `/v1/nodes`, `/v1/usage`) | Use that tenant's own credential; the administrator's operations take the admin credential |

**Tenants and credentials**

| HTTP | `code` | Meaning | What to do |
| --- | --- | --- | --- |
| 404 | `TENANT_NOT_FOUND` | No such tenant | `cowctl tenant create`, or check the name |
| 404 | `CREDENTIAL_NOT_FOUND` | This tenant has no such credential (wrong `keyId`, or it belongs to another tenant) | `cowctl credential list --tenant T` |
| 409 | `CONFLICT` | The tenant already exists; the hostname to register already belongs to another tenant (also when another tenant took it at the same moment: the change is undone); the hostname to remove is still routed by this tenant's Workers; the tenant is being deleted (no more writes accepted), or was deleted while a credential for it was being created; or another control plane kept writing the same tenant, so the change met another writer's revision 8 times in a row, with jittered pauses (nothing was written) | Change it as the reason says; retry on a concurrency conflict |
| 503 | `DELETION_INCOMPLETE` | Deleting the tenant stopped halfway (for example object storage was temporarily unavailable, or did not remove an object); `reason` says at which step; the tenant stays `deleting` and is not served | Once recovered, `DELETE` again; it continues from where it stopped (every step can run again) |

**Quotas and hostnames**

| HTTP | `code` | Meaning | What to do |
| --- | --- | --- | --- |
| 422 | `QUOTA_EXCEEDED` | Over the tenant's quota; `reason` starts with the quota's name: `workers`, `versionsPerWorker`, `packageBytes`, `assetFiles`, `assetBytes`, `secrets`, `hostnames`, `maxWorkerCpuMs`, `maxWorkerMemoryMiB` (a Worker-level limit above what the tenant allows), `defaultLimits` (the tenant's default limits above its own maximum), `durableObjectNamespaces` (a new Durable Object class or database beyond the tenant's namespaces), or `durableObjectBytes` (what its Durable Objects may hold; enforced by the nodes, see section 1). Admission failures do not publish the requested change; a durable write refusal marks the object for immediate repair, and the next request waits for confirmed history to be restored | Delete old versions or old Workers, or ask the administrator to adjust the quota (`cowctl tenant update T --quota name=value`) |
| 403 | `HOSTNAME_NOT_OWNED` | The route's hostname is not registered to this tenant; or the route is under the cluster's default host `*` with a path that is neither `/<tenant>` nor under `/<tenant>/` (written as `/<tenant>/`; when the administrator turned path routing under the default host off, every route under `*` that a deployment or a route change lists is refused, one already written included, while the nodes keep serving those written before) | Ask the administrator to register the hostname to the tenant (`cowctl tenant update T --hostname H` with every hostname the tenant keeps: the list given replaces the tenant's), or change the route |

**Deployments, versions and secrets**

| HTTP | `code` | Meaning | What to do |
| --- | --- | --- | --- |
| 422 | `LOAD_FAILED` | The new version's **trial load** failed and nothing was switched (the active version did not change); `category` in the table below | Fix the code and deploy again |
| 503 | `TRIAL_UNAVAILABLE` | No node can do the trial load: none registered, or none could run it -- unreachable, refusing the management token, or unable to get the version, its code or a ready process in time | `cowctl status`; retry once the cluster is ready |
| 422 | `DEPLOYMENT_REFUSED` | The checks before deployment did not pass: a declared secret has no value, the route belongs to another Worker, a Binding's target does not exist (targets are looked up only within the same tenant), the closure exceeds 64 Workers, the version to delete is the active one, and so on | Change it as the reason says; for a missing secret, `cowctl secret put` first |
| 409 | `VERSION_EXISTS` | This version id already has different content (versions are immutable); `cowctl deploy` moves on to the next id by itself, but not for one given with `--version` | Use another version id |
| 409 | `CONFLICT` | Another control plane changed the same tenant at the same time, and the change still met another writer's revision after 9 attempts (8 for a Worker's limits and for deleting a version); nothing was written | Retry |
| 404 | `WORKER_NOT_FOUND`, `VERSION_NOT_FOUND`, `SECRET_NOT_FOUND` | This tenant has no such Worker (or, to remove it or change its limits or routes, it has no active version) / the version was never uploaded / the secret is not set | Check the names |
| 422 | `SECRET_REFUSED` | The value is not UTF-8, contains NUL or is larger than 5 KiB (5,120 bytes, whatever its size), or deleting it would invalidate the active version (an invalid name -- it must be a JavaScript identifier -- answers `400 INVALID_REQUEST`); also a write that failed for a reason the value (8 bytes or more) would appear in, the reason withheld | Change it as the reason says |
| 413 | `PACKAGE_TOO_LARGE` | The uploaded bundle is larger than the control plane's own limit (`control.maxUploadBytes`, default 160 MiB: a bundle of exactly that size is accepted; when the tenant's quota is smaller, it reports `QUOTA_EXCEEDED`) | Make it smaller |

**Other**

| HTTP | `code` | Meaning | What to do |
| --- | --- | --- | --- |
| 400 | `INVALID_REQUEST` | The request is not valid (the bundle's format, the tenant's or Worker's name, JSON fields, parameters; an upload without activation whose manifest lists routes, which only an activation writes) | Use `cowctl`; names are lowercase letters, digits and `-`, at most 63 characters |
| 404 | `NOT_FOUND` | No such endpoint | -- |
| 405 | `METHOD_NOT_ALLOWED` | The endpoint exists but the method is wrong; `reason` lists the allowed methods | -- |
| 404 | `NODE_NOT_FOUND` | `/v1/nodes/{node}`: no such node (no live heartbeat in the object store) | `cowctl node list` |
| 503 | `STORAGE_UNAVAILABLE` | The control plane cannot reach object storage, or the store answered with an error: a change of the desired state, an upload, or the usage endpoints (`/v1/usage`, `/v1/tenants/{t}/usage`). Nothing was written, unless the failure came after the change's revision was created (its hint or the tenant index could not be written): the change then becomes visible once the control plane's next poll or start completes it, and a retry finds it made. Reads are answered from the control plane's copy of the desired state, and the nodes keep serving | Check the cluster (`docker compose ps`, `docker compose logs storage`) and retry |
| 503 | `LOGS_UNAVAILABLE` | A node's log reader process did not answer: that node's `/__coworkerd/logs` answers it, and `/v1`'s logs answer (200) marks the node `reachable: false` with its problem, which `cowctl logs` prints as unreachable | Retry a little later |
| 500 | `INTERNAL` | An error of the control plane itself; `reason` says what | Report it with the `reason` |

**Trial-load categories** (the `category` of `LOAD_FAILED`):

| `category` | Meaning |
| --- | --- |
| `MISSING_MODULE` | Imports a module the bundle does not have |
| `SYNTAX_ERROR` | A syntax error |
| `TOP_LEVEL_THROW` | The module throws at its top level |
| `TOP_LEVEL_AWAIT` | A top-level `await` that never completes (workerd refuses it while loading) |
| `ENTRYPOINT_MISSING` | No default export (when the Worker has routes, or no named entrypoint serves `fetch`), or a default export without `fetch` |
| `COMPATIBILITY_FLAG` | Imports `node:*` without `nodejs_compat` |
| `INVALID_MODULE` | A WebAssembly or data module cannot be compiled or decoded |
| `LIMIT_EXCEEDED` | Exceeds the CPU limit (an endless loop at the top level) or the memory limit while loading (`message` says which) |
| `TIMEOUT` | Loading takes more than 5 seconds |
| `LOAD_FAILED` | Anything else (for example the process exiting while loading) |

Trial loads run in the node's **quarantine processes** (sharded by tenant, see `quarantineProcesses` in section 5 of [installing and running](install.md)),
so a new version that crashes a process does not take other tenants' in-flight requests in the ordinary processes with it.

**At conversion** (before anything is uploaded; reported locally by `cowctl deploy`), for example: `Unsupported Wrangler configuration: <entry>` (see the [compatibility scope](compat.md)),
`Wrangler build failed: ...` (wrangler's own build errors, for example esbuild not finding an imported file), `no Wrangler configuration here`; others name what the configuration lacks or exceeds (`main` and `compatibility_date`, the static assets' limits, at most 64 secrets, secret names that collide with `vars` or Bindings).

## 3. Management endpoints (a node's `/__coworkerd/`)

A node needs a management token (compose sets one; without one a node starts only when its configuration says `management.allowWithoutToken: true`, and its endpoints are then open). Everything except `/__coworkerd/ready` and `/__coworkerd/probe` needs `Authorization: Bearer <token>`, otherwise 401
`UNAUTHORIZED`, and the node logs a line `[management] {"event":"unauthorized",...}` (without the token). When the control plane refuses a `/v1` request it likewise logs
`[control] {"event":"unauthorized",...}` (without the credential). The management endpoints also answer `METHOD_NOT_ALLOWED` (405), `NOT_DRAINING` (409, `GET /__coworkerd/drain?wait=1` on a node that is not draining), `INVALID_ROTATION_REQUEST` (400), `SERVER_DRAINING` and `RELOAD_UNAVAILABLE` (409, `POST /__coworkerd/reload`), `INVALID_LOG_QUERY` (400) and `LOGS_UNAVAILABLE` (503, `/__coworkerd/logs`). The management token is used by the control plane, the operations tool and operators calling a node; nodes do not use it among themselves (their sessions are signed with the cluster key). It has nothing to do with the `/v1` credentials.

## 4. A database's statements (errors `env.DB` throws)

A `d1_databases` Binding speaks D1's protocol, so its errors are D1's: statement failures throw an `Error` whose `message` starts with the D1 prefix, and `error.cause` carries the message alone. `batch()` rejects on any statement failure and rolls the whole batch back. `exec()` stops at its first failing line and keeps earlier successful lines.

| `message` | Meaning | What to do |
| --- | --- | --- |
| `D1_ERROR: <SQLite's message>` | The statement itself failed: a table that does not exist, a constraint, a type | Fix the statement |
| `D1_ERROR: DO_OBJECT_TOO_LARGE: ...` | The write would take the database past 10 GiB (section 1); the statement is not durable, and reads go on | Delete rows, or ask the administrator to raise the object limit; transactions that do not grow the database remain permitted |
| `D1_ERROR: QUOTA_EXCEEDED: ...` | The tenant's Durable Objects already hold more than `durableObjectBytes` (section 1) | Delete data, or ask for a larger quota; transactions that do not grow the database remain permitted |
| `D1_ERROR: STORAGE_UNAVAILABLE: ...` | The write could not be made durable: no peer confirmed it in time, or the object store is out of reach. The statement may have changed local state; confirmation failure isolates it and triggers recovery | Check state or use an application operation ID before retrying |
| `D1_EXEC_ERROR: Error in line N: <line>: ...` | One line of `exec()` failed; earlier successful lines remain applied and later lines were not run (`exec` is not a transaction) | Fix that line |
| `D1_TYPE_ERROR: Type 'x' not supported for value 'y'` | `bind()` was given a value that is not a number, a string, a boolean, `null`, an `ArrayBuffer` or a byte array | Convert it first |
| `D1_COLUMN_NOTFOUND: Column not found (c)` | `first("c")` on a row without that column | Check the statement's columns |
| `D1_ERROR: the D1 Sessions API is not supported on this platform` | `withSession()`; `dump()` and `query()` answer the same way | Not in this release |

The HTTP SQL interface (`POST /v1/tenants/{t}/databases/{d}/query`) sends the control plane's request through any node to the database's shard owner and answers the same shapes as the object does:
`{"success":true,"results":[...],"meta":{...}}`, or `{"success":false,"error":"..."}` for a statement that failed, and
the control plane's own `{"code","reason"}` when the database does not exist (`404 NOT_FOUND`) or no node serves it
(`503 STORAGE_UNAVAILABLE`). Node statuses and bodies pass through, including 200 with `success:false`, 413, 422 and 503. A dispatch refusal returns its original code and body without waiting for the rejected request body to be consumed. Another member is tried only if connection establishment failed, or a node refused with `PEER_UNAVAILABLE`, `SERVER_DRAINING`, `DO_UNAVAILABLE`, `DO_OWNER_EXPIRED`, `DO_OWNER_DRAINING` or `DO_SHARD_EPOCH_MISMATCH` before execution. A lost reply after connecting returns `503 RESULT_UNCERTAIN` without replay. The JavaScript binding preserves the platform error prefix.

## 4b. Method calls on a Binding (errors `env.SVC.method()` and `stub.method()` throw)

A method call on a Service Binding or on a Durable Object stub is a call of the tree like a `fetch` through the same
Binding, so the same things can refuse it -- but there is no response to read the code from: the call **throws**. The
platform's code is the start of the error's `message`, and on a call made through the Binding itself it is the error's
`code` property too (on a call made on a capability the target returned, the message carries it alone); an exception the target's own code threw arrives with its message. The Binding shim derives `.code` from any leading `UPPER_CASE:` prefix, including an application exception, so that property alone does not prove a platform failure.

| `message` | `code` | Meaning | What to do |
| --- | --- | --- | --- |
| whatever the method threw | absent unless the message starts with an uppercase code prefix | The target's code threw: the instance remains available; a Binding call may add `.code` based on the message prefix | The caller's own business |
| `SUBREQUEST_BUDGET_EXHAUSTED: ...` | `SUBREQUEST_BUDGET_EXHAUSTED` | The call tree's budget is used up: a method call draws on it exactly as a `fetch` through the Binding does (section 1) | Make fewer Binding calls |
| `LEASE_EXPIRED: ...` | `LEASE_EXPIRED` | The call tree's deadline has passed -- for a capability the target returned, this is also what a call on it answers once the tree that made it is over | Make the call within the tree that opened it |
| `CPU_LIMIT_EXCEEDED: ...`, `MEMORY_LIMIT_EXCEEDED: ...` | the same | A limit ended the method, as it would a request (section 1) | Do less per call |
| `VERSION_UNAVAILABLE: ...`, `VERSION_PIN_MISMATCH: ...` | the same | The deployment changed during the call (section 1) | Retry |
| `DO_HOP_LIMIT: ...`, `DO_OWNER_EXPIRED: ...`, `DO_SHARD_EPOCH_MISMATCH: ...`, `DO_RECOVERING: ...` | the same | The object could not be reached or run, as for a `fetch` to it (section 1) | As in section 1 |
| `Network connection lost.` and the like | absent | The node that ran the target went away while the caller still held one of its capabilities | Retry; the call tree is over |

A value a method sends or returns is at most 32 MiB serialized (workerd's own limit), and a value that is not
serializable (a `Request`, a `WebSocket`, a class instance that is not an `RpcTarget`) fails the call with the message
workerd gives it.

## 5. Outgoing requests of tenant code (errors `fetch` throws)

Tenant code's `fetch()` goes out through the egress service of the node it runs on ([installing and running](install.md), section 11). When the node refuses the destination or cannot connect to it, `fetch` **throws** an `Error`
(it does not return a response) whose `message` starts with the error code followed only by the rule's name or the failure's category, without any internal details (addresses, ports, names within the cluster):

```js
try { await fetch("http://10.0.0.5/"); }
catch (error) { error.message; }     // "EGRESS_DENIED: private-network"
```

| `message` | Meaning | What to do |
| --- | --- | --- |
| `EGRESS_DENIED: cluster` | The destination is one of the cluster's own addresses: a node, a peer, object storage, the control plane (it announces its addresses to the nodes; others in `egress.clusterAddresses`), on any port | Cannot be allowed |
| `EGRESS_DENIED: loopback`, `link-local`, `metadata`, `unspecified`, `reserved` | Loopback, link-local, the clouds' metadata addresses (169.254.169.254 and others), `0.0.0.0`, reserved and multicast addresses | Cannot be allowed |
| `EGRESS_DENIED: private-network` | An internal address (RFC 1918, 100.64/10, 198.18/15, IPv6 ULA `fc00::/7` and site-local `fec0::/10`; the clouds' metadata addresses among them, such as 100.100.100.200 and fd00:ec2::254, are `metadata`, which nothing opens) | The platform's own internal services are allowed by the administrator per tenant (`allowInternal`) |
| `EGRESS_DENIED: port` | The default policy `public` allows only ports 80 and 443 of public addresses | The administrator writes a rule with the port into the tenant's `allow` |
| `EGRESS_DENIED: default-deny` | The cluster's default policy is `deny`, and the tenant's `allow` does not have this destination | The administrator allows it |
| `EGRESS_DENIED: egress-off` | The node runs with `egress.mode` `off`: tenant code has no outgoing network | The administrator turns egress on |
| `EGRESS_DENIED: tenant-deny` | The tenant's `deny` rules | -- |
| `EGRESS_DENIED: concurrency-limit`, `rate-limit` | This tenant's outgoing concurrency (default 32) or requests per second (default 100) on this node are full | Retry a little later, or ask the administrator to raise them |
| `EGRESS_DENIED: scheme`, `url` | A destination the policy point cannot take: not `http`/`https`, or an invalid URL, such as one without a host. A scheme other than `http`/`https` usually never reaches the node -- `fetch` throws `TypeError: Fetch API cannot load` itself --, and a user name and password in the URL are dropped by `fetch`, so the request goes out without them | -- |
| `EGRESS_FAILED: dns` | The node cannot resolve this name | Check the name |
| `EGRESS_FAILED: connect`, `tls`, `timeout`, `protocol` | Cannot connect, the TLS handshake or the certificate check failed, a timeout (30 seconds by default), the other side's answer is not valid HTTP | Check the destination; certificates are verified by the node against the system's root certificates |

Redirects are followed by `fetch` itself, and each hop is checked again: a public address redirecting to an internal one is refused just the same. Every request the node's egress service handles (allowed, refused, failed) has a line in the node log -- with `egress.mode` `off` the runtime refuses without the node and no line is written --
`[egress] {"tenant","workerId","host","port","address","result","rule","status","requestBytes","responseBytes","durationMs",...}`,
not counted against the tenant's log quota; refusals for concurrency and rate are logged at most once per second per tenant (with the number of lines left out meanwhile), and the counts miss none.

## 8. Durable confirmation is not a rollback guarantee

`STORAGE_UNAVAILABLE`, `DO_OBJECT_TOO_LARGE` and `QUOTA_EXCEEDED` at a durable write's output gate mean the caller did not receive successful durable confirmation. The SQLite transaction may already have changed the local file. A size- or quota-refused transaction reaches no peer or uploaded history: the node immediately marks the object for repair, and the next request waits for recovery from confirmed history before serving, so it cannot read the refused transaction. These errors do not mean the SQL statement never executed locally. After confirmation timeout the node isolates its local records and asks peers to drop them before rebuilding. Peer-drop delivery is best effort and repeated on reconnect; exclusion metadata is published at the next seal even without new confirmed records. If publication fails, an unreachable peer can retain a tail that recovery may still see. A successful response also remains subject to the accepted loss of the unuploaded tail when the owner and all confirming peers fail together, as described in [Durable Objects](durable-objects.md), section 7. A failed flush on the no-peer path is logged and returns `STORAGE_UNAVAILABLE` without confirmation. Use application operation IDs and inspect state when retrying non-idempotent writes.

Alarms can run again after failures, and an absent discovery hint can delay them until the object is reopened. WebSocket clients reconnect after a process / node exit. Neither a timeout nor a broken RPC capability proves that a method did not run; after dispatch it is uncertain. RPC platform codes survive in the exception's message, with `.code` added when possible; getters and returned capabilities can expose only the message prefix.

# Compatibility with Cloudflare Workers

This document is for platform engineers and contributors who want to know how far Coworkerd runs code written for Cloudflare Workers, and why the line is where it is: how a wrangler project becomes a Coworkerd version, how its code runs, what is checked and refused, and where behaviour differs from Cloudflare's. The [compatibility scope](guide/compat.md) is the checklist for users -- which configuration entries work and which are refused; this document gives the reasons.

## 1. The goal and the line

An important reason to build on workerd is to reuse the Workers ecosystem: wrangler, its build pipeline and existing applications. The goal is that a stateless application built with wrangler runs without changing its code. The line is drawn by what a cluster of this kind provides:

- **Stateless execution only.** Durable Objects, KV, R2, D1, Queues, Workflows, Vectorize and the like need storage and placement with ownership that this release does not have; configuration that uses them is refused at conversion.
- **No Cloudflare cloud services.** Workers AI, Hyperdrive, Email, dispatch namespaces, Access, Rate Limiting and similar bindings are services of Cloudflare's network, not of the runtime.
- **Not yet.** Scheduled triggers need scheduling across the cluster -- one trigger runs once in the whole cluster -- and JS RPC needs capabilities and their lifetimes across machines; neither is done, and only `fetch` handlers are ever called.

Anything a project configures outside the line is refused at conversion and listed by name, never ignored silently: an application that silently loses its KV binding is worse than one that does not deploy.

## 2. From a wrangler project to a version

`cowctl deploy` does not bundle code itself:

1. **Check the configuration** against the lists of section 5, before building.
2. **Build with wrangler.** It runs `wrangler deploy --dry-run --outdir` -- the project's own wrangler when one is installed in the project or above it, otherwise the version `cowctl` pins -- and reads the configuration through wrangler's own interface, so TOML, JSON and the configuration a build tool such as the Vite plugin redirects to are read as wrangler reads them. With the pinned wrangler the build runs in a temporary copy of the project, so the output does not depend on where `cowctl` is installed.
3. **Check the asset directory's rule files**, `_redirects` and `_headers`, which the build may produce, after building.
4. **Make a package**: the modules wrangler produced, each with its type, hash and size, and the static assets, in a canonical manifest whose SHA-256 is the package hash. Configuration -- the main module, compatibility date and flags, variables, Bindings, the secret names the version reads, the assets settings -- goes into the version definition, not into the package, so the same code can be deployed with another configuration without storing it again: every deployment sends its whole package, and the control plane stores only the objects the store does not have yet.
5. **Upload** the package to the control plane as a new version, which validates it, trial-loads it in a node's quarantine process (an ordinary one when the node has none) when it has code, and only then makes it exist ([control plane API](control-plane-api.md), section 6).

## 3. How tenant code runs

workerd's dynamic Worker Loader loads a Worker with its own modules, compatibility date and flags and environment, which is what one version needs; the rest is the package format, the deployment state and a bridge module.

- **The runtime bridge** (`runtime/runtime.js`) fetches the version's manifest and modules from its node, checks the package hash and every module's hash, and loads them into an isolate of their own. A bridge module generated per version imports the tenant's main module and dispatches each call to the entry point it names: a class extending `WorkerEntrypoint`, constructed per call, or an object with `fetch`; a named export serves a Binding's `entrypoint`.
- **`env`** holds the version's variables and secrets, fixed when the isolate loads, and the call's Bindings, which change from call to call because they carry the call tree's identity and budget; they arrive through `ctx.props`. The bridge runs the handler inside `withEnv`, so `import { env } from "cloudflare:workers"` sees the same `env` during a call; at a module's top level it sees the variables and secrets.
- **A Binding is a capability bound to its target.** Tenant code gets a stub created by the platform for one target Worker and version; it cannot name another target, and the node executes exactly the target the definition approved.
- **Compatibility flags** are the version's own plus `enable_request_signal`, which the platform always turns on because cancellation depends on `Request.signal`; `disable_request_signal` is refused.

## 4. What is checked at deployment

| Item | Rule | Why |
| --- | --- | --- |
| Compatibility date | A real date, not later than the maximum of the bundled workerd | Later dates name behaviour this runtime does not have |
| Compatibility flags | On the allowed list generated from the bundled workerd's sources (both names of every non-experimental flag, 294 in all) | Experimental features stay off |
| Modules | Safe names (no `..`, not absolute, not starting with `__coworkerd_`), a supported type (ES module, CommonJS, text, data, JSON, WebAssembly), a main module that is an ES or CommonJS module; at most 10 MiB together, uncompressed | The bridge and the cache address modules by name and hash; counting uncompressed is simple and predictable |
| Variables | At most 64, JavaScript identifiers, each at most 5 KiB, plain text or JSON | As on Cloudflare |
| Secret names | At most 64 per version, JavaScript identifiers, none shared with a variable or Binding | Secrets, variables and Bindings share one `env` |
| Static assets | At most 20,000 files, 25 MiB each, 100 MiB in total: defaults that `COWORKERD_ASSET_MAX_COUNT`, `COWORKERD_ASSET_MAX_FILE_BYTES` and `COWORKERD_ASSET_MAX_TOTAL_BYTES` change for the adapter, the control plane and the nodes alike; the tenant's `assetFiles` and `assetBytes` quotas apply as well, and wrangler's asset manifest, which the adapter builds first over the asset directory (with the project's own wrangler, `.git` and the local value files included; the adapter's own wrangler builds from a copy without them), refuses a file over 25 MiB or more than 100,000 files whatever they say | As on Cloudflare per file; the total is conservative |
| Loading | The version is trial-loaded: imports, top-level code, the entry point's export, the CPU and memory limits while loading | A deployment is a rare, watched moment, the cheapest place to find a load error |

The flags' list and the maximum date are generated from the workerd sources the patch series applies to (`scripts/compatibility-flags.py`, into `common/generated/`, which is committed): following upstream means running it again by hand, since no build step runs it.

## 5. Configuration entries

- **Mapped**: `name`, `main`, `compatibility_date`, `compatibility_flags`, `vars`, `services` (Service Bindings, with `entrypoint`), `assets` and the build entries wrangler uses to produce the modules; `$schema` and `send_metrics` are accepted and change nothing. Source maps are not kept: the adapter drops wrangler's `.map` output, whatever `upload_source_maps` says.
- **Accepted, ignored, reported**: `observability`, `placement`, `workers_dev`, `preview_urls`, entries with empty values, and, in a configuration a build tool redirected, wrangler's own path and name fields (`configPath`, `userConfigPath`, `topLevelName`) and fields equal to their defaults. The first four are Cloudflare's own logging, scheduling and URL settings and change nothing the code does -- refusing them would leave almost no real project convertible --, and the others carry no configuration of the Worker; all are listed in the conversion's output as ignored.
- **Refused**: stateful and cloud bindings, `triggers`, wrangler's own `secrets` entry, `routes`, `route` and `env` -- routes in Coworkerd are the platform's: hostnames the administrator registers to a tenant, or `/<tenant>/...` under the cluster's default host -- `_redirects` and `_headers` with rules, a non-default `assets.html_handling`, and any key not on the lists.
- **Secrets**: a version declares the names it reads -- those of the project's `.dev.vars.example` and the `--secret NAME` arguments of that deployment; values are set separately with `cowctl secret put`. The adapter takes no values from `.dev.vars` or `.env`, and the pinned wrangler builds in a copy of the project that leaves them out, with an empty `--env-file`. A project's own wrangler builds in the project directory: there a wrangler too old for `--env-file`, or a build command of the project's own, can still read them. Whichever wrangler builds, they are never published as static assets: the adapter leaves `.git` and the files `.dev.vars`, `.dev.vars.*`, `.env` and `.env.*` (other than `*.example`) out of the assets.

## 6. Static assets

Assets are part of the package: each file with its path, hash, size and content type, stored by hash like modules, so an unchanged file is not stored again and a version's assets change only with its package hash. The adapter lists them as `wrangler dev` does, with the same ignore rules and content types, and in addition leaves out `.git` and the local value files `.dev.vars`, `.dev.vars.*`, `.env` and `.env.*` (`*.example` excepted).

The node serves them itself, without workerd or placement -- and, for a request at the ingress, without a lease -- in the order Cloudflare's asset routing uses: for `GET` and `HEAD`, the exact path, `index.html` for a path ending in `/`, `<path>.html` and `<path>/index.html` for the rest; where Cloudflare would redirect to an equivalent URL, the file is served directly. A hit gets `ETag` (the content hash) and, when the request has neither `Authorization` nor `Range`, `Cache-Control: public, max-age=0, must-revalidate`; a matching `If-None-Match` gets 304. A miss goes to the Worker; a site without a Worker, or a configuration that asks for it, falls back by `not_found_handling` (`none`, `404-page`, `single-page-application`). Methods other than `GET` and `HEAD` on paths the asset layer answers get 405. A browser navigation request (`Sec-Fetch-Mode: navigate`) falls back to the asset layer under the same compatibility flags as on Cloudflare (`assets_navigation_prefers_asset_serving`, `assets_navigation_has_no_effect`).

`assets.binding` gives the Worker an `ASSETS` Binding whose `fetch()` is answered by this version's assets; it starts no child call, so it does not draw on the subrequest budget. `run_worker_first` -- `true`, or path patterns with `!` exclusions -- sends matching requests to the Worker first. A Service Binding call to a Worker with assets goes through its asset layer too, unless it names an entry point, and is charged to the budget like any Binding call.

## 7. Differences from Cloudflare

| Item | Coworkerd | Why |
| --- | --- | --- |
| `request.cf` | Coworkerd's call metadata: `nodeId`, `workerId` as `tenant/Worker`, `versionId`, `invocationId` and others, 13 fields; `serverId`, the field's earlier name, still carries the same value as `nodeId`; no geography or TLS data | There is no source for that data inside a cluster |
| `request.url` | Always `http://`, with the host and without its port; the client's headers pass unchanged, and the node adds no client address of its own | The node's ingress rebuilds the request |
| Inbound WebSocket | Not supported: a Worker that answers with a WebSocket fails the request | The ingress relays HTTP only |
| `ctx.props` | The platform's (`{bindings, entrypoint}`); tenants cannot pass props through a Binding | Bindings carry the call tree's identity and budget |
| Service Bindings | Only to Workers of the same tenant; `fetch` only, no JS RPC | Authorisation across tenants and RPC across machines are not designed yet |
| Load failures | Refused at deployment by the trial load. At run time, before a version's first call in a process and again once its last check there is older than `placement.idleMs` (30 s), a load check refuses the call before it executes: `LOAD_FAILED` -- a missing entry point included --, `LEASE_EXPIRED` when the check runs out of time, `RESULT_UNCERTAIN` when the process dies during the check (`LOAD_FAILED`, `limit`, when the check was its culprit). The check confirms the version in a process, not one isolate, so a new isolate can load between checks without one (for example after an early eviction, after a call hit a limit, or after the Worker's limits changed); a load that fails then fails inside the dispatched call: `RESULT_UNCERTAIN`, or `CPU_LIMIT_EXCEEDED` / `MEMORY_LIMIT_EXCEEDED` when a limit ended it; `loadCheck` can turn the check off | A refusal before the code ran tells the caller that nothing executed |
| Failures after dispatch | Before the response started, `RESULT_UNCERTAIN`, or `CPU_LIMIT_EXCEEDED` / `MEMORY_LIMIT_EXCEEDED` when a limit ended the call; after it started, the response is cut | Part of the code may have run |
| `passThroughOnException` | After a throw, workerd sends the request on to the Worker's outbound, which is the node's egress service: it goes to the request's own URL like an outgoing `fetch`, and a refusal there answers `500`; with egress off the policy point refuses it (`EGRESS_DENIED: egress-off`) and the request fails on its own | Falls back to the origin |
| `ctx.waitUntil` | Work after the response completes; its Binding calls are refused once the call tree's deadline has passed (`LEASE_EXPIRED`) | The lease covers the call tree, not the work that follows it |
| Limits | CPU 200 ms per request and memory 128 MiB per isolate by default, adjustable per Worker within the tenant's quota | The CPU limit is also the bound of what one request can add to its neighbours' latency |
| Assets | No `html_handling` redirects (content is served directly), no compression, no `Range`, the route's prefix is not stripped from asset paths | Kept simple; the content is the same |
| Logs | `cowctl logs` from each node's per-tenant buffer, with quotas per tenant | No Workers Logs or Tail Workers |

## 8. How compatibility is checked

Real projects are deployed without changing their code, through the wrangler adapter `cowctl` uses, and their answers compared with `wrangler dev` (local workerd) for the same requests; Cloudflare's official templates are converted as they are: 3 of 41 convert with their configuration unchanged, and 11 in all when the templates' own build runs first; the others fail on entries this document lists as refused -- stateful and cloud services most of all -- or on their own build, not on the converter. The release package's `examples/` holds two of them as they are, `email-routing-template` (static assets and the `ASSETS` Binding) and `x402-proxy-template` (a secret).

# Cloudflare Worker ecosystem compatibility

> Whether existing applications packaged with wrangler, apart from those that depend on Cloudflare's cloud services, can run on Coworkerd without changing their code.

- **Status**: phase one **implemented**, results in section 9; static assets, secrets (phase one) and trial loads at deployment are implemented as well (sections 13-20)
- **Why**: an important reason for choosing workerd is reusing the Cloudflare Worker ecosystem -- build toolchains such as wrangler, and existing applications

## 1. Single-file versions (`format: "v1"`)

The single-file form of a version (version definitions v1, see section 4), from which this design starts:

| Aspect | Single-file versions |
| --- | --- |
| Modules | A version can only be **a single JS file** (an ES module), stored in object storage as `bundles/<content hash>` |
| Compatibility date and flags | One compatibility date for the whole deployment manifest; the flags are fixed by the runtime to `enable_request_signal`, and tenants cannot set them |
| Environment | No environment variables, no secrets; `env` holds only Bindings |
| Entry points | A tenant's default export must be an object with `fetch`; a Binding can only call its target's default entry point, and only HTTP `fetch` |

How the runtime does it: the tenant's code is one module, imported by a **bridge module** we generate; the bridge module passes each call's Bindings (which change from call to call and carry the call tree's identity and budget)
in through `ctx.props`, and wraps the tenant's `fetch` in `withEnv`. A dynamically loaded Worker's own `env` is empty.

## 2. What workerd's dynamic loading (the Worker Loader) can do

Against the source of workerd `v1.20260827.1` (`src/workerd/api/worker-loader.h`):

| Capability | Support |
| --- | --- |
| Module types | ES (`js`), CommonJS (`cjs`), text (`text`), binary (`data`), JSON (`json`), wasm, Python (`py`) |
| Compatibility date and flags | **Set per** dynamically loaded Worker, checked at loading by `compileCompatibilityFlags`: unknown flags and dates later than this version's maximum (2026-09-03) are errors; experimental flags require the caller itself to carry the `experimental` flag -- our runtime does not, so tenants cannot turn experimental features on, as intended |
| Environment | `env`: any value that can be serialised over RPC, **fixed at loading** |
| Entry points | `getEntrypoint(name, {props})`: named entry points; `getDurableObjectClass`: Durable Object classes |
| Resource limits | `limits`: `cpuMs`, `subRequests`. **The interface exists, but stock workerd's limit enforcer is an empty implementation** (`NullIsolateLimitEnforcer`) and enforces nothing; see the limits in [process isolation and resource governance](process-isolation.md) |
| Egress and tail | `globalOutbound`, `tails` |

**Conclusion: the gaps are in our own package format, deployment state and runtime bridge, not in workerd.**

## 3. The compatibility scope

| Item | Scope | Reason / notes |
| --- | --- | --- |
| Multiple modules: ES, CommonJS, text, binary, JSON, wasm | **Phase one** | Supported natively by the Worker Loader; common in wrangler's output |
| Python Workers | Not for now | A different toolchain and ecosystem; to be discussed separately |
| Compatibility date | **Phase one**, carried by each version | Single-file versions have one global date (section 1) |
| Compatibility flags (including `nodejs_compat`) | **Phase one**: the non-experimental flags this workerd version knows | Experimental flags are not opened (the Worker Loader itself does not allow them either); see section 6 |
| Environment variables (plain text and JSON) | **Phase one** | They go into the version definition, locked by the definition hash |
| Secrets | Phase two | They need a storage and distribution design, and must not go into etcd in plain text |
| Entry points: the default export object's `fetch`; class-style `WorkerEntrypoint` (default export) | **Phase one** | |
| Named entry points (a Binding naming its target's entry point) | **Phase one** (HTTP `fetch`) | The Bindings of single-file versions reach only the default entry point |
| JS RPC (calling class methods over a Service Binding) | Phase two | Cross-machine RPC semantics (passing capabilities and their lifetime) need a design of their own |
| Scheduled triggers (Cron) | Phase two | They need cluster-level scheduling: one trigger is executed once in the whole cluster |
| Static assets (Workers Static Assets) | Phase two | They need storage for assets and routing; the object storage for code can be reused |
| Durable Objects, KV, R2, D1, Queues and the like | **Not done** | They depend on Cloudflare's cloud services (explicitly excluded); Durable Objects also conflict with the premise of "stateless execution only" (see section 2 of [cluster state](cluster-state.md)) |
| `ctx.waitUntil`, `passThroughOnException` | Phase one **verifies and records their behaviour** | No promise to behave the same as Cloudflare; see section 7 |

## 4. Package format and version definitions

### A code bundle = a set of modules

- **A code bundle contains only modules**: each module has a name, a type (`esm`, `cjs`, `text`, `data`, `json`, `wasm`), a SHA-256 and a size in bytes.
  wrangler's (4.127.1) module rules have no JSON: imported JSON is inlined into JS by the bundler, so wrangler's output never has a separate JSON module;
  the `json` type is left for other tools and hand-built bundles;
- **The bundle manifest** is canonical JSON (fields and modules sorted by name, no extra whitespace), and **the bundle hash = the SHA-256 of the manifest's bytes**;
- Configuration (the main module, the compatibility date and flags, environment variables, Bindings) **does not go into the code bundle but into the version definition**. Reason: the same code can be deployed with another configuration without uploading it again;
  the definition hash covers both the bundle hash and the configuration, and the rules of version locking are unchanged.

### Version definitions v2 (`format: "v2"`)

On top of the single-file definitions v1 (section 1) they add: each version's own compatibility date, compatibility flags, environment variables, and the entry point names of Bindings; for a v2 definition the `contentHash` field is the bundle hash.
The definition's encoding takes its own version tag (`coworkerd-definition-v2`) and keeps the length-prefixed canonical encoding: set-like fields (flags, variables, Bindings) are sorted before encoding.

**v1 and v2 definitions coexist.** Single-file definitions stay valid; new deployments use v2.
Both the server and the runtime support both; they can be mixed in one call tree.

### Object storage and the local cache

| Key | Content |
| --- | --- |
| `modules/<SHA-256>` | Module bytes (v2 definitions) |
| `packages/<bundle hash>` | Bundle manifests (v2 definitions) |
| `definitions/<definition hash>` | Version definitions (v1 and v2) |
| `bundles/<content hash>` | Single-file code (v1 definitions) |

- The local cache stores **per module**: a module several versions share is stored once and fetched once; single flight per object;
- unverified bytes are never executed: a module read from disk or the network has its hash checked, and the runtime checks it once more before loading;
- prefetch: a newly activated version fetches its bundle manifest first, then the modules in it that the machine does not have.

## 5. The runtime bridge

- When the runtime fetches code from the server, for a v2 definition it gets **the bundle manifest's raw bytes and all the modules**; it first checks that the manifest's hash equals the bundle hash in the description, then checks each module's hash,
  then builds the `WorkerCode`: `modules` loaded by type, `compatibilityDate` and `compatibilityFlags` taken from the definition, `env` being the environment variables;
- **the compatibility flags = the definition's flags ∪ {`enable_request_signal`}**: the cancellation semantics depend on `Request.signal`, which the platform forces on; a tenant setting `disable_request_signal` is refused at deployment;
- **the bridge module is generated per version**: it imports the tenant's main module as a namespace and default-exports a dispatcher that takes the tenant's export by the entry point name this call names (`default` when none) --
  for a class (a `WorkerEntrypoint` subclass) each call constructs an instance with `(ctx, env)` and then calls `fetch`; for an object with `fetch` it calls `fetch(request, env, ctx)`.
  Both run inside `withEnv(env)`, so that `import { env } from "cloudflare:workers"` at a module's top level gets it too;
- the `env` here = the environment variables (fixed at loading) merged with this call's Bindings (which change from call to call, passed in through `ctx.props`). A name used by both is refused at deployment;
- named entry points: a Binding's entry point name goes with the `BoundWorker`'s props to the server, travels with the peer protocol across machines, and is finally written into the target call's description, where the bridge module dispatches by it.
  An entry point that does not exist in the target version -> an error at execution (the same as Cloudflare's runtime error); JS is not parsed at deployment;
- tenant module names must not start with `__coworkerd_` (reserved for the bridge module).

## 6. Deployment and validation

**Deploying is validating**: compiling is validating, and an invalid deployment changes no state. For bundles and version definitions:

| Item | Rule |
| --- | --- |
| Compatibility date | Not later than this workerd version's maximum (`maximum-compatibility-date.txt`; 2026-09-03 for `v1.20260827.1`) |
| Compatibility flags | Must be on the **allowed list**: generated from this workerd source's `compatibility-date.capnp` (the enable and disable names of every non-experimental flag), committed with each workerd version; `disable_request_signal` excepted |
| Modules | Safe names (no `..`, not starting with `/`, no reserved prefix); a supported type; the main module exists and is `esm` or `cjs` |
| Environment variables | Names are valid identifiers; at most 64, each no larger than 5 KiB (the same as Cloudflare); plain text or JSON; no name shared with a Binding |
| Size | One bundle no larger than 10 MiB in all (uncompressed; Cloudflare's 10 MB counts compressed, so this is stricter and simpler) |

### The wrangler adapter

- A separate small tool (Node; wrangler users have Node anyway) that **does no bundling of its own**: it calls the project's `wrangler deploy --dry-run --outdir` to produce the modules,
  and reads the configuration through wrangler's own exported interface (it does not parse TOML itself); the wrangler version is pinned and recorded;
- mapping: `main` and the output directory -> the code bundle; `compatibility_date`, `compatibility_flags`, `vars` -> the version definition; `services` (including `entrypoint`) -> Bindings;
- **unsupported configuration is always refused explicitly and listed** (KV, Durable Objects, R2, D1, Queues, static assets, scheduled triggers, secrets and so on), never silently ignored;
- it produces Coworkerd's bundle and deployment manifest, published by `coworkerd-server deploy`.

## 7. Deviations from Cloudflare (phase one, measured)

| Deviation | Measured behaviour | Reason / handling |
| --- | --- | --- |
| The platform forces `enable_request_signal` on | A tenant setting `disable_request_signal` is refused at deployment | The cancellation semantics depend on it |
| `request.cf` is Coworkerd's call metadata | Tenants see 12 fields such as `serverId`, `processId`, `workerId`, `versionId`, `invocationId`, `treeId` and `lease`, and none of Cloudflare's geographic information | There is no source for such data. Internal fields are exposed to tenants; phase two evaluates narrowing them |
| `ctx.props` is used by the platform to pass Bindings | Tenants cannot pass props of their own through a Binding | Phase one does not do caller props |
| Named entry points are HTTP `fetch` only | Calling a class method on a Binding throws `TypeError: The RPC receiver does not implement the method ...` | JS RPC is in phase two |
| Code loading failures surface only at call time | **There is a trial load at deployment**: a missing module, a syntax error, a top-level throw, a missing entry point, compatibility flags and exceeded limits are all refused at deployment (sections 19 and 20) | Eliminated (at deployment); a loading failure that appears only at run time is still reported as an uncertain result, to be refined with the call contract |
| A missing entry point, a throw after `passThroughOnException` | The caller gets 503 `RESULT_UNCERTAIN` (the latter is a reset connection -- there is no origin to fall back to) | Every failure after dispatch is reported as "result uncertain"; conservative for these two cases, where no business logic certainly ran |
| `ctx.waitUntil` | Background work after the response completes as usual; a Binding call it makes succeeds before the parent call's lease expires and gets 410 `LEASE_EXPIRED` after (certainly not executed). Background work does not count towards admission's in-flight number | Leases and admission cover only the request itself; governing background work is left to resource governance (see [process isolation and resource governance](process-isolation.md)) |
| Bundle size counted uncompressed | One bundle no larger than 10 MiB | Simple and predictable |

## 8. How phase one is checked

A few real projects packaged with wrangler -- at least one with several modules, one with wasm and one using `nodejs_compat`, plus one covering class entry points, named entry points and
environment variables -- are **deployed and run through the adapter without changing their code**; for the same set of requests, the responses must equal those of `wrangler dev` (local workerd).

## 9. Phase one: results

**Real projects**: 4 projects (5 Workers) deployed through the adapter to three Servers, 42 fixed requests per pass, three passes, each request entering at a random one of the three (ingress placement on);
**the responses' status codes, body bytes and `content-type` all equal those of `wrangler dev` (wrangler 4.127.1)**, and the projects' sources are byte-identical before and after:

| Project | Content | Source |
| --- | --- | --- |
| P1 multi-module | An extra ES module, a text module, a binary module; imported JSON inlined as wrangler does by default | A plain wrangler project written for this |
| P2 wasm | A separate `.wasm` module compiled from source by AssemblyScript 0.27.31 | Written for this |
| P3 `nodejs_compat` | Depends on `split2` (Node's stream API) | **workerd's official sample** `samples/nodejs-compat-streams-split2`, unchanged |
| P4 entry points and variables | A class-style default entry point, a named entry point called through a Service Binding, plain and JSON variables, a top-level `import { env }` | Written for this |

The adapter refuses to deploy a project that uses KV, R2 and scheduled triggers, and lists each of them.

**Limitation**: of the four projects only P3 is an existing project; P1, P2 and P4 were written for the purpose (reason: no suitable existing project was found). They cover every mechanism,
but are weak evidence that "applications written by others run"; existing open-source wrangler projects are covered by the scan of Cloudflare's official templates in section 11.

**Code bundles and distribution**: a module two versions share is fetched once per machine; when the same module is needed concurrently, the storage side reads it once; a corrupted cached module is detected, invalidated and fetched again;
with object storage offline, versions without a cache are refused explicitly, with zero executions; v1 and v2 definitions mixed in one call tree behave correctly.

**Version definitions v2**: changing the compatibility date, the flags, the variables, a Binding's entry point name or a module's content each changes the definition hash; the same version label with a different definition is refused;
in a consistency test with v2 definitions (about 1,200-1,300 requests per run, 20 switches of the active version), no call tree mixed two deployment states.

**Deployment validation**: one violating deployment per rule, all refused with a clear reason and no state changed. The flags' allowed list, regenerated by its generator script from workerd's source,
is byte-identical to the committed one (for `v1.20260827.1`: 290 flag names, experimental flags excluded; maximum date 2026-09-03).

**Runtime**: all six module types can be imported with the right content (native `json` verified with a hand-built bundle); the compatibility date takes effect (the same code behaves differently in `FormData` under an old and a new date);
without `nodejs_compat`, `node:buffer` is unavailable; environment variables are available both in `env` and through a top-level `import { env }`; class-style default entry points;
named entry points verified once on the local machine and once on another. The measured deviations are in section 7.

## 10. Phase two: order and positioning

Phase two covers secrets, JS RPC, scheduled triggers and static assets. **Its order, following the measurements of section 11: static assets -> secrets -> JS RPC -> scheduled triggers** (reasons there).
**The positioning**: do only what tenants on a multi-tenant platform really need; do not compete with celld on breadth in "moving an application off Cloudflare".
Static assets (sections 13-16) and secrets (sections 17 and 18) are implemented, and so is a trial load at deployment, turning code loading errors into refusals at deployment (sections 19 and 20); JS RPC and scheduled triggers remain.

## 11. A compatibility scan of real projects

The sample is all 41 Workers templates of Cloudflare's official template repository `cloudflare/templates` (pinned at commit `dfdf140`),
each converted offline with our adapter. This first scan, before the adapter fixes of section 12, found:

**Converted directly with the configuration as it is: 0/41.** All 41 templates' configurations have `"observability": {"enabled": true}` (Cloudflare's logging switch), which was not on the adapter's allowed list, so all were refused.
With the refused configuration entries removed, **40/41 convert**: 31 directly; 8 after running the framework's own build first (React Router, Astro, Remix); 1 after adding `node_modules/.bin` to the command path.
The remaining 1 (Next) is its own build tool failing on the scanning machine, unrelated to the adapter. The output's module lists equal those of wrangler's native dry-run for all 40.

**The gaps, ranked** (by the number of templates that need them):

| Class | Gaps |
| --- | --- |
| Already listed for phase two | **Static assets 25**, secrets 9, JS RPC 1, scheduled triggers **0** |
| Not done by the positioning: stateful services | Durable Objects 8, D1 7, KV 6, Containers 2, Queues 2, Workflows 2, R2 1 |
| Not done by the positioning: Cloudflare cloud services (conclusion 2 below) | Workers AI 7, dispatch namespaces 3, Hyperdrive 2, and Email, Access and Rate Limiting with 1 each |
| Platform configuration entries (conclusion 1 below) | observability 41, Smart Placement 4, workers.dev 3, and `routes` and `env` with 1 each |
| The adapter itself | Framework build needed first 8, the command path of a custom build 1 |

The compatibility dates are all within the allowed range; of the compatibility flags used, only one template's `experimental` is not on the allowed list (which never opens it); 23/41 need `nodejs_compat`.
The output's module count has a median of 1 and a maximum of 3; its size a median of about 120 KB and a maximum of about 4.9 MB.

**Conclusions**:

1. **Platform configuration entries are "accepted, ignored and reported explicitly"**: `observability`, `placement`, `workers_dev` and `preview_urls` are the Cloudflare platform's own logging, scheduling and URL settings,
   and do not change the code's behaviour. Refusing them would leave every template unconvertible. They are accepted, but listed explicitly as "ignored configuration entries" in the conversion result -- unlike celld, which accepts them silently.
   `routes` and `env` have real semantics and are still refused, to be mapped when needed;
2. **Cloudflare's cloud services are not done, by the positioning**: Workers AI, Hyperdrive, Email, dispatch namespaces, Access and Rate Limiting depend on Cloudflare's cloud services,
   in line with "apart from those that depend on Cloudflare's cloud services" at the top of this page, and are handled like stateful services;
3. **Phase two's order: static assets -> secrets -> JS RPC -> scheduled triggers.** Static assets are the largest gap (25/41), and not one official template uses scheduled triggers.

**Four problems in the adapter** (not gaps of the templates, but they blocked conversion; section 12 describes how they were fixed):

- It did not follow the Vite plugin's configuration redirect: the adapter always passed `--config` explicitly, so wrangler did not read `.wrangler/deploy/config.json`, and framework templates could only run their own build first;
- the pinned wrangler 4.127.1 lagged behind the 4.136.1 the templates lock, and failed outright reading the newer form of the Containers configuration;
- **the conversion's result depended on where the adapter is installed**: wrangler bundles polyfills from the adapter's own install directory and writes relative paths into the output. The same project converted from different directories gave different output bytes and bundle hashes (20 of the 40).
  Code bundles are addressed by content hash, so this had to be fixed;
- the commands of a custom build were looked up on the caller's command path, without the project's own `node_modules/.bin` on it.


## 12. Adapter polishing: results

The adapter was fixed for the four problems of section 11, and the same 41 official templates were scanned again.

**Results**:

- **Converted directly with the configuration as it is: 1/41** (0/41 in section 11). The other 40 fail for only two kinds of reasons: 37 have unsupported entries with real values (static assets 24, Durable Objects 8, Workers AI 7, D1 7 and so on),
  and 3 need the template's own build first (React Router). There is no third kind -- what stops the official templates now are real feature gaps, no longer the adapter.
- **Converted at some layer: 41/41** (40/41 in section 11): 1 as it is, 31 with the refused keys removed, 9 after the template's build and with the refused keys removed. The adapter now follows the configuration redirect the Vite plugin writes by itself.
- Compared with the native dry-run of the wrangler each template locks, the modules of all 41 templates are **byte-identical**.
- **The output is independent of location**: two adapter installs × two copies of a project, each template's 4 conversions give byte-identical modules, bundle manifests and deployment manifests (41/41); the same holds when the project has no wrangler of its own (41/41).
- The ignored entries (observability 41, placement 4, workers_dev 3, an empty assets 1) are each written into `adapter.json` and the command line's output; converting again with them removed leaves the deployment manifest and the bundle hash unchanged, 41/41.
- With the pinned wrangler at 4.136.1, the 5 Workers of section 9 give the same bytes and the same bundle hashes as with 4.127.1; the refused projects are still refused for their original reasons.

**How it is done**: when the project has wrangler installed, it builds in place with it (the same as native); when not, it uses the adapter's pinned version, copying the project into a temporary directory and building it with the adapter's dependencies at a fixed relative position,
without changing the caller's project directory. Limitation: the real configuration must be inside the project directory, and a build referring to files outside it fails. The project's own `node_modules/.bin` goes first on a custom build's command path.

**Conclusions**:

1. In configuration read through a redirect, the fields wrangler writes when normalising (`configPath`, `userConfigPath`, `topLevelName`) and fields equal to wrangler's defaults are likewise accepted, ignored and reported explicitly;
   in configuration the user wrote they are still refused. These fields are written by the build tool and do not express the user's intent; refusing them would leave following a redirect forever unconvertible;
2. `wrangler` in the adapter's record is the version the build actually used.

The ranking of the gaps is unchanged, and phase two's order remains: static assets -> secrets -> JS RPC -> scheduled triggers.

## 13. Static assets phase one: design

**Why first**: the scans of sections 11 and 12 found that the first reason official templates fail to convert is static assets (24/41 need them), and 8 templates lack only this.

**Scope**: in wrangler's `assets` configuration, `directory`, `html_handling` (only the default value accepted) and `not_found_handling` (all three values supported).
`binding` (a Worker fetching assets through `env.ASSETS.fetch()`) and `run_worker_first` are left for phase two: they require the Worker to call back into the assets layer, which needs a new form of Binding.

**Package format**: the code bundle's manifest gains an `assets` list -- each item with a `path` (starting with `/`, normalised, without `..`), `sha256`, `bytes` and `contentType`, sorted by `path`;
asset bytes are stored by hash as `modules/<SHA-256>` like modules (one copy per file, fetched once; files unchanged between versions are not uploaded again). The bundle hash covers the asset list, so changed assets make a new version, and the rules of version locking are unchanged.
Limits: no more than 20,000 files, a single file no larger than 25 MiB (the same as Cloudflare), assets no larger than 100 MiB in all (phase one's conservative value, configurable); code modules are still bound by the 10 MiB bundle limit, counted separately from assets.

**Serving**: the server serves them directly at the ingress, not through workerd, taking no call lease and no placement:

1. After the route has selected the Worker, look up assets only for GET / HEAD: first match the request path exactly; for a path ending in `/`, take `index.html`; for a path without an extension, try `<path>.html` and then `<path>/index.html`;
2. a hit: 200, `Content-Type` by the extension from a fixed table (`application/octet-stream` when unknown), `Content-Length`, `ETag` (the content hash), `Cache-Control: public, max-age=0, must-revalidate`;
   a matching `If-None-Match` returns 304; HEAD gives only the headers; the bytes are read from the local module cache (fetched by hash the first time they are used, single flight);
3. a miss: when the version has a Worker (`main`), hand over to the Worker, as Cloudflare does; an assets-only site without a Worker follows `not_found_handling`: `none` -> 404; `404-page` -> the nearest `404.html` going up the directories, status 404;
   `single-page-application` -> `/index.html`, status 200;
4. other methods: (following measurements of Cloudflare's actual behaviour, section 14) on every path the assets layer answers (a hit or a not_found fallback), anything other than GET / HEAD gets 405, with or without a Worker; on paths the assets layer does not answer, a site with a Worker hands over to the Worker, and an assets-only site gives 404.

**Deviations from Cloudflare (phase one)**: no redirects of `html_handling` (`/foo.html` and `/foo`, with or without a trailing slash, are all served directly, no 308); no compression; no Range support; no `ASSETS` Binding and no `run_worker_first`;
a route's `pathPrefix` is not stripped from the asset path (the Worker also sees the full path). The comparison is with `wrangler dev` (local workerd implements Cloudflare's asset routing), listing each difference.

## 14. Static assets phase one: results

**Scanning the official templates again** (the same pinned commit, the same 41):

- **Converted directly with the configuration as it is: 2/41** (1/41 in section 12); **6 more convert with the configuration as it is after running the template's own build first, 8/41 in all** (vite-react, react-starter, remix-starter and three React Router templates, with their configuration unchanged);
- of the 8 counted as "lacking only `assets`" (section 13), 4 now convert and 4 declare phase two's sub-entries (`assets.binding` or `run_worker_first`) -- that count was by top-level keys and did not split `assets` into its sub-entries. Split, the remaining refusal reasons include `assets.binding` 11 times and `run_worker_first` 6 times, now **the largest single gaps**;
- 26 templates convert with assets, 283 files, 17.6 MB, exactly the file sets wrangler itself uploads, 26/26 (server code is never exposed as an asset);
- the 2 that do not convert both fail in the template's own build (next-starter's OpenNext build reports an SQLite error on the scanning machine; workflows-starter's assets directory is a build output its `check` script does not produce).

**Compared with `wrangler dev`**: 5 sites, 61 requests × 3 Servers -- 49 identical in status, `Content-Type` and body; 12 are paths where wrangler dev answers 307, and the content we serve directly is exactly what it serves after following the redirect (the deviation declared in section 13); other differences 0.
The three Servers' answers are identical. One more Cloudflare behaviour outside section 13 was found: for a site with a Worker and an SPA fallback, a missed path in a browser navigation request (`Sec-Fetch-Mode: navigate`) gets `index.html` from Cloudflare, while phase one hands it to the Worker (phase two follows Cloudflare here, section 15).

**Caching, consistency and updates**: ETags and conditional requests, 150 all correct; the three Servers' bytes identical; a new version with 3 files changed and 2 added uploads only 7 objects (5 files + the definition + the manifest), and of 4,179 responses during the switch not one mixes old and new, each Server monotonic;
a 20 MiB asset takes 197-260 ms on its first request (including the fetch) and about 100 ms after (moving 20 MiB through Docker's port forwarding is itself on the order of a hundred milliseconds); exceeding the limits is refused with etcd unchanged; with object storage unreachable or the cached bytes corrupted, a clear 503 is returned, and the assets are fetched again after recovery.

**Overhead**: a 4 KB asset at p50 0.36 ms / p99 1.41 ms, about 0.4 / 0.6 times an ordinary Worker call on the same machine; a Worker with assets cannot be told apart on its Worker path from one without. Bundles without assets keep their manifest bytes and bundle hashes (the 5 Workers of section 9, 5/5).

**Conclusions**:

1. The 405 semantics for anything other than GET / HEAD follow Cloudflare's actual behaviour (item 4 of section 13);
2. the SPA fallback for navigation requests, the `ASSETS` Binding and `run_worker_first` together make up **static assets phase two** (section 15); in phase one, calling a Worker with assets through a Binding bypasses the assets layer, which phase two takes up too;
3. the server rereading the code bundle's manifest on every request goes into phase two as well: bundles without assets also use the per-hash manifest index built for assets.

## 15. Static assets phase two: design

**Why**: after phase one, the largest single refusal reasons of official templates are `assets.binding` (11) and `run_worker_first` (6); also, the fallback of an SPA site with a Worker for browser navigation requests differs from Cloudflare (section 14). All three concern "the relation between a Worker and the assets layer", so they are done together.

| Item | Design |
| --- | --- |
| The `ASSETS` Binding | The Binding named by `assets.binding` points to **this version's assets layer**: the response a Worker gets from `env.ASSETS.fetch(request)` equals what the same request gets going straight from the ingress to the assets layer (including `not_found_handling`'s fallback, ETag and 304, and 405). It is implemented as a new kind of Binding target -- dispatched to no Worker, answered directly by the server's assets layer; a Binding in the definition gains a "points to the assets layer" mark, encoded only when used, so definitions without it keep their hash |
| `run_worker_first` | `true`: every request goes to the Worker first (assets only through `ASSETS`); a list of path patterns (with `!` exclusions): matching paths go to the Worker first, the rest look up assets first as in phase one. The pattern syntax and matching rules follow Cloudflare's implementation |
| The fallback for navigation requests | For a site with a Worker and `not_found_handling` configured: when an asset misses and the request is a browser navigation (`Sec-Fetch-Mode: navigate`), fall back by `not_found_handling` instead of handing over to the Worker. By Cloudflare's compatibility flags: on by default from compatibility date 2025-04-01 (`assets_navigation_prefers_asset_serving`), off with `assets_navigation_has_no_effect` |
| Calling a Worker with assets through a Binding | The same as at the ingress: through the assets layer first (`run_worker_first` applies likewise), then to the Worker |
| Bundle manifests resident in memory | Without them, the server rereads the code bundle's manifest file and recomputes its hash on every request. Phase one built a per-bundle-hash manifest index for bundles with assets; phase two makes every bundle use it: under normal load the server's opens of bundle manifest files do not grow with requests |

**Deviations from Cloudflare (still kept after phase two)**: no redirects of `html_handling`, no compression, no Range; `_redirects` and `_headers` with rules are still refused.

The comparison is with `wrangler dev` (which implements all of the above), listing each difference; scanning the official templates again shows what remains once `assets.binding` and `run_worker_first` leave the refusal reasons.

## 16. Static assets phase two: results

- **Official templates**: 3/41 convert directly as they are, 11/41 as they are after building first (2 / 8 in section 14); `assets.binding` and `run_worker_first` are 0 among the refusal reasons. What still does not convert is stateful services, cloud services (which we do not do) or the templates' own build problems. Within our positioning, ecosystem compatibility has largely taken what it can.
- **Compared with `wrangler dev`**: 13 sites, 324 requests × 3 -- 256 identical, 68 are paths where it answers 307 and we serve the same content directly, differences 0; of them, all 45 navigation requests are identical (the two templates that differed in section 14 are now identical), and all 16 through the ASSETS Binding are identical.
- **Calling a Worker with assets through a Binding**: 64/64 as expected; the assets are answered by the Server where the caller is, and the cross-machine forwarding to the Worker holds; named entry points do not go through the assets layer (as in wrangler).
- **Bundle manifests resident in memory**: the server opens a manifest 0 times per request (1.0 times without them); after a version change the old manifest is no longer read; entries capped at 128, evicted least recently used first.
- **Hashes unchanged**: definitions without an ASSETS Binding keep their hashes, 5/5 + 5/5 + 3/3; bundles without assets keep their hashes.

**Conclusions**:

1. The ASSETS Binding does not draw on the subrequest budget (it reads only this version's assets, makes no subcall and does not go through workerd; an expired parent lease still gives 410 and the version lock is still checked); calling a Worker with assets through a Service Binding draws on the budget as before;
2. named entry points do not go through the assets layer, and an assets-only site cannot be a Binding target -- both following wrangler's actual behaviour, and section 15 is to be read accordingly;
3. deviations still kept after phase two: `html_handling`'s redirects, compression, Range, `_redirects`/`_headers`, `/cdn-cgi/` and image abuse protection.

## 17. Secrets: design

**Why**: the scan of section 11 found that 9 templates need secrets (API keys, signing keys and the like); in the ecosystem's phase two this comes after static assets. Cloudflare's model: secrets are not written in the configuration but set separately with `wrangler secret put`, a Worker reads them as strings in `env.NAME`, and once set no interface shows them again.

**Phase one's scope**:

| Item | Design |
| --- | --- |
| Declaration | The version definition lists the secrets' **names** (not their values): the definition hash covers the names, not the values. The names do not clash with environment variables or Bindings. The adapter takes the names from the project's `.dev.vars.example` (when present) and from `--secret NAME` arguments, and **never reads values** |
| Storage | Each Worker's secret values go into etcd at `secrets/<workerId>/<name>`, written by the deployment tool's `secret put <worker> <name>` reading from standard input (no command line argument, to keep them out of shell history); `secret delete`; `secret list` lists only names. Values are stored in plain text, relying on the boundary that etcd is not open to tenants -- encrypted storage and secret management are scheduled separately as hardening before production |
| Validity | A Worker is valid only when every secret its version declares has a value (one more rule of per-Worker validity); a Worker missing a secret returns an explicit refusal whose reason names only the name |
| Delivery to the Worker | Handed to the runtime with the definition at loading and merged into `env` (the same channel as environment variables); after a value is updated, newly created isolates use the new value, and loaded ones keep the old value until they are reclaimed (phase one does not invalidate actively) |
| Where they never appear | The state endpoint, the deployment endpoint, `deploy show`, the adapter's output, the server's and workerd's logs -- the value appears nowhere; the tool's and the server's logs write only names and lengths |
| Across machines | Secret values do not travel with a `RemoteInvocation`: the Server where the call executes takes the value from its own deployment state (by its local deployment view, like definitions), and the version lock locks only the definition |

**Compared with `wrangler dev`**: wrangler injects `.dev.vars` values into `env` locally, we inject them through `secret put`, and the same set of requests gets the same responses.

## 18. Secrets phase one: results

- **Use**: a Worker with two secrets reads the right values on all three machines, 18/18 (comparing only hashes and lengths); the same as `wrangler dev` (the same values injected through `.dev.vars`), 5/5; the official x402-proxy template gets the names through the adapter from `.dev.vars.example`, and with random values set verifies JWTs with them, 3/3.
- **Updating values**: loaded isolates keep the old value, and new isolates (first load and after reclamation) read the new one; the definition hash, the version lock and the valid set are all unchanged (changing only a value recomputes no Worker).
- **Across machines**: values do not travel with a `RemoteInvocation`; the Server where the call executes takes them from its own deployment view; a lagging view is refused explicitly (the refusal code `SECRET_UNAVAILABLE`, "not executed"), and the same call succeeds once the view catches up.
- **Missing secrets and mistakes**: an upsert declaring a secret that is not set is refused; deleting a value bypassing the tool makes only that Worker and the Workers bound to it invalid (validity is per Worker), while other Workers deploy and serve as usual; clashing or invalid names refused 10/10; `secret put` with the value as a command line argument refused 3/3.
- **Values do not leak**: after all cases ran with 29 random test values (including a tracer value), searching the three machines' logs, 74 raw copies of the state and deployment endpoints, the tool's and the adapter's output, the working directories, the containers' writable layers and the object storage's data directory, in three alignments (as is, uppercase and base64), found them **0 times**; the tracer value is only in its secret key in etcd. Counter-evidence that the adapter "never reads `.dev.vars`": with a `.dev.vars`/`.env` of mode 000 in the project, conversion succeeds as usual, where an adapter reading them would fail.
- **Hashes**: definitions without secrets keep their hashes, 5/5 for the Workers of section 9 and 12/13 for the template sites of section 16 -- the different one is astro-blog, found to be Astro's build itself not being reproducible (it writes a differently named manifest each time); the same build gives the same bundle hash with the adapter with and without secret support.

**Conclusions**:

1. The key's value is `{"value","setAtUnixMs"}` (`secret list` must give the time it was set, and etcd does not record times); section 17 is to be read accordingly;
2. the refusal code `SECRET_UNAVAILABLE` (the executing side's view lacks the value): like `WORKER_INVALID`, it is an explicit "not executed" refusal;
3. the adapter keeps wrangler's build from reading `.env` (an empty `--env-file`, and the staged copy does not copy `.dev.vars*` or `.env*`); the cost is that projects configuring their custom build command through `.env` must give the configuration otherwise (none of the 41 templates does);
4. values also remain in etcd's revision history, in plain text -- within phase one's boundary of "relying on etcd not being open to tenants"; **encrypted storage and history compaction go onto the hardening list before production**.

## 19. Trial loads at deployment: design

**Why**: the deviation in section 7 -- without a trial load, code loading failures (a missing module, a syntax error, a throw at a module's top level, importing `node:*` without `nodejs_compat`) surface only at the first call, and are reported as "result uncertain";
Cloudflare refuses them at upload. Deployment is a rare, watched moment, and an error is cheapest to surface there.

**How**: after "publishing the immutable parts" and before "flipping the pointer", the deployment tool asks a Server to **trial-load** each new version; if loading fails, the deployment is refused and not flipped.

| Item | Design |
| --- | --- |
| Who tries | The deployment tool picks a live Server from etcd's members (or one given with `--trial-server`) and calls its `POST /__coworkerd/trial-load` with `workerId`, `versionId` and `definitionHash`. The Server gets the definition and the code through its own deployment view and object storage (the definition key is already published at this point) |
| How | Through the normal dispatch path to choose a process, send one probe call that **loads without executing**: the runtime loads the modules (imports, the compatibility date and flags, `env` including secrets) and confirms the entry point's export exists (`fetch` of `default`, or a class), **without calling `fetch`**. The CPU limit and the memory limit apply as usual: an endless loop at the top level is terminated and a large top-level allocation refused, both counting as loading failures. Timeout 5 seconds |
| Result | Success: the tool goes on to flip, and its output records the trial load's duration and Server. Failure: the tool refuses without flipping, printing the category (missing module / syntax error / top-level throw / missing entry point / compatibility flags / limits exceeded / timeout) and a cleaned error message (no code, no secrets); the published definitions and objects stay (immutable, harmless) |
| Switch | On by default; off with `--no-trial` (for example an initial deployment to a cluster without members). `apply` likewise |
| Accounting | The probe call does not count towards the tenant's requests or subrequest budget; the state endpoint lists `trialLoads` and `trialFailures` separately; the isolate it loads is left for later requests (the same definition) |
| The deviation entry of section 7 | It becomes "eliminated" (at deployment); "every loading failure at run time is an uncertain result" is still left to a later refinement of the call contract |

## 20. Trial loads at deployment: results

- **Bad code refused at deployment, 10/10**: a missing module, a syntax error, a top-level throw, a missing entry point (two kinds), `node:*` without `nodejs_compat`, an endless loop at the top level (CPU limit exceeded), a large top-level ArrayBuffer (memory limit exceeded), invalid wasm, and a single large allocation V8 cannot stop midway (a fatal process OOM, with the culprit identified as this probe) -- categorised correctly, with etcd's pointers, routes and secrets unchanged key for key, and the active version serving as usual on all three machines; absolute paths in the messages replaced by `<path>`, secret values by `[redacted]`. **Control** (without trial loads): the same 10/10 deployed successfully, and the first call got 503.
- **Usable code**: the 5 Workers of section 9 and 3 template sites of section 16 trial-load 8/8 successfully and flip; the trial load happens on the Worker's Home, and the isolate is reused by the first request that follows (that request adds no cold load); a Worker with secrets has them in `env` at its trial load, and an unset secret is refused by the upsert before the trial load.
- **The probe does not count towards the tenant's requests or subrequest budget**: no Bindings, a lease budget of 0, no execution record, no placement decision; the state endpoint lists `trialLoads` and `trialFailures` separately.
- **The tool's behaviour**: with no live member it fails with a hint (pointing to `--no-trial`, for initial deployments); `--trial-server` to choose; expiry during loading is refused as `TIMEOUT`, leaving no probe in flight. A top-level `await` on a Promise that never settles is refused at loading in this workerd (its own category, `TOP_LEVEL_AWAIT`), never timing out.

**Conclusions**:

1. **The p99 of neighbours in the same process**: with 20 deployments squeezed into 2 seconds, the neighbours' p99 is about 5.6 ms higher. The cost is each new version's one cold load occupying the JS thread, which the trial load does not add to but moves from "the first request" to deployment; without trial loads the neighbours fare worse at the first request (p99 18.2 against 13.0 ms). The design stays as it is.
2. The entry "code loading failures surface only at call time" in section 7's deviation table is eliminated (at deployment); "every loading failure at run time is an uncertain result" is still left to a later refinement of the call contract.
3. **A problem to fix soon**: on the call path at run time, the 503 for a loading failure relays the startup error as it is -- if the message of a top-level throw carries a secret value, it appears in the 503's `reason` (seen in the control group). With trial loads such versions are not deployed, but deployments with `--no-trial` and errors thrown only at run time are still relayed. **It goes with the refinement of the call contract (loading failures becoming explicit refusals), with the message cleaned the same way.**

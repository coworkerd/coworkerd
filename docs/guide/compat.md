# Coworkerd compatibility scope: which features of a wrangler project work

> For release 0.4.0. Compiled from [compatibility with Cloudflare Workers](https://github.com/coworkerd/coworkerd/blob/main/docs/compatibility.md).
> `cowctl deploy` uses wrangler to read the configuration and build the project, then converts it into a Coworkerd code bundle; unsupported configuration keys are listed together at conversion and refused -- rule files in the asset directory after the build, compatibility dates and flags at upload --, never silently ignored.

## 1. What works

| wrangler configuration / feature | Notes |
| --- | --- |
| `name`, `main` | The Worker name (the name within the tenant, full name `tenant/name`; lowercase letters, digits and `-`, at most 63 characters) and the main module; `main` may be TypeScript, built by wrangler |
| `compatibility_date` | Carried by each version; may not be later than the maximum of the bundled workerd (`v1.20261012.1`), 2026-10-19 |
| `compatibility_flags` | The non-experimental flags this workerd knows, including `nodejs_compat`; the platform always turns on `enable_request_signal`, and `disable_request_signal` is refused |
| `vars` | Plain-text and JSON environment variables, part of the version definition; at most 64, each at most 5 KiB |
| Secrets | Names come from the project's `.dev.vars.example` (only the names are read) or `deploy --secret NAME`; values are set with `cowctl secret put`, stored in the cluster, and read in the Worker as `env.NAME`; at most 64, each at most 5 KiB of UTF-8 text without NUL. `cowctl` never reads `.dev.vars` or `.env` and never publishes them; a project with its own wrangler builds in place, where a wrangler too old for `--env-file`, or the project's own build command, can still read them; wrangler's own `secrets` key (`secrets.required`) is refused at conversion (section 2) |
| `services` (Service Binding, optionally with `entrypoint`) | Points to another Worker of **the same tenant** (`service` is the name within the tenant), and can run on another node; HTTP `fetch` only; Bindings across tenants are not supported |
| `assets` | Static assets: `directory`, `not_found_handling` (`none`, `404-page`, `single-page-application`), `binding` (`env.ASSETS.fetch()`), `run_worker_first` (`true` or path rules); `html_handling` accepts only the default value |
| Build-related: `build` (`command`, `cwd`, `watch_dir`), `base_dir`, `rules`, `no_bundle`, `find_additional_modules`, `preserve_file_names`, `minify`, `tsconfig`, `jsx_factory`, `jsx_fragment`, `alias`, `define`, `keep_names`, `upload_source_maps`, `send_metrics`, `$schema` | Take part in the build as wrangler means them, except that `send_metrics` and `$schema` change nothing (`cowctl` runs wrangler with metrics off) and source maps are never uploaded (`upload_source_maps` has no effect: the adapter drops wrangler's `.map` output) |
| Module types | ES modules, CommonJS, text, binary data and WebAssembly (wrangler's module rules); JSON a Worker imports is bundled into its JavaScript by wrangler |
| Entry points | A default export of an object with `fetch`, or a class extending `WorkerEntrypoint`; named entry points (the `entrypoint` of a Service Binding) only up to `fetch` |
| `ctx.waitUntil` | Background work after the response completes as usual; Binding calls inside it are refused after this call's deadline (`LEASE_EXPIRED`) |
| `import { env } from "cloudflare:workers"` at the module's top level | Gets the variables and secrets |

**Accepted, ignored, with a notice** (Cloudflare's own platform settings, which do not change what the code does): `observability`, `placement`, `workers_dev`, `preview_urls`,
and entries with empty values (`[]`, `{}` and the like, which build tools often write). In a configuration redirected by a build tool such as a Vite plugin, fields wrangler computes itself and fields equal to their defaults are ignored as well.

## 2. What does not work (refused at conversion, with the entries named)

| Category | Configuration entries |
| --- | --- |
| Stateful and cloud services (this platform does not provide them) | `kv_namespaces`, `durable_objects`, `migrations`, `r2_buckets`, `d1_databases`, `queues`, `hyperdrive`, `vectorize`, `ai`, `workflows`, `analytics_engine_datasets`, `browser`, `containers`, `dispatch_namespaces`, `send_email`, `mtls_certificates` and others |
| Ecosystem features not done yet | Scheduled triggers (`triggers`), JS RPC (calling class methods over a Service Binding), Tail Workers, email handlers (`email()` is never called) |
| Cloudflare's routing and environments | `routes`, `route`, `env` (Coworkerd's routes are decided by `cowctl deploy --route`: a hostname the administrator registered to the tenant, or `/<tenant>/...` under the cluster's default host) |
| Rule files among static assets | `_redirects` and `_headers` with rules |
| Other | Any top-level key not in section 1, and nested keys the adapter does not map (a `services` entry's other than `binding`, `service` and `entrypoint`, a `build` key other than `command`, `cwd` and `watch_dir`, an `assets` key not in section 1); non-default values of `assets.html_handling` |

## 3. Where behaviour differs from Cloudflare

| Item | This platform | Cloudflare |
| --- | --- | --- |
| Code that fails to load | Trial-loaded **at deployment**: missing modules, syntax errors, throwing at the top level, a missing entry point, compatibility flags and exceeding limits while loading are all refused at deployment, and the active version does not change | Refused at upload |
| Load failures that appear only at run time | Usually an explicit refusal before execution, 503 `LOAD_FAILED`, from the load check a node makes before a version's first call in a process (see [error codes](errors.md)); a load that fails between two checks fails inside the call (`RESULT_UNCERTAIN`, or the limit's code) | -- |
| `request.cf` | Coworkerd's call metadata (`nodeId`: the node running it, `workerId`: the full name `tenant/Worker`, `versionId`, `invocationId` and others), without geographic information. `serverId`, the field's earlier name, is a deprecated alias of `nodeId` (the same value), removed in 1.0 | Geography, TLS and other information |
| `ctx.props` | The platform uses it to pass Bindings; tenants cannot pass their own props through a Binding | Available |
| Static assets | Where `html_handling` would redirect, the content is returned directly (no 307/308); no compression; no Range; the route prefix is not removed from asset paths | Redirects, compression, Range |
| Bundle size | Code modules at most 10 MiB in total (**uncompressed**); assets at most 20,000 files, 25 MiB each, 100 MiB in total; the tenant's quotas (`packageBytes`, `assetFiles`, `assetBytes`) can be smaller | Counted compressed |
| CPU per request | 200 ms (the node's configuration; beyond it: 503 `CPU_LIMIT_EXCEEDED`). Worker-level limits are set through `/v1` and applied by the nodes per Worker, within the tenant's quota ([installing and running](install.md), end of section 5); a wrangler `limits` entry is refused at conversion | Set per Worker in the wrangler configuration (`limits.cpu_ms`) |
| Memory per isolate | 128 MiB (the node's configuration; beyond it: 503 `MEMORY_LIMIT_EXCEEDED`); Worker-level limits as above | 128 MB |
| Logs | `cowctl logs`: quotas are per **tenant**, each tenant keeps the latest 1 MiB on each node, at most 100 lines per second (the administrator can adjust both per tenant), 4 KB per line (longer lines are cut); lines over the per-second rate are counted as `[dropped]` and the buffer keeps the newest lines | Workers Logs, Tail |
| Throwing after `passThroughOnException` | The request is sent on to its own URL through the node's egress (a refusal there answers `500`; with egress off the policy point refuses it, `EGRESS_DENIED: egress-off`, and the request answers `500` as well) | Falls back to the origin |

## 4. Projects that were verified

Of the 41 official templates (`cloudflare/templates`, pinned commit `dfdf1409`), 3 convert directly with their configuration as it is, and 11 in total after running the template's own build first;
324 requests on 13 sites (6 of these templates and 7 test sites) compared with `wrangler dev` showed no difference besides the declared one: 68 answers where `wrangler dev` redirects (307) come back directly with the same content; `x402-proxy-template` takes its secret's name from `.dev.vars.example`, and with a value set by the operations tool's `secret put` it verified a JWT signed with that value on each of three nodes. The release package's `examples/`
holds two of them: `email-routing-template` and `x402-proxy-template`. The other templates fail to convert because of the entries in section 2 (stateful services most of all) or build problems of the templates themselves.

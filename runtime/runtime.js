import { WorkerEntrypoint } from "cloudflare:workers";

const SHA256_HEX = /^[0-9a-f]{64}$/;
const BRIDGE_MODULE = "__coworkerd_entrypoint__.js";
const TENANT_MODULE = "__coworkerd_tenant__.js";
// Tenant logs: this process's nonce, written once on standard output before any tenant code runs
// ("[runtime-log-nonce] {...}", at the start of the first call this runtime receives); the prelude module, evaluated
// before a version's own modules, turns each console.debug/info/log/warn/error call of the tenant into
// one line "[tenant-log] <nonce> {"workerId","versionId","level","message"}". The Server attributes a
// line to a Worker only when it carries its process's nonce, which no tenant can read (it lives in
// module scope, never in a value or a function's text), and the message is JSON: no tenant output can
// pass for another Worker's line or break out of its own.
const PRELUDE_MODULE = "__coworkerd_console__.js";
// Chosen in the first call (workerd generates no random values in global scope), before anything else.
let LOG_NONCE = null;
function announceNonce(env) {
  if (LOG_NONCE !== null) return;
  LOG_NONCE = hex(crypto.getRandomValues(new Uint8Array(16)));
  console.log("[runtime-log-nonce] " + JSON.stringify({ processId: env.PROCESS_ID,
    processGeneration: Number(env.PROCESS_GENERATION), nonce: LOG_NONCE }));
}
// The prelude of one version: its identity is fixed per isolate (a loader key is one version).
// Arguments are formatted as console.log formats them in Node (format specifiers, then the rest,
// objects shown to depth 2); the original console.log stays available to the bridge only.
function preludeSource(descriptor) {
  const head = "[tenant-log] " + LOG_NONCE + " {\"workerId\":" + JSON.stringify(descriptor.workerId) +
    ",\"versionId\":" + JSON.stringify(descriptor.versionId) + ",\"level\":";
  return `const HEAD = ${JSON.stringify(head)};
const original = { debug: console.debug, info: console.info, log: console.log, warn: console.warn, error: console.error };
function show(value, depth) {
  switch (typeof value) {
    case "string": return depth ? JSON.stringify(value) : value;
    case "number": case "boolean": case "undefined": return String(value);
    case "bigint": return value + "n";
    case "symbol": return value.toString();
    case "function": return "[Function: " + (value.name || "(anonymous)") + "]";
  }
  if (value === null) return "null";
  if (value instanceof Error) return value.stack || (value.name + ": " + value.message);
  if (value instanceof Date) return isNaN(value) ? "Invalid Date" : value.toISOString();
  if (value instanceof RegExp) return String(value);
  if (depth > 2) return Array.isArray(value) ? "[Array]" : "[Object]";
  if (Array.isArray(value)) {
    const items = value.slice(0, 100).map(v => show(v, depth + 1));
    if (value.length > 100) items.push("... " + (value.length - 100) + " more items");
    return "[ " + items.join(", ") + " ]";
  }
  if (value instanceof Map) return "Map(" + value.size + ") { " + [...value].slice(0, 100).map(([k, v]) => show(k, depth + 1) + " => " + show(v, depth + 1)).join(", ") + " }";
  if (value instanceof Set) return "Set(" + value.size + ") { " + [...value].slice(0, 100).map(v => show(v, depth + 1)).join(", ") + " }";
  const keys = Object.keys(value);
  const items = keys.slice(0, 100).map(k => (/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(k) ? k : JSON.stringify(k)) + ": " + show(value[k], depth + 1));
  if (keys.length > 100) items.push("... " + (keys.length - 100) + " more items");
  return items.length ? "{ " + items.join(", ") + " }" : "{}";
}
function format(args) {
  let at = 0, out = "";
  if (typeof args[0] === "string") {
    at = 1;
    out = args[0].replace(/%[sdifjoOc%]/g, specifier => {
      if (specifier === "%%") return "%";
      if (at >= args.length) return specifier;
      const value = args[at++];
      switch (specifier) {
        case "%s": return typeof value === "string" ? value : show(value, 1);
        case "%d": return typeof value === "bigint" ? value + "n" : String(Number(value));
        case "%i": return typeof value === "bigint" ? value + "n" : String(parseInt(value));
        case "%f": return String(parseFloat(value));
        case "%j": try { return JSON.stringify(value); } catch { return "[Circular]"; }
        case "%c": return "";
        default: return show(value, 1);
      }
    });
  }
  for (; at < args.length; ++at) out += (out.length || at ? " " : "") + show(args[at], 0);
  return out;
}
for (const level of ["debug", "info", "log", "warn", "error"]) {
  const write = original[level];
  console[level] = function (...args) {
    let message;
    try { message = format(args); } catch { message = "[unformattable]"; }
    write.call(console, HEAD + JSON.stringify(level) + ',"message":' + JSON.stringify(message) + "}");
  };
}
export const __coworkerdLog = original.log.bind(console);
`;
}
// Outbound requests: a tenant Worker's global outbound is the Egress entrypoint below, bound to that Worker's
// identity when the loader loads it (the props come from this runtime, never from the tenant). It hands the request
// to the node's egress service (EGRESS, <stack>/run/egress.sock, the proxy form) with that identity and this process's
// egress token -- in this isolate's env only, which no tenant isolate can reach, as the log nonce is in this module
// only -- and the node checks, resolves, connects, meters and answers. A refusal comes back as a header and is thrown
// to the tenant as an Error: "EGRESS_DENIED: <rule>" or "EGRESS_FAILED: <class>". Redirects are the tenant's fetch's
// to follow: each hop comes back here and is checked again. Without EGRESS (egress.mode off) a tenant has no network.
const EGRESS_SOURCE_HEADER = "x-coworkerd-egress-source";
const EGRESS_ERROR_HEADER = "x-coworkerd-egress-error";
export class Egress extends WorkerEntrypoint {
  async fetch(request) {
    const headers = new Headers(request.headers);
    headers.set(EGRESS_SOURCE_HEADER, this.env.EGRESS_TOKEN + " " + JSON.stringify(this.ctx.props));
    const response = await this.env.EGRESS.fetch(request.url, {
      method: request.method, headers, body: request.body, redirect: "manual",
    });
    const refused = response.headers.get(EGRESS_ERROR_HEADER);
    if (refused !== null) throw new Error(refused);
    return response;
  }
}
// The global outbound of a version's isolate: its identity as the Egress entrypoint's props (the Worker's full name,
// <tenant>/<worker>: the node takes the tenant from it), or no network.
function egressOutbound(descriptor, env, ctx) {
  if (!env.EGRESS || !ctx?.exports?.Egress) return null;
  return ctx.exports.Egress({ props: { workerId: descriptor.workerId, versionId: descriptor.versionId } });
}

// The props of a trial load. A v1 call's props are its Bindings by name, and
// ':' never appears in a Binding name; a v2 call's props are {bindings, entrypoint}.
const TRIAL_PROP = "coworkerd:trial";

// Part of both bridges: describes the tenant's exports for a trial load without
// calling any of them. Properties are inspected through their descriptors, so not
// even a getter runs; a class counts when it extends WorkerEntrypoint and has fetch.
const TRIAL_SOURCE = `
function __coworkerdHas(object, name) {
  for (let o = object; o !== null && (typeof o === "object" || typeof o === "function"); o = Object.getPrototypeOf(o)) {
    const property = Object.getOwnPropertyDescriptor(o, name);
    if (property) return typeof property.value === "function" || typeof property.get === "function";
  }
  return false;
}
function __coworkerdEntry(target, classes) {
  if (target === null || (typeof target !== "object" && typeof target !== "function")) return "unusable";
  if (classes && typeof target === "function" && WorkerEntrypoint.prototype.isPrototypeOf(target.prototype ?? null)) {
    return __coworkerdHas(target.prototype, "fetch") ? "class" : "unusable";
  }
  return __coworkerdHas(target, "fetch") ? "object" : "unusable";
}
// Whether a call naming this entrypoint gets as far as tenant code (v40's load check): the bridge's own
// checks before it calls the tenant, made from descriptors so that no getter runs, and as lenient as the
// bridge (a WorkerEntrypoint class may define fetch per instance). False only where the call would fail
// before any handler started.
function __coworkerdUsable(namespace, name, classes) {
  const target = Object.prototype.hasOwnProperty.call(namespace, name) ? namespace[name] : undefined;
  if (!target) return false;
  if (typeof target === "function") {
    if (classes && WorkerEntrypoint.prototype.isPrototypeOf(target.prototype ?? null)) return true;
    return __coworkerdHas(target, "fetch");
  }
  return typeof target === "object" && __coworkerdHas(target, "fetch");
}
function __coworkerdTrial(namespace, classes, requested) {
  const names = Object.keys(namespace);
  const hasDefault = names.includes("default");
  const named = names.filter(name => name !== "default" && __coworkerdEntry(namespace[name], classes) !== "unusable");
  const entrypoint = typeof requested === "string" && requested ? requested : "default";
  return Response.json({ ok: true, hasDefault, defaultKind: hasDefault ? __coworkerdEntry(namespace.default, classes) : null,
    namedEntrypoints: named.slice(0, 32), exportCount: names.length, entrypoint,
    entrypointUsable: __coworkerdUsable(namespace, entrypoint, classes) });
}
`;

const BRIDGE_SOURCE = `
import "./${PRELUDE_MODULE}";
import { withEnv, WorkerEntrypoint } from "cloudflare:workers";
import tenant from "./${TENANT_MODULE}";
${TRIAL_SOURCE}
export default {
  async fetch(request, _env, ctx) {
    const bindings = ctx.props;
    if (bindings === null || typeof bindings !== "object") {
      throw new Error("Missing trusted Coworkerd bindings");
    }
    if (bindings[${JSON.stringify(TRIAL_PROP)}] === true) return __coworkerdTrial({ default: tenant }, false, "default");
    return withEnv(bindings, () => tenant.fetch(request, bindings, ctx));
  },
};
`;

// A limit the descriptor names: absent, or a whole number of milliseconds or MiB that fits in the
// field the node sent it in. 0 means this Worker names none, and the process's own limit applies to it.
const limit = (value) =>
  value === undefined || (Number.isInteger(value) && value >= 0 && value <= 4294967295);

function descriptorFrom(request) {
  const value = request.cf;
  const remainingSubrequests = Number(value?.lease?.remainingSubrequests);
  const grant = Number(value?.lease?.grant);
  const orderSequence = Number(value?.orderSequence);
  const depth = Number(value?.depth);
  const expiresAtMonotonicMs = Number(value?.lease?.expiresAtMonotonicMs);
  if (
    value === null ||
    typeof value !== "object" ||
    typeof value.workerId !== "string" ||
    typeof value.versionId !== "string" ||
    typeof value.contentHash !== "string" ||
    !SHA256_HEX.test(value.contentHash) ||
    typeof value.definitionHash !== "string" ||
    !SHA256_HEX.test(value.definitionHash) ||
    typeof value.compatibilityDate !== "string" ||
    typeof value.mainModule !== "string" ||
    typeof value.invocationId !== "string" ||
    typeof value.treeId !== "string" ||
    !Array.isArray(value.bindings) ||
    value.lease === null ||
    typeof value.lease !== "object" ||
    !Number.isInteger(remainingSubrequests) ||
    remainingSubrequests < 0 ||
    remainingSubrequests > 65_535 ||
    !Number.isInteger(grant) ||
    grant < 0 ||
    grant > 65_535 ||
    !Number.isFinite(expiresAtMonotonicMs) ||
    expiresAtMonotonicMs <= 0 ||
    !Number.isInteger(orderSequence) ||
    orderSequence < 0 ||
    !Number.isInteger(depth) ||
    depth < 0 ||
    depth > 65_535 ||
    typeof value.requiresChildSequence !== "boolean" ||
    (value.loadCheck !== undefined && typeof value.loadCheck !== "boolean") ||
    (value.coldLoaded !== undefined && typeof value.coldLoaded !== "boolean") ||
    (value.sourceInvocationId !== undefined && typeof value.sourceInvocationId !== "string") ||
    !limit(value.cpuLimitMs) ||
    !limit(value.memoryLimitMiB)
  ) {
    throw new Error("Missing trusted Coworkerd invocation descriptor");
  }
  return {
    ...value,
    lease: { ...value.lease, remainingSubrequests, grant, expiresAtMonotonicMs },
    orderSequence,
    depth,
  };
}

const nextChildSequences = new Map();

function nextChildSequence(parentInvocationId) {
  const sequence = (nextChildSequences.get(parentInvocationId) ?? 0) + 1;
  nextChildSequences.set(parentInvocationId, sequence);
  return sequence;
}

function hex(bytes) {
  return [...new Uint8Array(bytes)]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
}

// Where a load failed inside our own code, for the load check's category: "package" (the code
// package or its verification), "decode" (a module or variable of the version that cannot be decoded) or
// "secrets"; the error itself carries the stage. Anything thrown without one is a package failure.
function staged(stage, error) {
  const value = error !== null && typeof error === "object" ? error : new Error(String(error));
  try { value.coworkerdStage = stage; } catch {}
  return value;
}
const stageOf = (error) => typeof error?.coworkerdStage === "string" ? error.coworkerdStage : "package";

async function loadSource(descriptor, env) {
  const url = new URL("http://coworkerd-source/v1/source");
  url.searchParams.set("workerId", descriptor.workerId);
  url.searchParams.set("versionId", descriptor.versionId);
  url.searchParams.set("contentHash", descriptor.contentHash);
  url.searchParams.set("definitionHash", descriptor.definitionHash);
  // A load check loads for the call it precedes: the cold start is that call's.
  url.searchParams.set("invocationId", descriptor.sourceInvocationId || descriptor.invocationId);
  url.searchParams.set("processId", env.PROCESS_ID);
  url.searchParams.set("processGeneration", env.PROCESS_GENERATION);
  const response = await env.SOURCE.fetch(url);
  if (!response.ok) {
    throw new Error(`Trusted source fetch failed: ${response.status}`);
  }
  const value = await response.json();
  if (
    value === null ||
    typeof value !== "object" ||
    value.workerId !== descriptor.workerId ||
    value.versionId !== descriptor.versionId ||
    value.contentHash !== descriptor.contentHash ||
    typeof value.source !== "string"
  ) {
    throw new Error("Trusted source identity mismatch");
  }
  if (descriptor.format === "v2") {
    const payload = JSON.parse(value.source);
    if (typeof payload.manifest !== "string" || !payload.modules ||
        hex(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(payload.manifest))) !== descriptor.contentHash) {
      throw new Error("Trusted package manifest hash mismatch");
    }
    const manifest = JSON.parse(payload.manifest);
    const modules = {};
    for (const module of manifest.modules) {
      const encoded = payload.modules[module.sha256];
      if (typeof encoded !== "string") throw new Error("Missing trusted module");
      const bytes = Uint8Array.from(atob(encoded), c => c.charCodeAt(0));
      if (bytes.length !== module.bytes || hex(await crypto.subtle.digest("SHA-256", bytes)) !== module.sha256) {
        throw new Error("Trusted module content hash mismatch");
      }
      const type = { esm: "js", cjs: "cjs", text: "text", data: "data", json: "json", wasm: "wasm" }[module.type];
      if (!type || module.name.startsWith("__coworkerd_")) throw new Error("Invalid trusted module");
      try {
        modules[module.name] = { [type]: type === "json" ? JSON.parse(new TextDecoder().decode(bytes)) :
          type === "data" || type === "wasm" ? bytes : new TextDecoder("utf-8", { fatal: true }).decode(bytes) };
      } catch (error) {
        throw staged("decode", error);
      }
    }
    return modules;
  }
  const actualHash = hex(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value.source)),
  );
  if (actualHash !== descriptor.contentHash) {
    throw new Error("Trusted source content hash mismatch");
  }
  return value.source;
}

async function makeWorkerCode(descriptor, env, ctx) {
  for (const binding of descriptor.bindings) {
    if (
      typeof binding.name !== "string" ||
      typeof binding.workerId !== "string" ||
      typeof binding.versionId !== "string" ||
      typeof binding.contentHash !== "string" ||
      !SHA256_HEX.test(binding.contentHash) ||
      typeof binding.definitionHash !== "string" ||
      !SHA256_HEX.test(binding.definitionHash)
    ) {
      throw new Error("Invalid trusted Binding plan");
    }
  }

  const source = await loadSource(descriptor, env);
  if (descriptor.format === "v2") {
    // Secrets join env through the same channel as variables (names never
    // collide, the deployment rejects that), fixed when the Worker is loaded:
    // a loaded Worker keeps the values it was loaded with. Errors name no value.
    const secrets = descriptor.secrets ?? [];
    if (!Array.isArray(secrets) || !secrets.every(s => s !== null && typeof s === "object" && typeof s.name === "string")) {
      throw staged("secrets", new Error("Invalid trusted secrets"));
    }
    const unset = secrets.filter(s => typeof s.value !== "string").map(s => s.name);
    if (unset.length) throw staged("secrets", new Error(`${unset.length === 1 ? "secret" : "secrets"} without a value: ${unset.join(", ")}`));
    let variables;
    try {
      variables = Object.fromEntries([
        ...descriptor.variables.map(v => [v.name, v.type === "json" ? JSON.parse(v.value) : v.value]),
        ...secrets.map(s => [s.name, s.value]),
      ]);
    } catch (error) {
      throw staged("decode", error);
    }
    const bridge = `import { __coworkerdLog } from "./${PRELUDE_MODULE}";
import { withEnv, WorkerEntrypoint } from "cloudflare:workers";
import * as tenant from ${JSON.stringify("./" + descriptor.mainModule)};
${TRIAL_SOURCE}
export default { fetch(request, variables, ctx) {
  if (ctx.props.trial === true) return __coworkerdTrial(tenant, true, ctx.props.entrypoint);
  const { bindings, entrypoint } = ctx.props;
  const env = { ...variables, ...bindings };
  return withEnv(env, () => {
    const target = tenant[entrypoint || "default"];
    if (!target || (typeof target !== "function" && typeof target.fetch !== "function")) {
      throw new Error("Worker entrypoint missing or unsupported: " + (entrypoint || "default"));
    }
    __coworkerdLog("[tenant-entry] " + JSON.stringify({ serverId: request.cf.serverId, processId: request.cf.processId,
      processGeneration: request.cf.processGeneration, invocationId: request.cf.invocationId, treeId: request.cf.treeId,
      target: ${JSON.stringify(descriptor.workerId + "@" + descriptor.versionId)}, entrypoint: entrypoint || "default",
      callId: request.headers.get("x-placement-call-id"), observedAtUnixMs: Date.now() }));
    if (typeof target === "function" && target.prototype instanceof WorkerEntrypoint) {
      return new target(ctx, env).fetch(request);
    }
    if (target && typeof target.fetch === "function") return target.fetch(request, env, ctx);
    throw new Error("Worker entrypoint missing or unsupported: " + (entrypoint || "default"));
  });
}};`;
    return { compatibilityDate: descriptor.compatibilityDate,
      compatibilityFlags: [...new Set([...descriptor.compatibilityFlags, "enable_request_signal"])],
      mainModule: BRIDGE_MODULE, modules: { ...source, [BRIDGE_MODULE]: bridge, [PRELUDE_MODULE]: preludeSource(descriptor) },
      env: variables, globalOutbound: egressOutbound(descriptor, env, ctx) };
  }
  return {
    compatibilityDate: descriptor.compatibilityDate,
    compatibilityFlags: ["enable_request_signal"],
    mainModule: BRIDGE_MODULE,
    modules: {
      [BRIDGE_MODULE]: BRIDGE_SOURCE,
      [TENANT_MODULE]: source,
      [PRELUDE_MODULE]: preludeSource(descriptor),
    },
    env: {},
    globalOutbound: egressOutbound(descriptor, env, ctx),
  };
}

export class BoundWorker extends WorkerEntrypoint {
  async fetch(request) {
    const {
      workerId,
      versionId,
      contentHash,
      definitionHash,
      treeId,
      parentInvocationId,
      orderSequence,
      expiresAtMonotonicMs,
      requiresChildSequence,
      entrypoint,
      assets,
    } = this.ctx.props;
    if (
      typeof workerId !== "string" ||
      typeof versionId !== "string" ||
      typeof contentHash !== "string" ||
      !SHA256_HEX.test(contentHash) ||
      typeof definitionHash !== "string" ||
      !SHA256_HEX.test(definitionHash) ||
      typeof treeId !== "string" ||
      typeof parentInvocationId !== "string" ||
      !Number.isFinite(expiresAtMonotonicMs) ||
      expiresAtMonotonicMs <= 0 ||
      typeof requiresChildSequence !== "boolean" ||
      !Number.isInteger(orderSequence) ||
      orderSequence < 0
    ) {
      throw new Error("Invalid bound Worker capability");
    }
    if (assets === true) {
      // The ASSETS Binding: the Server's asset layer of this version answers; no
      // child invocation, so no call sequence.
      return this.env.SERVER.fetch(request, {
        cf: {
          workerId, versionId, contentHash, definitionHash, treeId, parentInvocationId,
          callSequence: 0, orderSequence: 0, expiresAtMonotonicMs, sequenceMapSize: nextChildSequences.size,
          entrypoint: "", assets: true,
        },
      });
    }
    // The deliberately retained naive mode needs creation order for its test-only
    // counterexample. Correct mode is order-independent and must not retain a
    // per-parent sequence entry in this long-lived isolate.
    const callSequence = requiresChildSequence
      ? nextChildSequence(parentInvocationId)
      : 0;
    return this.env.SERVER.fetch(request, {
      cf: {
        workerId,
        versionId,
        contentHash,
        definitionHash,
        treeId,
        parentInvocationId,
        callSequence,
        orderSequence: requiresChildSequence ? (orderSequence || callSequence) : 0,
        expiresAtMonotonicMs,
        sequenceMapSize: nextChildSequences.size,
        entrypoint: entrypoint ?? "",
      },
    });
  }
}

// The limits segment of a loader key: the limits this Worker names, for the isolate workerd creates
// under this key. An isolate's CPU budget and heap constraint belong to the isolate, so they are part of its
// identity -- a Worker whose limits change is loaded as another isolate rather than kept at the old ones --
// and a Worker that names none keeps the key without a segment, so nothing it has loaded is reloaded.
const limitsSegment = (descriptor) => {
  const cpuMs = descriptor.cpuLimitMs ?? 0, memoryMiB = descriptor.memoryLimitMiB ?? 0;
  return cpuMs || memoryMiB ? `#limits=${cpuMs}:${memoryMiB}` : "";
};

const loaderKey = (descriptor) =>
  `${descriptor.workerId}@${descriptor.versionId}#${descriptor.contentHash}#${descriptor.definitionHash}${limitsSegment(descriptor)}`;

// A trial load: the version is loaded exactly as for a call -- same loader key,
// so the isolate stays for the next call of this definition -- and our bridge then
// reports the tenant's exports; no tenant handler runs and no Binding is given. A
// failure is returned as the error's name and message, for the Server to classify
// and clean up; this log line carries neither. The load check before a call's
// dispatch (`loadCheck`) is the same probe, for the entrypoint the call names:
// "code" failures also say where in our code (`codeStage`).
async function trialLoad(descriptor, env, ctx) {
  const started = performance.now();
  let coldLoad = false, stage = "load", codeStage = null;
  const worker = env.LOADER.get(loaderKey(descriptor), async () => {
    coldLoad = true;
    try { return await makeWorkerCode(descriptor, env, ctx); } catch (error) { stage = "code"; codeStage = stageOf(error); throw error; }
  });
  let report;
  try {
    const props = descriptor.format === "v2" ? { trial: true, entrypoint: descriptor.entrypoint || "default" } : { [TRIAL_PROP]: true };
    // The identity the CPU and memory limits attribute this entry to, as for a call.
    const cf = { nodeId: descriptor.serverId, serverId: descriptor.serverId, processId: env.PROCESS_ID,
      processGeneration: Number(env.PROCESS_GENERATION),
      workerId: descriptor.workerId, versionId: descriptor.versionId, contentHash: descriptor.contentHash,
      invocationId: descriptor.invocationId, treeId: descriptor.treeId };
    report = await (await worker.getEntrypoint(undefined, { props }).fetch("http://coworkerd-trial/", { cf })).json();
  } catch (error) {
    report = { ok: false, stage, codeStage, name: typeof error?.name === "string" ? error.name : typeof error,
      message: typeof error?.message === "string" ? error.message : String(error) };
  }
  report.coldLoad = coldLoad;
  report.loadMs = performance.now() - started;
  console.log((descriptor.loadCheck === true ? "[runtime-load] " : "[runtime-trial] ") + JSON.stringify({ serverId: descriptor.serverId,
    processId: env.PROCESS_ID, processGeneration: Number(env.PROCESS_GENERATION), invocationId: descriptor.invocationId,
    target: `${descriptor.workerId}@${descriptor.versionId}`, definitionHash: descriptor.definitionHash, ok: report.ok,
    stage: report.ok ? null : report.stage, ...(descriptor.loadCheck === true ? { codeStage: report.ok ? null : report.codeStage,
      entrypoint: report.entrypoint ?? null, entrypointUsable: report.entrypointUsable ?? null,
      forInvocationId: descriptor.sourceInvocationId || null } : {}),
    coldLoad, loadMs: report.loadMs, observedAtUnixMs: Date.now() }));
  return Response.json(report);
}

export default {
  async fetch(request, env, ctx) {
    announceNonce(env);
    if (new URL(request.url).pathname === "/__coworkerd/probe" && request.cf?.invocationId === undefined) {
      return new Response(`${env.PROCESS_ID}:${env.PROCESS_GENERATION}`);
    }
    const descriptor = descriptorFrom(request);
    if (descriptor.trial === true) return trialLoad(descriptor, env, ctx);
    const pathTiming = env.PATH_TIMING === "1" ? { receivedAtUnixMs: Date.now(), receivedMonotonicMs: performance.now() } : undefined;
    const key = loaderKey(descriptor);
    let coldStart = false;
    // The load check before this dispatch loaded the version for this call: its cold start.
    if (descriptor.coldLoaded === true) coldStart = true;
    const worker = env.LOADER.get(key, async () => {
      coldStart = true;
      return makeWorkerCode(descriptor, env, ctx);
    });
    const bindings = {};
    for (const binding of descriptor.bindings) {
      bindings[binding.name] = ctx.exports.BoundWorker({
        props: {
          workerId: binding.workerId,
          versionId: binding.versionId,
          contentHash: binding.contentHash,
          definitionHash: binding.definitionHash,
          treeId: descriptor.treeId,
          parentInvocationId: descriptor.invocationId,
          orderSequence: descriptor.orderSequence,
          expiresAtMonotonicMs: descriptor.lease.expiresAtMonotonicMs,
          requiresChildSequence: descriptor.requiresChildSequence,
          entrypoint: binding.entrypoint,
          ...(binding.assets === true ? { assets: true } : {}),
        },
      });
    }
    if (pathTiming) { pathTiming.dispatchedAtUnixMs = Date.now(); pathTiming.dispatchedMonotonicMs = performance.now(); }
    const props = descriptor.format === "v2" ? { bindings, entrypoint: descriptor.entrypoint || "default" } : bindings;
    return worker.getEntrypoint(undefined, { props }).fetch(request, {
      cf: {
        // The node this execution runs on. `serverId` carries the same value and remains through 0.3.x
        // (docs/guide/compat.md).
        nodeId: descriptor.serverId,
        serverId: descriptor.serverId,
        processId: env.PROCESS_ID,
        processGeneration: Number(env.PROCESS_GENERATION),
        workerId: descriptor.workerId,
        versionId: descriptor.versionId,
        contentHash: descriptor.contentHash,
        invocationId: descriptor.invocationId,
        treeId: descriptor.treeId,
        ingressServerId: descriptor.ingressServerId,
        lease: descriptor.lease,
        orderSequence: descriptor.orderSequence,
        depth: descriptor.depth,
      },
    }).finally(() => {
      console.log("[runtime-execution] " + JSON.stringify({
        serverId: descriptor.serverId, processId: env.PROCESS_ID,
        processGeneration: Number(env.PROCESS_GENERATION), invocationId: descriptor.invocationId,
        treeId: descriptor.treeId, target: `${descriptor.workerId}@${descriptor.versionId}`,
        coldStart, observedAtUnixMs: Date.now(),
        ...(pathTiming ? { pathTiming: { ...pathTiming, returnedAtUnixMs: Date.now(), returnedMonotonicMs: performance.now() } } : {}),
      }));
    });
  },
};

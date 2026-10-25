import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

const SHA256_HEX = /^[0-9a-f]{64}$/;
const BRIDGE_MODULE = "__coworkerd_entrypoint__.js";
const TENANT_MODULE = "__coworkerd_tenant__.js";
// Tenant logs: workerd records each console.debug/info/log/warn/error call of a tenant isolate, made while one of
// its invocations runs (its waitUntil work included), in that invocation's trace -- native code, out of the tenant's
// reach -- and streams it, as the call is made, to the isolate's streaming tail: the TenantLog entrypoint below, bound
// to the Worker's identity when the loader loads it (the props come from this runtime, never from the tenant).
// TenantLog sends the lines to the node over SOURCE (<stack>/run/source.sock, in this isolate's env only) as they
// come, naming the invocation they belong to -- the node's invocation id, from the cf this runtime gives the request --
// and the node keeps them as that Worker's, within 256 KB an event (a request, a request to a Durable Object, or a
// message of a WebSocket connection). What a tenant writes in its lines, or anywhere on standard output, counts only as
// its own: no value a tenant could read or forge decides whose line it is. The prelude module, evaluated before a
// version's own modules, formats each call's arguments into one line first; output of a module's top level (outside
// any invocation) stays in the process's own log. Streaming tails are an experimental API of workerd: the Worker
// Loader takes them only with allowExperimental, which only a loader running with the `experimental` compatibility
// flag may give (this runtime's configuration, node/process-pool.c++); a tenant's own flags stay on the non-experimental
// list (common/generated/), so the dynamic isolates' behaviour is unchanged.
const PRELUDE_MODULE = "__coworkerd_console__.js";
// The prelude of one version. Arguments are formatted as console.log formats them in Node (format specifiers, then
// the rest, objects shown to depth 2); the original console.log stays available to the bridge only.
function preludeSource() {
  return `const original = { debug: console.debug, info: console.info, log: console.log, warn: console.warn, error: console.error };
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
    write.call(console, message);
  };
}
export const __coworkerdLog = original.log.bind(console);
`;
}
// The bridge's own line of every call it hands to the tenant ([tenant-entry], below): the process's evidence, not
// one of the tenant's lines.
const ENTRY_LINE = "[tenant-entry] ";
const LOG_LEVELS = new Set(["debug", "info", "log", "warn", "error"]);
// A tenant's line from a streaming tail's log event, or null (another event, or the bridge's own line). A message too
// large for workerd to keep whole arrives truncated as one string.
function logLine(log) {
  if (log?.type !== "log") return null;
  const parts = Array.isArray(log.message) ? log.message : [log.message];
  if (parts.length === 1 && typeof parts[0] === "string" && parts[0].startsWith(ENTRY_LINE)) return null;
  const message = parts.length === 1 && typeof parts[0] === "string" ? parts[0]
    : parts.map(part => typeof part === "string" ? part : JSON.stringify(part) ?? String(part)).join(" ");
  return { level: LOG_LEVELS.has(log.level) ? log.level : "log", message };
}
export class TenantLog extends WorkerEntrypoint {
  // One invocation of the Worker's isolate: its lines go to the node in the order written, those written meanwhile
  // together, each post naming the invocation (onset.event.info.cfJson: the cf this runtime gave the request).
  tailStream(onset) {
    const env = this.env, { workerId, versionId } = this.ctx.props;
    const cf = onset?.event?.info?.cfJson;
    const invocationId = typeof cf?.invocationId === "string" ? cf.invocationId : "";
    let lines = [], sending = Promise.resolve(), queued = null;
    const flush = () => {
      queued = null;
      if (!lines.length) return sending;
      const batch = lines;
      lines = [];
      sending = sending.then(async () => {
        const url = new URL("http://coworkerd-source/v1/log");
        url.searchParams.set("processId", env.PROCESS_ID);
        url.searchParams.set("processGeneration", env.PROCESS_GENERATION);
        const response = await env.SOURCE.fetch(url, { method: "POST", headers: { "content-type": "application/json" },
          body: JSON.stringify({ workerId, versionId, invocationId, lines: batch }) });
        await response.body?.cancel();
      }).catch(() => {});
      return sending;
    };
    return {
      log(event) {
        const line = logLine(event?.event);
        if (!line) return;
        lines.push(line);
        return (queued ??= Promise.resolve().then(flush));
      },
      outcome() { return flush(); },
    };
  }
}
// The streaming tail of a version's isolate: TenantLog with the version's identity.
function tenantLogTail(descriptor, ctx) {
  return ctx?.exports?.TenantLog ? [ctx.exports.TenantLog({ props: { workerId: descriptor.workerId, versionId: descriptor.versionId } })] : [];
}
// Outbound requests: a tenant Worker's global outbound is the Egress entrypoint below, bound to that Worker's
// identity when the loader loads it (the props come from this runtime, never from the tenant). It hands the request
// to the node's egress service (EGRESS, <stack>/run/egress.sock, the proxy form) with that identity and this process's
// egress token -- in this isolate's env only, which no tenant isolate can reach -- and the node checks, resolves,
// connects, meters and answers. A refusal comes back as a header and is thrown to the tenant as an Error:
// "EGRESS_DENIED: <rule>" or "EGRESS_FAILED: <class>". Redirects are the tenant's fetch's to follow: each hop comes back
// here and is checked again. Without EGRESS (egress.mode off) the same entrypoint refuses every request
// ("EGRESS_DENIED: egress-off"): a tenant isolate always has an outbound that answers, never workerd's null one.
const EGRESS_SOURCE_HEADER = "x-coworkerd-egress-source";
const EGRESS_ERROR_HEADER = "x-coworkerd-egress-error";
export class Egress extends WorkerEntrypoint {
  async fetch(request) {
    if (!this.env.EGRESS) throw new Error("EGRESS_DENIED: egress-off");
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
// <tenant>/<worker>: the node takes the tenant from it), whether or not this process has EGRESS.
function egressOutbound(descriptor, ctx) {
  if (!ctx?.exports?.Egress) return null;
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
// A Durable Object class the version declares: an exported class that extends DurableObject or has fetch.
function __coworkerdDurableUsable(namespace, name) {
  const target = Object.prototype.hasOwnProperty.call(namespace, name) ? namespace[name] : undefined;
  if (target === undefined) return { name, usable: false, reason: "is not exported by the main module" };
  if (typeof target !== "function" || target.prototype === undefined) return { name, usable: false, reason: "is not a class" };
  if (DurableObject.prototype.isPrototypeOf(target.prototype) || __coworkerdHas(target.prototype, "fetch")) return { name, usable: true };
  return { name, usable: false, reason: "neither extends DurableObject nor has a fetch method" };
}
function __coworkerdTrial(namespace, classes, requested, durableClasses) {
  const names = Object.keys(namespace);
  const hasDefault = names.includes("default");
  const named = names.filter(name => name !== "default" && __coworkerdEntry(namespace[name], classes) !== "unusable" &&
    !(Array.isArray(durableClasses) && durableClasses.includes(name)));
  const entrypoint = typeof requested === "string" && requested ? requested : "default";
  return Response.json({ ok: true, hasDefault, defaultKind: hasDefault ? __coworkerdEntry(namespace.default, classes) : null,
    namedEntrypoints: named.slice(0, 32), exportCount: names.length, entrypoint,
    entrypointUsable: __coworkerdUsable(namespace, entrypoint, classes),
    ...(Array.isArray(durableClasses) && durableClasses.length ?
      { durableObjectClasses: durableClasses.map(name => __coworkerdDurableUsable(namespace, name)) } : {}) });
}
`;

// Durable Objects (docs/compatibility.md, section 7), in the bridge of a version with any. A namespace Binding is an
// object of this module, made for each call (a stateless Worker's: its Bindings carry the call tree) or each object (a
// class's env): idFromName (the SHA-256 of the namespace's 32 bytes, "name" and the name, as 64 hexadecimal digits:
// the same id for the same name, in this namespace only), newUniqueId (32 random bytes), idFromString (64 hexadecimal
// digits) and get, whose stub's fetch hands the request to the runtime's DurableNamespace entrypoint with the object's
// id in a header that entrypoint takes off. An id knows its namespace: another namespace's get refuses it. No RPC, no
// jurisdiction; a location hint is ignored. A class the version declares is exported under DURABLE_EXPORT_PREFIX: the
// tenant's class, constructed with an env that holds the version's variables and secrets and, made for the object, its
// Durable Object Bindings.
const DO_OBJECT_HEADER = "x-coworkerd-do-object";
const DO_HOPS_HEADER = "x-coworkerd-do-hops";
// The object's alarm: the time the node scheduled and how many times the handler has been tried. The node sets them
// on a request of its own; a tenant cannot reach the Host, and the Host sets them from the descriptor alone.
const DO_ALARM_HEADER = "x-coworkerd-do-alarm";
const DO_ALARM_RETRY_HEADER = "x-coworkerd-do-alarm-retry";
const DURABLE_EXPORT_PREFIX = "__coworkerd_do_";
const DURABLE_SOURCE = `
const __coworkerdK = new Uint32Array([0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
  0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb,
  0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f,
  0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2]);
// SHA-256, synchronously (idFromName returns its id at once): 64 hexadecimal digits.
function __coworkerdSha256(bytes) {
  const h = new Uint32Array([0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19]);
  const length = bytes.length, total = (length + 72) & ~63;
  const data = new Uint8Array(total);
  data.set(bytes);
  data[length] = 0x80;
  const view = new DataView(data.buffer);
  view.setUint32(total - 8, Math.floor(length / 0x20000000));
  view.setUint32(total - 4, (length << 3) >>> 0);
  const w = new Uint32Array(64);
  for (let offset = 0; offset < total; offset += 64) {
    for (let i = 0; i < 16; i++) w[i] = view.getUint32(offset + i * 4);
    for (let i = 16; i < 64; i++) {
      const x = w[i - 15], y = w[i - 2];
      w[i] = w[i - 16] + ((x >>> 7 | x << 25) ^ (x >>> 18 | x << 14) ^ (x >>> 3)) + w[i - 7] +
        ((y >>> 17 | y << 15) ^ (y >>> 19 | y << 13) ^ (y >>> 10));
    }
    let a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], k = h[7];
    for (let i = 0; i < 64; i++) {
      const t1 = (k + ((e >>> 6 | e << 26) ^ (e >>> 11 | e << 21) ^ (e >>> 25 | e << 7)) + ((e & f) ^ (~e & g)) + __coworkerdK[i] + w[i]) | 0;
      const t2 = (((a >>> 2 | a << 30) ^ (a >>> 13 | a << 19) ^ (a >>> 22 | a << 10)) + ((a & b) ^ (a & c) ^ (b & c))) | 0;
      k = g; g = f; f = e; e = (d + t1) | 0; d = c; c = b; b = a; a = (t1 + t2) | 0;
    }
    h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += k;
  }
  let out = "";
  for (const word of h) out += word.toString(16).padStart(8, "0");
  return out;
}
const __coworkerdHex = /^[0-9a-f]{64}$/;
const __coworkerdIdNamespace = new WeakMap();
class DurableObjectId {
  #id; #name;
  constructor(id, name) { this.#id = id; this.#name = name; }
  toString() { return this.#id; }
  toJSON() { return this.#id; }
  equals(other) { return other instanceof DurableObjectId && other.toString() === this.#id; }
  get name() { return this.#name; }
}
function __coworkerdNamespace(entry) {
  const namespaceId = typeof entry?.namespaceId === "string" ? entry.namespaceId : "";
  const fetcher = entry?.fetcher;
  const unavailable = () => new Error("DO_NAMESPACE_UNAVAILABLE: the Durable Object namespace of class " + entry?.className +
    " cannot be used (it is being deleted, or the version does not declare the class)");
  const prefix = __coworkerdHex.test(namespaceId) ? Uint8Array.from(namespaceId.match(/../g), byte => parseInt(byte, 16)) : null;
  const made = (id, name) => { const value = new DurableObjectId(id, name); __coworkerdIdNamespace.set(value, namespaceId); return value; };
  const space = {
    idFromName(name) {
      if (!prefix) throw unavailable();
      const text = new TextEncoder().encode("name" + String(name));
      const bytes = new Uint8Array(prefix.length + text.length);
      bytes.set(prefix); bytes.set(text, prefix.length);
      return made(__coworkerdSha256(bytes), String(name));
    },
    newUniqueId(options) {
      if (!prefix) throw unavailable();
      if (options?.jurisdiction !== undefined) throw new TypeError("Durable Object jurisdictions are not supported");
      const bytes = crypto.getRandomValues(new Uint8Array(32));
      return made([...bytes].map(byte => byte.toString(16).padStart(2, "0")).join(""), undefined);
    },
    idFromString(value) {
      if (!prefix) throw unavailable();
      if (typeof value !== "string" || !__coworkerdHex.test(value))
        throw new TypeError("Invalid Durable Object ID: 64 lowercase hexadecimal digits");
      return made(value, undefined);
    },
    get(id, options) {
      if (!prefix) throw unavailable();
      if (!(id instanceof DurableObjectId) || __coworkerdIdNamespace.get(id) !== namespaceId)
        throw new TypeError("ID is not for this Durable Object namespace.");
      const objectId = id.toString();
      const stub = { id, name: id.name, fetch(input, init) {
        const request = new Request(input, init);
        const headers = new Headers(request.headers);
        headers.set(${JSON.stringify(DO_OBJECT_HEADER)}, objectId);
        return fetcher.fetch(new Request(request, { headers }));
      } };
      // Any public method of the object's class, as on Cloudflare: the call goes to the node over the namespace's
      // channel, which answers with the object's own capability, and workerd's RPC delivers it there.
      const open = () => fetcher.rpcTarget(objectId);
      return new Proxy(Object.freeze(stub), {
        get(target, property) {
          if (Object.prototype.hasOwnProperty.call(target, property)) return target[property];
          if (typeof property !== "string" || property === "then") return undefined;
          return __coworkerdRpcPath(open, [property]);
        },
        has(target, property) { return typeof property === "string" && property !== "then"; },
      });
    },
    getByName(name, options) { return space.get(space.idFromName(name), options); },
    jurisdiction() { throw new TypeError("Durable Object jurisdictions are not supported"); },
  };
  return Object.freeze(space);
}
// Each Durable Object Binding of a call or an object, by name; a d1_databases Binding is a D1Database instead.
function __coworkerdNamespaces(entries) {
  const out = {};
  for (const entry of Array.isArray(entries) ? entries : [])
    out[entry.binding] = entry.database ? __coworkerdD1(entry) : __coworkerdNamespace(entry);
  return out;
}
// A d1_databases Binding. The wire protocol is the one workerd's own D1 wrapper speaks
// (src/cloudflare/internal/d1-api.ts): POST /query and /execute on the database with {sql, params} (or an array of
// them) and resultsFormat, answered with {success, meta, results} -- rows and columns, or an array of objects. The
// database is one Durable Object of the platform's own SQL Worker class, so every statement goes to its owner over
// the same path a Durable Object call takes. prepare/bind/first/run/all/raw, batch and exec are all this protocol;
// Sessions (withSession, query) and dump are not part of this release (docs/compatibility.md, section 7).
function __coworkerdD1Values(values) {
  return values.map((value) => {
    const kind = typeof value;
    if (kind === "number" || kind === "string") return value;
    if (kind === "boolean") return value ? 1 : 0;
    if (kind === "object") {
      if (value == null) return value;
      if (Array.isArray(value) && value.every((b) => typeof b === "number" && b >= 0 && b < 256)) return value;
      if (value instanceof ArrayBuffer) return Array.from(new Uint8Array(value));
      if (ArrayBuffer.isView(value)) return Array.from(value);
    }
    throw new Error("D1_TYPE_ERROR: Type '" + kind + "' not supported for value '" + value + "'",
      { cause: new Error("Type '" + kind + "' not supported for value '" + value + "'") });
  });
}
const __coworkerdD1First = (results) => Array.isArray(results) ? results.at(0) : results;
// The shim D1's own wrapper applies: rows and columns become an array of objects, a missing results an empty one.
function __coworkerdD1Objects(answer) {
  if (!("results" in answer)) return { ...answer, results: [] };
  const results = answer.results;
  if (Array.isArray(results)) return { ...answer, results };
  const { rows, columns } = results;
  return { ...answer, results: rows.map((row) => Object.fromEntries(row.map((cell, i) => [columns[i], cell]))) };
}
async function __coworkerdD1Send(entry, endpoint, query, params, resultsFormat) {
  const body = JSON.stringify(Array.isArray(query)
    ? query.map((sql, index) => ({ sql, params: params[index] }))
    : { sql: query, params });
  const headers = new Headers({ "content-type": "application/json" });
  headers.set(${JSON.stringify(DO_OBJECT_HEADER)}, entry.objectId);
  const url = "http://d1" + endpoint + "?resultsFormat=" + resultsFormat;
  let answer;
  try {
    const response = await entry.fetcher.fetch(new Request(url, { method: "POST", headers, body }));
    answer = await response.json();
  } catch (error) {
    const message = String(error?.cause?.message ?? error?.message ?? error) || "Something went wrong";
    throw new Error("D1_ERROR: " + message, { cause: new Error(message) });
  }
  // A statement that failed carries an error alone; a refusal of the platform (the object too large, the tenant's
  // quota, the storage) carries its code as well, which goes into the message the caller sees.
  const failed = (one) => ({ success: false, error: one.code ? one.code + ": " + (one.reason ?? one.error) : one.error });
  return Array.isArray(answer)
    ? answer.map((one) => (one?.error ? failed(one) : one))
    : answer?.error ? failed(answer) : answer;
}
async function __coworkerdD1SendOrThrow(entry, endpoint, query, params, resultsFormat) {
  const results = await __coworkerdD1Send(entry, endpoint, query, params, resultsFormat);
  const first = __coworkerdD1First(results);
  if (!first?.success) {
    const message = first?.error ?? "Something went wrong";
    throw new Error("D1_ERROR: " + message, { cause: new Error(message) });
  }
  return results;
}
class __coworkerdD1PreparedStatement {
  #entry;
  constructor(entry, statement, values) {
    this.#entry = entry;
    this.statement = statement;
    this.params = values || [];
  }
  bind(...values) {
    return new __coworkerdD1PreparedStatement(this.#entry, this.statement, __coworkerdD1Values(values));
  }
  async first(column) {
    const info = __coworkerdD1First(
      await __coworkerdD1SendOrThrow(this.#entry, "/query", this.statement, this.params, "ROWS_AND_COLUMNS"));
    const results = __coworkerdD1Objects(info).results;
    if (results.length === 0) return null;
    const first = results.at(0);
    if (column === undefined) return first;
    if (first?.[column] === undefined) {
      throw new Error("D1_COLUMN_NOTFOUND: Column not found (" + column + ")", { cause: new Error("Column not found") });
    }
    return first[column];
  }
  async run() {
    return __coworkerdD1First(
      await __coworkerdD1SendOrThrow(this.#entry, "/execute", this.statement, this.params, "NONE"));
  }
  async all() {
    return __coworkerdD1Objects(__coworkerdD1First(
      await __coworkerdD1SendOrThrow(this.#entry, "/query", this.statement, this.params, "ROWS_AND_COLUMNS")));
  }
  async raw(options) {
    const answer = __coworkerdD1First(
      await __coworkerdD1SendOrThrow(this.#entry, "/query", this.statement, this.params, "ROWS_AND_COLUMNS"));
    if (!("results" in answer)) return [];
    const results = answer.results;
    if (Array.isArray(results)) {
      const raw = [];
      for (const row of results) {
        if (options?.columnNames && raw.length === 0) raw.push(Array.from(Object.keys(row)));
        raw.push(Object.keys(row).map((key) => row[key]));
      }
      return raw;
    }
    return [...(options?.columnNames ? [results.columns] : []), ...results.rows];
  }
}
function __coworkerdD1(entry) {
  const unavailable = () => new Error("D1_ERROR: the database " + (entry?.database ?? "") +
    " cannot be used (it does not exist, or it is being deleted)");
  const usable = typeof entry?.objectId === "string" && entry.objectId.length === 64 && entry.fetcher;
  const database = {
    prepare(statement) {
      if (!usable) throw unavailable();
      return new __coworkerdD1PreparedStatement(entry, statement);
    },
    async batch(statements) {
      if (!usable) throw unavailable();
      const list = Array.from(statements ?? []);
      const answers = await __coworkerdD1SendOrThrow(entry, "/query", list.map((one) => one.statement),
        list.map((one) => one.params), "ROWS_AND_COLUMNS");
      return (Array.isArray(answers) ? answers : [answers]).map(__coworkerdD1Objects);
    },
    async exec(query) {
      if (!usable) throw unavailable();
      const lines = String(query).trim().split("\\n");
      const answers = await __coworkerdD1Send(entry, "/execute", lines, [], "NONE");
      const results = Array.isArray(answers) ? answers : [answers];
      let duration = 0;
      for (const [index, one] of results.entries()) {
        if (!one?.success) {
          const message = one?.error || "Something went wrong";
          const legacy = "Error in line " + (index + 1) + ": " + lines[index] + ": " + message;
          throw new Error("D1_EXEC_ERROR: " + legacy, { cause: new Error(legacy) });
        }
        duration += one.meta?.duration ?? 0;
      }
      return { count: results.length, duration };
    },
    async dump() {
      throw new Error("D1_ERROR: dump() is not supported on this platform");
    },
    withSession() {
      throw new Error("D1_ERROR: the D1 Sessions API is not supported on this platform");
    },
    async query() {
      throw new Error("D1_ERROR: D1Database.query() is not supported on this platform");
    },
  };
  return Object.freeze(database);
}
const __coworkerdObjectEnvs = new WeakMap();
// The class a declared class runs as: the tenant's, constructed with its object's env, and its fetch run within it
// (import { env } from "cloudflare:workers" sees the same). A class that is not exported fails when its object starts
// (the trial load refuses such a version first).
// The object's alarm, which the node schedules and fires as an event of this Worker. The time in the object's own
// database is the truth: a firing the node asked for that the database does not hold any more runs nothing. A
// handler that ran leaves no alarm behind -- the delete is part of the object's own transaction, which its peers
// confirm before this answer leaves, so the shard's next owner never fires it again.
async function __coworkerdAlarm(object, scheduledAtUnixMs, retryCount) {
  const storage = object?.ctx?.storage;
  if (!storage) return Response.json({ outcome: "notDue", atUnixMs: 0, reason: "the object has no storage" });
  const held = await storage.getAlarm();
  if (held === null || held === undefined) return Response.json({ outcome: "notDue", atUnixMs: 0 });
  if (held > Date.now()) return Response.json({ outcome: "notDue", atUnixMs: held });
  if (typeof object.alarm !== "function") {
    // Nothing can ever run it: the alarm goes rather than being retried for ever.
    await storage.deleteAlarm();
    return Response.json({ outcome: "fired", atUnixMs: 0, reason: "the class has no alarm() handler" });
  }
  try {
    await object.alarm({ retryCount, isRetry: retryCount > 0 });
  } catch (error) {
    return Response.json({ outcome: "failed", reason: String(error?.message ?? error).slice(0, 300) });
  }
  if ((await storage.getAlarm()) === held) await storage.deleteAlarm();
  return Response.json({ outcome: "fired", atUnixMs: 0 });
}
function __coworkerdDurableClass(namespace, name) {
  const base = Object.prototype.hasOwnProperty.call(namespace, name) ? namespace[name] : undefined;
  const missing = class { constructor() { throw new Error("Durable Object class " + name + " is not a class of this version"); } };
  if (typeof base !== "function") return missing;
  try {
    const wrapped = class extends base {
      constructor(ctx, variables) {
        const env = { ...variables, ...__coworkerdNamespaces(ctx?.props?.durableObjects) };
        super(ctx, env);
        __coworkerdObjectEnvs.set(this, env);
      }
    };
    const fetch = base.prototype?.fetch;
    Object.defineProperty(wrapped.prototype, "fetch", { configurable: true, writable: true, value: function (...args) {
      const env = __coworkerdObjectEnvs.get(this);
      let scheduled = null, retry = 0;
      try {
        scheduled = args[0]?.headers?.get(${JSON.stringify(DO_ALARM_HEADER)}) ?? null;
        retry = Number(args[0]?.headers?.get(${JSON.stringify(DO_ALARM_RETRY_HEADER)}) ?? 0) || 0;
      } catch {}
      if (scheduled !== null) return withEnv(env, () => __coworkerdAlarm(this, Number(scheduled), retry));
      if (typeof fetch !== "function")
        throw new Error("Durable Object class " + name + " has no fetch handler");
      return withEnv(env, () => fetch.apply(this, args));
    } });
    // The object as a capability, for a method call on its stub: the Host asks the facet for it and the node hands it
    // to the caller, so the call is delivered to this instance by workerd's own RPC.
    Object.defineProperty(wrapped.prototype, "__coworkerdRpcTarget", { configurable: true, get: function () {
      const env = __coworkerdObjectEnvs.get(this);
      return withEnv(env, () => __coworkerdRpcFacade(this, env));
    } });
    Object.defineProperty(wrapped, "name", { value: name });
    return wrapped;
  } catch {
    return missing;
  }
}
`;

// The JS RPC source every version's bridge carries (docs/compatibility.md, section 7). Three things live here:
//   __coworkerdRpcFacade: the capability a method call is delivered to. It is a genuine RpcTarget of this isolate --
//     the only kind workerd serializes as a stub, and therefore the only kind that can be a capability of its own
//     across processes -- whose methods are the entrypoint's or the object's own public methods, each run on that
//     instance within its env. The arguments are decoded here, in the isolate that owns the code, and nowhere else.
//   __coworkerdService: what `env.SVC` is for a Service Binding. `fetch` is the Binding's own channel, unchanged;
//     every other property is a method call, which opens a session to the node through the same channel (the shim's
//     rpcTarget) and is pipelined onto the capability the node answers with, so the node never decodes it.
//   __coworkerdRpcPath: property paths on a Binding (`env.SVC.counter.value()`), which workerd's own RPC carries as
//     the path of one call.
const RPC_SOURCE = `
function __coworkerdMemberNames(instance) {
  const names = [];
  for (let proto = Object.getPrototypeOf(instance); proto && proto !== Object.prototype; proto = Object.getPrototypeOf(proto)) {
    for (const name of Object.getOwnPropertyNames(proto)) {
      if (name === "constructor" || names.includes(name)) continue;
      names.push(name);
    }
  }
  return names;
}
function __coworkerdRpcFacade(instance, env) {
  class Facade extends RpcTarget {}
  for (const name of __coworkerdMemberNames(instance)) {
    const described = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(instance), name) ??
      { value: instance[name] };
    if (typeof described.get === "function") {
      Object.defineProperty(Facade.prototype, name, { configurable: true,
        get() { return withEnv(env, () => instance[name]); } });
    } else if (typeof described.value === "function") {
      Object.defineProperty(Facade.prototype, name, { configurable: true, writable: true,
        value(...args) { return withEnv(env, () => instance[name](...args)); } });
    }
  }
  return new Facade();
}
// A platform code at the start of an RPC error's message, as a property of the error the tenant catches
// (docs/guide/errors.md): the message itself keeps the code, since that is all an RPC exception carries.
function __coworkerdRpcCode(error) {
  const reason = String(error?.message ?? error);
  const colon = reason.indexOf(":");
  const code = colon > 0 ? reason.slice(0, colon) : "";
  if (/^[A-Z][A-Z_]*$/.test(code)) { try { error.code = code; } catch {} }
  return error;
}
function __coworkerdRpcPath(open, path) {
  const call = async (...args) => {
    let target = open();
    for (const step of path.slice(0, -1)) target = target[step];
    try {
      return await target[path[path.length - 1]](...args);
    } catch (error) {
      throw __coworkerdRpcCode(error);
    }
  };
  return new Proxy(call, {
    get(_target, name) { return typeof name === "string" && name !== "then" ? __coworkerdRpcPath(open, [...path, name]) : undefined; },
    apply(_target, _self, args) { return call(...args); },
  });
}
function __coworkerdService(fetcher) {
  if (!fetcher) return fetcher;
  const open = () => fetcher.rpcTarget();
  return new Proxy(fetcher, {
    get(target, name) {
      if (typeof name !== "string" || name === "then") return undefined;
      if (name === "fetch" || name === "connect") return (...args) => target[name](...args);
      return __coworkerdRpcPath(open, [name]);
    },
    has(target, name) { return typeof name === "string" && name !== "then"; },
  });
}
function __coworkerdServices(bindings) {
  const out = {};
  for (const [name, value] of Object.entries(bindings ?? {}))
    out[name] = value !== null && typeof value === "object" && typeof value.fetch === "function" ? __coworkerdService(value) : value;
  return out;
}
`;

const BRIDGE_SOURCE = `
import "./${PRELUDE_MODULE}";
import { withEnv, DurableObject, WorkerEntrypoint } from "cloudflare:workers";
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

const CLASS_NAME = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
const DECIMAL = /^[0-9]{1,20}$/;
// The descriptor's Durable Object fields, when present: the version's namespace Bindings, a request's object, the
// declared classes.
const RESOURCE_NAME = /^[a-z0-9][a-z0-9-]{0,62}$/;
function durableFieldsValid(value) {
  const bindings = value.durableObjects, target = value.durableObject, classes = value.durableObjectClasses;
  if (bindings !== undefined && !(Array.isArray(bindings) && bindings.every(b => b !== null && typeof b === "object" &&
      typeof b.name === "string" && CLASS_NAME.test(b.className ?? "") && (b.namespaceId === "" || SHA256_HEX.test(b.namespaceId ?? "")) &&
      (b.database === undefined || b.database === "" || RESOURCE_NAME.test(b.database)) &&
      (b.objectId === undefined || b.objectId === "" || SHA256_HEX.test(b.objectId))))) return false;
  if (target !== undefined && !(target !== null && typeof target === "object" && SHA256_HEX.test(target.namespaceId ?? "") &&
      SHA256_HEX.test(target.objectId ?? "") && CLASS_NAME.test(target.className ?? "") && Number.isInteger(target.hops ?? 0) &&
      Number.isInteger(target.shard ?? 0) && DECIMAL.test(String(target.shardEpoch ?? "0")) &&
      DECIMAL.test(String(target.dataEpoch ?? "0")) &&
      (target.database === undefined || target.database === "" || RESOURCE_NAME.test(target.database)) &&
      (target.alarmAtUnixMs === undefined || DECIMAL.test(String(target.alarmAtUnixMs))))) return false;
  if (classes !== undefined && !(Array.isArray(classes) && classes.every(name => typeof name === "string" && CLASS_NAME.test(name)))) return false;
  return true;
}

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
    !limit(value.memoryLimitMiB) ||
    !durableFieldsValid(value)
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
    const classes = Array.isArray(descriptor.durableObjectClasses) ? descriptor.durableObjectClasses : [];
    const bridge = `import { __coworkerdLog } from "./${PRELUDE_MODULE}";
import { withEnv, DurableObject, RpcTarget, WorkerEntrypoint } from "cloudflare:workers";
import * as tenant from ${JSON.stringify("./" + descriptor.mainModule)};
${TRIAL_SOURCE}
${RPC_SOURCE}
${classes.length ? DURABLE_SOURCE + classes.map(name => `export const ${DURABLE_EXPORT_PREFIX}${name} = __coworkerdDurableClass(tenant, ${JSON.stringify(name)});\n`).join("") : ""}
function __coworkerdEnvOf(variables, props) {
  const { bindings, durableObjects } = props;
  return { ...variables, ...__coworkerdServices(bindings)${classes.length ? ", ...__coworkerdNamespaces(durableObjects)" : ""} };
}
export default class extends WorkerEntrypoint {
  fetch(request) {
    const variables = this.env, ctx = this.ctx;
    if (ctx.props.trial === true) return __coworkerdTrial(tenant, true, ctx.props.entrypoint, ctx.props.classes);
    const entrypoint = ctx.props.entrypoint;
    const env = __coworkerdEnvOf(variables, ctx.props);
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
  }
  // The entrypoint as a capability, for a method call on a Service Binding: the node asks this runtime for it and
  // hands it to the caller, so the call is delivered here by workerd's own RPC. Only a WorkerEntrypoint class has
  // methods, as on Cloudflare.
  get __coworkerdRpcTarget() {
    const entrypoint = this.ctx.props.entrypoint || "default";
    const env = __coworkerdEnvOf(this.env, this.ctx.props);
    const target = tenant[entrypoint];
    if (typeof target !== "function" || !(target.prototype instanceof WorkerEntrypoint)) {
      throw new Error("Worker entrypoint has no methods (it does not extend WorkerEntrypoint): " + entrypoint);
    }
    return withEnv(env, () => __coworkerdRpcFacade(new target(this.ctx, env), env));
  }
}`;
    // `rpc` is on by default from Cloudflare's 2024-04-03, and a Binding of ours has methods whatever the version's
    // compatibility date says (docs/compatibility.md, section 7): a Worker that turned RPC off keeps it off.
    const flags = [...descriptor.compatibilityFlags];
    if (!flags.includes("no_rpc") && !flags.includes("rpc")) flags.push("rpc");
    return { compatibilityDate: descriptor.compatibilityDate,
      compatibilityFlags: [...new Set([...flags, "enable_request_signal"])],
      mainModule: BRIDGE_MODULE, modules: { ...source, [BRIDGE_MODULE]: bridge, [PRELUDE_MODULE]: preludeSource() },
      env: variables, globalOutbound: egressOutbound(descriptor, ctx), allowExperimental: true,
      streamingTails: tenantLogTail(descriptor, ctx) };
  }
  return {
    compatibilityDate: descriptor.compatibilityDate,
    compatibilityFlags: ["enable_request_signal"],
    mainModule: BRIDGE_MODULE,
    modules: {
      [BRIDGE_MODULE]: BRIDGE_SOURCE,
      [TENANT_MODULE]: source,
      [PRELUDE_MODULE]: preludeSource(),
    },
    env: {},
    globalOutbound: egressOutbound(descriptor, ctx),
    allowExperimental: true,
    streamingTails: tenantLogTail(descriptor, ctx),
  };
}

// The cf a tenant's request carries: who executes it, for whom, under which call tree (13 fields; docs/compatibility.md).
function tenantCf(descriptor, env) {
  return {
    // The node this execution runs on. `serverId` carries the same value: a deprecated alias of `nodeId`, removed in
    // 1.0 (docs/guide/compat.md).
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
  };
}

// A Durable Object namespace Binding's channel to the node: the bridge's stub hands its request here with the object's id
// in DO_OBJECT_HEADER, and this sends it to the node with the calling version, the namespace and class (the props come
// from this runtime: a stateless call's Bindings, or an object's Host) and the caller -- a stateless Worker's call tree,
// or the calling object.
export class DurableNamespace extends WorkerEntrypoint {
  // A method call on a stub of this namespace: the same envelope its `fetch` puts in cf, with the object the stub
  // names, as the one method of a session to the node. The node finds the shard's owner (the epoch and the fencing as
  // for a request) and answers with the object's own capability, which the tenant's call is then pipelined onto.
  rpcTarget(objectId) {
    const { workerId, versionId, contentHash, definitionHash, namespaceId, className, database, treeId,
      parentInvocationId, orderSequence, expiresAtMonotonicMs, requiresChildSequence, caller } = this.ctx.props;
    if (typeof objectId !== "string" || !SHA256_HEX.test(objectId)) throw new Error("Invalid Durable Object call");
    if (typeof workerId !== "string" || typeof versionId !== "string" || !SHA256_HEX.test(contentHash ?? "") ||
        !SHA256_HEX.test(definitionHash ?? "") || !SHA256_HEX.test(namespaceId ?? "") || !CLASS_NAME.test(className ?? "")) {
      throw new Error("Invalid Durable Object call");
    }
    const fromObject = caller !== undefined && caller !== null;
    const callSequence = !fromObject && requiresChildSequence ? nextChildSequence(parentInvocationId) : 0;
    return this.env.SERVER[JSON.stringify({
      workerId, versionId, contentHash, definitionHash,
      treeId: fromObject ? "" : treeId, parentInvocationId: fromObject ? "" : parentInvocationId,
      callSequence, orderSequence: !fromObject && requiresChildSequence ? (orderSequence || callSequence) : 0,
      expiresAtMonotonicMs: fromObject ? 0 : expiresAtMonotonicMs, sequenceMapSize: nextChildSequences.size, entrypoint: "",
      durableObject: { namespaceId, className, objectId, database: database ?? "",
        callerNamespaceId: fromObject ? caller.namespaceId : "", callerObjectId: fromObject ? caller.objectId : "" },
    })]();
  }

  async fetch(request) {
    const { workerId, versionId, contentHash, definitionHash, namespaceId, className, database, treeId, parentInvocationId,
      orderSequence, expiresAtMonotonicMs, requiresChildSequence, caller } = this.ctx.props;
    const objectId = request.headers.get(DO_OBJECT_HEADER);
    if (typeof workerId !== "string" || typeof versionId !== "string" || !SHA256_HEX.test(contentHash ?? "") ||
        !SHA256_HEX.test(definitionHash ?? "") || !SHA256_HEX.test(namespaceId ?? "") || !CLASS_NAME.test(className ?? "") ||
        typeof objectId !== "string" || !SHA256_HEX.test(objectId)) {
      throw new Error("Invalid Durable Object call");
    }
    const headers = new Headers(request.headers);
    headers.delete(DO_OBJECT_HEADER);
    const fromObject = caller !== undefined && caller !== null;
    const callSequence = !fromObject && requiresChildSequence ? nextChildSequence(parentInvocationId) : 0;
    return this.env.SERVER.fetch(new Request(request, { headers }), {
      cf: {
        workerId, versionId, contentHash, definitionHash,
        treeId: fromObject ? "" : treeId, parentInvocationId: fromObject ? "" : parentInvocationId,
        callSequence, orderSequence: !fromObject && requiresChildSequence ? (orderSequence || callSequence) : 0,
        expiresAtMonotonicMs: fromObject ? 0 : expiresAtMonotonicMs, sequenceMapSize: nextChildSequences.size, entrypoint: "",
        durableObject: { namespaceId, className, objectId, database: database ?? "",
          callerNamespaceId: fromObject ? caller.namespaceId : "", callerObjectId: fromObject ? caller.objectId : "" },
      },
    });
  }
}

// A version's Durable Object Bindings for its isolate: each with a DurableNamespace channel of the caller (none for a
// namespace that cannot be used).
function durableBindings(descriptor, ctx, caller) {
  return (descriptor.durableObjects ?? []).map(binding => ({ binding: binding.name, className: binding.className,
    namespaceId: binding.namespaceId, database: binding.database ?? "", objectId: binding.objectId ?? "",
    fetcher: binding.namespaceId ? ctx.exports.DurableNamespace({ props: {
      workerId: descriptor.workerId, versionId: descriptor.versionId, contentHash: descriptor.contentHash,
      definitionHash: descriptor.definitionHash, namespaceId: binding.namespaceId, className: binding.className,
      database: binding.database ?? "", ...caller,
    } }) : null }));
}

// The code of a facet's failure (the node's codes): the storage layer could not make a write durable, a limit ended
// it -- workerd's own error, not one the class threw (`remote`) --, or anything else. A write whose frames the node
// did not confirm fails with STORAGE_UNAVAILABLE, and the object is rebuilt from its replicated history before it
// serves anything again (docs/guide/errors.md).
function facetCode(error) {
  const reason = String(error?.message ?? error);
  if (reason.includes("STORAGE_UNAVAILABLE")) return "STORAGE_UNAVAILABLE";
  if (reason.includes("DO_OBJECT_TOO_LARGE")) return "DO_OBJECT_TOO_LARGE";
  if (reason.includes("QUOTA_EXCEEDED")) return "QUOTA_EXCEEDED";
  if (error?.remote === true) return "EXECUTION_FAILED";
  return reason.includes("CPU_LIMIT_EXCEEDED") ? "CPU_LIMIT_EXCEEDED"
    : reason.includes("MEMORY_LIMIT_EXCEEDED") ? "MEMORY_LIMIT_EXCEEDED" : "EXECUTION_FAILED";
}

// A Durable Object (docs/compatibility.md, section 7) on a durable process. Each object is a Host -- this runtime's own
// SQLite-backed class, its id HOST.idFromName(<namespace id>/<object id>/<data epoch>) -- that runs the Worker's class as its facet
// "app", with the facet's own SQLite. The class comes from the Worker Loader's isolate of the version, under the same
// loader key as the version's stateless calls, so the tenant's code runs in that dynamic isolate with its limits: CPU
// topped up at every event, memory per isolate. The Host remembers the key its facet started with: a request of another
// version (a version activated since, or other limits) aborts the facet and starts the new class on the same storage. A
// request over a limit aborts it too and answers 503 with the limit's code; the next request starts it again, from a
// new isolate (the old one was revoked, which ends every object of the version in this process). An exception the
// class throws leaves the object as it is: 500 EXECUTION_FAILED with its message. Requests reach the facet with the
// tenant's cf and DO_HOPS_HEADER, how many Durable Object calls led to them.
export class Host extends DurableObject {
  async fetch(request) {
    const descriptor = descriptorFrom(request);
    const target = descriptor.durableObject;
    if (!target) throw new Error("Missing Durable Object target");
    // A database's object runs this runtime's own SQL Worker class, so its facet does not depend on a version; a
    // Worker's class does, and a version activated since starts the new class on the same storage.
    const key = target.database ? "database#" + target.database : loaderKey(descriptor) + "#" + target.className;
    if (this.facetKey !== undefined && this.facetKey !== key) this.abortFacet("the Worker's active version changed");
    this.facetKey = key;
    try {
      const facet = this.ctx.facets.get("app", () => this.start(descriptor));
      const headers = new Headers(request.headers);
      headers.set(DO_HOPS_HEADER, String(target.hops ?? 0));
      // The node's own request: the object's alarm. Only the node can send one (a tenant never reaches the Host),
      // and the headers come from the descriptor, never from the request the node relayed.
      // A 64-bit field comes through as a string of decimal digits (Cap'n Proto's JSON).
      const alarmAtUnixMs = Number(target.alarmAtUnixMs ?? 0);
      if (alarmAtUnixMs > 0) {
        headers.set(DO_ALARM_HEADER, String(alarmAtUnixMs));
        headers.set(DO_ALARM_RETRY_HEADER, String(Number(target.alarmRetryCount ?? 0)));
      } else {
        headers.delete(DO_ALARM_HEADER);
        headers.delete(DO_ALARM_RETRY_HEADER);
      }
      return await facet.fetch(new Request(request, { headers, cf: tenantCf(descriptor, this.env) }));
    } catch (error) {
      const reason = String(error?.message ?? error);
      const code = facetCode(error);
      // A limit ended the class's isolate, or workerd reset the object: the facet goes, and the next request starts it
      // again -- from a new isolate when the old one was revoked, which ends every object of the version here.
      if (code !== "EXECUTION_FAILED" || error?.durableObjectReset === true) {
        this.abortFacet("the Durable Object's class failed: " + reason.slice(0, 200));
        return Response.json({ error: code === "CPU_LIMIT_EXCEEDED" ? "CPU limit exceeded during execution" :
          code === "MEMORY_LIMIT_EXCEEDED" ? "Memory limit exceeded during execution" :
          code === "STORAGE_UNAVAILABLE" ? "Durable Object storage is unavailable" :
          code === "DO_OBJECT_TOO_LARGE" ? "Durable Object is too large" :
          code === "QUOTA_EXCEEDED" ? "the tenant's Durable Object storage quota is exhausted" : "Durable Object reset",
          code, reason: reason.slice(0, 300) },
          { status: code === "DO_OBJECT_TOO_LARGE" ? 413 : code === "QUOTA_EXCEEDED" ? 422 : 503 });
      }
      // The class threw: the object stays, as on Cloudflare -- the same instance takes the next request -- and the
      // caller gets the exception's message.
      return Response.json({ error: "Durable Object execution failed", code: "EXECUTION_FAILED", reason: reason.slice(0, 300) },
        { status: 500 });
    }
  }
  // The object as a capability, for a method call on its stub: the facet is started exactly as a request starts it
  // (the same loader key, the same class, a version activated since aborting the old facet), and the capability is
  // the object's own, from the facet's isolate -- so a method call is delivered there and its arguments are decoded
  // there. A method call is an event of the object like a request: workerd tops up its CPU and its lines are the
  // event's.
  async rpcTarget(envelope) {
    const descriptor = descriptorFrom({ cf: JSON.parse(envelope), headers: new Headers() });
    const target = descriptor.durableObject;
    if (!target) throw new Error("Missing Durable Object target");
    if (target.database) throw new Error("a database has no methods");
    const key = loaderKey(descriptor) + "#" + target.className;
    if (this.facetKey !== undefined && this.facetKey !== key) this.abortFacet("the Worker's active version changed");
    this.facetKey = key;
    try {
      const facet = this.ctx.facets.get("app", () => this.start(descriptor));
      return await facet.__coworkerdRpcTarget;
    } catch (error) {
      const reason = String(error?.message ?? error);
      const code = facetCode(error);
      if (code !== "EXECUTION_FAILED" || error?.durableObjectReset === true) {
        this.abortFacet("the Durable Object's class failed: " + reason.slice(0, 200));
      }
      throw new Error(code + ": " + reason.slice(0, 300));
    }
  }

  abortFacet(reason) {
    try { this.ctx.facets.abort("app", new Error(reason)); } catch {}
  }
  start(descriptor) {
    const target = descriptor.durableObject, env = this.env, ctx = this.ctx;
    // A database: this runtime's own SQL Worker class, taken from the SQL_CLASS Binding (a Durable Object class
    // Binding without a namespace of its own, which is what a facet's class may be). No tenant code is loaded.
    if (target.database) return { class: env.SQL_CLASS, id: target.objectId };
    const worker = env.LOADER.get(loaderKey(descriptor), () => makeWorkerCode(descriptor, env, ctx));
    const durableObjects = durableBindings(descriptor, ctx, { caller: { namespaceId: target.namespaceId, objectId: target.objectId } });
    return { class: worker.getDurableObjectClass(DURABLE_EXPORT_PREFIX + target.className, { props: { durableObjects } }),
      id: target.objectId };
  }
}

// The platform's own SQL Worker class: a tenant's database is one object of it (docs/compatibility.md, section 7).
// It answers the protocol workerd's own D1 wrapper speaks -- POST /query and /execute with {sql, params} or an array
// of them and a resultsFormat, answered with {success, meta, results} -- so the same statements reach it from a
// d1_databases Binding and from the HTTP SQL interface of /v1. Its storage is the object's own SQLite, which the
// node replicates and rebuilds like any other object's.
export class SqlDatabase extends DurableObject {
  async fetch(request) {
    // The node's own alarm request: a database has no alarm handler, so the alarm goes rather than being retried.
    const scheduled = request.headers.get(DO_ALARM_HEADER);
    if (scheduled !== null) {
      const held = await this.ctx.storage.getAlarm();
      if (held !== null && held !== undefined) await this.ctx.storage.deleteAlarm();
      return Response.json({ outcome: "fired", atUnixMs: 0, reason: "a database has no alarm() handler" });
    }
    const { pathname, searchParams } = new URL(request.url);
    const query = pathname === "/query", execute = pathname === "/execute";
    if (request.method !== "POST" || (!query && !execute)) {
      return Response.json({ error: "Not found" }, { status: 404 });
    }
    let body;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "the body of a statement is JSON" }, { status: 400 });
    }
    const asked = searchParams.get("resultsFormat");
    const resultsFormat = asked === "ROWS_AND_COLUMNS" ? "ROWS_AND_COLUMNS" : asked === "NONE" ? "NONE" : "ARRAY_OF_OBJECTS";
    const one = (statement) => {
      try {
        return this.runStatement(statement, resultsFormat);
      } catch (error) {
        // What D1 does with a failing statement: the answer says so, it is not an exception of the object.
        return { success: false, error: String(error instanceof Error ? error.message : error) };
      }
    };
    return Response.json(Array.isArray(body) ? body.map(one) : one(body));
  }

  runStatement(statement, resultsFormat) {
    const { sql, params = [] } = statement ?? {};
    if (typeof sql !== "string") throw new Error("a statement names its sql");
    const sqlite = this.ctx.storage.sql;
    const counters = sqlite.prepare("SELECT total_changes() AS changes, last_insert_rowid() AS last_row_id");
    const sizeBefore = sqlite.databaseSize;
    const [[changesBefore, lastRowBefore]] = Array.from(counters().raw());
    const cursor = sqlite.prepare(sql)(...params);
    const columns = cursor.columnNames;
    const rows = Array.from(cursor.raw());
    const results = resultsFormat === "ROWS_AND_COLUMNS"
      ? { columns, rows }
      : rows.map((row) => Object.fromEntries(columns.map((column, i) => [column, row[i]])));
    const [[changesAfter, lastRowAfter]] = Array.from(counters().raw());
    const sizeAfter = sqlite.databaseSize;
    const changes = changesAfter - changesBefore;
    return {
      success: true,
      results,
      meta: {
        duration: 0,
        changes,
        last_row_id: lastRowAfter,
        changed_db: changes !== 0 || lastRowAfter !== lastRowBefore || sizeAfter !== sizeBefore,
        size_after: sizeAfter,
        rows_read: cursor.rowsRead,
        rows_written: cursor.rowsWritten,
      },
    };
  }
}

export class BoundWorker extends WorkerEntrypoint {
  // A method call on this Binding: the envelope this shim holds -- the same fields its `fetch` puts in cf -- names
  // the one method of a session to the node over the SERVER channel, and the node answers with the target's own
  // entrypoint (node/main.c++, RpcBindTarget). The tenant's call is pipelined onto it, so its arguments are
  // serialized in the tenant's isolate and decoded in the target's; nothing in between reads them.
  rpcTarget() {
    const envelope = this.envelope();
    if (envelope.assets === true) throw new Error("the ASSETS Binding has no methods");
    return this.env.SERVER[JSON.stringify(envelope)]();
  }

  envelope() {
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
    this.check();
    const callSequence = requiresChildSequence ? nextChildSequence(parentInvocationId) : 0;
    return {
      workerId, versionId, contentHash, definitionHash, treeId, parentInvocationId,
      callSequence, orderSequence: requiresChildSequence ? (orderSequence || callSequence) : 0,
      expiresAtMonotonicMs, sequenceMapSize: nextChildSequences.size, entrypoint: entrypoint ?? "",
      ...(assets === true ? { assets: true } : {}),
    };
  }

  check() {
    const { workerId, versionId, contentHash, definitionHash, treeId, parentInvocationId, orderSequence,
      expiresAtMonotonicMs, requiresChildSequence } = this.ctx.props;
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
  }

  async fetch(request) {
    const { workerId, versionId, contentHash, definitionHash, treeId, parentInvocationId, orderSequence,
      expiresAtMonotonicMs, requiresChildSequence, entrypoint, assets } = this.ctx.props;
    this.check();
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
    const props = descriptor.format === "v2" ? { trial: true, entrypoint: descriptor.entrypoint || "default",
      classes: descriptor.durableObjectClasses ?? [] } : { [TRIAL_PROP]: true };
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

// This runtime's entrypoint. A request is a `fetch` of it; a method call on a Binding or on a Durable Object stub is
// a JS RPC session on it, whose node end reads __coworkerdRpcBind naming the call's descriptor (node/main.c++,
// RpcDescriptorTarget) and hands the answer -- the version's entrypoint, or the object's -- to the caller.
export default class extends WorkerEntrypoint {
  fetch(request) { return serve(request, this.env, this.ctx); }
  get __coworkerdRpcBind() { return rpcBind(this.env, this.ctx); }
}

// The descriptor of a method call, as a property name. Only this runtime's node sends one, over the channel its
// SERVER Binding opens; a tenant reaches neither.
function rpcBind(env, ctx) {
  return new Proxy({}, {
    getOwnPropertyDescriptor() { return { value: undefined, writable: true, enumerable: true, configurable: true }; },
    has() { return true; },
    get(_target, name) {
      if (typeof name !== "string" || !name.startsWith("{")) return undefined;
      return rpcEntrypoint(JSON.parse(name), env, ctx);
    },
  });
}

// The capability a method call is delivered to: the object's own, from its Host, or the version's entrypoint, from the
// isolate the Worker Loader keeps under this version's loader key -- the same isolate its requests run in.
async function rpcEntrypoint(value, env, ctx) {
  const descriptor = descriptorFrom({ cf: value, headers: new Headers() });
  const target = descriptor.durableObject;
  if (target) {
    if (!env.HOST) throw new Error("A Durable Object method call reached a process without Durable Objects");
    const dataEpoch = String(target.dataEpoch ?? "0");
    const host = env.HOST.get(env.HOST.idFromName(target.namespaceId + "/" + target.objectId +
      (dataEpoch === "0" ? "" : "/" + dataEpoch)));
    return host.rpcTarget(JSON.stringify(value));
  }
  const worker = env.LOADER.get(loaderKey(descriptor), () => makeWorkerCode(descriptor, env, ctx));
  return worker.getEntrypoint(undefined, { props: callProps(descriptor, ctx) }).__coworkerdRpcTarget;
}

async function serve(request, env, ctx) {
  if (new URL(request.url).pathname === "/__coworkerd/probe" && request.cf?.invocationId === undefined) {
    return new Response(`${env.PROCESS_ID}:${env.PROCESS_GENERATION}`);
  }
  const descriptor = descriptorFrom(request);
  if (descriptor.trial === true) return trialLoad(descriptor, env, ctx);
  // A request to an object: its Host, on this durable process. The Host is named by the object and the epoch since which
  // this node's copy of the object's shard is the one (the node's data epoch for it): a shard this node took over from
  // another node starts its objects from empty, one it took back from itself keeps them.
  if (descriptor.durableObject) {
    if (!env.HOST) throw new Error("A Durable Object request reached a process without Durable Objects");
    const target = descriptor.durableObject;
    const dataEpoch = String(target.dataEpoch ?? "0");
    return env.HOST.get(env.HOST.idFromName(target.namespaceId + "/" + target.objectId +
      (dataEpoch === "0" ? "" : "/" + dataEpoch))).fetch(request);
  }
  const pathTiming = env.PATH_TIMING === "1" ? { receivedAtUnixMs: Date.now(), receivedMonotonicMs: performance.now() } : undefined;
  const key = loaderKey(descriptor);
  let coldStart = false;
  // The load check before this dispatch loaded the version for this call: its cold start.
  if (descriptor.coldLoaded === true) coldStart = true;
  const worker = env.LOADER.get(key, async () => {
    coldStart = true;
    return makeWorkerCode(descriptor, env, ctx);
  });
  const props = callProps(descriptor, ctx);
  if (pathTiming) { pathTiming.dispatchedAtUnixMs = Date.now(); pathTiming.dispatchedMonotonicMs = performance.now(); }
  return worker.getEntrypoint(undefined, { props }).fetch(request, { cf: tenantCf(descriptor, env) }).finally(() => {
    console.log("[runtime-execution] " + JSON.stringify({
      serverId: descriptor.serverId, processId: env.PROCESS_ID,
      processGeneration: Number(env.PROCESS_GENERATION), invocationId: descriptor.invocationId,
      treeId: descriptor.treeId, target: `${descriptor.workerId}@${descriptor.versionId}`,
      coldStart, observedAtUnixMs: Date.now(),
      ...(pathTiming ? { pathTiming: { ...pathTiming, returnedAtUnixMs: Date.now(), returnedMonotonicMs: performance.now() } } : {}),
    }));
  });
}

// The props of one call of a version: its Service Bindings and its Durable Object Bindings, each a channel of this
// runtime with the call's own envelope, and which entrypoint runs. A method call gets the same props as a request,
// so the target's own Bindings work the same way inside it.
function callProps(descriptor, ctx) {
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
  // The version's Durable Object Bindings, for this call: their calls are Binding calls of its call tree.
  const durableObjects = durableBindings(descriptor, ctx, { treeId: descriptor.treeId, parentInvocationId: descriptor.invocationId,
    orderSequence: descriptor.orderSequence, expiresAtMonotonicMs: descriptor.lease.expiresAtMonotonicMs,
    requiresChildSequence: descriptor.requiresChildSequence });
  return descriptor.format === "v2" ? { bindings, entrypoint: descriptor.entrypoint || "default",
    ...(durableObjects.length ? { durableObjects } : {}) } : bindings;
}

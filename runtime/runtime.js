import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { DO_ALARM_HEADER } from "./sql-database.js";
export { SqlDatabase } from "./sql-database.js";

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
    const { workerId, versionId, callerInvocation } = this.ctx.props;
    headers.set(EGRESS_SOURCE_HEADER, this.env.EGRESS_TOKEN + " " +
      JSON.stringify({ workerId, versionId, invocationId: callerInvocation?.invocationId }));
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
// Whether a call naming this entrypoint gets as far as tenant code: the bridge's own
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
// on a request of its own; the runtime sets them from the trusted descriptor alone.
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
  const failed = (one) => {
    const message = one.reason ?? one.error;
    const nativeSqlReason = one.code === "SQL_LIMIT_EXCEEDED" && typeof message === "string"
      && message.startsWith("SQL_LIMIT_EXCEEDED:");
    return { success: false, error: nativeSqlReason ? message : one.code ? one.code + ": " + message : one.error };
  };
  return Array.isArray(answer)
    ? answer.map((one) => (one?.error ? failed(one) : one))
    : answer?.error ? failed(answer) : answer;
}
async function __coworkerdD1SendOrThrow(entry, endpoint, query, params, resultsFormat) {
  const results = await __coworkerdD1Send(entry, endpoint, query, params, resultsFormat);
  const list = Array.isArray(results) ? results : [results];
  const failed = list.findIndex(one => !one?.success);
  if (failed !== -1) {
    const message = list[failed]?.error ?? "Something went wrong";
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
  if (held !== scheduledAtUnixMs) return Response.json({ outcome: "notDue", atUnixMs: held });
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
  if (typeof base !== "function") throw new Error("Durable Object class " + name + " is not a class of this version");
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
      if (typeof fetch !== "function")
        throw new Error("Durable Object class " + name + " has no fetch handler");
      return withEnv(env, () => fetch.apply(this, args));
    } });
    Object.defineProperty(wrapped.prototype, "__coworkerdAlarm", {
      configurable: true,
      value: function (scheduledAtUnixMs, retryCount) {
        const env = __coworkerdObjectEnvs.get(this);
        return withEnv(env, async () => (await __coworkerdAlarm(this, Number(scheduledAtUnixMs), Number(retryCount))).json());
      },
    });
    // The object as a capability for a method call on its stub. The node hands it to the caller, so workerd's
    // own RPC delivers the call to this instance.
    Object.defineProperty(wrapped.prototype, "__coworkerdRpcTarget", { configurable: true, get: function () {
      const env = __coworkerdObjectEnvs.get(this);
      return withEnv(env, () => __coworkerdRpcFacade(this, env, ["__coworkerdAlarm"]));
    } });
    Object.defineProperty(wrapped, "name", { value: name });
    return wrapped;
  } catch (error) {
    throw new Error("Durable Object class " + name + " cannot be loaded: " + String(error?.message ?? error));
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
function __coworkerdRpcFacade(instance, env, hidden = []) {
  class Facade extends RpcTarget {}
  for (const name of __coworkerdMemberNames(instance)) {
    if (hidden.includes(name)) continue;
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
function __coworkerdRpcPath(open, path) {
  const call = async (...args) => {
    let target = open();
    for (const step of path.slice(0, -1)) target = target[step];
    return await target[path[path.length - 1]](...args);
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
      (target.alarmAtUnixMs === undefined || DECIMAL.test(String(target.alarmAtUnixMs))) &&
      limit(target.alarmFacetId) && limit(target.alarmRetryCount))) return false;
  if (classes !== undefined && !(Array.isArray(classes) && classes.every(name => typeof name === "string" && CLASS_NAME.test(name)))) return false;
  return true;
}

function descriptorFrom(request) {
  const value = request.cf;
  const remainingSubrequests = Number(value?.lease?.remainingSubrequests);
  const subrequestLimit = Number(value?.lease?.subrequestLimit ?? value?.lease?.grant);
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
    remainingSubrequests > 10_000_000 ||
    !Number.isInteger(subrequestLimit) ||
    subrequestLimit < 0 ||
    subrequestLimit > 10_000_000 ||
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
    lease: { ...value.lease, remainingSubrequests, subrequestLimit, grant: subrequestLimit, expiresAtMonotonicMs },
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
      const binary = atob(encoded), bytes = new Uint8Array(binary.length);
      // Avoid the iterator's intermediate number array for large modules. Keep
      // code verification responsive to probes and other tenants on this loop.
      for (let first = 0; first < binary.length; first += 65536) {
        const end = Math.min(first + 65536, binary.length);
        for (let index = first; index < end; ++index) bytes[index] = binary.charCodeAt(index);
        if (binary.length > 262144 && end < binary.length)
          await new Promise(resolve => setTimeout(resolve, 0));
      }
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

async function makeWorkerCode(descriptor, env, ctx, root = false) {
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
    // A trial load shares this isolate's loader key but has no bindings. Include the namespace helpers here;
    // install bindings from each call's props, independently of the classes declared by this Worker.
    const bridge = `import { __coworkerdLog } from "./${PRELUDE_MODULE}";
import { withEnv, DurableObject, RpcTarget, WorkerEntrypoint } from "cloudflare:workers";
import * as tenant from ${JSON.stringify("./" + descriptor.mainModule)};
${root ? `export * from ${JSON.stringify("./" + descriptor.mainModule)};\n` : ""}${TRIAL_SOURCE}
${RPC_SOURCE}
${DURABLE_SOURCE}
${root ? classes.map(name => `const ${DURABLE_EXPORT_PREFIX}${name} = __coworkerdDurableClass(tenant, ${JSON.stringify(name)});\nexport { ${DURABLE_EXPORT_PREFIX}${name}, ${DURABLE_EXPORT_PREFIX}${name} as ${name} };\n`).join("") : ""}
${root ? `const __coworkerdRootTrialNamespace = { ...tenant, ${classes.map(name => `${JSON.stringify(name)}: ${DURABLE_EXPORT_PREFIX}${name}`).join(", ")} };` : `let __coworkerdRootTrialNamespace = tenant, __coworkerdRootTrialError;
if (${JSON.stringify(classes)}.length) {
  try { __coworkerdRootTrialNamespace = { ...tenant,
    ...Object.fromEntries(${JSON.stringify(classes)}.map(name => [name, __coworkerdDurableClass(tenant, name)])) }; }
  catch (error) { __coworkerdRootTrialError = {name: error.name,
    message: "Failed to start Worker:\\nUncaught " + String(error.stack || (error.name + ": " + error.message))}; }
}`}
function __coworkerdEnvOf(variables, props) {
  const { bindings, durableObjects } = props;
  return { ...variables, ...__coworkerdServices(bindings), ...__coworkerdNamespaces(durableObjects) };
}
export default class extends WorkerEntrypoint {
  fetch(request) {
    const variables = this.env, ctx = this.ctx;
    if (ctx.props.trial === true) {
      ${root ? "" : `if (ctx.props.rootTrial === true && __coworkerdRootTrialError)
        return Response.json({ok:false, stage:"load", ...__coworkerdRootTrialError});`}
      return __coworkerdTrial(ctx.props.rootTrial === true ? __coworkerdRootTrialNamespace : tenant,
        true, ctx.props.entrypoint, ctx.props.classes);
    }
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
// from this runtime: a stateless call's Bindings, or an object's plan) and the caller -- a stateless Worker's call tree,
// or the calling object.
export class DurableNamespace extends WorkerEntrypoint {
  // A method call on a stub of this namespace: the same envelope its `fetch` puts in cf, with the object the stub
  // names, as the one method of a session to the node. The node finds the shard's owner (the epoch and the fencing as
  // for a request) and answers with the object's own capability, which the tenant's call is then pipelined onto.
  #envelope(objectId) {
    const { workerId, versionId, contentHash, definitionHash, namespaceId, className, database, caller,
      callerInvocation } = this.ctx.props;
    if (typeof workerId !== "string" || typeof versionId !== "string" || !SHA256_HEX.test(contentHash ?? "") ||
        !SHA256_HEX.test(definitionHash ?? "") || !SHA256_HEX.test(namespaceId ?? "") || !CLASS_NAME.test(className ?? "") ||
        typeof objectId !== "string" || !SHA256_HEX.test(objectId)) {
      throw new Error("Invalid Durable Object call");
    }
    if (caller && !callerInvocation?.invocationId)
      throw new Error("LEASE_EXPIRED: the Durable Object has no active invocation");
    const treeId = callerInvocation?.treeId ?? this.ctx.props.treeId;
    const parentInvocationId = callerInvocation?.invocationId ?? this.ctx.props.parentInvocationId;
    const orderSequence = callerInvocation?.orderSequence ?? this.ctx.props.orderSequence;
    const expiresAtMonotonicMs = callerInvocation?.leaseDeadline
      ? Number(callerInvocation.leaseDeadline) : this.ctx.props.expiresAtMonotonicMs;
    const requiresChildSequence = callerInvocation?.requiresChildSequence ?? this.ctx.props.requiresChildSequence;
    const callSequence = requiresChildSequence ? nextChildSequence(parentInvocationId) : 0;
    return {
      workerId, versionId, contentHash, definitionHash, treeId, parentInvocationId, callSequence,
      orderSequence: requiresChildSequence ? (orderSequence || callSequence) : 0,
      expiresAtMonotonicMs, sequenceMapSize: nextChildSequences.size, entrypoint: "",
      durableObject: { namespaceId, className, objectId, database: database ?? "",
        callerNamespaceId: caller?.namespaceId ?? "", callerObjectId: caller?.objectId ?? "" },
    };
  }

  rpcTarget(objectId) {
    return this.env.SERVER[JSON.stringify(this.#envelope(objectId))]();
  }

  async fetch(request) {
    const cf = this.#envelope(request.headers.get(DO_OBJECT_HEADER));
    const headers = new Headers(request.headers);
    headers.delete(DO_OBJECT_HEADER);
    return this.env.SERVER.fetch(new Request(request, { headers }), { cf });
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
function cleanDurableReason(error, secrets = []) {
  let text = String(error?.message ?? error).split("\n")
    .filter((line, i) => i === 0 || !line.trimStart().startsWith("at ")).join("\n");
  const values = secrets.flatMap(({ value }) => typeof value === "string" && value.length
    ? [value, JSON.stringify(value).slice(1, -1)] : []).sort((a, b) => b.length - a.length);
  for (const value of values) text = text.split(value).join("[redacted]");
  const starts = " \t\n\"'(<[=,", ends = " \t\n\"')>],";
  let cleaned = "";
  for (let i = 0; i < text.length;) {
    const boundary = i === 0 || starts.includes(text[i - 1]);
    if (boundary && (text.startsWith("file:", i) || (text[i] === "/" && text[i + 1] !== "/"))) {
      let end = i;
      while (end < text.length && !ends.includes(text[end])) ++end;
      const token = text.slice(i, end);
      if (token.startsWith("file:") || token.indexOf("/", 1) >= 0) { cleaned += "<path>"; i = end; continue; }
    }
    cleaned += text[i++];
  }
  if (cleaned.includes("not valid JSON")) cleaned = cleaned.replace(/"[^"\n]*"/g, '"…"');
  cleaned = cleaned.replaceAll("__coworkerd_tenant__.js", "script").replace(/[\x00-\x1f]/g, " ");
  const bytes = new TextEncoder().encode(cleaned);
  if (bytes.length <= 400) return cleaned;
  let end = 397; // Leave room for the UTF-8 ellipsis within the 400-byte limit.
  while (end > 0 && (bytes[end] & 0xc0) === 0x80) --end;
  return new TextDecoder().decode(bytes.subarray(0, end)) + "…";
}

function durableCode(error) {
  const reason = String(error?.message ?? error);
  if (reason.includes("STORAGE_UNAVAILABLE")) return "STORAGE_UNAVAILABLE";
  if (reason.includes("DO_OBJECT_TOO_LARGE")) return "DO_OBJECT_TOO_LARGE";
  if (reason.includes("QUOTA_EXCEEDED")) return "QUOTA_EXCEEDED";
  if (error?.remote === true) return "EXECUTION_FAILED";
  return reason.includes("CPU_LIMIT_EXCEEDED") ? "CPU_LIMIT_EXCEEDED"
    : reason.includes("MEMORY_LIMIT_EXCEEDED") ? "MEMORY_LIMIT_EXCEEDED" : "EXECUTION_FAILED";
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

// The capability a method call is delivered to: the selected object's own, or the version's entrypoint, from the
// isolate the Worker Loader keeps under this version's loader key -- the same isolate its requests run in.
function rootModule(name, value) {
  if (typeof value === "string") return { name, source: value };
  const type = Object.keys(value)[0], source = value[type];
  if (type === "json") return { name, type, source: JSON.stringify(source) };
  if (type === "data" || type === "wasm") {
    let binary = "";
    for (let at = 0; at < source.length; at += 8192) binary += String.fromCharCode(...source.subarray(at, at + 8192));
    return { name, type, source: btoa(binary) };
  }
  return { name, type, source };
}

const rootPreparationErrors = new WeakSet();
const rootTenantCodeErrors = new WeakSet();
const rootMissingPlans = new WeakSet();
function rootManagementFailure(error, missing = false) {
  const value = error && typeof error === "object" ? error : new Error(String(error));
  rootPreparationErrors.add(value);
  if (missing) rootMissingPlans.add(value);
  return value;
}
const rootPlanKeys = new Map();
const MAX_ROOT_PLAN_KEYS = 256;
async function selectedRoot(descriptor, env, ctx, preparationNonce) {
  return prepareSelectedRoot(descriptor, env, ctx, preparationNonce);
}

async function prepareSelectedRoot(descriptor, env, ctx, preparationNonce) {
  if (!descriptor.rootPlan || !descriptor.rootSelection || descriptor.rootSelection.dispatchChecked !== true)
    throw new Error("Missing trusted root dispatch");
  const base = descriptor.rootPlan;
  const manage = async (path, value) => {
    let response, reply;
    try {
      response = await env.ROOTS.fetch("http://roots/" + path, { method: "POST", body: JSON.stringify(value) });
      reply = await response.json();
    } catch (error) { throw rootManagementFailure(error); }
    if (!response.ok) {
      const error = new Error(reply?.error?.message ?? "Root plan " + path + " failed: HTTP " + response.status);
      if (reply?.error?.kind === "tenant-code") {
        rootTenantCodeErrors.add(error);
        throw error;
      }
      throw rootManagementFailure(error, reply?.error?.kind === "plan-missing");
    }
    if (!reply || typeof reply !== "object" ||
        (path === "lookup" && reply.plan !== null && typeof reply.plan !== "string") ||
        (path === "install" && (typeof reply.plan !== "string" || !reply.info || typeof reply.info !== "object")))
      throw rootManagementFailure(new Error("Invalid root plan " + path + " reply"));
    return reply;
  };
  const cacheKey = JSON.stringify(base);
  const install = async () => {
    let deployment = base;
    if (base.kind === "tenant") {
      let code;
      try { code = await makeWorkerCode(descriptor, env, ctx, true); }
      catch (error) { throw rootManagementFailure(error); }
      const names = [code.mainModule, ...Object.keys(code.modules).filter(name => name !== code.mainModule)];
      deployment = { ...base, compatibilityDate: code.compatibilityDate, compatibilityFlags: code.compatibilityFlags,
        environment: code.env, modules: names.map(name => rootModule(name, code.modules[name])) };
    }
    let installed;
    try { installed = await manage("install", deployment); }
    catch (error) {
      if (!error || typeof error !== "object" || !rootMissingPlans.has(error)) throw error;
      installed = await manage("install", deployment);
    }
    const plan = installed.plan;
    console.log("[root-plan] " + JSON.stringify({ plan, kind: base.kind, tenant: base.tenant, workerId: base.workerId,
      versionId: base.versionId, creationRecord: base.creationRecord?.present === true,
      platformClasses: [installed.info].filter(value => value.kind === "platform")
        .map(({ tenant, workerId, configuredWorker, platformClass }) => ({ tenant, workerId, configuredWorker, platformClass })) }));
    return plan;
  };
  const remember = plan => {
    if (typeof plan !== "string") throw rootManagementFailure(new Error("Invalid root plan reply"));
    rootPlanKeys.delete(cacheKey);
    rootPlanKeys.set(cacheKey, plan);
    if (rootPlanKeys.size > MAX_ROOT_PLAN_KEYS) rootPlanKeys.delete(rootPlanKeys.keys().next().value);
    return plan;
  };
  let plan = rootPlanKeys.get(cacheKey);
  if (plan === undefined) {
    ({ plan } = await manage("lookup", base));
    plan = remember(plan === null ? await install() : plan);
  } else remember(plan);
  const select = async () => {
    const selection = { ...descriptor.rootSelection, plan, tenantCf: tenantCf(descriptor, env), preparationNonce,
      leaseDeadline: String(descriptor.lease.expiresAtMonotonicMs), requiresChildSequence: descriptor.requiresChildSequence };
    try {
      return await env.ROOTS.getRoot(JSON.stringify(selection));
    } catch (error) {
      // This private binding call never delivers an event. Its own missing-plan reply can refresh the key.
      throw rootManagementFailure(error, rootPreparationKind(error, preparationNonce) === "plan-missing");
    }
  };
  try { return await select(); }
  catch (error) {
    if (!error || typeof error !== "object" || !rootMissingPlans.has(error)) throw error;
    rootPlanKeys.delete(cacheKey);
    plan = remember(await install());
    return await select();
  }
}

function rootPreparationKind(error, nonce) {
  if (error && typeof error === "object" && rootTenantCodeErrors.has(error)) return "tenant-code";
  if (error && typeof error === "object" && rootPreparationErrors.has(error)) return "platform";
  const message = String(error?.message ?? error);
  for (const kind of ["platform", "plan-missing", "tenant-code"])
    if (nonce && message.includes("COWORKERD_ROOT_PREPARATION:" + nonce + ":" + kind + ":")) return kind;
  return null;
}

function rootFailureReason(error) {
  return String(error?.message ?? error).replace(
    /COWORKERD_ROOT_PREPARATION:[^:\s]+:(?:platform|plan-missing|tenant-code):\s*/g, "");
}

// Deployment validation runs the same generated root module as an object,
// on a durable process. It creates no object, namespace, or storage facet.
async function trialRoot(descriptor, env, ctx) {
  const started = performance.now();
  let report, stage = "load", codeStage = null;
  try {
    const base = descriptor.rootPlan;
    if (descriptor.trial !== true || descriptor.rootTrial !== true || descriptor.format !== "v2" ||
        base?.kind !== "tenant" || !Array.isArray(base.namespaces) || base.namespaces.length ||
        descriptor.rootSelection?.kind !== "plain" || descriptor.rootSelection.dispatchChecked !== true)
      throw new Error("Missing trusted root trial plan");
    const code = await makeWorkerCode(descriptor, env, ctx);
    const names = [code.mainModule, ...Object.keys(code.modules).filter(name => name !== code.mainModule)];
    const response = await env.ROOTS.fetch("http://roots/install", {method:"POST", body:JSON.stringify({
      ...base, mainModule:code.mainModule, compatibilityDate:code.compatibilityDate,
      compatibilityFlags:code.compatibilityFlags, environment:code.env,
      startupIdentity:JSON.stringify({workerId:descriptor.workerId, versionId:descriptor.versionId,
        invocationId:descriptor.invocationId}),
      modules:names.map(name => rootModule(name, code.modules[name]))})});
    const reply = await response.json();
    if (!response.ok || typeof reply?.plan !== "string") {
      const error = new Error(reply?.error?.message || "Root trial installation failed");
      if (typeof reply?.error?.name === "string") error.name = reply.error.name;
      if (reply?.error?.kind !== "tenant-code") throw staged("package", error);
      throw error;
    }
    const root = env.ROOTS.getPlain(JSON.stringify({...descriptor.rootSelection, plan:reply.plan,
      activePlan:false, trial:true, rootTrial:true, entrypoint:descriptor.entrypoint || "default",
      classes:descriptor.durableObjectClasses ?? []}));
    report = await (await root.fetch("http://coworkerd-root-trial/", {cf:tenantCf(descriptor, env)})).json();
  } catch (error) {
    if (typeof error?.coworkerdStage === "string") {stage = "code"; codeStage = stageOf(error);}
    report = {ok:false, stage, codeStage, name:typeof error?.name === "string" ? error.name : typeof error,
      message:typeof error?.message === "string" ? error.message : String(error)};
  }
  return Response.json({...report, rootBridgeChecked:true, loadMs:performance.now()-started});
}

async function serveRoot(request, descriptor, env, ctx) {
  const target = descriptor.durableObject;
  const preparationNonce = crypto.randomUUID();
  try {
    const root = await selectedRoot(descriptor, env, ctx, preparationNonce);
    const headers = new Headers(request.headers);
    headers.set(DO_HOPS_HEADER, String(target.hops ?? 0));
    headers.delete(DO_ALARM_HEADER);
    headers.delete(DO_ALARM_RETRY_HEADER);
    const scheduled = Number(target.alarmAtUnixMs ?? 0);
    if (scheduled > 0) {
      if (!target.database)
        return Response.json(await root.__coworkerdAlarm(scheduled, Number(target.alarmRetryCount ?? 0)));
      headers.set(DO_ALARM_HEADER, String(scheduled));
      headers.set(DO_ALARM_RETRY_HEADER, String(Number(target.alarmRetryCount ?? 0)));
    }
    return await root.fetch(new Request(request, { headers, cf: tenantCf(descriptor, env) }));
  } catch (error) {
    const preparation = rootPreparationKind(error, preparationNonce);
    const reason = cleanDurableReason({ message: rootFailureReason(error) }, descriptor.secrets);
    const sqlLimit = target.database && error?.code === "SQL_LIMIT_EXCEEDED";
    const code = preparation === "tenant-code" ? "EXECUTION_FAILED" :
      sqlLimit && !reason.includes("STORAGE_UNAVAILABLE") && !reason.includes("RESULT_UNCERTAIN")
        ? "SQL_LIMIT_EXCEEDED" : durableCode(error);
    if (preparation && preparation !== "tenant-code")
      return Response.json({ error: "Durable Object is unavailable", code: "DO_UNAVAILABLE", reason, retryable: true },
        { status: 503 });
    if (code !== "EXECUTION_FAILED" || error?.durableObjectReset === true)
      return Response.json({ error: code === "CPU_LIMIT_EXCEEDED" ? "CPU limit exceeded during execution" :
        code === "SQL_LIMIT_EXCEEDED" ? reason :
        code === "MEMORY_LIMIT_EXCEEDED" ? "Memory limit exceeded during execution" :
        code === "STORAGE_UNAVAILABLE" ? "Durable Object storage is unavailable" :
        code === "DO_OBJECT_TOO_LARGE" ? "Durable Object is too large" :
        code === "QUOTA_EXCEEDED" ? "the tenant's Durable Object storage quota is exhausted" : "Durable Object reset",
        code, reason }, { status: code === "DO_OBJECT_TOO_LARGE" ? 413 : code === "QUOTA_EXCEEDED" ? 422 : 503 });
    return Response.json({ error: "Durable Object execution failed", code: "EXECUTION_FAILED", reason }, { status: 500 });
  }
}

async function rpcEntrypoint(value, env, ctx) {
  const descriptor = descriptorFrom({ cf: value, headers: new Headers() });
  const target = descriptor.durableObject;
  if (target) {
    if (target.database) throw new Error("a database has no methods");
    const preparationNonce = crypto.randomUUID();
    try { return await (await selectedRoot(descriptor, env, ctx, preparationNonce)).__coworkerdRpcTarget; }
    catch (error) {
      const preparation = rootPreparationKind(error, preparationNonce);
      const reason = cleanDurableReason({ message: rootFailureReason(error) }, descriptor.secrets);
      const code = preparation === "tenant-code" ? "EXECUTION_FAILED" : durableCode(error);
      throw new Error((preparation && preparation !== "tenant-code" ? "DO_UNAVAILABLE" : code) +
        ": " + reason.slice(0, 300));
    }
  }
  throw new Error("a durable process requires a Durable Object target");
}

async function serve(request, env, ctx) {
  if (new URL(request.url).pathname === "/__coworkerd/root-maintenance" && request.cf?.invocationId === undefined && env.ROOTS) {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    const body = await request.json();
    if (!Array.isArray(body.namespaces) || body.namespaces.some(id => typeof id !== "string" || !/^[0-9a-f]{64}$/.test(id)))
      return new Response("Invalid root maintenance request", { status: 400 });
    for (const namespaceId of body.namespaces) {
      const response = await env.ROOTS.fetch("http://roots/remove-namespace", { method: "POST",
        body: JSON.stringify({ namespaceId, permanent: true }) });
      if (!response.ok) return response;
    }
    return env.ROOTS.fetch("http://roots/collect", { method: "POST" });
  }
  if (new URL(request.url).pathname === "/__coworkerd/probe" && request.cf?.invocationId === undefined) {
    return new Response(`${env.PROCESS_ID}:${env.PROCESS_GENERATION}`);
  }
  const descriptor = descriptorFrom(request);
  if (descriptor.trial === true && descriptor.rootTrial === true) return trialRoot(descriptor, env, ctx);
  if (descriptor.durableObject) return serveRoot(request, descriptor, env, ctx);
  return Response.json({code: "DO_UNAVAILABLE", error: "a durable process requires a Durable Object target"}, {status: 400});
}

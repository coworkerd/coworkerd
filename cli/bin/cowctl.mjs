#!/usr/bin/env node
// cowctl: deploy a Wrangler project to a Coworkerd cluster and look
// after it, through the control plane's /v1 API (`coworkerd-apiserver`, docs/control-plane-api.md). Run it in the
// project's directory. Every command acts within one tenant: --tenant, else cowctl.json's `tenant`.
//
// The control plane's address comes from cowctl.json (`cowctl init` writes it); the credential (a tenant's, or the
// administrator's for the tenant, credential, node and usage commands) only from the environment variable COWCTL_TOKEN
// or the file cowctl.json names (tokenFile), never from the command line. A failure prints one sentence and the control
// plane's reason (already cleaned there).
import { existsSync, readFileSync } from "node:fs";
import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Node.js 22.5 or newer: the Wrangler this package pins needs 22, and the adapter path.matchesGlob, new in 22.5.
const [NODE_MAJOR, NODE_MINOR] = process.versions.node.split(".").map(Number);
if (NODE_MAJOR < 22 || (NODE_MAJOR === 22 && NODE_MINOR < 5)) {
  console.error(`cowctl: Node.js 22.5 or newer is needed (this is ${process.version})`);
  process.exit(1);
}
const VERSION = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")).version;
// The Wrangler adapter (adapter.mjs) of this package, in the repository and in a release.
const ADAPTER = new URL("../adapter.mjs", import.meta.url);
const CONFIG_FILE = "cowctl.json";
const DEFAULTS = { controlPlane: "http://127.0.0.1:18090", ingress: "http://127.0.0.1:18080", tokenFile: "~/.cowctl/token" };
const WRANGLER_CONFIGS = ["wrangler.json", "wrangler.jsonc", "wrangler.toml"];
// A tenant's and a Worker's name in the API; a version keeps the deployment names.
const NAME = /^[a-z0-9][a-z0-9-]{0,62}$/;
const VERSION_NAME = /^[A-Za-z0-9._-]{1,255}$/;
const SECRET_NAME = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

const USAGE = `cowctl ${VERSION}: deploy a Wrangler project to a Coworkerd cluster

usage: cowctl <command> [options]              (in the project's directory)

  init --tenant T [--url URL] [--ingress URL] [--token-file FILE] [--worker NAME] [--route PREFIX] [--force]
                              write cowctl.json: the tenant, the control plane, the ingress, where the credential is
  deploy [--no-activate] [--version ID] [--route PREFIX] [--secret NAME]... [--no-wait]
                              build with Wrangler, upload as a new version, trial-load it and make it active; then
                              wait (at most 15 s) until every node serves it. --no-activate only uploads (no route,
                              no trial load: both come with the activation, cowctl rollback <version>)
  versions                    the Worker's versions, the active one marked
  rollback <version> [--no-wait]
                              make an earlier version active again (and wait until every node serves it)
  remove [--yes]              stop serving the Worker (its versions stay)
  secret put <NAME> [--from-file FILE] [--no-wait]
                              set a secret: the value from standard input (typed without echo) or the file
  secret list                 the secrets' names and lengths
  secret delete <NAME>
  status                      the cluster's nodes, their processes and versions
  logs [--follow] [--since 10m|2h|<ISO time>|<unix ms>] [--limit N] [--json]
                              the Worker's console output from every node, merged by time

  administrator (the administrator's credential):
  tenant create <name> [--hostname HOST]... [--quota NAME=N]... [--default-cpu-ms N] [--default-memory-mib N]
  tenant list | get <name> | delete <name> [--yes]
  tenant update <name> [--hostname HOST]... [--no-hostnames] [--quota NAME=N]... [--default-cpu-ms N] [--default-memory-mib N]
  credential create [--description TEXT] [--expires-in 30d]
                              a credential of the tenant (printed once; give it to the tenant)
  credential list | revoke <keyId>   (a tenant revokes its own credentials too)
  node list                   every node's state, processes, sandbox, suspects
  usage [--all] [--since 24h] [--until <time>] [--json]
                              hourly usage: requests, CPU ms, egress bytes, log lines, isolate time

  --tenant T selects the tenant (default: cowctl.json's tenant); --worker NAME the Worker (default: cowctl.json, else
  the Wrangler configuration's name). Names are [a-z0-9][a-z0-9-]{0,62}.
  The credential: COWCTL_TOKEN, else the file named by cowctl.json's tokenFile (default ~/.cowctl/token).
  COWCTL_CONTROL_PLANE overrides cowctl.json's controlPlane.`;

class Failure extends Error {
  constructor(message, { code = 1, detail } = {}) { super(message); this.exitCode = code; this.detail = detail; }
}
const usage = message => new Failure(message + "\n\n" + USAGE.split("\n").slice(2).join("\n"), { code: 2 });

// ---- options -----------------------------------------------------------------------------------------------
function parse(args, spec) {
  const options = {}, positional = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith("--")) { positional.push(arg); continue; }
    const [name, inline] = arg.slice(2).split(/=(.*)/s, 2);
    const kind = spec[name];
    if (!kind) throw usage(`unknown option --${name}`);
    if (kind === "flag") { if (inline !== undefined) throw usage(`--${name} takes no value`); options[name] = true; continue; }
    const value = inline ?? args[++i];
    if (value === undefined) throw usage(`--${name} needs a value`);
    if (kind === "list") (options[name] ??= []).push(value); else options[name] = value;
  }
  return { options, positional };
}

// ---- configuration ----------------------------------------------------------------------------------------
function findConfig(start = process.cwd()) {
  for (let directory = resolve(start); ; directory = dirname(directory)) {
    const file = join(directory, CONFIG_FILE);
    if (existsSync(file)) return file;
    if (dirname(directory) === directory) return null;
  }
}
function loadConfig() {
  const file = findConfig();
  let config = { ...DEFAULTS };
  if (file) {
    try { config = { ...config, ...JSON.parse(readFileSync(file, "utf8")) }; } catch (error) {
      throw new Failure(`cannot read ${file}: ${error.message}`);
    }
    config.file = file;
  }
  if (process.env.COWCTL_CONTROL_PLANE) config.controlPlane = process.env.COWCTL_CONTROL_PLANE;
  config.controlPlane = String(config.controlPlane).replace(/\/+$/, "");
  config.ingress = String(config.ingress ?? DEFAULTS.ingress).replace(/\/+$/, "");
  return config;
}
const expandHome = path => path === "~" ? homedir() : path.startsWith("~/") ? join(homedir(), path.slice(2)) : path;
function tokenOf(config) {
  if (process.env.COWCTL_TOKEN) return process.env.COWCTL_TOKEN.trim();
  const file = expandHome(String(config.tokenFile ?? DEFAULTS.tokenFile));
  const path = config.file && !file.startsWith("/") ? resolve(dirname(config.file), file) : file;
  try {
    const token = readFileSync(path, "utf8").trim();
    if (token) return token;
  } catch {}
  throw new Failure(`no credential: set COWCTL_TOKEN or put it in ${path}` +
    ` (the administrator's on the compose cluster: docker compose run --rm tool admin-credential > ${path}; a tenant's: cowctl credential create)`);
}
// The project's Wrangler configuration (as Wrangler looks for it) and the name it gives the Worker.
function wranglerConfig(directory = process.cwd()) {
  for (const name of WRANGLER_CONFIGS) {
    const file = join(directory, name);
    if (existsSync(file)) return file;
  }
  return null;
}
// A JSON or JSONC text (comments and trailing commas, as Wrangler reads wrangler.jsonc), parsed; null when it does not
// parse. Strings are kept as they are: "//" inside one (a URL) is not a comment.
function parseJsonc(text) {
  let out = "";
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === '"') {
      let end = i + 1;
      while (end < text.length && text[end] !== '"') end += text[end] === "\\" ? 2 : 1;
      out += text.slice(i, end + 1);
      i = end;
    } else if (c === "/" && text[i + 1] === "/") {
      while (i < text.length && text[i] !== "\n") i++;
      out += "\n";
    } else if (c === "/" && text[i + 1] === "*") {
      const end = text.indexOf("*/", i + 2);
      i = end < 0 ? text.length : end + 1;
      out += " ";
    } else if (c === ",") {
      // A trailing comma: the next character that is not white space or a comment closes an object or an array.
      let next = i + 1;
      for (;;) {
        while (next < text.length && /\s/.test(text[next])) next++;
        if (text.startsWith("//", next)) { while (next < text.length && text[next] !== "\n") next++; continue; }
        if (text.startsWith("/*", next)) { const end = text.indexOf("*/", next + 2); next = end < 0 ? text.length : end + 2; continue; }
        break;
      }
      if (text[next] !== "}" && text[next] !== "]") out += c;
    } else {
      out += c;
    }
  }
  try { return JSON.parse(out); } catch { return null; }
}
function wranglerName(file) {
  if (!file) return null;
  const text = readFileSync(file, "utf8");
  if (file.endsWith(".toml")) {
    // The top-level name: before the first [table].
    for (const line of text.split(/\r?\n/)) {
      if (/^\s*\[/.test(line)) break;
      const match = /^\s*name\s*=\s*["']([^"']+)["']/.exec(line);
      if (match) return match[1];
    }
    return null;
  }
  // The top-level name only (vars and other objects may hold a "name" of their own).
  const config = parseJsonc(text);
  return config && typeof config === "object" && typeof config.name === "string" ? config.name : null;
}
function workerOf(options, config) {
  const worker = options.worker ?? config.worker ?? wranglerName(wranglerConfig(config.file ? dirname(config.file) : process.cwd()));
  if (!worker) throw new Failure("no Worker: run this in a Wrangler project's directory (a wrangler.json, .jsonc or .toml with a name), or pass --worker NAME");
  if (!NAME.test(worker)) throw new Failure(`not a Worker name: ${worker} (lowercase letters, digits and '-', at most 63; set another with --worker)`);
  return worker;
}
function tenantOf(options, config) {
  const tenant = options.tenant ?? config.tenant;
  if (!tenant) throw new Failure("no tenant: pass --tenant T, or name it in cowctl.json (cowctl init --tenant T)");
  if (!NAME.test(tenant)) throw new Failure(`not a tenant name: ${tenant} (lowercase letters, digits and '-', at most 63)`);
  return tenant;
}
const tenantPath = (tenant) => `/v1/tenants/${tenant}`;
const workerPath = (tenant, worker) => `${tenantPath(tenant)}/workers/${worker}`;

// ---- the control plane ------------------------------------------------------------------------------------
async function call(config, method, path, { body, headers = {}, expect = [200] } = {}) {
  const token = tokenOf(config);
  let response;
  try {
    response = await fetch(config.controlPlane + path, { method, body, headers: { authorization: `Bearer ${token}`, ...headers } });
  } catch (error) {
    throw new Failure(`cannot reach the control plane at ${config.controlPlane} (${error.cause?.code ?? error.message}); ` +
      `is the cluster running? (docker compose up -d --wait)`);
  }
  const text = await response.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch {}
  if (expect.includes(response.status)) return { status: response.status, json, text };
  const reason = json?.reason ?? (text.slice(0, 400) || `HTTP ${response.status}`);
  const code = json?.code ?? `HTTP_${response.status}`;
  if (response.status === 401) throw new Failure(`the control plane refused the credential (${code}): check COWCTL_TOKEN or ${config.tokenFile}`, { detail: json });
  const failure = new Failure(`${code}: ${reason}`, { detail: json });
  failure.status = response.status;
  failure.code = code;
  throw failure;
}

// After a change the control plane answered: until every node that is not draining reports that it has applied the tenant
// index's revision naming the change (its state's storage.appliedTenantsRevision, which /v1/status shows), at most 15 s.
// A node that has not by then is named: the change is made, and it serves the change at its next read of the index.
const WAIT_MS = 15000;
async function waitForNodes(config, revision) {
  if (!(revision > 0)) return;
  const started = performance.now();
  let pending = [], nodes = [];
  for (;;) {
    try {
      nodes = ((await call(config, "GET", "/v1/status")).json?.nodes ?? []).filter(n => n.state !== "draining");
      pending = nodes.filter(n => !(Number(n.storage?.appliedTenantsRevision ?? 0) >= revision));
    } catch (error) {
      pending = [{ nodeId: `(the control plane: ${error.message})` }];
    }
    if (!pending.length) break;
    if (performance.now() - started >= WAIT_MS) {
      console.log(`  not served yet by ${pending.map(n => n.nodeId).join(", ")} after ${WAIT_MS / 1000} s (the change is made; ` +
        "a node serves it once it reads the tenant index again)");
      return;
    }
    await new Promise(done => setTimeout(done, 250));
  }
  if (nodes.length) console.log(`  served by ${nodes.length} node${nodes.length === 1 ? "" : "s"} (tenant index revision ${revision}) ` +
    `in ${((performance.now() - started) / 1000).toFixed(1)} s`);
}

// The Worker as the control plane has it (versions, active version, routes, secrets), or null when it has none.
async function workerState(config, tenant, worker) {
  try {
    return (await call(config, "GET", workerPath(tenant, worker))).json;
  } catch (error) {
    if (error.code === "WORKER_NOT_FOUND") return null;
    throw error;
  }
}
function versionNumber(id) { const match = /^v([0-9]{1,9})$/.exec(id); return match ? Number(match[1]) : 0; }

// ---- the package: a deterministic tar (ustar, pax "path" records for long names) ------------------------------
function tarHeader(name, size, type) {
  const header = Buffer.alloc(512);
  header.write(name, 0, 100, "utf8");
  header.write("0000644\0", 100, "ascii");
  header.write("0000000\0", 108, "ascii");
  header.write("0000000\0", 116, "ascii");
  header.write(size.toString(8).padStart(11, "0") + "\0", 124, "ascii");
  header.write("00000000000\0", 136, "ascii");
  header.fill(" ", 148, 156);
  header.write(type, 156, "ascii");
  header.write("ustar\0", 257, "ascii");
  header.write("00", 263, "ascii");
  let sum = 0;
  for (const byte of header) sum += byte;
  header.write(sum.toString(8).padStart(6, "0") + "\0 ", 148, "ascii");
  return header;
}
const pad = size => Buffer.alloc((512 - (size % 512)) % 512);
function tar(entries) {
  const parts = [];
  for (const { name, content } of [...entries].sort((a, b) => Buffer.compare(Buffer.from(a.name), Buffer.from(b.name)))) {
    if (Buffer.byteLength(name) > 100 || /[^\x20-\x7e]/.test(name)) {
      let record = "";
      for (let length = 0; ; ) {
        record = `${length} path=${name}\n`;
        const actual = Buffer.byteLength(record);
        if (actual === length) break;
        length = actual;
      }
      const body = Buffer.from(record);
      parts.push(tarHeader("PaxHeader", body.length, "x"), body, pad(body.length));
      parts.push(tarHeader(name.slice(-90).replace(/[^\x20-\x7e]/g, "_"), content.length, "0"), content, pad(content.length));
    } else {
      parts.push(tarHeader(name, content.length, "0"), content, pad(content.length));
    }
  }
  parts.push(Buffer.alloc(1024));
  return Buffer.concat(parts);
}
async function filesBelow(root, prefix = "") {
  const result = [];
  for (const entry of await readdir(join(root, prefix), { withFileTypes: true })) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) result.push(...await filesBelow(root, path));
    else if (entry.isFile()) result.push(path);
    else throw new Failure(`the converted project holds something that is not a file: ${path}`);
  }
  return result;
}
// The adapter's output as the control plane takes it: manifest.json and <worker>/<version>/..., with the version
// (and its manifest) renamed when the number the adapter built under is already taken, and the route the
// command line names (a <host>/<prefix> route: the adapter itself only takes a path prefix). An upload without activation
// names no route (routes come with the activation).
async function packageOf(output, worker, built, version, route, activate = true) {
  const manifest = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  for (const entry of manifest.workers) {
    if (entry.activeVersion === built) entry.activeVersion = version;
    for (const v of entry.versions) if (v.versionId === built) v.versionId = version;
  }
  if (route !== undefined && !route.startsWith("/")) {
    const slash = route.indexOf("/");
    manifest.routes = [{ host: route.slice(0, slash), pathPrefix: route.slice(slash), workerId: worker }];
  }
  if (!activate) manifest.routes = [];
  const entries = [{ name: "manifest.json", content: Buffer.from(JSON.stringify(manifest, null, 2) + "\n") }];
  const root = join(output, worker, built);
  for (const file of await filesBelow(root)) entries.push({ name: `${worker}/${version}/${file}`, content: await readFile(join(root, file)) });
  return { archive: tar(entries), manifest, files: entries.length };
}

// ---- commands ---------------------------------------------------------------------------------------------
async function init(args) {
  const { options, positional } = parse(args, { url: "value", ingress: "value", "token-file": "value", worker: "value", route: "value",
    tenant: "value", force: "flag" });
  if (positional.length) throw usage(`init takes no arguments: ${positional.join(" ")}`);
  const file = resolve(CONFIG_FILE);
  if (existsSync(file) && !options.force) throw new Failure(`${file} exists (--force replaces it)`);
  if (!options.tenant) throw usage("init takes the tenant: cowctl init --tenant T");
  if (!NAME.test(options.tenant)) throw new Failure(`not a tenant name: ${options.tenant} (lowercase letters, digits and '-', at most 63)`);
  const name = options.worker ?? wranglerName(wranglerConfig());
  if (name !== null && name !== undefined && !NAME.test(name))
    throw new Failure(`not a Worker name: ${name} (lowercase letters, digits and '-', at most 63; pass --worker NAME)`);
  if (options.route && !/^(\/|[^/\s]+\/)/.test(options.route)) throw usage("--route takes /<prefix> or <host>/<prefix>");
  const config = { tenant: options.tenant, controlPlane: (options.url ?? DEFAULTS.controlPlane).replace(/\/+$/, ""),
    ingress: (options.ingress ?? DEFAULTS.ingress).replace(/\/+$/, ""), tokenFile: options["token-file"] ?? DEFAULTS.tokenFile,
    ...(name ? { worker: name } : {}), ...(options.route ? { route: options.route } : {}) };
  await writeFile(file, JSON.stringify(config, null, 2) + "\n");
  console.log(`wrote ${file} (tenant ${config.tenant}${name ? `, Worker ${name}` : ""})`);
  // A check, not a condition: the cluster may start later.
  try {
    const loaded = { ...config, file };
    const status = (await call(loaded, "GET", "/v1/status")).json;
    const nodes = status.nodes ?? [];
    console.log(`control plane ${config.controlPlane}: ${nodes.length} node${nodes.length === 1 ? "" : "s"} (${nodes.map(m => m.nodeId).join(", ") || "none yet"})` +
      (status.caller?.admin ? ", the administrator's credential" : `, a credential of tenant ${status.caller?.tenant}`));
  } catch (error) {
    console.log(`not checked: ${error.message}`);
  }
}

async function deploy(args) {
  const { options, positional } = parse(args, { "no-activate": "flag", version: "value", route: "value", secret: "list", worker: "value",
    config: "value", tenant: "value", "no-wait": "flag" });
  if (positional.length) throw usage(`deploy takes no arguments: ${positional.join(" ")}`);
  if (options["no-activate"] && options.route !== undefined)
    throw usage("--route takes effect when a version is activated: an upload without activation (--no-activate) takes no route");
  const config = loadConfig();
  const tenant = tenantOf(options, config);
  const projectDirectory = config.file ? dirname(config.file) : process.cwd();
  const wrangler = options.config ? resolve(options.config) : wranglerConfig(projectDirectory) ?? wranglerConfig();
  if (!wrangler || !existsSync(wrangler)) throw new Failure("no Wrangler configuration here (wrangler.json, wrangler.jsonc or wrangler.toml)");
  const worker = options.worker ?? config.worker ?? wranglerName(wrangler);
  if (!worker || !NAME.test(worker))
    throw new Failure(`no usable Worker name (${worker ?? "none"}): lowercase letters, digits and '-'; name it in the Wrangler configuration or pass --worker`);
  if (options.version !== undefined && !VERSION_NAME.test(options.version)) throw usage(`not a version name: ${options.version}`);
  const route = options.route ?? config.route;
  if (route !== undefined && !/^(\/|[^/\s]+\/)/.test(route)) throw usage("--route takes /<prefix> or <host>/<prefix>");
  for (const name of options.secret ?? []) if (!SECRET_NAME.test(name)) throw usage("--secret takes a name (a JavaScript identifier), never a value");
  tokenOf(config);
  // The next free number among the Worker's versions (v1, v2, ...), unless named.
  const known = await workerState(config, tenant, worker);
  let number = Math.max(0, ...(known?.versions ?? []).map(v => versionNumber(v.versionId))) + 1;
  let version = options.version ?? `v${number}`;
  const started = performance.now();
  const temporary = await mkdtemp(join(tmpdir(), "coworkerd-deploy-"));
  try {
    const { adapt } = await import(ADAPTER.href).catch(error => {
      throw new Failure(`cannot load the Wrangler adapter (${fileURLToPath(ADAPTER)}): ${error.message}`);
    });
    const output = join(temporary, "out");
    let converted;
    try {
      // The default route is under the cluster's default host, /<tenant>/<worker>.
      converted = await adapt({ config: wrangler, output, workerId: worker, versionId: version, secrets: options.secret ?? [],
        pathPrefix: route?.startsWith("/") ? route : `/${tenant}/${worker}`,
        onIgnored: item => console.error(`ignored Wrangler configuration: ${item.key} (${item.reason}: ${item.detail})`) });
    } catch (error) {
      throw new Failure(`the project does not convert: ${error.message.split("\n").slice(0, 12).join("\n")}`);
    }
    const built = version;
    const builtMs = performance.now() - started;
    for (let attempt = 0; ; attempt++) {
      const { archive: body, manifest, files } = await packageOf(output, worker, built, version, route, !options["no-activate"]);
      try {
        const answer = await call(config, "PUT", `${workerPath(tenant, worker)}/versions/${version}${options["no-activate"] ? "" : "?activate=true"}`,
          { body, headers: { "content-type": "application/x-tar" } });
        report(config, `${tenant}/${worker}`, version, manifest, answer.json, { files, bytes: body.length, builtMs, totalMs: performance.now() - started,
          activated: !options["no-activate"], secrets: converted.record.secrets?.names ?? [] });
        if (!options["no-activate"] && !options["no-wait"]) await waitForNodes(config, Number(answer.json?.record?.revision ?? 0));
        return;
      } catch (error) {
        // Someone else took this number in the meantime: the next one (a version named with --version is never renumbered).
        if (error.code === "VERSION_EXISTS" && !options.version && attempt < 5) { version = `v${++number}`; continue; }
        if (error.code === "QUOTA_EXCEEDED") throw new Failure(`deploy refused: tenant ${tenant}'s quota: ${error.message.replace(/^QUOTA_EXCEEDED: /, "")}`);
        if (error.code === "HOSTNAME_NOT_OWNED") throw new Failure(`deploy refused: ${error.message.replace(/^HOSTNAME_NOT_OWNED: /, "")}`);
        if (error.code === "DEPLOYMENT_REFUSED" && /secret/i.test(error.message) && /without a value|missing secret/i.test(error.message)) {
          const names = converted.record.secrets?.names ?? [];
          throw new Failure(`deploy refused: ${error.message.replace(/^DEPLOYMENT_REFUSED: /, "")}\n` +
            `set ${names.length > 1 ? "them" : "it"} first: ${names.map(n => `cowctl secret put ${n}`).join("; ") || "cowctl secret put <NAME>"}`);
        }
        if (error.code === "LOAD_FAILED") {
          const detail = error.detail ?? {};
          throw new Failure(`deploy refused: version ${version} does not load (${detail.category ?? "LOAD_FAILED"}): ${detail.message ?? error.message}` +
            `\nnothing changed: the active version is still ${known?.activeVersion ?? "none"}`);
        }
        throw new Failure(`deploy failed: ${error.message}`, { detail: error.detail });
      }
    }
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}
function urlOf(config, route) {
  if (route.host === "*") return config.ingress + route.pathPrefix;
  const ingress = new URL(config.ingress);
  return `${ingress.protocol}//${route.host}${ingress.port ? ":" + ingress.port : ""}${route.pathPrefix}`;
}
function report(config, worker, version, manifest, answer, facts) {
  const record = answer.record ?? {};
  const trial = (record.trial?.versions ?? [])[0];
  console.log(`${facts.activated ? "deployed" : "uploaded"} ${worker} ${version}${facts.activated ? " (active)" : " (not active: cowctl rollback " + version + " makes it active)"}`);
  console.log(`  package: ${facts.files} files, ${facts.bytes} bytes; built in ${(facts.builtMs / 1000).toFixed(1)} s, deployed in ${(facts.totalMs / 1000).toFixed(1)} s`);
  if (trial) console.log(`  trial load: ${trial.server} (its quarantine process), ${Math.round(trial.elapsedMs)} ms`);
  else if (record.trial?.atActivation) console.log("  trial load: when it is activated");
  else if (record.trial?.enabled === false) console.log("  trial load: off");
  if (facts.activated) for (const route of manifest.routes ?? []) console.log(`  route ${route.host}${route.pathPrefix}  ->  ${urlOf(config, route)}`);
  if (facts.secrets.length) console.log(`  secrets declared: ${facts.secrets.join(", ")}`);
}

async function versions(args) {
  const { options, positional } = parse(args, { worker: "value", json: "flag", tenant: "value" });
  if (positional.length) throw usage("versions takes no arguments");
  const config = loadConfig(), tenant = tenantOf(options, config), worker = workerOf(options, config);
  const found = await workerState(config, tenant, worker);
  if (!found) throw new Failure(`Worker ${worker} has never been deployed in tenant ${tenant}`);
  if (options.json) { console.log(JSON.stringify(found, null, 2)); return; }
  const limits = Object.entries(found.limits ?? {}).map(([name, value]) => `${name} ${value}`).join(", ");
  console.log(`${tenant}/${worker}: active ${found.activeVersion ?? "none"}${found.valid ? "" : ` (not serving: ${found.reason})`}${limits ? `; limits ${limits}` : ""}`);
  for (const v of found.versions) {
    console.log(`${v.versionId === found.activeVersion ? "*" : " "} ${v.versionId.padEnd(8)} uploaded at revision ${v.uploadedRevision.padEnd(8)} ` +
      `package ${(v.contentHash ?? "").slice(0, 12)}${v.secrets?.length ? `  secrets ${v.secrets.join(",")}` : ""}${v.problem ? `  problem: ${v.problem}` : ""}`);
  }
  for (const r of found.routes) console.log(`  route ${r.host}${r.pathPrefix}${r.effective ? "" : " (not effective)"}  ->  ${urlOf(config, r)}`);
}

async function rollback(args) {
  const { options, positional } = parse(args, { worker: "value", tenant: "value", "no-wait": "flag" });
  if (positional.length !== 1) throw usage("rollback takes the version: cowctl rollback <version>");
  const [version] = positional;
  if (!VERSION_NAME.test(version)) throw usage(`not a version name: ${version}`);
  const config = loadConfig(), tenant = tenantOf(options, config), worker = workerOf(options, config);
  const before = await workerState(config, tenant, worker);
  try {
    const answer = (await call(config, "POST", `${workerPath(tenant, worker)}/versions/${version}:activate`)).json;
    if (answer.unchanged) { console.log(`${worker}: ${version} is already the active version`); return; }
    console.log(`${worker}: ${version} is active (was ${before?.activeVersion ?? "none"})`);
    // An activation keeps the Worker's routes: one that never had any answers only through Service Bindings.
    if (!(before?.routes ?? []).length) console.log("  it has no route: cowctl deploy --route ... (or PUT .../routes) gives it one");
    if (!options["no-wait"]) await waitForNodes(config, Number(answer.revision ?? 0));
  } catch (error) {
    if (error.code === "LOAD_FAILED") throw new Failure(`rollback refused: ${version} does not load: ${error.detail?.message ?? error.message}`);
    throw new Failure(`rollback failed: ${error.message}`);
  }
}

async function remove(args) {
  const { options, positional } = parse(args, { worker: "value", yes: "flag", tenant: "value" });
  if (positional.length) throw usage("remove takes no arguments");
  const config = loadConfig(), tenant = tenantOf(options, config), worker = workerOf(options, config);
  if (!options.yes) {
    if (!process.stdin.isTTY) throw new Failure(`remove stops serving ${worker}: pass --yes to confirm`);
    const answer = await prompt(`stop serving ${worker}? type its name to confirm: `);
    if (answer.trim() !== worker) throw new Failure("not removed");
  }
  try {
    await call(config, "DELETE", workerPath(tenant, worker));
  } catch (error) {
    throw new Failure(`remove failed: ${error.message}`);
  }
  console.log(`${worker}: removed (its versions stay; cowctl deploy serves it again)`);
}

async function secret(args) {
  const [operation, ...rest] = args;
  if (!["put", "list", "delete"].includes(operation)) throw usage("secret put <NAME> | secret list | secret delete <NAME>");
  const { options, positional } = parse(rest, { worker: "value", "from-file": "value", tenant: "value", "no-wait": "flag" });
  const config = loadConfig(), tenant = tenantOf(options, config), worker = workerOf(options, config);
  if (operation === "list") {
    if (positional.length) throw usage("secret list takes no arguments");
    const answer = (await call(config, "GET", `${workerPath(tenant, worker)}/secrets`)).json;
    if (!answer.secrets.length) console.log(`${worker}: no secrets`);
    for (const s of answer.secrets)
      console.log(`${s.name}  ${s.bytes ?? "?"} bytes, set ${s.setAtUnixMs ? new Date(s.setAtUnixMs).toISOString() : "(unusable: " + s.problem + ")"}`);
    return;
  }
  // Never echo an argument: a value typed in place of the name must not reach the screen or a log.
  if (positional.length !== 1 || !SECRET_NAME.test(positional[0]))
    throw usage(`secret ${operation} takes one name (a JavaScript identifier); a secret's value comes from standard input or --from-file, never from the command line`);
  const [name] = positional;
  if (operation === "delete") {
    try {
      await call(config, "DELETE", `${workerPath(tenant, worker)}/secrets/${name}`);
    } catch (error) {
      throw new Failure(`secret delete failed: ${error.message}`);
    }
    console.log(`${worker}: secret ${name} deleted`);
    return;
  }
  let value;
  if (options["from-file"]) value = readFileSync(resolve(options["from-file"]), "utf8");
  else if (process.stdin.isTTY) value = await prompt(`value of ${name}: `, { hidden: true });
  else value = await readStdin();
  // As `coworkerd-apiserver tool secret put` and `wrangler secret put` read it: trailing whitespace removed.
  value = value.replace(/[ \t\r\n\v\f]+$/, "");
  if (!value) throw new Failure("the value is empty");
  let answer;
  try {
    answer = (await call(config, "PUT", `${workerPath(tenant, worker)}/secrets/${name}`, { body: value, headers: { "content-type": "application/octet-stream" } })).json;
  } catch (error) {
    if (error.code === "QUOTA_EXCEEDED") throw new Failure(`secret put refused: tenant ${tenant}'s quota: ${error.message.replace(/^QUOTA_EXCEEDED: /, "")}`);
    throw new Failure(`secret put failed: ${error.message}`);
  }
  console.log(`${worker}: secret ${name} ${answer.created ? "set" : "replaced"} (${answer.bytes} bytes)`);
  if (!answer.declaredByActiveVersion)
    console.log(`  no active version declares ${name} yet: list it in .dev.vars.example (or deploy --secret ${name}), then cowctl deploy`);
  else console.log("  isolates loaded from now on read the new value (loaded ones keep theirs until they are recycled)");
  if (!options["no-wait"]) await waitForNodes(config, Number(answer.revision ?? 0));
}

async function status(args) {
  const { options, positional } = parse(args, { json: "flag" });
  if (positional.length) throw usage("status takes no arguments");
  const config = loadConfig();
  const answer = (await call(config, "GET", "/v1/status")).json;
  if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
  const control = answer.apiserver?.version ?? {};
  console.log(`control plane ${config.controlPlane}: coworkerd-apiserver ${control.server ?? "?"} (source ${control.source ?? "?"}), tenants revision ${answer.storage?.tenantsRevision ?? "?"}, credentials revision ${answer.storage?.credentialsRevision ?? "?"}` +
    (answer.caller?.admin ? "" : `; tenant ${answer.caller?.tenant}`));
  if (!answer.nodes.length) console.log("no node has registered");
  for (const m of answer.nodes) {
    if (!m.reachable) { console.log(`${m.nodeId}  ${m.state}  unreachable${m.address ? ` at ${m.address}` : ""}${m.problem ? `: ${m.problem}` : ""}`); continue; }
    if (!m.processes) { console.log(`${m.nodeId}  ${m.state}  coworkerd ${m.version?.server ?? "?"}`); continue; }
    const processes = (m.processes ?? []).map(p => `${p.processId} ${p.state ?? "?"} (pid ${p.pid}, generation ${p.generation})`);
    const deployment = m.deployment ?? {};
    console.log(`${m.nodeId}  ${m.state}${m.drain?.draining ? " (draining)" : ""}  coworkerd ${m.version?.server ?? "?"}  ` +
      `${deployment.validWorkerCount ?? "?"}/${deployment.workerCount ?? "?"} Workers valid at revision ${deployment.revision ?? "?"}`);
    console.log(`  processes: ${processes.join(", ") || "none"}`);
  }
}

function sinceOf(text) {
  if (text === undefined) return 0;
  const relative = /^([0-9]+)(s|m|h|d)$/.exec(text);
  if (relative) return Date.now() - Number(relative[1]) * { s: 1e3, m: 6e4, h: 3.6e6, d: 8.64e7 }[relative[2]];
  if (/^[0-9]{10,16}$/.test(text)) return Number(text);
  const parsed = Date.parse(text);
  if (Number.isNaN(parsed)) throw usage(`--since takes 10m, 2h, 1d, an ISO time or Unix milliseconds: ${text}`);
  return parsed;
}
function printLine(line, json) {
  if (json) { console.log(JSON.stringify(line)); return; }
  const at = new Date(line.atUnixMs ?? 0).toISOString().replace("T", " ").replace("Z", "");
  if (line.dropped !== undefined) {
    console.log(`${at} ${line.nodeId} [dropped] ${line.dropped} line${line.dropped === 1 ? "" : "s"} of tenant ${line.tenant} over ${line.linesPerSecond} lines/s`);
    return;
  }
  console.log(`${at} ${line.nodeId} ${(line.level ?? "log").padEnd(5)} ${line.versionId} ${line.message}${line.truncated ? ` [truncated from ${line.messageBytes} bytes]` : ""}`);
}
async function logs(args) {
  const { options, positional } = parse(args, { follow: "flag", since: "value", limit: "value", worker: "value", json: "flag", tenant: "value" });
  if (positional.length) throw usage("logs takes no arguments");
  const config = loadConfig(), tenant = tenantOf(options, config), worker = workerOf(options, config);
  const since = sinceOf(options.since);
  const limit = options.limit === undefined ? 200 : Number(options.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > 5000) throw usage("--limit takes 1..5000");
  const query = `limit=${limit}${since ? `&since=${Math.floor(since)}` : ""}`;
  const path = `${workerPath(tenant, worker)}/logs`;
  if (!options.follow) {
    const answer = (await call(config, "GET", `${path}?${query}`)).json;
    for (const s of answer.nodes) if (!s.reachable) console.error(`(${s.nodeId} unreachable: ${s.problem})`);
    for (const line of answer.lines) printLine(line, options.json);
    if (!answer.lines.length && !options.json) console.error(`(no lines of ${worker}${since ? " since " + new Date(since).toISOString() : ""})`);
    return;
  }
  const token = tokenOf(config);
  let response;
  try {
    response = await fetch(`${config.controlPlane}${path}?${query}&follow=1`, { headers: { authorization: `Bearer ${token}` } });
  } catch (error) {
    throw new Failure(`cannot reach the control plane at ${config.controlPlane} (${error.cause?.code ?? error.message})`);
  }
  if (response.status !== 200) {
    const text = await response.text();
    let json = null; try { json = JSON.parse(text); } catch {}
    throw new Failure(`logs failed: ${json?.code ?? response.status}: ${json?.reason ?? text.slice(0, 400)}`);
  }
  const decoder = new TextDecoder();
  let pending = "", event = "message";
  for await (const chunk of response.body) {
    pending += decoder.decode(chunk, { stream: true });
    for (let end; (end = pending.indexOf("\n")) >= 0; ) {
      const line = pending.slice(0, end);
      pending = pending.slice(end + 1);
      if (line.startsWith("event: ")) { event = line.slice(7); continue; }
      if (!line.startsWith("data: ")) { if (line === "") event = "message"; continue; }
      const data = JSON.parse(line.slice(6));
      if (event === "nodes") { for (const s of data) if (!s.reachable) console.error(`(${s.nodeId} unreachable: ${s.problem})`); continue; }
      printLine(data, options.json);
    }
  }
  throw new Failure("the control plane closed the stream");
}

// ---- the administrator's commands -------------------------------------------------------------------------
function quotaFields(list = []) {
  const quotas = {};
  for (const item of list) {
    const match = /^([A-Za-z]+)=([0-9]+)$/.exec(item);
    if (!match) throw usage(`--quota takes NAME=N (for example workers=10): ${item}`);
    quotas[match[1]] = Number(match[2]);
  }
  return quotas;
}
function tenantBody(options, creating) {
  const body = {};
  if (options["no-hostnames"]) body.hostnames = [];
  if (options.hostname) body.hostnames = options.hostname;
  const quotas = quotaFields(options.quota);
  if (Object.keys(quotas).length) body.quotas = quotas;
  if (options["default-cpu-ms"] !== undefined || options["default-memory-mib"] !== undefined) {
    body.defaultLimits = {};
    for (const [flag, name] of [["default-cpu-ms", "cpuMs"], ["default-memory-mib", "memoryMiB"]]) {
      if (options[flag] === undefined) continue;
      if (!/^[0-9]+$/.test(options[flag])) throw usage(`--${flag} takes a number`);
      body.defaultLimits[name] = Number(options[flag]);
    }
  }
  if (!creating && !Object.keys(body).length) throw usage("tenant update takes --hostname, --no-hostnames, --quota or --default-*");
  return body;
}
function printTenant(tenant, extra = {}) {
  const quotas = Object.entries(tenant.quotas ?? {}).map(([k, v]) => `${k}=${v}`).join(" ");
  console.log(`${tenant.name}  ${tenant.status}  created ${new Date(tenant.createdAtUnixMs).toISOString()}`);
  console.log(`  hostnames: ${(tenant.hostnames ?? []).join(", ") || "none (path routes under * /" + tenant.name + "/ only)"}`);
  console.log(`  quotas: ${quotas}`);
  const limits = Object.entries(tenant.defaultLimits ?? {}).map(([k, v]) => `${k} ${v}`).join(", ");
  console.log(`  default limits: ${limits || "the nodes' own"}`);
  if (extra.counts) console.log(`  holds: ${extra.counts.workers} Workers, ${extra.counts.versions} versions, ${extra.counts.secrets} secrets`);
  if (extra.usageLast24h) console.log(`  last 24 h: ${usageText(extra.usageLast24h)}`);
  if (tenant.deletion) console.log(`  deletion: ${JSON.stringify(tenant.deletion)}`);
}
async function tenant(args) {
  const [operation, ...rest] = args;
  const spec = { hostname: "list", quota: "list", "default-cpu-ms": "value", "default-memory-mib": "value", "no-hostnames": "flag", json: "flag", yes: "flag" };
  if (!["create", "list", "get", "update", "delete"].includes(operation)) throw usage("tenant create|list|get|update|delete");
  const { options, positional } = parse(rest, spec);
  const config = loadConfig();
  if (operation === "list") {
    if (positional.length) throw usage("tenant list takes no arguments");
    const answer = (await call(config, "GET", "/v1/tenants")).json;
    if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
    if (!answer.tenants.length) console.log("no tenants");
    for (const t of answer.tenants) console.log(`${t.name.padEnd(20)} ${t.status.padEnd(9)} ${(t.hostnames ?? []).join(",")}`);
    return;
  }
  if (positional.length !== 1 || !NAME.test(positional[0])) throw usage(`tenant ${operation} takes the tenant's name ([a-z0-9][a-z0-9-]{0,62})`);
  const [name] = positional;
  if (operation === "create") {
    const answer = (await call(config, "POST", "/v1/tenants", { body: JSON.stringify({ name, ...tenantBody(options, true) }),
      headers: { "content-type": "application/json" } })).json;
    if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
    printTenant(answer.tenant);
    console.log(`  next: cowctl credential create --tenant ${name} (its credential, printed once)`);
    return;
  }
  if (operation === "get") {
    const answer = (await call(config, "GET", tenantPath(name))).json;
    if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
    printTenant(answer.tenant, answer);
    return;
  }
  if (operation === "update") {
    const answer = (await call(config, "PATCH", tenantPath(name), { body: JSON.stringify(tenantBody(options, false)),
      headers: { "content-type": "application/json" } })).json;
    if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
    printTenant(answer.tenant);
    return;
  }
  if (!options.yes) {
    if (!process.stdin.isTTY) throw new Failure(`tenant delete removes tenant ${name} with every Worker, version, secret and credential: pass --yes to confirm`);
    const typed = await prompt(`delete tenant ${name} and everything of it? type its name to confirm: `);
    if (typed.trim() !== name) throw new Failure("not deleted");
  }
  const answer = (await call(config, "DELETE", tenantPath(name))).json;
  if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
  console.log(`tenant ${name} deleted`);
  for (const step of answer.steps ?? []) console.log(`  ${step.step}: ${JSON.stringify(step.result)}`);
}
function durationMs(text) {
  const match = /^([0-9]+)(s|m|h|d)$/.exec(text ?? "");
  if (!match) throw usage(`--expires-in takes 30d, 12h, 90m or 600s: ${text}`);
  return Number(match[1]) * { s: 1e3, m: 6e4, h: 3.6e6, d: 8.64e7 }[match[2]];
}
async function credential(args) {
  const [operation, ...rest] = args;
  if (!["create", "list", "revoke"].includes(operation)) throw usage("credential create|list|revoke <keyId>");
  const { options, positional } = parse(rest, { tenant: "value", description: "value", "expires-in": "value", json: "flag" });
  const config = loadConfig(), tenant = tenantOf(options, config);
  if (operation === "create") {
    if (positional.length) throw usage("credential create takes no arguments");
    const body = { ...(options.description ? { description: options.description } : {}),
      ...(options["expires-in"] ? { expiresAtUnixMs: Date.now() + durationMs(options["expires-in"]) } : {}) };
    const answer = (await call(config, "POST", `${tenantPath(tenant)}/credentials`, { body: JSON.stringify(body),
      headers: { "content-type": "application/json" } })).json;
    if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
    // The only time the credential is shown: the control plane keeps its hash.
    console.log(answer.credential);
    console.error(`credential ${answer.keyId} of tenant ${tenant}${answer.expiresAtUnixMs ? `, expires ${new Date(answer.expiresAtUnixMs).toISOString()}` : ""}: ` +
      "shown once; store it (for example in ~/.cowctl/token, mode 600)");
    return;
  }
  if (operation === "list") {
    if (positional.length) throw usage("credential list takes no arguments");
    const answer = (await call(config, "GET", `${tenantPath(tenant)}/credentials`)).json;
    if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
    if (!answer.credentials.length) console.log(`tenant ${tenant}: no credentials`);
    for (const c of answer.credentials) {
      const state = c.revokedAtUnixMs ? `revoked ${new Date(c.revokedAtUnixMs).toISOString()}`
        : c.expiresAtUnixMs ? `expires ${new Date(c.expiresAtUnixMs).toISOString()}` : "no expiry";
      console.log(`${c.keyId}  ${c.scope}  created ${new Date(c.createdAtUnixMs).toISOString()}  ${state}${c.description ? `  ${c.description}` : ""}`);
    }
    return;
  }
  if (positional.length !== 1 || !/^[0-9a-f]{16}$/.test(positional[0])) throw usage("credential revoke takes the key id (16 hex characters)");
  const answer = (await call(config, "DELETE", `${tenantPath(tenant)}/credentials/${positional[0]}`)).json;
  if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
  console.log(`credential ${answer.keyId} of tenant ${tenant}: ${answer.unchanged ? "already revoked" : "revoked"}`);
}
async function node(args) {
  const [operation, ...rest] = args;
  if (operation !== "list") throw usage("node list");
  const { options, positional } = parse(rest, { json: "flag" });
  if (positional.length) throw usage("node list takes no arguments");
  const config = loadConfig();
  const answer = (await call(config, "GET", "/v1/nodes")).json;
  if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
  if (!answer.nodes.length) console.log("no node has registered");
  for (const m of answer.nodes) {
    if (!m.reachable) { console.log(`${m.nodeId}  ${m.state}  unreachable at ${m.address}: ${m.problem}`); continue; }
    const c = m.configuration ?? {};
    console.log(`${m.nodeId}  ${m.state}  coworkerd ${m.version?.server ?? "?"}  ${c.count ?? "?"} processes + ${c.quarantineProcesses ?? "?"} quarantine, ` +
      `limits ${c.cpuLimitMs ?? "?"} ms / ${c.memoryLimitMiB ?? "?"} MiB, sandbox ${m.sandbox?.mode ?? "?"}${m.sandbox?.active ? " (on)" : ""}`);
    for (const p of m.processes ?? []) console.log(`  ${p.processId} ${p.processClass} ${p.state} pid ${p.pid} generation ${p.generation} rotations ${p.rotationCount ?? 0}`);
    for (const s of m.suspects ?? []) console.log(`  suspect: tenant ${s.tenant} (${s.workerId}) until ${new Date(s.expiresAtUnixMs).toISOString()}`);
  }
}
function usageText(row) {
  return `${row.requests} requests, ${Number(row.cpuMs).toFixed(1)} CPU ms, ${row.egressBytes} egress bytes, ${row.logLines} log lines, ` +
    `${(Number(row.isolateMs) / 1000).toFixed(1)} isolate-seconds`;
}
async function usage_(args) {
  const { options, positional } = parse(args, { tenant: "value", all: "flag", since: "value", until: "value", json: "flag" });
  if (positional.length) throw usage("usage takes no arguments");
  const config = loadConfig();
  const to = options.until ? sinceOf(options.until) : Date.now(), from = sinceOf(options.since ?? "24h");
  const query = `from=${Math.floor(from)}&to=${Math.ceil(to)}&by=hour`;
  const all = options.all, tenant = all ? null : tenantOf(options, config);
  const answer = (await call(config, "GET", all ? `/v1/usage?${query}` : `${tenantPath(tenant)}/usage?${query}`)).json;
  if (options.json) { console.log(JSON.stringify(answer, null, 2)); return; }
  console.log(`${all ? "every tenant" : `tenant ${tenant}`}, ${new Date(answer.from).toISOString()} to ${new Date(answer.to).toISOString()} ` +
    `(${answer.nodes.length} node${answer.nodes.length === 1 ? "" : "s"})`);
  for (const row of answer.rows) console.log(`  ${row.hour}${row.tenant ? ` ${row.tenant}` : ""}  ${usageText(row)}`);
  console.log(`  total  ${usageText(answer.total)}`);
}

// ---- terminal input ---------------------------------------------------------------------------------------
async function readStdin() {
  const chunks = [];
  let size = 0;
  for await (const chunk of process.stdin) {
    size += chunk.length;
    if (size > 64 * 1024) throw new Failure("the value is larger than 64 KiB (a secret is at most 5 KiB)");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}
function prompt(question, { hidden = false } = {}) {
  return new Promise((done, failed) => {
    process.stderr.write(question);
    const stdin = process.stdin;
    let value = "";
    if (hidden) stdin.setRawMode(true);
    stdin.setEncoding("utf8");
    stdin.resume();
    const onData = text => {
      for (const ch of text) {
        if (ch === "\u0003") { cleanup(); process.stderr.write("\n"); failed(new Failure("interrupted", { code: 130 })); return; }
        if (ch === "\r" || ch === "\n") { cleanup(); process.stderr.write("\n"); done(value); return; }
        if (ch === "\u007f" || ch === "\b") { value = value.slice(0, -1); continue; }
        value += ch;
        if (!hidden) process.stderr.write(ch);
      }
    };
    const cleanup = () => { stdin.off("data", onData); if (hidden) stdin.setRawMode(false); stdin.pause(); };
    stdin.on("data", onData);
  });
}

// ---- main -------------------------------------------------------------------------------------------------
const COMMANDS = { init, deploy, versions, rollback, remove, secret, status, logs, tenant, credential, node, usage: usage_ };
async function main(argv) {
  const [command, ...args] = argv;
  if (!command || command === "--help" || command === "-h" || command === "help") { console.log(USAGE); return 0; }
  if (command === "--version" || command === "version") { console.log(`cowctl ${VERSION}`); return 0; }
  const run = COMMANDS[command];
  if (!run) throw usage(`unknown command: ${command}`);
  await run(args);
  return 0;
}
main(process.argv.slice(2)).then(code => process.exit(code), error => {
  if (error instanceof Failure) {
    console.error(`cowctl ${process.argv[2] ?? ""}: ${error.message}`.replace(/^cowctl\s+: /, "cowctl: "));
    process.exit(error.exitCode);
  }
  console.error(`cowctl ${process.argv[2] ?? ""}: unexpected error: ${error?.stack ?? error}`);
  process.exit(1);
});

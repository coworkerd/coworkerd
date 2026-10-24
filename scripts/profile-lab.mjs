// The single-node environment the build's system call profile runs in (scripts/syscall-profile.mjs). Run with Node
// inside a profiling container (the toolchain image, this checkout at its own path, the Docker options the launcher
// needs). One node daemon with the file backends and its workerd processes, production defaults; the node runs as an
// unprivileged user (uid 10001, as in the image and the systemd unit), the harness as root reads every process's /proc.
// The binary directory needs a `coworkerd-server` that executes the node daemon `coworkerd` with its arguments: this
// environment starts the node by that name (a two-line shell script will do).
//   V45_BIN    the node, the launcher, workerd and the two system call tables (Linux, this machine's architecture)
//   V45_STACK  the stack directory (default /tmp/coworkerd-lab-stack, inside the container)
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, readlinkSync, statSync } from "node:fs";
import { chmod, copyFile, cp, mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { StringDecoder } from "node:string_decoder";
import { fileURLToPath } from "node:url";

export const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const BIN = process.env.V45_BIN;
export const STACK = process.env.V45_STACK ?? "/tmp/coworkerd-lab-stack";
export const USER = "10001";
export const base = "http://127.0.0.1:8930";
export const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
export const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");

// The ordinary Worker: the latency target.
export const ordinary = `export default { async fetch(request) {
  return Response.json({ node: request.cf.nodeId, server: request.cf.serverId, process: request.cf.processId,
    generation: request.cf.processGeneration, invocationId: request.cf.invocationId });
} };\n`;

// Production defaults (common/schema/coworkerd.capnp) on one machine: four ordinary processes and two quarantine
// processes, CPU 200 ms, memory 128 MiB, idle eviction 300 s, rotation on a workerd change, by hand or at 70% of the
// memory, the collections' defaults (gc), the memory watermark on; homeProbability 0, so every call runs where it
// arrives. `nodeId` names the node. The management endpoints the profile reads are open (a development node: 0.4
// requires a management token otherwise).
export const DEFAULTS = { nodeId: "s1", processPool: { count: 4 }, placement: { homeProbability: 0 },
  management: { allowWithoutToken: true } };

let server = null;

export function binaries(directory = BIN) {
  const hash = path => existsSync(path) ? sha256(readFileSync(path)) : null;
  return { coworkerdServer: hash(`${directory}/coworkerd-server`), coworkerdLauncher: hash(`${directory}/coworkerd-launcher`),
    workerd: hash(`${directory}/workerd`), runtime: hash(`${REPO}/runtime/runtime.js`) };
}

export async function request(path, options = {}) {
  const startedAtUnixMs = Date.now(), started = performance.now();
  try {
    const response = await fetch(base + path, { ...options, signal: options.signal ?? AbortSignal.timeout(options.timeoutMs ?? 10000) });
    const body = await response.text();
    return { path, startedAtUnixMs, elapsedMs: performance.now() - started, status: response.status, body };
  } catch (error) {
    return { path, startedAtUnixMs, elapsedMs: performance.now() - started, status: 0, error: String(error?.cause ?? error) };
  }
}
export const json = response => { try { return JSON.parse(response.body); } catch { return null; } };
export const state = async () => JSON.parse((await request("/__coworkerd/state")).body);

export async function waitFor(check, label, timeoutMs = 30000, intervalMs = 50) {
  const started = performance.now();
  while (performance.now() - started < timeoutMs) {
    const value = await check();
    if (value) return { value, elapsedMs: performance.now() - started };
    await delay(intervalMs);
  }
  throw new Error(`timed out: ${label}`);
}

// Writes the stack (manifest, Worker sources, runtime, Server config; owned by the Server's user) and starts the
// Server as that user. `sandbox` is the configuration's sandbox.mode; `ownBinaries` copies the three binaries into
// the stack (a case that replaces files on disk); `keepStack` starts again on the stack as it is (3.4).
export async function setup({ sandbox, syscalls = null, workers = { ordinary }, config = {}, env = {}, waitReady = true,
    keepStack = false, ownBinaries = false, workerdPath = null, trace = false }) {
  await stop();
  let bin = BIN;
  if (!keepStack) {
    await rm(STACK, { recursive: true, force: true });
    await mkdir(`${STACK}/workers`, { recursive: true });
    await mkdir(`${STACK}/logs`, { recursive: true });
    await cp(`${REPO}/runtime`, `${STACK}/runtime`, { recursive: true });
    const manifest = { compatibilityDate: "2026-08-27", routes: [], workers: [] };
    // A Worker is its source, or { source, bindings } when it calls another one (the service binding of
    // patch 0001, which the runtime makes over run/binding.sock -- the profile's `binding` window).
    for (const [id, entry] of Object.entries(workers)) {
      const source = typeof entry === "string" ? entry : entry.source;
      const bindings = typeof entry === "string" ? [] : (entry.bindings ?? []);
      await mkdir(`${STACK}/workers/${id}/v1`, { recursive: true });
      await writeFile(`${STACK}/workers/${id}/v1/index.js`, source);
      manifest.routes.push({ host: "*", pathPrefix: `/${id}`, workerId: id });
      manifest.workers.push({ workerId: id, activeVersion: "v1", versions: [{ versionId: "v1", mainModule: "index.js", contentHash: sha256(source), bindings }] });
    }
    await writeFile(`${STACK}/workers/manifest.json`, JSON.stringify(manifest));
    if (ownBinaries) {
      await mkdir(`${STACK}/bin`, { recursive: true });
      for (const name of ["coworkerd-server", "coworkerd-launcher", "workerd"]) {
        await copyFile(`${BIN}/${name}`, `${STACK}/bin/${name}`);
        await chmod(`${STACK}/bin/${name}`, 0o755);
      }
      // The Server finds the allowlist beside its own executable, so the copy needs it too.
      for (const name of ["coworkerd-syscalls-startup.txt", "coworkerd-syscalls-steady.txt"])
        if (existsSync(`${BIN}/${name}`)) await copyFile(`${BIN}/${name}`, `${STACK}/bin/${name}`);
    }
  }
  if (existsSync(`${STACK}/bin/workerd`)) bin = `${STACK}/bin`;
  const configuration = { ...DEFAULTS, ...config,
    sandbox: { mode: sandbox, ...(syscalls ? { syscalls } : {}), ...(config.sandbox ?? {}) } };
  await writeFile(`${STACK}/server-config.json`, JSON.stringify(configuration));
  execFileSync("chown", ["-R", `${USER}:${USER}`, STACK]);
  if (bin !== BIN) execFileSync("chown", ["-R", "root:root", `${STACK}/bin`]);
  const environment = { COWORKERD_LEASE_MODE: "correct", COWORKERD_SUBREQUEST_GRANT: "2", COWORKERD_LEASE_TTL_MS: "10000",
    COWORKERD_LOG_BUFFER_BYTES: "4194304", ...env };
  const child = spawn("setpriv", ["--reuid", USER, "--regid", USER, "--clear-groups",
    ...(trace ? ["--inh-caps", "+sys_ptrace", "--ambient-caps", "+sys_ptrace"] : []), "--", `${bin}/coworkerd-server`,
    `${STACK}/workers/manifest.json`, workerdPath ?? `${bin}/workerd`, "0.0.0.0:8930", "127.0.0.1:8932", "127.0.0.1:8933", `${STACK}/server-config.json`],
    { env: { ...process.env, ...environment }, stdio: ["ignore", "pipe", "pipe"], detached: true, cwd: "/" });
  const output = collector();
  // V45_SERVER_LOG: the Server's output also appended to that file as it comes (diagnosis).
  const mirror = process.env.V45_SERVER_LOG ? (await import("node:fs")).createWriteStream(process.env.V45_SERVER_LOG, { flags: "a" }) : null;
  child.stdout.on("data", chunk => { output.take("stdout", chunk); mirror?.write(chunk); });
  child.stderr.on("data", chunk => { output.take("stderr", chunk); mirror?.write(chunk); });
  const exited = new Promise(resolve => child.on("exit", (code, signal) => { mirror?.write(`[v45-lab] Server ${child.pid} exited code=${code} signal=${signal}\n`); resolve({ code, signal, atUnixMs: Date.now() }); }));
  server = { child, output, exited, sandbox, exit: null };
  exited.then(result => { if (server?.child === child) server.exit = result; });
  const startedAtUnixMs = Date.now();
  if (waitReady) {
    await waitFor(async () => (await request("/__coworkerd/ready", { timeoutMs: 500 })).status === 200, "Server ready", 60000);
    await waitFor(async () => (await state()).processes.entries.every(e => e.state === "ready"), "processes ready", 60000);
  }
  return { sandbox, syscalls, trace, binaries: binaries(bin), binaryDirectory: bin, workerdPath: workerdPath ?? `${bin}/workerd`,
    configuration, environment: Object.keys(env), serverPid: child.pid, startedAtUnixMs };
}
// The Server's output as it comes, per stream: its own records (the lines lines() reads) are kept whole; the rest -- among
// it the per-request decision lines, about 2 KB a request -- only as the last TAIL characters (a 30-minute dense load
// prints more than one string can hold).
const RECORD = /^\[(process(-[a-z-]+)?|sandbox|syscall-observed|server)\] \{/, TAIL = 1 << 20;
function collector() {
  const records = [], partial = { stdout: "", stderr: "" }, decoders = { stdout: new StringDecoder("utf8"), stderr: new StringDecoder("utf8") };
  let tail = "";
  return {
    records,
    tail: () => tail,
    take(stream, chunk) {
      const text = decoders[stream].write(chunk);
      const parts = (partial[stream] + text).split("\n");
      partial[stream] = parts.pop();
      for (const line of parts) if (RECORD.test(line)) records.push(line);
      tail = (tail + text).slice(-TAIL);
    },
  };
}
export const serverOutput = () => server ? server.output.tail() : "";
export const serverPid = () => server?.child.pid ?? null;
export const serverExit = () => server?.exit ?? null;

// SIGTERM to the Server process group (the Server stops its workerd children), SIGKILL after 10 s.
export async function stop() {
  if (!server) return null;
  const { child, exited, output } = server;
  server = null;
  try { process.kill(-child.pid, "SIGTERM"); } catch {}
  const result = await Promise.race([exited, delay(10000).then(() => null)]);
  if (!result) { try { process.kill(-child.pid, "SIGKILL"); } catch {} }
  const final = result ?? await exited;
  return { ...final, lines: parse(output.records), outputTail: output.tail().slice(-4000) };
}
// Forgets the Server without stopping it (3.4: it was killed; its output is kept).
export function abandon() {
  const current = server;
  server = null;
  if (!current) return "";
  // A process of its group may keep the output pipes open (its async log writer drains before it exits): they no
  // longer hold this harness.
  current.child.stdout.destroy(); current.child.stderr.destroy(); current.child.unref();
  return current.output.tail();
}

// The Server's own records from its standard output: [process], [process-launch], [process-rotation],
// [process-binary], [sandbox], [server] lines.
export function lines(text) {
  return parse(text === undefined ? (server?.output.records ?? []) : text.split("\n").filter(l => RECORD.test(l)));
}
function parse(records) {
  return records.map(l => {
    const at = l.indexOf("] ");
    try { return { tag: l.slice(1, at), ...JSON.parse(l.slice(at + 2)) }; } catch { return { tag: l.slice(1, at), raw: l.slice(0, 2000) }; }
  });
}

// What /proc shows of a process, read as root: credentials, capability sets, the
// no_new_privs bit, the owner of its /proc files (root when it is not dumpable), namespaces against `against`'s,
// id maps, mounts, network interfaces and routes, descriptors.
export function observe(pid, against) {
  const read = path => { try { return readFileSync(path, "utf8"); } catch (error) { return null; } };
  const status = read(`/proc/${pid}/status`) ?? "";
  const field = name => (status.match(new RegExp(`^${name}:\\s*(.*)$`, "m")) ?? [])[1]?.trim().replace(/\s+/g, " ") ?? null;
  const ns = name => { try { return readlinkSync(`/proc/${pid}/ns/${name}`); } catch (error) { return `unreadable: ${error.code}`; } };
  const theirs = name => { try { return readlinkSync(`/proc/${against}/ns/${name}`); } catch (error) { return `unreadable: ${error.code}`; } };
  const namespaces = Object.fromEntries(["mnt", "net", "user", "ipc", "uts", "pid", "cgroup"].map(n => [n, { process: ns(n), server: theirs(n) }]));
  const mounts = (read(`/proc/${pid}/mountinfo`) ?? "").trim().split("\n").filter(Boolean).map(line => {
    const fields = line.split(" "), dash = fields.indexOf("-");
    return { point: fields[4], options: fields[5], root: fields[3], fstype: fields[dash + 1] };
  });
  const interfaces = (read(`/proc/${pid}/net/dev`) ?? "").split("\n").filter(l => l.includes(":")).map(l => l.split(":")[0].trim());
  // Routes without RTF_REJECT (0x0200): every IPv6 table holds the kernel's null entries, which are not routes.
  const count = (text, header, flagsField) => (text ?? "").split("\n").filter(Boolean).slice(header)
    .filter(line => !(parseInt(line.trim().split(/\s+/)[flagsField], 16) & 0x0200)).length;
  let owner = null;
  try { owner = statSync(`/proc/${pid}/environ`).uid; } catch {}
  const descriptors = {};
  try { for (const fd of readdirSync(`/proc/${pid}/fd`)) { try { descriptors[fd] = readlinkSync(`/proc/${pid}/fd/${fd}`); } catch {} } } catch {}
  return { pid, observedAtUnixMs: Date.now(), uids: field("Uid"), gids: field("Gid"), capInh: field("CapInh"), capPrm: field("CapPrm"),
    capEff: field("CapEff"), capBnd: field("CapBnd"), capAmb: field("CapAmb"), noNewPrivs: field("NoNewPrivs"), seccomp: field("Seccomp"),
    // How many filters the process carries: the launcher's startup table, and the steady-state table the
    // process installs itself once it serves. Reported by the kernel since 5.9.
    seccompFilters: field("Seccomp_filters"),
    procOwnerUid: owner, uidMap: (read(`/proc/${pid}/uid_map`) ?? "").trim().replace(/\s+/g, " "),
    gidMap: (read(`/proc/${pid}/gid_map`) ?? "").trim().replace(/\s+/g, " "), namespaces, mounts, interfaces,
    ipv4Routes: count(read(`/proc/${pid}/net/route`), 1, 3), ipv6Routes: count(read(`/proc/${pid}/net/ipv6_route`), 0, 8),
    exeTarget: (() => { try { return readlinkSync(`/proc/${pid}/exe`); } catch (error) { return `unreadable: ${error.code}`; } })(), descriptors };
}
// The verdict on each layer from an observation (the same definitions the Server uses for its own report).
export function layers(o) {
  const differs = name => o.namespaces[name].process !== o.namespaces[name].server && !o.namespaces[name].process.startsWith("unreadable");
  const zero = value => value !== null && /^0+$/.test(value);
  const single = map => /^65534 \d+ 1$/.test(map);
  return { user: differs("user") && single(o.uidMap) && single(o.gidMap), mount: differs("mnt") && o.mounts.some(m => m.point === "/" && /^ro(,|$)/.test(m.options)),
    network: differs("net") && o.ipv4Routes === 0 && o.ipv6Routes === 0, ipc: differs("ipc"), uts: differs("uts"),
    capabilities: ["capInh", "capPrm", "capEff", "capBnd", "capAmb"].every(set => zero(o[set])), noNewPrivs: o.noNewPrivs === "1",
    nonDumpable: o.procOwnerUid === null ? null : o.procOwnerUid === 0, seccomp: o.seccomp === "2",
    steadySyscalls: o.seccompFilters === null ? null : Number(o.seccompFilters) >= 2 };
}
// What the workerd processes themselves wrote (the node's drainer appends their output to logs/workerd.log):
// the [coworkerd-seccomp] rows among them, which only the process can write.
export function workerdRows(tag = null) {
  let text = "";
  try { text = readFileSync(`${STACK}/logs/workerd.log`, "utf8"); } catch { return []; }
  const rows = [];
  for (const line of text.split("\n")) {
    const at = line.indexOf("] {");
    if (!line.startsWith("[") || at < 0) continue;
    const name = line.slice(1, at);
    if (tag && name !== tag) continue;
    try { rows.push({ tag: name, ...JSON.parse(line.slice(at + 2)) }); } catch { rows.push({ tag: name, raw: line.slice(0, 500) }); }
  }
  return rows;
}
export function processState(pid) {
  try { return readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1]?.[0] ?? null; } catch { return null; }
}
export const alive = pid => { const s = processState(pid); return s !== null && s !== "Z" && s !== "X"; };

export async function saveRecord(path, data) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(data, null, 1) + "\n");
}
export const sh = (command, options = {}) => execFileSync("bash", ["-c", command], { encoding: "utf8", maxBuffer: 1 << 28, ...options });
export { assert };

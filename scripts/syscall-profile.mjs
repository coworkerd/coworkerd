// Profiles the system calls of a workerd build: the Server starts
// its processes as it always does, with the "workerd executable" replaced by scripts/syscall-trace-wrapper.sh,
// which lets strace attach to itself and then executes workerd -- so the pid the Server knows is workerd, and
// every thread is traced from that execve on. Each process's startup is the part before the Server saw it
// ready; the steady state is cut into the workload windows below, by the Server's own timestamps.
//
// This is the step of the build that scripts/syscall-allowlist.py turns into the allowlist of this workerd
// (the allowlist is generated with the build). The sandbox is off while profiling: the launcher's own calls are not workerd's,
// and the allowlist's generator adds them with their reason.
//
// Usage (inside a profiling container: scripts/profile-lab.mjs):
//   node scripts/syscall-profile.mjs <main|idle> <output-dir> [--minutes N]
//     main  the workload windows: idle 10 s; cold (20
//           distinct Workers, one request each: the runtime fetches every source over source.sock); normal
//           (8 clients, 20 ms period, 15 s: the dense load); bytes (sequential byte-dense requests, 3 s);
//           apis (the APIs that hand native bytes to JS); binding (a service binding call: the runtime calls
//           back over binding.sock); cpu-limit (an infinite loop the CPU limit ends); memory-limit (an
//           over-limit ArrayBuffer refused); oom (a Wasm allocation: V8's fatal OOM, the exit, the next
//           generation's startup); after-oom; rotation (every slot rotated: a generation change); then
//           the Server's SIGTERM (the exit)
//     idle  idle eviction at 2 s: an isolate removed, destroyed and recreated
// Environment: V45_BIN (the binaries to profile), V45_STACK, V45_MACHINE (names kept from the tool's first version).
import { chmod, mkdir, readdir } from "node:fs/promises";
import { resolve } from "node:path";
import * as h from "./profile-lab.mjs";
import * as w from "./profile-workers.mjs";

const [run, outputArgument] = process.argv.slice(2);
// Absolute: the Server runs with "/" as its working directory, so the wrapper's strace would resolve a
// relative trace directory against the root, fail to open its output, and leave the wrapper waiting.
const output = resolve(outputArgument);
const traceDir = `${output}/traces`;
await mkdir(traceDir, { recursive: true });
// The wrapper's strace writes as the Server's user (10001), while this harness runs as root: a trace
// directory it cannot write would leave every wrapper waiting for a tracer that never attaches.
await chmod(traceDir, 0o777);
const workerd = `${h.BIN}/workerd`;
const data = { schemaVersion: 1, round: "v45", kind: "profile", run, machine: process.env.V45_MACHINE ?? null,
  traceProgram: workerd, startedAtUnixMs: Date.now(), windows: [], passed: false };
const windows = data.windows;
const t0 = Date.now();
const note = (...values) => console.error(`[v45-profile] ${((Date.now() - t0) / 1000).toFixed(1)}s`, ...values);

async function window(name, body) {
  const startUnixMs = Date.now();
  const result = await body();
  windows.push({ name, startUnixMs, endUnixMs: Date.now(), result });
  note(name, JSON.stringify(result).slice(0, 160));
}
function load({ path, clients, periodMs, durationMs }) {
  const statuses = [];
  let running = true;
  const tasks = Array.from({ length: clients }, async () => {
    while (running) {
      const started = performance.now();
      statuses.push((await h.request(path, { timeoutMs: 3000 })).status);
      if (periodMs) await h.delay(Math.max(0, periodMs - (performance.now() - started)));
    }
  });
  return (async () => {
    await h.delay(durationMs);
    running = false;
    await Promise.all(tasks);
    return { requests: statuses.length, ok: statuses.filter(s => s === 200).length };
  })();
}
async function sequential(path, ms) {
  const statuses = [], end = Date.now() + ms;
  while (Date.now() < end) statuses.push((await h.request(path)).status);
  return { requests: statuses.length, ok: statuses.filter(s => s === 200).length };
}
async function repeat(path, count) {
  const out = [];
  for (let n = 0; n < count; n++) {
    const r = await h.request(path);
    out.push({ status: r.status, code: h.json(r)?.code ?? null });
  }
  return out;
}
// Every traced workerd as the Server sees it: its descriptors, its threads, its /proc facts.
async function census() {
  const current = await h.state();
  return current.processes.entries.map(e => ({ processId: e.processId, generation: e.generation, pid: e.pid,
    state: e.state, sandbox: e.sandbox, observed: h.observe(e.pid, process.pid) }));
}

const environment = { COWORKERD_TRACE_DIR: traceDir, COWORKERD_TRACE_PROGRAM: workerd };
try {
  if (run === "main") {
    const workers = { ordinary: w.ordinary, "bytes-heavy": w.bytesHeavy, probe: w.sandboxProbe, "cpu-tenant": w.cpuTenant,
      "mem-ab": w.memoryTenant("mem-ab", ["arraybuffer"]), "mem-wasm": w.memoryTenant("mem-wasm", ["wasm"]),
      caller: { source: w.bindingCaller, bindings: [{ name: "TARGET", workerId: "callee" }] }, callee: w.bindingCallee };
    for (const id of w.isolateIds(20)) workers[id] = w.ordinary;
    Object.assign(data, await h.setup({ sandbox: "off", workers, env: environment, trace: true,
      workerdPath: `${h.REPO}/scripts/syscall-trace-wrapper.sh` }));
    data.afterStartup = await census();
    await window("idle", async () => { await h.delay(10000); return {}; });
    await window("cold", async () => {
      const statuses = [];
      for (const id of w.isolateIds(20)) statuses.push((await h.request(`/${id}`)).status);
      return { statuses };
    });
    await window("normal", () => load({ path: "/ordinary", clients: 8, periodMs: 20, durationMs: 15000 }));
    await window("bytes", () => sequential("/bytes-heavy", 3000));
    await window("apis", () => repeat("/probe?apis", 5));
    await window("binding", () => repeat("/caller", 10));
    await window("cpu-limit", () => repeat("/cpu-tenant?infinite", 3));
    await window("memory-limit", () => repeat("/mem-ab?arraybuffer", 3));
    await window("oom", async () => {
      const r = await h.request("/mem-wasm?wasm");
      await h.delay(500);
      await h.waitFor(async () => (await h.state()).processes.entries.every(e => e.state === "ready"), "ready after the OOM", 30000);
      return { status: r.status, body: (r.body ?? r.error ?? "").slice(0, 300) };
    });
    await window("after-oom", () => repeat("/ordinary", 5));
    // The generation change: every slot rotated, each slot's next generation starting while requests run.
    await window("rotation", async () => {
      const before = await h.state();
      const requested = await h.request("/__coworkerd/rotate?all=1", { method: "POST", timeoutMs: 30000 });
      const requests = load({ path: "/ordinary", clients: 2, periodMs: 50, durationMs: 1000 });
      await h.waitFor(async () => {
        const now = await h.state();
        return now.processes.rotation.inProgress === null && now.processes.rotation.queue.length === 0 &&
          now.processes.entries.every(e => e.state === "ready") &&
          now.processes.entries.every((e, i) => e.generation > before.processes.entries[i].generation);
      }, "every slot rotated", 120000);
      return { requested: requested.status, load: await requests,
        generations: (await h.state()).processes.entries.map(e => ({ processId: e.processId, generation: e.generation })) };
    });
    data.beforeShutdown = await census();
  } else if (run === "idle") {
    Object.assign(data, await h.setup({ sandbox: "off", workers: { ordinary: w.ordinary }, env: environment, trace: true,
      workerdPath: `${h.REPO}/scripts/syscall-trace-wrapper.sh`,
      config: { idleEvictMs: 2000, placement: { homeProbability: 0, idleMs: 1000 } } }));
    data.afterStartup = await census();
    await window("first", () => repeat("/ordinary", 3));
    await window("idle-evict", async () => { await h.delay(6000); return {}; });
    await window("recreate", () => repeat("/ordinary", 3));
    data.beforeShutdown = await census();
  } else {
    throw new Error(`unknown run ${run}`);
  }
  const shutdownStart = Date.now();
  data.shutdown = await h.stop();
  windows.push({ name: "shutdown", startUnixMs: shutdownStart, endUnixMs: Date.now(),
    result: { code: data.shutdown?.code, signal: data.shutdown?.signal } });
  // Every [process] line of the Server's output, the shutdown included: each pid's spawn, ready and exit time.
  data.processLines = data.shutdown.lines;
  data.traces = (await readdir(traceDir)).sort();
  data.passed = true;
} catch (error) {
  data.error = String(error?.stack ?? error);
  data.processLines = h.lines();
  data.serverOutputTail = h.serverOutput().slice(-20000);
  try { data.shutdown ??= await h.stop(); } catch {}
  process.exitCode = 1;
} finally {
  data.finishedAtUnixMs = Date.now();
  await h.saveRecord(`${output}/profile-${run}.json`, data);
  console.log(JSON.stringify({ run, passed: data.passed, traces: data.traces?.length ?? 0,
    error: data.error?.split("\n")[0] }));
}

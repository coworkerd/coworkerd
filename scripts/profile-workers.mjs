// Worker sources of the system call profile (scripts/syscall-profile.mjs): each is a minimal workload of one
// mechanism (CPU limits, memory limits, an ordinary request, a service binding through patch 0001), so that the
// profile that generates the allowlist puts a workerd process through all of them.

// The ordinary Worker: the latency target and the per-isolate memory fixture.
export const ordinary = `export default { async fetch(request) {
  return Response.json({ server: request.cf.serverId, process: request.cf.processId,
    generation: request.cf.processGeneration, invocationId: request.cf.invocationId });
} };\n`;

// The CPU tenant, without a caller binding.
export const cpuTenant = `let instance, prepared;
function burn(count) { let checksum = 0; for (let n = 0; n < count; n++) checksum = Math.imul(checksum ^ n, 1664525) | 0; return checksum; }
export default { async fetch(request) {
  const url = new URL(request.url);
  instance ??= crypto.randomUUID();
  const identity = { workerId: "cpu-tenant", invocationId: request.cf.invocationId, executingProcess: request.cf.processId,
    processGeneration: request.cf.processGeneration, instance };
  if (url.searchParams.has("prepare")) { prepared = 'ab'.repeat(Number(url.searchParams.get("mib") || 32) * 1024 * 1024 / 2); return Response.json(identity); }
  if (url.searchParams.has("builtin")) { prepared.replace(/a/g, 'c'); return Response.json(identity); }
  if (url.searchParams.has("infinite")) { while (true) {} }
  const checksum = burn(Number(url.searchParams.get("iterations") || 0));
  return Response.json({ ...identity, checksum });
} };\n`;

// The over-limit tenant with every allocation kind; one tenant id per kind
// so a fatal kind's suspicion does not route another kind to the quarantine process.
const allocations = {
  gradual: "if (url.searchParams.has('gradual')) { const value = new Array(MiB / 4).fill(7); held.push(value); checksum = value[0]; }",
  'array-single': "if (url.searchParams.has('array-single')) { const value = new Array(256 * MiB / 4).fill(7); held.push(value); checksum = value[0]; }",
  arraybuffer: "if (url.searchParams.has('arraybuffer')) { const value = new ArrayBuffer(Number(url.searchParams.get('mib') || 2048) * MiB); checksum = touch(value); held.push(value); }",
  wasm: "if (url.searchParams.has('wasm')) { const value = new WebAssembly.Memory({ initial: 1, maximum: 8192 }); held.push(value); value.grow(2047); checksum = touch(value.buffer); }",
  builtin: "if (url.searchParams.has('prepare')) { prepared = 'ab'.repeat(32 * MiB / 2); checksum = prepared.length; }\n  if (url.searchParams.has('builtin')) { while (true) { const value = prepared.replace(/a/g, 'c'); checksum ^= value.length; } }",
  // One heap allocation of 320 MiB, far above the 128 MiB limit: repeat() builds a rope, and charCodeAt()
  // flattens it into one sequential one-byte string (a single copy, well inside the CPU limit).
  bigstring: "if (url.searchParams.has('bigstring')) { const value = 'ab'.repeat(160 * MiB); checksum = value.charCodeAt(0); held.push(value); }",
  // Main-cage exhaustion (run with the memory limit off): 32 MiB double arrays (fast
  // elements, grown by push) accumulate in the pointer-compression cage.
  cage: "if (url.searchParams.has('cage')) { for (let n = 0; n < Number(url.searchParams.get('count') || 1); n++) { const value = []; for (let i = 0; i < 4 * MiB; i++) value.push(0.5); held.push(value); } checksum = held.length; }",
};
export function memoryTenant(id, kinds) {
  return `let instance, prepared, held = [];
const MiB = 1024 * 1024;
function touch(buffer) { const view = new Uint8Array(buffer); view.fill(37); let checksum = 0; for (let i = 0; i < view.length; i += 4096) checksum += view[i]; return checksum; }
export default { async fetch(request) {
  instance ??= crypto.randomUUID();
  const url = new URL(request.url), identity = { workerId: ${JSON.stringify(id)}, invocationId: request.cf.invocationId,
    executingProcess: request.cf.processId, processGeneration: request.cf.processGeneration, instance };
  let checksum = 0;
  ${kinds.map(kind => allocations[kind]).join("\n  ")}
  if (url.searchParams.has('small')) { const value = new ArrayBuffer(64 * 1024); checksum = touch(value); }
  return Response.json({ ...identity, retainedObjects: held.length, checksum });
} };\n`;
}

// Where tenant memory lives, and the paths that hand native bytes to JS. hold keeps an ArrayBuffer
// of the given size alive in the isolate; apis exercises APIs whose results are created from memory
// allocated outside V8 (digests, bodies, blobs, compression) and returns their digests.
export const sandboxProbe = `const held = [];
async function hex(buffer) { return [...new Uint8Array(await crypto.subtle.digest('SHA-256', buffer))].map(b => b.toString(16).padStart(2, '0')).join(''); }
export default { async fetch(request) {
  const url = new URL(request.url);
  if (url.searchParams.has('hold')) {
    const bytes = Number(url.searchParams.get('hold')) * 1024 * 1024, value = new ArrayBuffer(bytes);
    new Uint8Array(value).fill(91); held.push(value);
    return Response.json({ held: held.length, bytes, process: request.cf.processId });
  }
  if (url.searchParams.has('apis')) {
    const input = new Uint8Array(1 << 20); for (let i = 0; i < input.length; i++) input[i] = (i * 2654435761) >>> 24;
    const results = {};
    results.digest = await hex(input);
    results.responseBody = await hex(await new Response(input).arrayBuffer());
    results.blob = await hex(await new Blob([input, input]).arrayBuffer());
    const gzip = await new Response(new Blob([input]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer();
    results.roundTrip = await hex(await new Response(new Blob([gzip]).stream().pipeThrough(new DecompressionStream('gzip'))).arrayBuffer());
    results.textEncoder = await hex(new TextEncoder().encode('sandbox '.repeat(4096)));
    results.structuredClone = await hex(structuredClone(input.buffer));
    const random = new Uint8Array(65536); crypto.getRandomValues(random);
    results.randomNonZero = random.some(b => b !== 0);
    const key = await crypto.subtle.importKey('raw', input.slice(0, 32), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    results.hmac = await hex(await crypto.subtle.sign('HMAC', key, input));
    return Response.json({ process: request.cf.processId, results });
  }
  return Response.json({ held: held.length, process: request.cf.processId });
} };\n`;

// A supplementary sequential load for the byte-dense paths: ArrayBuffers allocated in the isolate, and bytes
// produced natively (bodies, digests) handed to JS.
export const bytesHeavy = `const input = new Uint8Array(256 * 1024).fill(7);
export default { async fetch(request) {
  let total = 0;
  for (let i = 0; i < 8; i++) total += (await new Response(input).arrayBuffer()).byteLength;
  const small = []; for (let i = 0; i < 2000; i++) small.push(new Uint8Array(512));
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', input));
  return Response.json({ process: request.cf.processId, total, small: small.length, digest: digest[0] });
} };\n`;

// Patch 0001: a call through a service binding leaves workerd over the Cap'n Proto external service
// (env.SERVER in the runtime) and comes back as a child invocation of the same tree.
export const bindingCaller = `export default { async fetch(request, env) {
  const response = await env.TARGET.fetch(new Request("http://fixture/child?from=caller"));
  return Response.json({ caller: { invocationId: request.cf.invocationId, treeId: request.cf.treeId, process: request.cf.processId },
    childStatus: response.status, child: await response.json() });
} };\n`;
export const bindingCallee = `export default { async fetch(request) {
  return Response.json({ workerId: "callee", invocationId: request.cf.invocationId, treeId: request.cf.treeId,
    process: request.cf.processId, query: new URL(request.url).search });
} };\n`;

// The collections between requests (gc.idleCollectAfterMs): each request keeps `keep` MiB of small objects alive
// until the next one, so the old generation grows by that much and its isolate is collected once it is idle.
export const gcTenant = `let kept = [];
export default { async fetch(request) {
  const mib = Number(new URL(request.url).searchParams.get("keep") || 2), next = [];
  for (let i = 0; i < mib * 16384; i++) next.push({ i, s: "gc-" + i, t: [i] });
  kept = next;
  return Response.json({ process: request.cf.processId, kept: kept.length });
} };\n`;

export const isolateIds = count => Array.from({ length: count }, (_, i) => `iso-${String(i).padStart(3, "0")}`);

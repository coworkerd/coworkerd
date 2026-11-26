import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { access, open, rm, stat } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { eventually } from '../cluster.mjs';

// Use the innermost recognizable stage. Shared Cap'n Proto frames have no direction on
// their own, so defer those until all named frames in the stack have been considered.
const stages = [
  ['durableLog', /DurableLog::|DurableChannel::|DurableShip::|DurableReceiver::|appendToLogFile/],
  ['loggingMetering', /TenantUsage::|WorkerdOutput::|AsyncLog::|LogSink::|SourceService::.*(?:post|log|usage)|ServerCore::(?:\w*Log|\w*Debit|debitObjectCall|observeRuntimeCpu|noteCpu|charge|usage|recordExecution|recordRelay|recordForward)|tenantLineText|cleanReason|std::(?:basic_ostream|__ostream_insert)|AdmissionSlot::report|AdmissionHttpService::limitCode/],
  ['nodeToRuntime', /ServerCore::encodeDescriptor\(/],
  ['leaseAdmission', /ServerCore::(?:rootDeadline|allocateRoot|allocateChild|executeAllocated|reserve|admission\w*|admit\w*|addLease|expireLeases|reapExpiredLeases|releaseLease|holdLease|renewLease|whenLeaseExpires|confirmCapacityRejection)|AdmissionSlot|InvocationLeaseState|LeaseMembrane|runLeaseReaper\(|remainingLeaseMs|initialPlacementAdmission|tenantAdmission/],
  ['runtimeToNode', /BindingBootstrap::|BindingRpcService::|BindingDispatch::|RpcBindTarget::|DurableDispatch::|DeferredObjectDispatcher::|PeerForwarder::(?:allocate|dispatch|place|start|openBinding)|ServerCore::(?:recordBinding|resolveDefinition|durableTarget)/],
  ['nodeToRuntime', /HttpForwarder::|AdmissionHttpService::|AdmissionDispatcher::|ExecutionResponse::|RuntimeConnection::|::ensureRuntime\(|::openRuntimeEvent\(|ServerCore::(?:execute\(|open\(|loadCheck|selectProcess|selectDurableProcess|ensureCode|ensureDefinition|descriptor\()/],
  ['receiveRoute', /GatewayService::|TrackingHttpResponse::|ServerCore::(?:route\(|ingressBegin|ingressEnd)|DeploymentState::(?:select|route)|hostWithoutPort|kj::(?:\(anonymous namespace\)::)?Http(?:InputStream|Server|Header)/],
];
const sharedProtocol = /capnp::.*(?:Rpc|rpc|TwoParty|HttpOverCapnp|HttpService|ByteStream|Message|Segment|Membrane|Pipeline|Capability|ClientHook|copyToUnchecked)|CoarseTwoParty/;
const allocation = /^(?:malloc|cfree|free|__libc_calloc|operator delete\(|operator new\()/;
const residualStages = new Set(['allocation', 'unresolved', 'other']);
const stageNames = [...new Set(stages.map(([name]) => name)), 'sharedProtocol', ...residualStages];

function emptyHistogram() {
  return Object.fromEntries(stageNames.map(stage => [stage, { samples: 0, periodNs: 0 }]));
}

function classify(stack) {
  for (const frame of stack) {
    const match = stages.find(([, pattern]) => pattern.test(frame));
    if (match) return match[0];
  }
  if (stack.some(frame => sharedProtocol.test(frame))) return 'sharedProtocol';
  if (stack.some(frame => allocation.test(frame))) return 'allocation';
  return stack.some(frame => frame !== '[unknown]') ? 'other' : 'unresolved';
}

export async function summarizeStacks(path, [begin, end]) {
  const start = BigInt(begin);
  const stop = BigInt(end);
  const histogram = emptyHistogram();
  const stacks = {};
  const residualByDso = {};
  let period = 0;
  let stack = [];
  let leafDso;
  let available = 0;
  const firstLines = [];
  let select = false;
  function finish() {
    if (!select) return;
    const stage = classify(stack);
    ++histogram[stage].samples;
    histogram[stage].periodNs += period;
    const dso = leafDso ?? '[unknown]';
    if (residualStages.has(stage)) residualByDso[dso] = (residualByDso[dso] ?? 0) + period;
    const signature = JSON.stringify([stage, { dso }, ...stack]);
    const count = stacks[signature] ??= { samples: 0, periodNs: 0 };
    ++count.samples;
    count.periodNs += period;
  }

  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (firstLines.length < 8) firstLines.push(line.slice(0, 160));
    const sample = line.match(/\s(\d+)\.(\d{3,9}):\s+(\d+)\s+cpu-clock(?:[:\w-]*):/);
    if (sample) {
      ++available;
      finish();
      const time = BigInt(sample[1]) * 1_000_000_000n + BigInt(sample[2].padEnd(9, '0'));
      select = time >= start && time < stop;
      period = Number(sample[3]);
      stack = [];
      leafDso = undefined;
    } else if (select) {
      const frame = line.match(/^\s*[0-9a-f]+\s+(.+?)(?:\+0x[0-9a-f]+)?\s+\(([^()]*)\)(?:\s+\(inlined\))?\s*$/i);
      if (frame) {
        stack.push(frame[1]);
        leafDso ??= frame[2];
      }
    }
  }
  finish();
  const samples = Object.values(histogram).reduce((sum, entry) => sum + entry.samples, 0);
  assert.ok(samples > 0, `no in-window cpu-clock samples among ${available} records for ${begin}–${end} ns in ${path}\n${firstLines.join('\n')}`);
  return { samples, histogram, residualByDso, stacks };
}

export async function recordPerf({ pid, data, script, drive, seconds }) {
  const ready = data + '.ready';
  // perf releases its child when events are open. The data file can appear well before that.
  const child = spawn('sudo', ['-n', 'perf', 'record', '--clockid', 'mono',
    '-e', 'cpu-clock', '-F', '499', '--call-graph', 'fp', '--buildid-all', '-t', String(pid), '-o', data,
    '--', 'sh', '-c', 'touch "$1"; exec sleep "$2"', 'perf-recording', ready, String(seconds)],
  { stdio: ['ignore', 'ignore', 'pipe'] });
  const closed = once(child, 'close');
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
  let result;
  try {
    await Promise.race([
      eventually('perf event is open', () => access(ready), { timeout: 15000 }),
      closed.then(([code]) => { throw new Error(`perf record exited (${code}) before measurement\n${stderr}`); }),
    ]);
    result = await drive();
  } finally {
    const [code] = await closed;
    assert.equal(code, 0, stderr);
  }
  const fd = await open(script, 'w');
  try {
    const dump = spawn('sudo', ['-n', 'perf', 'script', '-i', data, '--ns',
      '--symfs', '/proc/' + pid + '/root', '-F', 'comm,tid,time,event,period,ip,sym,dso'],
    { stdio: ['ignore', fd.fd, 'inherit'] });
    const [code] = await once(dump, 'close');
    assert.equal(code, 0, 'perf script failed');
  } finally {
    await fd.close();
    await Promise.all([data, ready].map(path => rm(path, { force: true })));
  }
  const bytes = (await stat(script)).size;
  assert.ok(bytes > 0, 'perf produced no stack dump for TID ' + pid + '\n' + stderr);
  return { ...result, perfCapture: { tid: pid, scriptBytes: bytes, recordLog: stderr.trim() } };
}

// Reapply the same classifier to an archived stack-signature artifact.
if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const [path] = process.argv.slice(2);
  assert.ok(path, 'usage: node perf.mjs <stacks.jsonl>');
  const rows = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of rows) {
    const { operation, clients, round, samples, stacks } = JSON.parse(line);
    const histogram = emptyHistogram();
    const residualByDso = {};
    for (const [key, count] of Object.entries(stacks)) {
      const [, ...stack] = JSON.parse(key);
      const dso = typeof stack[0] === 'object' ? stack.shift().dso : undefined;
      const stage = classify(stack);
      if (dso && residualStages.has(stage)) residualByDso[dso] = (residualByDso[dso] ?? 0) + count.periodNs;
      const bucket = histogram[stage];
      bucket.samples += count.samples;
      bucket.periodNs += count.periodNs;
    }
    assert.equal(Object.values(histogram).reduce((sum, bucket) => sum + bucket.samples, 0), samples);
    console.log(JSON.stringify({ operation, clients, round, samples, histogram,
      ...(Object.keys(residualByDso).length > 0 && { residualByDso }) }));
  }
}

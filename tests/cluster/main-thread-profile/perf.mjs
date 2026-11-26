import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { access, open, rm } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { eventually } from '../cluster.mjs';

// Use the innermost recognizable stage. Shared Cap'n Proto frames have no direction on
// their own, so defer those until all named frames in the stack have been considered.
const stages = [
  ['durableLog', /DurableLog::|DurableChannel::|DurableShip::|DurableReceiver::|appendToLogFile/],
  ['loggingMetering', /TenantUsage::|WorkerdOutput::|AsyncLog::|LogSink::|SourceService::.*(?:post|log|usage)|ServerCore::(?:\w*Log|\w*Debit|debitObjectCall|observeRuntimeCpu|noteCpu|charge|usage|recordExecution|recordRelay|recordForward)|tenantLineText|cleanReason/],
  ['leaseAdmission', /ServerCore::(?:rootDeadline|allocateRoot|allocateChild|executeAllocated|reserve|admission\w*|admit\w*|addLease|expireLeases|reapExpiredLeases|releaseLease|holdLease|renewLease|whenLeaseExpires|confirmCapacityRejection)|AdmissionSlot|InvocationLeaseState|LeaseMembrane|remainingLeaseMs|initialPlacementAdmission|tenantAdmission/],
  ['runtimeToNode', /BindingBootstrap::|BindingRpcService::|BindingDispatch::|RpcBindTarget::|DurableDispatch::|DeferredObjectDispatcher::|PeerForwarder::(?:allocate|dispatch|place|start|openBinding)|ServerCore::(?:recordBinding|resolveDefinition|durableTarget)/],
  ['nodeToRuntime', /HttpForwarder::|AdmissionHttpService::|AdmissionDispatcher::|ExecutionResponse::|RuntimeConnection::|::ensureRuntime\(|::openRuntimeEvent\(|ServerCore::(?:execute\(|open\(|loadCheck|selectProcess|selectDurableProcess|ensureCode|ensureDefinition|descriptor\()/],
  ['receiveRoute', /GatewayService::|TrackingHttpResponse::|ServerCore::(?:route\(|ingressBegin|ingressEnd)|DeploymentState::(?:select|route)|hostWithoutPort|kj::(?:\(anonymous namespace\)::)?Http(?:InputStream|Server|Header)/],
];
const sharedProtocol = /capnp::.*(?:Rpc|rpc|TwoParty|HttpOverCapnp|HttpService|ByteStream|Message|Segment|copyToUnchecked)|CoarseTwoParty/;
const stageNames = [...stages.map(([name]) => name), 'sharedProtocol', 'other'];

function classify(stack) {
  for (const frame of stack) {
    const match = stages.find(([, pattern]) => pattern.test(frame));
    if (match) return [match[0], frame];
  }
  const protocol = stack.find(frame => sharedProtocol.test(frame));
  return protocol ? ['sharedProtocol', protocol] : ['other', stack.find(frame => frame !== '[unknown]') ?? '[unknown]'];
}

export async function summarizeStacks(path, [begin, end]) {
  const start = BigInt(begin);
  const stop = BigInt(end);
  const histogram = Object.fromEntries(stageNames.map(stage => [stage, { samples: 0, periodNs: 0 }]));
  const stacks = {};
  let period = 0;
  let stack = [];
  let available = 0;
  let select = false;
  function finish() {
    if (!select) return;
    const [stage] = classify(stack);
    ++histogram[stage].samples;
    histogram[stage].periodNs += period;
    const signature = JSON.stringify([stage, ...stack]);
    const count = stacks[signature] ??= { samples: 0, periodNs: 0 };
    ++count.samples;
    count.periodNs += period;
  }

  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    const sample = line.match(/^\s*\S+\s+\d+\s+(\d+)\.(\d{9}):\s+(\d+)\s+cpu-clock:/);
    if (sample) {
      ++available;
      finish();
      const time = BigInt(sample[1]) * 1_000_000_000n + BigInt(sample[2]);
      select = time >= start && time < stop;
      period = Number(sample[3]);
      stack = [];
    } else if (select) {
      const frame = line.match(/^\s*[0-9a-f]+\s+(.+?)(?:\+0x[0-9a-f]+)?\s+\((.+)\)\s*$/i);
      if (frame) stack.push(frame[1]);
    }
  }
  finish();
  const samples = Object.values(histogram).reduce((sum, entry) => sum + entry.samples, 0);
  assert.ok(samples > 0, `no in-window cpu-clock samples among ${available} records in ${path}`);
  return { samples, histogram, stacks };
}

export async function recordPerf({ pid, data, script, drive }) {
  const child = spawn('sudo', ['-n', 'perf', 'record', '--control=fd:0', '--clockid', 'mono',
    '-e', 'cpu-clock', '-F', '499', '--call-graph', 'dwarf,8192', '-t', String(pid), '-o', data],
  { stdio: ['pipe', 'ignore', 'pipe'] });
  const closed = once(child, 'close');
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
  child.stdin.on('error', error => { stderr += error.message; });
  let result;
  try {
    await Promise.race([
      eventually('perf event is open', () => access(data), { timeout: 10000 }),
      closed.then(([code]) => { throw new Error(`perf record exited (${code}) before measurement\n${stderr}`); }),
    ]);
    result = await drive();
  } finally {
    child.stdin.end('stop\n');
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
    await rm(data, { force: true });
  }
  return result;
}

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = (path) => readFileSync(new URL(path, import.meta.url), 'utf8');
const node = read('./main.c++');
const runtime = read('../runtime/runtime.js');
const guide = read('../docs/guide/errors.md');

function section(source, start, end) {
  const a = source.indexOf(start);
  const b = source.indexOf(end, a + start.length);
  assert.ok(a >= 0 && b > a, `Missing contract section: ${start} / ${end}`);
  return source.slice(a, b);
}

function evidence(source, pattern) {
  const result = source.match(pattern);
  assert.ok(result, `Public error source shape changed: ${pattern}`);
  return result;
}

function table(text) {
  const rows = new Map();
  for (const line of text.split('\n').filter((line) => /^\| \d{3} \|/.test(line))) {
    const [status, codes, meaning, executed, action] = line.slice(2, -2).split(' | ');
    for (const [, code] of codes.matchAll(/`([A-Z][A-Z_]+)`/g)) {
      const key = `${status}:${code}`;
      assert.ok(!rows.has(key), `Duplicate row: ${key}`);
      rows.set(key, { meaning, executed, action });
    }
  }
  assert.ok(rows.size > 0, 'No Worker HTTP error rows parsed');
  return rows;
}

// Inventories the node-owned Worker HTTP responses and Binding refusals in the
// first table. DO stub replies, management endpoints and raw WebSocket closes
// are separate protocols. These checks read the producers; no status/code list
// is copied from the guide. Keep the producer boundaries explicit so a change
// to a helper or to a branch fails visibly instead of dropping its rows.
function workerResponses() {
  const expected = new Map();
  const add = (status, code, executed = 'Not executed', flags = {}) => {
    const key = `${status}:${code}`;
    assert.ok(!expected.has(key) || expected.get(key).executed === executed,
      `Conflicting execution semantics for ${key}`);
    expected.set(key, { executed, ...flags });
  };
  const [, defaultStatus] = evidence(node, /InvocationRejection refusal\(std::string code, std::string reason, uint status = (\d{3})/);
  const generic = [
    section(node, 'class ServerCore final', node.includes('std::optional<std::string> confirmPreparation') ? 'std::optional<std::string> confirmPreparation' : 'std::optional<std::string> confirmCapacityRejection'),
    section(node, 'class PeerForwarder final', 'class PeerBootstrap final'),
    section(node, 'class PeerBootstrap final', 'class PeerRpcService final'),
    section(node, 'class GatewayService final', '\n};'),
  ].join('\n');
  // Refusals with literal codes. The only DO branch in these shared producer
  // sections is separated by its DO_ prefix; its HTTP table is independent.
  for (const [, code, tail] of generic.matchAll(/\b(?:refusal|rejected)\("([A-Z_]+)",([^;]+);/gs)) {
    if (code.startsWith('DO_') || code === 'LOAD_FAILED') continue;
    const status = tail.match(/,\s*(\d{3})(?:\s*[,);}]|$)/g)?.at(-1)?.match(/\d{3}/)[0] ?? defaultStatus;
    add(status, code, code === 'RESULT_UNCERTAIN' ? 'Result uncertain' : 'Not executed');
  }
  const [, quarantine, unavailable, processStatus] = evidence(generic,
    /\brefusal\(error\.quarantine \? "([A-Z_]+)" : "([A-Z_]+)",[\s\S]*?, (\d{3}), root\);/);
  for (const code of [quarantine, unavailable]) add(processStatus, code);

  const [, missing, brokenRoute, brokenWorker] = evidence(generic,
    /std::string code = missing \? "([A-Z_]+)" : selected\.kind == Kind::ROUTE_INVALID \? "([A-Z_]+)" : "([A-Z_]+)"/);
  const [, missingStatus, invalidStatus] = evidence(generic, /refusal\(code, reason, missing \? (\d{3}) : (\d{3})/);
  add(missingStatus, missing);
  for (const code of [brokenRoute, brokenWorker]) add(invalidStatus, code);

  const dispatch = section(node, 'class AdmissionHttpService final', 'struct InvocationAttempt');
  let uncertain;
  const limitSection = section(dispatch, 'std::string limitCode() const', 'kj::Own<kj::HttpService> inner');
  const limits = [...limitSection.matchAll(/process->([a-zA-Z]+)\(\)\) return "([A-Z_]+)";/g)];
  assert.ok(limits.length >= 2, 'CPU and memory limit producers');
  if (dispatch.includes('const bool unsupported =')) {
    const [, unsupported, specialStatus, normalStatus, reportedUncertain] = evidence(dispatch,
      /const bool unsupported = code == "([A-Z_]+)";\s*slot->report\(unsupported \? (\d{3}) : (\d{3}), code.empty\(\) \? "([A-Z_]+)" : code\);/);
    uncertain = reportedUncertain;
    add(specialStatus, unsupported, 'Partly executed');
    add(normalStatus, uncertain, 'Result uncertain');
    for (const [, method, code] of limits) {
      add(normalStatus, code, method === 'processMemoryPressure' ? 'Result uncertain' : 'Partly executed');
    }
  } else {
    const [, responseStatus, reportedUncertain] = evidence(dispatch, /slot->report\((\d{3}), code\.empty\(\) \? "([A-Z_]+)" : code\);/);
    uncertain = reportedUncertain;
    add(responseStatus, uncertain, 'Result uncertain');
    assert.equal(limits.length, 2, 'CPU and memory limit responses');
    for (const [, , code] of limits) add(responseStatus, code, 'Partly executed');
  }

  const [, budget, expired, outOfOrder] = evidence(node,
    /server\.refusal\(budget \? "([A-Z_]+)" :\s*expired \? "([A-Z_]+)" : "([A-Z_]+)", detail, status\)/);
  const [, budgetStatus, expiredStatus] = evidence(node,
    /const uint status = budget \? (\d{3}) : expired \? (\d{3}) : (\d{3});/);
  add(budgetStatus, budget);
  add(expiredStatus, expired);
  // The ordering refusal is only reachable in the intentionally incorrect
  // lease counterexample. Check that guard before excluding it from the table.
  assert.equal(outOfOrder, 'BINDING_ARRIVAL_OUT_OF_ORDER');
  const orderCheck = section(node, '    if (leaseConfig.naive()) {\n      auto orderKey', '      ++expected;');
  evidence(orderCheck, /if \(orderSequence != expected\)[\s\S]*lastRejection = "Binding arrival out of order:/);
  for (const [, status, code] of node.matchAll(/sendText\(response, (\d{3}), "[^"]+",\s*rejectionJson\("([A-Z_]+)"/g)) {
    add(status, code, code === uncertain ? 'Result uncertain' : 'Not executed');
  }

  if (node.includes('std::optional<std::string> confirmPreparation')) {
  const [, loadCode, prepareStatus, loadStatus] = evidence(generic,
    /refusal\("([A-Z_]+)", trialClean\(message, secrets, variables\), preparationLimit \? (\d{3}) : (\d{3}), root\)/);
  evidence(generic, /flag\("preparationLimit"\)\) \{\s*category = "limit"; detail = "PREPARATION_LIMIT";[^\n]*preparationLimit = true;/);
  evidence(generic, /else if \(!probe\.culprit\.empty\(\)\) \{\s*category = "limit";[^\n]*\n[^}]*memory limit exceeded while loading[^}]*CPU limit exceeded while loading/);
  const confirm = section(node, 'std::optional<std::string> confirmPreparation', '  void reload()');
  const [, runtimeLimit, runtimeUnavailable, limited, unavailableStatus] = evidence(confirm,
    /refusal\(slot->preparationLimit \? "([A-Z_]+)" : "([A-Z_]+)", slot->preparationReason, slot->preparationLimit \? (\d{3}) : (\d{3})/);
  assert.equal(runtimeLimit, loadCode);
  assert.equal(limited, prepareStatus);
  const [, platformLimited, platformUnavailable] = evidence(runtime,
    /return Response\.json\(reply\.refusal, \{status:preparationLimit \? (\d{3}) : (\d{3})\}\)/);
  assert.deepEqual([platformLimited, platformUnavailable], [limited, unavailableStatus]);
  evidence(confirm, /slot->decide\(false\);/);
  evidence(confirm, /if \(slot->preparationLimit\) refused\.category = "limit";/);
  const [, serialCode, serialStatus, serialCategory, dispatched, retryable] = evidence(node,
    /if \(code == "([A-Z_]+)" && status == (\d{3}) && category == "([a-z-]+)"\)\s*out << ",\\"dispatched\\":(true|false),\\"retryable\\":(true|false)";/);
  assert.deepEqual([serialCode, serialStatus, serialCategory, dispatched], [loadCode, prepareStatus, 'limit', 'false']);
  const [, unavailableDispatched, unavailableRetryable] = evidence(confirm,
    /if \(!slot->preparationLimit\) \{\s*body\.pop_back\(\); body \+= ",\\"dispatched\\":(true|false),\\"retryable\\":(true|false)\}";/);
  assert.equal(unavailableDispatched, 'false');
  add(prepareStatus, loadCode, 'Not executed', { dispatched, retryable, category: serialCategory });
  add(loadStatus, loadCode);
  add(unavailableStatus, runtimeUnavailable, 'Not executed', { dispatched: unavailableDispatched, retryable: unavailableRetryable });
  } else {
    // Ordinary preparation and its 409/CODE_UNAVAILABLE HTTP
    // producers are retired. Do not import dead producers just to match the main table.
    const [, loadCode, loadStatus] = evidence(generic,
      /refusal\("([A-Z_]+)", trialClean\(message, secrets, variables\), (\d{3}), root\)/);
    add(loadStatus, loadCode);
    evidence(node, /confirmCapacityRejection/);
  }
  return expected;
}

test('Worker HTTP error table ↔ node producers', () => {
  const actual = table(section(guide, '## 1. When calling a Worker', 'Once the Worker accepted a WebSocket'));
  const expected = workerResponses();
  assert.deepEqual({ undocumented: [...expected.keys()].filter((key) => !actual.has(key)).sort(),
    hasNoProducer: [...actual.keys()].filter((key) => !expected.has(key)).sort() },
  { undocumented: [], hasNoProducer: [] });
  for (const [key, contract] of expected) {
    const { meaning, executed, action } = actual.get(key);
    assert.equal(executed, contract.executed, `${key}: execution`);
    for (const field of ['dispatched', 'retryable', 'category']) {
      if (contract[field] !== undefined) assert.ok(meaning.includes(`\`${field}: ${contract[field]}\``), `${key}: ${field}`);
    }
    if (contract.retryable === 'false') assert.doesNotMatch(action, /\bretry\b/i, `${key}: retry advice`);
    else if (contract.retryable === 'true') assert.match(action, /\bretry\b/i, `${key}: retry advice`);
    if (key === '503:RESULT_UNCERTAIN') assert.match(action, /\bidempotent\b/i, `${key}: retry advice`);
    if (key === '503:LOAD_FAILED') {
      assert.match(meaning, /`limit` \(exceeding the CPU or memory limit while loading the Worker\)/);
      assert.doesNotMatch(meaning, /preparation memory budget|`retryable:/);
    }
  }
});

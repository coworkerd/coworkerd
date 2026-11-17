#include "alarm-publication.h"
#include "durable-shards.h"
#include "json.h"
#include "support.h"
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <openssl/rand.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <sstream>
#include <time.h>
#include <charconv>
#include <optional>
#include <vector>
#include <fstream>

namespace coworkerd {
namespace {
uint64_t threadTime() { timespec t{};clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);return uint64_t(t.tv_sec) * 1000000000ull + t.tv_nsec; }
bool canceled(const kj::Exception& e) { return std::string_view(e.getDescription().cStr()).find("alarm publication ownership changed") != std::string_view::npos; }
uint64_t nanoTime() { return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
std::string wide(uint64_t n) { auto s = std::to_string(n);return s.size() >= 20 ? s : std::string(20 - s.size(), '0') + s; }
std::string markerPrefix(const std::string& node, uint64_t member, const std::string& ns, uint32_t shard, uint64_t epoch) {
  return "alarm-wake/by-owner/" + node + "/" + wide(member) + "/" + ns + "/" + std::to_string(shard) + "/" + wide(epoch) + "/";
}
std::string hintPrefix(const std::string& ns, uint32_t shard) { return "do/" + ns + "/shards/" + std::to_string(shard) + "/alarms/"; }
std::string presence() {
  unsigned char bytes[16];KJ_REQUIRE(RAND_bytes(bytes, sizeof(bytes)) == 1, "cannot draw a presence identifier");
  static constexpr char hex[] = "0123456789abcdef";std::string s;
  for (auto b: bytes) { s += hex[b >> 4];s += hex[b & 15]; }return s;
}
uint64_t unixMs() { return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(); }
bool markerName(const std::string& s) { return s.size() == 32 && std::all_of(s.begin(), s.end(), [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }); }
bool accessDenied(const kj::Exception& e) {
  const std::string_view text(e.getDescription().cStr());return text.find("HTTP 403") != text.npos || text.find("AccessDenied") != text.npos;
}
}
void AlarmPublication::Metric::add(uint64_t ns) {
  uint64_t index = ns / 100;
  if (ns >= 51200) {
    uint64_t base = 51200;unsigned group = 0;
    while (ns >= base * 2 && group < 31) { base *= 2;++group; }
    index = 512 + group * 16 + std::min<uint64_t>(15, (ns - base) * 16 / base);
  }
  ++bins[index];++count;maximum = std::max(maximum, ns);
}
double AlarmPublication::Metric::percentile(unsigned percent) const {
  if (!count) return 0;uint64_t n = 0, target = (count * percent + 99) / 100;
  for (size_t i = 0; i < bins.size(); ++i) if ((n += bins[i]) >= target) {
    if (i < 512) return (i + 1) / 10.0;
    const uint64_t base = 51200ull << ((i - 512) / 16);
    return (i == bins.size() - 1 ? maximum : base + base * ((i - 512) % 16 + 1) / 16) / 1000.0;
  }
  return 0;
}
AlarmPublication::Scope::Scope(AlarmPublication& owner, Metric* metric): owner(owner), metric(metric), started(metric || !owner.cpuDepth ? nanoTime() : 0) {
  if (!owner.cpuDepth++) { owner.cpuStarted = started;if (owner.measureThreadCpu) owner.threadCpuStarted = threadTime(); }
}
AlarmPublication::Scope::~Scope() {
  const auto now = metric || owner.cpuDepth == 1 ? nanoTime() : 0;if (metric) metric->add(now - started);if (!--owner.cpuDepth) { owner.cpuNs += now - owner.cpuStarted;if (owner.measureThreadCpu) owner.threadCpuNs += threadTime() - owner.threadCpuStarted; }
}

bool AlarmPublication::Gate::available(double now) const { return disabled || !open || (!probing && now >= next); }
bool AlarmPublication::Gate::enter(double now, const std::string& key) {
  if (disabled || !open) return false;
  KJ_REQUIRE(!probing && now >= next);probing = true;lastProbe = key;return true;
}
void AlarmPublication::Gate::result(bool success, bool probe, double now, const std::string& key) {
  if (probe) probing = false;
  if (success) { failures = 0;diverse = false;firstFailure.clear();lastProbe.clear();open = false;next = 0;return; }
  if (disabled) return;
  if (!open) {
    if (!failures) firstFailure = key;else if (firstFailure != key) diverse = true;
    if (failures < 8) ++failures;
    if (failures >= 8 && diverse) { open = true;next = now + 5000; }
  } else if (probe) next = now + 5000;
}
void AlarmPublication::Gate::cancel(bool probe) { if (probe) probing = false; }

AlarmPublication::AlarmPublication(ObjectStore& store, kj::Timer& timer, std::string nodeId, Current current, Adopt adopt, Log log)
    : store(store), timer(timer), nodeId(std::move(nodeId)), current(std::move(current)), adopt(std::move(adopt)), log(std::move(log)), tasks(*this) {
  KJ_REQUIRE(RAND_bytes(reinterpret_cast<unsigned char*>(&retryRandom), sizeof(retryRandom)) == 1);
  if (!retryRandom) retryRandom = 0x9e3779b97f4a7c15ull;
  if (const auto* value = std::getenv("COWORKERD_ALARM_THREAD_CPU_FOR_TEST")) measureThreadCpu = std::string_view(value) == "1";
  if (const auto* value = std::getenv("COWORKERD_ALARM_HINT_CHECK_MS_FOR_TEST")) {
    char* end = nullptr;const auto n = std::strtoull(value, &end, 10);
    if (end && !*end && n >= 200 && n <= 3600000) hintRoundMs = n;
  }
}
AlarmPublication::~AlarmPublication() noexcept(false) { stopping = true; }
AlarmPublication::ReadBudget::Permit::~Permit() noexcept(false) { budget.release(); }
kj::Own<AlarmPublication::ReadBudget::Permit> AlarmPublication::ReadBudget::acquireNow() {
  KJ_REQUIRE(active < limit);++active;return kj::heap<Permit>(*this);
}
kj::Promise<kj::Own<AlarmPublication::ReadBudget::Permit>> AlarmPublication::ReadBudget::acquire() {
  if (active < limit) return acquireNow();
  auto pair = kj::newPromiseAndFulfiller<kj::Own<Permit>>();waiters.push_back(kj::mv(pair.fulfiller));return kj::mv(pair.promise);
}
void AlarmPublication::ReadBudget::release() {
  while (!waiters.empty()) {
    auto waiter = kj::mv(waiters.front());waiters.pop_front();
    if (waiter->isWaiting()) { waiter->fulfill(kj::heap<Permit>(*this));return; }
  }
  --active;if (available) available();
}
kj::Promise<void> AlarmPublication::guardRead(kj::Promise<void> promise) {
  return promise.attach(readBudget->acquireNow(), kj::defer([this]() { pump(); }));
}
void AlarmPublication::pendingFailure() {
  ++pendingFileFailures;
  if (monotonicTimeMs() >= nextPendingLog) {
    nextPendingLog = monotonicTimeMs() + 30000;
    log("alarm-pending-file-failed", "\"failures\":" + std::to_string(pendingFileFailures));
  }
}
void AlarmPublication::pending(Shard& s, bool value) {
  if (pendingDirectory.empty() || s.pendingRecorded == value || (value && s.markerWritten)) return;
  std::error_code error;const auto path = pendingDirectory / (s.ns + "-" + std::to_string(s.shard) + ".pending");
  if (value) {
    std::filesystem::create_directories(pendingDirectory, error);
    if (error) { pendingFailure();return; }
    std::ofstream file(path);file.close();
    if (!file.good()) { pendingFailure();return; }
  } else {
    std::filesystem::remove(path, error);
    if (error) { pendingFailure();return; }
  }
  s.pendingRecorded = value;
}
bool AlarmPublication::forgetPending(const std::string& ns, uint32_t shard) {
  const auto key = shardKey(ns, shard);
  if (inFlightHints.contains(key)) return false;
  auto it = shards.find(key);
  if (it != shards.end() && valid(it->second) && !it->second.markerWritten && !it->second.desired.empty()) return false;
  if (it != shards.end() && it->second.markerWritten && !it->second.pendingRecorded) return true;
  if (!pendingDirectory.empty()) {
    std::error_code error;std::filesystem::remove(pendingDirectory / (ns + "-" + std::to_string(shard) + ".pending"), error);
    if (error) pendingFailure();else if (it != shards.end()) it->second.pendingRecorded = false;
  }
  return true;
}
std::string AlarmPublication::shardKey(const std::string& ns, uint32_t shard) const { return ns + "/" + std::to_string(shard); }
bool AlarmPublication::valid(const Shard& s) const { return !stopping && nodeEpoch && s.memberEpoch == nodeEpoch && current(s.ns, s.shard, s.epoch, s.memberEpoch); }
bool AlarmPublication::valid(const Write& w) const {
  auto s = shards.find(shardKey(w.ns, w.shard));return s != shards.end() && s->second.generation == w.generation && valid(s->second);
}
bool AlarmPublication::deleting(const std::string& key) const {
  auto w = writes.find(key);return inFlightDeletes.contains(key) || (w != writes.end() && w->second.marker && !w->second.at);
}
double AlarmPublication::hintCheckInterval() const { return hintRoundMs / std::max<uint64_t>(1, nonemptyShards); }
void AlarmPublication::setNodeEpoch(uint64_t epoch) {
  if (epoch == nodeEpoch) return;nodeEpoch = epoch;shards.clear();writes.clear();writeSweep.clear();sweepCursor = writeSweep.end();
  deadlines.clear();readyHints.clear();readyReads.clear();readyMarkers.clear();readyScans.clear();ownMarkers.clear();observedOwn.clear();
  markerPuts.clear();markerRemoves.clear();cleanupReady.clear();nonemptyShards = 0;initialMarkerScan = true;nextMarkerScan = 0;
}
void AlarmPublication::take(std::string ns, uint32_t shard, uint64_t epoch, std::string tenant) {
  if (!nodeEpoch || !epoch) return;const auto key = shardKey(ns, shard);auto it = shards.find(key);
  if (it != shards.end() && it->second.epoch == epoch && it->second.memberEpoch == nodeEpoch && valid(it->second)) {
    auto& s = it->second;s.startupOnly = false;if (!s.reading) { s.adopted = false;s.next = 0; }
    offerRead(s);pump();return;
  }
  gone(ns, shard);Shard s;s.ns = std::move(ns);s.shard = shard;s.epoch = epoch;s.tenant = std::move(tenant);
  s.memberEpoch = s.markerMemberEpoch = nodeEpoch;s.generation = s.order = ++serial;s.checked = monotonicTimeMs();
  if (!pendingDirectory.empty()) { std::error_code error;s.pendingRecorded = std::filesystem::exists(pendingDirectory / (s.ns + "-" + std::to_string(shard) + ".pending"), error); }
  auto [held, inserted] = shards.emplace(key, std::move(s));offerRead(held->second);pump();
}
void AlarmPublication::ensureTake(std::string ns, uint32_t shard, uint64_t epoch, std::string tenant) {
  auto it = shards.find(shardKey(ns, shard));
  if (it != shards.end() && it->second.epoch == epoch && valid(it->second) &&
      (!it->second.adopted || it->second.reading || !it->second.hints.empty() || !it->second.desired.empty())) {
    offerRead(it->second);pump();return;
  }
  take(std::move(ns), shard, epoch, std::move(tenant));
}
void AlarmPublication::restore(std::string ns, uint32_t shard, std::string tenant) {
  const auto key = shardKey(ns, shard);if (!nodeEpoch || shards.contains(key)) return;
  Shard s;s.ns = std::move(ns);s.shard = shard;s.tenant = std::move(tenant);s.startupOnly = true;
  s.memberEpoch = nodeEpoch;s.generation = s.order = ++serial;s.checked = monotonicTimeMs();
  auto [held, inserted] = shards.emplace(key, std::move(s));offerRead(held->second);pump();
}
void AlarmPublication::gone(const std::string& ns, uint32_t shard) {
  const auto key = shardKey(ns, shard);auto it = shards.find(key);if (it == shards.end()) return;auto& s = it->second;
  readyReads.erase({s.tenant, s.order, key, 0});readyScans.erase({s.tenant, s.repairOrder, key, 2});readyScans.erase({s.tenant, s.previousOrder, key, 4});
  if (s.readTimed) deadlines.erase({s.next, 0, key, s.generation});
  if (s.hintTimed) deadlines.erase({s.hintAt, 2, key, s.generation});
  if (s.previousTimed) deadlines.erase({s.previousNext, 3, key, s.generation});
  if (s.repairTimed) deadlines.erase({s.repairNext, 4, key, s.generation});
  if (s.nonempty) --nonemptyShards;ownMarkers.erase(s.marker);cleanupReady.erase(key);shards.erase(it);
  // Retire old write rows in bounded sweep slices; requests retain their key reservations.
}
void AlarmPublication::ownershipChanged(const std::string& key, uint64_t generation) {
  ++ownerChanges;auto it = shards.find(key);if (it != shards.end() && it->second.generation == generation) gone(it->second.ns, it->second.shard);
}
void AlarmPublication::unqueue(Write& w) {
  w.queued = false;
  (w.marker ? readyMarkers : readyHints).erase({w.tenant, w.order, w.key, w.marker ? 3u : 1u});
  if (w.timed) { deadlines.erase({w.next, 1, w.key, w.generation});w.timed = false; }
}
void AlarmPublication::eraseWrite(std::unordered_map<std::string, Write>::iterator it) {
  auto& w = it->second;unqueue(w);markerPuts.erase(w.key);markerRemoves.erase(w.key);
  if (auto s = shards.find(shardKey(w.ns, w.shard));s != shards.end() && s->second.generation == w.generation) s->second.writeKeys.erase(w.key);
  if (sweepCursor == w.sweep) ++sweepCursor;writeSweep.erase(w.sweep);writes.erase(it);
}
void AlarmPublication::refresh(Write& w, double now) {
  unqueue(w);if (!valid(w) || inFlightKeys.contains(w.key)) return;
  if (w.next > now) { deadlines.insert({w.next, 1, w.key, w.generation});w.timed = true;return; }
  const auto& s = shards.at(shardKey(w.ns, w.shard));if (!s.adopted || s.reading) return;
  (w.marker ? readyMarkers : readyHints).insert({w.tenant, w.order, w.key, w.marker ? 3u : 1u});w.queued = true;
}
void AlarmPublication::offerRead(Shard& s) {
  const auto key = shardKey(s.ns, s.shard);readyReads.erase({s.tenant, s.order, key, 0});
  if (s.readTimed) { deadlines.erase({s.next, 0, key, s.generation});s.readTimed = false; }
  if (!valid(s) || s.adopted || s.reading || inFlightHints.contains(key)) return;
  if (s.next > monotonicTimeMs()) { deadlines.insert({s.next, 0, key, s.generation});s.readTimed = true; }
  else readyReads.insert({s.tenant, s.order, key, 0});
}
void AlarmPublication::offerScan(Shard& s, bool repair) {
  const auto key = shardKey(s.ns, s.shard);auto& timed = repair ? s.repairTimed : s.previousTimed;
  auto& next = repair ? s.repairNext : s.previousNext;auto& order = repair ? s.repairOrder : s.previousOrder;
  const unsigned kind = repair ? 2 : 4, deadlineKind = repair ? 4 : 3;
  readyScans.erase({s.tenant, order, key, kind});
  if (timed) { deadlines.erase({next, deadlineKind, key, s.generation});timed = false; }
  if (!valid(s) || !s.adopted || s.reading || s.scanning || !(repair ? s.repairPending : s.previousPending)) return;
  if (!order) order = ++serial;
  if (next > monotonicTimeMs()) { deadlines.insert({next, deadlineKind, key, s.generation});timed = true; }
  else readyScans.insert({s.tenant, order, key, kind});
}
void AlarmPublication::refreshActivity(Shard& s) {
  const bool nonempty = !s.hints.empty() || !s.desired.empty() || !s.marker.empty();const auto key = shardKey(s.ns, s.shard);
  if (nonempty != s.nonempty) { if (nonempty) ++nonemptyShards;else --nonemptyShards;s.nonempty = nonempty; }
  if (s.hintTimed && (!nonempty || !s.adopted || s.reading)) { deadlines.erase({s.hintAt, 2, key, s.generation});s.hintTimed = false; }
  if (nonempty && s.adopted && !s.reading && !s.hintTimed) {
    s.hintAt = std::max(monotonicTimeMs(), s.checked + hintRoundMs);s.hintTimed = true;deadlines.insert({s.hintAt, 2, key, s.generation});
  }
}
void AlarmPublication::wakeWrites(Shard& s) {
  for (const auto& key: s.writeKeys) if (auto it = writes.find(key);it != writes.end() && it->second.generation == s.generation) refresh(it->second, monotonicTimeMs());
}
void AlarmPublication::queueHint(Shard& s, const std::string& object, uint64_t at) {
  const auto key = hintPrefix(s.ns, s.shard) + object + "/0";auto [it, inserted] = writes.try_emplace(key);auto& w = it->second;
  if (inserted) { writeSweep.push_back(key);w.sweep = std::prev(writeSweep.end()); }
  const bool keepReady = w.queued && w.generation == s.generation && w.tenant == s.tenant && s.adopted && !s.reading;
  // A ready identity is unchanged by a new target/version; keep it indexed until dispatch.
  if (w.timed || w.generation != s.generation || w.tenant != s.tenant) unqueue(w);
  w.key = key;w.ns = s.ns;w.object = object;w.tenant = s.tenant;w.shard = s.shard;
  w.generation = s.generation;w.at = at;w.marker = false;w.version = ++serial;w.order = w.order ? w.order : serial;w.next = 0;w.failures = 0;
  s.writeKeys.insert(key);peakPending = std::max<uint64_t>(peakPending, writes.size());if (!keepReady) refresh(w, 0);
}
void AlarmPublication::queueMarker(Shard& s, const std::string& key, bool present, bool mayExist) {
  auto it = writes.find(key);
  if (it != writes.end() && it->second.generation == s.generation && it->second.at == uint64_t(present)) return;
  const bool inserted = it == writes.end();auto& w = writes[key];
  if (inserted) { writeSweep.push_back(key);w.sweep = std::prev(writeSweep.end()); }
  unqueue(w);markerPuts.erase(key);markerRemoves.erase(key);w.key = key;w.ns = s.ns;w.object.clear();w.tenant = s.tenant;w.shard = s.shard;
  w.generation = s.generation;w.marker = true;w.at = present;w.mayExist = w.mayExist || mayExist;w.version = ++serial;
  w.order = w.order ? w.order : serial;w.next = 0;w.failures = 0;s.writeKeys.insert(key);(present ? markerPuts : markerRemoves).insert(key);
  peakPending = std::max<uint64_t>(peakPending, writes.size());refresh(w, monotonicTimeMs());
}
void AlarmPublication::report(std::string ns, std::string object, uint32_t shard, uint64_t epoch, std::string tenant, uint64_t at) {
  Scope measured(*this, &reportMetric);const auto key = shardKey(ns, shard);
  if (!shards.contains(key)) take(ns, shard, epoch, std::move(tenant));
  auto it = shards.find(key);if (it == shards.end() || it->second.epoch != epoch || !valid(it->second)) return;
  auto& s = it->second;if (at) s.desired[object] = at;else s.desired.erase(object);
  if (!s.adopted || s.reading) s.duringRead[object] = at;queueHint(s, object, at);reconcile(s);pump();
}
void AlarmPublication::cleanup(Shard& s) {
  if (!s.previousMarkers.empty() && s.adopted && !s.reading && (s.markerWritten || (s.hints.empty() && s.desired.empty()))) cleanupReady.insert(shardKey(s.ns, s.shard));
}
void AlarmPublication::reconcile(Shard& s) {
  if (!valid(s) || !s.adopted || s.reading) { refreshActivity(s);return; }
  if (!s.hints.empty()) {
    if (s.marker.empty()) {
      s.marker = markerPrefix(nodeId, s.markerMemberEpoch, s.ns, s.shard, s.epoch) + presence();s.markerWritten = s.markerMayExist = false;
      ownMarkers[s.marker] = {shardKey(s.ns, s.shard), s.generation};
    }
    if (!s.markerWritten) queueMarker(s, s.marker, true, s.markerMayExist);
  } else if (s.desired.empty() && !s.marker.empty()) {
    const auto old = s.marker;auto w = writes.find(old);const bool mayExist = s.markerWritten || s.markerMayExist || (w != writes.end() && w->second.mayExist);
    if (mayExist || inFlightKeys.contains(old)) queueMarker(s, old, false, mayExist);
    else if (w != writes.end()) eraseWrite(w);
    ownMarkers.erase(old);s.marker.clear();s.markerWritten = s.markerMayExist = false;s.hintCommittedNs = 0;
  }
  refreshActivity(s);cleanup(s);
}
void AlarmPublication::acceptOwnMarker(Shard& s, const std::string& key) {
  if (!valid(s) || !s.adopted || s.reading || deleting(key) || deletedDuringScan.contains(key)) return;
  if (s.marker.empty() && !s.hints.empty()) {
    s.marker = key;s.markerWritten = s.markerMayExist = true;s.markerConfirmedAt = monotonicTimeMs();ownMarkers[key] = {shardKey(s.ns, s.shard), s.generation};
  }
  if (s.marker == key) { s.previousMarkers.erase(key);s.markerWritten = s.markerMayExist = true; }
  else s.previousMarkers.insert(key);
  cleanup(s);
}
uint32_t AlarmPublication::retry(uint32_t& failures) {
  failures = std::min<uint32_t>(failures + 1, 32);++retries;const auto base = std::min<uint32_t>(5000, 250u << std::min<uint32_t>(failures - 1, 5));
  retryRandom ^= retryRandom << 13;retryRandom ^= retryRandom >> 7;retryRandom ^= retryRandom << 17;
  return std::min<uint32_t>(5000, base + retryRandom % std::max<uint32_t>(1, base / 4));
}
void AlarmPublication::failure(const char* operation) {
  if (operation) ++failuresToLog;if (!failuresToLog || monotonicTimeMs() < nextFailureLog) return;
  const auto count = failuresToLog;failuresToLog = 0;nextFailureLog = monotonicTimeMs() + 1000;++failureLogLines;
  log("alarm-publication-failures", "\"count\":" + std::to_string(count) + ",\"operation\":" + json::quote(operation ? operation : "merged") + ",\"markerAccessDenied\":" + (markerDenied ? "true" : "false"));
}
void AlarmPublication::denied(Probe operation, std::string prefix) {
  ++markerDenials;if (markerDenied) { if (operation != Probe::LIST) deniedOperation = operation;return; }markerDenied = true;deniedOperation = operation;deniedPrefix = std::move(prefix);nextMarkerProbe = monotonicTimeMs() + 30000;failure("marker-access");
}
void AlarmPublication::markerRestored() {
  if (!markerDenied) return;markerDenied = false;markerGate.result(true, false, monotonicTimeMs(), nodeId);
  log("alarm-marker-access-restored", "\"denials\":" + std::to_string(markerDenials));
}
void AlarmPublication::pump(unsigned limit) {
  Scope measured(*this);if (stopping) return;
  for (unsigned attempt = 0; attempt < limit && active < 16; ++attempt) {
    const auto now = monotonicTimeMs();Ready work{};bool have = false, permissionProbe = false, gateProbe = false;
    auto choose = [this](std::set<Ready>& queue, const Gate& gate) {
      auto it = queue.upper_bound({lastTenant, UINT64_MAX, "", 5});if (it == queue.end()) it = queue.begin();
      // A bounded skip prevents a permanently failing item from monopolizing probes within one tenant.
      if (gate.open && it->key == gate.lastProbe && queue.size() > 1) { if (++it == queue.end()) it = queue.begin(); }
      auto value = *it;queue.erase(it);if (value.kind == 1 || value.kind == 3) if (auto w = writes.find(value.key);w != writes.end()) w->second.queued = false;return value;
    };
    // Separate capacities preserve seven H slots even if all eight marker writes and one scan hang.
    // M writes precede every scan; the total is bounded by sixteen.
    if (activeMarkers < 8 && markerDenied && !permissionProbeActive && now >= nextMarkerProbe) {
      auto& candidates = deniedOperation == Probe::DELETE ? markerRemoves : markerPuts;
      if (deniedOperation != Probe::LIST) {
        unsigned n = 0;
        for (auto it = candidates.begin();it != candidates.end() && n++ < 16;++it) {
          auto w = writes.find(*it);if (w == writes.end() || !valid(w->second) || inFlightKeys.contains(*it)) continue;
          auto& s = shards.at(shardKey(w->second.ns, w->second.shard));if (!s.adopted || s.reading) continue;
          work = {w->second.tenant, w->second.order, w->first, 3};unqueue(w->second);have = permissionProbe = true;break;
        }
      }
      nextMarkerProbe = now + 30000;permissionProbeActive = true;++markerProbes;
      if (!have && (activeScans || readBudget->active >= readBudget->limit)) { permissionProbeActive = false;nextMarkerProbe = now + 200;continue; }
      if (!have) { ++active;++activeScans;tasks.add(guardRead(kj::evalLater([this]() { return probeMarkers(); })));continue; }
    }
    if (!have && activeMarkers < 8 && !markerDenied && !readyMarkers.empty() && markerGate.available(now)) { work = choose(readyMarkers, markerGate);have = true; }
    if (!have && activeHints < 7 && !readyHints.empty() && hintGate.available(now)) { work = choose(readyHints, hintGate);have = true; }
    if (!have && activeHints < 7 && readBudget->active < readBudget->limit && !readyReads.empty() && hintGate.available(now)) { work = choose(readyReads, hintGate);have = true; }
    if (!have && !activeScans && readBudget->active < readBudget->limit && !markerDenied && markerGate.available(now)) {
      if (!readyScans.empty()) { work = choose(readyScans, markerGate);have = true; }
      else if (!globalScanRunning && (initialMarkerScan || (!ownMarkers.empty() && now >= nextMarkerScan))) {
        gateProbe = markerGate.enter(now, nodeId);globalScanRunning = true;++active;++activeScans;
        tasks.add(guardRead(kj::evalLater([this, gateProbe]() { return scanMarkers(gateProbe); })));continue;
      }
    }
    if (!have) return;lastTenant = work.tenant;
    if (work.kind == 0 || work.kind == 2 || work.kind == 4) {
      auto it = shards.find(work.key);if (it == shards.end()) continue;
      if (!valid(it->second)) { ownershipChanged(work.key, it->second.generation);continue; }auto& s = it->second;const auto generation = s.generation;
      if (work.kind == 0) {
        if (s.reading || s.adopted || inFlightHints.contains(work.key)) continue;
        gateProbe = hintGate.enter(now, work.key);s.reading = true;refreshActivity(s);++active;++activeHints;
        tasks.add(guardRead(kj::evalLater([this, key = work.key, generation, gateProbe]() -> kj::Promise<void> {
          auto it = shards.find(key);
          if (it == shards.end() || it->second.generation != generation) { hintGate.cancel(gateProbe);--active;--activeHints;pump();return kj::READY_NOW; }
          return readHints(key, gateProbe);
        })));
      } else {
        if (s.scanning || !s.adopted || s.reading) continue;
        gateProbe = markerGate.enter(now, work.key);s.scanning = true;++active;++activeScans;
        tasks.add(guardRead(kj::evalLater([this, key = work.key, generation, repair = work.kind == 2, gateProbe]() -> kj::Promise<void> {
          auto it = shards.find(key);
          if (it == shards.end() || it->second.generation != generation) { markerGate.cancel(gateProbe);--active;--activeScans;pump();return kj::READY_NOW; }
          return scanPrevious(key, repair, gateProbe);
        })));
      }
    } else {
      auto it = writes.find(work.key);
      if (it == writes.end() || !valid(it->second) || inFlightKeys.contains(work.key)) {
        if (permissionProbe) permissionProbeActive = false;
        if (it != writes.end() && !inFlightKeys.contains(work.key) && !valid(it->second)) eraseWrite(it);continue;
      }
      const auto& s = shards.at(shardKey(it->second.ns, it->second.shard));
      if (!s.adopted || s.reading) { if (permissionProbe) permissionProbeActive = false;continue; }
      if (!permissionProbe) gateProbe = (it->second.marker ? markerGate : hintGate).enter(now, work.key);
      inFlightKeys.insert(work.key);if (it->second.marker && !it->second.at) inFlightDeletes.insert(work.key);
      ++active;if (it->second.marker) ++activeMarkers;else { ++activeHints;++inFlightHints[shardKey(it->second.ns, it->second.shard)]; }
      tasks.add(kj::evalLater([this, sent = it->second, gateProbe, permissionProbe]() mutable { return write(std::move(sent), gateProbe, permissionProbe); }));
    }
  }
}
kj::Promise<void> AlarmPublication::write(Write sent, bool probe, bool permissionProbe) {
  const auto key = sent.key;
  if (auto it = writes.find(key);it != writes.end() && it->second.generation == sent.generation) sent = it->second;
  auto release = kj::defer([this, &key, &sent, permissionProbe]() {
    inFlightKeys.erase(key);inFlightDeletes.erase(key);--active;
    if (permissionProbe) permissionProbeActive = false;
    if (sent.marker) --activeMarkers;
    else {
      --activeHints;const auto sk = shardKey(sent.ns, sent.shard);auto it = inFlightHints.find(sk);
      if (it != inFlightHints.end() && !--it->second) { inFlightHints.erase(it);if (auto s = shards.find(sk);s != shards.end()) offerRead(s->second); }
    }
    if (auto it = writes.find(key);it != writes.end()) refresh(it->second, monotonicTimeMs());pump();
  });
  auto& gate = sent.marker ? markerGate : hintGate;
  if (sent.marker && !sent.at) inFlightDeletes.insert(key);
  if (permissionProbe && ((deniedOperation == Probe::PUT && !sent.at) || (deniedOperation == Probe::DELETE && sent.at))) { nextMarkerProbe = monotonicTimeMs();co_return; }
  if (!valid(sent)) { gate.cancel(probe);ownershipChanged(shardKey(sent.ns, sent.shard), sent.generation);co_return; }
  bool success = false, rejected = false;
  try {
    // This auxiliary file precedes H PUT, so a process killed before M PUT has a bounded startup candidate index.
    // It is outside SQLite, the WAL, the manifest and snapshots, and is never on the acknowledgement path.
    if (!sent.marker && sent.at) pending(shards.at(shardKey(sent.ns, sent.shard)), true);
    kj::Promise<ObjectResponse> operation = nullptr;
    {
      Scope measured(*this);
      if (sent.marker && sent.at) {
        writes.at(key).mayExist = true;auto& s = shards.at(shardKey(sent.ns, sent.shard));
        if (s.marker == key) { s.markerMayExist = true;if (s.hintCommittedNs) { markerLatencyMetric.add(nanoTime() - s.hintCommittedNs);s.hintCommittedNs = 0; } }
      }
      if (!sent.at) operation = store.remove(key);
      else if (sent.marker) operation = store.put(key, "");
      else operation = store.put(key, "{\"atUnixMs\":" + std::to_string(sent.at) + ",\"facetId\":0,\"nodeId\":" + json::quote(nodeId) + ",\"writtenAtUnixMs\":" + std::to_string(unixMs()) + "}\n");
    }
    co_await operation;success = true;
  } catch (const kj::Exception& error) { rejected = sent.marker && accessDenied(error); }
  Scope measured(*this, &completionMetric);
  if (!valid(sent)) { gate.cancel(probe);ownershipChanged(shardKey(sent.ns, sent.shard), sent.generation);co_return; }
  gate.result(success, probe, monotonicTimeMs(), key);if (permissionProbe && success) markerRestored();
  // The initial outage is logged; repeated circuit probes retain failure/retry counters without synchronous log I/O.
  if (!success) { if (!sent.marker) ++hintFailures;if (rejected) denied(sent.at ? Probe::PUT : Probe::DELETE);else if (!probe) failure(sent.marker ? "marker-write" : "hint-write"); }
  else if (sent.marker) { if (sent.at) ++markerWrites;else ++markerDeletes;if (permissionProbe) markerRestored(); }
  else ++hintWrites;
  auto found = writes.find(key);if (found == writes.end() || found->second.generation != sent.generation) co_return;
  auto& w = found->second;auto& s = shards.at(shardKey(sent.ns, sent.shard));
  if (sent.marker && rejected && sent.at) { w.mayExist = sent.mayExist;if (s.marker == key) s.markerMayExist = sent.mayExist; }
  if (success) {
    if (sent.marker) {
      w.mayExist = sent.at;
      if (s.marker == key && sent.at) { s.markerWritten = s.markerMayExist = true;s.markerConfirmedAt = monotonicTimeMs();pending(s, false); }
      if (!sent.at) {
        if (globalScanRunning) deletedDuringScan.insert(key);
        const auto prefix = key.substr(0, key.rfind('/') + 1);auto cached = observedOwn.find(prefix);
        if (cached != observedOwn.end()) { cached->second.erase(key);if (cached->second.empty()) observedOwn.erase(cached); }
      }
    } else {
      if (sent.at) { if (s.hints.empty() && s.marker.empty()) s.hintCommittedNs = nanoTime();s.hints[sent.object] = sent.at; }
      else s.hints.erase(sent.object);
      if (s.markerWritten || (s.hints.empty() && s.desired.empty())) pending(s, false);
    }
    if (w.version == sent.version) eraseWrite(found);else { w.next = 0;w.failures = 0; }
  } else {
    w.order = ++serial;
    if (w.version == sent.version) w.next = monotonicTimeMs() + retry(w.failures);else w.next = 0;
  }
  if (rejected && sent.at && w.at == 0 && !w.mayExist) { eraseWrite(found); }
  // A rejected PUT that never existed has a cancellable PUT row, never a ghost DELETE.
  reconcile(s);
}
kj::Promise<bool> AlarmPublication::verifyOwner(std::string key, uint64_t generation) {
  auto held = [this, &key, generation]() -> Shard& {
    auto it = shards.find(key);KJ_REQUIRE(it != shards.end() && it->second.generation == generation && valid(it->second), "alarm publication ownership changed");return it->second;
  };
  const auto ns = held().ns;const auto shard = held().shard;std::string token, latest;
  do {
    auto page = co_await store.listPage(shardEpochsPrefix(ns, shard) + "/", 1000, token);held();
    for (const auto& row: page.objects) {
      const auto part = row.key.substr(row.key.rfind('/') + 1);
      if (part.size() == 20 && std::all_of(part.begin(), part.end(), [](char c) { return c >= '0' && c <= '9'; })) latest = std::max(latest, row.key);
    }
    token = std::move(page.nextToken);
  } while (!token.empty());
  if (latest.empty()) { ownershipChanged(key, generation);co_return false; }
  auto response = co_await store.get(latest);held();requireStorage(response.status == 200, "alarm owner read failed: HTTP " + std::to_string(response.status));
  auto owner = parseShardEpoch(response.bytes);requireStorage(owner.has_value() && owner->namespaceId == ns && owner->shard == shard, "invalid alarm owner record");
  if (owner->nodeId != nodeId || (held().epoch && (owner->epoch != held().epoch || owner->nodeEpoch != held().markerMemberEpoch))) { ownershipChanged(key, generation);co_return false; }
  auto released = co_await store.get(shardReleasedKey(ns, shard, owner->epoch));held();requireStorage(released.status == 200 || released.status == 404, "alarm release read failed: HTTP " + std::to_string(released.status));
  if (released.status == 200) { ownershipChanged(key, generation);co_return false; }
  auto& s = held();s.epoch = owner->epoch;s.markerMemberEpoch = owner->nodeEpoch;
  if (owner->previousEpoch) s.previousPrefix = markerPrefix(owner->previousNodeId, owner->previousNodeEpoch, ns, shard, owner->previousEpoch);
  co_return true;
}
kj::Promise<void> AlarmPublication::readHints(std::string key, bool probe) {
  const auto generation = shards.at(key).generation;bool finished = false;
  auto release = kj::defer([this, &key, generation, &finished, probe]() {
    if (!finished) hintGate.cancel(probe);--active;--activeHints;if (auto it = shards.find(key);it != shards.end() && it->second.generation == generation) it->second.reading = false;if (auto it = shards.find(key);it != shards.end() && it->second.generation == generation) { offerRead(it->second);offerScan(it->second, false);offerScan(it->second, true); }pump();
  });
  auto held = [this, &key, generation]() -> Shard& { auto it = shards.find(key);KJ_REQUIRE(it != shards.end() && it->second.generation == generation && valid(it->second), "alarm publication ownership changed");return it->second; };
  try {
    if (!co_await verifyOwner(key, generation)) co_return;
    const auto ns = held().ns;const auto shard = held().shard;const auto epoch = held().epoch;
    std::map<std::string, uint64_t> observed;std::string token;const auto prefix = hintPrefix(ns, shard);
    do {
      auto page = co_await store.listPage(prefix, 1000, token);held();++pages;maxPage = std::max<uint64_t>(maxPage, page.objects.size());
      for (const auto& row: page.objects) {
        const auto part = row.key.substr(prefix.size());if (part.size() != 66 || !part.ends_with("/0")) continue;
        auto response = co_await store.get(row.key);held();if (response.status == 404) continue;
        requireStorage(response.status == 200, "alarm hint read failed: HTTP " + std::to_string(response.status));
        Scope measured(*this);
        try {
          capnp::MallocMessageBuilder message;auto value = message.initRoot<capnp::JsonValue>();capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.bytes.data(), response.bytes.size()), value);
          const auto at = json::field(value.asReader(), "atUnixMs"), facet = json::field(value.asReader(), "facetId");
          if (!at.isNumber() || !std::isfinite(at.getNumber()) || at.getNumber() <= 0 || at.getNumber() > 9007199254740991.0 || at.getNumber() != std::floor(at.getNumber()) || !facet.isNumber() || facet.getNumber() != 0) { ++badHints;continue; }
          observed[part.substr(0, 64)] = uint64_t(at.getNumber());
        } catch (const kj::Exception&) { ++badHints; }
      }
      token = std::move(page.nextToken);
    } while (!token.empty());
    Scope measured(*this);auto& s = held();s.hints = std::move(observed);
    for (const auto& [object, at]: s.duringRead) { if (at) s.desired[object] = at;else s.desired.erase(object); }
    for (const auto& [object, at]: s.hints) {
      const auto pending = writes.find(prefix + object + "/0");
      if (pending != writes.end() && pending->second.generation == generation) {
        if (pending->second.at) s.desired[object] = pending->second.at;else s.desired.erase(object);
      } else if (!s.duringRead.contains(object) && !s.desired.contains(object)) s.desired[object] = at;
      if (!s.startupOnly && s.desired.contains(object)) adopt(ns, object, shard, epoch, s.tenant, s.desired.at(object));
    }
    s.duringRead.clear();s.reading = false;s.adopted = true;s.failures = 0;s.checked = monotonicTimeMs();
    pending(s, !s.hints.empty() && !s.markerWritten);
    for (const auto& [object, at]: s.desired) if (!s.hints.contains(object) || s.hints.at(object) != at) queueHint(s, object, at);
    if (s.initial) {
      const auto ownPrefix = markerPrefix(nodeId, s.markerMemberEpoch, ns, shard, epoch);
      if (auto own = observedOwn.find(ownPrefix);own != observedOwn.end()) for (const auto& marker: own->second) acceptOwnMarker(s, marker);
      s.initial = false;
    }
    reconcile(s);wakeWrites(s);
    if (!s.previousDone && !s.previousPrefix.empty()) { s.previousPending = true;s.previousNext = 0;offerScan(s, false); }
    hintGate.result(true, probe, monotonicTimeMs(), key);finished = true;
  } catch (const kj::Exception& error) {
    Scope measured(*this);if (canceled(error)) { ownershipChanged(key, generation);co_return; }
    hintGate.result(false, probe, monotonicTimeMs(), key);finished = true;++hintFailures;auto it = shards.find(key);if (it != shards.end() && it->second.generation == generation) { it->second.adopted = false;it->second.order = ++serial;it->second.next = monotonicTimeMs() + retry(it->second.failures); }if (!probe) failure("hint-scan");
  }
}
kj::Promise<void> AlarmPublication::scanPrevious(std::string key, bool repair, bool probe) {
  const auto generation = shards.at(key).generation;bool finished = false;
  auto release = kj::defer([this, &key, generation, probe, &finished]() {
    if (!finished) markerGate.cancel(probe);--active;--activeScans;
    if (auto it = shards.find(key);it != shards.end() && it->second.generation == generation) {
      it->second.scanning = false;offerScan(it->second, false);offerScan(it->second, true);
    }
    pump();
  });
  auto held = [this, &key, generation]() -> Shard& { auto it = shards.find(key);KJ_REQUIRE(it != shards.end() && it->second.generation == generation && valid(it->second), "alarm publication ownership changed");return it->second; };
  bool listing = false;
  try {
    if (repair) {
      const auto marker = held().marker;
      if (!co_await verifyOwner(key, generation)) co_return;
      auto& s = held();s.repairPending = false;s.repairNext = 0;
      if (s.marker == marker && !marker.empty() && !deleting(marker) && !s.hints.empty()) { s.markerWritten = s.markerMayExist = false;reconcile(s); }
    } else {
      const auto prefix = held().previousPrefix;std::string token;std::set<std::string> previous;
      if (!prefix.empty()) do {
        listing = true;auto page = co_await store.listPage(prefix, 1000, token);listing = false;held();++markerPages;
        for (const auto& row: page.objects) if (row.size == 0 && markerName(row.key.substr(prefix.size()))) previous.insert(row.key);
        token = std::move(page.nextToken);
      } while (!token.empty());
      auto& s = held();s.previousMarkers.insert(previous.begin(), previous.end());s.previousMarkers.erase(s.marker);s.previousDone = true;s.previousPending = false;s.previousNext = 0;cleanup(s);
    }
    markerGate.result(true, probe, monotonicTimeMs(), key);finished = true;
  } catch (const kj::Exception& error) {
    Scope measured(*this);
    if (canceled(error)) { ownershipChanged(key, generation);co_return; }
    markerGate.result(false, probe, monotonicTimeMs(), key);finished = true;
    auto it = shards.find(key);
    if (it != shards.end() && it->second.generation == generation) {
      (repair ? it->second.repairOrder : it->second.previousOrder) = ++serial;
      (repair ? it->second.repairNext : it->second.previousNext) = monotonicTimeMs() + 5000;
      if (listing && accessDenied(error)) denied(Probe::LIST, it->second.previousPrefix);else if (!probe) failure(repair ? "marker-owner-check" : "predecessor-marker-scan");
    }
  }
}
namespace {
struct MarkerAddress { std::string ns, prefix;uint32_t shard;uint64_t member, epoch; };
std::optional<MarkerAddress> markerAddress(const std::string& key, const std::string& root) {
  if (!key.starts_with(root)) return {};
  std::string_view rest(key.data() + root.size(), key.size() - root.size());std::vector<std::string_view> part;
  for (;;) { auto slash = rest.find('/');part.push_back(rest.substr(0, slash));if (slash == rest.npos) break;rest.remove_prefix(slash + 1); }
  if (part.size() != 5 || part[0].size() != 20 || part[1].size() != 64 || part[3].size() != 20 || !markerName(std::string(part[4]))) return {};
  uint64_t member = 0, epoch = 0;uint32_t shard = 0;
  auto decimal = [](auto text, auto& number) { const auto result = std::from_chars(text.data(), text.data() + text.size(), number);return result.ec == std::errc() && result.ptr == text.data() + text.size(); };
  if (!decimal(part[0], member) || !decimal(part[2], shard) || !decimal(part[3], epoch) || !member || !epoch) return {};
  return MarkerAddress{std::string(part[1]), key.substr(0, key.rfind('/') + 1), shard, member, epoch};
}
}
kj::Promise<void> AlarmPublication::scanMarkers(bool probe) {
  const auto epoch = nodeEpoch;const auto started = monotonicTimeMs();const auto root = "alarm-wake/by-owner/" + nodeId + "/";bool finished = false;
  deletedDuringScan.clear();
  auto release = kj::defer([this, probe, &finished]() {
    if (!finished) markerGate.cancel(probe);globalScanRunning = false;deletedDuringScan.clear();--active;--activeScans;pump();
  });
  auto currentEpoch = [this, epoch]() { KJ_REQUIRE(!stopping && nodeEpoch == epoch, "alarm publication ownership changed"); };
  try {
    std::map<std::string, std::set<std::string>> observed;std::string token;
    do {
      auto page = co_await store.listPage(root, 1000, token);currentEpoch();++markerPages;
      for (const auto& row: page.objects) if (row.size == 0) if (auto a = markerAddress(row.key, root)) observed[a->prefix].insert(row.key);
      token = std::move(page.nextToken);
    } while (!token.empty());
    // Streaming applies only listed markers. Empty held shards are never enumerated.
    unsigned slice = 0;
    for (auto& [prefix, keys]: observed) for (auto it = keys.begin();it != keys.end();) {
      const auto marker = *it;
      if (deletedDuringScan.contains(marker) || deleting(marker)) { it = keys.erase(it);continue; }
      auto a = markerAddress(marker, root);KJ_REQUIRE(a.has_value());auto s = shards.find(shardKey(a->ns, a->shard));
      if (s != shards.end() && s->second.epoch == a->epoch && s->second.markerMemberEpoch == a->member) acceptOwnMarker(s->second, marker);
      ++it;
      if (++slice == 32) { slice = 0;co_await kj::evalLater([]() {});currentEpoch(); }
    }
    // No iterator into publisher state survives a yield. Concurrent marker changes are allowed.
    std::string cursor;
    for (;;) {
      auto entry = ownMarkers.upper_bound(cursor);if (entry == ownMarkers.end()) break;
      const auto marker = entry->first;const auto ref = entry->second;cursor = marker;
      auto s = shards.find(ref.shard);
      if (s != shards.end() && s->second.generation == ref.generation && !valid(s->second)) ownershipChanged(ref.shard, ref.generation);
      else if (s != shards.end() && s->second.generation == ref.generation) {
        auto& held = s->second;const auto prefix = marker.substr(0, marker.rfind('/') + 1);const auto found = observed.find(prefix);
        const bool seen = found != observed.end() && found->second.contains(marker) && !deletedDuringScan.contains(marker);
        if (!seen && held.markerWritten && held.markerConfirmedAt < started && !inFlightKeys.contains(marker) && !deleting(marker)) {
          held.repairPending = true;held.repairNext = 0;offerScan(held, true);
        }
      }
      if (++slice == 32) { slice = 0;co_await kj::evalLater([]() {});currentEpoch(); }
    }
    for (const auto& marker: deletedDuringScan) { const auto prefix = marker.substr(0, marker.rfind('/') + 1);if (auto it = observed.find(prefix);it != observed.end()) it->second.erase(marker); }
    observedOwn = std::move(observed);initialMarkerScan = false;nextMarkerScan = monotonicTimeMs() + 60000;
    markerGate.result(true, probe, monotonicTimeMs(), nodeId);finished = true;
  } catch (const kj::Exception& error) {
    Scope measured(*this);if (canceled(error)) { ++ownerChanges;co_return; }
    markerGate.result(false, probe, monotonicTimeMs(), nodeId);finished = true;nextMarkerScan = monotonicTimeMs() + 5000;
    if (accessDenied(error)) denied(Probe::LIST, root);else if (!probe) failure("marker-scan");
  }
}
kj::Promise<void> AlarmPublication::probeMarkers() {
  const auto epoch = nodeEpoch;
  auto release = kj::defer([this]() { permissionProbeActive = false;--active;--activeScans;pump(); });
  try {
    // Prefer a live marker write in pump(). With no applicable row, use one bounded
    // own-prefix check object; a PUT is not called restored until its DELETE succeeds.
    const auto check = "alarm-wake/by-owner/" + nodeId + "/check/" + wide(epoch);
    if (deniedOperation == Probe::LIST) co_await store.listPage(deniedPrefix.empty() ? "alarm-wake/by-owner/" + nodeId + "/" : deniedPrefix, 1);
    else if (deniedOperation == Probe::DELETE) co_await store.remove(check);
    else {
      co_await store.put(check, "");
      try { co_await store.remove(check); }
      catch (const kj::Exception& error) { if (accessDenied(error)) deniedOperation = Probe::DELETE;throw; }
    }
    if (!stopping && nodeEpoch == epoch) markerRestored();
  } catch (const kj::Exception& error) { Scope measured(*this);if (accessDenied(error)) ++markerDenials;else failure("marker-probe"); }
}
unsigned AlarmPublication::drainDeadlines(unsigned limit) {
  Scope measured(*this, &deadlineMetric);const auto now = monotonicTimeMs();
  unsigned done = 0;for (;done < limit && !deadlines.empty() && deadlines.begin()->at <= now;++done) {
    const auto due = *deadlines.begin();deadlines.erase(deadlines.begin());
    if (due.kind == 1) {
      auto it = writes.find(due.key);if (it != writes.end() && it->second.generation == due.generation && it->second.timed && it->second.next == due.at) { it->second.timed = false;refresh(it->second, now); }
    } else {
      auto it = shards.find(due.key);if (it == shards.end() || it->second.generation != due.generation) continue;auto& s = it->second;
      if (!valid(s)) { ownershipChanged(due.key, due.generation);continue; }
      if (due.kind == 0 && s.readTimed && s.next == due.at) { s.readTimed = false;offerRead(s); }
      else if (due.kind == 2 && s.hintTimed && s.hintAt == due.at) { s.hintTimed = false;s.adopted = false;s.next = 0;offerRead(s); }
      else if (due.kind == 3 && s.previousTimed && s.previousNext == due.at) { s.previousTimed = false;offerScan(s, false); }
      else if (due.kind == 4 && s.repairTimed && s.repairNext == due.at) { s.repairTimed = false;offerScan(s, true); }
    }
  }
  pump(8);return done;
}
void AlarmPublication::scheduleDeadlineDrain() {
  if (drainingDeadlines || deadlines.empty() || deadlines.begin()->at > monotonicTimeMs()) return;
  drainingDeadlines = true;tasks.add(kj::evalLater([this]() {
    drainingDeadlines = false;if (!stopping) { drainDeadlines(64);scheduleDeadlineDrain(); }
  }));
}
void AlarmPublication::tick() {
  Scope measured(*this, &tickMetric);const auto dueItems = drainDeadlines(64);unsigned work = 0;
  // Stable list cursors survive unordered_map rehash; no pass over the table.
  for (unsigned n = 0;n < 16 && !writeSweep.empty();++n) {
    if (sweepCursor == writeSweep.end()) sweepCursor = writeSweep.begin();const auto key = *sweepCursor++;++work;
    auto it = writes.find(key);if (it != writes.end() && !valid(it->second) && !inFlightKeys.contains(key)) eraseWrite(it);
  }
  for (unsigned n = 0;n < 16 && !cleanupReady.empty();++n) {
    const auto key = *cleanupReady.begin();auto it = shards.find(key);++work;
    if (it == shards.end()) { cleanupReady.erase(key);continue; }auto& s = it->second;
    if (!valid(s)) { ownershipChanged(key, s.generation);continue; }
    if (!s.adopted || s.reading || !(s.markerWritten || (s.hints.empty() && s.desired.empty())) || s.previousMarkers.empty()) { cleanupReady.erase(key);continue; }
    const auto old = *s.previousMarkers.begin();s.previousMarkers.erase(s.previousMarkers.begin());
    if (old != s.marker) queueMarker(s, old, false, true);
    if (s.previousMarkers.empty()) cleanupReady.erase(key);
  }
  tickMaxItems = std::max<uint64_t>(tickMaxItems, work + dueItems);scheduleDeadlineDrain();pump(8);failure(nullptr);
}
void AlarmPublication::taskFailed(kj::Exception&& error) { if (canceled(error)) ++ownerChanges;else failure("task"); }
std::string AlarmPublication::json() const {
  uint64_t bytes = 0, markerPending = 0, desired = 0;double oldest = 0;
  for (const auto& [key, w]: writes) { bytes += sizeof(Write) + key.capacity() + w.key.capacity() + w.ns.capacity() + w.object.capacity() + w.tenant.capacity();markerPending += w.marker; }
  for (const auto& [key, s]: shards) { if (s.nonempty) oldest = std::max(oldest, monotonicTimeMs() - s.checked);desired += s.desired.size(); }
  std::ostringstream out;
  out << "\"publicationPending\":" << writes.size() << ",\"publicationInFlight\":" << active << ",\"publicationPeakPending\":" << peakPending << ",\"publicationBytes\":" << bytes
      << ",\"publicationRetries\":" << retries << ",\"markerWrites\":" << markerWrites << ",\"markerDeletes\":" << markerDeletes << ",\"adoptionPages\":" << pages << ",\"adoptionMaxPage\":" << maxPage
      << ",\"oldestHintCheckMs\":" << oldest << ",\"hintCheckIntervalMs\":" << hintCheckInterval() << ",\"publicationShards\":" << shards.size() << ",\"publicationDesired\":" << desired
      << ",\"invalidAlarmHints\":" << badHints << ",\"markerAccessDenied\":" << (markerDenied ? "true" : "false") << ",\"markerDenials\":" << markerDenials << ",\"markerProbes\":" << markerProbes << ",\"markerPending\":" << markerPending
      << ",\"publicationMaxConcurrency\":16,\"publicationHintCapacity\":7,\"publicationMarkerWriteCapacity\":8,\"publicationMarkerScanCapacity\":1"
      << ",\"publicationHintPending\":" << writes.size() - markerPending << ",\"publicationOwnerChanges\":" << ownerChanges << ",\"markerScanPages\":" << markerPages
      << ",\"hintPublicationBackoff\":" << (hintGate.open ? "true" : "false") << ",\"markerPublicationBackoff\":" << (markerGate.open ? "true" : "false")
      << ",\"publicationTickMaxItems\":" << tickMaxItems << ",\"publicationDeadlineRows\":" << deadlines.size()
      << ",\"publicationFailureLogLines\":" << failureLogLines << ",\"publicationCpuNs\":" << cpuNs << ",\"publicationThreadCpuNs\":" << threadCpuNs;
  auto metric = [&out](const char* name, const Metric& m) { out << ",\"" << name << "Samples\":" << m.count << ",\"" << name << "P50Us\":" << m.percentile(50) << ",\"" << name << "P99Us\":" << m.percentile(99) << ",\"" << name << "MaxUs\":" << m.maximum / 1000.0; };
  metric("publicationReport", reportMetric);metric("publicationCompletion", completionMetric);metric("publicationTick", tickMetric);metric("publicationDeadlineSlice", deadlineMetric);metric("hintToMarker", markerLatencyMetric);return "\"pendingFileFailures\":" + std::to_string(pendingFileFailures) + "," + out.str();
}
}  // namespace coworkerd

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

namespace coworkerd {
namespace {
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
AlarmPublication::Scope::Scope(AlarmPublication& owner, Metric* metric): owner(owner), metric(metric), started(nanoTime()) {
  if (!owner.cpuDepth++) owner.cpuStarted = started;
}
AlarmPublication::Scope::~Scope() {
  const auto now = nanoTime();if (metric) metric->add(now - started);if (!--owner.cpuDepth) owner.cpuNs += now - owner.cpuStarted;
}

// Fixed histograms and nested scopes measure running publisher code, never suspended I/O.
// Metric storage is declared in the header so component tests can inspect the same production counters.
AlarmPublication::AlarmPublication(ObjectStore& store, kj::Timer& timer, std::string nodeId, Current current, Adopt adopt, Log log)
    : store(store), timer(timer), nodeId(std::move(nodeId)), current(std::move(current)), adopt(std::move(adopt)), log(std::move(log)), tasks(*this) {
  KJ_REQUIRE(RAND_bytes(reinterpret_cast<unsigned char*>(&retryRandom), sizeof(retryRandom)) == 1);
  if (!retryRandom) retryRandom = 0x9e3779b97f4a7c15ull;
  if (const auto* value = std::getenv("COWORKERD_ALARM_HINT_CHECK_MS_FOR_TEST")) {
    char* end = nullptr;const auto n = std::strtoull(value, &end, 10);
    if (end && !*end && n >= 200 && n <= 3600000) hintRoundMs = n;
  }
}
AlarmPublication::~AlarmPublication() noexcept(false) { stopping = true; }
std::string AlarmPublication::shardKey(const std::string& ns, uint32_t shard) const { return ns + "/" + std::to_string(shard); }
bool AlarmPublication::valid(const Shard& s) const { return !stopping && nodeEpoch && s.memberEpoch == nodeEpoch && current(s.ns, s.shard, s.epoch, s.memberEpoch); }
bool AlarmPublication::valid(const Write& w) const {
  auto s = shards.find(shardKey(w.ns, w.shard));return s != shards.end() && s->second.generation == w.generation && valid(s->second);
}
double AlarmPublication::hintCheckInterval() const { return hintRoundMs / std::max<uint64_t>(1, nonemptyShards); }
void AlarmPublication::setNodeEpoch(uint64_t epoch) {
  if (epoch == nodeEpoch) return;nodeEpoch = epoch;shards.clear();writes.clear();readyHints.clear();readyMarkers.clear();nonemptyShards = 0;nextHintCheck = 0;
}
void AlarmPublication::take(std::string ns, uint32_t shard, uint64_t epoch, std::string tenant) {
  if (!nodeEpoch || !epoch) return;const auto key = shardKey(ns, shard);auto it = shards.find(key);
  if (it != shards.end() && it->second.epoch == epoch && it->second.memberEpoch == nodeEpoch) {
    auto& s = it->second;s.startupOnly = false;if (!s.reading) { s.adopted = false;s.next = 0; }offerRead(s);pump();return;
  }
  gone(ns, shard);Shard s;s.ns = std::move(ns);s.shard = shard;s.epoch = epoch;s.tenant = std::move(tenant);
  s.memberEpoch = s.markerMemberEpoch = nodeEpoch;s.generation = s.order = ++serial;s.checked = monotonicTimeMs();
  auto [held, inserted] = shards.emplace(key, std::move(s));offerRead(held->second);pump();
}
void AlarmPublication::restore(std::string ns, uint32_t shard, std::string tenant) {
  const auto key = shardKey(ns, shard);if (!nodeEpoch || shards.contains(key)) return;
  Shard s;s.ns = std::move(ns);s.shard = shard;s.tenant = std::move(tenant);s.startupOnly = true;
  s.memberEpoch = nodeEpoch;s.generation = s.order = ++serial;s.checked = monotonicTimeMs();
  auto [held, inserted] = shards.emplace(key, std::move(s));offerRead(held->second);pump();
}
void AlarmPublication::gone(const std::string& ns, uint32_t shard) {
  auto it = shards.find(shardKey(ns, shard));if (it == shards.end()) return;auto& s = it->second;
  readyHints.erase({s.tenant, s.order, it->first, 0});readyMarkers.erase({s.tenant, s.markerOrder, it->first, 2});
  if (s.nonempty) --nonemptyShards;shards.erase(it);
  // Retire stale write rows in tick. In-flight keys remain reserved independently of these rows.
}
void AlarmPublication::unqueue(Write& w) { (w.marker ? readyMarkers : readyHints).erase({w.tenant, w.order, w.key, w.marker ? 3u : 1u}); }
void AlarmPublication::refresh(Write& w, double now) {
  unqueue(w);if (!valid(w) || w.next > now || inFlightKeys.contains(w.key)) return;
  const auto& s = shards.at(shardKey(w.ns, w.shard));if (!s.adopted || s.reading || (w.marker && markerDenied)) return;
  (w.marker ? readyMarkers : readyHints).insert({w.tenant, w.order, w.key, w.marker ? 3u : 1u});
}
void AlarmPublication::offerRead(Shard& s) {
  const auto key = shardKey(s.ns, s.shard);readyHints.erase({s.tenant, s.order, key, 0});
  if (valid(s) && !s.adopted && !s.reading && s.next <= monotonicTimeMs() && !inFlightHints.contains(key)) readyHints.insert({s.tenant, s.order, key, 0});
}
void AlarmPublication::queueHint(Shard& s, const std::string& object, uint64_t at) {
  const auto key = hintPrefix(s.ns, s.shard) + object + "/0";auto it = writes.find(key);
  if (it != writes.end() && it->second.generation == s.generation && it->second.at == at) return;
  auto& w = writes[key];unqueue(w);w.key = key;w.ns = s.ns;w.object = object;w.tenant = s.tenant;
  w.shard = s.shard;w.generation = s.generation;w.at = at;w.marker = false;w.version = ++serial;
  w.order = w.order ? w.order : serial;w.next = 0;w.failures = 0;peakPending = std::max<uint64_t>(peakPending, writes.size());refresh(w, monotonicTimeMs());
}
void AlarmPublication::queueMarker(Shard& s, const std::string& key, bool present, bool mayExist) {
  if (markerDenied && present) return;auto it = writes.find(key);
  if (it != writes.end() && it->second.generation == s.generation && it->second.at == uint64_t(present)) return;
  auto& w = writes[key];unqueue(w);w.key = key;w.ns = s.ns;w.tenant = s.tenant;w.shard = s.shard;
  w.generation = s.generation;w.marker = true;w.at = present;w.mayExist = w.mayExist || mayExist;
  w.version = ++serial;w.order = w.order ? w.order : serial;w.next = 0;w.failures = 0;
  peakPending = std::max<uint64_t>(peakPending, writes.size());refresh(w, monotonicTimeMs());
}
void AlarmPublication::report(std::string ns, std::string object, uint32_t shard, uint64_t epoch, std::string tenant, uint64_t at) {
  Scope measured(*this, &reportMetric);const auto key = shardKey(ns, shard);
  if (!shards.contains(key)) take(ns, shard, epoch, std::move(tenant));
  auto it = shards.find(key);if (it == shards.end() || it->second.epoch != epoch || !valid(it->second)) return;
  auto& s = it->second;if (at) s.desired[object] = at;else s.desired.erase(object);
  if (!s.adopted || s.reading) s.duringRead[object] = at;queueHint(s, object, at);reconcile(s);pump();
}
void AlarmPublication::reconcile(Shard& s) {
  const bool nonempty = !s.hints.empty() || !s.desired.empty() || !s.marker.empty();
  if (nonempty != s.nonempty) { if (nonempty) ++nonemptyShards;else --nonemptyShards;s.nonempty = nonempty; }
  const auto next = monotonicTimeMs() + hintCheckInterval();nextHintCheck = nextHintCheck ? std::min(nextHintCheck, next) : next;
  if (!valid(s) || !s.adopted || s.reading || !s.markerGeneration || s.markerGeneration != markerGeneration) return;
  if (!s.hints.empty()) {
    if (s.marker.empty()) { s.marker = markerPrefix(nodeId, s.markerMemberEpoch, s.ns, s.shard, s.epoch) + presence();s.markerWritten = s.markerMayExist = false; }
    if (!s.markerWritten) queueMarker(s, s.marker, true, s.markerMayExist);
  } else if (s.desired.empty() && !s.marker.empty()) {
    const auto old = s.marker;auto w = writes.find(old);
    const bool mayExist = s.markerWritten || s.markerMayExist || (w != writes.end() && w->second.mayExist);
    if (mayExist || inFlightKeys.contains(old)) queueMarker(s, old, false, mayExist);
    else if (w != writes.end()) { unqueue(w->second);writes.erase(w); }
    s.marker.clear();s.markerWritten = s.markerMayExist = false;
    if (s.nonempty) { --nonemptyShards;s.nonempty = false; }
  }
}
uint32_t AlarmPublication::retry(uint32_t& failures) {
  failures = std::min<uint32_t>(failures + 1, 32);++retries;
  const auto base = std::min<uint32_t>(5000, 250u << std::min<uint32_t>(failures - 1, 5));
  // Retry jitter is not a token: seed once, then avoid a crypto call on each failed completion.
  retryRandom ^= retryRandom << 13;retryRandom ^= retryRandom >> 7;retryRandom ^= retryRandom << 17;
  return std::min<uint32_t>(5000, base + retryRandom % std::max<uint32_t>(1, base / 4));
}
void AlarmPublication::failure(const char* operation, bool denied) {
  if (denied) {
    ++markerDenials;if (markerDenied) return;
    markerDenied = true;nextMarkerProbe = monotonicTimeMs() + 30000;
  }
  if (operation) ++failuresToLog;
  if (!failuresToLog || monotonicTimeMs() < nextFailureLog) return;
  const auto count = failuresToLog;failuresToLog = 0;nextFailureLog = monotonicTimeMs() + 1000;++failureLogLines;
  log("alarm-publication-failures", "\"count\":" + std::to_string(count) + ",\"operation\":" + json::quote(operation ? operation : "merged") + ",\"markerAccessDenied\":" + (markerDenied ? "true" : "false"));
}
void AlarmPublication::pump(unsigned limit) {
  Scope measured(*this);if (stopping) return;
  for (unsigned attempt = 0; attempt < limit && active < 8; ++attempt) {
    auto* queue = activeHints < 7 && !readyHints.empty() ? &readyHints : !markerDenied && !activeMarkers && !readyMarkers.empty() ? &readyMarkers : nullptr;
    if (!queue) return;
    auto choice = queue->upper_bound({lastTenant, UINT64_MAX, "", 4});if (choice == queue->end()) choice = queue->begin();
    auto work = *choice;queue->erase(choice);lastTenant = work.tenant;
    if (work.kind == 0 || work.kind == 2) {
      auto it = shards.find(work.key);if (it == shards.end() || !valid(it->second)) continue;auto& s = it->second;
      if (work.kind == 0) {
        if (s.reading || s.adopted || inFlightHints.contains(work.key)) continue;s.reading = true;++active;++activeHints;
        const auto generation = s.generation;
        tasks.add(kj::evalLater([this, key = work.key, generation]() -> kj::Promise<void> {
          auto it = shards.find(key);
          if (it == shards.end() || it->second.generation != generation) { --active;--activeHints;pump();return kj::READY_NOW; }
          return readHints(key);
        }));
      } else {
        if (s.markerReading || !s.adopted || s.reading) continue;s.markerReading = true;++active;++activeMarkers;
        const auto generation = s.generation;
        tasks.add(kj::evalLater([this, key = work.key, generation]() -> kj::Promise<void> {
          auto it = shards.find(key);
          if (it == shards.end() || it->second.generation != generation) { --active;--activeMarkers;pump();return kj::READY_NOW; }
          return scanMarkers(key);
        }));
      }
    } else {
      auto it = writes.find(work.key);if (it == writes.end() || !valid(it->second) || inFlightKeys.contains(work.key)) continue;
      const auto& s = shards.at(shardKey(it->second.ns, it->second.shard));if (!s.adopted || s.reading) continue;
      inFlightKeys.insert(work.key);++active;if (it->second.marker) ++activeMarkers;else { ++activeHints;++inFlightHints[shardKey(it->second.ns, it->second.shard)]; }
      tasks.add(kj::evalLater([this, sent = it->second]() mutable { return write(std::move(sent)); }));
    }
  }
}
kj::Promise<void> AlarmPublication::write(Write sent) {
  const auto key = sent.key;
  if (auto it = writes.find(key);it != writes.end() && it->second.generation == sent.generation) sent = it->second;
  bool success = false, denied = false;
  try {
    KJ_REQUIRE(valid(sent), "alarm publication owner changed");
    kj::Promise<ObjectResponse> operation = nullptr;
    {
      Scope measured(*this);
      if (sent.marker && sent.at) { writes.at(key).mayExist = true;auto& s = shards.at(shardKey(sent.ns, sent.shard));if (s.marker == key) s.markerMayExist = true; }
      if (!sent.at) operation = store.remove(key);
      else if (sent.marker) operation = store.put(key, "");
      else operation = store.put(key, "{\"atUnixMs\":" + std::to_string(sent.at) + ",\"facetId\":0,\"nodeId\":" + json::quote(nodeId) + ",\"writtenAtUnixMs\":" + std::to_string(unixMs()) + "}\n");
    }
    co_await operation;success = true;
  } catch (const kj::Exception& error) { denied = sent.marker && accessDenied(error); }
  Scope measured(*this, &completionMetric);
  auto release = kj::defer([this, &key, &sent]() {
    inFlightKeys.erase(key);--active;
    if (sent.marker) --activeMarkers;
    else { --activeHints;auto it = inFlightHints.find(shardKey(sent.ns, sent.shard));if (it != inFlightHints.end() && !--it->second) inFlightHints.erase(it); }
    if (auto it = writes.find(key);it != writes.end()) refresh(it->second, monotonicTimeMs());pump();
  });
  if (!success) { if (!sent.marker) ++hintFailures;failure(sent.marker ? "marker-write" : "hint-write", denied); }
  else if (sent.marker) { if (sent.at) ++markerWrites;else ++markerDeletes; } else ++hintWrites;
  auto found = writes.find(key);if (found == writes.end() || found->second.generation != sent.generation || !valid(sent)) co_return;
  auto& w = found->second;auto& s = shards.at(shardKey(sent.ns, sent.shard));
  if (sent.marker && denied && sent.at) { w.mayExist = sent.mayExist;if (s.marker == key) s.markerMayExist = sent.mayExist; }
  if (success) {
    if (sent.marker) { w.mayExist = sent.at;if (s.marker == key && sent.at) s.markerWritten = s.markerMayExist = true; }
    else { if (sent.at) s.hints[sent.object] = sent.at;else s.hints.erase(sent.object); }
    if (w.version == sent.version) { unqueue(w);writes.erase(found); }
    else { w.next = 0;w.failures = 0; }
  } else if (denied && !w.mayExist) { unqueue(w);writes.erase(found); }
  else if (w.version == sent.version) w.next = monotonicTimeMs() + retry(w.failures);
  else w.next = 0;
  reconcile(s);
}
kj::Promise<bool> AlarmPublication::verifyOwner(std::string key, uint64_t generation) {
  auto held = [this, &key, generation]() -> Shard& {
    auto it = shards.find(key);KJ_REQUIRE(it != shards.end() && it->second.generation == generation && valid(it->second), "alarm owner changed during verification");return it->second;
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
  if (latest.empty()) { gone(ns, shard);co_return false; }
  auto response = co_await store.get(latest);held();requireStorage(response.status == 200, "alarm owner read failed: HTTP " + std::to_string(response.status));
  auto owner = parseShardEpoch(response.bytes);requireStorage(owner.has_value() && owner->namespaceId == ns && owner->shard == shard, "invalid alarm owner record");
  if (owner->nodeId != nodeId || (held().epoch && (owner->epoch != held().epoch || owner->nodeEpoch != held().markerMemberEpoch))) { gone(ns, shard);co_return false; }
  auto released = co_await store.get(shardReleasedKey(ns, shard, owner->epoch));held();requireStorage(released.status == 200 || released.status == 404, "alarm release read failed: HTTP " + std::to_string(released.status));
  if (released.status == 200) { gone(ns, shard);co_return false; }
  auto& s = held();s.epoch = owner->epoch;s.markerMemberEpoch = owner->nodeEpoch;
  if (owner->previousEpoch) s.previousPrefix = markerPrefix(owner->previousNodeId, owner->previousNodeEpoch, ns, shard, owner->previousEpoch);
  co_return true;
}
kj::Promise<void> AlarmPublication::readHints(std::string key) {
  const auto generation = shards.at(key).generation;
  auto release = kj::defer([this, &key, generation]() {
    --active;--activeHints;if (auto it = shards.find(key);it != shards.end() && it->second.generation == generation) it->second.reading = false;pump();
  });
  auto held = [this, &key, generation]() -> Shard& { auto it = shards.find(key);KJ_REQUIRE(it != shards.end() && it->second.generation == generation && valid(it->second), "alarm adoption owner changed");return it->second; };
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
    for (const auto& [object, at]: s.desired) if (!s.hints.contains(object) || s.hints.at(object) != at) queueHint(s, object, at);
    reconcile(s);s.markerNext = 0;
  } catch (const kj::Exception&) {
    Scope measured(*this);auto it = shards.find(key);if (it != shards.end() && it->second.generation == generation) { it->second.adopted = false;it->second.next = monotonicTimeMs() + retry(it->second.failures); }failure("hint-scan");
  }
}
kj::Promise<void> AlarmPublication::scanMarkers(std::string key) {
  const auto generation = shards.at(key).generation;
  auto release = kj::defer([this, &key, generation]() {
    --active;--activeMarkers;if (auto it = shards.find(key);it != shards.end() && it->second.generation == generation) it->second.markerReading = false;pump();
  });
  auto held = [this, &key, generation]() -> Shard& { auto it = shards.find(key);KJ_REQUIRE(it != shards.end() && it->second.generation == generation && valid(it->second), "alarm marker owner changed");return it->second; };
  bool markerRequest = false;
  try {
    if (!co_await verifyOwner(key, generation)) co_return;
    const auto prefix = markerPrefix(nodeId, held().markerMemberEpoch, held().ns, held().shard, held().epoch);
    std::set<std::string> own, previous;std::string token;
    for (const auto& scan: {prefix, held().previousPrefix}) {
      if (scan.empty()) continue;token.clear();
      do {
        markerRequest = true;auto page = co_await store.listPage(scan, 1000, token);markerRequest = false;held();
        for (const auto& row: page.objects) if (row.size == 0 && markerName(row.key.substr(scan.size()))) (scan == prefix ? own : previous).insert(row.key);
        token = std::move(page.nextToken);
      } while (!token.empty());
    }
    Scope measured(*this);auto& s = held();s.previousMarkers.insert(previous.begin(), previous.end());
    if (!s.marker.empty() && !own.contains(s.marker)) { s.markerWritten = s.markerMayExist = false; }
    if (s.marker.empty() && !s.hints.empty() && !own.empty()) { s.marker = *own.begin();s.markerWritten = s.markerMayExist = true; }
    for (const auto& old: own) if (old != s.marker) s.previousMarkers.insert(old);
    s.markerNext = monotonicTimeMs() + 60000;s.markerGeneration = markerGeneration;reconcile(s);
  } catch (const kj::Exception& error) {
    Scope measured(*this);auto it = shards.find(key);if (it != shards.end() && it->second.generation == generation) it->second.markerNext = monotonicTimeMs() + 5000;failure("marker-scan", markerRequest && accessDenied(error));
  }
}
kj::Promise<void> AlarmPublication::probeMarkers() {
  auto release = kj::defer([this]() { --active;--activeMarkers;pump(); });++markerProbes;
  try {
    co_await store.listPage("alarm-wake/by-owner/" + nodeId + "/", 1);Scope measured(*this);markerDenied = false;++markerGeneration;
    log("alarm-marker-access-restored", "\"denials\":" + std::to_string(markerDenials));
  }
  catch (const kj::Exception& error) { Scope measured(*this);failure("marker-probe", accessDenied(error)); }
  nextMarkerProbe = monotonicTimeMs() + 30000;
}
void AlarmPublication::tick() {
  Scope measured(*this, &tickMetric);const auto now = monotonicTimeMs();readyHints.clear();readyMarkers.clear();
  for (auto it = shards.begin(); it != shards.end();) {
    if (!valid(it->second)) { const auto ns = it->second.ns;const auto shard = it->second.shard;++it;gone(ns, shard); } else ++it;
  }
  if (now >= nextHintCheck) {
    Shard* oldest = nullptr;for (auto& [key, s]: shards) if (s.adopted && !s.reading && s.nonempty && (!oldest || s.checked < oldest->checked)) oldest = &s;
    if (oldest) { oldest->adopted = false;oldest->next = 0;oldest->order = ++serial; }
    nextHintCheck = now + std::max(200.0, hintCheckInterval());
  }
  for (auto& [key, s]: shards) {
    offerRead(s);reconcile(s);
    if (!markerDenied && s.adopted && !s.reading && !s.markerReading && (now >= s.markerNext || s.markerGeneration != markerGeneration)) {
      if (!s.markerOrder) s.markerOrder = ++serial;readyMarkers.insert({s.tenant, s.markerOrder, key, 2});
    }
    if (!markerDenied && s.adopted && !s.reading && s.markerGeneration == markerGeneration && (s.markerWritten || (s.hints.empty() && s.desired.empty()))) {
      for (const auto& old: s.previousMarkers) queueMarker(s, old, false, true);s.previousMarkers.clear();
    }
  }
  for (auto it = writes.begin(); it != writes.end();) {
    if (!valid(it->second) && !inFlightKeys.contains(it->first)) { it = writes.erase(it);continue; }refresh(it->second, now);++it;
  }
  if (markerDenied && !activeMarkers && now >= nextMarkerProbe) { ++active;++activeMarkers;tasks.add(probeMarkers()); }
  pump(8);failure(nullptr);
}
void AlarmPublication::taskFailed(kj::Exception&&) { failure("task"); }
std::string AlarmPublication::json() const {
  uint64_t bytes = 0, markerPending = 0;double oldest = 0;
  for (const auto& [key, w]: writes) { bytes += sizeof(Write) + key.capacity() + w.key.capacity() + w.ns.capacity() + w.object.capacity() + w.tenant.capacity();markerPending += w.marker; }
  for (const auto& [key, s]: shards) if (s.nonempty) oldest = std::max(oldest, monotonicTimeMs() - s.checked);
  std::ostringstream out;
  out << "\"publicationPending\":" << writes.size() << ",\"publicationInFlight\":" << active << ",\"publicationPeakPending\":" << peakPending << ",\"publicationBytes\":" << bytes
      << ",\"publicationRetries\":" << retries << ",\"markerWrites\":" << markerWrites << ",\"markerDeletes\":" << markerDeletes << ",\"adoptionPages\":" << pages << ",\"adoptionMaxPage\":" << maxPage
      << ",\"oldestHintCheckMs\":" << oldest << ",\"hintCheckIntervalMs\":" << hintCheckInterval() << ",\"publicationShards\":" << shards.size() << ",\"publicationDesired\":";
  uint64_t desired = 0;for (const auto& [key, s]: shards) desired += s.desired.size();out << desired;
  out << ",\"invalidAlarmHints\":" << badHints << ",\"markerAccessDenied\":" << (markerDenied ? "true" : "false") << ",\"markerDenials\":" << markerDenials << ",\"markerProbes\":" << markerProbes << ",\"markerPending\":" << markerPending
      << ",\"publicationFailureLogLines\":" << failureLogLines << ",\"publicationCpuNs\":" << cpuNs;
  auto metric = [&out](const char* name, const Metric& m) { out << ",\"" << name << "Samples\":" << m.count << ",\"" << name << "P50Us\":" << m.percentile(50) << ",\"" << name << "P99Us\":" << m.percentile(99) << ",\"" << name << "MaxUs\":" << m.maximum / 1000.0; };
  metric("publicationReport", reportMetric);metric("publicationCompletion", completionMetric);metric("publicationTick", tickMetric);return out.str();
}
}  // namespace coworkerd

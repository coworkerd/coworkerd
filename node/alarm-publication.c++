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
#include <limits>
#include <sstream>

namespace coworkerd {
namespace {
std::string wide(uint64_t n) {
  auto s = std::to_string(n);return s.size() >= 20 ? s : std::string(20 - s.size(), '0') + s;
}
std::string markerPrefix(const std::string& node, uint64_t member, const std::string& ns,
    uint32_t shard, uint64_t epoch) {
  return "alarm-wake/by-owner/" + node + "/" + wide(member) + "/" + ns + "/" +
      std::to_string(shard) + "/" + wide(epoch) + "/";
}
std::string hintPrefix(const std::string& ns, uint32_t shard) {
  return "do/" + ns + "/shards/" + std::to_string(shard) + "/alarms/";
}
std::string presence() {
  unsigned char bytes[16];KJ_REQUIRE(RAND_bytes(bytes, sizeof(bytes)) == 1, "cannot draw a presence identifier");
  static constexpr char hex[] = "0123456789abcdef";std::string s;
  for (auto b: bytes) { s += hex[b >> 4];s += hex[b & 15]; }return s;
}
uint64_t unixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
bool markerName(const std::string& value) {
  return value.size() == 32 && std::all_of(value.begin(), value.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}
}

AlarmPublication::AlarmPublication(ObjectStore& store, kj::Timer& timer, std::string nodeId,
    Current current, Adopt adopt, Log log)
    : store(store), timer(timer), nodeId(std::move(nodeId)), current(std::move(current)),
      adopt(std::move(adopt)), log(std::move(log)), tasks(*this) {}
AlarmPublication::~AlarmPublication() noexcept(false) { stopping = true; }

std::string AlarmPublication::shardKey(const std::string& ns, uint32_t shard) const {
  return ns + "/" + std::to_string(shard);
}
bool AlarmPublication::valid(const Shard& s) const {
  return !stopping && nodeEpoch && s.memberEpoch == nodeEpoch && current(s.ns, s.shard, s.epoch, s.memberEpoch);
}
bool AlarmPublication::valid(const Write& w) const {
  auto s = shards.find(shardKey(w.ns, w.shard));
  return s != shards.end() && s->second.epoch == w.epoch && s->second.memberEpoch == w.memberEpoch && valid(s->second);
}
void AlarmPublication::setNodeEpoch(uint64_t epoch) {
  if (epoch == nodeEpoch) return;
  nodeEpoch = epoch;shards.clear();writes.clear();nextHintCheck = nextMarkerCheck = 0;
}
void AlarmPublication::take(std::string ns, uint32_t shard, uint64_t epoch, std::string tenant) {
  if (!nodeEpoch || !epoch) return;
  const auto key = shardKey(ns, shard);auto found = shards.find(key);
  if (found != shards.end() && found->second.epoch == epoch && found->second.memberEpoch == nodeEpoch) {
    found->second.startupOnly = false;
    if (!found->second.reading) { found->second.adopted = false;found->second.next = 0;found->second.order = ++serial; }
    pump();return;
  }
  gone(ns, shard);
  Shard s;s.ns = std::move(ns);s.shard = shard;s.epoch = epoch;s.tenant = std::move(tenant);
  s.memberEpoch = s.markerMemberEpoch = nodeEpoch;s.generation = s.order = ++serial;
  s.checked = monotonicTimeMs();shards.emplace(key, std::move(s));
  const auto next = monotonicTimeMs() + 3600000.0 / std::max<size_t>(1, shards.size());
  nextHintCheck = nextHintCheck ? std::min(nextHintCheck, next) : next;pump();
}
void AlarmPublication::restore(std::string ns, uint32_t shard, std::string tenant) {
  if (!nodeEpoch || shards.contains(shardKey(ns, shard))) return;
  Shard s;s.ns = std::move(ns);s.shard = shard;s.tenant = std::move(tenant);
  s.startupOnly = true;
  s.memberEpoch = s.markerMemberEpoch = nodeEpoch;s.generation = s.order = ++serial;s.checked = monotonicTimeMs();
  auto key = shardKey(s.ns, s.shard);shards.emplace(std::move(key), std::move(s));
  const auto next = monotonicTimeMs() + 3600000.0 / shards.size();
  nextHintCheck = nextHintCheck ? std::min(nextHintCheck, next) : next;pump();
}
void AlarmPublication::gone(const std::string& ns, uint32_t shard) {
  shards.erase(shardKey(ns, shard));
  for (auto it = writes.begin(); it != writes.end();)
    if (it->second.ns == ns && it->second.shard == shard) it = writes.erase(it);else ++it;
}
void AlarmPublication::queueHint(Shard& s, const std::string& object, uint64_t at) {
  const auto key = hintPrefix(s.ns, s.shard) + object + "/0";
  auto it = writes.find(key);
  if (it != writes.end() && it->second.epoch == s.epoch && it->second.memberEpoch == s.memberEpoch && it->second.at == at) return;
  auto& w = writes[key];w.key = key;w.ns = s.ns;w.object = object;w.tenant = s.tenant;
  w.shard = s.shard;w.epoch = s.epoch;w.memberEpoch = s.memberEpoch;w.at = at;
  w.version = ++serial;w.order = w.order ? w.order : serial;w.next = 0;w.failures = 0;
  peakPending = std::max<uint64_t>(peakPending, writes.size());
}
void AlarmPublication::queueMarker(Shard& s, const std::string& key, bool present) {
  auto it = writes.find(key);
  if (it != writes.end() && it->second.at == uint64_t(present)) return;
  auto& w = writes[key];w.key = key;w.ns = s.ns;w.tenant = s.tenant;w.shard = s.shard;
  w.epoch = s.epoch;w.memberEpoch = s.memberEpoch;w.marker = true;w.at = present;
  w.version = ++serial;w.order = w.order ? w.order : serial;w.next = 0;w.failures = 0;
  peakPending = std::max<uint64_t>(peakPending, writes.size());
}
void AlarmPublication::report(std::string ns, std::string object, uint32_t shard, uint64_t epoch,
    std::string tenant, uint64_t at) {
  auto key = shardKey(ns, shard);
  if (!shards.contains(key)) take(ns, shard, epoch, std::move(tenant));
  auto it = shards.find(key);if (it == shards.end() || it->second.epoch != epoch || !valid(it->second)) return;
  auto& s = it->second;
  if (at) s.desired[object] = at;else s.desired.erase(object);
  if (!s.adopted || s.reading) s.duringRead[object] = at;
  queueHint(s, object, at);reconcile(s);pump();
}
void AlarmPublication::reconcile(Shard& s) {
  if (!valid(s) || !s.adopted || s.reading) return;
  if (!s.hints.empty()) {
    if (s.marker.empty()) {
      s.marker = markerPrefix(nodeId, s.markerMemberEpoch, s.ns, s.shard, s.epoch) + presence();
      s.markerWritten = false;queueMarker(s, s.marker, true);
    }
    if (!s.markerWritten) { queueMarker(s, s.marker, true);return; }
  } else {
    if (!s.desired.empty()) return;
    if (!s.marker.empty()) {
      queueMarker(s, s.marker, false);s.marker.clear();s.markerWritten = false;
    }
  }
  // Complete adoption and either a published replacement or a proved-empty set precede old-marker deletion.
  for (const auto& key: s.previousMarkers) queueMarker(s, key, false);
  s.previousMarkers.clear();
}
uint32_t AlarmPublication::retry(uint32_t& failures) {
  failures = std::min<uint32_t>(failures + 1, 32);++retries;
  const auto base = std::min<uint32_t>(5000, 250u << std::min<uint32_t>(failures - 1, 5));
  uint32_t random = 0;KJ_REQUIRE(RAND_bytes(reinterpret_cast<unsigned char*>(&random), sizeof(random)) == 1);
  return std::min<uint32_t>(5000, base + random % std::max<uint32_t>(1, base / 4));
}

void AlarmPublication::pump() {
  if (stopping) return;
  const auto now = monotonicTimeMs();
  while (active < 8) {
    struct Work { std::string key;uint64_t order;bool read; };
    std::map<std::string, Work> ready;
    auto offer = [&ready](const std::string& tenant, Work work) {
      auto found = ready.find(tenant);
      if (found == ready.end() || work.order < found->second.order) ready.insert_or_assign(tenant, std::move(work));
    };
    for (const auto& [key, s]: shards) {
      if (s.adopted || s.reading || s.next > now || !valid(s)) continue;
      const bool writing = std::any_of(writes.begin(), writes.end(), [&s](const auto& row) {
        return !row.second.marker && row.second.inFlight && row.second.ns == s.ns && row.second.shard == s.shard;
      });
      if (!writing) offer(s.tenant, {key, s.order, true});
    }
    for (const auto& [key, w]: writes) {
      if (w.inFlight || w.next > now || !valid(w)) continue;
      const auto& s = shards.at(shardKey(w.ns, w.shard));
      if (!s.adopted || s.reading) continue;
      offer(w.tenant, {key, w.order, false});
    }
    if (ready.empty()) break;
    auto choice = ready.upper_bound(lastTenant);if (choice == ready.end()) choice = ready.begin();
    lastTenant = choice->first;const auto work = choice->second;++active;
    if (work.read) {
      auto& s = shards.at(work.key);s.reading = true;s.order = ++serial;
      tasks.add(readHints(work.key));
    } else {
      auto& w = writes.at(work.key);w.inFlight = true;w.order = ++serial;
      tasks.add(write(work.key));
    }
  }
}
kj::Promise<void> AlarmPublication::write(std::string key) {
  const auto sent = writes.at(key);
  auto release = kj::defer([this]() { --active;pump(); });
  bool success = false;
  try {
    KJ_REQUIRE(valid(sent), "the alarm publication owner changed");
    if (!sent.at) co_await store.remove(sent.key);
    else if (sent.marker) co_await store.put(sent.key, "");
    else co_await store.put(sent.key, "{\"atUnixMs\":" + std::to_string(sent.at) +
        ",\"facetId\":0,\"nodeId\":" + json::quote(nodeId) + ",\"writtenAtUnixMs\":" + std::to_string(unixMs()) + "}\n");
    success = true;
    if (sent.marker) { if (sent.at) ++markerWrites;else ++markerDeletes; } else ++hintWrites;
  } catch (const kj::Exception& error) {
    if (!sent.marker) ++hintFailures;
    log("alarm-publication-failed", "\"key\":" + json::quote(sent.key) + ",\"error\":" +
        json::quote(std::string(error.getDescription().cStr())));
  }
  if (!valid(sent)) {
    auto it = writes.find(key);
    if (it != writes.end() && it->second.version == sent.version) writes.erase(it);
    co_return;
  }
  auto found = writes.find(key);
  if (found == writes.end()) co_return;
  // A dropped owner can have an old answer arrive after a new row under the same key.
  if (found->second.memberEpoch != sent.memberEpoch || found->second.epoch != sent.epoch) co_return;
  found->second.inFlight = false;
  auto& s = shards.at(shardKey(sent.ns, sent.shard));
  if (success) {
    if (sent.marker) {
      if (s.marker == sent.key && sent.at) s.markerWritten = true;
    } else {
      if (sent.at) s.hints[sent.object] = sent.at;else s.hints.erase(sent.object);
    }
    if (found->second.version == sent.version) writes.erase(found);
    else { found->second.next = 0;found->second.failures = 0; }
  } else if (found->second.version == sent.version) {
    found->second.next = monotonicTimeMs() + retry(found->second.failures);
  } else found->second.next = 0;
  reconcile(s);
}

kj::Promise<void> AlarmPublication::readHints(std::string key) {
  const auto generation = shards.at(key).generation;
  const auto ns = shards.at(key).ns;const auto shard = shards.at(key).shard;
  auto release = kj::defer([this]() { --active;pump(); });
  auto held = [this, &key, generation]() -> Shard& {
    auto it = shards.find(key);
    KJ_REQUIRE(it != shards.end() && it->second.generation == generation && valid(it->second), "the alarm adoption owner changed");
    return it->second;
  };
  try {
    if (!held().epoch) {
      // Startup uses only locally known shards and reads existing ownership. It never creates an epoch.
      std::string token, latest;
      do {
        auto page = co_await store.listPage(shardEpochsPrefix(ns, shard) + "/", 1000, token);held();
        for (const auto& row: page.objects) {
          const auto part = row.key.substr(row.key.rfind('/') + 1);
          if (part.size() == 20 && std::all_of(part.begin(), part.end(), [](char c) { return c >= '0' && c <= '9'; })) latest = row.key;
        }
        token = std::move(page.nextToken);
      } while (!token.empty());
      if (latest.empty()) { gone(ns, shard);co_return; }
      auto response = co_await store.get(latest);held();
      requireStorage(response.status == 200, "startup alarm owner read failed");
      auto owner = parseShardEpoch(response.bytes);
      requireStorage(owner.has_value(), "startup alarm owner record is invalid");
      requireStorage(owner->namespaceId == ns && owner->shard == shard, "startup alarm owner names another shard");
      if (owner->nodeId != nodeId) { gone(ns, shard);co_return; }
      auto& s = held();s.epoch = owner->epoch;s.markerMemberEpoch = owner->nodeEpoch;
    }
    const auto epoch = held().epoch;
    std::map<std::string, uint64_t> observed;
    std::string token;
    do {
      auto page = co_await store.listPage(hintPrefix(ns, shard), 1000, token);held();
      ++pages;maxPage = std::max<uint64_t>(maxPage, page.objects.size());
      for (const auto& row: page.objects) {
        const auto part = row.key.substr(hintPrefix(ns, shard).size());
        if (part.size() != 66 || !part.ends_with("/0")) continue;
        auto response = co_await store.get(row.key);held();
        if (response.status == 404) continue;
        requireStorage(response.status == 200, "alarm hint read failed");
        capnp::MallocMessageBuilder message;auto value = message.initRoot<capnp::JsonValue>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.bytes.data(), response.bytes.size()), value);
        const auto at = json::field(value.asReader(), "atUnixMs"), facet = json::field(value.asReader(), "facetId");
        if (!at.isNumber() || at.getNumber() <= 0 || !facet.isNumber() || facet.getNumber() != 0) continue;
        observed[part.substr(0, 64)] = uint64_t(at.getNumber());
      }
      token = std::move(page.nextToken);
    } while (!token.empty());
    std::set<std::string> previous, own;
    const auto ownPrefix = markerPrefix(nodeId, held().markerMemberEpoch, ns, shard, epoch);
    token.clear();
    do {
      auto page = co_await store.listPage(ownPrefix, 1000, token);held();
      for (const auto& row: page.objects)
        if (markerName(row.key.substr(ownPrefix.size())) && row.size == 0) own.insert(row.key);
      token = std::move(page.nextToken);
    } while (!token.empty());
    if (epoch > 1) {
      auto response = co_await store.get(shardEpochKey(ns, shard, epoch));held();
      requireStorage(response.status == 200 || response.status == 404, "alarm predecessor read failed");
      auto owner = response.status == 200 ? parseShardEpoch(response.bytes) : std::nullopt;
      requireStorage(response.status == 404 || owner.has_value(), "alarm predecessor record is invalid");
      requireStorage(!owner || (owner->namespaceId == ns && owner->shard == shard && owner->epoch == epoch &&
          owner->nodeId == nodeId), "alarm predecessor record names another owner");
      if (owner && owner->previousEpoch) {
        const auto prefix = markerPrefix(owner->previousNodeId, owner->previousNodeEpoch, ns, shard, owner->previousEpoch);
        token.clear();
        do {
          auto page = co_await store.listPage(prefix, 1000, token);held();
          for (const auto& row: page.objects)
            if (markerName(row.key.substr(prefix.size())) && row.size == 0) previous.insert(row.key);
          token = std::move(page.nextToken);
        } while (!token.empty());
      }
    }
    auto& s = held();s.hints = std::move(observed);
    if (s.marker.empty() && !own.empty()) s.marker = *own.begin();
    if (!s.marker.empty()) s.markerWritten = own.contains(s.marker);
    for (const auto& marker: own) if (marker != s.marker) previous.insert(marker);
    s.previousMarkers.insert(previous.begin(), previous.end());
    for (const auto& [object, at]: s.hints) {
      auto local = s.duringRead.find(object);
      if (local != s.duringRead.end() && !local->second) continue;
      auto [it, inserted] = s.desired.emplace(object, at);
      if (!s.startupOnly) adopt(ns, object, shard, epoch, s.tenant, it->second);
    }
    s.adopted = true;s.reading = false;s.failures = 0;s.next = 0;s.checked = monotonicTimeMs();
    for (const auto& [object, at]: s.desired) {
      auto found = s.hints.find(object);
      if (found == s.hints.end() || found->second != at) queueHint(s, object, at);
    }
    for (const auto& [object, at]: s.duringRead) if (!at && s.hints.contains(object)) queueHint(s, object, 0);
    s.duringRead.clear();reconcile(s);
  } catch (const kj::Exception& error) {
    auto it = shards.find(key);
    if (it != shards.end() && it->second.generation == generation && valid(it->second)) {
      it->second.reading = false;it->second.next = monotonicTimeMs() + retry(it->second.failures);
      log("alarm-adoption-failed", "\"namespaceId\":" + json::quote(ns) + ",\"error\":" +
          json::quote(std::string(error.getDescription().cStr())));
    }
  }
}

kj::Promise<void> AlarmPublication::checkMarkers() {
  const auto memberEpoch = nodeEpoch;
  auto release = kj::defer([this]() { checkingMarkers = false;--active;pump(); });
  try {
    std::set<std::string> present;std::set<uint64_t> members{memberEpoch};
    for (const auto& [key, s]: shards) if (valid(s) && s.epoch) members.insert(s.markerMemberEpoch);
    for (const auto member: members) {
      std::string token;
      do {
        auto page = co_await store.listPage("alarm-wake/by-owner/" + nodeId + "/" + wide(member) + "/", 1000, token);
        if (nodeEpoch != memberEpoch) co_return;
        for (const auto& row: page.objects) present.insert(row.key);
        token = std::move(page.nextToken);
      } while (!token.empty());
    }
    for (auto& [key, s]: shards) {
      if (!valid(s) || !s.adopted || s.reading) continue;
      if (!s.marker.empty() && s.markerWritten && !present.contains(s.marker)) { s.markerWritten = false;queueMarker(s, s.marker, true); }
      const auto prefix = markerPrefix(nodeId, s.markerMemberEpoch, s.ns, s.shard, s.epoch);
      if (s.markerWritten || (s.marker.empty() && s.hints.empty() && s.desired.empty()))
        for (const auto& old: present)
          if (old != s.marker && old.starts_with(prefix) && markerName(old.substr(prefix.size()))) queueMarker(s, old, false);
    }
    nextMarkerCheck = monotonicTimeMs() + 60000;
  } catch (const kj::Exception& error) {
    nextMarkerCheck = monotonicTimeMs() + 5000;
    log("alarm-marker-check-failed", "\"error\":" + json::quote(std::string(error.getDescription().cStr())));
  }
}
void AlarmPublication::tick() {
  const auto now = monotonicTimeMs();
  for (auto it = shards.begin(); it != shards.end();)
    if (!valid(it->second)) { auto ns = it->second.ns;auto shard = it->second.shard;++it;gone(ns, shard); } else ++it;
  if (!shards.empty() && now >= nextHintCheck) {
    Shard* oldest = nullptr;
    for (auto& [key, s]: shards)
      if (s.adopted && !s.reading && (!oldest || s.checked < oldest->checked)) oldest = &s;
    if (oldest) { oldest->adopted = false;oldest->next = 0;oldest->order = ++serial; }
    nextHintCheck = now + std::max(200.0, 3600000.0 / shards.size());
  }
  pump();
  if (nodeEpoch && active < 8 && !shards.empty() && !checkingMarkers && now >= nextMarkerCheck) {
    checkingMarkers = true;++active;tasks.add(checkMarkers());
  }
}
void AlarmPublication::taskFailed(kj::Exception&& error) {
  log("alarm-publication-task-failed", "\"error\":" + json::quote(std::string(error.getDescription().cStr())));
}
std::string AlarmPublication::json() const {
  uint64_t bytes = 0;double oldest = 0;
  for (const auto& [key, w]: writes)
    bytes += sizeof(Write) + key.capacity() + w.key.capacity() + w.ns.capacity() + w.object.capacity() + w.tenant.capacity();
  for (const auto& [key, s]: shards) oldest = std::max(oldest, monotonicTimeMs() - s.checked);
  std::ostringstream out;
  out << "\"publicationPending\":" << writes.size() << ",\"publicationInFlight\":" << active
      << ",\"publicationPeakPending\":" << peakPending << ",\"publicationBytes\":" << bytes
      << ",\"publicationRetries\":" << retries << ",\"markerWrites\":" << markerWrites
      << ",\"markerDeletes\":" << markerDeletes << ",\"adoptionPages\":" << pages
      << ",\"adoptionMaxPage\":" << maxPage << ",\"oldestHintCheckMs\":" << oldest;
  return out.str();
}
}  // namespace coworkerd

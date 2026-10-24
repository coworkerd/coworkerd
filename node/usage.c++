#include "usage.h"
#include "json.h"
#include "support.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace coworkerd {
namespace {
using namespace json;
uint64_t wall() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
double mono() {
  return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
}  // namespace

TenantUsage::TenantUsage(UsageSettings settings, std::string nodeId, std::optional<ObjectStoreConfig> storeConfig,
    kj::Network& network, kj::Timer& timer, Sources sources)
    : settings(settings), nodeId(std::move(nodeId)), timer(timer), sources(std::move(sources)) {
  if (storeConfig) store = std::make_unique<ObjectStore>(std::move(*storeConfig), network, timer);
}

TenantUsage::Counts& TenantUsage::now(const std::string& tenant) { return hours[usageHour(wallTimeMsForTest())][tenant]; }

void TenantUsage::request(const std::string& tenant) {
  if (tenant.empty()) return;
  auto& entry = now(tenant);
  ++entry.counts.requests;
  entry.dirty = true;
}

void TenantUsage::egress(const std::string& tenant, uint64_t bytes) {
  if (tenant.empty() || !bytes) return;
  auto& entry = now(tenant);
  entry.counts.egressBytes += bytes;
  entry.dirty = true;
}

kj::Promise<void> TenantUsage::start() {
  lastSample = mono();
  if (!store) co_return;
  // This hour's object, as the previous process of this node left it: the counts go on from there. Usage is no reason not
  // to serve: a read that fails is tried again before the hour's first write (flush()).
  seedPending = usageHour(wallTimeMsForTest());
  try {
    co_await seed();
    co_await prune();
  } catch (const kj::Exception& error) {
    lastError = error.getDescription().cStr();
  }
}

// The pending hour's object read back and added to what this process counted since: the stored counts and its own.
kj::Promise<void> TenantUsage::seed() {
  if (seedPending.empty()) co_return;
  const auto hour = seedPending;
  auto response = co_await store->get(nodeUsageKey(nodeId, hour));
  requireStorage(response.status == 200 || response.status == 404, "object store read failed: HTTP " + std::to_string(response.status));
  if (response.status == 200) {
    try {
      auto stored = decodeNodeUsageHour(response.bytes);
      if (stored.nodeId == nodeId && stored.hour == hour) {
        for (const auto& [tenant, counts] : stored.tenants) {
          hours[hour][tenant].counts.add(counts);
          ++seededTenants;
        }
        seededHour = hour;
      }
    } catch (const kj::Exception& error) {
      // Not a usage object: nothing to go on from (the next write replaces it).
      lastError = std::string("this hour's usage object does not read: ") + error.getDescription().cStr();
    }
  }
  seedPending.clear();
}

void TenantUsage::stop(std::string reason) { stoppedReason = std::move(reason); }

kj::Promise<void> TenantUsage::sample() {
  const auto at = mono();
  const double elapsed = at - lastSample;
  lastSample = at;
  for (const auto& [tenant, ns] : sources.takeCpuNs()) {
    if (!ns) continue;
    auto& entry = now(tenant);
    entry.counts.cpuMs += double(ns) / 1e6;
    entry.dirty = true;
  }
  for (const auto& [tenant, count] : sources.residentIsolates()) {
    if (!count) continue;
    auto& entry = now(tenant);
    entry.counts.isolateMs += elapsed * double(count);
    entry.dirty = true;
  }
  std::string text;
  try { text = co_await sources.logLines(); } catch (const kj::Exception&) { co_return; }
  try {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), json);
    auto epochField = field(json, "epoch");
    const std::string epoch = epochField.isString() ? epochField.getString().cStr() : "";
    if (epoch != logEpoch) { lastKept.clear(); logEpoch = epoch; }
    auto tenants = field(json, "tenants");
    if (tenants.isObject()) for (auto entry : tenants.getObject()) {
      const std::string tenant = entry.getName().cStr();
      auto kept = field(entry.getValue(), "keptLines");
      if (!kept.isNumber()) continue;
      const auto total = static_cast<uint64_t>(kept.getNumber());
      auto& last = lastKept[tenant];
      if (total > last) {
        auto& counts = now(tenant);
        counts.counts.logLines += total - last;
        counts.dirty = true;
      }
      last = total;
    }
  } catch (const kj::Exception&) {}
}

kj::Promise<void> TenantUsage::flush() {
  if (!store || !stoppedReason.empty()) co_return;
  const auto at = wallTimeMsForTest();
  const auto current = usageHour(at), previous = usageHour(at > 3600000 ? at - 3600000 : 0);
  // The start's read of its hour, tried again: until it succeeds, that hour is not written (its object is never replaced
  // by counts from zero).
  if (!seedPending.empty()) {
    ++seedRetries;
    try { co_await seed(); } catch (const kj::Exception& error) { lastError = error.getDescription().cStr(); }
  }
  try {
    // Each hour with a change: its object again, every tenant of the hour in it.
    for (auto& [hour, tenants] : hours) {
      if (hour == seedPending) continue;
      if (std::none_of(tenants.begin(), tenants.end(), [](const auto& entry) { return entry.second.dirty; })) continue;
      NodeUsageHour object{nodeId, hour, at, {}};
      for (const auto& [tenant, entry] : tenants) object.tenants[tenant] = entry.counts;
      co_await store->put(nodeUsageKey(nodeId, hour), nodeUsageHourJson(object));
      for (auto& [tenant, entry] : tenants) entry.dirty = false;
      ++objectsWritten;
      rowsWritten += object.tenants.size();
    }
    ++flushes;
    lastFlushAtUnixMs = wall();
    lastError.clear();
  } catch (const kj::Exception& error) {
    ++flushFailures;
    lastError = error.getDescription().cStr();
    co_return;
  }
  // Hours before the previous one that are written stay only in the store.
  for (auto it = hours.begin(); it != hours.end();) {
    const bool clean = std::all_of(it->second.begin(), it->second.end(), [](const auto& entry) { return !entry.second.dirty; });
    if (it->first < previous && clean) it = hours.erase(it); else ++it;
  }
  if (current != lastPruneHour) {
    try { co_await prune(); } catch (const kj::Exception& error) { lastError = error.getDescription().cStr(); }
  }
}

// Once an hour: this node's objects of the hours before retentionHours ago, found by listing its own prefix.
kj::Promise<void> TenantUsage::prune() {
  if (!store || !settings.retentionHours || !stoppedReason.empty()) co_return;
  const auto at = wallTimeMsForTest(), keep = uint64_t(settings.retentionHours) * 3600000;
  lastPruneHour = usageHour(at);
  if (at <= keep) co_return;
  const auto cutoff = usageHour(at - keep);
  auto listing = co_await store->list(nodeUsagePrefix(nodeId));
  for (const auto& object : listing.objects) {
    auto parsed = parseNodeUsageKey(object.key);
    if (!parsed || parsed->first != nodeId || parsed->second >= cutoff) continue;
    co_await store->remove(object.key);
    ++prunedObjects;
  }
  ++prunes;
  prunedThrough = cutoff;
}

kj::Promise<std::string> TenantUsage::flushNow() {
  if (!stoppedReason.empty())
    co_return "{\"backend\":\"" + std::string(store ? "objectStore" : "none") + "\",\"objectsWritten\":0,\"rowsWritten\":0,"
        "\"stopped\":" + quote(stoppedReason) + "}";
  const auto objectsBefore = objectsWritten, rowsBefore = rowsWritten, failuresBefore = flushFailures;
  std::string failure;
  try {
    co_await sample();
    co_await flush();
  } catch (const kj::Exception& error) {
    failure = error.getDescription().cStr();
  }
  size_t unwritten = 0;
  for (const auto& [hour, tenants] : hours) for (const auto& [tenant, entry] : tenants) if (entry.dirty) ++unwritten;
  std::ostringstream out;
  out << "{\"backend\":\"" << (store ? "objectStore" : "none") << "\",\"objectsWritten\":" << (objectsWritten - objectsBefore)
      << ",\"rowsWritten\":" << (rowsWritten - rowsBefore) << ",\"flushFailures\":" << (flushFailures - failuresBefore)
      << ",\"unwrittenRows\":" << unwritten << ",\"lastError\":" << quote(failure.empty() ? lastError : failure) << "}";
  co_return out.str();
}

kj::Promise<void> TenantUsage::run() {
  auto nextFlush = mono() + settings.flushIntervalMs;
  while (true) {
    co_await timer.afterDelay(settings.sampleIntervalMs * kj::MILLISECONDS);
    co_await sample();
    if (mono() >= nextFlush) {
      co_await flush();
      nextFlush = mono() + settings.flushIntervalMs;
    }
  }
}

std::string TenantUsage::json() const {
  std::ostringstream out;
  out << std::setprecision(17) << "{\"backend\":\"" << (store ? "objectStore" : "none") << "\",\"prefix\":"
      << quote(store ? nodeUsagePrefix(nodeId) : std::string()) << ",\"flushIntervalMs\":" << settings.flushIntervalMs
      << ",\"retentionHours\":" << settings.retentionHours << ",\"sampleIntervalMs\":" << settings.sampleIntervalMs
      << ",\"flushes\":" << flushes << ",\"flushFailures\":" << flushFailures << ",\"objectsWritten\":" << objectsWritten
      << ",\"rowsWritten\":" << rowsWritten << ",\"seededHour\":" << quote(seededHour) << ",\"seededRows\":" << seededTenants
      << ",\"prunes\":" << prunes << ",\"prunedObjects\":" << prunedObjects << ",\"prunedThrough\":" << quote(prunedThrough)
      << ",\"seedPending\":" << quote(seedPending) << ",\"seedRetries\":" << seedRetries << ",\"stopped\":" << quote(stoppedReason)
      << ",\"lastFlushAtUnixMs\":" << lastFlushAtUnixMs << ",\"lastError\":" << quote(lastError) << ",\"hours\":{";
  size_t h = 0;
  for (const auto& [hour, tenants] : hours) {
    out << (h++ ? "," : "") << quote(hour) << ":{";
    size_t t = 0;
    for (const auto& [tenant, entry] : tenants) {
      const auto& c = entry.counts;
      out << (t++ ? "," : "") << quote(tenant) << ":{\"requests\":" << c.requests << ",\"cpuMs\":" << c.cpuMs << ",\"egressBytes\":"
          << c.egressBytes << ",\"logLines\":" << c.logLines << ",\"isolateMs\":" << c.isolateMs << ",\"unwritten\":"
          << (entry.dirty ? "true" : "false") << "}";
    }
    out << "}";
  }
  out << "}}";
  return out.str();
}

}  // namespace coworkerd

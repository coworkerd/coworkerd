#include "usage.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace coworkerd {
namespace {
using namespace etcd;
uint64_t wall() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
double mono() {
  return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
std::string end(std::string prefix) { ++prefix.back(); return prefix; }
}  // namespace

TenantUsage::TenantUsage(UsageSettings settings, std::string nodeId, const MembershipConfig& authority, kj::Network& network,
    kj::Timer& timer, const kj::HttpHeaderTable& headers, Sources sources)
    : settings(settings), nodeId(std::move(nodeId)), timer(timer), sources(std::move(sources)) {
  if (authority.backend == "etcd")
    client = std::make_unique<EtcdClient>(authority.endpoint, authority.requestTimeoutMs, network, timer, headers);
}

TenantUsage::Counts& TenantUsage::now(const std::string& tenant) { return hours[usageHour(wall())][tenant]; }

void TenantUsage::request(const std::string& tenant) {
  if (tenant.empty()) return;
  auto& counts = now(tenant);
  ++counts.requests;
  counts.dirty = true;
}

void TenantUsage::egress(const std::string& tenant, uint64_t bytes) {
  if (tenant.empty() || !bytes) return;
  auto& counts = now(tenant);
  counts.egressBytes += bytes;
  counts.dirty = true;
}

kj::Promise<void> TenantUsage::start() {
  lastSample = mono();
  if (!client) co_return;
  // This hour's own rows, as the previous process of this node left them: the counts go on from there.
  const auto hour = usageHour(wall());
  try {
    auto raw = co_await client->range(nodeUsagePrefix(nodeId) + hour + "/");
    for (const auto& [key, kv] : raw.values) {
      try {
        auto row = decodeUsageRow(kv.value);
        if (row.hour != hour || row.nodeId != nodeId || key != nodeUsageKey(nodeId, hour, row.tenant)) continue;
        auto& counts = hours[hour][row.tenant];
        counts.requests += row.requests; counts.logLines += row.logLines; counts.egressBytes += row.egressBytes;
        counts.cpuMs += row.cpuMs; counts.isolateMs += row.isolateMs;
        ++seededRows;
      } catch (const kj::Exception&) {}
    }
    co_await prune();
  } catch (const kj::Exception& error) {
    // Usage is no reason not to serve: counted from zero, written when etcd answers.
    lastError = error.getDescription().cStr();
  }
}

kj::Promise<void> TenantUsage::sample() {
  const auto at = mono();
  const double elapsed = at - lastSample;
  lastSample = at;
  for (const auto& [tenant, ns] : sources.takeCpuNs()) {
    if (!ns) continue;
    auto& counts = now(tenant);
    counts.cpuMs += double(ns) / 1e6;
    counts.dirty = true;
  }
  for (const auto& [tenant, count] : sources.residentIsolates()) {
    if (!count) continue;
    auto& counts = now(tenant);
    counts.isolateMs += elapsed * double(count);
    counts.dirty = true;
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
        counts.logLines += total - last;
        counts.dirty = true;
      }
      last = total;
    }
  } catch (const kj::Exception&) {}
}

kj::Promise<void> TenantUsage::flush() {
  if (!client) co_return;
  const auto current = usageHour(wall());
  std::vector<std::pair<std::string, std::string>> rows;  // key, value
  std::vector<std::pair<std::string, std::string>> flushed;  // hour, tenant
  const auto updated = wall();
  for (const auto& [hour, tenants] : hours) for (const auto& [tenant, counts] : tenants) {
    if (!counts.dirty) continue;
    rows.emplace_back(nodeUsageKey(nodeId, hour, tenant), usageRowJson({tenant, hour, nodeId, counts.requests, counts.logLines,
        counts.egressBytes, counts.cpuMs, counts.isolateMs, updated}));
    flushed.emplace_back(hour, tenant);
  }
  try {
    for (size_t start = 0; start < rows.size(); start += 100) {
      std::string txn = "{\"compare\":[],\"success\":[";
      for (size_t i = start; i < std::min(rows.size(), start + 100); ++i)
        txn += std::string(i > start ? "," : "") + "{\"request_put\":{\"key\":" + quote(base64(rows[i].first)) + ",\"value\":" +
            quote(base64(rows[i].second)) + "}}";
      co_await client->request("/v3/kv/txn", txn + "]}");
    }
    for (const auto& [hour, tenant] : flushed) hours[hour][tenant].dirty = false;
    rowsWritten += rows.size();
    ++flushes;
    lastFlushAtUnixMs = wall();
    lastError.clear();
  } catch (const kj::Exception& error) {
    ++flushFailures;
    lastError = error.getDescription().cStr();
    co_return;
  }
  // Hours that are over and written stay only in etcd.
  bool rolled = false;
  for (auto it = hours.begin(); it != hours.end();) {
    bool clean = std::all_of(it->second.begin(), it->second.end(), [](const auto& entry) { return !entry.second.dirty; });
    if (it->first < current && clean) { it = hours.erase(it); rolled = true; } else ++it;
  }
  if (rolled) co_await prune();
}

kj::Promise<void> TenantUsage::prune() {
  if (!client || !settings.retentionHours) co_return;
  const auto now = wall(), keep = uint64_t(settings.retentionHours) * 3600000;
  if (now <= keep) co_return;
  const auto cutoff = usageHour(now - keep);
  const auto prefix = nodeUsagePrefix(nodeId);
  // Every row of an hour before the cutoff: [prefix, prefix + cutoff).
  auto response = co_await client->request("/v3/kv/deleterange", "{\"key\":" + quote(base64(prefix)) + ",\"range_end\":" +
      quote(base64(prefix + cutoff)) + "}");
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
  prunes += integer(json, "deleted");
  prunedThrough = cutoff;
}

kj::Promise<std::string> TenantUsage::flushNow() {
  const auto rowsBefore = rowsWritten, failuresBefore = flushFailures;
  std::string failure;
  try {
    co_await sample();
    co_await flush();
  } catch (const kj::Exception& error) {
    failure = error.getDescription().cStr();
  }
  size_t unwritten = 0;
  for (const auto& [hour, tenants] : hours) for (const auto& [tenant, counts] : tenants) if (counts.dirty) ++unwritten;
  std::ostringstream out;
  out << "{\"backend\":\"" << (client ? "etcd" : "none") << "\",\"rowsWritten\":" << (rowsWritten - rowsBefore)
      << ",\"flushFailures\":" << (flushFailures - failuresBefore) << ",\"unwrittenRows\":" << unwritten
      << ",\"lastError\":" << quote(failure.empty() ? lastError : failure) << "}";
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
  out << std::setprecision(17) << "{\"backend\":\"" << (client ? "etcd" : "none") << "\",\"flushIntervalMs\":" << settings.flushIntervalMs
      << ",\"retentionHours\":" << settings.retentionHours << ",\"sampleIntervalMs\":" << settings.sampleIntervalMs
      << ",\"flushes\":" << flushes << ",\"flushFailures\":" << flushFailures << ",\"rowsWritten\":" << rowsWritten
      << ",\"seededRows\":" << seededRows << ",\"prunedRows\":" << prunes << ",\"prunedThrough\":" << quote(prunedThrough)
      << ",\"lastFlushAtUnixMs\":" << lastFlushAtUnixMs << ",\"lastError\":" << quote(lastError) << ",\"hours\":{";
  size_t h = 0;
  for (const auto& [hour, tenants] : hours) {
    out << (h++ ? "," : "") << quote(hour) << ":{";
    size_t t = 0;
    for (const auto& [tenant, c] : tenants) {
      out << (t++ ? "," : "") << quote(tenant) << ":{\"requests\":" << c.requests << ",\"cpuMs\":" << c.cpuMs << ",\"egressBytes\":"
          << c.egressBytes << ",\"logLines\":" << c.logLines << ",\"isolateMs\":" << c.isolateMs << ",\"unwritten\":"
          << (c.dirty ? "true" : "false") << "}";
    }
    out << "}";
  }
  out << "}}";
  return out.str();
}

}  // namespace coworkerd

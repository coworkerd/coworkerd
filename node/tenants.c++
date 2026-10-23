#include "tenants.h"

#include <chrono>
#include <iostream>
#include <sstream>

namespace coworkerd {

TenantView::TenantView(const MembershipConfig& authority, kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headers)
    : config(authority), timer(timer) {
  if (config.backend == "etcd") {
    client = std::make_unique<EtcdClient>(config.endpoint, config.requestTimeoutMs, network, timer, headers);
    watcher = std::make_unique<EtcdWatcher>(*client, TENANTS_PREFIX, timer, config.retryMs,
        [this](const EtcdSnapshot& snapshot) { return apply(snapshot); });
  }
}

void TenantView::onQuota(std::function<void(const std::string&, uint64_t, uint64_t)> callback) { quota = std::move(callback); }

kj::Promise<void> TenantView::start() {
  if (!watcher) co_return;
  const auto until = timer.now() + config.startupTimeoutMs * kj::MILLISECONDS;
  while (true) {
    std::string error;
    try { co_await watcher->fullRead(); co_return; }
    catch (const kj::Exception& failure) { error = failure.getDescription().cStr(); }
    KJ_REQUIRE(timer.now() < until, "the tenants cannot be read at startup", error.c_str());
    co_await timer.afterDelay(config.retryMs * kj::MILLISECONDS);
  }
}

kj::Promise<void> TenantView::run() {
  if (!watcher) return kj::NEVER_DONE;
  return watcher->run();
}

kj::Promise<void> TenantView::apply(const EtcdSnapshot& snapshot) {
  std::map<std::string, TenantRecord> next;
  uint64_t bad = 0;
  for (const auto& [key, kv] : snapshot.values) {
    try {
      auto record = decodeTenantRecord(kv.value);
      if (key != std::string(TENANTS_PREFIX) + record.name) { ++bad; continue; }
      next.emplace(record.name, std::move(record));
    } catch (const kj::Exception&) { ++bad; }  // not a record the control plane wrote: no tenant
  }
  for (const auto& [name, record] : next) {
    auto before = tenants.find(name);
    if (before == tenants.end() || before->second.status != record.status)
      std::cout << "[tenant] {\"event\":\"" << (before == tenants.end() ? "added" : "status") << "\",\"tenant\":\"" << name
                << "\",\"status\":\"" << record.status << "\",\"revision\":" << snapshot.revision << "}" << std::endl;
  }
  for (const auto& [name, record] : tenants)
    if (!next.contains(name))
      std::cout << "[tenant] {\"event\":\"removed\",\"tenant\":\"" << name << "\",\"revision\":" << snapshot.revision << "}" << std::endl;
  tenants = std::move(next);
  revision = snapshot.revision;
  unreadable = bad;
  // Which tenant each hostname belongs to (the control plane keeps them unique; one listed twice belongs to no one).
  std::map<std::string, std::string> hosts;
  uint64_t shared = 0;
  for (const auto& [name, record] : tenants) {
    for (const auto& hostname : record.hostnames) {
      auto [entry, added] = hosts.emplace(hostname, name);
      if (!added && !entry->second.empty()) { entry->second.clear(); ++shared; }
    }
  }
  owners = std::move(hosts);
  sharedHostnames = shared;
  // The log quotas the drainer holds follow the records.
  std::map<std::string, std::pair<uint64_t, uint64_t>> wanted;
  for (const auto& [name, record] : tenants) wanted[name] = {record.quotas.logLinesPerSecond, record.quotas.logBufferBytes};
  if (quota) {
    for (const auto& [name, value] : wanted) {
      auto held = quotas.find(name);
      if (held == quotas.end() || held->second != value) quota(name, value.first, value.second);
    }
    for (const auto& [name, value] : quotas) if (!wanted.contains(name)) quota(name, 0, 0);
  }
  quotas = std::move(wanted);
  co_return;
}

const TenantRecord* TenantView::tenant(const std::string& name) const {
  auto found = tenants.find(name);
  return found == tenants.end() ? nullptr : &found->second;
}

bool TenantView::deleting(const std::string& name) const {
  auto found = tenants.find(name);
  return found != tenants.end() && found->second.status == "deleting";
}

std::string TenantView::unavailable(const std::string& name) const {
  auto found = tenants.find(name);
  if (found != tenants.end()) return found->second.status == "deleting" ? "tenant " + name + " is being deleted" : "";
  // Without etcd there are no records; the operations tool's default tenant never has one.
  if (!watcher || name == DEFAULT_TENANT) return "";
  return "tenant " + name + " does not exist";
}

const std::string* TenantView::owner(const std::string& hostname) const {
  auto found = owners.find(hostname);
  return found == owners.end() ? nullptr : &found->second;
}

std::string TenantView::json() const {
  std::ostringstream out;
  out << "{\"backend\":\"" << (watcher ? "etcd" : "none") << "\",\"revision\":\"" << revision << "\",\"watchConnected\":"
      << (watcher && watcher->view().connected ? "true" : "false") << ",\"unreadableRecords\":" << unreadable
      << ",\"hostnames\":" << owners.size() << ",\"sharedHostnames\":" << sharedHostnames << ",\"records\":{";
  size_t index = 0;
  for (const auto& [name, record] : tenants) {
    out << (index++ ? "," : "") << "\"" << name << "\":{\"status\":\"" << record.status << "\",\"logLinesPerSecond\":"
        << record.quotas.logLinesPerSecond << ",\"logBufferBytes\":" << record.quotas.logBufferBytes << ",\"defaultLimits\":"
        << workerLimitsJson(record.defaultLimits) << ",\"hostnames\":" << record.hostnames.size() << "}";
  }
  out << "}}";
  return out.str();
}

}  // namespace coworkerd

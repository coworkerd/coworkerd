#include "tenants.h"

#include <iostream>
#include <sstream>

namespace coworkerd {

void TenantView::onQuota(std::function<void(const std::string&, uint64_t, uint64_t)> callback) { quota = std::move(callback); }

void TenantView::update(const std::map<std::string, std::optional<TenantRecord>>& named, uint64_t at, bool records) {
  hasRecords = records;
  std::map<std::string, TenantRecord> next;
  uint64_t bare = 0;
  for (const auto& [name, record] : named) {
    if (record) next.emplace(name, *record);
    else ++bare;  // a tenant only the operations tool writes: no record
  }
  for (const auto& [name, record] : next) {
    auto before = tenants.find(name);
    if (before == tenants.end() || before->second.status != record.status)
      std::cout << "[tenant] {\"event\":\"" << (before == tenants.end() ? "added" : "status") << "\",\"tenant\":\"" << name
                << "\",\"status\":\"" << record.status << "\",\"revision\":" << at << "}" << std::endl;
  }
  for (const auto& [name, record] : tenants)
    if (!next.contains(name))
      std::cout << "[tenant] {\"event\":\"removed\",\"tenant\":\"" << name << "\",\"revision\":" << at << "}" << std::endl;
  tenants = std::move(next);
  revision = at;
  withoutRecord = bare;
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
  // Without records (the file backend) every tenant is served; the operations tool's default tenant never has one.
  if (!hasRecords || name == DEFAULT_TENANT) return "";
  return "tenant " + name + " does not exist";
}

const std::string* TenantView::owner(const std::string& hostname) const {
  auto found = owners.find(hostname);
  return found == owners.end() ? nullptr : &found->second;
}

std::string TenantView::json() const {
  std::ostringstream out;
  out << "{\"backend\":\"" << (hasRecords ? "objectStore" : "none") << "\",\"revision\":\"" << revision
      << "\",\"tenantsWithoutRecord\":" << withoutRecord << ",\"hostnames\":" << owners.size()
      << ",\"sharedHostnames\":" << sharedHostnames << ",\"records\":{";
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

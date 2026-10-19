#pragma once

#include "config.h"
#include "etcd.h"
#include "tenancy.h"

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <functional>
#include <map>
#include <memory>
#include <string>

namespace coworkerd {

// The tenants as this node sees them: each record below /coworkerd/v2/tenants/, watched (read only; the
// control plane writes them). The node takes three things from a record: a tenant being deleted is not served (new
// roots of its Workers are refused before anything runs), its log quota (handed to the log drainer through
// `onQuota`), and its Workers' default limits (reported beside each Worker's own). A node without etcd (the file
// backend) has no records, and every tenant is served with the node's defaults.
class TenantView final {
public:
  TenantView(const MembershipConfig& authority, kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headers);
  // Called for every tenant whose log quota a record sets or changed, and with (tenant, 0, 0) when it no longer does.
  void onQuota(std::function<void(const std::string&, uint64_t linesPerSecond, uint64_t bufferBytes)> callback);
  kj::Promise<void> start();
  kj::Promise<void> run();
  const TenantRecord* tenant(const std::string& name) const;
  bool deleting(const std::string& name) const;
  std::string json() const;

private:
  MembershipConfig config;
  kj::Timer& timer;
  std::unique_ptr<EtcdClient> client;
  std::unique_ptr<EtcdWatcher> watcher;
  std::map<std::string, TenantRecord> tenants;
  std::map<std::string, std::pair<uint64_t, uint64_t>> quotas;  // tenant -> (lines per second, buffer bytes) handed over
  std::function<void(const std::string&, uint64_t, uint64_t)> quota;
  uint64_t revision = 0, unreadable = 0;
  kj::Promise<void> apply(const EtcdSnapshot& snapshot);
};

}  // namespace coworkerd

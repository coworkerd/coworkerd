#pragma once

#include "etcd.h"
#include "membership.h"
#include <memory>

namespace coworkerd {

// A suspicion (of a tenant): the Worker whose invocation was the culprit, where and when.
struct Suspect {
  std::string tenant, workerId, sourceServerId, processId, invocationId;
  uint64_t generation = 0, count = 0, recognizedAtUnixMs = 0;
  uint64_t expiresAtUnixMs = 0, durationMs = 0;
  bool synced = false;
  std::string json() const;
};

// The request path reads only this local view. Only a validated watchdog report
// may create a local suspicion; authority I/O runs independently of dispatch.
// A suspicion is of a tenant (control-plane-api.md section 1): any Worker of it confirmed as a culprit
// puts every Worker of the tenant into the quarantine processes until the observation period ends. Each node
// writes its own suspicions, /coworkerd/v2/nodes/<nodeId>/suspects/<tenant> (under a lease of the remaining
// period), and adopts every node's.
class Suspects final {
public:
  Suspects(const MembershipConfig& authority, std::string serverId,
      uint32_t baseMs, uint32_t maxMs, kj::Network& network, kj::Timer& timer,
      const kj::HttpHeaderTable& headers);
  bool contains(const std::string& tenant) const;
  // The culprit's Worker (a full name); its tenant is the suspect.
  void confirm(const std::string& worker, const std::string& process,
      uint64_t generation, const std::string& invocation);
  kj::Promise<void> start();
  kj::Promise<void> run();
  std::string json() const;
private:
  MembershipConfig config;
  std::string serverId;
  uint32_t baseMs, maxMs;
  kj::Timer& timer;
  std::map<std::string, Suspect> entries, pending;
  std::map<std::string, uint64_t> counts;
  std::map<std::string, double> deadlines;
  std::unique_ptr<EtcdClient> client;
  std::unique_ptr<EtcdWatcher> watcher;
  std::string lastError;
  kj::Promise<void> apply(const EtcdBatch& batch);
  kj::Promise<void> publish(Suspect record);
  kj::Promise<void> maintain();
  void expire();
  void remember(Suspect record, const char* event);
};
}  // namespace coworkerd

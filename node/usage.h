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

// Each tenant's usage on this node (control-plane-api.md section 4): requests (each execution of one of its
// Workers here), CPU milliseconds (the `completed` rows of those executions, the metering the CPU limit already
// has), egress bytes (counted through egress()), log lines (the lines the drainer kept for it) and isolate
// time (its resident isolates, sampled). Counted per UTC hour; the current hour's rows are written every
// flushIntervalMs to /coworkerd/v2/nodes/<nodeId>/usage/<hour>/<tenant> -- this node's own prefix, the only one its
// etcd user may write -- and rows older than retentionHours are deleted. A restart within the hour continues from
// the row this node wrote last, so at most one flush interval is lost. A node without etcd only counts.
class TenantUsage final {
public:
  struct Sources {
    std::function<std::map<std::string, uint64_t>()> takeCpuNs;         // per tenant since the last call
    std::function<std::map<std::string, uint64_t>()> residentIsolates;  // per tenant, now
    std::function<kj::Promise<std::string>()> logLines;                 // the drainer's counts (WorkerdOutput::tenantLines)
  };
  TenantUsage(UsageSettings settings, std::string nodeId, const MembershipConfig& authority, kj::Network& network,
      kj::Timer& timer, const kj::HttpHeaderTable& headers, Sources sources);
  void request(const std::string& tenant);
  void egress(const std::string& tenant, uint64_t bytes);
  kj::Promise<void> start();
  kj::Promise<void> run();
  // The last counts before this node leaves: the sample and the write the run loop would have done at
  // its next interval, so a drained node loses nothing instead of up to one flush interval. Called once the
  // processes are closed, so every execution's row has been read, and before the lease is revoked, which is
  // the last moment this node may write. Without etcd it only samples. Returns what it wrote, for the drain
  // record; it throws nothing -- a node that cannot write its last minute still has to leave.
  kj::Promise<std::string> flushNow();
  std::string json() const;

private:
  struct Counts {
    uint64_t requests = 0, logLines = 0, egressBytes = 0;
    double cpuMs = 0, isolateMs = 0;
    bool dirty = false;
  };
  UsageSettings settings;
  std::string nodeId;
  kj::Timer& timer;
  Sources sources;
  std::unique_ptr<EtcdClient> client;
  std::map<std::string, std::map<std::string, Counts>> hours;  // hour -> tenant -> counts
  std::map<std::string, uint64_t> lastKept;                    // the drainer's kept lines at the last sample
  std::string logEpoch;
  double lastSample = 0;
  uint64_t flushes = 0, flushFailures = 0, rowsWritten = 0, prunes = 0, lastFlushAtUnixMs = 0, seededRows = 0;
  std::string lastError, prunedThrough;
  Counts& now(const std::string& tenant);
  kj::Promise<void> sample();
  kj::Promise<void> flush();
  kj::Promise<void> prune();
};

}  // namespace coworkerd

#pragma once

#include "config.h"
#include "object-store.h"
#include "tenancy.h"

#include <kj/async-io.h>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>

namespace coworkerd {

// Each tenant's usage on this node (control-plane-api.md section 4): requests (each execution of one of its
// Workers here), CPU milliseconds (the `completed` rows of those executions, the metering the CPU limit already
// has), egress bytes (counted through egress()), log lines (the lines the drainer kept for it) and isolate
// time (its resident isolates, sampled). Counted per UTC hour; every flushIntervalMs the hours whose counts changed are
// written to the object store, one object per hour holding every tenant, nodes/<nodeId>/usage/<hour> -- this node's own
// prefix, the only one its identity may write -- and once an hour the objects older than retentionHours are removed
// (a listing of the prefix). A restart within the hour reads that hour's object back first and continues from it, so at
// most one flush interval is lost; when that read fails (not a missing object), it is tried again before each flush and
// the hour is not written until it succeeds -- never from zero over the object. A process whose node id another process
// took writes no more usage (stop()). A node without the object store (the file membership) only counts. The hours are
// those of wallTimeMsForTest() (clockOffsetMsForTest moves them, for the tests of the retention).
class TenantUsage final {
public:
  struct Sources {
    std::function<std::map<std::string, uint64_t>()> takeCpuNs;         // per tenant since the last call
    std::function<std::map<std::string, uint64_t>()> residentIsolates;  // per tenant, now
    std::function<kj::Promise<std::string>()> logLines;                 // the drainer's counts (WorkerdOutput::tenantLines)
  };
  // `store`: where the usage objects go (the node's own identity), or none: counted only.
  TenantUsage(UsageSettings settings, std::string nodeId, std::optional<ObjectStoreConfig> store, kj::Network& network,
      kj::Timer& timer, Sources sources);
  void request(const std::string& tenant);
  void egress(const std::string& tenant, uint64_t bytes);
  kj::Promise<void> start();
  kj::Promise<void> run();
  // The last counts before this node leaves: the sample and the write the run loop would have done at
  // its next interval, so a drained node loses nothing instead of up to one flush interval. Called once the
  // processes are closed, so every execution's row has been read, and before the membership stops. Without the object
  // store it only samples. Returns what it wrote, for the drain record; it throws nothing -- a node that cannot write
  // its last minute still has to leave.
  kj::Promise<std::string> flushNow();
  // No more writes from this process (another process holds the node's id and writes its objects).
  void stop(std::string reason);
  std::string json() const;

private:
  struct Counts {
    UsageCounts counts;
    bool dirty = false;
  };
  UsageSettings settings;
  std::string nodeId;
  kj::Timer& timer;
  Sources sources;
  std::unique_ptr<ObjectStore> store;
  std::map<std::string, std::map<std::string, Counts>> hours;  // hour -> tenant -> counts
  std::map<std::string, uint64_t> lastKept;                    // the drainer's kept lines at the last sample
  std::string logEpoch;
  double lastSample = 0;
  uint64_t flushes = 0, flushFailures = 0, objectsWritten = 0, rowsWritten = 0, prunes = 0, prunedObjects = 0;
  uint64_t lastFlushAtUnixMs = 0;
  uint64_t seededTenants = 0;
  std::string lastError, prunedThrough, lastPruneHour, seededHour;
  // The hour whose object has not been read back yet (the start's read failed), and how often it was tried again.
  std::string seedPending;
  uint64_t seedRetries = 0;
  std::string stoppedReason;
  Counts& now(const std::string& tenant);
  kj::Promise<void> seed();
  kj::Promise<void> sample();
  kj::Promise<void> flush();
  kj::Promise<void> prune();
};

}  // namespace coworkerd

#pragma once

#include "object-store.h"
#include <kj/async.h>
#include <functional>
#include <map>
#include <set>
#include <string>

namespace coworkerd {

// Auxiliary discovery only. SQLite and the ordinary object/ownership path remain authoritative.
class AlarmPublication final: private kj::TaskSet::ErrorHandler {
public:
  using Current = std::function<bool(const std::string&, uint32_t, uint64_t, uint64_t)>;
  using Adopt = std::function<void(const std::string&, const std::string&, uint32_t, uint64_t,
      const std::string&, uint64_t)>;
  using Log = std::function<void(const std::string&, const std::string&)>;
  AlarmPublication(ObjectStore& store, kj::Timer& timer, std::string nodeId, Current current, Adopt adopt, Log log);
  ~AlarmPublication() noexcept(false);
  void setNodeEpoch(uint64_t epoch);
  void take(std::string ns, uint32_t shard, uint64_t epoch, std::string tenant);
  // Only local startup candidates; verify their latest existing owner, without acquiring an epoch.
  void restore(std::string ns, uint32_t shard, std::string tenant);
  void gone(const std::string& ns, uint32_t shard);
  void report(std::string ns, std::string object, uint32_t shard, uint64_t epoch,
      std::string tenant, uint64_t at);
  void tick();
  std::string json() const;
  uint64_t hintWrites = 0, hintFailures = 0;

private:
  struct Shard {
    std::string ns, tenant, marker;
    uint32_t shard = 0;
    uint64_t epoch = 0, memberEpoch = 0, markerMemberEpoch = 0, generation = 0, order = 0;
    bool adopted = false, reading = false, markerWritten = false, startupOnly = false;
    uint32_t failures = 0;
    double next = 0, checked = 0;
    std::map<std::string, uint64_t> desired, hints;
    // Reports received during a scan override its old values, including a clearing.
    std::map<std::string, uint64_t> duringRead;
    std::set<std::string> previousMarkers;
  };
  struct Write {
    std::string key, ns, object, tenant;
    uint32_t shard = 0, failures = 0;
    uint64_t epoch = 0, memberEpoch = 0, version = 0, at = 0, order = 0;
    bool marker = false, inFlight = false;
    double next = 0;
  };
  std::string shardKey(const std::string& ns, uint32_t shard) const;
  bool valid(const Shard& shard) const;
  bool valid(const Write& write) const;
  void queueHint(Shard& shard, const std::string& object, uint64_t at);
  void queueMarker(Shard& shard, const std::string& key, bool present);
  void reconcile(Shard& shard);
  void pump();
  kj::Promise<void> write(std::string key);
  kj::Promise<void> readHints(std::string key);
  kj::Promise<void> checkMarkers();
  uint32_t retry(uint32_t& failures);
  void taskFailed(kj::Exception&& error) override;

  ObjectStore& store;
  kj::Timer& timer;
  std::string nodeId;
  Current current;
  Adopt adopt;
  Log log;
  uint64_t nodeEpoch = 0, serial = 0;
  std::map<std::string, Shard> shards;
  std::map<std::string, Write> writes;
  std::string lastTenant;
  uint32_t active = 0;
  uint64_t peakPending = 0, retries = 0, markerWrites = 0, markerDeletes = 0, pages = 0, maxPage = 0;
  double nextHintCheck = 0, nextMarkerCheck = 0;
  bool checkingMarkers = false, stopping = false;
  kj::TaskSet tasks;
};
}  // namespace coworkerd

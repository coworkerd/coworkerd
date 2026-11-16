#pragma once
#include "object-store.h"
#include <kj/async.h>
#include <array>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace coworkerd {
// Auxiliary discovery only. SQLite and the ordinary ownership path remain authoritative.
class AlarmPublication final: private kj::TaskSet::ErrorHandler {
public:
  using Current = std::function<bool(const std::string&, uint32_t, uint64_t, uint64_t)>;
  using Adopt = std::function<void(const std::string&, const std::string&, uint32_t, uint64_t,
      const std::string&, uint64_t)>;
  using Log = std::function<void(const std::string&, const std::string&)>;
  AlarmPublication(ObjectStore&, kj::Timer&, std::string, Current, Adopt, Log);
  ~AlarmPublication() noexcept(false);
  void setNodeEpoch(uint64_t);
  void take(std::string ns, uint32_t shard, uint64_t epoch, std::string tenant);
  void restore(std::string ns, uint32_t shard, std::string tenant);
  void gone(const std::string& ns, uint32_t shard);
  void report(std::string ns, std::string object, uint32_t shard, uint64_t epoch,
      std::string tenant, uint64_t at);
  void tick();
  std::string json() const;
  uint64_t hintWrites = 0, hintFailures = 0;
private:
  struct Shard {
    std::string ns, tenant, marker, previousPrefix;
    uint32_t shard = 0, failures = 0;
    uint64_t epoch = 0, memberEpoch = 0, markerMemberEpoch = 0, generation = 0, order = 0;
    uint64_t markerOrder = 0, markerGeneration = 0;
    bool adopted = false, reading = false, startupOnly = false, nonempty = false;
    bool markerWritten = false, markerMayExist = false, markerReading = false;
    double next = 0, checked = 0, markerNext = 0;
    std::map<std::string, uint64_t> desired, hints, duringRead;
    std::set<std::string> previousMarkers;
  };
  struct Write {
    std::string key, ns, object, tenant;
    uint32_t shard = 0, failures = 0;
    uint64_t generation = 0, version = 0, at = 0, order = 0;
    bool marker = false, mayExist = false;
    double next = 0;
  };
  struct Ready {
    std::string tenant;uint64_t order;std::string key;unsigned kind;
    bool operator<(const Ready& b) const {
      return std::tie(tenant, order, key, kind) < std::tie(b.tenant, b.order, b.key, b.kind);
    }
  };
  struct Metric {
    std::array<uint64_t, 512> bins{};
    uint64_t count = 0, maximum = 0;
    void add(uint64_t ns);
    double percentile(unsigned p) const;
  };
  struct Scope {
    AlarmPublication& owner;Metric* metric;uint64_t started;
    explicit Scope(AlarmPublication&, Metric* = nullptr);
    ~Scope();
  };
  std::string shardKey(const std::string&, uint32_t) const;
  bool valid(const Shard&) const;
  bool valid(const Write&) const;
  void unqueue(Write&);
  void refresh(Write&, double now);
  void offerRead(Shard&);
  void queueHint(Shard&, const std::string& object, uint64_t at);
  void queueMarker(Shard&, const std::string& key, bool present, bool mayExist = false);
  void reconcile(Shard&);
  void pump(unsigned limit = 1);
  void failure(const char* operation, bool denied = false);
  kj::Promise<void> write(std::string key);
  kj::Promise<bool> verifyOwner(std::string key, uint64_t generation);
  kj::Promise<void> readHints(std::string key);
  kj::Promise<void> scanMarkers(std::string key);
  kj::Promise<void> probeMarkers();
  uint32_t retry(uint32_t& failures);
  double hintCheckInterval() const;
  void taskFailed(kj::Exception&&) override;

  ObjectStore& store;kj::Timer& timer;std::string nodeId;Current current;Adopt adopt;Log log;
  uint64_t nodeEpoch = 0, serial = 0, nonemptyShards = 0, markerGeneration = 1;
  std::unordered_map<std::string, Shard> shards;
  std::unordered_map<std::string, Write> writes;
  // These survive gone()/setNodeEpoch(): a physical request owns its key until it returns.
  std::unordered_set<std::string> inFlightKeys;
  std::unordered_map<std::string, unsigned> inFlightHints;
  std::set<Ready> readyHints, readyMarkers;
  std::string lastTenant;
  uint32_t active = 0, activeHints = 0, activeMarkers = 0;
  uint64_t peakPending = 0, retries = 0, markerWrites = 0, markerDeletes = 0;
  uint64_t pages = 0, maxPage = 0, badHints = 0, markerDenials = 0, markerProbes = 0;
  uint64_t failuresToLog = 0, failureLogLines = 0, cpuNs = 0, cpuStarted = 0;
  unsigned cpuDepth = 0;
  double hintRoundMs = 3600000, nextHintCheck = 0, nextMarkerProbe = 0, nextFailureLog = 0;
  bool markerDenied = false, stopping = false;
  Metric reportMetric, completionMetric, tickMetric;
  kj::TaskSet tasks;
};
}  // namespace coworkerd

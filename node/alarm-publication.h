#pragma once
#include "object-store.h"
#include <kj/async.h>
#include <array>
#include <functional>
#include <list>
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
    uint64_t previousOrder = 0, repairOrder = 0, hintCommittedNs = 0;
    bool adopted = false, reading = false, startupOnly = false, nonempty = false, initial = true;
    bool markerWritten = false, markerMayExist = false, scanning = false, previousDone = false;
    bool previousPending = false, repairPending = false;
    bool readTimed = false, hintTimed = false, previousTimed = false, repairTimed = false;
    double next = 0, checked = 0, hintAt = 0, previousNext = 0, repairNext = 0, markerConfirmedAt = 0;
    std::map<std::string, uint64_t> desired, hints, duringRead;
    std::set<std::string> previousMarkers, writeKeys;
  };
  struct Write {
    std::string key, ns, object, tenant;
    uint32_t shard = 0, failures = 0;
    uint64_t generation = 0, version = 0, at = 0, order = 0;
    bool marker = false, mayExist = false, timed = false;
    double next = 0;
    std::list<std::string>::iterator sweep;
  };
  struct Ready {
    std::string tenant;uint64_t order;std::string key;unsigned kind;
    bool operator<(const Ready& b) const {
      return std::tie(tenant, order, key, kind) < std::tie(b.tenant, b.order, b.key, b.kind);
    }
  };
  struct Deadline {
    double at;unsigned kind;std::string key;uint64_t generation;
    bool operator<(const Deadline& b) const { return std::tie(at, kind, key) < std::tie(b.at, b.kind, b.key); }
  };
  struct Gate {
    uint32_t failures = 0;bool open = false, probing = false, disabled = false;double next = 0;
    bool available(double now) const;
    bool enter(double now);
    void result(bool success, bool probe, double now);
    void cancel(bool probe);
  };
  struct Metric {
    std::array<uint64_t, 1024> bins{};
    uint64_t count = 0, maximum = 0;
    void add(uint64_t ns);
    double percentile(unsigned p) const;
  };
  struct Scope {
    AlarmPublication& owner;Metric* metric;uint64_t started;
    explicit Scope(AlarmPublication&, Metric* = nullptr);
    ~Scope();
  };
  enum class Probe { LIST, PUT, DELETE };
  struct MarkerRef { std::string shard;uint64_t generation; };
  std::string shardKey(const std::string&, uint32_t) const;
  bool valid(const Shard&) const;
  bool valid(const Write&) const;
  bool deleting(const std::string&) const;
  void ownershipChanged(const std::string&, uint64_t);
  void unqueue(Write&);
  void eraseWrite(std::unordered_map<std::string, Write>::iterator);
  void refresh(Write&, double now);
  void offerRead(Shard&);
  void offerScan(Shard&, bool repair);
  void refreshActivity(Shard&);
  void wakeWrites(Shard&);
  void queueHint(Shard&, const std::string& object, uint64_t at);
  void queueMarker(Shard&, const std::string& key, bool present, bool mayExist = false);
  void reconcile(Shard&);
  void cleanup(Shard&);
  void acceptOwnMarker(Shard&, const std::string&);
  void pump(unsigned limit = 8);
  unsigned drainDeadlines(unsigned limit);
  void scheduleDeadlineDrain();
  void failure(const char* operation);
  void denied(Probe, std::string prefix = {});
  void markerRestored();
  kj::Promise<void> write(Write sent, bool probe, bool permissionProbe);
  kj::Promise<bool> verifyOwner(std::string key, uint64_t generation);
  kj::Promise<void> readHints(std::string key, bool probe);
  kj::Promise<void> scanPrevious(std::string key, bool repair, bool probe);
  kj::Promise<void> scanMarkers(bool probe);
  kj::Promise<void> probeMarkers();
  uint32_t retry(uint32_t& failures);
  double hintCheckInterval() const;
  void taskFailed(kj::Exception&&) override;

  ObjectStore& store;kj::Timer& timer;std::string nodeId;Current current;Adopt adopt;Log log;
  uint64_t nodeEpoch = 0, serial = 0, nonemptyShards = 0, retryRandom = 0;
  std::unordered_map<std::string, Shard> shards;
  std::unordered_map<std::string, Write> writes;
  std::list<std::string> writeSweep;
  std::list<std::string>::iterator sweepCursor = writeSweep.end();
  std::set<Deadline> deadlines;
  std::map<std::string, MarkerRef> ownMarkers;
  std::map<std::string, std::set<std::string>> observedOwn;
  std::set<std::string> deletedDuringScan, cleanupReady, markerPuts, markerRemoves;
  // These survive gone()/setNodeEpoch(): a physical client request owns its key until it returns.
  std::unordered_set<std::string> inFlightKeys, inFlightDeletes;
  std::unordered_map<std::string, unsigned> inFlightHints;
  std::set<Ready> readyHints, readyMarkers, readyScans;
  std::string lastTenant, deniedPrefix;
  Probe deniedOperation = Probe::LIST;
  Gate hintGate, markerGate;
  uint32_t active = 0, activeHints = 0, activeMarkers = 0, activeScans = 0;
  uint64_t peakPending = 0, retries = 0, markerWrites = 0, markerDeletes = 0;
  uint64_t pages = 0, maxPage = 0, badHints = 0, markerDenials = 0, markerProbes = 0, ownerChanges = 0;
  uint64_t markerPages = 0, failuresToLog = 0, failureLogLines = 0, cpuNs = 0, cpuStarted = 0;
  uint64_t threadCpuNs = 0, threadCpuStarted = 0, tickMaxItems = 0;
  unsigned cpuDepth = 0;
  double hintRoundMs = 3600000, nextMarkerScan = 0, nextMarkerProbe = 0, nextFailureLog = 0;
  bool markerDenied = false, stopping = false, globalScanRunning = false, initialMarkerScan = true, drainingDeadlines = false, permissionProbeActive = false;
  Metric reportMetric, completionMetric, tickMetric, deadlineMetric, markerLatencyMetric;
  kj::TaskSet tasks;
};
}  // namespace coworkerd

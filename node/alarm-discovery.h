#pragma once
#include "durable-shards.h"
#include "alarm-publication.h"
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <set>
#include <tuple>

namespace coworkerd {
// Markers are candidates only. All ownership decisions remain in DurableShards.
class AlarmDiscovery final: private kj::TaskSet::ErrorHandler {
public:
  using Take = std::function<kj::Promise<ShardRoute>(std::string, uint32_t, uint32_t)>;
  using Tenant = std::function<std::string(const std::string&)>;
  using Rank = std::function<uint32_t(const std::string&, uint32_t, const std::string&)>;
  struct Settings { std::string nodeId;std::filesystem::path pendingDirectory;uint32_t intervalMs = 60000, takeovers = 4, reads = 2; };
  AlarmDiscovery(Settings, ObjectStore&, kj::Timer&, const MembershipView&, AlarmPublication::ReadBudget&,
      std::function<bool()> ready, Take, Tenant, Rank, std::function<bool(const std::string&)> active,
      std::function<void(const std::string&, uint32_t, uint64_t)> adopt,
      std::function<bool(const std::string&, uint32_t)> forgetPending);
  ~AlarmDiscovery() noexcept(false);
  std::string json() const;
private:
  struct Marker { std::string node, ns;uint64_t member = 0, epoch = 0;uint32_t shard = 0; };
  struct Candidate {
    std::string ns, tenant;uint32_t shard = 0, failures = 0, waitedMs = 0, priority = 0;
    uint64_t version = 0, order = 0;double next = 0;bool active = false, localPending = false;
    std::optional<ShardRoute> owner;
    std::set<std::string> markers;
  };
  using Ready = std::tuple<uint32_t, std::string, uint64_t, std::string>;
  struct Scope { AlarmDiscovery& owner;uint64_t started = 0;explicit Scope(AlarmDiscovery&);~Scope(); };
  static std::optional<Marker> parse(const std::string&);
  bool living(const std::string&, uint64_t) const;
  void directory(std::string, bool urgent = false);
  void candidate(std::string, uint32_t, std::string marker = {}, bool local = false);
  void loadPending();
  void pump();
  void schedule(const std::string&, double);
  void erase(const std::string&);
  void cache(const std::string&, ShardRoute);
  void failed(const kj::Exception&);
  void taskFailed(kj::Exception&&) override;
  kj::Promise<void> loop();
  kj::Promise<ObjectListingPage> list(std::string, uint32_t, std::string = {}, std::string = {});
  kj::Promise<void> scan(std::string);
  kj::Promise<void> process(std::string);
  kj::Promise<bool> hints(std::string, uint32_t);
  kj::Promise<bool> sourcePresent(std::string);
  Settings settings;ObjectStore& store;kj::Timer& timer;const MembershipView& membership;
  AlarmPublication::ReadBudget& readBudget;
  std::function<bool()> ready;Take take;Tenant tenant;Rank rank;
  std::function<bool(const std::string&)> namespaceActive;
  std::function<void(const std::string&, uint32_t, uint64_t)> adopt;
  std::function<bool(const std::string&, uint32_t)> forgetPending;
  std::map<std::string, std::string> members;
  std::deque<std::string> directories;std::set<std::string> queuedDirectories;
  std::map<std::string, Candidate> candidates;
  std::set<Ready> runnable;std::set<std::pair<double, std::string>> delayed;
  std::map<std::pair<std::string, uint64_t>, std::set<std::string>> ownerWaiters;
  std::deque<std::pair<std::string, uint64_t>> invalidOwners;
  std::string lastTenant;
  uint64_t memberEpoch = 0, memberChanges = UINT64_MAX, serial = 0, discovered = 0, acquired = 0, forwarded = 0;
  uint64_t removed = 0, failures = 0, denials = 0, lists = 0, peakPending = 0, peakActive = 0, evicted = 0;
  uint64_t inactiveDropped = 0, deferred = 0, takeCalls = 0, takeNs = 0, cpuNs = 0;
  unsigned cpuDepth = 0;uint32_t active = 0, scans = 0, peakReads = 0;
  double nextRoot = 0;bool access = false, stopping = false, measureCpu = false, pumping = false;
  kj::TaskSet tasks;
};
}  // namespace coworkerd

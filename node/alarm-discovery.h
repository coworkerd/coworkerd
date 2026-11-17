#pragma once

#include "durable-shards.h"
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <set>
#include <tuple>

namespace coworkerd {
// Markers supply candidates only. DurableShards makes every ownership decision.
class AlarmDiscovery final: private kj::TaskSet::ErrorHandler {
public:
  using Take = std::function<kj::Promise<ShardRoute>(std::string, uint32_t)>;
  using Tenant = std::function<std::string(const std::string&)>;
  struct Settings {
    std::string nodeId;
    std::filesystem::path pendingDirectory;
    uint32_t intervalMs = 60000, takeovers = 4, reads = 2;
  };
  AlarmDiscovery(Settings, ObjectStore&, kj::Timer&, const MembershipView&, uint32_t& readActive,
      std::function<bool()> ready, Take, Tenant);
  ~AlarmDiscovery() noexcept(false);
  std::string json() const;
private:
  struct Marker {
    std::string node, ns;
    uint64_t member = 0, epoch = 0;
    uint32_t shard = 0;
  };
  struct Candidate {
    std::string ns, tenant;
    uint32_t shard = 0;
    uint64_t version = 0, order = 0;
    bool active = false;
    std::set<std::string> markers;
  };
  using Ready = std::tuple<std::string, uint64_t, std::string>;
  static std::optional<Marker> parse(const std::string&);
  bool living(const std::string&, uint64_t) const;
  void directory(std::string);
  void candidate(std::string, uint32_t, std::string marker = {});
  void loadPending();
  void finish(const std::string&, uint64_t, bool);
  void failed(const kj::Exception&);
  void taskFailed(kj::Exception&&) override;
  kj::Promise<void> loop();
  kj::Promise<ObjectListingPage> list(std::string, uint32_t, std::string = {}, std::string = {});
  kj::Promise<void> scan(std::string);
  kj::Promise<void> process(std::string);
  Settings settings;
  ObjectStore& store;
  kj::Timer& timer;
  const MembershipView& membership;
  uint32_t& readActive;
  std::function<bool()> ready;
  Take take;
  Tenant tenant;
  std::map<std::string, std::string> members;
  std::deque<std::string> directories;
  std::set<std::string> queuedDirectories;
  std::map<std::string, Candidate> candidates;
  std::set<Ready> runnable;
  std::multimap<double, std::string> delayed;
  std::string lastTenant;
  uint64_t memberEpoch = 0, serial = 0, discovered = 0, acquired = 0, forwarded = 0;
  uint64_t removed = 0, failures = 0, denials = 0, lists = 0, peakPending = 0, peakActive = 0;
  uint32_t active = 0, scans = 0, peakReads = 0;
  double nextRoot = 0, lastLoop = 0, maxLoopDelayMs = 0;
  bool access = false, stopping = false;
  kj::TaskSet tasks;
};
}  // namespace coworkerd

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace coworkerd {

// What the forked log writers' shared dictionary (common/log-cleaning.h) holds on
// this node, and which calls with secrets the node may start. A value goes into
// it before the first descriptor that carries it is built, and leaves it when
// nothing on this node can still print it, after a grace for output still
// queued. What holds a value:
//   - an admitted call, until the end of its reservation;
//   - a descriptor sent to a process generation for a plan revision, while that
//     generation lives and either its native snapshot lists the revision among
//     its resident isolates (loading and retired ones included), the snapshot
//     cannot be read (any other process: kept until the generation is gone), or
//     the call's deadline has not passed.
// So a value of the current view that no call here needs is not in it, a value
// that left the view while no isolate here had it is gone at once, and one that
// a still-running isolate holds stays until that isolate is gone.
//
// Two bounds. The dictionary's total (nodeBytes, nodePatterns) is the node's
// safety limit: a call whose values do not fit is refused here
// (SECRET_UNAVAILABLE: nothing executed, another node may take it). A tenant's
// earlier values -- held, no longer in its current view -- are bounded by
// tenantEarlierBytes: past it, that tenant's calls with secrets are refused here
// until the isolates that hold them are gone. A tenant's current values have no
// per-tenant bound (the deployment quotas bound them).
//
// Used on the node's event loop only.
class SecretDictionary {
public:
  struct Limits {
    size_t nodeBytes = size_t(16) << 20;
    size_t nodePatterns = 65536;
    size_t tenantEarlierBytes = size_t(1) << 20;
    double graceMs = 60000;
    // The native processes evict an isolate after this long without a call (0: never).
    double idleEvictMs = 300000;
    double compactEveryMs = 1000;
  };
  // The shared dictionary: append adds needles, replace rebuilds it as exactly
  // the given ones; both false when the region cannot take them.
  struct Sink {
    std::function<bool(const std::vector<std::string>&)> append, replace;
  };
  // One live process generation: whether its resident isolates are known (a
  // native snapshot was read), and their plan revisions.
  struct Residents {
    std::string processId;
    uint64_t generation = 0;
    bool known = false;
    std::set<std::string> revisions;
  };

  SecretDictionary(Limits limits, Sink sink);

  // The current view's secret values, by tenant.
  void setCurrent(std::map<std::string, std::set<std::string>> values);
  // Before a call of `tenant` that carries `values` starts here: nullopt (the
  // values are held until `untilMs`), or why it is refused on this node.
  std::optional<std::string> admit(const std::string& tenant, const std::set<std::string>& values,
      double nowMs, double untilMs);
  // A descriptor that carries `values` is about to go to (processId, generation)
  // for plan `revision`; `native`: a native server, whose isolates are idle-evicted.
  // False when the shared dictionary cannot hold them: the descriptor must not
  // be sent.
  bool sent(const std::string& tenant, const std::set<std::string>& values, const std::string& processId,
      uint64_t generation, const std::string& revision, bool native, double nowMs, double untilMs);
  // Releases what nothing has held for the grace, and rebuilds the shared
  // dictionary without it (at most every compactEveryMs).
  void collect(double nowMs, const std::vector<Residents>& residents);

  // Held values that are no longer current, for the in-process cleaning, and a
  // number that changes whenever they may have.
  std::vector<std::string> earlierValues() const;
  uint64_t earlierRevision() const { return revision; }
  bool holds(const std::string& value) const;
  size_t bytes() const { return liveBytes; }
  size_t earlierBytes(const std::string& tenant) const;
  std::string json(double nowMs) const;

private:
  struct Holder {
    std::string tenant;
    std::set<std::string> values;
    std::string processId;  // empty: the tenant's admitted calls
    uint64_t generation = 0;
    std::string revision;
    bool native = false;
    double untilMs = 0, lastMs = 0;
    std::optional<double> releasedMs;
  };
  using Key = std::tuple<std::string, std::string, uint64_t, std::string>;
  // Adds `values` to the holder; false (and nothing added) when they cannot be published.
  bool hold(Holder& holder, const std::set<std::string>& values, double nowMs);
  void unhold(const std::string& tenant, const std::string& value);
  bool pruneReservation(Holder& holder);
  bool publish(const std::vector<std::string>& needles, double nowMs);
  bool compact(double nowMs);
  std::string recovery(const std::string& tenant, double nowMs) const;
  void countRefusal(const std::string& tenant, bool full);

  Limits limits;
  Sink sink;
  std::map<std::string, std::set<std::string>> current;
  std::map<Key, Holder> holders;
  struct Held {
    uint32_t holders = 0;
    size_t bytes = 0;  // its needles' size in the shared dictionary
  };
  std::map<std::pair<std::string, std::string>, Held> held;  // (tenant, value)
  std::map<std::string, uint32_t> needleRefs;                     // needle -> held pairs needing it
  std::set<std::string> published;                                // in the shared dictionary, garbage included
  size_t liveBytes = 0, livePatterns = 0, publishedBytes = 0;
  double lastCompactMs = -1e300;
  uint64_t revision = 0, compactions = 0, refusedFull = 0, refusedEarlier = 0, sinkFailures = 0;
  std::map<std::string, std::pair<uint64_t, uint64_t>> refusedByTenant;  // (full, earlier), at most 256 tenants
};

}  // namespace coworkerd

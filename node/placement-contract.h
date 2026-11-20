#pragma once

#include "support.h"
#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace coworkerd {

enum class InitialPlacementAdmission { Allowed, Expired, Saturated };
inline InitialPlacementAdmission initialPlacementAdmission(double remainingLeaseMs, uint32_t activeRoots,
    uint32_t activeNested, bool reserved, bool root, uint32_t rootLimit, uint32_t nestedLimit) {
  if (remainingLeaseMs == 0) return InitialPlacementAdmission::Expired;
  if (activeRoots + activeNested - (reserved ? 1 : 0) >= (root ? rootLimit : nestedLimit))
    return InitialPlacementAdmission::Saturated;
  return InitialPlacementAdmission::Allowed;
}

inline uint32_t advancePlacementAvoidance(uint32_t& failures, uint32_t advertisedMs, bool connectionFailure,
    uint32_t connectBackoffMs, uint32_t avoidMaxMs) {
  auto delay = std::min(advertisedMs, avoidMaxMs);
  if (connectionFailure) {
    failures = std::min<uint32_t>(failures + 1, 32);
    delay = std::min<uint64_t>(uint64_t(connectBackoffMs) << (failures - 1), avoidMaxMs);
  } else failures = 0;
  return delay;
}

// Moderately hot versions can continue through yellow. Red still admits the
// ingress node's own root, but rejects forwarded roots and nested calls.
template <typename IsHot>
const char* placementBandRefusal(bool check, bool root, bool forwardedRoot, std::string_view band, IsHot isHot) {
  if (check && (!root || forwardedRoot) && (band == "red" || (band == "yellow" && !isHot()))) {
    return band == "red" ? (forwardedRoot ? "red band rejects forwarded roots" : "red band rejects nested invocations")
                        : "yellow band rejects cold version";
  }
  return nullptr;
}

inline bool consumePlacementHop(uint8_t& hops) {
  if (hops == 0) return false;
  --hops;
  return true;
}

inline const char* placementExhaustionReason(uint8_t hops) {
  return hops == 0 ? "placement hops exhausted" : "all candidates rejected or unavailable";
}

inline constexpr const char* PACKAGE_UNAVAILABLE = "package-unavailable";
inline bool placementRetryable(std::string_view code, std::string_view category) {
  return code != "RESULT_UNCERTAIN" && (code != "LOAD_FAILED" || category == PACKAGE_UNAVAILABLE);
}

template <typename Members>
std::vector<std::string> rankPlacementMembers(const std::string& worker, const Members& members,
    const std::string& selfId, bool lastResort) {
  std::vector<std::string> ids;
  bool self = false;
  for (const auto& [id, member] : members) {
    if (id == selfId && lastResort) self = true;
    else if (id == selfId || member.state != "draining") ids.push_back(id);
  }
  auto order = rendezvousOrder(worker, ids);
  if (self) order.push_back(selfId);
  return order;
}

inline std::vector<std::string> placementOrder(const std::vector<std::string>& ranked,
    const std::string& selfId, bool lastResort, const std::string& home, bool selfRanked) {
  std::vector<std::string> ordered;
  if (!home.empty()) ordered.push_back(home);
  if (!lastResort && home != selfId && selfRanked) ordered.push_back(selfId);
  for (const auto& id : ranked) if (id != home && id != selfId) ordered.push_back(id);
  if (lastResort && home != selfId && selfRanked) ordered.push_back(selfId);
  return ordered;
}

template <typename IsHot, typename IsAdmitted>
inline bool chooseHotLocalPlacement(bool lastResort, bool selfRanked, IsHot isHot, IsAdmitted isAdmitted) {
  return !lastResort && selfRanked && isHot() && isAdmitted();
}

inline void prioritizePlacementNode(std::vector<std::string>& ordered, const std::string& selfId) {
  ordered.erase(std::remove(ordered.begin(), ordered.end(), selfId), ordered.end());
  ordered.insert(ordered.begin(), selfId);
}

template <typename IsAvailable>
void orderByPlacementAvoidance(std::vector<std::string>& ordered, bool localFirst, IsAvailable isAvailable) {
  auto first = ordered.begin();
  if (localFirst && first != ordered.end()) ++first;
  std::stable_partition(first, ordered.end(), isAvailable);
}

// v138 section 2: a Worker saturated on a node (WORKER_SATURATED: its isolate there busy most of the last window)
// or a node whose CPU is full (OVERLOADED, category node-cpu-full) refuses a new call before anything runs; the
// call is placed on another node. Neither sets the node's avoidance (a saturated Worker says nothing about the
// node's other Workers); the caller remembers a saturated (node, Worker) for one window instead, and when every
// candidate refused for one of the two reasons, its own node takes the call anyway.
inline constexpr const char* WORKER_SATURATED = "WORKER_SATURATED";
inline constexpr const char* NODE_CPU_FULL = "node-cpu-full";
inline bool saturationRefusal(std::string_view code, std::string_view category) {
  return code == WORKER_SATURATED || (code == "OVERLOADED" && category == NODE_CPU_FULL);
}

// The caller's record of (node, Worker) pairs that answered WORKER_SATURATED, each until its window ends.
class SaturationMarks {
public:
  void mark(const std::string& node, const std::string& worker, double untilMs) {
    marks[{node, worker}] = untilMs;
    if (marks.size() > 4096) prune(untilMs - 1e9);
  }
  bool marked(const std::string& node, const std::string& worker, double nowMs) const {
    const auto found = marks.find({node, worker});
    return found != marks.end() && found->second > nowMs;
  }
  void prune(double nowMs) {
    for (auto at = marks.begin(); at != marks.end();) at = at->second <= nowMs ? marks.erase(at) : std::next(at);
  }
  size_t size() const { return marks.size(); }

private:
  std::map<std::pair<std::string, std::string>, double> marks;
};

// Candidates the Worker is known saturated on go last, the rest keep their order (a hot local version that stays
// first keeps its place).
template <typename IsMarked>
void orderBySaturation(std::vector<std::string>& ordered, bool localFirst, IsMarked isMarked) {
  auto first = ordered.begin();
  if (localFirst && first != ordered.end()) ++first;
  std::stable_partition(first, ordered.end(), [&](const std::string& id) { return !isMarked(id); });
}

// Whether the caller's own node takes the call despite saturation: every candidate was tried and every one refused
// for a saturated Worker or a full CPU, and this node is one of them ("nowhere to go": the call is slowed, not refused).
inline bool placeDespiteSaturation(size_t attempted, size_t candidates, bool allSaturation, bool selfCandidate) {
  return attempted > 0 && attempted == candidates && allSaturation && selfCandidate;
}

} // namespace coworkerd

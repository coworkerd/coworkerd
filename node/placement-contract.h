#pragma once

#include "support.h"
#include <algorithm>
#include <cstdint>
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

} // namespace coworkerd

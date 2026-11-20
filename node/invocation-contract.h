#pragma once

#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <ostream>
#include <string>

namespace coworkerd {

inline std::ostream& exactDeadlinePrecision(std::ostream& out) { return out << std::setprecision(17); }

inline double remainingLeaseMs(double deadline, double now) {
  const auto remaining = deadline - now;
  return remaining > 0 ? remaining : 0;
}

inline bool presentsLease(double value, double deadline, double originallyPresented) {
  return value == (originallyPresented > 0 ? originallyPresented : deadline);
}

struct InheritedLease {
  double deadline;
  const std::string& ingressServerId;
};

template<class Parent>
InheritedLease inheritLease(const Parent& parent) {
  return {parent.expiresAtMonotonicMs, parent.ingressServerId};
}

inline std::string invocationName(const std::string& node, const std::string& epoch, const char* kind,
    uint64_t sequence) {
  return node + "." + epoch + kind + std::to_string(sequence);
}

struct ChildBudget {
  uint16_t grant, charged, remaining;
};
// The caller has already rejected an empty budget in the correct mode. An
// allocation carries the resulting grant and charge through placement attempts.
inline ChildBudget childBudget(uint16_t before, uint16_t cap, bool naive) {
  if (naive) return {before, 0, before};
  const auto grant = std::min<uint16_t>(cap, static_cast<uint16_t>(before - 1));
  const auto charged = static_cast<uint16_t>(1 + grant);
  return {grant, charged, static_cast<uint16_t>(before - charged)};
}

struct TenantAdmissionCounts {
  uint32_t roots = 0, nested = 0;
  uint64_t rootRejections = 0, nestedRejections = 0;
  uint64_t rootBusyRejections = 0, nestedBusyRejections = 0;
  uint32_t& active(bool root) { return root ? roots : nested; }
  uint32_t active(bool root) const { return root ? roots : nested; }
  uint32_t borrowed(bool root, uint32_t limit) const {
    const auto count = active(root);
    return limit != 0 && count > limit ? count - limit : 0;
  }
  bool empty() const { return roots == 0 && nested == 0 && rootRejections == 0 && nestedRejections == 0; }
};

enum class TenantAdmission { Allowed, Borrowed, Refused };
inline TenantAdmission tenantAdmission(uint32_t count, uint32_t limit, bool reserved, uint32_t nodeRoots,
    uint32_t nodeNested, uint32_t nodeLimit, uint32_t borrowBelowPercent) {
  if (limit == 0 || reserved || count < limit) return TenantAdmission::Allowed;
  // Use the same shared occupancy as the node-wide placement guard. Compare before reserving without rounding.
  if ((uint64_t{nodeRoots} + nodeNested) * 100 < uint64_t{nodeLimit} * borrowBelowPercent)
    return TenantAdmission::Borrowed;
  return TenantAdmission::Refused;
}

// A Durable Object call nothing was sent for: SECRET_UNAVAILABLE when the node's secret dictionary could not take
// its values -- the code its admission gives when the values do not fit, whichever of the two finds it (v138
// section 4) -- and DO_UNAVAILABLE when the durable process is not ready.
inline const char* durableNotSentCode(bool secretDictionary) {
  return secretDictionary ? "SECRET_UNAVAILABLE" : "DO_UNAVAILABLE";
}

}  // namespace coworkerd

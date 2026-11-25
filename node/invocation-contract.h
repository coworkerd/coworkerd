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
  uint32_t grant, charged, remaining;
};
// The caller has already rejected an empty budget in the correct mode. An
// allocation carries the resulting grant and charge through placement attempts.
inline ChildBudget childBudget(uint32_t before, uint32_t cap, bool naive) {
  if (naive) return {before, 0, before};
  if (before == 0) return {};
  const auto grant = std::min(cap, before - 1);
  return {grant, 1 + grant, before - 1 - grant};
}

struct InvocationBudget {
  uint32_t grant, remaining;
  uint32_t d1Limit, d1Remaining;

  InvocationBudget(uint32_t grant, uint32_t d1Limit)
      : grant(grant), remaining(grant), d1Limit(d1Limit), d1Remaining(d1Limit) {}

  uint32_t childCap(bool root) const {
    // Keep room for wide root fan-out, and let children use most of their grant
    // on a deeper branch while still keeping some for siblings.
    return std::max(1u, root ? std::min(128u, grant / 128) : grant * 3 / 4);
  }

  void refill() {
    remaining = grant;
    d1Remaining = d1Limit;
  }
};

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
// its values -- the code its admission gives when the values do not fit, whichever of the two finds it -- and
// DO_UNAVAILABLE when the durable process is not ready.
inline const char* durableNotSentCode(bool secretDictionary) {
  return secretDictionary ? "SECRET_UNAVAILABLE" : "DO_UNAVAILABLE";
}

// The persistent threaded owner must reserve and validate its own alarm token.
inline bool acceptsForwardedAlarm(bool threadedDurableStorage, uint64_t alarmAtUnixMs) {
  return !threadedDurableStorage || alarmAtUnixMs == 0;
}

}  // namespace coworkerd

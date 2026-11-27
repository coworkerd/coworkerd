#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

namespace coworkerd {

inline constexpr uint32_t WORKER_BUSY_PERMILLE = 900;

using SaturatedPlans = std::map<std::string, double, std::less<>>;  // Plan revision to window end in monotonic ms.

SaturatedPlans parseIsolateSaturation(std::string_view snapshot);
bool hasSaturatedPlan(const SaturatedPlans& saturated, std::string_view planRevision, double nowMs);

}  // namespace coworkerd

#pragma once

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <limits>

namespace coworkerd {

inline uint32_t quotaCpuCount(uint64_t quota, uint64_t period) {
  if (!quota || !period) return 0;
  const auto count = quota / period + (quota % period != 0);
  return static_cast<uint32_t>(std::min<uint64_t>(count, UINT32_MAX));
}
inline uint32_t tenantExecutionShare(uint32_t cpus) {
  return std::max({cpus > 3 ? cpus - 3 : 0, cpus / 2 + cpus % 2, 1u});
}
inline bool tenantExecutionLimited(uint32_t own, uint32_t total, uint32_t limit) {
  return limit != 0 && total > own && own >= limit;
}

// The affinity count, narrowed by every visible ancestor's cgroup v2 quota.
// The explicit root makes the same production traversal testable with files.
uint32_t cpusWithinQuota(uint32_t affinity, std::filesystem::path directory,
    const std::filesystem::path& root);
uint32_t availableProcessCpus();

}  // namespace coworkerd

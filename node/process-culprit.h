#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>

namespace coworkerd {

// The current CPU watchdog evidence only matches a live dispatched invocation.
inline bool cpuCulpritMatches(std::string_view event, uint32_t reportedPid, uint32_t pid,
    uint32_t reportedLimit, uint32_t limit, uint64_t usedNs, bool dispatched,
    std::string_view reportedWorker, std::string_view worker,
    std::string_view reportedVersion, std::string_view version) {
  return limit && event == "culprit" && reportedPid == pid && reportedLimit == limit &&
      usedNs >= uint64_t(limit) * 1000000 && dispatched &&
      reportedWorker == worker && reportedVersion == version;
}

// Unlike CPU, the OOM row can match a completed dispatch in the same process generation.
template<class Calls, class Recent>
bool memoryCulpritDispatch(const Calls& live, const Recent& recent, const std::string& invocation,
    const std::string& worker, const std::string& version) {
  auto current = live.find(invocation);
  if (current != live.end() && current->second->dispatched &&
      current->second->workerId == worker && current->second->versionId == version) return true;
  return std::any_of(recent.begin(), recent.end(), [&](const auto& call) {
    return call.invocationId == invocation && call.workerId == worker && call.versionId == version;
  });
}

}  // namespace coworkerd

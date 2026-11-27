#pragma once

#include "cpu-capacity.h"
#include <deque>
#include <fstream>

namespace coworkerd {

inline constexpr uint32_t WORKER_BUSY_PERMILLE = 900;
inline constexpr uint32_t NODE_CPU_FULL_PERMILLE = 950;

class NodeCpuUse {
public:
  void sample(double nowMs) {
    if (stat.empty()) return;
    std::ifstream input(stat);
    std::string key;
    uint64_t usec;
    while (input >> key >> usec) {
      if (key != "usage_usec") continue;
      if (!samples.empty() && usec < samples.back().usageUsec) samples.clear();
      samples.push_back({nowMs, usec});
      while (samples.size() > 1 && samples.front().atMs < nowMs - 3000) {
        if (samples[1].atMs > nowMs - 3000) break;
        samples.pop_front();
      }
      return;
    }
    samples.clear();
  }

  bool full(double nowMs) const {
    if (samples.size() < 2 || nowMs - samples.back().atMs > 2000) return false;
    const auto& from = samples.front();
    const auto& to = samples.back();
    const auto elapsedMs = to.atMs - from.atMs;
    if (elapsedMs < 2000 || elapsedMs > 4500) return false;
    return double(to.usageUsec - from.usageUsec) >= elapsedMs * capacity * NODE_CPU_FULL_PERMILLE;
  }

private:
  static std::filesystem::path cpuStat() {
    auto group = processCpuCgroup();
    return group.empty() ? group : group / "cpu.stat";
  }
  const std::filesystem::path stat = cpuStat();
  const double capacity = availableProcessCpuCapacity();
  struct Sample { double atMs; uint64_t usageUsec; };
  std::deque<Sample> samples;
};

}  // namespace coworkerd

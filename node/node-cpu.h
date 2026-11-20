#pragma once

#include <cstdint>
#include <deque>
#include <fcntl.h>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <unistd.h>

namespace coworkerd {

// The node's CPU use over its last few seconds (v138 section 2): the usage of its cgroup (cgroup v2 cpu.stat
// usage_usec: the node and every process it started) against the CPUs it may use. Sampled once a second, never on a
// request; a node whose use reaches the full share (95 per cent by default) refuses new calls while another node
// may take them.
class NodeCpuUse {
public:
  NodeCpuUse(double windowMs, uint32_t cpus): windowMs(windowMs), cpus(cpus ? cpus : 1) {}
  void sample(double nowMs, uint64_t usageUsec) {
    samples.push_back({nowMs, usageUsec});
    while (samples.size() > 2 && samples[1].first <= nowMs - windowMs) samples.pop_front();
  }
  // The use over the window (0 to 1), 0 until two samples span at least half of it.
  double utilization() const {
    if (samples.size() < 2) return 0;
    const auto& [fromMs, fromUsec] = samples.front();
    const auto& [toMs, toUsec] = samples.back();
    if (toMs - fromMs < windowMs / 2 || toUsec < fromUsec) return 0;
    return double(toUsec - fromUsec) / ((toMs - fromMs) * 1000.0 * cpus);
  }
  // usage_usec of a cpu.stat text.
  static std::optional<uint64_t> usageIn(const std::string& text) {
    std::istringstream input(text);
    std::string key; uint64_t value;
    while (input >> key >> value) if (key == "usage_usec") return value;
    return std::nullopt;
  }
  // The cgroup cpu.stat of this process (the "0::<path>" line of /proc/self/cgroup).
  static std::string statPath() {
    std::ifstream input("/proc/self/cgroup");
    for (std::string line; std::getline(input, line);) {
      if (!line.starts_with("0::")) continue;
      const auto path = line.substr(3);
      return "/sys/fs/cgroup" + (path == "/" ? std::string() : path) + "/cpu.stat";
    }
    return "/sys/fs/cgroup/cpu.stat";
  }

private:
  const double windowMs;
  const uint32_t cpus;
  std::deque<std::pair<double, uint64_t>> samples;
};

}  // namespace coworkerd

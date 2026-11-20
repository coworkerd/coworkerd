#include "cpu-capacity.h"

#include <bit>
#include <cerrno>
#include <fstream>
#include <string>
#include <thread>
#include <vector>
#ifdef __linux__
#include <sched.h>
#endif

namespace coworkerd {

uint32_t cpusWithinQuota(uint32_t affinity, std::filesystem::path directory,
    const std::filesystem::path& root) {
  auto count = std::max(affinity, 1u);
  directory = directory.lexically_normal();
  const auto boundary = root.lexically_normal();
  for (;;) {
    const auto relative = directory.lexically_relative(boundary);
    if (directory != boundary && (relative.empty() || *relative.begin() == "..")) break;
    std::ifstream input(directory / "cpu.max");
    std::string quota;
    uint64_t period = 0;
    if (input >> quota >> period; input && quota != "max") {
      uint64_t value = 0;
      bool valid = !quota.empty();
      for (const auto c : quota) {
        if (c < '0' || c > '9' || value > (UINT64_MAX - (c - '0')) / 10) { valid = false; break; }
        value = value * 10 + c - '0';
      }
      if (const auto cap = valid ? quotaCpuCount(value, period) : 0) count = std::min(count, cap);
    }
    if (directory == boundary || directory == directory.parent_path()) break;
    directory = directory.parent_path();
  }
  return count;
}

uint32_t availableProcessCpus() {
  auto cpus = std::max(std::thread::hardware_concurrency(), 1u);
#ifdef __linux__
  // Grow the affinity mask for hosts with more than CPU_SETSIZE logical CPUs.
  for (size_t words = 16; words <= 16384; words *= 2) {
    std::vector<unsigned long> mask(words);
    if (sched_getaffinity(0, mask.size() * sizeof(mask[0]), reinterpret_cast<cpu_set_t*>(mask.data())) == 0) {
      cpus = 0;
      for (const auto word : mask) cpus += std::popcount(word);
      break;
    }
    if (errno != EINVAL) break;
  }
  const std::filesystem::path root = "/sys/fs/cgroup";
  auto directory = root;
  std::ifstream membership("/proc/self/cgroup");
  for (std::string line; std::getline(membership, line);) if (line.starts_with("0::/")) {
    const auto candidate = root / line.substr(4);
    if (std::filesystem::exists(candidate / "cpu.max")) directory = candidate;
  }
  cpus = cpusWithinQuota(cpus, directory, root);
#endif
  return std::max(cpus, 1u);
}

}  // namespace coworkerd

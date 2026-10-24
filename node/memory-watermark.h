#pragma once

#include <kj/async.h>
#include <kj/timer.h>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace coworkerd {

// The Server owns the signal. Children only load the published atomic word.
class MemoryWatermark final {
public:
  MemoryWatermark(bool enabled, uint64_t configuredBudget);
  ~MemoryWatermark();
  kj::Promise<void> run(kj::Timer& timer);
  void setTestBand(const std::string& value);
  std::string effectiveBand() const;
  std::string json() const;
  // The shared memory holding the published band (one 32-bit word), passed to each workerd at
  // startup and mapped read-only there; -1 while the watermark is off.
  int channelDescriptor() const;
  static uint64_t residentBytes(int pid);
  // The memory this node may use, and where that came from: its cgroup's memory.max (a container's limit), else the
  // configured budget, else the machine's physical memory. Rotation's default memory threshold is a share of it.
  static std::pair<uint64_t, std::string> nodeMemory(uint64_t configuredBudget);
  // Memory-triggered process rotation (no default threshold is set): true when the process's resident
  // memory reached the threshold. 0 disables it.
  static bool rotationDue(int pid, uint64_t thresholdBytes);
private:
  class Impl;
  std::unique_ptr<Impl> impl;
};

}  // namespace coworkerd

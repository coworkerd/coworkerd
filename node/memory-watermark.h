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
  // This process's smaps/task_info and allocator snapshot; unavailable measurements are null.
  // pid: Current daemon namespace PID; startTicks distinguishes PID reuse on Linux.
  // startTicks: Linux /proc/self/stat field 22; null on native macOS.
  // rssBytes: Current resident pages, read afresh from smaps_rollup or task_info.
  // pssBytes: Current proportional resident bytes from Linux smaps_rollup; otherwise null.
  // anonBytes: Current Linux Anonymous bytes; otherwise null.
  // swapBytes: Current Linux Swap bytes; otherwise null.
  // malloc.arena: Current glibc non-mmap heap space; the entire malloc object is null elsewhere.
  // malloc.uordblks: Current arena in-use bytes, separate from mmap allocations.
  // malloc.fordblks: Current arena free bytes; release depends on allocator geometry and trim.
  // malloc.hblkhd: Current live mmap allocation bytes; freed large allocations can return directly.
  // malloc.keepcost: Current releasable top chunk, not all free pages.
  // malloc.trims: Explicit calls since boot, limited to once a minute after a minute without DO appends.
  // malloc.policy.configured: Whether all three startup mallopt settings succeeded.
  // malloc.policy.trimThresholdBytes: Startup-fixed 128 KiB trim threshold.
  // malloc.policy.mmapThresholdBytes: Startup-fixed 128 KiB mmap threshold (disables dynamic growth).
  // malloc.policy.topPadBytes: Startup-fixed zero extra brk padding.
  // malloc.policy.idleTrimAfterMs: Fixed 60000 ms of no own or peer append before an idle trim.
  static std::string daemonJson(const std::string& containers);
  // Own or peer Durable Object append activity, for the daemon's idle allocator trim.
  static void noteDaemonAppend();
  // The shared memory holding the published band (one 32-bit word), passed to each workerd at
  // startup and mapped read-only there; -1 while the watermark is off.
  int channelDescriptor() const;
  static uint64_t residentBytes(int pid);
  // The memory this node may use, and where that came from: its cgroup's memory.max (a container's limit), else the
  // configured budget, else the machine's physical memory. Rotation's default memory threshold is a share of it.
  static std::pair<uint64_t, std::string> nodeMemory(uint64_t configuredBudget);
  // Memory-triggered process rotation (rotation.residentBytes, 70% of the node's memory by default since 0.4.1): true when the process's resident
  // memory reached the threshold. 0 disables it.
  static bool rotationDue(int pid, uint64_t thresholdBytes);
private:
  class Impl;
  std::unique_ptr<Impl> impl;
};

}  // namespace coworkerd

#include "memory-watermark.h"
#include <kj/debug.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <optional>
#include <cerrno>
#ifdef __linux__
#include <sys/prctl.h>
#endif
#ifdef __GLIBC__
#include <malloc.h>
#endif
#ifdef __APPLE__
#include <libproc.h>
#include <mach/mach.h>
#endif

namespace coworkerd {
namespace {
double nowMs() {
  return std::chrono::duration<double, std::milli>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
#ifdef __GLIBC__
constexpr int DAEMON_TRIM_BYTES = 128 * 1024;
constexpr double DAEMON_IDLE_TRIM_MS = 60000;
uint64_t daemonTrims = 0;  // Explicit malloc_trim calls since this daemon started; no history retained.
double daemonLastAppendMs = nowMs(), daemonLastTrimMs = daemonLastAppendMs;
bool daemonAllocatorConfigured = false;
#endif
std::optional<int> daemonThpDisableResult;  // Startup prctl return value; unavailable on non-Linux.
#ifdef __linux__
int daemonThpDisableErrno = 0;
#endif
constexpr std::array<const char*, 3> BANDS = {"green", "yellow", "red"};
uint32_t number(const std::string& value) {
  for (uint32_t i = 0; i < BANDS.size(); ++i) if (value == BANDS[i]) return i;
  KJ_FAIL_REQUIRE("invalid memory band");
}
bool readNumber(const std::string& path, uint64_t& value) {
  std::ifstream input(path);
  return bool(input >> value);
}
#ifdef __linux__
// This process's unified cgroup directory. Docker's private cgroup namespace exposes the container cgroup at the mount
// root; with a host namespace, our own cgroup is found relative to that mount.
std::string cgroupDirectory() {
  std::string cgroup = "/sys/fs/cgroup";
  std::ifstream membership("/proc/self/cgroup");
  std::string line;
  while (std::getline(membership, line)) if (line.starts_with("0::") && line.substr(3) != "/") {
    auto candidate = cgroup + line.substr(3);
    if (std::filesystem::exists(candidate + "/memory.current")) cgroup = candidate;
  }
  return cgroup;
}
#endif
}

class MemoryWatermark::Impl {
public:
  bool requested, enabled = false;
  uint64_t budget = 0, current = 0, inactiveFile = 0, workingSet = 0, samples = 0, changes = 0;
  uint32_t band = 0, testBand = 0;
  std::array<double, 3> durations{};
  double since = nowMs();
  std::string source = "none", cgroup = "/sys/fs/cgroup";
  uint32_t* shared = nullptr;
  int channel = -1;

  Impl(bool requested, uint64_t configuredBudget): requested(requested) {
#ifdef __linux__
    cgroup = cgroupDirectory();
    if (readNumber(cgroup + "/memory.max", budget)) source = "memory.max";
    else if (configuredBudget) { budget = configuredBudget; source = "memoryBudgetBytes"; }
    enabled = requested && budget && readNumber(cgroup + "/memory.current", current);
#endif
    if (enabled) {
#ifdef __linux__
      // An anonymous memory file: nothing on disk, and once sealed no process it is passed to can
      // resize it or map it writable (F_SEAL_FUTURE_WRITE, where the kernel has it).
      channel = memfd_create("coworkerd-memory-band", MFD_CLOEXEC | MFD_ALLOW_SEALING);
      KJ_REQUIRE(channel >= 0, "cannot create memory band channel");
      KJ_REQUIRE(ftruncate(channel, 4096) == 0, "cannot size memory band channel");
      auto mapping = mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, channel, 0);
      KJ_REQUIRE(mapping != MAP_FAILED, "cannot map memory band channel");
      shared = static_cast<uint32_t*>(mapping);
#ifdef F_SEAL_FUTURE_WRITE
      fcntl(channel, F_ADD_SEALS, F_SEAL_FUTURE_WRITE);
#endif
      fcntl(channel, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL);
      __atomic_store_n(shared, 0, __ATOMIC_RELEASE);
      sample();
#endif
    }
  }
  ~Impl() {
    if (shared) munmap(shared, 4096);
    if (channel >= 0) close(channel);
  }
  uint32_t effective() const { return std::max(band, testBand); }
  void publish() { if (shared) __atomic_store_n(shared, effective(), __ATOMIC_RELEASE); }
  void sample() {
    uint64_t nextCurrent, nextInactive = 0;
    std::ifstream stat(cgroup + "/memory.stat");
    std::string key; uint64_t value; bool found = false;
    while (stat >> key >> value) if (key == "inactive_file") { nextInactive = value; found = true; }
    // Retain the last valid signal on a read failure, rather than silently going green.
    if (!found || !readNumber(cgroup + "/memory.current", nextCurrent)) return;
    current = nextCurrent; inactiveFile = nextInactive;
    workingSet = current - std::min(current, inactiveFile); ++samples;
    auto previous = band;
    const long double fraction = static_cast<long double>(workingSet) / budget;
    if (fraction >= .90L) band = 2;
    else if (fraction < .65L) band = 0;
    else if (fraction >= .75L) band = std::max(band, 1u);
    else if (band == 2) band = 1;
    if (band != previous) {
      auto now = nowMs(); durations[previous] += now - since; since = now; ++changes;
      std::cout << "[memory-band] {\"from\":\"" << BANDS[previous] << "\",\"band\":\"" << BANDS[band]
        << "\",\"workingSetBytes\":" << workingSet << ",\"budgetBytes\":" << budget
        << ",\"monotonicMs\":" << now << "}" << std::endl;
      publish();
    }
  }
};

void MemoryWatermark::initializeDaemonThp() {
#ifdef __linux__
  // Disable THP before daemon allocations and forks; workerd's launcher restores the host default.
  daemonThpDisableResult = prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0);
  daemonThpDisableErrno = *daemonThpDisableResult == 0 ? 0 : errno;
#endif
}

MemoryWatermark::MemoryWatermark(bool enabled, uint64_t budget)
    : impl(std::make_unique<Impl>(enabled, budget)) {
#ifdef __linux__
  if (daemonThpDisableResult) {
    std::cout << "[memory-policy] {\"thpDisableResult\":" << *daemonThpDisableResult
        << ",\"errno\":" << daemonThpDisableErrno << "}" << std::endl;
  }
#endif
#ifdef __GLIBC__
  // Prevent large temporary segment/state buffers from raising glibc's adaptive mmap/trim thresholds indefinitely.
  // This configures only this node; exec'ed workerd processes initialize their own allocator.
  const bool trim = mallopt(M_TRIM_THRESHOLD, DAEMON_TRIM_BYTES) != 0;
  const bool pad = mallopt(M_TOP_PAD, 0) != 0;
  const bool mmap = mallopt(M_MMAP_THRESHOLD, DAEMON_TRIM_BYTES) != 0;
  daemonAllocatorConfigured = trim && pad && mmap;
  daemonLastAppendMs = daemonLastTrimMs = nowMs();
#endif
}
MemoryWatermark::~MemoryWatermark() = default;
kj::Promise<void> MemoryWatermark::run(kj::Timer& timer) {
  while (true) {
    co_await timer.afterDelay(100 * kj::MILLISECONDS);
    if (impl->enabled) impl->sample();
#ifdef __GLIBC__
    const auto now = nowMs();
    // No more than once a minute, and only after a full minute with neither own nor peer DO appends.
    // malloc_trim releases free pages only; it never removes a log record or changes an acknowledgement.
    if (now - daemonLastAppendMs >= DAEMON_IDLE_TRIM_MS && now - daemonLastTrimMs >= DAEMON_IDLE_TRIM_MS) {
      daemonLastTrimMs = now;
      malloc_trim(0);
      ++daemonTrims;
    }
#endif
  }
}
void MemoryWatermark::noteDaemonAppend() {
#ifdef __GLIBC__
  daemonLastAppendMs = nowMs();
#endif
}
void MemoryWatermark::setTestBand(const std::string& value) { impl->testBand = number(value); impl->publish(); }
std::string MemoryWatermark::effectiveBand() const { return BANDS[impl->effective()]; }
int MemoryWatermark::channelDescriptor() const { return impl->channel; }
std::string MemoryWatermark::json() const {
  const auto& s = *impl;
  auto durations = s.durations; durations[s.band] += nowMs() - s.since;
  std::ostringstream out;
  out << "{\"enabled\":" << (s.enabled ? "true" : "false") << ",\"requested\":" << (s.requested ? "true" : "false")
    << ",\"workingSetBytes\":" << s.workingSet << ",\"currentBytes\":" << s.current
    << ",\"inactiveFileBytes\":" << s.inactiveFile << ",\"budgetBytes\":" << s.budget
    << ",\"budgetSource\":\"" << s.source << "\",\"band\":\"" << BANDS[s.band]
    << "\",\"effectiveBand\":\"" << BANDS[s.effective()] << "\",\"changes\":" << s.changes
    << ",\"samples\":" << s.samples << ",\"sampleIntervalMs\":100"
    << ",\"thresholds\":{\"yellow\":0.75,\"red\":0.90,\"greenBelow\":0.65,\"redBelow\":0.75}"
    << ",\"durationMs\":{\"green\":" << durations[0] << ",\"yellow\":" << durations[1] << ",\"red\":" << durations[2] << "}}";
  return out.str();
}
uint64_t MemoryWatermark::residentBytes(int pid) {
#ifdef __linux__
  if (pid <= 0) return 0;
  uint64_t pages, resident;
  std::ifstream input("/proc/" + std::to_string(pid) + "/statm");
  if (input >> pages >> resident) return resident * uint64_t(sysconf(_SC_PAGESIZE));
#elif defined(__APPLE__)
  if (pid <= 0) return 0;
  proc_taskinfo info{};
  if (proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &info, sizeof(info)) == sizeof(info))
    return info.pti_resident_size;
#endif
  return 0;
}
bool MemoryWatermark::rotationDue(int pid, uint64_t thresholdBytes) {
  return thresholdBytes > 0 && pid > 0 && residentBytes(pid) >= thresholdBytes;
}
std::pair<uint64_t, std::string> MemoryWatermark::nodeMemory(uint64_t configuredBudget) {
  uint64_t bytes = 0;
#ifdef __linux__
  // memory.max reads "max" without a limit, which is not a number.
  if (readNumber(cgroupDirectory() + "/memory.max", bytes) && bytes > 0) return {bytes, "memory.max"};
#endif
  if (configuredBudget) return {configuredBudget, "memoryBudgetBytes"};
  const auto pages = sysconf(_SC_PHYS_PAGES), page = sysconf(_SC_PAGESIZE);
  bytes = pages > 0 && page > 0 ? uint64_t(pages) * uint64_t(page) : 0;
  return {bytes, "physical memory"};
}

std::string MemoryWatermark::daemonJson(const std::string& containers) {
#ifdef __GLIBC__
  // Sample before formatting: arena is non-mmap space; uordblks/fordblks are its in-use/free bytes,
  // hblkhd is live mmap space, and keepcost is the top-most releasable block (not all free space).
  const auto allocated = mallinfo2();
#endif
  std::optional<uint64_t> rss, pss, anon, anonHuge, swap, startTicks;
#ifdef __linux__
  // Kernel-resident/private accounting, read afresh; no history retained. A failed/missing reading stays null.
  std::ifstream smaps("/proc/self/smaps_rollup");
  std::string line;
  while (std::getline(smaps, line)) {
    std::istringstream in(line);
    std::string key, unit; uint64_t kib;
    if (!(in >> key >> kib >> unit) || unit != "kB") continue;
    if (key == "Rss:") rss = kib * 1024;
    else if (key == "Pss:") pss = kib * 1024;
    else if (key == "Anonymous:") anon = kib * 1024;
    else if (key == "AnonHugePages:") anonHuge = kib * 1024;  // Kernel huge-page bytes, sampled afresh.
    else if (key == "Swap:") swap = kib * 1024;
  }
  std::ifstream stat("/proc/self/stat");
  if (std::getline(stat, line)) {
    // comm can contain spaces and ')'; field 22 follows the final ')'.
    const auto end = line.rfind(')');
    if (end != std::string::npos) {
      std::istringstream fields(line.substr(end + 1)); std::string field;
      for (unsigned number = 3; number <= 22 && fields >> field; ++number)
        if (number == 22) startTicks = std::stoull(field);
    }
  }
#elif defined(__APPLE__)
  // task_info is this process's RSS; PSS/Anonymous/Swap and Linux start ticks have no equivalent here.
  mach_task_basic_info_data_t info{}; mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) == KERN_SUCCESS)
    rss = info.resident_size;
#endif
  const auto value = [](const auto& n) { return n ? std::to_string(*n) : std::string("null"); };
  const char* thpDisabled = !daemonThpDisableResult ? "null" : *daemonThpDisableResult == 0 ? "true" : "false";
  std::ostringstream out;
  out << "{\"pid\":" << getpid() << ",\"startTicks\":" << value(startTicks)
      << ",\"rssBytes\":" << value(rss) << ",\"pssBytes\":" << value(pss)
      << ",\"anonBytes\":" << value(anon) << ",\"anonHugeBytes\":" << value(anonHuge) << ",\"swapBytes\":" << value(swap) << ",\"malloc\":";
#ifdef __GLIBC__
  out << "{\"arena\":" << allocated.arena << ",\"uordblks\":" << allocated.uordblks
      << ",\"fordblks\":" << allocated.fordblks << ",\"hblkhd\":" << allocated.hblkhd
      << ",\"keepcost\":" << allocated.keepcost << ",\"trims\":" << daemonTrims
      << ",\"policy\":{\"thpDisabled\":" << thpDisabled
      << ",\"configured\":" << (daemonAllocatorConfigured ? "true" : "false")
      << ",\"trimThresholdBytes\":" << DAEMON_TRIM_BYTES  // Startup-fixed trim threshold.
      << ",\"mmapThresholdBytes\":" << DAEMON_TRIM_BYTES  // Large temporary buffers are returned via munmap.
      << ",\"topPadBytes\":0,\"idleTrimAfterMs\":60000}}";  // No top padding; at most one idle trim per minute.
#else
  out << "{\"arena\":null,\"uordblks\":null,\"fordblks\":null,\"hblkhd\":null,\"keepcost\":null,\"trims\":null,\"policy\":{\"thpDisabled\":"
      << thpDisabled << "}}";
#endif
  out << ",\"containers\":" << containers << '}';
  return out.str();
}

}  // namespace coworkerd

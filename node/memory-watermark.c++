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
#ifdef __APPLE__
#include <libproc.h>
#endif

namespace coworkerd {
namespace {
double nowMs() {
  return std::chrono::duration<double, std::milli>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
constexpr std::array<const char*, 3> BANDS = {"green", "yellow", "red"};
uint32_t number(const std::string& value) {
  for (uint32_t i = 0; i < BANDS.size(); ++i) if (value == BANDS[i]) return i;
  KJ_FAIL_REQUIRE("invalid memory band");
}
bool readNumber(const std::string& path, uint64_t& value) {
  std::ifstream input(path);
  return bool(input >> value);
}
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
    // Docker's private cgroup namespace exposes the container cgroup at the mount root.
    // With a host namespace, locate our own unified cgroup relative to that mount.
    std::ifstream membership("/proc/self/cgroup");
    std::string line;
    while (std::getline(membership, line)) if (line.starts_with("0::") && line.substr(3) != "/") {
      auto candidate = cgroup + line.substr(3);
      if (std::filesystem::exists(candidate + "/memory.current")) cgroup = candidate;
    }
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

MemoryWatermark::MemoryWatermark(bool enabled, uint64_t budget)
    : impl(std::make_unique<Impl>(enabled, budget)) {}
MemoryWatermark::~MemoryWatermark() = default;
kj::Promise<void> MemoryWatermark::run(kj::Timer& timer) {
  while (true) {
    co_await timer.afterDelay(100 * kj::MILLISECONDS);
    if (impl->enabled) impl->sample();
  }
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
}  // namespace coworkerd

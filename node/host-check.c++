#include "host-check.h"
#include "support.h"
#include <kj/debug.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <spawn.h>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <type_traits>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/prctl.h>
#else
#include <mach/mach.h>
#endif

extern char** environ;
namespace coworkerd {
namespace {
constexpr uint64_t MIB = 1u << 20;
constexpr uint32_t MAX_MIB = 4096, REPORT_MAGIC = 0x686f7374;
constexpr double ROUND_TIMEOUT_MS = 30000;
constexpr size_t LARGEST = 8;
uint64_t unixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}
double monotonicMs() {
  timespec t;
  if (clock_gettime(CLOCK_MONOTONIC, &t) != 0) throw std::system_error(errno, std::generic_category(), "host clock");
  return double(t.tv_sec) * 1000 + double(t.tv_nsec) / 1e6;
}
struct Cpu { double user, system; };
Cpu threadCpu() {
#ifdef __linux__
  rusage r;
  if (getrusage(RUSAGE_THREAD, &r) != 0) throw std::system_error(errno, std::generic_category(), "host thread CPU");
  return {double(r.ru_utime.tv_sec) * 1000 + r.ru_utime.tv_usec / 1000.0,
          double(r.ru_stime.tv_sec) * 1000 + r.ru_stime.tv_usec / 1000.0};
#else
  thread_basic_info_data_t info;
  mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
  const auto thread = mach_thread_self();
  const auto error = thread_info(thread, THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&info), &count);
  mach_port_deallocate(mach_task_self(), thread);
  if (error != KERN_SUCCESS) throw std::runtime_error("cannot read host thread CPU");
  return {double(info.user_time.seconds) * 1000 + info.user_time.microseconds / 1000.0,
          double(info.system_time.seconds) * 1000 + info.system_time.microseconds / 1000.0};
#endif
}
struct Chunk {
  double wallMs, userMs, systemMs;
  uint64_t observedAtUnixMs;
  uint32_t chunkIndex;
};
// Private, fixed-size, same-executable pipe record. No RPC or node-to-node schema.
struct Report {
  uint32_t magic, requestedMiB, touchedMiB, pageBytes, count5, count20, count100, largestCount;
  uint64_t startedAtUnixMs, finishedAtUnixMs, lastStallAtUnixMs;
  double wallMs, userMs, systemMs, stalledMsTotal;
  Chunk largest[LARGEST];
  char error[256];
};
static_assert(std::is_trivially_copyable_v<Report>);
Report measure(uint32_t mib) {
  Report r;
  std::memset(&r, 0, sizeof r);
  r.magic = REPORT_MAGIC; r.requestedMiB = mib;
  r.pageBytes = static_cast<uint32_t>(sysconf(_SC_PAGESIZE));
  r.startedAtUnixMs = unixMs();
  void* region = MAP_FAILED;
  try {
    const auto cpu = threadCpu();
    const auto start = monotonicMs();
    region = mmap(nullptr, size_t(mib) * MIB, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (region == MAP_FAILED) throw std::system_error(errno, std::generic_category(), "host anonymous memory");
    auto* bytes = static_cast<volatile unsigned char*>(region);
    for (uint32_t mb = 0; mb < mib; ++mb) {
      const auto before = threadCpu();
      const auto at = monotonicMs();
      // Same 1 MiB / 256 writes as first-touch-probe.py on a 4 KiB Linux page.
      // A larger native page is written more than once, never skipped.
      for (size_t offset = size_t(mb) * MIB; offset < size_t(mb + 1) * MIB; offset += 4096) bytes[offset] = 1;
      const auto elapsed = monotonicMs() - at;
      const auto after = threadCpu();
      const Chunk chunk{elapsed, after.user - before.user, after.system - before.system, unixMs(), mb};
      ++r.touchedMiB;
      if (elapsed >= 5) { ++r.count5; r.stalledMsTotal += elapsed; }
      if (elapsed >= 20) { ++r.count20; r.lastStallAtUnixMs = chunk.observedAtUnixMs; }
      if (elapsed >= 100) ++r.count100;
      size_t index = 0;
      while (index < r.largestCount && r.largest[index].wallMs >= elapsed) ++index;
      if (index < LARGEST) {
        for (size_t j = std::min<size_t>(r.largestCount, LARGEST - 1); j > index; --j) r.largest[j] = r.largest[j - 1];
        r.largest[index] = chunk;
        r.largestCount = std::min<uint32_t>(r.largestCount + 1, LARGEST);
      }
    }
    munmap(region, size_t(mib) * MIB); region = MAP_FAILED;
    r.wallMs = monotonicMs() - start;
    const auto after = threadCpu();
    r.userMs = after.user - cpu.user; r.systemMs = after.system - cpu.system;
  } catch (const std::exception& error) {
    if (region != MAP_FAILED) munmap(region, size_t(mib) * MIB);
    std::snprintf(r.error, sizeof r.error, "%s", error.what());
  }
  r.finishedAtUnixMs = unixMs();
  return r;
}
struct Balloon { std::string json; bool reporting = false; };
Balloon balloon() {
#ifndef __linux__
  return {"{\"status\":\"non-linux\",\"freePageReporting\":false,\"devices\":[]}"};
#else
  const std::filesystem::path base = "/sys/bus/virtio/drivers/virtio_balloon";
  try {
    if (!std::filesystem::exists(base))
      return {"{\"status\":\"driver-missing\",\"freePageReporting\":false,\"devices\":[]}"};
    std::vector<std::filesystem::path> paths;
    for (const auto& entry : std::filesystem::directory_iterator(base)) {
      const auto name = entry.path().filename().string();
      if (name.starts_with("virtio") && name.size() > 6 &&
          std::all_of(name.begin() + 6, name.end(), [](char c) { return c >= '0' && c <= '9'; })) paths.push_back(entry.path());
    }
    std::sort(paths.begin(), paths.end());
    bool reporting = false, complete = true;
    std::ostringstream devices;
    size_t i = 0;
    for (const auto& path : paths) {
      if (i++) devices << ',';
      std::ifstream input(path / "features");
      std::string bits; input >> bits;
      const bool readable = input && bits.size() > 5 && std::all_of(bits.begin(), bits.end(), [](char c) { return c == '0' || c == '1'; });
      const bool enabled = readable && bits[5] == '1';
      complete = complete && readable; reporting = reporting || enabled;
      devices << "{\"device\":\"" << jsonEscape(path.filename().string()) << "\",\"featureBits\":"
          << (readable ? "\"" + bits + "\"" : "null") << ",\"freePageReporting\":"
          << (readable ? (enabled ? "true" : "false") : "null") << '}';
    }
    return {"{\"status\":\"" + std::string(!complete ? "read-error" : paths.empty() ? "no-devices" : "detected") +
        "\",\"freePageReporting\":" + (reporting ? "true" : complete ? "false" : "null") + ",\"devices\":[" + devices.str() + "]}", reporting};
  } catch (const std::exception&) {
    return {"{\"status\":\"read-error\",\"freePageReporting\":null,\"devices\":[]}"};
  }
#endif
}
std::string reportJson(const Report& r) {
  std::ostringstream out;
  out << std::setprecision(17) << "{\"ok\":" << (r.error[0] ? "false" : "true")
      << ",\"requestedMiB\":" << r.requestedMiB << ",\"touchedMiB\":" << r.touchedMiB
      << ",\"chunkMiB\":1,\"pageBytes\":" << r.pageBytes
      << ",\"chunksAtLeast5ms\":" << r.count5 << ",\"chunksAtLeast20ms\":" << r.count20
      << ",\"chunksAtLeast100ms\":" << r.count100 << ",\"stalledMsTotal\":" << r.stalledMsTotal
      << ",\"wallMs\":" << r.wallMs << ",\"userMs\":" << r.userMs << ",\"systemMs\":" << r.systemMs
      << ",\"maxChunkWallMs\":" << (r.largestCount ? r.largest[0].wallMs : 0)
      << ",\"maxChunkSystemMs\":" << (r.largestCount ? r.largest[0].systemMs : 0)
      << ",\"startedAtUnixMs\":" << r.startedAtUnixMs << ",\"finishedAtUnixMs\":" << r.finishedAtUnixMs
      << ",\"lastStallAtUnixMs\":" << r.lastStallAtUnixMs << ",\"largest\":[";
  for (size_t i = 0; i < r.largestCount; ++i) {
    if (i) out << ',';
    const auto& c = r.largest[i];
    out << "{\"chunkIndex\":" << c.chunkIndex << ",\"wallMs\":" << c.wallMs << ",\"userMs\":" << c.userMs
        << ",\"systemMs\":" << c.systemMs << ",\"observedAtUnixMs\":" << c.observedAtUnixMs << '}';
  }
  out << "],\"error\":\"" << jsonEscape(r.error) << "\"}";
  return out.str();
}
uint32_t integer(const char* text, uint32_t maximum) {
  uint32_t value = 0;
  const auto end = text + std::strlen(text);
  const auto parsed = std::from_chars(text, end, value);
  if (parsed.ec != std::errc() || parsed.ptr != end || value == 0 || value > maximum)
    throw std::invalid_argument("host check needs an integer MiB size from 1 to 4096");
  return value;
}
void closeFd(int& fd) { if (fd >= 0) { ::close(fd); fd = -1; } }
int raised(int fd) {
  const auto moved = fcntl(fd, F_DUPFD_CLOEXEC, 11);
  const auto error = errno;
  ::close(fd);
  if (moved < 0) throw std::system_error(error, std::generic_category(), "host check descriptor");
  return moved;
}
void makePipe(int& read, int& write) {
  int fds[2];
  if (pipe(fds) != 0) throw std::system_error(errno, std::generic_category(), "host check pipe");
  // Both sources are above all fixed workerd destinations, including descriptor 10.
  fcntl(fds[0], F_SETFD, FD_CLOEXEC); fcntl(fds[1], F_SETFD, FD_CLOEXEC);
  try { read = raised(fds[0]); } catch (...) { ::close(fds[1]); throw; }
  try { write = raised(fds[1]); } catch (...) { closeFd(read); throw; }
}
}  // namespace

int runHostCheck(int argc, char** argv) {
  try {
    if (argc > 1) throw std::invalid_argument("usage: coworkerd check host [MiB]");
    const auto mib = argc ? integer(argv[0], MAX_MIB) : 256;
    const auto r = measure(mib);
    auto json = reportJson(r); json.pop_back();
    std::cout << json << ",\"balloon\":" << balloon().json << '}' << std::endl;
    return r.error[0] ? 2 : r.count20 ? 1 : 0;
  } catch (const std::exception& error) {
    std::cout << "{\"ok\":false,\"error\":\"" << jsonEscape(error.what()) << "\"}" << std::endl;
    return 2;
  }
}
int runHostCheckChild(int argc, char** argv) {
  if (argc != 2) return 2;
  try {
    const auto mib = integer(argv[0], MAX_MIB);
    const auto parent = integer(argv[1], INT32_MAX);
#ifdef __linux__
    // Register before touching the sample, then check the spawn/exec/death race.
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || getppid() != static_cast<pid_t>(parent)) return 2;
#else
    if (getppid() != static_cast<pid_t>(parent)) return 2;
    std::thread([] { char byte; while (::read(STDIN_FILENO, &byte, 1) < 0 && errno == EINTR) {} _exit(0); }).detach();
#endif
    const auto report = measure(mib);
    const char* bytes = reinterpret_cast<const char*>(&report);
    size_t offset = 0;
    while (offset < sizeof report) {
      const auto n = ::write(STDOUT_FILENO, bytes + offset, sizeof report - offset);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) return 2;
      offset += size_t(n);
    }
    return report.error[0] ? 2 : report.count20 ? 1 : 0;
  } catch (...) { return 2; }
}

class HostCheck::Impl {
public:
  HostCheckConfig config;
  std::function<std::string()> memoryBand;
  Balloon detected = balloon();
  pid_t pid = 0;
  int result = -1, parentWrite = -1;
  std::array<char, sizeof(Report)> buffer{};
  size_t received = 0;
  bool exposure = false, startup = true, available = false, timedOut = false;
  double nextRound = 0, spawnedAt = 0;
  uint64_t rounds = 0, skipped = 0, errors = 0, touched = 0, count20 = 0, count100 = 0, lastStall = 0;
  double maximumWall = 0, maximumSystem = 0;
  std::string lastResult = "null", lastError;
  Impl(HostCheckConfig config, std::function<std::string()> band): config(config), memoryBand(std::move(band)) {}
  ~Impl() {
    closeFd(parentWrite); closeFd(result);
    if (pid > 0) {
      kill(pid, SIGKILL);
      while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {}
    }
  }
  std::string json() const {
    std::ostringstream out;
    out << std::setprecision(17) << "{\"enabled\":" << (config.intervalMs ? "true" : "false")
        << ",\"available\":" << (available ? "true" : "false")
        << ",\"intervalMs\":" << config.intervalMs << ",\"sampleMiB\":" << config.sampleMiB
        << ",\"rounds\":" << rounds << ",\"skippedRounds\":" << skipped << ",\"errors\":" << errors
        << ",\"touchedMiB\":" << touched << ",\"chunksAtLeast20ms\":" << count20 << ",\"chunksAtLeast100ms\":" << count100
        << ",\"maxChunkWallMs\":" << maximumWall << ",\"maxChunkSystemMs\":" << maximumSystem
        << ",\"lastStallAtUnixMs\":" << lastStall << ",\"pid\":" << pid
        << ",\"lastError\":\"" << jsonEscape(lastError) << "\",\"lastResult\":" << lastResult
        << ",\"balloon\":" << detected.json << '}';
    return out.str();
  }
  void log(bool slow = false) {
    if (!startup && !slow) return;
    std::cout << "[host-check] {\"event\":\"" << (startup ? "startup" : "result")
        << "\",\"observedAtUnixMs\":" << unixMs() << ",\"host\":" << json();
    if (startup && (detected.reporting || slow))
      std::cout << ",\"warning\":\"Host memory first touches may stall and be charged as system CPU. Run coworkerd check host; "
          "ask the operator to disable host free-page reclaim or reserve guest memory (see the installation guide).\"";
    std::cout << '}' << std::endl;
    startup = false;
  }
  void failed(const std::string& reason) { ++errors; available = false; lastError = reason; log(); }
  void spawn() {
    int read = -1, write = -1, parentRead = -1;
    auto cleanup = kj::defer([&] { closeFd(read); closeFd(write); closeFd(parentRead); });
    makePipe(read, write); makePipe(parentRead, parentWrite);
    posix_spawn_file_actions_t actions;
    auto check = [](int error) { if (error) throw std::system_error(error, std::generic_category(), "host check spawn"); };
    check(posix_spawn_file_actions_init(&actions));
    auto destroyActions = kj::defer([&] { posix_spawn_file_actions_destroy(&actions); });
    check(posix_spawn_file_actions_adddup2(&actions, parentRead, STDIN_FILENO));
    check(posix_spawn_file_actions_adddup2(&actions, write, STDOUT_FILENO));
    check(posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, "/dev/null", O_WRONLY, 0));
#ifdef __linux__
    check(posix_spawn_file_actions_addclosefrom_np(&actions, 3));
#endif
    posix_spawnattr_t attr;
    check(posix_spawnattr_init(&attr));
    auto destroyAttr = kj::defer([&] { posix_spawnattr_destroy(&attr); });
    sigset_t mask, defaults; sigemptyset(&mask); sigemptyset(&defaults);
    sigaddset(&defaults, SIGTERM); sigaddset(&defaults, SIGINT); sigaddset(&defaults, SIGPIPE);
    check(posix_spawnattr_setsigmask(&attr, &mask)); check(posix_spawnattr_setsigdefault(&attr, &defaults));
    short flags = POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF;
#ifdef __APPLE__
    flags |= POSIX_SPAWN_CLOEXEC_DEFAULT;
#endif
    check(posix_spawnattr_setflags(&attr, flags));
    const auto self = std::filesystem::exists("/proc/self/exe") ? std::filesystem::path("/proc/self/exe")
        : std::filesystem::canonical(executableArgument);
    std::array<std::string, 4> args = {self.string(), "--host-check-child", std::to_string(config.sampleMiB), std::to_string(getpid())};
    std::array<char*, 5> argv = {args[0].data(), args[1].data(), args[2].data(), args[3].data(), nullptr};
    const auto error = posix_spawn(&pid, self.c_str(), &actions, &attr, argv.data(), environ);
    if (error) { pid = 0; closeFd(parentWrite); check(error); }
    exposure = true; spawnedAt = monotonicMs(); received = 0; timedOut = false;
    result = read; read = -1;
    fcntl(result, F_SETFL, fcntl(result, F_GETFL) | O_NONBLOCK);
  }
  void readResult() {
    while (received < buffer.size()) {
      const auto n = ::read(result, buffer.data() + received, buffer.size() - received);
      if (n > 0) { received += size_t(n); continue; }
      if (n < 0 && errno == EINTR) continue;
      break;
    }
  }
  void poll() {
    if (pid <= 0) return;
    readResult();
    int status;
    const auto reaped = waitpid(pid, &status, WNOHANG);
    if (reaped == 0 || (reaped < 0 && errno == EINTR)) return;
    // The child may have written between the first nonblocking read and waitpid.
    if (reaped > 0) readResult();
    // The OOM counter can precede waitpid. Keep exposure until the first counter
    // read after reaping, even if an earlier read already took the live marker.
    exposure = true; pid = 0;
    closeFd(parentWrite); closeFd(result);
    if (reaped < 0) { failed("cannot reap host check"); return; }
    if (timedOut) { failed("host check exceeded 30000 ms"); return; }
    if (!WIFEXITED(status) || received != sizeof(Report)) { failed("host check exited without a complete result"); return; }
    Report r;
    std::memcpy(&r, buffer.data(), sizeof r);
    if (r.magic != REPORT_MAGIC || r.largestCount > LARGEST || r.requestedMiB != config.sampleMiB ||
        r.touchedMiB != config.sampleMiB || r.error[0] || WEXITSTATUS(status) != (r.count20 ? 1 : 0)) {
      failed("host check returned an invalid or failed result"); return;
    }
    ++rounds; touched += r.touchedMiB; count20 += r.count20; count100 += r.count100;
    if (r.largestCount && r.largest[0].wallMs > maximumWall) {
      maximumWall = r.largest[0].wallMs; maximumSystem = r.largest[0].systemMs;
    }
    if (r.lastStallAtUnixMs) lastStall = r.lastStallAtUnixMs;
    available = true; lastError.clear(); lastResult = reportJson(r);
    log(r.count20 != 0);
  }
  void tick() {
    poll();
    const auto now = monotonicMs();
    if (pid > 0 && !timedOut && now - spawnedAt >= ROUND_TIMEOUT_MS) { timedOut = true; kill(pid, SIGKILL); }
    if (now < nextRound) return;
    nextRound = now + config.intervalMs;
    if (pid > 0 || memoryBand() != "green") { ++skipped; log(); return; }
    try { spawn(); }
    catch (const std::exception& error) { closeFd(parentWrite); failed(error.what()); }
    catch (const kj::Exception&) { closeFd(parentWrite); failed("cannot start host check"); }
  }
};
HostCheck::HostCheck(HostCheckConfig config, std::function<std::string()> band)
    : impl(std::make_unique<Impl>(config, std::move(band))) {}
HostCheck::~HostCheck() = default;
std::string HostCheck::json() const { return impl->json(); }
bool HostCheck::takeOomExposure() { const bool exposed = impl->exposure || impl->pid > 0; impl->exposure = false; return exposed; }
kj::Promise<void> HostCheck::run(kj::Timer& timer) {
  if (!impl->config.intervalMs) { impl->log(); co_await kj::Promise<void>(kj::NEVER_DONE); }
  for (;;) {
    impl->tick();
    const auto delay = impl->pid > 0 ? 10 : std::max<double>(1, impl->nextRound - monotonicMs());
    co_await timer.afterDelay(static_cast<int64_t>(delay) * kj::MILLISECONDS);
  }
}
}  // namespace coworkerd

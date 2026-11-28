#include "v8-sandbox.h"

#include "support.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sstream>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace coworkerd {
namespace {

constexpr const char* ROW = "[coworkerd-sandbox] ";

// The value of `"name":` in one flat JSON row: a string's text, or a number's or literal's spelling.
std::string field(const std::string& row, const std::string& name) {
  const auto key = "\"" + name + "\":";
  auto at = row.find(key);
  if (at == std::string::npos) return {};
  at += key.size();
  if (at < row.size() && row[at] == '"') {
    std::string text;
    for (auto i = at + 1; i < row.size() && row[i] != '"'; ++i) {
      if (row[i] == '\\' && i + 1 < row.size()) ++i;
      text += row[i];
    }
    return text;
  }
  auto end = row.find_first_of(",}", at);
  return row.substr(at, end == std::string::npos ? std::string::npos : end - at);
}

std::string readText(const char* path) {
  std::ifstream file(path);
  std::ostringstream text;
  text << file.rdbuf();
  return text.str();
}

int openProbePipe(int (&fds)[2]) {
#ifdef __linux__
  if (pipe2(fds, O_CLOEXEC) != 0) return errno;
#else
  if (pipe(fds) != 0) return errno;
  int error = 0;
  if (fcntl(fds[0], F_SETFD, FD_CLOEXEC) < 0 || fcntl(fds[1], F_SETFD, FD_CLOEXEC) < 0) {
    error = errno;
  } else if (fds[1] == STDERR_FILENO) {
    // macOS leaves FD_CLOEXEC set when a spawn dup2 action uses the same descriptor twice.
    const int writeFd = fcntl(fds[1], F_DUPFD_CLOEXEC, STDERR_FILENO + 1);
    if (writeFd < 0) {
      error = errno;
    } else {
      close(fds[1]);
      fds[1] = writeFd;
    }
  }
  if (error != 0) {
    close(fds[0]);
    close(fds[1]);
    return error;
  }
#endif
  return 0;
}

}  // namespace

SandboxBuildFacts parseSandboxFacts(const std::string& output, int exitStatus) {
  SandboxBuildFacts facts;
  std::istringstream lines(output);
  std::string line;
  while (std::getline(lines, line)) {
    auto at = line.find(ROW);
    if (at == std::string::npos) continue;
    auto row = line.substr(at + std::strlen(ROW));
    if (field(row, "process") != "true") continue;
    facts.probed = true;
    facts.mode = field(row, "mode");
    facts.groupPerIsolate = field(row, "groupPerIsolate") == "true";
    auto reservation = field(row, "reservationBytes");
    facts.reservationBytes = reservation.empty() ? 0 : std::stoull(reservation);
    facts.problem = field(row, "problem");
  }
  for (std::istringstream again(output); std::getline(again, line);) {
    auto at = line.find(ROW);
    if (at == std::string::npos) continue;
    auto row = line.substr(at + std::strlen(ROW));
    if (field(row, "capacity") != "true") continue;
    auto groups = field(row, "groups");
    facts.capacityMeasured = !groups.empty();
    facts.groupCapacity = groups.empty() ? 0 : std::stoull(groups);
  }
  if (!facts.probed)
    facts.probeError = "no [coworkerd-sandbox] row (exit status " + std::to_string(exitStatus) + ")";
  else if (exitStatus != 0 && facts.problem.empty())
    facts.problem = "the sandbox probe ended with status " + std::to_string(exitStatus);
  return facts;
}

namespace {
struct ThreadedProbe {
  std::string output, error;
  int status = 1;
};

ThreadedProbe probeThreaded(const std::string& threadedExecutable, std::string option, int outputFd) {
  std::string program = threadedExecutable;
  char* argv[] = {program.data(), option.data(), nullptr};
  int pipeFds[2];
  if (const int error = openProbePipe(pipeFds); error != 0) {
    return {{}, std::string("pipe: ") + std::strerror(error)};
  }
  posix_spawn_file_actions_t actions;
  pid_t pid = -1;
  int spawned = posix_spawn_file_actions_init(&actions);
  if (spawned == 0) {
    spawned = posix_spawn_file_actions_adddup2(&actions, pipeFds[1], outputFd);
    if (spawned == 0) spawned = posix_spawn_file_actions_addopen(&actions, outputFd == STDOUT_FILENO ? STDERR_FILENO : STDOUT_FILENO, "/dev/null", O_WRONLY, 0);
    if (spawned == 0) spawned = posix_spawn(&pid, program.c_str(), &actions, nullptr, argv, environ);
    posix_spawn_file_actions_destroy(&actions);
  }
  close(pipeFds[1]);
  if (spawned != 0) {
    close(pipeFds[0]);
    return {{}, "cannot run " + threadedExecutable + ": " + std::strerror(spawned)};
  }
  // The probe only starts V8 and answers; 30 s is far beyond it.
  std::string output;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  bool timedOut = false;
  for (;;) {
    auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
    if (left <= 0) { timedOut = true; break; }
    pollfd waiting{pipeFds[0], POLLIN, 0};
    int ready = poll(&waiting, 1, static_cast<int>(left));
    if (ready < 0 && errno == EINTR) continue;
    if (ready <= 0) { timedOut = ready == 0; break; }
    char buffer[4096];
    auto n = read(pipeFds[0], buffer, sizeof(buffer));
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;
    output.append(buffer, n);
  }
  close(pipeFds[0]);
  if (timedOut) kill(pid, SIGKILL);
  int status = 0;
  while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
  if (timedOut) {
    return {{}, "the probe of " + threadedExecutable + " did not answer within 30 s"};
  }
  return {std::move(output), {}, WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status)};
}
}  // namespace

SandboxBuildFacts probeSandboxBuild(const std::string& threadedExecutable) {
  const auto probe = probeThreaded(threadedExecutable, "--sandbox-facts", STDERR_FILENO);
  if (probe.error.empty()) return parseSandboxFacts(probe.output, probe.status);
  SandboxBuildFacts facts;
  facts.probeError = probe.error;
  return facts;
}

bool probeDemandDrivenCalls(const std::string& threadedExecutable) {
  const auto probe = probeThreaded(threadedExecutable, "--ordinary-admission-version", STDOUT_FILENO);
  return probe.status == 0 && probe.output == "1\n";
}

ProtectionKeyFacts parseCpuFlags(const std::string& cpuinfo) {
  ProtectionKeyFacts facts;
  std::istringstream lines(cpuinfo);
  std::string line;
  while (std::getline(lines, line)) {
    if (!line.starts_with("flags")) continue;
    std::istringstream words(line.substr(line.find(':') == std::string::npos ? line.size() : line.find(':') + 1));
    std::string word;
    while (words >> word) {
      if (word == "pku") facts.cpu = true;
      if (word == "ospke") facts.kernel = true;
    }
    break;  // the first processor's flags
  }
  return facts;
}

ProtectionKeyFacts probeProtectionKeys() {
  auto facts = parseCpuFlags(readText("/proc/cpuinfo"));
#if defined(__linux__) && defined(SYS_pkey_alloc)
  long key = syscall(SYS_pkey_alloc, 0, 0);
  if (key >= 0) {
    facts.usable = true;
    syscall(SYS_pkey_free, key);
  } else {
    facts.error = errno;
  }
#else
  facts.error = ENOSYS;
#endif
  return facts;
}

uint64_t addressSpaceFromMaps(const std::string& maps) {
  // The highest user mapping (the stack, near the top), rounded up to a power of two: 2^47 with
  // 48-bit virtual addresses on x86-64 and 2^48 on arm64, 2^39 on an arm64 kernel with 39 bits.
  uint64_t highest = 0;
  std::istringstream lines(maps);
  std::string line;
  while (std::getline(lines, line)) {
    if (line.find("[vsyscall]") != std::string::npos) continue;  // the kernel's fixed page
    auto dash = line.find('-'), space = line.find(' ');
    if (dash == std::string::npos || space == std::string::npos || space < dash) continue;
    highest = std::max<uint64_t>(highest, std::stoull(line.substr(dash + 1, space - dash - 1), nullptr, 16));
  }
  if (highest == 0) return 0;
  uint64_t space = 1;
  while (space < highest && space < (1ull << 63)) space <<= 1;
  return space;
}

MachineFacts probeMachine() {
  MachineFacts facts;
  facts.addressSpaceBytes = addressSpaceFromMaps(readText("/proc/self/maps"));
  auto count = readText("/proc/sys/vm/max_map_count");
  facts.maxMapCount = count.empty() ? 0 : std::stoull(count);
  return facts;
}

std::string missingProtectionKeys(const ProtectionKeyFacts& keys) {
  if (keys.usable) return {};
  if (!keys.cpu) return "cpu";
  if (!keys.kernel) return "kernel";
  return "container";
}

std::string protectionKeysReason(const ProtectionKeyFacts& keys) {
  auto missing = missingProtectionKeys(keys);
  if (missing.empty()) return "this machine has memory protection keys";
  if (missing == "cpu") return "the CPU has no memory protection keys (no pku in /proc/cpuinfo)";
  if (missing == "kernel") return "the kernel has not enabled memory protection keys (pku without ospke in /proc/cpuinfo)";
  return std::string("the CPU and the kernel have memory protection keys, but pkey_alloc fails here (") +
      std::strerror(keys.error) + "): the container or its system call filter does not let processes use them";
}

SandboxPlan planV8Sandbox(const std::string& setting, const SandboxBuildFacts& build,
    const ProtectionKeyFacts& keys, const MachineFacts& machine, uint32_t configuredResidentLimit,
    uint32_t configuredProcesses, const SandboxPlanConstants& constants) {
  SandboxPlan plan;
  plan.setting = setting;
  plan.keyFacts = keys;
  plan.build = build;
  plan.machine = machine;
  plan.keys = keys.usable ? "available" : "missing: " + missingProtectionKeys(keys);
  plan.configuredResidentLimit = plan.residentLimit = configuredResidentLimit;
  plan.configuredProcesses = plan.processes = configuredProcesses;
  if (setting == "required") {
    plan.refusal = keys.usable
        ? "sandbox.protectionKeys is required and this machine has memory protection keys, but this "
          "build has no protection-key mode yet; set sandbox.protectionKeys to auto or off"
        : "sandbox.protectionKeys is required, but " + protectionKeysReason(keys);
  }
  if (!build.probed) {
    // Nothing is planned: each process keeps its configured limit and V8's fallible group
    // creation is the only line. A startup warning reports the failed probe.
    plan.mode = "unknown";
    plan.warning = "the V8 sandbox of the ordinary processes is unknown: the probe (coworkerd-threaded "
        "--sandbox-facts) failed (" + build.probeError + "); each process keeps its resident limit of " +
        std::to_string(configuredResidentLimit) + " and only V8's own refusal bounds what fits in its address space";
    return plan;
  }
  if (!build.problem.empty()) {
    if (plan.refusal.empty()) plan.refusal = "the ordinary processes would refuse to start: " + build.problem;
    plan.mode = "insecure";
    return plan;
  }
  if (build.mode == "off" || !build.groupPerIsolate || build.reservationBytes == 0) {
    // One shared pointer cage: its own resident limit (processPool.threadedResidentLimit) holds.
    plan.mode = "off";
    return plan;
  }
  plan.mode = "guard-regions";
  const uint64_t perIsolate = build.reservationBytes + constants.outsideSandboxBytes;
  const uint64_t overhead = build.reservationBytes + constants.processOverheadBytes;
  // A fact this machine did not give (no /proc) bounds nothing; V8's fallible group creation
  // still refuses the isolate that does not fit.
  plan.addressLimit = machine.addressSpaceBytes == 0 ? UINT32_MAX
      : machine.addressSpaceBytes > overhead ? (machine.addressSpaceBytes - overhead) / perIsolate : 0;
  plan.mapLimit = machine.maxMapCount == 0 ? UINT32_MAX
      : machine.maxMapCount > constants.processMaps
      ? (machine.maxMapCount - constants.processMaps) / constants.mapsPerIsolate : 0;
  plan.measuredLimit = build.groupCapacity;
  // A measured 0 bounds (and refuses below); only an unmeasured capacity falls back to the size.
  const bool measured = build.capacityMeasured || build.groupCapacity > 0;
  const uint64_t space = measured ? std::min(plan.addressLimit, plan.measuredLimit) : plan.addressLimit;
  const uint64_t bound = std::min(space, plan.mapLimit) * constants.marginPercent / 100;
  if (bound == 0) {
    if (plan.refusal.empty()) {
      plan.refusal = "this machine's address space (" + std::to_string(machine.addressSpaceBytes >> 30) +
          " GiB) and vm.max_map_count (" + std::to_string(machine.maxMapCount) +
          ") hold no Worker with one V8 sandbox per isolate" +
          (measured && plan.measuredLimit == 0
              ? ": the probe (coworkerd-threaded --sandbox-facts) created no isolate group; besides the "
                "address space this can be a commit limit (vm.overcommit_memory=2) or a data limit "
                "(ulimit -d, systemd LimitDATA=), which each group's 2 GiB buffer pool counts against"
              : "");
    }
    return plan;
  }
  limitPerProcess(plan, static_cast<uint32_t>(std::min<uint64_t>(bound, UINT32_MAX)), constants);
  return plan;
}

void limitPerProcess(SandboxPlan& plan, uint32_t perProcessLimit, const SandboxPlanConstants& constants) {
  plan.perProcessLimit = perProcessLimit;
  // The configured process count holds: only each process's resident limit is lowered.
  plan.processes = plan.configuredProcesses;
  plan.residentLimit = std::min(plan.configuredResidentLimit, perProcessLimit);
  plan.residentCapacity = uint64_t(plan.processes) * plan.residentLimit;
  plan.lowered.clear();
  if (plan.residentLimit < plan.configuredResidentLimit) {
    plan.lowered = "the per-process resident limit is lowered from " + std::to_string(plan.configuredResidentLimit) +
        " (processPool.threadedResidentLimit) to " + std::to_string(plan.residentLimit) +
        " (one V8 sandbox per isolate): this node holds " + std::to_string(plan.processes) + " x " +
        std::to_string(plan.residentLimit) + " = " + std::to_string(plan.residentCapacity) +
        " resident Workers in its ordinary processes; for more, raise processPool.count";
  }
  plan.warning = plan.lowered;
  if (perProcessLimit < constants.warnBelow) {
    const uint64_t perIsolate = plan.build.reservationBytes + constants.outsideSandboxBytes;
    const bool measured = plan.build.capacityMeasured || plan.measuredLimit > 0;
    const uint64_t space = measured ? std::min(plan.addressLimit, plan.measuredLimit) : plan.addressLimit;
    plan.warning += std::string(plan.warning.empty() ? "" : "; ") + "only " + std::to_string(perProcessLimit) +
        " Worker(s) per process fit: one isolate's V8 sandbox reserves " + std::to_string(perIsolate >> 30) +
        " GiB of address space and this machine's user address space is " +
        std::to_string(plan.machine.addressSpaceBytes >> 30) + " GiB" +
        (plan.mapLimit < space ? " and vm.max_map_count is " + std::to_string(plan.machine.maxMapCount) : std::string()) +
        "; an arm64 host needs a kernel with 48-bit virtual addresses (CONFIG_ARM64_VA_BITS_48 or more), see "
        "docs/guide/install.md, \"The V8 sandbox and memory protection keys\"";
  }
}

std::string SandboxPlan::json() const {
  std::ostringstream out;
  out << "{\"setting\":\"" << jsonEscape(setting) << "\",\"mode\":\"" << jsonEscape(mode)
      << "\",\"protectionKeys\":{\"state\":\"" << jsonEscape(keys) << "\",\"cpu\":" << (keyFacts.cpu ? "true" : "false")
      << ",\"kernel\":" << (keyFacts.kernel ? "true" : "false") << ",\"usable\":" << (keyFacts.usable ? "true" : "false")
      << ",\"errno\":" << keyFacts.error << ",\"reason\":\"" << jsonEscape(protectionKeysReason(keyFacts))
      << "\"},\"build\":{\"probed\":" << (build.probed ? "true" : "false") << ",\"v8Mode\":\"" << jsonEscape(build.mode)
      << "\",\"groupPerIsolate\":" << (build.groupPerIsolate ? "true" : "false")
      << ",\"reservationBytes\":" << build.reservationBytes << ",\"capacityMeasured\":"
      << (build.capacityMeasured ? "true" : "false") << ",\"groupCapacity\":" << build.groupCapacity;
  if (!build.probeError.empty()) out << ",\"probeError\":\"" << jsonEscape(build.probeError) << "\"";
  if (!build.problem.empty()) out << ",\"problem\":\"" << jsonEscape(build.problem) << "\"";
  out << "},\"addressSpaceBytes\":" << machine.addressSpaceBytes << ",\"maxMapCount\":" << machine.maxMapCount
      << ",\"addressLimit\":" << addressLimit << ",\"measuredLimit\":" << measuredLimit << ",\"mapLimit\":" << mapLimit
      << ",\"perProcessLimit\":" << perProcessLimit << ",\"configuredResidentLimit\":" << configuredResidentLimit
      << ",\"residentLimit\":" << residentLimit << ",\"configuredProcesses\":" << configuredProcesses
      << ",\"processes\":" << processes << ",\"residentCapacity\":" << residentCapacity;
  if (!lowered.empty()) out << ",\"lowered\":\"" << jsonEscape(lowered) << "\"";
  if (!refusal.empty()) out << ",\"refusal\":\"" << jsonEscape(refusal) << "\"";
  if (!warning.empty()) out << ",\"warning\":\"" << jsonEscape(warning) << "\"";
  out << "}";
  return out.str();
}

}  // namespace coworkerd

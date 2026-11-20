#pragma once

#include <cstdint>
#include <string>

// The V8 sandbox of the ordinary processes (kernel patch 0043): which mode this machine and build
// run in, and how many Workers one process holds in it. With memory protection keys every isolate's
// 8 GiB sandbox would be fenced by keys (S3b, not in this build); without them it is fenced by V8's
// guard regions, and one isolate reserves 136 GiB of address space, so a process holds fewer than a
// thousand. The node plans this once at startup (sandbox.protectionKeys: auto, off or required),
// lowers the per-process resident limit to the plan's (the configured process count holds, r50
// §6.4), and reports the plan, with the node's resident capacity, in its startup log and state.
namespace coworkerd {

// What the ordinary processes' binary reports about its V8 sandbox (coworkerd-threaded
// --sandbox-facts: one [coworkerd-sandbox] row).
struct SandboxBuildFacts {
  bool probed = false;          // the binary answered with its row
  std::string probeError;       // why not
  std::string mode;             // V8's mode on this machine ("secure", ...); "off" without the sandbox
  bool groupPerIsolate = false;
  uint64_t reservationBytes = 0;  // one isolate's sandbox with its guard regions
  // How many isolate groups the probe process could create before V8 refused, when it measured
  // (its capacity row); a measured 0 holds no Worker. V8 places sandboxes at random addresses, so
  // about three quarters of the address space's share fit (random sequential packing).
  bool capacityMeasured = false;
  uint64_t groupCapacity = 0;
  std::string problem;          // the binary refuses to start here (exit 78) and says why
};

// This machine's memory protection keys.
struct ProtectionKeyFacts {
  bool cpu = false;     // pku in /proc/cpuinfo
  bool kernel = false;  // ospke: the kernel has enabled them
  bool usable = false;  // pkey_alloc succeeded in this process
  int error = 0;        // pkey_alloc's errno when it failed
};

struct MachineFacts {
  uint64_t addressSpaceBytes = 0;  // the user address space: 128 TiB with 48-bit virtual addresses
  uint64_t maxMapCount = 0;        // vm.max_map_count
};

// The parts of a process's plan. The node keeps a margin below what the process can hold: V8's
// fallible group creation (fork patch patches/v8/0057) is the second line behind this limit.
struct SandboxPlanConstants {
  // Outside an isolate's sandbox: its trusted range, code range and tables (about 2 GiB measured;
  // V8's TryCreate() requires 4 GiB of headroom).
  uint64_t outsideSandboxBytes = 4ull << 30;
  // The process itself besides its isolates: the process-wide sandbox (one reservation) plus this.
  uint64_t processOverheadBytes = 64ull << 30;
  // Memory mappings per resident isolate, and the process's own.
  uint64_t mapsPerIsolate = 100;
  uint64_t processMaps = 2000;
  // The share of the smaller of the two bounds the node uses.
  uint32_t marginPercent = 90;
  // Below this per-process limit the node warns (a 39-bit arm64 kernel holds about one isolate).
  uint32_t warnBelow = 50;
};

struct SandboxPlan {
  std::string setting;            // auto, off or required
  std::string mode;               // guard-regions; off (a build without the V8 sandbox); unknown (the probe failed)
  std::string keys;               // available, or what is missing
  ProtectionKeyFacts keyFacts;
  SandboxBuildFacts build;
  MachineFacts machine;
  // Isolates per process by each bound: the address space's size divided by one reservation, what
  // the probe measured (0: not measured), and the mappings.
  uint64_t addressLimit = 0, measuredLimit = 0, mapLimit = 0;
  uint32_t perProcessLimit = 0;   // the node's per-process limit for this mode (0: none)
  uint32_t configuredResidentLimit = 0, residentLimit = 0;
  uint32_t configuredProcesses = 0, processes = 0;  // processes is always the configured count
  uint64_t residentCapacity = 0;  // processes x residentLimit, once a per-process limit applies
  std::string lowered;            // non-empty: the resident limit is below the configured one, and why
  std::string refusal;            // non-empty: the node refuses to start
  std::string warning;            // non-empty: the node starts, and says this loudly
  std::string json() const;
};

// The plan, from the setting and the facts. Pure.
SandboxPlan planV8Sandbox(const std::string& setting, const SandboxBuildFacts& build,
    const ProtectionKeyFacts& keys, const MachineFacts& machine, uint32_t configuredResidentLimit,
    uint32_t configuredProcesses, const SandboxPlanConstants& constants = {});

// Applies a per-process limit to the plan: the resident limit, the node's resident capacity, and
// the warning (the lowering, and below constants.warnBelow why so few fit). The process count stays.
void limitPerProcess(SandboxPlan& plan, uint32_t perProcessLimit, const SandboxPlanConstants& constants = {});

// What is missing for memory protection keys: "" when usable, else "cpu", "kernel" or
// "container", and the sentence the node writes.
std::string missingProtectionKeys(const ProtectionKeyFacts& keys);
std::string protectionKeysReason(const ProtectionKeyFacts& keys);

// The probes.
SandboxBuildFacts probeSandboxBuild(const std::string& threadedExecutable);
SandboxBuildFacts parseSandboxFacts(const std::string& output, int exitStatus);
ProtectionKeyFacts probeProtectionKeys();
ProtectionKeyFacts parseCpuFlags(const std::string& cpuinfo);
MachineFacts probeMachine();
uint64_t addressSpaceFromMaps(const std::string& maps);

}  // namespace coworkerd

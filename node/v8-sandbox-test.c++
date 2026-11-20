// The node's V8 sandbox plan (node/v8-sandbox.h): the mode, the per-process limit from the address
// space and vm.max_map_count, more ordinary processes for the configured capacity, and the refusals
// of sandbox.protectionKeys = required.
#include "v8-sandbox.h"

#include <kj/debug.h>

#include <iostream>
#include <string>

namespace {

using namespace coworkerd;

constexpr uint64_t GiB = 1ull << 30;

// The build of kernel patch 0043 on a 48-bit x86-64 machine with Ubuntu's map count.
SandboxBuildFacts sandboxBuild() {
  SandboxBuildFacts build;
  build.probed = true;
  build.mode = "secure";
  build.groupPerIsolate = true;
  build.reservationBytes = 136 * GiB;
  build.groupCapacity = 720;  // what --sandbox-facts measured on cwkd-p40
  return build;
}
MachineFacts machine48() { return {.addressSpaceBytes = 128ull << 40, .maxMapCount = 1048576}; }
ProtectionKeyFacts noKeys() { return {}; }

void guardRegions() {
  const auto plan = planV8Sandbox("auto", sandboxBuild(), noKeys(), machine48(), 1840, 1);
  KJ_REQUIRE(plan.refusal.empty(), plan.refusal.c_str());
  KJ_REQUIRE(plan.mode == "guard-regions" && plan.keys == "missing: cpu", plan.mode.c_str(), plan.keys.c_str());
  // (128 TiB - 136 GiB - 64 GiB) / (136 + 4) GiB = 934 isolates by the address space's size, of
  // which random placement leaves 720 (measured); the map count allows (1,048,576 - 2,000) / 100 =
  // 10,465. 90% of the smallest: 648.
  KJ_REQUIRE(plan.addressLimit == 934 && plan.measuredLimit == 720 && plan.mapLimit == 10465,
      plan.addressLimit, plan.measuredLimit, plan.mapLimit);
  KJ_REQUIRE(plan.perProcessLimit == 648 && plan.residentLimit == 648, plan.perProcessLimit);
  // Without a measurement the address space's size bounds it: 90% of 934.
  auto unmeasured = sandboxBuild();
  unmeasured.groupCapacity = 0;
  KJ_REQUIRE(planV8Sandbox("auto", unmeasured, noKeys(), machine48(), 1840, 1).perProcessLimit == 840);
  // "off" plans the same: guard regions are this build's only mode.
  const auto off = planV8Sandbox("off", sandboxBuild(), noKeys(), machine48(), 1840, 1);
  KJ_REQUIRE(off.refusal.empty() && off.mode == "guard-regions" && off.perProcessLimit == 648);
  // Keys present (S3b not in this build): auto still plans guard regions and says the keys are there.
  ProtectionKeyFacts keys{.cpu = true, .kernel = true, .usable = true};
  const auto keyed = planV8Sandbox("auto", sandboxBuild(), keys, machine48(), 1840, 1);
  KJ_REQUIRE(keyed.refusal.empty() && keyed.mode == "guard-regions" && keyed.keys == "available");
  KJ_REQUIRE(plan.json().find("\"perProcessLimit\":648") != std::string::npos, plan.json().c_str());
}

void scale() {
  // The configured capacity (1 process x 1,840 residents) needs ceil(1840 / 648) = 3 processes.
  const auto plan = planV8Sandbox("auto", sandboxBuild(), noKeys(), machine48(), 1840, 1);
  KJ_REQUIRE(plan.processes == 3 && plan.configuredProcesses == 1, plan.processes);
  const auto four = planV8Sandbox("auto", sandboxBuild(), noKeys(), machine48(), 1840, 4);
  KJ_REQUIRE(four.processes == 12, four.processes);
  // A configured limit already below the plan's needs no more processes.
  const auto small = planV8Sandbox("auto", sandboxBuild(), noKeys(), machine48(), 500, 2);
  KJ_REQUIRE(small.residentLimit == 500 && small.processes == 2, small.residentLimit, small.processes);
}

void offBuild() {
  // A build with one shared pointer cage: its own limit holds and nothing scales.
  SandboxBuildFacts build;
  build.probed = true;
  build.mode = "off";
  const auto plan = planV8Sandbox("auto", build, noKeys(), machine48(), 1840, 1);
  KJ_REQUIRE(plan.refusal.empty() && plan.mode == "off" && plan.perProcessLimit == 0);
  KJ_REQUIRE(plan.residentLimit == 1840 && plan.processes == 1);
}

void insecureBuild() {
  auto build = sandboxBuild();
  build.mode = "smi-accessible";
  build.problem = "the first 4 GiB of the address space are not reserved";
  const auto plan = planV8Sandbox("auto", build, noKeys(), machine48(), 1840, 1);
  KJ_REQUIRE(plan.refusal.find("would refuse to start: the first 4 GiB") != std::string::npos, plan.refusal.c_str());
}

void required() {
  const auto cpu = planV8Sandbox("required", sandboxBuild(), noKeys(), machine48(), 1840, 1);
  KJ_REQUIRE(cpu.refusal.find("the CPU has no memory protection keys") != std::string::npos, cpu.refusal.c_str());
  ProtectionKeyFacts kernel{.cpu = true};
  const auto noKernel = planV8Sandbox("required", sandboxBuild(), kernel, machine48(), 1840, 1);
  KJ_REQUIRE(noKernel.refusal.find("the kernel has not enabled") != std::string::npos, noKernel.refusal.c_str());
  ProtectionKeyFacts container{.cpu = true, .kernel = true, .usable = false, .error = 1 /* EPERM */};
  const auto blocked = planV8Sandbox("required", sandboxBuild(), container, machine48(), 1840, 1);
  KJ_REQUIRE(blocked.refusal.find("the container or its system call filter") != std::string::npos, blocked.refusal.c_str());
  KJ_REQUIRE(missingProtectionKeys(container) == "container");
  ProtectionKeyFacts present{.cpu = true, .kernel = true, .usable = true};
  const auto keyed = planV8Sandbox("required", sandboxBuild(), present, machine48(), 1840, 1);
  KJ_REQUIRE(keyed.refusal.find("no protection-key mode yet") != std::string::npos, keyed.refusal.c_str());
}

void unknown() {
  SandboxBuildFacts build;
  build.probeError = "cannot run coworkerd-threaded";
  const auto plan = planV8Sandbox("auto", build, noKeys(), machine48(), 1840, 1);
  KJ_REQUIRE(plan.refusal.empty() && plan.mode == "unknown" && plan.residentLimit == 1840 && plan.processes == 1);
  KJ_REQUIRE(plan.json().find("cannot run coworkerd-threaded") != std::string::npos, plan.json().c_str());
}

void machineGaps() {
  // No /proc: only the probe's measurement bounds it (90% of 720).
  const auto none = planV8Sandbox("auto", sandboxBuild(), noKeys(), MachineFacts{}, 1840, 1);
  KJ_REQUIRE(none.refusal.empty() && none.residentLimit == 648, none.refusal.c_str(), none.residentLimit);
  // Nothing known at all: nothing is lowered, V8's fallible groups remain.
  auto unmeasured = sandboxBuild();
  unmeasured.groupCapacity = 0;
  KJ_REQUIRE(planV8Sandbox("auto", unmeasured, noKeys(), MachineFacts{}, 1840, 1).residentLimit == 1840);
  // A 39-bit arm64 kernel (512 GiB): (512 - 200) / 140 = 2 isolates, 90% of it is 1.
  const auto arm39 = planV8Sandbox("auto", sandboxBuild(), noKeys(), {.addressSpaceBytes = 512 * GiB, .maxMapCount = 65530}, 1840, 1);
  KJ_REQUIRE(arm39.addressLimit == 2 && arm39.perProcessLimit == 1, arm39.addressLimit, arm39.perProcessLimit);
  KJ_REQUIRE(arm39.refusal.empty() && arm39.warning.find("48-bit virtual addresses") != std::string::npos, arm39.warning.c_str());
  KJ_REQUIRE(planV8Sandbox("auto", sandboxBuild(), noKeys(), machine48(), 1840, 1).warning.empty());
  // An address space without room for one isolate refuses.
  const auto tiny = planV8Sandbox("auto", sandboxBuild(), noKeys(), {.addressSpaceBytes = 256 * GiB, .maxMapCount = 65530}, 1840, 1);
  KJ_REQUIRE(tiny.refusal.find("hold no Worker") != std::string::npos, tiny.refusal.c_str());
}

void mapCountFirst() {
  // The usual default of vm.max_map_count (65,530): (65,530 - 2,000) / 100 = 635 < 720 measured.
  const auto plan = planV8Sandbox("auto", sandboxBuild(), noKeys(), {.addressSpaceBytes = 128ull << 40, .maxMapCount = 65530}, 1840, 1);
  KJ_REQUIRE(plan.mapLimit == 635 && plan.measuredLimit == 720 && plan.perProcessLimit == 571, plan.mapLimit, plan.perProcessLimit);
}

void parse() {
  const std::string row =
      "noise\n[coworkerd-sandbox] {\"process\":true,\"mode\":\"secure\",\"sandboxBytes\":8589934592,"
      "\"reservationBytes\":146028888064,\"guardBytes\":68719476736,\"trailingGuardBytes\":68719476736,"
      "\"typedArrayAccessMasks\":true,\"groupPerIsolate\":true}\n[coworkerd-sandbox] {\"capacity\":true,\"groups\":721}\n";
  const auto facts = parseSandboxFacts(row, 0);
  KJ_REQUIRE(facts.probed && facts.mode == "secure" && facts.groupPerIsolate && facts.reservationBytes == 146028888064ull);
  KJ_REQUIRE(facts.groupCapacity == 721, facts.groupCapacity);
  const auto refused = parseSandboxFacts(
      "[coworkerd-sandbox] {\"process\":true,\"mode\":\"smi-accessible\",\"reservationBytes\":1,\"groupPerIsolate\":true,"
      "\"problem\":\"the first 4 GiB \\\"x\\\"\"}\nworkerd: refusing to start\n", 78);
  KJ_REQUIRE(refused.probed && refused.problem == "the first 4 GiB \"x\"", refused.problem.c_str());
  const auto off = parseSandboxFacts("[coworkerd-sandbox] {\"process\":true,\"mode\":\"off\",\"groupPerIsolate\":false}\n", 0);
  KJ_REQUIRE(off.probed && off.mode == "off" && !off.groupPerIsolate && off.reservationBytes == 0);
  KJ_REQUIRE(!parseSandboxFacts("nothing\n", 1).probed);
  // CPU flags and the address space.
  auto flags = parseCpuFlags("processor\t: 0\nflags\t\t: fpu vme pku ospke avx\nflags\t\t: other\n");
  KJ_REQUIRE(flags.cpu && flags.kernel);
  KJ_REQUIRE(!parseCpuFlags("flags\t\t: fpu vme\n").cpu);
  KJ_REQUIRE(addressSpaceFromMaps("55d0-55e0 r-xp 0 0:0 0 /bin/x\n7ffc12340000-7ffc12361000 rw-p 0 0:0 0 [stack]\n"
      "ffffffffff600000-ffffffffff601000 --xp 0 0:0 0 [vsyscall]\n") == (1ull << 47));
  KJ_REQUIRE(addressSpaceFromMaps("ffffec33c000-ffffec35d000 rw-p 0 0:0 0 [stack]\n") == (1ull << 48));
  KJ_REQUIRE(addressSpaceFromMaps("7fd1230000-7fd1251000 rw-p 0 0:0 0 [stack]\n") == (1ull << 39));
}

void probe() {
  // This machine's facts, as the node reads them.
  const auto machine = probeMachine();
  const auto keys = probeProtectionKeys();
  std::cout << "[v8-sandbox-test] {\"addressSpaceBytes\":" << machine.addressSpaceBytes << ",\"maxMapCount\":"
            << machine.maxMapCount << ",\"keys\":\"" << missingProtectionKeys(keys) << "\",\"errno\":" << keys.error << "}\n";
#ifdef __linux__
  KJ_REQUIRE(machine.addressSpaceBytes >= (1ull << 39) && machine.maxMapCount > 0, machine.addressSpaceBytes, machine.maxMapCount);
#endif
  const auto missing = probeSandboxBuild("/nonexistent/coworkerd-threaded");
  KJ_REQUIRE(!missing.probed && !missing.probeError.empty(), missing.probeError.c_str());
}

}  // namespace

int main(int argc, char** argv) {
  try {
    KJ_REQUIRE(argc == 2, "select a V8 sandbox test case");
    const std::string_view label = argv[1];
    if (label == "guard-regions") guardRegions();
    else if (label == "scale") scale();
    else if (label == "off-build") offBuild();
    else if (label == "insecure-build") insecureBuild();
    else if (label == "required") required();
    else if (label == "unknown") unknown();
    else if (label == "machine-gaps") machineGaps();
    else if (label == "map-count-first") mapCountFirst();
    else if (label == "parse") parse();
    else if (label == "probe") probe();
    else KJ_FAIL_REQUIRE("unknown V8 sandbox test case", argv[1]);
  } catch (const kj::Exception& error) {
    std::cerr << error.getDescription().cStr() << '\n';
    return 1;
  }
}

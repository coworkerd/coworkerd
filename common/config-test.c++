#include "config.h"
#include <kj/debug.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

namespace {
namespace fs = std::filesystem;

// This is a separate CTest process. Remove inherited limit overrides so that it
// also exercises the node configuration file's own defaults.
struct Fixture {
  Fixture() {
    std::string scratch = (fs::temp_directory_path() / "coworkerd-config-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(scratch.data()) != nullptr, "could not create test directory");
    directory = scratch;
    unsetenv("COWORKERD_CPU_LIMIT_MS");
    unsetenv("COWORKERD_MEMORY_LIMIT_MIB");
  }
  ~Fixture() { std::error_code ignored; fs::remove_all(directory, ignored); }
  coworkerd::ServerConfig load(const std::string& json) {
    const auto path = directory / "server.json";
    std::ofstream out(path);
    out << json;
    out.close();
    KJ_REQUIRE(out.good(), "could not write test configuration");
    return coworkerd::ServerConfig::load(path);
  }
  fs::path directory;
};

struct IdleCase { const char* name; unsigned threshold, interval; const char* diagnostic; };
constexpr std::array idleCases{
  IdleCase{"off", 0, 10, ""},
  IdleCase{"valid", 101, 10, ""},
  IdleCase{"equal", 100, 10, "idleEvictMs"},
  IdleCase{"below", 99, 10, "idleEvictMs"},
  IdleCase{"interval-zero", 101, 0, "idleDestroyIntervalMs"},
};

void idleConfiguration(Fixture& fixture) {
  for (const auto& row : idleCases) {
    std::string reason;
    try {
      const auto config = fixture.load("{\"placement\":{\"idleMs\":100},\"idleEvictMs\":" +
          std::to_string(row.threshold) + ",\"idleDestroyIntervalMs\":" + std::to_string(row.interval) + "}");
      config.processPool.validate();
      KJ_REQUIRE(config.processPool.heatIdleMs == 100 && config.processPool.idleEvictMs == row.threshold &&
          config.processPool.idleDestroyIntervalMs == row.interval, "idle settings not mapped", row.name);
    } catch (const kj::Exception& error) { reason = error.getDescription().cStr(); }
    if (*row.diagnostic) {
      KJ_REQUIRE(reason.find(row.diagnostic) != std::string::npos, "expected a named refusal", row.name, reason.c_str());
    } else {
      KJ_REQUIRE(reason.empty(), "valid configuration refused", row.name, reason.c_str());
    }
  }
}

void memoryConfiguration(Fixture& fixture) {
  const auto config = fixture.load(R"({"memoryWatermarkEnabled":true,"memoryBudgetBytes":2147483648,"processPool":{"count":4}})");
  KJ_REQUIRE(config.memoryWatermarkEnabled && config.memoryBudgetBytes == 2147483648ULL && config.processPool.count == 4,
      "explicit watermark budget and pool size must be preserved");
  KJ_REQUIRE(config.processPool.memoryLimitMiB == 128, "default limit must be 128 MiB");
  KJ_REQUIRE(fixture.load(R"({"memoryLimitMiB":64})").processPool.memoryLimitMiB == 64, "explicit limit must be 64 MiB");
  std::string refusal;
  try { fixture.load(R"({"memoryLimitMiB":0})"); }
  catch (const kj::Exception& error) { refusal = error.getDescription().cStr(); }
  KJ_REQUIRE(refusal.find("memoryLimitMiB") != std::string::npos, "the file cannot turn off the limit");
  setenv("COWORKERD_MEMORY_LIMIT_MIB", "0", 1);
  KJ_REQUIRE(fixture.load(R"({"memoryLimitMiB":0})").processPool.memoryLimitMiB == 0,
      "a deliberate environment override permits zero");
}

void tenantBorrowConfiguration(Fixture& fixture) {
  const auto defaults = fixture.load("{}");
  KJ_REQUIRE(defaults.tenantRootLimit == 0 && defaults.tenantNestedLimit == 0 && defaults.tenantBorrowBelowPercent == 0);
  const auto explicitConfig = fixture.load(R"({"placement":{"tenantRootLimit":16,"tenantNestedLimit":16,"tenantBorrowBelowPercent":88}})");
  KJ_REQUIRE(explicitConfig.tenantRootLimit == 16 && explicitConfig.tenantNestedLimit == 16 && explicitConfig.tenantBorrowBelowPercent == 88);
  KJ_REQUIRE(fixture.load(R"({"placement":{"tenantBorrowBelowPercent":100}})").tenantBorrowBelowPercent == 100);
  std::string refusal;
  try { fixture.load(R"({"placement":{"tenantBorrowBelowPercent":101}})"); }
  catch (const kj::Exception& error) { refusal = error.getDescription().cStr(); }
  KJ_REQUIRE(refusal.find("tenantBorrowBelowPercent") != std::string::npos);
}

// With the object store's membership the grace must exceed the heartbeat interval by more than 1,150 ms; the
// refusal names every value. The defaults (15 s, 5 s, 10 s) and the small test configuration (5 s, 1 s, 3 s) start.
void livenessGraceConfiguration(Fixture& fixture) {
  const auto load = [&](const std::string& membership) {
    return fixture.load(R"({"storage":{"endpoint":"http://127.0.0.1:9","bucket":"b"},"desiredState":{"backend":"file"})" +
        (membership.empty() ? std::string() : ",\"membership\":" + membership) + "}");
  };
  // What a node does at startup: load, then its own lease's margin.
  const auto with = [&](const std::string& membership) {
    auto config = load(membership);
    coworkerd::checkNodeLeaseMargin(config.membership);
    return config;
  };
  const auto defaults = with("");
  KJ_REQUIRE(defaults.membership.backend == "objectStore" && defaults.membership.ttlSeconds == 15 &&
      defaults.membership.heartbeatIntervalMs == 5000 && defaults.membership.livenessGraceMs == 10000);
  KJ_REQUIRE(with(R"({"ttlSeconds":5,"heartbeatIntervalMs":1000,"livenessGraceMs":3000})").membership.livenessGraceMs == 3000);
  KJ_REQUIRE(with(R"({"ttlSeconds":5,"heartbeatIntervalMs":1000,"livenessGraceMs":2151})").membership.livenessGraceMs == 2151);
  struct Refused { const char* membership; std::vector<std::string> named; };
  for (const auto& row : {
           Refused{R"({"ttlSeconds":5,"heartbeatIntervalMs":1000,"livenessGraceMs":2150})",
               {"livenessGraceMs 2150", "heartbeatIntervalMs 1000", "ttlSeconds 5", "+ 1150 ms", "is 2151"}},
           Refused{R"({"ttlSeconds":4,"heartbeatIntervalMs":500,"livenessGraceMs":750})",
               {"livenessGraceMs 750", "heartbeatIntervalMs 500", "ttlSeconds 4", "is 1651"}}}) {
    std::string refusal;
    try { with(row.membership); } catch (const kj::Exception& error) { refusal = error.getDescription().cStr(); }
    KJ_REQUIRE(refusal.rfind("membership.livenessGraceMs must be greater than", 0) == 0, "the refusal is the message itself",
        refusal.c_str());
    for (const auto& value : row.named)
      KJ_REQUIRE(refusal.find(value) != std::string::npos, "the refusal must name the value", value.c_str(), refusal.c_str());
  }
  // The control plane loads the same file without the node's check: the install guide has it set only the TTL and
  // the grace, so the interval stays the default 5000 ms, which a node with that grace would refuse.
  const auto control = load(R"({"ttlSeconds":15,"livenessGraceMs":5000})");
  KJ_REQUIRE(control.membership.heartbeatIntervalMs == 5000 && control.membership.livenessGraceMs == 5000);
  std::string nodeRefusal;
  try { coworkerd::checkNodeLeaseMargin(control.membership); }
  catch (const kj::Exception& error) { nodeRefusal = error.getDescription().cStr(); }
  KJ_REQUIRE(nodeRefusal.find("is 6151") != std::string::npos, nodeRefusal.c_str());
  // The file membership has no lease: its timing is not checked.
  const auto file = fixture.load(R"({"membership":{"backend":"file","heartbeatIntervalMs":500,"livenessGraceMs":750}})");
  coworkerd::checkNodeLeaseMargin(file.membership);
  KJ_REQUIRE(file.membership.backend == "file");
}
}

int main(int argc, char** argv) {
  try {
    KJ_REQUIRE(argc == 2, "select a configuration test case");
    Fixture fixture;
    const std::string_view label = argv[1];
    if (label == "idle") idleConfiguration(fixture);
    else if (label == "memory") memoryConfiguration(fixture);
    else if (label == "tenant-borrow") tenantBorrowConfiguration(fixture);
    else if (label == "liveness-grace") livenessGraceConfiguration(fixture);
    else KJ_FAIL_REQUIRE("unknown configuration test case", argv[1]);
  } catch (const kj::Exception& error) {
    std::cerr << error.getDescription().cStr() << '\n';
    return 1;
  }
}

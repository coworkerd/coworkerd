#include "config.h"
#include <kj/debug.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>

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
}

int main(int argc, char** argv) {
  try {
    KJ_REQUIRE(argc == 2, "select idle or memory");
    Fixture fixture;
    const std::string_view label = argv[1];
    if (label == "idle") idleConfiguration(fixture);
    else if (label == "memory") memoryConfiguration(fixture);
    else KJ_FAIL_REQUIRE("unknown configuration test case", argv[1]);
  } catch (const kj::Exception& error) {
    std::cerr << error.getDescription().cStr() << '\n';
    return 1;
  }
}

#include "platform-definition.h"
#include "platform-source.h"

#ifdef COWORKERD_PLATFORM_FIXTURE
#include "json.h"
#include "support.h"

#include <capnp/message.h>
#include <kj/debug.h>
#include <cstdlib>
#endif

namespace coworkerd {

const std::string& sqlDatabaseSource() {
  static const auto source = []() {
    std::string module = SQL_DATABASE_SOURCE;
#ifdef COWORKERD_PLATFORM_FIXTURE
    // Both database hosts use the same module. Only the fixture executable can append test code.
    if (const auto* path = std::getenv("COWORKERD_TEST_DATABASE_ALARM_FIXTURE")) {
      module += '\n';
      module += readFile(path);
    }
#endif
    return module;
  }();
  return source;
}

PlatformDefinitions loadPlatformDefinitions() {
  PlatformDefinitions result;
#ifdef COWORKERD_PLATFORM_FIXTURE
  const char* threadedDatabases = std::getenv("COWORKERD_TEST_THREADED_DATABASES");
  result.threadedDatabasesForTest = threadedDatabases != nullptr && std::string_view(threadedDatabases) == "1";
  const char* fixturePath = std::getenv("COWORKERD_PLATFORM_FIXTURE");
  if (fixturePath == nullptr) return result;
  const std::filesystem::path file(fixturePath);
  const auto fixtureBytes = readFile(file);
  capnp::MallocMessageBuilder message;
  auto config = message.initRoot<json::Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(fixtureBytes.data(), fixtureBytes.size()), config);
  const auto release = json::text(config, "release");
  auto classes = json::field(config, "classes");
  KJ_REQUIRE(deploymentSafeName(release) && classes.isArray(), "invalid platform fixture");
  auto source = readFile(file.parent_path() / "platform.js");
  result.fixture.emplace("coworkerd:platform-test", release,
      std::map<std::string, std::string>{{"platform.js", std::move(source)}});
  for (auto entry : classes.getArray()) {
    auto worker = json::text(entry, "worker");
    auto bindingClass = json::text(entry, "bindingClass");
    auto className = json::text(entry, "className");
    KJ_REQUIRE(workerFullName(worker) && environmentName(bindingClass) && environmentName(className),
        "invalid platform fixture class reference");
    KJ_REQUIRE(result.fixtureClasses.emplace(std::pair{std::move(worker), std::move(bindingClass)},
        PlatformClassRef{result.fixture->name, std::move(className)}).second,
        "duplicate platform fixture class reference");
  }
#endif
  return result;
}

}  // namespace coworkerd

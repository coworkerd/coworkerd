#include "platform-definition.h"

#ifdef COWORKERD_PLATFORM_FIXTURE
#include "json.h"
#include "support.h"

#include <capnp/message.h>
#include <kj/debug.h>
#include <cstdlib>
#endif

namespace coworkerd {

PlatformDefinitions loadPlatformDefinitions() {
  PlatformDefinitions result;
#ifdef COWORKERD_PLATFORM_FIXTURE
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

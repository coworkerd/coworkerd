#include "platform-definition.h"

#include <cstdlib>
#include <kj/debug.h>

int main() {
  using namespace coworkerd;
  setenv("COWORKERD_PLATFORM_FIXTURE", "/unused-platform-fixture", 1);
  const auto installed = loadPlatformDefinitions();
  KJ_REQUIRE(!installed.fixture && installed.fixtureClasses.empty(), "production enables only built-in definitions");

  PlatformDefinition first("coworkerd:platform-test", "build-a", {{"platform.js", "export class Record {}"}});
  PlatformDefinition changed("coworkerd:platform-test", "build-a", {{"platform.js", "export class Other {}"}});
  PlatformDefinition upgrade("coworkerd:platform-test", "build-b", {{"platform.js", "export class Record {}"}});
  const auto alpha = first.version("alpha");
  const auto beta = first.version("beta");
  KJ_REQUIRE(alpha.definitionHash == beta.definitionHash && alpha.contentHash == beta.contentHash);
  KJ_REQUIRE(alpha.definitionHash != changed.version("alpha").definitionHash);
  const auto key = platformCopyKey("alpha", first.name, alpha, 500, 64);
  KJ_REQUIRE(key != platformCopyKey("beta", first.name, beta, 500, 64));
  KJ_REQUIRE(key != platformCopyKey("alpha", first.name, upgrade.version("alpha"), 500, 64));
  KJ_REQUIRE(key != platformCopyKey("alpha", first.name, alpha, 501, 64));
  const auto payload = first.sourceJson(versionPin(alpha));
  KJ_REQUIRE(payload.find(alpha.contentHash) != std::string::npos);
  auto refusal = kj::runCatchingExceptions([&] { first.sourceJson(versionPin(changed.version("alpha"))); });
  KJ_REQUIRE(refusal != kj::none, "source is served only for the exact pin");
}

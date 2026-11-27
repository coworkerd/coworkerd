#include "platform-definition.h"

#include "tenancy.h"

#include <cstdlib>
#include <kj/debug.h>

int main() {
  using namespace coworkerd;
  setenv("COWORKERD_PLATFORM_FIXTURE", "/unused-platform-fixture", 1);
  setenv("COWORKERD_TEST_DATABASE_ALARM_FIXTURE", "/unused-database-alarm-fixture", 1);
  const auto installed = loadPlatformDefinitions();
  KJ_REQUIRE(!installed.fixture && installed.fixtureClasses.empty(),
      "the release executable ignores platform fixture settings");

  const PlatformClassRef reference{"coworkerd:platform-test", "PlatformStore"};
  const DeploymentState::NamespaceOwner owner{"alpha", "alpha/object", "Record", "", "active", 1024, reference};
  KJ_REQUIRE(platformClassFor(owner, "alpha/object", "Record") == reference);
  KJ_REQUIRE(!platformClassFor(owner, "beta/object", "Record"), "a different tenant cannot use the namespace");
  KJ_REQUIRE(!platformClassFor(owner, "alpha/other", "Record"), "a different Worker cannot use the namespace");
  auto otherTenant = owner;
  otherTenant.tenant = "beta";
  KJ_REQUIRE(!platformClassFor(otherTenant, "alpha/object", "Record"), "the namespace tenant must match the Worker");

  PlatformDefinition first("coworkerd:platform-test", "build-a", {{"platform.js", "export class Record {}"}});
  PlatformDefinition changed("coworkerd:platform-test", "build-a", {{"platform.js", "export class Other {}"}});
  PlatformDefinition upgrade("coworkerd:platform-test", "build-b", {{"platform.js", "export class Record {}"}});
  const auto alpha = first.version("alpha");
  const auto beta = first.version("beta");
  KJ_REQUIRE(tenantOf(alpha.workerId) == "alpha" && !workerFullName(alpha.workerId),
      "platform execution identity stays outside tenant Worker names");
  KJ_REQUIRE(alpha.definitionHash == beta.definitionHash && alpha.contentHash == beta.contentHash);
  KJ_REQUIRE(alpha.definitionHash != changed.version("alpha").definitionHash);
  const auto key = platformCopyKey("alpha", first.name, alpha, 500, 64);
  KJ_REQUIRE(key != platformCopyKey("beta", first.name, beta, 500, 64));
  KJ_REQUIRE(key != platformCopyKey("alpha", first.name, upgrade.version("alpha"), 500, 64));
  KJ_REQUIRE(key != platformCopyKey("alpha", first.name, alpha, 501, 64));
  const auto database = platformCopyKey("alpha", first.name, alpha, 500, 64, "namespace-one", "object-one");
  KJ_REQUIRE(database != key);
  KJ_REQUIRE(database != platformCopyKey("alpha", first.name, alpha, 500, 64, "namespace-two", "object-one"));
  KJ_REQUIRE(database != platformCopyKey("alpha", first.name, alpha, 500, 64, "namespace-one", "object-two"));
  const auto payload = first.sourceJson(versionPin(alpha));
  KJ_REQUIRE(payload.find(alpha.contentHash) != std::string::npos);
  auto refusal = kj::runCatchingExceptions([&] { first.sourceJson(versionPin(changed.version("alpha"))); });
  KJ_REQUIRE(refusal != kj::none, "source is served only for the exact pin");
}

#include "platform-definition.h"

#include "code-package.h"
#include "platform-source.h"
#include "support.h"
#include "tenancy.h"

#include <kj/debug.h>

namespace coworkerd {

PlatformDefinition::PlatformDefinition(std::string name, std::string release,
    std::map<std::string, std::string> modules): name(std::move(name)) {
  definition.mainModule = "platform.js";
  definition.compatibilityDate = "2026-08-27";
  definition.versionId = std::move(release);
  definition.format = "v2";
  KJ_REQUIRE(modules.contains(definition.mainModule), "missing platform entry module");
  CodePackage entries;
  std::map<std::string, std::string> bytesByHash;
  for (auto& [file, bytes] : modules) {
    auto sha = sha256Hex(bytes);
    entries.push_back({file, "esm", sha, bytes.size()});
    bytesByHash.emplace(std::move(sha), std::move(bytes));
  }
  const auto manifest = packageJson(std::move(entries));
  readCodePackage(manifest);
  definition.contentHash = sha256Hex(manifest);
  definition.definitionHash = sha256Hex("coworkerd-platform-definition-v1\n" + this->name + '\n' +
      definition.mainModule + '\n' + definition.compatibilityDate + "\n[]\n" + manifest);
  source = packageSource(manifest, bytesByHash);
}

VersionDefinition PlatformDefinition::version(const std::string& tenant) const {
  auto result = definition;
  result.workerId = tenant + std::string(PLATFORM_WORKER_SUFFIX);
  return result;
}

std::string PlatformDefinition::sourceJson(const VersionPin& pin) const {
  KJ_REQUIRE(pin.versionId == definition.versionId && pin.definitionHash == definition.definitionHash &&
      pin.contentHash == definition.contentHash, "platform source pin mismatch");
  using json::quote;
  return "{\"workerId\":" + quote(pin.workerId) + ",\"versionId\":" + quote(pin.versionId) +
      ",\"contentHash\":" + quote(pin.contentHash) + ",\"source\":" + quote(source) + '}';
}

PlatformDefinitions::PlatformDefinitions(): product("coworkerd:platform", COWORKERD_VERSION,
    {{"platform.js", PLATFORM_SOURCE}}) {}

const PlatformDefinition* PlatformDefinitions::definition(const std::string& name) const {
  if (product.name == name) return &product;
  if (fixture && fixture->name == name) return &*fixture;
  return nullptr;
}

std::optional<PlatformClassRef> PlatformDefinitions::fixtureClassFor(const DeploymentState::NamespaceOwner& owner) const {
  const auto found = fixtureClasses.find({owner.workerId, owner.className});
  if (found == fixtureClasses.end()) return std::nullopt;
  return found->second;
}

std::string platformCopyKey(std::string_view tenant, const std::string& name,
    const VersionDefinition& version, uint32_t cpuLimitMs, uint32_t memoryLimitMib) {
  // The binding set is empty. Keep this encoding in step with the threaded host's admission check.
  return "platform-v1:" + sha256Hex(std::string(tenant) + '\n' + name + '\n' + version.versionId + '\n' +
      version.definitionHash + '\n' + version.contentHash + '\n' + std::to_string(cpuLimitMs) + '\n' +
      std::to_string(memoryLimitMib));
}

RootDispatch preparePlatformRootDispatch(const PlatformClassRef& target, const VersionDefinition& version,
    uint32_t cpuLimitMs, uint32_t memoryLimitMib, const std::string& namespaceId,
    const std::string& objectId, uint64_t dataEpoch, uint64_t alarmAtUnixMs, uint32_t alarmFacetId) {
  using json::quote;
  const auto tenant = tenantOf(version.workerId);
  auto key = platformCopyKey(tenant, target.definition, version, cpuLimitMs, memoryLimitMib);
  auto plan = "{\"kind\":\"platformDurable\",\"tenant\":" + quote(tenant) +
      ",\"definitionName\":" + quote(target.definition) + ",\"versionFingerprint\":" + quote(key) + '}';
  auto selection = "{\"namespaceId\":" + quote(namespaceId) + ",\"objectId\":" + quote(objectId) +
      ",\"dataEpoch\":" + quote(std::to_string(dataEpoch)) + ",\"className\":" + quote(target.name) +
      ",\"dispatchChecked\":true";
  if (alarmAtUnixMs) selection += ",\"alarmFacetId\":" + std::to_string(alarmFacetId);
  selection += '}';
  return {std::move(plan), std::move(selection), std::move(key)};
}

}  // namespace coworkerd

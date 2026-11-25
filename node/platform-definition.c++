#include "platform-definition.h"

#include "code-package.h"
#include "platform-source.h"
#include "support.h"
#include "tenancy.h"

#include <kj/debug.h>

namespace coworkerd {

PlatformDefinition::PlatformDefinition(std::string name, std::string release,
    std::map<std::string, std::string> modules, std::vector<std::string> compatibilityFlags)
    : name(std::move(name)) {
  definition.mainModule = "platform.js";
  definition.compatibilityDate = "2026-08-27";
  definition.versionId = std::move(release);
  definition.compatibilityFlags = std::move(compatibilityFlags);
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
  std::string flags = "[";
  for (const auto& flag : definition.compatibilityFlags) {
    if (flags.size() > 1) flags += ',';
    flags += json::quote(flag);
  }
  flags += ']';
  definition.definitionHash = sha256Hex("coworkerd-platform-definition-v1\n" + this->name + '\n' +
      definition.mainModule + '\n' + definition.compatibilityDate + '\n' + flags + '\n' + manifest);
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
    {{"platform.js", PLATFORM_SOURCE}, {"sql-database.js", SQL_DATABASE_SOURCE}}, {"experimental"}) {}

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

std::optional<PlatformClassRef> platformClassFor(const DeploymentState::NamespaceOwner& owner,
    const std::string& workerId, const std::string& className) {
  if (!owner.active() || owner.workerId != workerId || owner.className != className ||
      owner.tenant != tenantOf(workerId)) return std::nullopt;
  return owner.platformClass;
}

std::string platformCopyKey(std::string_view tenant, const std::string& name,
    const VersionDefinition& version, uint32_t cpuLimitMs, uint32_t memoryLimitMib,
    std::string_view namespaceId, std::string_view objectId) {
  // The binding set is empty. Keep this encoding in step with the threaded host's admission check.
  const auto identity = std::string(tenant) + '\n' + name + '\n' + version.versionId + '\n' +
      version.definitionHash + '\n' + version.contentHash + '\n' + std::to_string(cpuLimitMs) + '\n' +
      std::to_string(memoryLimitMib);
  KJ_REQUIRE(namespaceId.empty() == objectId.empty(), "incomplete platform object identity");
  if (namespaceId.empty()) return "platform-v1:" + sha256Hex(identity);
  return "platform-object-v1:" + sha256Hex(identity + '\n' + std::string(namespaceId) + '\n' + std::string(objectId));
}

RootDispatch preparePlatformRootDispatch(const PlatformClassRef& target, const VersionDefinition& version,
    uint32_t cpuLimitMs, uint32_t memoryLimitMib, const std::string& namespaceId,
    const std::string& objectId, uint64_t dataEpoch, uint64_t alarmAtUnixMs, uint32_t alarmFacetId,
    bool database) {
  using json::quote;
  const auto tenant = tenantOf(version.workerId);
  auto key = database ? platformCopyKey(tenant, target.definition, version, cpuLimitMs, memoryLimitMib,
      namespaceId, objectId) : platformCopyKey(tenant, target.definition, version, cpuLimitMs, memoryLimitMib);
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

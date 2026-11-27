#pragma once

#include "deployment.h"
#include "root-objects.h"

namespace coworkerd {

class PlatformDefinition {
public:
  PlatformDefinition(std::string name, std::string release, std::map<std::string, std::string> modules,
      std::vector<std::string> compatibilityFlags = {});
  const std::string name;
  VersionDefinition version(const std::string& tenant) const;
  std::string sourceJson(const VersionPin& pin) const;

private:
  VersionDefinition definition;
  std::string source;
};

struct PlatformDefinitions {
  PlatformDefinitions();
  const PlatformDefinition* definition(const std::string& name) const;
  // The fixture executable is the only caller that can install test namespace references.
  std::optional<PlatformClassRef> fixtureClassFor(const DeploymentState::NamespaceOwner& owner) const;
  const PlatformDefinition product;
  std::optional<PlatformDefinition> fixture;
  std::map<std::pair<std::string, std::string>, PlatformClassRef> fixtureClasses;
};

PlatformDefinitions loadPlatformDefinitions();
const std::string& sqlDatabaseSource();
inline constexpr std::string_view PLATFORM_WORKER_SUFFIX = "/:platform";
std::optional<PlatformClassRef> platformClassFor(const DeploymentState::NamespaceOwner& owner,
    const std::string& workerId, const std::string& className);
std::string platformCopyKey(std::string_view tenant, const std::string& name,
    const VersionDefinition& version, uint32_t cpuLimitMs, uint32_t memoryLimitMib,
    std::string_view namespaceId = {}, std::string_view objectId = {});
RootDispatch preparePlatformRootDispatch(const PlatformClassRef& target, const VersionDefinition& version,
    uint32_t cpuLimitMs, uint32_t memoryLimitMib, const std::string& namespaceId,
    const std::string& objectId, uint64_t dataEpoch, uint64_t alarmAtUnixMs, uint32_t alarmFacetId,
    bool database);

}  // namespace coworkerd

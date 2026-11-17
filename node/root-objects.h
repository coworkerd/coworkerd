#pragma once

#include "deployment.h"

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <tuple>

namespace coworkerd {

class DeploymentState;

constexpr const char* DURABLE_ROOT_UNIQUE_KEY_PREFIX = "coworkerd-do-root-v1-";

// Local storage is selected by trusted object identity, never by a reported filename.
struct RootStorageRef {
  std::string namespaceId, objectId;
  uint64_t dataEpoch = 0;
  std::string uniqueKey;
  std::string storageId;

  std::filesystem::path directory(const std::filesystem::path& stack) const;
  std::filesystem::path databasePath(const std::filesystem::path& stack, uint32_t facetId,
      const char* suffix = "", const std::string& staging = "") const;
  std::filesystem::path facetIndexPath(const std::filesystem::path& stack) const;
  std::filesystem::path positionPath(const std::filesystem::path& stack) const;
};

RootStorageRef makeRootStorageRef(const std::string& namespaceId, const std::string& objectId,
    uint64_t dataEpoch);
bool isObjectStorageDirectory(const std::string& name);
std::string rootNamespaceUniqueKey(const std::string& namespaceId);
// Empty local data is initialized once; existing unmarked data is never changed.
void checkRootStorageFormat(const std::filesystem::path& stack, bool initialize);

std::string rootLoaderBinding(bool durable);
std::string rootLoaderService(bool durable, const std::string& storageDirectory);
struct RootDispatch {
  std::string plan, selection;
  std::string descriptor(std::string encoded) const;
};

// Owned by one node's event loop. A new catalogue generation invalidates every
// entry, including changes to declared secrets, variables and namespace state.
struct PlainPlanCache {
  using Key = std::tuple<std::string, std::string, std::string, uint32_t, uint32_t>;
  std::optional<uint64_t> generation;
  std::map<Key, std::string> plans;
};

std::string plainEnvironmentRevision(const DeploymentState& catalog, const VersionDefinition& version);

RootDispatch preparePlainDispatch(const DeploymentState& catalog, const VersionDefinition& version,
    const VersionPins& pins, uint32_t cpuLimitMs, uint32_t memoryLimitMib,
    PlainPlanCache* cache = nullptr);

std::optional<RootDispatch> prepareRootDispatch(const DeploymentState& catalog,
    const VersionDefinition& version, uint32_t cpuLimitMs, uint32_t memoryLimitMib,
    const std::string& namespaceId, const std::string& objectId, uint64_t dataEpoch,
    bool database, uint64_t alarmAtUnixMs, uint32_t alarmFacetId);

}  // namespace coworkerd

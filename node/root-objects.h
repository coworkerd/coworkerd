#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

namespace coworkerd {

constexpr const char* DURABLE_HOST_UNIQUE_KEY = "coworkerd-do-host-v1";
constexpr const char* DURABLE_ROOT_UNIQUE_KEY_PREFIX = "coworkerd-do-root-v1-";

// Local storage is selected by trusted object identity, never by a reported filename.
struct RootStorageRef {
  std::string namespaceId, objectId;
  uint64_t dataEpoch = 0;
  std::string uniqueKey = DURABLE_HOST_UNIQUE_KEY;
  std::string storageId;

  std::filesystem::path directory(const std::filesystem::path& stack) const;
  std::filesystem::path databasePath(const std::filesystem::path& stack, uint32_t facetId,
      const char* suffix = "", const std::string& staging = "") const;
  std::filesystem::path facetIndexPath(const std::filesystem::path& stack) const;
  std::filesystem::path positionPath(const std::filesystem::path& stack) const;
};

RootStorageRef makeRootStorageRef(const std::string& namespaceId, const std::string& objectId,
    uint64_t dataEpoch, bool roots, const std::string& hostUniqueKey = DURABLE_HOST_UNIQUE_KEY);
bool isObjectStorageDirectory(const std::string& name, const std::string& hostUniqueKey);

}  // namespace coworkerd

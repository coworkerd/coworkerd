#include "root-objects.h"
#include "durable-log.h"

#include <algorithm>
#include <string_view>

namespace coworkerd {

RootStorageRef makeRootStorageRef(const std::string& namespaceId, const std::string& objectId,
    uint64_t dataEpoch, bool roots, const std::string& hostUniqueKey) {
  RootStorageRef ref;
  ref.namespaceId = namespaceId;
  ref.objectId = objectId;
  ref.dataEpoch = dataEpoch;
  ref.uniqueKey = roots ? std::string(DURABLE_ROOT_UNIQUE_KEY_PREFIX) + namespaceId : hostUniqueKey;
  ref.storageId = durableHostId(ref.uniqueKey, durableHostName(namespaceId, objectId, dataEpoch));
  return ref;
}

std::filesystem::path RootStorageRef::directory(const std::filesystem::path& stack) const {
  return stack / "do" / uniqueKey;
}

std::filesystem::path RootStorageRef::databasePath(const std::filesystem::path& stack, uint32_t facetId,
    const char* suffix, const std::string& staging) const {
  const auto base = storageId + staging;
  const auto name = facetId == 0 ? base + ".sqlite" + suffix
      : base + "." + std::to_string(facetId) + ".sqlite" + suffix;
  return directory(stack) / name;
}

std::filesystem::path RootStorageRef::facetIndexPath(const std::filesystem::path& stack) const {
  return directory(stack) / (storageId + ".facets");
}

std::filesystem::path RootStorageRef::positionPath(const std::filesystem::path& stack) const {
  return directory(stack) / (storageId + ".position");
}

bool isObjectStorageDirectory(const std::string& name, const std::string& hostUniqueKey) {
  if (name == hostUniqueKey) return true;
  const std::string_view prefix = DURABLE_ROOT_UNIQUE_KEY_PREFIX;
  if (!name.starts_with(prefix)) return false;
  const auto namespaceId = std::string_view(name).substr(prefix.size());
  return namespaceId.size() == 64 && std::all_of(namespaceId.begin(), namespaceId.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}

}  // namespace coworkerd

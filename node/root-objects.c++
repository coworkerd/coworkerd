#include "root-objects.h"
#include "durable-log.h"
#include "deployment.h"
#include "durable-shards.h"
#include "json.h"
#include "support.h"

#include <kj/debug.h>

#include <algorithm>
#include <string_view>
#include <sstream>

namespace coworkerd {

RootStorageRef makeRootStorageRef(const std::string& namespaceId, const std::string& objectId,
    uint64_t dataEpoch) {
  RootStorageRef ref;
  ref.namespaceId = namespaceId;
  ref.objectId = objectId;
  ref.dataEpoch = dataEpoch;
  ref.uniqueKey = rootNamespaceUniqueKey(namespaceId);
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

bool isObjectStorageDirectory(const std::string& name) {
  const std::string_view prefix = DURABLE_ROOT_UNIQUE_KEY_PREFIX;
  if (!name.starts_with(prefix)) return false;
  const auto namespaceId = std::string_view(name).substr(prefix.size());
  return namespaceId.size() == 64 && std::all_of(namespaceId.begin(), namespaceId.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}

std::string rootNamespaceUniqueKey(const std::string& namespaceId) {
  return std::string(DURABLE_ROOT_UNIQUE_KEY_PREFIX) + namespaceId;
}

std::string rootLoaderBinding(bool durable) {
  return durable ? "(name = \"ROOTS\", coworkerdRootLoader = \"roots\"),\n" : "";
}

std::string rootLoaderService(bool durable, const std::string& storageDirectory) {
  return durable ? "(name = \"roots\", coworkerdRootLoader = (storagePath = " +
      json::quote(storageDirectory) + ")),\n" : "";
}

std::optional<RootDispatch> prepareRootDispatch(const DeploymentState& catalog,
    const VersionDefinition& version, uint32_t cpuLimitMs, uint32_t memoryLimitMib,
    const std::string& namespaceId, const std::string& objectId, uint64_t dataEpoch,
    bool database, uint64_t alarmAtUnixMs, uint32_t alarmFacetId) {
  using json::quote;
  const auto tenant = tenantOf(version.workerId);
  std::ostringstream spaces;
  spaces << '[';
  bool first = true, selected = false;
  auto space = [&](const DurableObjectNamespace& value, const std::string& className) {
    if (value.status != "active") return;
    spaces << (first ? "" : ",") << "{\"id\":" << quote(value.id) << ",\"className\":" << quote(className)
        << ",\"binding\":" << quote("root_"+value.id) << ",\"uniqueKey\":" << quote(rootNamespaceUniqueKey(value.id)) << '}';
    first = false;
    selected |= value.id == namespaceId;
  };
  if (database) {
    if (const auto* databases = catalog.databases(tenant))
      for (const auto& [name, value] : *databases)
        if (value.id == namespaceId) space(value, "SqlDatabase");
  } else {
    for (const auto& name : version.declaredClasses)
      if (const auto* value = catalog.durableNamespace(version.workerId, name)) space(*value, "__coworkerd_do_"+name);
  }
  spaces << ']';
  if (!selected) return std::nullopt;
  std::ostringstream plan;
  plan << "{\"kind\":" << quote(database ? "platform" : "tenant") << ",\"tenant\":" << quote(tenant)
      << ",\"workerId\":" << quote(version.workerId) << ",\"versionId\":" << quote(version.versionId)
      << ",\"compatibilityDate\":" << quote(version.compatibilityDate) << ",\"compatibilityFlags\":[],\"modules\":[]"
      << ",\"namespaces\":" << spaces.str();
  auto target = [&](const std::string& entrypoint, const std::string& props) {
    return "{\"service\":\"runtime\",\"entrypoint\":"+quote(entrypoint)+",\"props\":"+props+'}';
  };
  if (database) {
    plan << ",\"platformClass\":" << target("SqlDatabase", "{}");
  } else {
    const auto identity = "{\"workerId\":"+quote(version.workerId)+",\"versionId\":"+quote(version.versionId)+'}';
    plan << ",\"creationRecord\":{\"present\":true,\"tenant\":" << quote(tenant)
        << ",\"workerId\":" << quote(version.workerId) << ",\"versionId\":" << quote(version.versionId)
        << ",\"cpuLimitMs\":" << cpuLimitMs << ",\"memoryLimitMib\":" << memoryLimitMib << '}'
        << ",\"globalOutbound\":" << target("Egress", identity)
        << ",\"streamingTail\":" << target("TenantLog", identity) << ",\"durableObjects\":[";
    bool firstBinding = true;
    for (const auto& binding : version.durableObjects) {
      const auto* value = binding.database.empty() ? catalog.durableNamespace(version.workerId, binding.className)
          : catalog.database(tenant, binding.database);
      const auto ns = value != nullptr && value->status == "active" ? value->id : "";
      const auto id = !binding.database.empty() && !ns.empty() ? durableObjectIdFromName(ns, DATABASE_OBJECT_NAME) : "";
      const auto metadata = "{\"binding\":"+quote(binding.binding)+",\"className\":"+quote(binding.className)+
          ",\"namespaceId\":"+quote(ns)+",\"database\":"+quote(binding.database)+",\"objectId\":"+quote(id)+'}';
      const auto props = "{\"workerId\":"+quote(version.workerId)+",\"versionId\":"+quote(version.versionId)+
          ",\"contentHash\":"+quote(version.contentHash)+",\"definitionHash\":"+quote(version.definitionHash)+
          ",\"namespaceId\":"+quote(ns)+",\"className\":"+quote(binding.className)+",\"database\":"+quote(binding.database)+'}';
      plan << (firstBinding ? "" : ",") << "{\"metadata\":" << metadata << ",\"target\":"
          << target("DurableNamespace", props) << '}';
      firstBinding = false;
    }
    plan << ']';
    // The source definition alone does not identify resolved bindings or rotated secrets.
    // Hash their current trusted values without putting those values in keys or telemetry.
    std::ostringstream environment;
    for (const auto& value : version.variables)
      environment << quote(value.name) << ':' << quote(value.type) << ':' << quote(value.value) << ',';
    for (const auto& name : version.secrets) {
      const auto* value = catalog.secret(version.workerId, name);
      environment << quote(name) << ':' << (value ? quote(value->value) : "null") << ',';
    }
    plan << ",\"versionFingerprint\":" << quote(version.definitionHash+":"+
        sha256Hex(plan.str()+environment.str()));
  }
  plan << '}';
  std::ostringstream selection;
  selection << "{\"namespaceId\":" << quote(namespaceId) << ",\"objectId\":" << quote(objectId)
      << ",\"dataEpoch\":" << quote(std::to_string(dataEpoch)) << ",\"dispatchChecked\":true";
  if (alarmAtUnixMs) selection << ",\"alarmFacetId\":" << alarmFacetId;
  selection << '}';
  return RootDispatch{plan.str(), selection.str()};
}

std::string RootDispatch::descriptor(std::string encoded) const {
  KJ_REQUIRE(!encoded.empty() && encoded.back() == '}', "an invocation descriptor is a JSON object");
  encoded.pop_back();
  return encoded+",\"rootPlan\":"+plan+",\"rootSelection\":"+selection+'}';
}

}  // namespace coworkerd

#include "root-objects.h"
#include "durable-log.h"
#include "deployment.h"
#include "durable-shards.h"
#include "json.h"
#include "support.h"
#include "tenant-manifest.h"

#include <kj/debug.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string_view>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>

namespace coworkerd {

void checkRootStorageFormat(const std::filesystem::path& stack, bool initialize) {
  const auto directory = stack / "do", marker = directory / "format";
  if (std::filesystem::exists(marker)) {
    KJ_REQUIRE(readFile(marker) == DO_ROOT_FORMAT_BYTES,
        "this data directory has an incompatible Durable Object format marker; use a new directory");
    return;
  }
  KJ_REQUIRE(!std::filesystem::exists(directory) || std::filesystem::is_empty(directory),
      "this data directory contains old-format Durable Object files without a format marker; use a new directory");
  if (!initialize) return;
  std::filesystem::create_directories(directory);
  const int fd = ::open(marker.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0 && errno == EEXIST) {
    KJ_REQUIRE(readFile(marker) == DO_ROOT_FORMAT_BYTES, "incompatible Durable Object format marker; use a new directory");
    return;
  }
  KJ_REQUIRE(fd >= 0, "cannot create the local Durable Object format marker", strerror(errno));
  const auto close = kj::defer([fd]() { ::close(fd); });
  const std::string bytes = DO_ROOT_FORMAT_BYTES;
  size_t offset = 0;
  while (offset < bytes.size()) {
    const auto written = ::write(fd, bytes.data() + offset, bytes.size() - offset);
    if (written < 0 && errno == EINTR) continue;
    KJ_REQUIRE(written > 0, "cannot write the local Durable Object format marker", strerror(errno));
    offset += size_t(written);
  }
  KJ_REQUIRE(::fsync(fd) == 0, "cannot sync the local Durable Object format marker", strerror(errno));
  const int directoryFd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  KJ_REQUIRE(directoryFd >= 0, "cannot open the Durable Object data directory", strerror(errno));
  const auto closeDirectory = kj::defer([directoryFd]() { ::close(directoryFd); });
  KJ_REQUIRE(::fsync(directoryFd) == 0, "cannot sync the Durable Object data directory", strerror(errno));
}

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

std::string rootLoaderBinding(bool) {
  return "(name = \"ROOTS\", coworkerdRootLoader = \"roots\"),\n";
}

std::string rootLoaderService(bool durable, const std::string& storageDirectory) {
  return "(name = \"roots\", coworkerdRootLoader = (plainOnly = " +
      std::string(durable ? "false" : "true") + ", storagePath = " +
      json::quote(durable ? storageDirectory : "") + ")),\n";
}

RootDispatch preparePlainDispatch(const DeploymentState& catalog, const VersionDefinition& version,
    const VersionPins& pins, uint32_t cpuLimitMs, uint32_t memoryLimitMib) {
  using json::quote;
  const auto tenant = tenantOf(version.workerId);
  const auto identity = "{\"workerId\":"+quote(version.workerId)+",\"versionId\":"+quote(version.versionId)+'}';
  auto target = [&](const std::string& entrypoint, const std::string& props) {
    return "{\"service\":\"runtime\",\"entrypoint\":"+quote(entrypoint)+",\"props\":"+props+'}';
  };
  std::ostringstream plan;
  plan << "{\"kind\":\"tenant\",\"tenant\":" << quote(tenant)
      << ",\"workerId\":" << quote(version.workerId) << ",\"versionId\":" << quote(version.versionId)
      << ",\"compatibilityDate\":" << quote(version.compatibilityDate)
      << ",\"compatibilityFlags\":[],\"modules\":[],\"namespaces\":[],\"structuredProps\":"
      << (version.format == "v2" ? "true" : "false")
      << ",\"creationRecord\":{\"present\":true,\"tenant\":" << quote(tenant)
      << ",\"workerId\":" << quote(version.workerId) << ",\"versionId\":" << quote(version.versionId)
      << ",\"cpuLimitMs\":" << cpuLimitMs << ",\"memoryLimitMib\":" << memoryLimitMib << '}'
      << ",\"globalOutbound\":" << target("Egress", identity)
      << ",\"streamingTail\":" << target("TenantLog", identity) << ",\"serviceBindings\":[";
  bool first = true;
  for (const auto& binding : version.bindings) {
    const auto resolved = binding.assets ? versionPin(version) : pins.at(binding.workerId);
    const auto props = "{\"workerId\":"+quote(resolved.workerId)+",\"versionId\":"+quote(resolved.versionId)+
        ",\"contentHash\":"+quote(resolved.contentHash)+",\"definitionHash\":"+quote(resolved.definitionHash)+
        ",\"entrypoint\":"+quote(binding.entrypoint)+(binding.assets ? ",\"assets\":true}" : "}");
    plan << (first ? "" : ",") << "{\"metadata\":{\"name\":" << quote(binding.name)
        << "},\"target\":" << target("BoundWorker", props) << '}';
    first = false;
  }
  plan << "],\"durableObjects\":[";first = true;
  for (const auto& binding : version.durableObjects) {
    const auto* value = binding.database.empty() ? catalog.durableNamespace(version.workerId, binding.className)
        : catalog.database(tenant, binding.database);
    const auto ns = value && value->status == "active" ? value->id : "";
    const auto id = !binding.database.empty() && !ns.empty() ? durableObjectIdFromName(ns, DATABASE_OBJECT_NAME) : "";
    const auto metadata = "{\"binding\":"+quote(binding.binding)+",\"className\":"+quote(binding.className)+
        ",\"namespaceId\":"+quote(ns)+",\"database\":"+quote(binding.database)+",\"objectId\":"+quote(id)+'}';
    const auto props = "{\"workerId\":"+quote(version.workerId)+",\"versionId\":"+quote(version.versionId)+
        ",\"contentHash\":"+quote(version.contentHash)+",\"definitionHash\":"+quote(version.definitionHash)+
        ",\"namespaceId\":"+quote(ns)+",\"className\":"+quote(binding.className)+",\"database\":"+quote(binding.database)+'}';
    plan << (first ? "" : ",") << "{\"metadata\":" << metadata << ",\"target\":" << target("DurableNamespace", props) << '}';
    first = false;
  }
  plan << ']';
  std::ostringstream revisions;
  for (const auto& value : version.variables) revisions << quote(value.name) << ':' << quote(value.type) << ':' << quote(value.value) << ',';
  for (const auto& name : version.secrets) {
    const auto* value = catalog.secret(version.workerId, name);
    revisions << quote(name) << ':' << (value ? std::to_string(value->setAtUnixMs) : "null") << ',';
  }
  plan << ",\"versionFingerprint\":" << quote(version.definitionHash+":"+sha256Hex(plan.str()+revisions.str())) << '}';
  const auto* active = catalog.worker(version.workerId);
  const bool current = active && active->valid && active->versionId == version.versionId && active->definitionHash == version.definitionHash;
  return RootDispatch{plan.str(), "{\"kind\":\"plain\",\"dispatchChecked\":true,\"catalogGeneration\":"+
      std::to_string(catalog.generation())+",\"activePlan\":"+(current ? "true}" : "false}")};
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
    // Secret revisions identify rotation without deriving any observable bytes from secret values.
    std::ostringstream environment;
    for (const auto& value : version.variables)
      environment << quote(value.name) << ':' << quote(value.type) << ':' << quote(value.value) << ',';
    for (const auto& name : version.secrets) {
      const auto* value = catalog.secret(version.workerId, name);
      environment << quote(name) << ':' << (value ? std::to_string(value->setAtUnixMs) : "null") << ',';
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

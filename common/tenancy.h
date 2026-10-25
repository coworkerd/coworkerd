#pragma once

#include "json.h"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

// Tenants (docs/control-plane-api.md): the two-layer model tenant -> Worker and the records the control plane and the
// nodes share -- tenants, credentials, Worker limits, usage.
namespace coworkerd {

// The tenant of the deployment tool's subcommands without --tenant (the tests and the daemon itself).
inline constexpr const char* DEFAULT_TENANT = "default";

// [a-z0-9][a-z0-9-]{0,62}. A tenant's name is also none of the names deploy/ keeps for its own keys ("routes",
// "generation"); an API Worker name has the same characters.
bool tenantName(const std::string& value);
bool apiName(const std::string& value);
// <tenant>/<worker>: a Worker's full name, the id of every node-side record. The worker part is a deployment safe name
// (letters, digits, '-', '_', '.'): the API takes the stricter apiName; the deployment tool keeps the older names.
bool workerFullName(const std::string& value);
std::string tenantOf(const std::string& fullName);      // "" when the value is not a full name
std::string workerNameOf(const std::string& fullName);  // the part after the tenant, else the value itself
std::string workerFullId(const std::string& tenant, const std::string& worker);

// A Worker's own CPU and memory limits (the active pointer's `limits`); 0 means unset.
struct WorkerLimits {
  uint32_t cpuMs = 0, memoryMiB = 0;
  bool empty() const { return !cpuMs && !memoryMiB; }
  bool operator==(const WorkerLimits&) const = default;
};
// {"cpuMs":..,"memoryMiB":..}, the set fields only ("{}" when none).
std::string workerLimitsJson(const WorkerLimits& limits);
// The fields of a JSON object: cpuMs and memoryMiB, positive integers (or null: unset). Throws with the field's name.
WorkerLimits readWorkerLimits(json::Json::Reader object);

// A tenant's quotas. The control plane refuses what would exceed one (QUOTA_EXCEEDED, the reason names it); the logs'
// two are enforced by the nodes; the Worker limits' ceilings bound what a Worker may set.
struct TenantQuotas {
  uint64_t workers = 100;
  uint64_t versionsPerWorker = 50;
  uint64_t packageBytes = 167772160;
  uint64_t assetFiles = 20000;
  uint64_t assetBytes = 104857600;
  uint64_t secrets = 1000;
  uint64_t hostnames = 100;
  uint64_t logLinesPerSecond = 100;
  uint64_t logBufferBytes = 1048576;
  uint64_t maxWorkerCpuMs = 30000;
  uint64_t maxWorkerMemoryMiB = 512;
  // Durable Object namespaces over all the tenant's Workers, the deleting ones included (their data is still there),
  // and the tenant's databases, which are namespaces too.
  uint64_t durableObjectNamespaces = 16;
  // What the tenant's Durable Objects may hold together, as the objects' manifests and snapshots add up (the nodes
  // publish what they own every hour): a write past it is refused with 413 QUOTA_EXCEEDED, reads go on.
  uint64_t durableObjectBytes = 10ull << 30;
  bool operator==(const TenantQuotas&) const = default;
};
std::string tenantQuotasJson(const TenantQuotas& quotas);
// Sets the fields an object names; an unknown name or a value that is not a positive integer throws with the name.
void applyTenantQuotas(TenantQuotas& quotas, json::Json::Reader object);

struct TenantRecord {
  std::string name, status = "active";  // "active" or "deleting"
  uint64_t createdAtUnixMs = 0, updatedAtUnixMs = 0;
  std::vector<std::string> hostnames;
  TenantQuotas quotas;
  WorkerLimits defaultLimits;
  std::string deletion = "null";  // the deletion's progress (a JSON object) while deleting
};
std::string tenantRecordJson(const TenantRecord& tenant);
// Lenient: fields a later version adds are ignored; a record without a valid name throws.
TenantRecord decodeTenantRecord(const std::string& bytes);
// A hostname a tenant may own: lowercase DNS labels of [a-z0-9-] (1 to 63 each), at most 253 characters, at least
// two labels. "*" (the default host of path routes) is nobody's.
bool hostnameValid(const std::string& host);

// A tenant credential: stored by its key id with the SHA-256 of its secret (the secret itself only in the answer that
// created it). Its text is "cwk_<keyId>_<secret>": 16 and 64 lowercase hex characters.
struct CredentialRecord {
  std::string keyId, scope, sha256, description;  // scope "tenant:<t>"
  uint64_t createdAtUnixMs = 0, expiresAtUnixMs = 0, revokedAtUnixMs = 0;
};
std::string credentialRecordJson(const CredentialRecord& record);
CredentialRecord decodeCredentialRecord(const std::string& bytes);
struct CredentialToken { std::string keyId, secret; };
std::optional<CredentialToken> parseCredential(const std::string& value);
std::string credentialText(const CredentialToken& token);

// One node's usage in one hour (UTC), every tenant's counts in one object on the object store,
// nodes/<nodeId>/usage/<hour>: the node overwrites it every usage.flushIntervalMs (when its counts changed) and removes
// its own past retentionHours; the control plane lists nodes/ and sums the hours a request asks for, and removes what a
// node that left kept past retentionHours.
struct UsageCounts {
  uint64_t requests = 0, logLines = 0, egressBytes = 0;
  double cpuMs = 0, isolateMs = 0;
  void add(const UsageCounts& other);
};
// "requests":..,"cpuMs":..,"egressBytes":..,"logLines":..,"isolateMs":.. (the fields, without braces).
std::string usageCountsFields(const UsageCounts& counts);
struct NodeUsageHour {
  std::string nodeId, hour;
  uint64_t updatedAtUnixMs = 0;
  std::map<std::string, UsageCounts> tenants;
};
std::string nodeUsageHourJson(const NodeUsageHour& usage);
// Lenient about fields a later version adds; an object without a valid node id, hour or tenant name throws.
NodeUsageHour decodeNodeUsageHour(const std::string& bytes);
// The hour a time falls in, "YYYY-MM-DDTHH" (UTC; sorts as text), and its start.
std::string usageHour(uint64_t unixMs);
uint64_t usageHourStartMs(const std::string& hour);  // throws on a malformed hour

// The nodes' own objects: nodes/<nodeId>/... -- a node's identity writes only its own prefix (and members/<nodeId>/).
inline constexpr const char* NODES_OBJECT_PREFIX = "nodes/";
std::string nodeUsagePrefix(const std::string& nodeId);                         // nodes/<nodeId>/usage/
std::string nodeUsageKey(const std::string& nodeId, const std::string& hour);  // nodes/<nodeId>/usage/<hour>
// The node and the hour of a key nodes/<nodeId>/usage/<hour>; none for any other key.
std::optional<std::pair<std::string, std::string>> parseNodeUsageKey(const std::string& key);

}  // namespace coworkerd

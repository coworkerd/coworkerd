#pragma once

// The control plane's operations (by tenant), behind /v1 (main.c++): the deployment tool's own
// (deployment-tool.c++) and tenants, credentials and usage (tenants.c++). The
// desired state -- tenants, credentials, versions, pointers, routes, secrets -- is the object store's (manifests.h): every
// read is the in-memory mirror's, every write one aggregate's next revision. The deployment operations answer
// {"ok":true,"record":...} or {"ok":false,"kind","reason","record"}; main.c++ maps a reason to a /v1 code. Worker names
// are full names, <tenant>/<worker>.
#include "deployment.h"
#include "json.h"
#include "manifests.h"
#include "object-store.h"
#include "tenancy.h"

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace coworkerd {

struct ControlContext {
  ObjectStore& storage;
  ManifestStore& state;
  kj::Timer& timer;
  kj::Network& network;
  const kj::HttpHeaderTable& headers;
  std::string token;  // the nodes' management token (trial loads)
  // The members trial loads go to: the heartbeats under members/ in `storage`, live within ttlSeconds + livenessGraceMs
  // of the listing's Date.
  uint32_t memberTtlSeconds = 15, memberGraceMs = 10000;
};

// ---- deployment-tool.c++ ----
// A tenant's Workers (or one of them): versions in upload order, the active version, validity, limits, routes,
// secrets' names and lengths.
kj::Promise<std::string> controlWorkers(ManifestStore& state, std::string tenant, std::optional<std::string> workerId = std::nullopt);
// PUT a version: `manifest` holds exactly that version of that Worker. Always trial-loaded; its definition is
// published only once it loads; `activate` also flips the pointer (the manifest's routes, or the Worker's current
// ones when it lists none).
kj::Promise<std::string> controlDeploy(ControlContext& context, std::filesystem::path manifest, std::string workerId,
    std::string versionId, bool activate);
kj::Promise<std::string> controlActivate(ControlContext& context, std::string workerId, std::string versionId,
    std::optional<std::vector<RouteDefinition>> routes);
kj::Promise<std::string> controlRemove(ControlContext& context, std::string workerId);
kj::Promise<std::string> controlLimits(ControlContext& context, std::string workerId, WorkerLimits limits);
kj::Promise<std::string> controlDeleteVersion(ControlContext& context, std::string workerId, std::string versionId);
kj::Promise<std::string> controlSecretPut(ControlContext& context, std::string workerId, std::string name, std::string value);
kj::Promise<std::string> controlSecretDelete(ControlContext& context, std::string workerId, std::string name);
kj::Promise<std::string> controlSecretList(ControlContext& context, std::string workerId);
// A tenant's deletion cascade over the store's objects (idempotent): the objects no other tenant's definition
// references -- definitions, packages, modules, bundles.
kj::Promise<std::string> controlDeleteTenantObjects(ControlContext& context, std::string tenant);

// What a tenant's quotas are checked against (one read): its Workers, each Worker's versions, its secrets.
struct TenantCounts {
  uint64_t revision = 0;
  std::map<std::string, std::set<std::string>> versions;  // full name -> its versions (every Worker the state names)
  std::map<std::string, std::set<std::string>> secrets;   // full name -> its secrets' names
  uint64_t secretTotal = 0;
  bool hasWorker(const std::string& workerId) const { return versions.contains(workerId); }
};
TenantCounts tenantCounts(const ManifestStore& state, const std::string& tenant);

int runDeploymentTool(int argc, char** argv);

// ---- tenants.c++: tenants, credentials, usage and the nodes' records ----
// A /v1 answer: 200 with a JSON body, or an error status with its code and reason (main.c++ cleans the reason).
struct ApiAnswer {
  uint status = 200;
  std::string code, reason, body;
  static ApiAnswer ok(std::string body) { return {200, {}, {}, std::move(body)}; }
  static ApiAnswer error(uint status, std::string code, std::string reason) { return {status, std::move(code), std::move(reason), {}}; }
};
// Who a request is: the administrator (the configured credential), or a tenant (a credential of its own).
struct Caller {
  bool admin = false;
  std::string tenant, keyId;
  bool mayAct(const std::string& on) const { return admin || tenant == on; }
};
// `Authorization: Bearer <credential>`: the administrator's credential, else a live tenant credential (its record
// from the mirror of the credentials index, which this process updates as it writes, so a revocation takes effect at
// once). An answer when it is neither (401).
struct Authenticated { std::optional<Caller> caller; ApiAnswer refusal; std::string kind; };
Authenticated authenticate(const ManifestStore& state, const std::string& adminCredential, const kj::HttpHeaders& headers);

// A tenant's record (none: no such tenant -- not in the index, or a manifest without a record).
struct ReadTenant { std::optional<TenantRecord> tenant; uint64_t manifestRevision = 0, revision = 0; };
ReadTenant readTenant(const ManifestStore& state, const std::string& tenant);
kj::Promise<ApiAnswer> createTenant(ManifestStore& state, std::string body);
ApiAnswer listTenants(const ManifestStore& state);
kj::Promise<ApiAnswer> updateTenant(ManifestStore& state, std::string tenant, std::string body);
// The deletion (control-plane-api.md section 5): a revision that marks it deleting (the nodes stop serving it), the
// objects only it uses, its credentials, then it leaves the index -- its routes and pointers with it -- and its manifests
// go. Each step is recorded in the record's `deletion`; running it again resumes.
kj::Promise<ApiAnswer> deleteTenant(ControlContext& context, std::string tenant);
kj::Promise<ApiAnswer> createCredential(ManifestStore& state, std::string tenant, std::string body);
ApiAnswer listCredentials(const ManifestStore& state, std::string tenant);
kj::Promise<ApiAnswer> revokeCredential(ManifestStore& state, std::string tenant, std::string keyId);
// Hourly usage summed over every node's objects nodes/<nodeId>/usage/<hour> (history does not depend on a node still
// being there): of one tenant, or with none of every tenant. `from`/`to` are Unix milliseconds (to exclusive). One listing
// of nodes/ (and one of members/: the answer's nodes are the ones with objects under either); the hours in range are
// read only where this process has no copy of the object's current ETag. Throws when the store cannot be read.
struct UsageCache {
  std::map<std::string, std::pair<std::string, NodeUsageHour>> objects;  // key -> (ETag, the object)
  uint64_t reads = 0;
};
kj::Promise<ApiAnswer> usage(ObjectStore& storage, UsageCache& cache, std::optional<std::string> tenant, uint64_t from,
    uint64_t to);
// The usage objects older than retentionHours at `nowUnixMs` (every node's: a node that left removes none of its own any
// more), removed; the control plane runs it at its start and then once a day. 0 hours keeps everything.
struct UsageCleanup {
  std::string cutoff;  // the first hour kept
  uint64_t listed = 0, removed = 0;
  std::string json() const;
};
kj::Promise<UsageCleanup> cleanUsage(ObjectStore& storage, uint32_t retentionHours, uint64_t nowUnixMs);

// ---- rotation-tool.c++ ----
int runRotationTool(int argc, char** argv);

}  // namespace coworkerd

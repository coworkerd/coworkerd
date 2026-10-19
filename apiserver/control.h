#pragma once

// The control plane's operations (by tenant), behind /v1 (main.c++): the deployment tool's own
// (deployment-tool.c++), tenants, credentials and usage (tenants.c++) and etcd's users and roles (etcd-auth.c++). The
// deployment operations answer {"ok":true,"record":...} or {"ok":false,"kind","reason","record"}; main.c++ maps a
// reason to a /v1 code. Worker names are full names, <tenant>/<worker>.
#include "deployment.h"
#include "etcd.h"
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
  EtcdClient& etcd;
  ObjectStore& storage;
  kj::Timer& timer;
  kj::Network& network;
  const kj::HttpHeaderTable& headers;
  std::string token;  // the nodes' management token (trial loads)
};

// ---- deployment-tool.c++ ----
// A tenant's Workers (or one of them): versions in upload order, the active version, validity, limits, routes,
// secrets' names and lengths.
kj::Promise<std::string> controlWorkers(EtcdClient& client, std::string tenant, std::optional<std::string> workerId = std::nullopt);
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
// A tenant's deletion cascade over the deployment state (idempotent): its routes, pointers, the objects no other
// tenant's definition references, then every key below deploy/<tenant>/.
kj::Promise<std::string> controlDeleteTenantState(ControlContext& context, std::string tenant);

// What a tenant's quotas are checked against (one read): its Workers, each Worker's versions, its secrets.
struct TenantCounts {
  uint64_t revision = 0;
  std::map<std::string, std::set<std::string>> versions;  // full name -> its versions (every Worker the state names)
  std::map<std::string, std::set<std::string>> secrets;   // full name -> its secrets' names
  uint64_t secretTotal = 0;
  bool hasWorker(const std::string& workerId) const { return versions.contains(workerId); }
};
kj::Promise<TenantCounts> tenantCounts(EtcdClient& client, std::string tenant);

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
// read from etcd for every request, so a revocation takes effect at once). An answer when it is neither (401).
struct Authenticated { std::optional<Caller> caller; ApiAnswer refusal; std::string kind; };
kj::Promise<Authenticated> authenticate(EtcdClient& client, const std::string& adminCredential, const kj::HttpHeaders& headers);

// A tenant's record and the modification revision it was read at (none: no such tenant).
struct ReadTenant { std::optional<TenantRecord> tenant; uint64_t modRevision = 0, revision = 0; };
kj::Promise<ReadTenant> readTenant(EtcdClient& client, const std::string& tenant);
kj::Promise<ApiAnswer> createTenant(EtcdClient& client, kj::Timer& timer, std::string body);
kj::Promise<ApiAnswer> listTenants(EtcdClient& client);
kj::Promise<ApiAnswer> updateTenant(EtcdClient& client, kj::Timer& timer, std::string tenant, std::string body);
// The deletion (control-plane-api.md section 5): marked deleting, then the deployment state, the objects and the
// credentials, then the record. Each step is recorded in the record's `deletion`; running it again resumes.
kj::Promise<ApiAnswer> deleteTenant(ControlContext& context, std::string tenant);
kj::Promise<ApiAnswer> createCredential(EtcdClient& client, kj::Timer& timer, std::string tenant, std::string body);
kj::Promise<ApiAnswer> listCredentials(EtcdClient& client, std::string tenant);
kj::Promise<ApiAnswer> revokeCredential(EtcdClient& client, kj::Timer& timer, std::string tenant, std::string keyId);
// Hourly usage rows summed over every node's rows (history does not depend on a node still being there): of one
// tenant, or with none of every tenant. `from`/`to` are Unix milliseconds (to exclusive).
kj::Promise<ApiAnswer> usage(EtcdClient& client, std::optional<std::string> tenant, uint64_t from, uint64_t to);
// Every node id that has keys below /coworkerd/v2/nodes/ (members and the nodes that left records behind).
kj::Promise<std::vector<std::string>> nodeIds(EtcdClient& client);

// ---- rotation-tool.c++ ----
int runRotationTool(int argc, char** argv);
// ---- etcd-auth.c++ ----
// etcd's users and roles of the v2 layout, each user's password from its file, then authentication on.
// The compose file's init runs it at every start of the cluster.
int runEtcdAuthTool(int argc, char** argv);

}  // namespace coworkerd

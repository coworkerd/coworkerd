#pragma once

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <array>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "json.h"
#include "code-package.h"
#include "tenancy.h"

namespace coworkerd {

inline constexpr size_t MAX_DEPLOYMENT_CLOSURE = 64;
// Secrets, as Cloudflare limits environment values: names per version, bytes per value.
inline constexpr size_t MAX_SECRETS = 64;
inline constexpr size_t MAX_SECRET_BYTES = 5 * 1024;

struct BindingDefinition {
  std::string name;
  std::string workerId;
  std::string entrypoint;
  // The version's own asset layer (wrangler's assets.binding): names no Worker,
  // never dispatches; workerId and entrypoint are empty.
  bool assets = false;
};

struct EnvironmentVariable { std::string name, type, value; };

struct VersionDefinition {
  // The Worker's full name, <tenant>/<worker>: part of the definition's identity, so the same code of two
  // tenants has two definitions. Bindings name full names of the same tenant.
  std::string workerId;
  std::string versionId;
  std::string compatibilityDate;
  std::string mainModule;
  std::string contentHash;
  std::string definitionHash;
  std::vector<BindingDefinition> bindings;
  std::string format = "v1";
  std::vector<std::string> compatibilityFlags;
  std::vector<EnvironmentVariable> variables;
  // Static assets (v2): present when the package carries assets; how a
  // request that matches no asset is answered when there is no Worker code.
  bool assets = false;
  std::string assetsNotFoundHandling;
  // run_worker_first: true (every request goes to the Worker first) or a list of
  // Cloudflare's path patterns ("!/..." excludes); empty and false by default.
  bool assetsRunWorkerFirst = false;
  std::vector<std::string> assetsRunWorkerFirstRules;
  // Names of the secrets the version reads from env (v2). The definition holds
  // names only; values live under secrets/<workerId>/<name>.
  std::vector<std::string> secrets;
  // Input location only; excluded from immutable identity and authority state.
  std::filesystem::path packagePath;
  std::string key() const { return workerId + "@" + versionId; }
  // A version without a main module serves static assets only.
  bool hasCode() const { return !mainModule.empty(); }
  // The ASSETS Binding, when the version declares one.
  const BindingDefinition* assetsBinding() const {
    for (const auto& binding : bindings) if (binding.assets) return &binding;
    return nullptr;
  }
};

struct VersionPin {
  std::string workerId;
  std::string versionId;
  std::string contentHash;
  std::string definitionHash;
  bool operator==(const VersionPin&) const = default;
};

using VersionPins = std::map<std::string, VersionPin>;
using SharedVersionPins = std::shared_ptr<const VersionPins>;

struct RouteDefinition {
  std::string host;
  std::string pathPrefix;
  std::string workerId;
};

struct DeploymentInput {
  std::map<std::string, std::shared_ptr<const VersionDefinition>> versions;
  VersionPins active;
  std::vector<RouteDefinition> routes;
};

bool deploymentSafeName(const std::string& value);
// A deployment name or a Worker's full name (<tenant>/<worker>): what a definition, a pin or a route may name.
// A JavaScript identifier of ASCII letters, digits, '_' and '$' (environment
// variable and secret names).
bool environmentName(const std::string& value);
std::string deploymentSha256(const std::string& bytes);
std::string definitionEncoding(const VersionDefinition& version);
std::string definitionJson(const VersionDefinition& version);
// `extra` (fields without braces, e.g. the secrets' names and lengths) is appended
// when not empty.
std::string versionStatusJson(const VersionDefinition& version, const std::string& extra = {});
std::string routesJson(const std::vector<RouteDefinition>& routes);
// A request's host as routes and tenants' hostnames are compared with it: lower case, without a final dot.
std::string requestHost(const std::string& host);
// A tenant's route under the default host * as the control plane writes it: /<tenant> becomes /<tenant>/, so that
// the prefix stays within the tenant's path space; any other prefix is kept as it is.
std::string defaultHostPrefix(const std::string& tenant, const std::string& pathPrefix);
void validateDefinition(const VersionDefinition& version);
VersionPin versionPin(const VersionDefinition& version);
// A manifest names Workers by their names in one tenant: each Worker, Binding target and route is read as
// <tenant>/<name>. The deployment tool's default is DEFAULT_TENANT.
DeploymentInput readDeploymentManifest(const std::filesystem::path& manifest, const std::string& tenant = DEFAULT_TENANT);
// Verified code and assets of a manifest's versions.
struct DeploymentCode {
  std::map<std::string, std::string> sources;  // definitionHash -> what the runtime loads
  std::map<std::string, std::string> objects;  // object-store key -> bytes (bundles/, packages/, modules/)
  std::set<std::string> assetObjects;          // keys of `objects` that hold static asset bytes
};
DeploymentCode readDeploymentCode(const DeploymentInput& input, const std::filesystem::path& codeRoot);

// The deployment state's keys (by tenant), as the tenants' manifests translate into them (tenant-manifest.h). Every route has its own key, derived from the route:
// "routes/<host><pathPrefix>" -- a host never contains '/', a prefix starts with '/', so the key names exactly one
// (host, pathPrefix) slot, across tenants; its value names the tenant and the Worker. A tenant's other keys are below
// "<tenant>/": definitions/<worker>/<version>, active/<worker>, secrets/<worker>/<name> (the value of one secret of
// a Worker, for every version that declares the name) and binders/<target>/<binder> (the tool's reverse Binding
// index; it never affects validity). "generation" is informational. Each function takes full names.
std::string deploymentRouteKey(const std::string& host, const std::string& pathPrefix);
std::string deploymentActiveKey(const std::string& workerId);
std::string deploymentDefinitionKey(const std::string& workerId, const std::string& versionId);
std::string deploymentBinderKey(const std::string& target, const std::string& binder);
std::string deploymentSecretKey(const std::string& workerId, const std::string& name);
// "<tenant>/": the prefix of a tenant's keys.
std::string deploymentTenantPrefix(const std::string& tenant);
// The active pointer {versionId, definitionHash[, limits]}: a Worker's own limits travel with it.
std::string activePointerJson(const std::string& versionId, const std::string& definitionHash, const WorkerLimits& limits = {});
std::string routeValueJson(const RouteDefinition& route);
std::shared_ptr<const VersionDefinition> decodeDeploymentDefinition(const std::string& bytes);

// One secret value as stored: {"value":<string>,"setAtUnixMs":<ms>}. A value is
// UTF-8 text without NUL, at most MAX_SECRET_BYTES. Reasons never carry the value.
struct SecretValue {
  std::string value;
  uint64_t setAtUnixMs = 0;
  std::string error;  // empty when usable
};
std::string secretRecordJson(const std::string& value, uint64_t setAtUnixMs);
SecretValue decodeSecretRecord(const std::string& bytes);
// Why a value cannot be a secret ("" when it can); names the rule, never the value.
std::string secretValueProblem(const std::string& value);

// Order-independent sum of SHA-256 element hashes modulo 2^256. Adding and
// removing one element is constant time, so a Server keeps the digest of its
// valid set and effective routes current without rescanning either.
class SetDigest {
public:
  void add(const std::string& element);
  void remove(const std::string& element);
  std::string hex() const;
  bool operator==(const SetDigest&) const = default;
private:
  std::array<uint64_t, 4> limbs{};
};

// The deployment keys of one revision of the desired state and the deterministic per-Worker
// view derived from them. A Worker is valid when its active pointer names an
// existing, well-formed definition with the same hash, every secret that
// definition declares has a usable value, every Worker reachable through
// Bindings is likewise usable, and that closure has at most 64 Workers. A
// route is effective when its key and value agree and it names a valid Worker.
// The result depends only on the keys, never on what was applied before:
// apply() decodes only changed keys and re-evaluates only the changed Workers
// and the Workers that bind them, and equals a full evaluation. A secret's
// value never affects validity or version pins, only whether it is usable.
class DeploymentState {
public:
  struct Change {
    std::string key;                   // a deployment key (routes/..., <tenant>/..., generation)
    std::optional<std::string> value;  // nullopt: deleted
  };
  // A Worker whose served definition changed: became valid, invalid, or valid
  // with another definition. `before`/`after` are null when not valid.
  struct ServedChange {
    std::string workerId;
    std::shared_ptr<const VersionDefinition> before, after;
  };
  struct ApplyStats {
    size_t keys = 0;        // changed keys in the batch
    size_t workers = 0;     // Workers whose own keys changed
    size_t evaluated = 0;   // Workers re-evaluated (changed plus upstream)
    size_t routes = 0;      // routes re-evaluated
    std::vector<ServedChange> served;
    std::vector<std::shared_ptr<const VersionDefinition>> definitions;  // newly well-formed definitions
  };
  struct Worker {
    std::string versionId, definitionHash;  // as written in the pointer
    WorkerLimits limits;                    // the pointer's own limits; empty: the tenant's, then the node's
    std::string pointerError;               // the pointer itself cannot be used
    bool own = false;                       // pointer and its definition are usable
    std::string ownReason;
    std::shared_ptr<const VersionDefinition> version;  // when own
    std::vector<std::string> targets;                  // when own: distinct targets in Binding-name order
    std::vector<std::string> missingSecrets;           // declared by the active definition, no usable value
    bool valid = false;
    std::string reason;
    SharedVersionPins closure;                         // when valid
    bool counted = false;                              // included in digest and counters
  };
  struct Route {
    RouteDefinition value;
    std::string slotHost, slotPrefix;  // from the key; slotPrefix empty when the key names no slot
    std::string error;                 // key or value problem
    bool effective = false;
    std::string reason;                // why it is not effective
    bool counted = false;
  };
  struct Definition {
    std::shared_ptr<const VersionDefinition> version;  // set when well-formed
    std::string error;
  };
  struct Selection {
    enum class Kind { SELECTED, NOT_FOUND, ROUTE_INVALID, WORKER_INVALID };
    Kind kind = Kind::NOT_FOUND;
    std::string routeKey, workerId, reason;
    std::shared_ptr<const VersionDefinition> version;
    SharedVersionPins pins;
  };

  ApplyStats apply(const std::vector<Change>& changes, bool full, uint64_t revision);
  // A manifest as a complete state; duplicate routes are rejected. A manifest
  // carries no secret values: without `requireSecretValues` declared secrets
  // count as set (compiling a manifest checks everything else).
  static DeploymentState fromInput(const DeploymentInput& input, uint64_t revision, uint64_t generation,
      bool requireSecretValues = true);

  uint64_t revision() const { return revisionValue; }
  uint64_t generation() const { return generationValue; }
  // The route of a request: the longest prefix that covers the path at a segment boundary (/acme covers /acme and
  // /acme/..., never /acme-corp/...), at each length the request's host before the default host *. `owner` is the
  // tenant the host is registered to, when it is one's: then only that tenant's routes count -- its routes on the
  // host, then its routes under * -- and another tenant's route never serves the host. A route without a readable
  // owner counts for every host (it answers ROUTE_INVALID). The host is compared as requestHost() gives it.
  Selection route(const std::string& host, const std::string& path, const std::string* owner = nullptr) const;
  const Worker* worker(const std::string& id) const;
  bool valid(const std::string& id) const;
  const std::map<std::string, Worker>& workers() const { return workerMap; }
  const std::map<std::string, Route>& routes() const { return routeMap; }
  const std::map<std::string, Definition>& definitions() const { return definitionMap; }
  const std::map<std::string, std::set<std::string>>& routesByWorker() const { return workerRoutes; }
  std::shared_ptr<const VersionDefinition> definition(const std::string& label) const;
  size_t validCount() const { return validWorkers; }
  size_t effectiveRouteCount() const { return effectiveRoutes; }
  size_t maxClosureSize() const { return closureSizes.empty() ? 0 : closureSizes.rbegin()->first; }
  // First invalid Worker or route; empty when the whole state is usable. With a tenant, only that tenant's
  // Workers and routes count.
  std::string firstProblem(const std::string& tenant = {}) const;
  const SetDigest& validDigest() const { return validSet; }
  const SetDigest& routeDigest() const { return routeSet; }
  // Status fields (no braces): revision, digests, counts and up to `limit`
  // invalid Workers, routes and unknown keys with reasons.
  std::string statusFields(size_t limit) const;
  // The usable value of a Worker's secret, or null.
  const SecretValue* secret(const std::string& workerId, const std::string& name) const;
  // The secrets a version declares that have no usable value for its Worker.
  std::vector<std::string> missingSecrets(const VersionDefinition& version) const;
  // `"secrets":[{"name":..,"bytes":..},...]` for a version that declares secrets
  // (bytes null without a usable value), empty otherwise: names and lengths only.
  std::string secretsStatusField(const VersionDefinition& version) const;
  // Every secret key's record by Worker and name, usable or not.
  const std::map<std::string, std::map<std::string, SecretValue>>& secrets() const { return secretMap; }

private:
  uint64_t revisionValue = 0;
  uint64_t generationValue = 0;
  std::string generationError;
  std::map<std::string, Definition> definitionMap;   // label workerId@versionId
  std::map<std::string, Worker> workerMap;           // every Worker with an active key
  std::map<std::string, Route> routeMap;              // key without "routes/"
  std::map<std::string, std::set<std::string>> workerRoutes;   // route value workerId -> keys
  std::map<std::string, std::set<std::string>> binders;        // target -> own-usable Workers binding it
  std::map<size_t, size_t> slotLengths;                        // prefix length -> slots
  std::map<size_t, size_t> closureSizes;                       // closure size -> valid Workers
  std::set<std::string> invalidWorkers, invalidRoutes, invalidDefinitions;
  std::set<std::string> unknownKeys, binderKeys;
  std::map<std::string, std::map<std::string, SecretValue>> secretMap;  // workerId -> name -> record
  size_t secretKeys = 0, unusableSecrets = 0;
  bool requireSecretValues = true;
  size_t validWorkers = 0, effectiveRoutes = 0;
  SetDigest validSet, routeSet;

  void retract(const std::string& id);
  void refreshOwn(const std::string& id);
  void evaluate(const std::string& id, ApplyStats& stats);
  void evaluateRoute(const std::string& key);
  void setRoute(const std::string& key, const std::optional<std::string>& value);
};

// Local index of definitions by hash, append-only. Identity is the hash; a
// label only distinguishes "known label, other hash" from "unknown label", and
// it lasts while the desired state names it: a label no manifest names any
// more -- a version deleted, a tenant deleted or created again -- is forgotten
// (retain), so that another definition may take it.
class DefinitionIndex {
public:
  // Validates and adds; a known hash is ignored.
  void remember(std::shared_ptr<const VersionDefinition> version);
  // The labels the applied desired state names, each with its definition's hash: a label it names with that hash only,
  // every other label forgotten (its definitions stay, by hash). Returns how many labels were forgotten.
  size_t retain(const std::map<std::string, std::string>& named);
  std::shared_ptr<const VersionDefinition> find(const VersionPin& pin, std::string& reason) const;
  std::shared_ptr<const VersionDefinition> byHash(const std::string& hash) const;
  // The definition with this label: by its hash when given (whose definition must carry the label), else the one hash
  // the label names.
  std::shared_ptr<const VersionDefinition> exact(const std::string& label, const std::string& hash = {}) const;
  size_t size() const { return hashes.size(); }
  size_t labelCount() const { return labels.size(); }

private:
  std::map<std::string, std::shared_ptr<const VersionDefinition>> hashes;
  std::map<std::string, std::set<std::string>> labels;
};

// The static assets of one package, by request path.
struct AssetIndex {
  std::map<std::string, CodeAsset> byPath;
  uint64_t bytes = 0;
  const CodeAsset* find(const std::string& path) const {
    auto found = byPath.find(path);
    return found == byPath.end() ? nullptr : &found->second;
  }
};
using SharedAssetIndex = std::shared_ptr<const AssetIndex>;
// The asset path of a request path, decoded the way Cloudflare's asset layer
// decodes it: '\\' as '/', dot segments removed, each segment percent-decoded
// (a segment that does not decode stays as it is), repeated slashes collapsed.
std::string assetRequestPath(const std::string& rawPath);
// What the asset layer answers for a decoded path: the lookups of wrangler's
// default html_handling ("auto-trailing-slash"), where Cloudflare would
// redirect to an equivalent URL the asset is served directly instead, then
// `notFoundHandling` ("none" for a version with a Worker: misses go to it).
// No asset: Cloudflare has no intent for the path.
struct AssetIntent {
  const CodeAsset* asset = nullptr;
  unsigned status = 200;
  bool fallback = false;  // served by not_found_handling
};
AssetIntent resolveAsset(const AssetIndex& index, const std::string& path, const std::string& notFoundHandling);
// The URL pathname of a request path as a URL parser yields it (what
// Cloudflare's router matches run_worker_first rules against): '\\' as '/',
// dot segments removed, percent-encoding kept.
std::string assetUrlPath(const std::string& rawPath);
// Cloudflare's run_worker_first rule: '*' matches any characters, everything
// else literally, over the whole pathname.
bool assetRuleMatches(const std::string& rule, const std::string& urlPath);
// Where the asset layer sends one request of a version with assets, as
// Cloudflare's router does (wrangler 4.136.1 router-worker and asset-worker
// canFetch): a version without code answers everything from its assets;
// otherwise "!" rules route to the assets, the other rules and
// run_worker_first: true to the Worker, and the rest to the assets when they
// have an intent for the path. That intent counts not_found_handling only
// with rules, or for a navigation request (Sec-Fetch-Mode: navigate) when
// assets_navigation_prefers_asset_serving is on.
struct AssetRoute {
  enum class Target { ASSETS, WORKER } target = Target::ASSETS;
  enum class Reason { NO_CODE, EXCLUDED, RUN_WORKER_FIRST, RULE, INTENT, NAVIGATION, MISS } reason = Reason::NO_CODE;
};
AssetRoute routeAsset(const VersionDefinition& version, const AssetIndex& index, const std::string& rawPath,
    bool navigate);
// assets_navigation_prefers_asset_serving: on by default from compatibility date
// 2025-04-01, off with assets_navigation_has_no_effect.
bool assetsNavigationPrefersAssets(const VersionDefinition& version);
struct AssetRead {
  std::string bytes;
  std::string reason;  // empty when the bytes are verified
};

}  // namespace coworkerd

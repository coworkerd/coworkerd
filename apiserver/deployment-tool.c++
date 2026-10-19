#include "control.h"
#include "deployment.h"
#include "coworkerd.capnp.h"
#include "object-store.h"
#include "code-package.h"
#include "support.h"

#include <capnp/message.h>

#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <functional>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>

namespace coworkerd {
namespace {
using namespace etcd;
namespace rpc = v1::rpc;

uint64_t nowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

// Per-Worker maps in the full status are bounded; tests that need every
// Worker use the counts and digests.
constexpr size_t STATUS_PROBLEM_LIMIT = 20;

// ---------------------------------------------------------------------------
// Deployment tool: per-key writes conditioned only on the keys of the change.

std::string absolute(const std::string& relative) { return std::string(DEPLOYMENT_PREFIX) + relative; }

// A refusal reads as one sentence: "coworkerd-apiserver failed: <reason>".
[[noreturn]] void refuse(const std::string& reason) {
  kj::throwFatalException(kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::str(reason.c_str())));
}

struct Transaction {
  std::vector<std::string> compares, operations;
  std::vector<DeploymentState::Change> changes;  // the operations' effect, relative keys
  std::set<std::string> written;
  void put(const std::string& key, const std::string& value) {
    KJ_REQUIRE(written.insert(key).second, "internal: key written twice in one transaction", key.c_str());
    operations.push_back("{\"request_put\":{\"key\":" + quote(base64(absolute(key))) + ",\"value\":" + quote(base64(value)) + "}}");
    changes.push_back({key, value});
  }
  void remove(const std::string& key) {
    KJ_REQUIRE(written.insert(key).second, "internal: key written twice in one transaction", key.c_str());
    operations.push_back("{\"request_delete_range\":{\"key\":" + quote(base64(absolute(key))) + "}}");
    changes.push_back({key, std::nullopt});
  }
  // The key is as read: same modification revision, or still absent.
  void unchanged(const std::string& key, const EtcdKv* kv) {
    if (kv) compares.push_back("{\"target\":\"MOD\",\"result\":\"EQUAL\",\"key\":" + quote(base64(absolute(key))) +
        ",\"mod_revision\":\"" + std::to_string(kv->modRevision) + "\"}");
    else compares.push_back("{\"target\":\"VERSION\",\"result\":\"EQUAL\",\"key\":" + quote(base64(absolute(key))) +
        ",\"version\":\"0\"}");
  }
  // The key still exists from the same creation (updates are allowed).
  void sameCreation(const std::string& key, const EtcdKv& kv) {
    compares.push_back("{\"target\":\"CREATE\",\"result\":\"EQUAL\",\"key\":" + quote(base64(absolute(key))) +
        ",\"create_revision\":\"" + std::to_string(kv.createRevision) + "\"}");
  }
  // No key below the prefix was created or modified after `revision`.
  void untouchedSince(const std::string& prefix, uint64_t revision) {
    auto end = absolute(prefix);
    ++end.back();
    compares.push_back("{\"target\":\"MOD\",\"result\":\"LESS\",\"key\":" + quote(base64(absolute(prefix))) +
        ",\"range_end\":" + quote(base64(end)) + ",\"mod_revision\":\"" + std::to_string(revision + 1) + "\"}");
  }
  size_t size() const { return std::max(compares.size(), operations.size()); }
  std::string json() const {
    std::string out = "{\"compare\":[";
    for (size_t i = 0; i < compares.size(); ++i) { if (i) out += ','; out += compares[i]; }
    out += "],\"success\":[";
    for (size_t i = 0; i < operations.size(); ++i) { if (i) out += ','; out += operations[i]; }
    return out + "]}";
  }
};

const EtcdKv* kvOf(const EtcdSnapshot& raw, const std::string& relative) {
  auto found = raw.values.find(absolute(relative));
  return found == raw.values.end() ? nullptr : &found->second;
}

DeploymentState stateOf(const EtcdSnapshot& raw) {
  const std::string prefix = DEPLOYMENT_PREFIX;
  std::vector<DeploymentState::Change> changes;
  changes.reserve(raw.values.size());
  for (const auto& [key, kv] : raw.values) changes.push_back({key.substr(prefix.size()), kv.value});
  DeploymentState state;
  state.apply(changes, true, raw.revision);
  return state;
}

// Binder index keys currently naming `binder`, by target (below the binder's tenant, full names).
std::map<std::string, std::string> binderKeysOf(const EtcdSnapshot& raw, const std::string& binder) {
  std::map<std::string, std::string> found;
  const auto tenant = tenantOf(binder), name = workerNameOf(binder);
  const std::string prefix = absolute(deploymentTenantPrefix(tenant) + "binders/");
  for (auto it = raw.values.lower_bound(prefix); it != raw.values.end() && it->first.starts_with(prefix); ++it) {
    auto rest = it->first.substr(prefix.size());
    auto slash = rest.find('/');
    if (slash != std::string::npos && rest.substr(slash + 1) == name)
      found.emplace(workerFullId(tenant, rest.substr(0, slash)), it->first.substr(std::string(DEPLOYMENT_PREFIX).size()));
  }
  return found;
}

// The keys a change of one tenant reads: the tenant's own, every route (a route's key is global, its owner
// in its value) and the generation -- three ranges at one revision. Bindings stay within a tenant, so the
// validity of the tenant's Workers is complete in it; other tenants' routes show as naming absent Workers.
kj::Promise<EtcdSnapshot> tenantSnapshot(EtcdClient& client, const std::string& tenant) {
  auto raw = co_await client.range(absolute(deploymentTenantPrefix(tenant)));
  auto routes = co_await client.range(absolute("routes/"), raw.revision);
  auto generation = co_await client.range(absolute("generation"), raw.revision);
  for (auto& [key, kv] : routes.values) raw.values.emplace(key, std::move(kv));
  // A prefix read: "generation" itself only (a tenant may be called "generations").
  if (auto found = generation.values.find(absolute("generation")); found != generation.values.end())
    raw.values.emplace(found->first, std::move(found->second));
  co_return raw;
}

enum class Operation { APPLY, UPSERT, REMOVE };

struct ToolOptions {
  // The tenant whose Workers the change names; the manifest's names are read in it.
  std::string tenant = DEFAULT_TENANT;
  std::filesystem::path manifest, codeRoot;
  uint64_t pauseMs = 0;
  uint32_t maxRetries = 0;
  uint32_t maxTxnOps = 128;
  std::vector<std::string> workers;  // remove
  // Trial loads: on unless --no-trial. A named Server (--trial-server
  // URL) or else the cluster's members, Home first; a member's advertised
  // address may be mapped to the address this tool dials (--trial-address-map).
  bool trial = true;
  std::string trialServer;
  uint32_t trialTimeoutMs = 5000;
  std::map<std::string, std::string> trialAddressMap;
  // The Servers' management token, sent with every trial load: --token, else
  // COWORKERD_MANAGEMENT_TOKEN.
  std::string token;
  // The control plane. An upload publishes the manifest's version (objects and definition)
  // and moves no pointer; an activation moves a Worker's pointer to a version published before.
  bool publishOnly = false;
  // An activating upload whose manifest lists no routes keeps the Worker's current ones.
  bool keepRoutes = false;
  struct Activation {
    // An empty versionId is the version the Worker's pointer names at each attempt (PUT routes).
    std::string workerId, versionId;
    // The Worker's routes after the change; unset keeps the routes it has.
    std::optional<std::vector<RouteDefinition>> routes;
  };
  std::optional<Activation> activation;
};

struct Plan {
  Transaction txn;
  std::vector<DeploymentState::Change> definitions;  // to publish before the flip
  std::vector<std::string> workers;                  // Workers whose pointers this change writes
};

// Pointer, routes and Binding index of one Worker in its new state.
void writeWorker(Plan& plan, const EtcdSnapshot& raw, const DeploymentState& before,
    const std::string& id, const VersionDefinition& version, const std::vector<RouteDefinition>& routes,
    const std::set<std::string>& batch, const std::map<std::string, std::set<std::string>>& claimed,
    const std::set<std::string>& removed) {
  auto& txn = plan.txn;
  const auto pointerKey = deploymentActiveKey(id);
  txn.unchanged(pointerKey, kvOf(raw, pointerKey));
  // The Worker's own limits stay with its pointer whatever version it names.
  const auto* current = before.worker(id);
  txn.put(pointerKey, activePointerJson(version.versionId, version.definitionHash, current ? current->limits : WorkerLimits{}));
  plan.workers.push_back(id);
  std::set<std::string> wanted;
  for (const auto& route : routes) {
    const auto key = deploymentRouteKey(route.host, route.pathPrefix);
    const auto relative = key.substr(7);
    wanted.insert(relative);
    auto existing = before.routes().find(relative);
    const auto* kv = kvOf(raw, key);
    const auto bytes = routeValueJson(route);
    if (existing != before.routes().end()) {
      const auto& owner = existing->second.value.workerId;
      if (owner == id && kv && kv->value == bytes) continue;
      // A route keeps its owner unless the owner is part of this change and
      // no longer lists it, or is removed by it. An unreadable value has no
      // owner and may be repaired.
      if (!(owner.empty() || owner == id || removed.contains(owner) || (batch.contains(owner) &&
          !(claimed.contains(owner) && claimed.at(owner).contains(relative)))))
        refuse("route " + relative + " belongs to Worker " + owner + "; include that Worker in the same upsert to move it");
    }
    txn.unchanged(key, kv);
    txn.put(key, bytes);
  }
  auto owned = before.routesByWorker().find(id);
  if (owned != before.routesByWorker().end()) for (const auto& relative : owned->second) {
    if (wanted.contains(relative)) continue;
    bool movedInBatch = false;
    for (const auto& [other, keys] : claimed) if (other != id && keys.contains(relative)) movedInBatch = true;
    if (movedInBatch) continue;
    const auto key = "routes/" + relative;
    txn.unchanged(key, kvOf(raw, key));
    txn.remove(key);
  }
  // The secrets the version declares were validated as set at the read; each
  // must still exist from the same creation (a new value is fine), so a
  // concurrent `secret delete` and this flip cannot both commit.
  for (const auto& name : version.secrets) {
    const auto key = deploymentSecretKey(id, name);
    if (const auto* kv = kvOf(raw, key)) txn.sameCreation(key, *kv);
  }
  std::set<std::string> targets;
  for (const auto& binding : version.bindings) if (!binding.assets) targets.insert(binding.workerId);
  for (const auto& target : targets) {
    // Every write of a binder refreshes its index keys, so a concurrent removal
    // of the target sees the change; the target must still be the same Worker.
    txn.put(deploymentBinderKey(target, id), activePointerJson(version.versionId, version.definitionHash));
    if (batch.contains(target)) continue;
    const auto* kv = kvOf(raw, deploymentActiveKey(target));
    if (kv) txn.sameCreation(deploymentActiveKey(target), *kv);
  }
  for (const auto& [target, key] : binderKeysOf(raw, id)) if (!targets.contains(target)) txn.remove(key);
}

void removeWorker(Plan& plan, const EtcdSnapshot& raw, const DeploymentState& before, const std::string& id,
    const std::set<std::string>& taken, bool guardBinders = true) {
  auto& txn = plan.txn;
  const auto pointerKey = deploymentActiveKey(id);
  const auto* pointer = kvOf(raw, pointerKey);
  txn.unchanged(pointerKey, pointer);
  if (pointer) txn.remove(pointerKey);
  auto owned = before.routesByWorker().find(id);
  if (owned != before.routesByWorker().end()) for (const auto& relative : owned->second) {
    if (taken.contains(relative)) continue;  // another Worker of this change writes it
    const auto key = "routes/" + relative;
    txn.unchanged(key, kvOf(raw, key));
    txn.remove(key);
  }
  for (const auto& [target, key] : binderKeysOf(raw, id)) txn.remove(key);
  // A Worker that started binding this one after the read refreshed its index
  // key; the removal must then be re-validated.
  if (guardBinders) txn.untouchedSince(deploymentTenantPrefix(tenantOf(id)) + "binders/" + workerNameOf(id) + "/", raw.revision);
}

std::string preflightDefinitions(const EtcdSnapshot& raw, const DeploymentInput& desired, Plan& plan) {
  for (const auto& [label, version] : desired.versions) {
    const auto key = deploymentDefinitionKey(version->workerId, version->versionId);
    const auto* existing = kvOf(raw, key);
    const auto bytes = definitionJson(*version);
    if (existing && existing->value != bytes)
      refuse("deployment label conflict: existing definition bytes differ for " + label + "; publish changes under a new versionId");
    if (!existing) plan.definitions.push_back({key, bytes});
  }
  return {};
}

// Every Worker valid before the change stays valid unless the change removes it.
void requireNoCollateral(const DeploymentState& before, const DeploymentState& after,
    const std::set<std::string>& removed, const char* operation) {
  for (const auto& [id, worker] : before.workers()) {
    if (!worker.valid || removed.contains(id) || after.valid(id)) continue;
    const auto* now = after.worker(id);
    refuse(std::string(operation) + " would make valid Worker " + id + " invalid: " +
        (now ? now->reason : std::string("its active pointer would be removed")));
  }
}

void requireSize(const Transaction& txn, uint32_t limit) {
  if (txn.size() > limit)
    refuse("change needs " + std::to_string(txn.size()) + " operations in one etcd transaction (" +
        std::to_string(txn.compares.size()) + " comparisons, " + std::to_string(txn.operations.size()) +
        " writes), more than the limit " + std::to_string(limit) +
        "; split it, or raise --max-txn-ops to match the etcd server's --max-txn-ops");
}

Plan plan(Operation operation, const EtcdSnapshot& raw, const DeploymentState& before,
    const DeploymentInput& desired, const ToolOptions& options, bool checkSize = true, bool bumpGeneration = true) {
  Plan result;
  std::set<std::string> removed;
  if (operation == Operation::REMOVE) {
    for (const auto& id : options.workers) {
      if (!before.worker(id) && !before.routesByWorker().contains(id)) refuse("Worker " + id + " is not deployed");
      removed.insert(id);
    }
    for (const auto& id : removed) removeWorker(result, raw, before, id, {});
  } else {
    preflightDefinitions(raw, desired, result);
    std::map<std::string, std::vector<RouteDefinition>> routes;
    std::map<std::string, std::set<std::string>> claimed;
    std::set<std::string> keys;
    for (const auto& route : desired.routes) {
      if (route.host.empty() || route.host.find('/') != std::string::npos)
        refuse("route host must be non-empty and contain no '/': " + route.host);
      if (!route.pathPrefix.starts_with('/')) refuse("route pathPrefix must start with /: " + route.pathPrefix);
      if (!keys.insert(route.host + route.pathPrefix).second) refuse("duplicate route " + route.host + route.pathPrefix);
      if (!desired.active.contains(route.workerId))
        refuse(std::string(operation == Operation::UPSERT ? "route names a Worker that this upsert does not activate: "
                                                          : "route Worker has no active version: ") + route.workerId);
      routes[route.workerId].push_back(route);
      claimed[route.workerId].insert(route.host + route.pathPrefix);
    }
    std::set<std::string> batch;
    for (const auto& [id, pin] : desired.active) batch.insert(id);
    if (operation == Operation::APPLY) {
      if (desired.routes.empty()) refuse("at least one route is required");
      // The tenant's whole state: its Workers and routes outside the manifest are removed; other tenants'
      // are not touched.
      const auto& tenant = options.tenant;
      for (const auto& [id, worker] : before.workers()) if (tenantOf(id) == tenant && !batch.contains(id)) removed.insert(id);
      for (const auto& [id, owned] : before.routesByWorker())
        if (tenantOf(id) == tenant && !batch.contains(id)) removed.insert(id);
      // One guard over the tenant's Binding index keeps a large whole-state
      // removal within one transaction; apply has no retry anyway.
      for (const auto& id : removed) removeWorker(result, raw, before, id, keys, false);
      if (!removed.empty()) result.txn.untouchedSince(deploymentTenantPrefix(tenant) + "binders/", raw.revision);
      for (const auto& id : batch) {
        // A Worker whose pointer and routes already match is not rewritten.
        const auto& pin = desired.active.at(id);
        const auto* pointer = kvOf(raw, deploymentActiveKey(id));
        const auto* existing = before.worker(id);
        bool same = pointer && pointer->value == activePointerJson(pin.versionId, pin.definitionHash,
            existing ? existing->limits : WorkerLimits{});
        auto owned = before.routesByWorker().find(id);
        auto current = owned == before.routesByWorker().end() ? std::set<std::string>() : owned->second;
        same = same && current == (claimed.contains(id) ? claimed.at(id) : std::set<std::string>());
        for (const auto& route : routes[id]) {
          const auto* kv = kvOf(raw, deploymentRouteKey(route.host, route.pathPrefix));
          same = same && kv && kv->value == routeValueJson(route);
        }
        if (!same) writeWorker(result, raw, before, id, *desired.versions.at(id + "@" + pin.versionId),
            routes[id], batch, claimed, removed);
      }
      // Anything else under routes/ of this tenant (or without a readable owner) that the manifest does not list.
      for (const auto& [key, route] : before.routes()) {
        const auto relative = "routes/" + key;
        if (result.txn.written.contains(relative) || keys.contains(key)) continue;
        if (!route.value.workerId.empty() && tenantOf(route.value.workerId) != tenant) continue;
        result.txn.unchanged(relative, kvOf(raw, relative));
        result.txn.remove(relative);
      }
    } else {
      for (const auto& id : batch) {
        const auto& pin = desired.active.at(id);
        writeWorker(result, raw, before, id, *desired.versions.at(id + "@" + pin.versionId), routes[id], batch, claimed, removed);
      }
    }
  }
  if (bumpGeneration) result.txn.put("generation", quote(std::to_string(before.generation() + 1)));
  // Validate exactly what will be written, with the shared evaluation.
  DeploymentState after = before;
  std::vector<DeploymentState::Change> simulated = result.definitions;
  simulated.insert(simulated.end(), result.txn.changes.begin(), result.txn.changes.end());
  after.apply(simulated, false, raw.revision);
  const char* name = operation == Operation::APPLY ? "apply" : operation == Operation::UPSERT ? "upsert" : "remove";
  if (operation == Operation::APPLY) {
    auto problem = after.firstProblem(options.tenant);
    if (!problem.empty()) refuse("apply rejected: " + problem);
  } else {
    for (const auto& [id, pin] : desired.active) {
      const auto* worker = after.worker(id);
      if (!worker || !worker->valid) refuse("Worker " + id + " would be invalid after upsert: " + (worker ? worker->reason : std::string()));
    }
    for (const auto& route : desired.routes) {
      auto found = after.routes().find(route.host + route.pathPrefix);
      if (found == after.routes().end() || !found->second.effective)
        refuse("route " + route.host + route.pathPrefix + " would not be effective: " +
            (found != after.routes().end() ? found->second.reason : std::string()));
    }
    requireNoCollateral(before, after, removed, name);
  }
  if (checkSize) requireSize(result.txn, options.maxTxnOps);
  return result;
}

// An initial whole-state deployment too large for one transaction is written
// as upserts of new Workers, Binding targets first and every Binding cycle in
// one step. Only used when the deployment state is empty: nothing is served
// yet, so no request can observe a partly written state it relied on.
std::vector<DeploymentInput> stages(const DeploymentInput& desired, uint32_t limit) {
  std::map<std::string, std::vector<std::string>> targets;
  std::map<std::string, size_t> routes;
  for (const auto& [id, pin] : desired.active) {
    for (const auto& binding : desired.versions.at(id + "@" + pin.versionId)->bindings)
      if (desired.active.contains(binding.workerId)) targets[id].push_back(binding.workerId);
  }
  for (const auto& route : desired.routes) ++routes[route.workerId];
  // Tarjan emits each strongly connected component after every component it reaches.
  std::map<std::string, size_t> index, low;
  std::vector<std::string> stack;
  std::set<std::string> onStack;
  std::vector<std::vector<std::string>> units;
  size_t counter = 0;
  std::function<void(const std::string&)> connect = [&](const std::string& id) {
    index[id] = low[id] = counter++;
    stack.push_back(id); onStack.insert(id);
    for (const auto& target : targets[id]) {
      if (!index.contains(target)) { connect(target); low[id] = std::min(low[id], low[target]); }
      else if (onStack.contains(target)) low[id] = std::min(low[id], index[target]);
    }
    if (low[id] != index[id]) return;
    std::vector<std::string> unit;
    while (true) {
      auto top = stack.back(); stack.pop_back(); onStack.erase(top);
      unit.push_back(top);
      if (top == id) break;
    }
    units.push_back(std::move(unit));
  };
  for (const auto& [id, pin] : desired.active) if (!index.contains(id)) connect(id);
  std::vector<DeploymentInput> result;
  DeploymentInput current;
  size_t writes = 1, compares = 0;  // the generation write of the last step
  auto flush = [&] { if (!current.active.empty()) result.push_back(std::move(current)); current = {}; writes = 1; compares = 0; };
  for (const auto& unit : units) {
    size_t unitWrites = 0, unitCompares = 0;
    for (const auto& id : unit) {
      const auto& version = *desired.versions.at(id + "@" + desired.active.at(id).versionId);
      const auto bindings = version.bindings.size();
      unitWrites += 1 + routes[id] + bindings;
      unitCompares += 1 + routes[id] + bindings + version.secrets.size();
    }
    if (std::max(unitWrites + 1, unitCompares) > limit)
      refuse("a Binding cycle of " + std::to_string(unit.size()) + " Workers does not fit one etcd transaction");
    if (std::max(writes + unitWrites, compares + unitCompares) > limit) flush();
    for (const auto& id : unit) {
      const auto& pin = desired.active.at(id);
      current.active.emplace(id, pin);
      for (const auto& [label, version] : desired.versions) if (version->workerId == id) current.versions.emplace(label, version);
      for (const auto& route : desired.routes) if (route.workerId == id) current.routes.push_back(route);
    }
    writes += unitWrites; compares += unitCompares;
  }
  flush();
  return result;
}

// The immutable objects first (then the trial loads, then the definition keys -- a version exists in the
// deployment state only once it loads). A failed PUT leaves only immutable objects, never a partly published
// deployment.
kj::Promise<void> publishObjects(ObjectStore& storage, const DeploymentInput& desired, const DeploymentCode& code,
    std::ostream& report) {
  const auto uploadStartedAtUnixMs = nowUnixMs();
  uint64_t uploaded = 0, skipped = 0, uploadedModules = 0, skippedModules = 0, uploadedAssets = 0, skippedAssets = 0;
  std::map<std::string, const std::string*> objects;
  std::map<std::string, std::string> definitions;
  for (const auto& [label, version] : desired.versions) {
    auto entry = definitions.emplace("definitions/" + version->definitionHash, definitionJson(*version)).first;
    objects.emplace(entry->first, &entry->second);
  }
  // The manifest's code objects: bundles/, packages/ and modules/ (code module
  // and static asset bytes share modules/<SHA-256>).
  for (const auto& [key, bytes] : code.objects) objects.emplace(key, &bytes);
  for (const auto& [key, bytes] : objects) {
    const bool module = key.starts_with("modules/"), asset = code.assetObjects.contains(key);
    if (co_await storage.exists(key)) { ++skipped; skippedModules += module; skippedAssets += asset; }
    else { co_await storage.put(key, *bytes); ++uploaded; uploadedModules += module; uploadedAssets += asset; }
  }
  const auto uploadFinishedAtUnixMs = nowUnixMs();
  report << ",\"uploadStartedAtUnixMs\":" << uploadStartedAtUnixMs << ",\"uploadFinishedAtUnixMs\":" << uploadFinishedAtUnixMs
         << ",\"uploadedObjects\":" << uploaded << ",\"skippedObjects\":" << skipped
         << ",\"uploadedModules\":" << uploadedModules << ",\"skippedModules\":" << skippedModules;
  // Asset objects are reported only for deployments with assets, so the output
  // of code-only deployments keeps its fields.
  if (!code.assetObjects.empty()) report << ",\"uploadedAssets\":" << uploadedAssets << ",\"skippedAssets\":" << skippedAssets;
}

// The definition keys of a change: each created only where none is (an existing one must hold the same bytes).
kj::Promise<void> publishDefinitions(EtcdClient& client, const Plan& planned, std::ostream& report, uint64_t& revision) {
  for (const auto& definition : planned.definitions) {
    const auto key = absolute(definition.key);
    const auto& bytes = *definition.value;
    auto response = co_await client.request("/v3/kv/txn",
        "{\"compare\":[{\"target\":\"VERSION\",\"result\":\"EQUAL\",\"key\":" + quote(base64(key)) +
        ",\"version\":\"0\"}],\"success\":[{\"request_put\":{\"key\":" + quote(base64(key)) + ",\"value\":" +
        quote(base64(bytes)) + "}}],\"failure\":[{\"request_range\":{\"key\":" + quote(base64(key)) + "}}]}");
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    revision = std::max(revision, integer(field(json, "header"), "revision"));
    if (!boolean(json, "succeeded")) {
      auto kvs = field(field(field(json, "responses").getArray()[0], "response_range"), "kvs");
      KJ_REQUIRE(kvs.isArray() && kvs.getArray().size() == 1 && unbase64(text(kvs.getArray()[0], "value")) == bytes,
          "deployment label conflict during definition publication", definition.key.c_str());
    }
  }
  report << ",\"publishedDefinitions\":" << planned.definitions.size();
}

// ---------------------------------------------------------------------------
// Trial loads: after the immutable parts are published and before the
// flip, one Server loads each new version (POST /__coworkerd/trial-load). A
// version that does not load stops the change: nothing is flipped, the
// published definitions and objects stay (immutable, harmless).

struct TrialContext {
  kj::Network& network;
  const kj::HttpHeaderTable& headers;
  std::string token;
};

struct TrialAttempt {
  bool answered = false;
  uint status = 0;
  std::string body, error;
  double elapsedMs = 0;
};

double millisecondsSince(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

// Awaited at once by postTrial, which owns the referenced values.
kj::Promise<void> exchangeTrial(TrialContext& context, kj::Timer& timer, const std::string& url, const std::string& body,
    TrialAttempt& attempt) {
  auto http = kj::newHttpClient(timer, context.headers, context.network, kj::none);
  kj::HttpHeaders headers(context.headers);
  headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
  headers.setPtr(kj::HttpHeaderId::CONNECTION, "close");
  if (!context.token.empty()) headers.addPtr("Authorization", kj::str("Bearer ", context.token.c_str()));
  auto call = http->request(kj::HttpMethod::POST, url.c_str(), headers, body.size());
  co_await call.body->write(kj::arrayPtr(body.data(), body.size()).asBytes());
  call.body = nullptr;
  auto response = co_await call.response;
  auto text = co_await response.body->readAllText(1 << 20);
  attempt.status = response.statusCode;
  attempt.body = text.cStr();
  attempt.answered = true;
}

kj::Promise<TrialAttempt> postTrial(TrialContext& context, kj::Timer& timer, std::string url, std::string body, uint32_t timeoutMs) {
  const auto started = std::chrono::steady_clock::now();
  TrialAttempt attempt;
  try {
    co_await timer.timeoutAfter(timeoutMs * kj::MILLISECONDS, exchangeTrial(context, timer, url, body, attempt));
  } catch (const kj::Exception& error) {
    attempt.answered = false;
    attempt.error = error.getDescription().cStr();
  }
  attempt.elapsedMs = millisecondsSince(started);
  co_return attempt;
}

struct TrialMember { std::string id, address; };

// The cluster's nodes (their member keys /coworkerd/v2/nodes/<nodeId>; the keys below one are its records).
kj::Promise<std::vector<TrialMember>> trialMembers(EtcdClient& client) {
  const std::string prefix = NODES_PREFIX;
  auto raw = co_await client.range(prefix);
  std::vector<TrialMember> members;
  for (const auto& [key, kv] : raw.values) {
    if (key.find('/', prefix.size()) != std::string::npos) continue;
    try {
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<Json>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(kv.value.data(), kv.value.size()), json);
      auto id = text(json, "nodeId"), address = text(json, "address");
      if (key == prefix + id && !address.empty()) members.push_back({id, address});
    } catch (const kj::Exception&) {}  // not a member record: not a candidate
  }
  co_return members;
}

// The verdict's JSON as the Server sent it, re-encoded (so a damaged answer
// cannot break this tool's output), and its outcome.
struct TrialVerdict { bool valid = false, ok = false; std::string category, message, serverId, json; };

TrialVerdict readTrialVerdict(const std::string& body) {
  TrialVerdict verdict;
  try {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(body.data(), body.size()), json);
    auto ok = field(json, "ok");
    KJ_REQUIRE(ok.isBoolean(), "trial verdict without ok");
    verdict.ok = ok.getBoolean();
    auto category = field(json, "category"), detail = field(json, "message"), server = field(json, "nodeId");
    if (category.isString()) verdict.category = category.getString().cStr();
    if (detail.isString()) verdict.message = detail.getString().cStr();
    if (server.isString()) verdict.serverId = server.getString().cStr();
    KJ_REQUIRE(verdict.ok == verdict.category.empty(), "inconsistent trial verdict");
    verdict.json = capnp::JsonCodec().encodeRaw(json).cStr();
    verdict.valid = true;
  } catch (const kj::Exception&) {}
  return verdict;
}

// Trials of every version whose pointer this change moves; adds the "trial"
// field to the report, or writes the refusal's record to `out` and refuses.
kj::Promise<void> runTrials(EtcdClient& client, kj::Timer& timer, TrialContext& context, const char* operation,
    const DeploymentInput& desired, const DeploymentState& before, const Plan& planned, uint64_t revision,
    const ToolOptions& options, std::ostream& report, std::ostream& out) {
  if (!options.trial) { report << ",\"trial\":{\"enabled\":false}"; co_return; }
  const auto phaseStarted = nowUnixMs();
  std::string records;
  size_t skipped = 0;
  std::vector<std::shared_ptr<const VersionDefinition>> versions;
  for (const auto& id : planned.workers) {
    const auto& version = desired.versions.at(id + "@" + desired.active.at(id).versionId);
    const auto* current = before.worker(id);
    // Not new: the pointer already names this definition.
    if (current && current->pointerError.empty() && current->definitionHash == version->definitionHash) continue;
    if (!version->hasCode()) { ++skipped; continue; }  // static assets only: nothing to load
    versions.push_back(version);
  }
  std::vector<TrialMember> members;
  if (!options.trialServer.empty()) members.push_back({"", options.trialServer});
  else if (!versions.empty()) members = co_await trialMembers(client);
  auto fail = [&](const std::string& reason, const std::string& record) {
    out << "{\"operation\":\"" << operation << "\",\"flipped\":false,\"trial\":{\"enabled\":true,\"server\":"
        << (options.trialServer.empty() ? std::string("\"members\"") : quote(options.trialServer))
        << ",\"timeoutMs\":" << options.trialTimeoutMs << ",\"versions\":[" << records
        << (records.empty() || record.empty() ? "" : ",") << record << "],\"startedAtUnixMs\":" << phaseStarted
        << ",\"finishedAtUnixMs\":" << nowUnixMs() << "},\"reason\":" << quote(reason) << '}' << std::endl;
    refuse(reason);
  };
  if (!versions.empty() && members.empty())
    fail("no member of the cluster can run the trial load (no node has registered); deploy with --no-trial "
        "(for example the initial deployment, before any node runs) or name a node with --trial-server URL", "");
  for (const auto& version : versions) {
    bool routed = false;
    for (const auto& route : desired.routes) routed = routed || route.workerId == version->workerId;
    const auto body = "{\"workerId\":" + quote(version->workerId) + ",\"versionId\":" + quote(version->versionId) +
        ",\"definitionHash\":" + quote(version->definitionHash) + ",\"contentHash\":" + quote(version->contentHash) +
        ",\"revision\":\"" + std::to_string(revision) + "\",\"timeoutMs\":" + std::to_string(options.trialTimeoutMs) +
        ",\"routed\":" + (routed ? "true" : "false") + "}";
    // Home first, as the Servers place this Worker: the isolate the trial
    // leaves behind is where its requests go.
    std::vector<TrialMember> order = members;
    if (options.trialServer.empty()) {
      std::vector<std::string> ids;
      for (const auto& member : members) ids.push_back(member.id);
      order.clear();
      for (const auto& id : rendezvousOrder(version->workerId, ids))
        for (const auto& member : members) if (member.id == id) order.push_back(member);
    }
    std::string attempts, problems;
    TrialVerdict decided;
    double elapsedMs = 0;
    std::string chosen;
    for (const auto& member : order) {
      auto address = member.address;
      if (auto mapped = options.trialAddressMap.find(address); mapped != options.trialAddressMap.end()) address = mapped->second;
      const auto url = options.trialServer.empty() ? "http://" + address + "/__coworkerd/trial-load"
          : (address.ends_with("/__coworkerd/trial-load") ? address : address + "/__coworkerd/trial-load");
      auto attempt = co_await postTrial(context, timer, url, body, options.trialTimeoutMs + 2000);
      auto verdict = attempt.answered && attempt.status == 200 ? readTrialVerdict(attempt.body) : TrialVerdict();
      std::string problem = !attempt.answered ? "unreachable: " + attempt.error
          : attempt.status == 401 ? "HTTP 401: the node requires the management token (--token or COWORKERD_MANAGEMENT_TOKEN)"
          : attempt.status != 200 ? "HTTP " + std::to_string(attempt.status)
          : !verdict.valid ? "unreadable answer"
          : verdict.category == "UNAVAILABLE" ? "could not try: " + verdict.message : "";
      attempts += std::string(attempts.empty() ? "" : ",") + "{\"server\":" + quote(member.id.empty() ? url : member.id) +
          ",\"url\":" + quote(url) + ",\"elapsedMs\":" + std::to_string(attempt.elapsedMs) +
          ",\"status\":" + std::to_string(attempt.status) + ",\"problem\":" + (problem.empty() ? "null" : quote(problem)) + "}";
      if (!problem.empty()) {
        problems += std::string(problems.empty() ? "" : "; ") + (member.id.empty() ? url : member.id) + ": " + problem;
        continue;
      }
      decided = verdict;
      elapsedMs = attempt.elapsedMs;
      chosen = member.id.empty() ? url : member.id;
      break;
    }
    const auto label = version->workerId + "@" + version->versionId;
    auto record = "{\"workerId\":" + quote(version->workerId) + ",\"versionId\":" + quote(version->versionId) +
        ",\"definitionHash\":" + quote(version->definitionHash) + ",\"routed\":" + (routed ? "true" : "false") +
        ",\"server\":" + (chosen.empty() ? std::string("null") : quote(chosen)) + ",\"elapsedMs\":" + std::to_string(elapsedMs) +
        ",\"attempts\":[" + attempts + "],\"verdict\":" + (decided.valid ? decided.json : std::string("null")) + "}";
    if (!decided.valid) {
      fail(options.trialServer.empty()
          ? "none of the " + std::to_string(members.size()) + " members could run the trial load of " + label +
              " (" + problems + "); nothing flipped. Name a reachable node with --trial-server URL, or deploy with --no-trial"
          : "the trial node could not run the trial load of " + label + " (" + problems + "); nothing flipped", record);
    }
    if (!decided.ok) {
      fail("trial load of " + label + " failed on node " + decided.serverId + ": " + decided.category + ": " + decided.message +
          "; nothing flipped (the version was not published: it does not load)", record);
    }
    records += std::string(records.empty() ? "" : ",") + record;
  }
  report << ",\"trial\":{\"enabled\":true,\"server\":" << (options.trialServer.empty() ? std::string("\"members\"") : quote(options.trialServer))
         << ",\"timeoutMs\":" << options.trialTimeoutMs << ",\"versions\":[" << records << "],\"skippedWithoutCode\":" << skipped
         << ",\"startedAtUnixMs\":" << phaseStarted << ",\"finishedAtUnixMs\":" << nowUnixMs() << "}";
}

// Activation: the input that moves a Worker's pointer to a version published before -- its
// definition as stored -- with the Worker's current usable routes, or the routes the activation names.
// Built from each attempt's read, so a retry sees the routes as they are then.
DeploymentInput activationInput(const EtcdSnapshot& raw, const DeploymentState& before, ToolOptions::Activation activation) {
  if (activation.versionId.empty()) {
    const auto* worker = before.worker(activation.workerId);
    if (!worker || !worker->pointerError.empty()) refuse("Worker " + activation.workerId + " is not deployed (it has no active version)");
    activation.versionId = worker->versionId;
  }
  const auto* kv = kvOf(raw, deploymentDefinitionKey(activation.workerId, activation.versionId));
  if (!kv) refuse("version " + activation.versionId + " of Worker " + activation.workerId + " was never uploaded");
  auto known = before.definitions().find(activation.workerId + "@" + activation.versionId);
  if (known == before.definitions().end() || !known->second.version)
    refuse("version " + activation.versionId + " of Worker " + activation.workerId + " has an unusable definition: " +
        (known == before.definitions().end() ? std::string("unreadable") : known->second.error));
  const auto& version = known->second.version;
  DeploymentInput input;
  input.versions.emplace(version->key(), version);
  input.active.emplace(activation.workerId, versionPin(*version));
  if (activation.routes) input.routes = *activation.routes;
  else if (auto owned = before.routesByWorker().find(activation.workerId); owned != before.routesByWorker().end()) {
    for (const auto& key : owned->second) {
      auto route = before.routes().find(key);
      if (route != before.routes().end() && route->second.error.empty()) input.routes.push_back(route->second.value);
    }
  }
  return input;
}

// The tool's writes. The record of a change goes to `out` (the tool's standard output); a refusal is an
// exception with one sentence, after a refused trial's record. Every read is the tenant's snapshot. The
// order: the immutable objects, the trial loads, the definition keys, the flip -- a version that does not load is
// never published in the deployment state (control-plane-api.md: "a version exists only if it loads").
kj::Promise<void> write(Operation operation, EtcdClient& client, kj::Timer& timer, ObjectStore* storage,
    const ToolOptions& options, TrialContext& trials, std::ostream& out) {
  const auto startedAtUnixMs = nowUnixMs();
  DeploymentInput desired;
  DeploymentCode code;
  if (operation != Operation::REMOVE && !options.activation) {
    desired = readDeploymentManifest(options.manifest, options.tenant);
    code = readDeploymentCode(desired, options.codeRoot.empty() ? options.manifest.parent_path() : options.codeRoot);
  }
  std::ostringstream report;
  bool published = false;
  std::mt19937 random(std::random_device{}());
  if (options.publishOnly) {
    // An upload: the immutable parts, the trial loads of its versions, then their definitions, after the same
    // conflict check as a deployment; no pointer moves.
    auto raw = co_await tenantSnapshot(client, options.tenant);
    auto before = stateOf(raw);
    Plan uploaded;
    preflightDefinitions(raw, desired, uploaded);
    for (const auto& [id, pin] : desired.active) uploaded.workers.push_back(id);
    uint64_t publishedRevision = raw.revision;
    co_await publishObjects(*storage, desired, code, report);
    co_await runTrials(client, timer, trials, "upload", desired, before, uploaded, raw.revision, options, report, out);
    co_await publishDefinitions(client, uploaded, report, publishedRevision);
    out << "{\"operation\":\"upload\",\"flipped\":false,\"revision\":\"" << publishedRevision << "\",\"readRevision\":\""
        << raw.revision << "\",\"versions\":[";
    size_t index = 0;
    for (const auto& [label, version] : desired.versions) out << (index++ ? "," : "") << quote(label);
    out << "],\"startedAtUnixMs\":" << startedAtUnixMs << ",\"finishedAtUnixMs\":" << nowUnixMs() << report.str() << '}' << std::endl;
    co_return;
  }
  const bool manifestRoutes = !desired.routes.empty();
  for (uint32_t attempt = 0;; ++attempt) {
    const auto readStarted = std::chrono::steady_clock::now();
    auto raw = co_await tenantSnapshot(client, options.tenant);
    const auto readFinished = std::chrono::steady_clock::now();
    auto before = stateOf(raw);
    if (options.activation) desired = activationInput(raw, before, *options.activation);
    if (options.keepRoutes && !manifestRoutes && !options.activation) {
      desired.routes.clear();
      for (const auto& [id, pin] : desired.active) {
        auto owned = before.routesByWorker().find(id);
        if (owned == before.routesByWorker().end()) continue;
        for (const auto& key : owned->second) {
          auto route = before.routes().find(key);
          if (route != before.routes().end() && route->second.error.empty()) desired.routes.push_back(route->second.value);
        }
      }
    }
    auto planned = plan(operation, raw, before, desired, options, false);
    const auto planned_at = std::chrono::steady_clock::now();
    if (planned.txn.size() > options.maxTxnOps) {
      bool tenantEmpty = true;
      for (const auto& [id, worker] : before.workers()) tenantEmpty = tenantEmpty && tenantOf(id) != options.tenant;
      for (const auto& [id, keys] : before.routesByWorker()) tenantEmpty = tenantEmpty && tenantOf(id) != options.tenant;
      if (operation != Operation::APPLY || !tenantEmpty) requireSize(planned.txn, options.maxTxnOps);
      // A tenant's initial deployment staged as upserts; the whole state was validated above.
      uint64_t publishedRevision = raw.revision;
      co_await publishObjects(*storage, desired, code, report);
      co_await runTrials(client, timer, trials, "apply", desired, before, planned, raw.revision, options, report, out);
      co_await publishDefinitions(client, planned, report, publishedRevision);
      auto steps = stages(desired, options.maxTxnOps);
      uint64_t revision = 0, generation = 0;
      size_t compares = 0, operations = 0;
      for (size_t step = 0; step < steps.size(); ++step) {
        auto current = co_await tenantSnapshot(client, options.tenant);
        auto state = stateOf(current);
        auto part = plan(Operation::UPSERT, current, state, steps[step], options, true, step + 1 == steps.size());
        if (!part.definitions.empty()) KJ_FAIL_REQUIRE("definitions disappeared after publication; refusing to continue");
        auto response = co_await client.request("/v3/kv/txn", part.txn.json());
        capnp::MallocMessageBuilder message;
        auto json = message.initRoot<Json>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
        if (!boolean(json, "succeeded"))
          refuse("期间有其他部署 (another deployment wrote during a staged initial deployment, step " + std::to_string(step + 1) +
              " of " + std::to_string(steps.size()) + "); earlier steps remain");
        revision = integer(field(json, "header"), "revision");
        generation = state.generation() + 1;
        compares = std::max(compares, part.txn.compares.size());
        operations = std::max(operations, part.txn.operations.size());
      }
      out << "{\"operation\":\"apply\",\"tenant\":" << quote(options.tenant) << ",\"generation\":" << generation << ",\"revision\":\""
          << revision << "\",\"readRevision\":\"" << raw.revision << "\",\"attempts\":1,\"retries\":0,\"staged\":" << steps.size()
          << ",\"compares\":" << compares << ",\"operations\":" << operations << ",\"workers\":[";
      size_t index = 0;
      for (const auto& [id, pin] : desired.active) out << (index++ ? "," : "") << quote(id);
      out << "],\"removed\":[],\"startedAtUnixMs\":" << startedAtUnixMs << ",\"committedAtUnixMs\":" << nowUnixMs()
          << report.str() << '}' << std::endl;
      co_return;
    }
    const char* name = options.activation ? "activate"
        : operation == Operation::APPLY ? "apply" : operation == Operation::UPSERT ? "upsert" : "remove";
    if (!published) {
      if (operation == Operation::REMOVE) report << ",\"publishedDefinitions\":0";
      else {
        uint64_t publishedRevision = raw.revision;
        // An activated version was published when it was uploaded: its objects are in the store.
        if (!options.activation) co_await publishObjects(*storage, desired, code, report);
        // Every new version loads on a node before anything flips (before its definition is published).
        co_await runTrials(client, timer, trials, name, desired, before, planned, raw.revision, options, report, out);
        if (options.activation) report << ",\"publishedDefinitions\":" << planned.definitions.size();
        else co_await publishDefinitions(client, planned, report, publishedRevision);
      }
      published = true;
    } else if (!planned.definitions.empty()) {
      KJ_FAIL_REQUIRE("definitions disappeared after publication; refusing to continue");
    }
    if (options.activation && !planned.definitions.empty())
      KJ_FAIL_REQUIRE("the activated version's definition is not published; refusing to continue");
    if (options.pauseMs) co_await timer.afterDelay(options.pauseMs * kj::MILLISECONDS);
    const auto submittedAtUnixMs = nowUnixMs();
    auto response = co_await client.request("/v3/kv/txn", planned.txn.json());
    const auto committedAtUnixMs = nowUnixMs();
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    if (boolean(json, "succeeded")) {
      out << "{\"operation\":\"" << name << "\",\"tenant\":" << quote(options.tenant) << ",\"generation\":" << before.generation() + 1
          << ",\"revision\":\"" << integer(field(json, "header"), "revision")
          << "\",\"readRevision\":\"" << raw.revision << "\",\"attempts\":" << attempt + 1
          << ",\"retries\":" << attempt << ",\"compares\":" << planned.txn.compares.size()
          << ",\"operations\":" << planned.txn.operations.size() << ",\"workers\":[";
      for (size_t i = 0; i < planned.workers.size(); ++i) out << (i ? "," : "") << quote(planned.workers[i]);
      out << "],\"removed\":[";
      for (size_t i = 0; i < options.workers.size(); ++i) out << (i ? "," : "") << quote(options.workers[i]);
      out << "],\"startedAtUnixMs\":" << startedAtUnixMs << ",\"submittedAtUnixMs\":" << submittedAtUnixMs
          << ",\"committedAtUnixMs\":" << committedAtUnixMs
          << ",\"readMicros\":" << std::chrono::duration_cast<std::chrono::microseconds>(readFinished - readStarted).count()
          << ",\"planMicros\":" << std::chrono::duration_cast<std::chrono::microseconds>(planned_at - readFinished).count()
          << ",\"readKeys\":" << raw.values.size() << report.str() << '}' << std::endl;
      co_return;
    }
    if (attempt >= options.maxRetries)
      refuse("期间有其他部署 (another deployment changed a key of this change; " + std::to_string(attempt + 1) +
          " attempts); flip aborted, nothing written");
    // Re-read, re-validate and retry after a short jittered pause.
    const auto ceiling = std::min<uint32_t>(10u << std::min<uint32_t>(attempt, 4u), 160u);
    co_await timer.afterDelay(std::uniform_int_distribution<uint32_t>(ceiling / 2, ceiling)(random) * kj::MILLISECONDS);
  }
}

// ---------------------------------------------------------------------------
// Secrets: secret put <worker> <name> (value on standard input), secret delete
// <worker> <name>, secret list <worker>. A write is conditioned on the secret's
// own key (and, for a delete, on the Worker's pointer), never on another
// Worker's keys. Nothing here prints, logs or reports a value: output carries
// names, lengths and times.

// The value as `wrangler secret put` reads it: a line typed without echo on a
// terminal, otherwise all of standard input; trailing whitespace removed.
std::string readSecretValue() {
  std::string value;
  if (isatty(STDIN_FILENO)) {
    std::cerr << "Enter the secret value: " << std::flush;
    termios saved{};
    const bool terminal = tcgetattr(STDIN_FILENO, &saved) == 0;
    if (terminal) { auto quiet = saved; quiet.c_lflag &= ~static_cast<tcflag_t>(ECHO); tcsetattr(STDIN_FILENO, TCSAFLUSH, &quiet); }
    std::getline(std::cin, value);
    if (terminal) tcsetattr(STDIN_FILENO, TCSAFLUSH, &saved);
    std::cerr << std::endl;
  } else {
    // Bounded: anything longer than the limit is refused without being kept.
    char buffer[4096];
    while (true) {
      const auto count = ::read(STDIN_FILENO, buffer, sizeof(buffer));
      if (count < 0 && errno == EINTR) continue;
      KJ_REQUIRE(count >= 0, "cannot read the secret value from standard input");
      if (count == 0) break;
      value.append(buffer, static_cast<size_t>(count));
      if (value.size() > 64 * 1024) { value.clear(); refuse("secret value exceeds 5 KiB"); }
    }
  }
  while (!value.empty() && (value.back() == ' ' || value.back() == '\t' || value.back() == '\n' ||
      value.back() == '\r' || value.back() == '\v' || value.back() == '\f')) value.pop_back();
  return value;
}

[[noreturn]] void refuseSecretUsage(const std::string& operation) {
  // Never echo the arguments: a value passed by mistake must not reach the output.
  if (operation == "put")
    refuse("secret put reads the value from standard input and takes no value argument or option; usage: "
        "coworkerd-apiserver tool secret put <worker> <name> --endpoint URL < value-file (or type it when prompted)");
  refuse("usage: coworkerd-apiserver tool secret put|delete <worker> <name> --endpoint URL [--tenant T], "
      "or coworkerd-apiserver tool secret list <worker> --endpoint URL [--tenant T]");
}

// The revision of a committed transaction, 0 when a comparison failed.
kj::Promise<uint64_t> commitSecret(EtcdClient& client, const Transaction& txn) {
  auto response = co_await client.request("/v3/kv/txn", txn.json());
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
  co_return boolean(json, "succeeded") ? integer(field(json, "header"), "revision") : 0;
}

kj::Promise<void> secretPut(EtcdClient& client, kj::Timer& timer, std::string worker, std::string name,
    std::string value, uint32_t maxRetries, std::ostream& out) {
  if (auto problem = secretValueProblem(value); !problem.empty()) { value.clear(); refuse(problem + " (" + worker + "/" + name + ")"); }
  const auto key = deploymentSecretKey(worker, name);
  std::mt19937 random(std::random_device{}());
  for (uint32_t attempt = 0;; ++attempt) {
    auto raw = co_await tenantSnapshot(client, tenantOf(worker));
    auto state = stateOf(raw);
    const auto* existing = kvOf(raw, key);
    const auto setAtUnixMs = nowUnixMs();
    Transaction txn;
    txn.unchanged(key, existing);
    txn.put(key, secretRecordJson(value, setAtUnixMs));
    if (auto revision = co_await commitSecret(client, txn)) {
      // The active version, as read: whether it declares the name (a typo shows here).
      const auto* current = state.worker(worker);
      auto pointed = current ? state.definition(worker + "@" + current->versionId) : nullptr;
      const bool declared = pointed && std::find(pointed->secrets.begin(), pointed->secrets.end(), name) != pointed->secrets.end();
      out << "{\"operation\":\"secret put\",\"workerId\":" << quote(worker) << ",\"name\":" << quote(name)
          << ",\"bytes\":" << value.size() << ",\"created\":" << (existing ? "false" : "true")
          << ",\"declaredByActiveVersion\":" << (declared ? "true" : "false")
          << ",\"setAtUnixMs\":" << setAtUnixMs << ",\"revision\":\"" << revision
          << "\",\"readRevision\":\"" << raw.revision << "\",\"attempts\":" << attempt + 1 << '}' << std::endl;
      co_return;
    }
    if (attempt >= maxRetries)
      refuse("another write changed secret " + worker + "/" + name + " (" + std::to_string(attempt + 1) + " attempts); nothing written");
    co_await timer.afterDelay(std::uniform_int_distribution<uint32_t>(5, 40)(random) * kj::MILLISECONDS);
  }
}

kj::Promise<void> secretDelete(EtcdClient& client, kj::Timer& timer, std::string worker, std::string name,
    uint32_t maxRetries, std::ostream& out) {
  const auto key = deploymentSecretKey(worker, name);
  std::mt19937 random(std::random_device{}());
  for (uint32_t attempt = 0;; ++attempt) {
    auto raw = co_await tenantSnapshot(client, tenantOf(worker));
    auto before = stateOf(raw);
    const auto* existing = kvOf(raw, key);
    if (!existing) refuse("secret " + name + " of Worker " + worker + " is not set");
    // The shared evaluation decides: a delete must leave every valid Worker
    // valid (the active version must not declare the name). The Worker's own
    // reason first, then any Worker that binds it.
    DeploymentState after = before;
    after.apply({{key, std::nullopt}}, false, raw.revision);
    if (before.valid(worker) && !after.valid(worker))
      refuse("secret delete would make valid Worker " + worker + " invalid: " + after.worker(worker)->reason +
          "; deploy a version that does not declare " + name + " first");
    requireNoCollateral(before, after, {}, "secret delete");
    Transaction txn;
    txn.unchanged(key, existing);
    // A concurrent flip of this Worker to a version declaring the name.
    txn.unchanged(deploymentActiveKey(worker), kvOf(raw, deploymentActiveKey(worker)));
    txn.remove(key);
    if (auto revision = co_await commitSecret(client, txn)) {
      out << "{\"operation\":\"secret delete\",\"workerId\":" << quote(worker) << ",\"name\":" << quote(name)
          << ",\"revision\":\"" << revision << "\",\"readRevision\":\"" << raw.revision
          << "\",\"attempts\":" << attempt + 1 << '}' << std::endl;
      co_return;
    }
    if (attempt >= maxRetries)
      refuse("another write changed secret " + worker + "/" + name + " or the Worker's pointer (" +
          std::to_string(attempt + 1) + " attempts); nothing deleted");
    co_await timer.afterDelay(std::uniform_int_distribution<uint32_t>(5, 40)(random) * kj::MILLISECONDS);
  }
}

kj::Promise<void> secretList(EtcdClient& client, std::string worker, std::ostream& out) {
  auto raw = co_await client.range(absolute(deploymentTenantPrefix(tenantOf(worker)) + "secrets/" + workerNameOf(worker) + "/"));
  auto state = stateOf(raw);
  out << "{\"workerId\":" << quote(worker) << ",\"revision\":\"" << raw.revision << "\",\"secrets\":[";
  size_t index = 0;
  auto names = state.secrets().find(worker);
  // Names, lengths and times: never a value.
  if (names != state.secrets().end()) for (const auto& [name, record] : names->second) {
    out << (index++ ? "," : "") << "{\"name\":" << quote(name) << ",\"bytes\":";
    if (record.error.empty()) out << record.value.size() << ",\"setAtUnixMs\":" << record.setAtUnixMs;
    else out << "null,\"setAtUnixMs\":null,\"problem\":" << quote(record.error);
    out << '}';
  }
  out << "]}" << std::endl;
}

int runSecretTool(int argc, char** argv) {
  if (argc < 1) refuseSecretUsage("");
  const std::string operation = argv[0];
  if (operation != "put" && operation != "delete" && operation != "list") refuseSecretUsage("");
  const size_t positionals = operation == "list" ? 1 : 2;
  std::vector<std::string> names;
  std::string endpoint, tenant = DEFAULT_TENANT;
  uint32_t maxRetries = 8;
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument == "--endpoint" && index + 1 < argc) { endpoint = argv[++index]; continue; }
    if (argument == "--tenant" && index + 1 < argc) {
      tenant = argv[++index];
      if (!tenantName(tenant)) refuse("invalid tenant ([a-z0-9][a-z0-9-]{0,62})");
      continue;
    }
    if (argument == "--max-retries" && index + 1 < argc) {
      const std::string text = argv[++index];
      if (text.empty() || text.size() > 3 || !std::all_of(text.begin(), text.end(), [](unsigned char c) { return std::isdigit(c); }))
        refuseSecretUsage(operation);
      maxRetries = static_cast<uint32_t>(std::stoul(text));
      continue;
    }
    if (argument.starts_with("-") || names.size() == positionals) refuseSecretUsage(operation);
    names.push_back(argument);
  }
  if (names.size() != positionals || endpoint.empty()) refuseSecretUsage(operation);
  // Not echoed either: a value typed in place of a name must not reach the output.
  if (!deploymentSafeName(names[0])) refuse("invalid workerId (letters, digits, '-', '_' and '.')");
  if (positionals == 2 && !environmentName(names[1]))
    refuse("invalid secret name (a JavaScript identifier of ASCII letters, digits, '_' and '$')");
  std::string value;
  if (operation == "put") value = readSecretValue();
  auto io = kj::setupAsyncIo();
  auto headers = kj::HttpHeaderTable::Builder().build();
  EtcdClient client(endpoint, 750, io.provider->getNetwork(), io.provider->getTimer(), *headers);
  const auto worker = workerFullId(tenant, names[0]);
  try {
    if (operation == "put") secretPut(client, io.provider->getTimer(), worker, names[1], value, maxRetries, std::cout).wait(io.waitScope);
    else if (operation == "delete") secretDelete(client, io.provider->getTimer(), worker, names[1], maxRetries, std::cout).wait(io.waitScope);
    else secretList(client, worker, std::cout).wait(io.waitScope);
  } catch (const kj::Exception& error) {
    // Defence in depth: a failure description never quotes the value.
    std::string description = error.getDescription().cStr();
    if (operation == "put" && !value.empty() && (description.find(value) != std::string::npos ||
        description.find(base64(value)) != std::string::npos))
      refuse("secret put failed (detail withheld: it would quote the value)");
    throw;
  }
  return 0;
}
}  // namespace

// ---------------------------------------------------------------------------
// The control plane (`coworkerd-apiserver`, served by main.c++; declared in control.h): the
// tool's own operations behind an HTTP API -- the same reads, plans, publication, trial loads, flips
// and secret writes, with the tool's retries. Each answers {"ok":true,"record":<the tool's record>}
// or {"ok":false,"kind":<exception kind>,"reason":<the refusal or failure>,"record":<a refused trial's
// record, or null>}; the caller cleans a reason before it leaves the control plane.

namespace {
std::string controlSuccess(const std::string& printed) {
  auto record = printed;
  while (!record.empty() && (record.back() == '\n' || record.back() == ' ')) record.pop_back();
  return "{\"ok\":true,\"record\":" + (record.empty() ? std::string("null") : record) + '}';
}
std::string controlFailure(const kj::Exception& error, const std::string& printed, const std::string& withheld = {}) {
  std::string reason = error.getDescription().cStr();
  // A secret's value never leaves in a reason (as the secret tool withholds it).
  if (!withheld.empty() && (reason.find(withheld) != std::string::npos || reason.find(base64(withheld)) != std::string::npos))
    reason = "secret put failed (detail withheld: it would quote the value)";
  auto record = printed;
  while (!record.empty() && (record.back() == '\n' || record.back() == ' ')) record.pop_back();
  // Only a refused trial prints before it refuses: its record is the last line.
  if (auto line = record.rfind('\n'); line != std::string::npos) record = record.substr(line + 1);
  const char* kind = error.getType() == kj::Exception::Type::DISCONNECTED ? "disconnected"
      : error.getType() == kj::Exception::Type::OVERLOADED ? "overloaded" : "failed";
  return "{\"ok\":false,\"kind\":\"" + std::string(kind) + "\",\"reason\":" + quote(reason) +
      ",\"record\":" + (record.starts_with("{") ? record : std::string("null")) + '}';
}
ToolOptions controlOptions(const std::string& token) {
  ToolOptions options;
  options.maxRetries = 8;
  options.token = token;
  return options;
}
}  // namespace

// The Workers of a tenant the deployment state names (a pointer, a definition or a route), with their versions in
// upload order, active version, validity, limits, routes and secrets (names and lengths) -- or one of them.
kj::Promise<std::string> controlWorkers(EtcdClient& client, std::string tenant, std::optional<std::string> only) {
  std::ostringstream printed;
  try {
    auto raw = co_await tenantSnapshot(client, tenant);
    auto state = stateOf(raw);
    std::map<std::string, std::vector<std::pair<uint64_t, std::string>>> versions;
    for (const auto& [label, definition] : state.definitions()) {
      const auto at = label.find('@');
      if (at == std::string::npos) continue;
      const auto id = label.substr(0, at), versionId = label.substr(at + 1);
      if (tenantOf(id) != tenant) continue;
      const auto* kv = kvOf(raw, deploymentDefinitionKey(id, versionId));
      std::string entry = "{\"versionId\":" + quote(versionId) + ",\"uploadedRevision\":\"" +
          std::to_string(kv ? kv->createRevision : 0) + '"';
      if (const auto& version = definition.version) {
        entry += ",\"definitionHash\":" + quote(version->definitionHash) + ",\"contentHash\":" + quote(version->contentHash) +
            ",\"format\":" + quote(version->format) + ",\"compatibilityDate\":" + quote(version->compatibilityDate) +
            ",\"mainModule\":" + quote(version->mainModule) + ",\"assets\":" + (version->assets ? "true" : "false") + ",\"secrets\":[";
        for (size_t i = 0; i < version->secrets.size(); ++i) entry += (i ? "," : "") + quote(version->secrets[i]);
        entry += ']';
      } else {
        entry += ",\"problem\":" + quote(definition.error);
      }
      versions[id].emplace_back(kv ? kv->createRevision : 0, entry + '}');
    }
    std::set<std::string> ids;
    for (const auto& [id, list] : versions) ids.insert(id);
    for (const auto& [id, worker] : state.workers()) if (tenantOf(id) == tenant) ids.insert(id);
    for (const auto& [id, keys] : state.routesByWorker()) if (tenantOf(id) == tenant) ids.insert(id);
    if (only && !ids.contains(*only)) refuse("Worker " + *only + " is not deployed");
    auto describe = [&](const std::string& id) {
      std::string out = "{\"tenant\":" + quote(tenant) + ",\"worker\":" + quote(workerNameOf(id)) + ",\"workerId\":" + quote(id) +
          ",\"activeVersion\":";
      if (const auto* worker = state.worker(id)) {
        out += (worker->pointerError.empty() ? quote(worker->versionId) : std::string("null")) + ",\"valid\":" +
            (worker->valid ? "true" : "false") + ",\"reason\":" + (worker->valid ? std::string("null") : quote(worker->reason)) +
            ",\"limits\":" + workerLimitsJson(worker->limits);
        if (!worker->missingSecrets.empty()) {
          out += ",\"missingSecrets\":[";
          for (size_t i = 0; i < worker->missingSecrets.size(); ++i) out += (i ? "," : "") + quote(worker->missingSecrets[i]);
          out += ']';
        }
      } else {
        out += "null,\"valid\":false,\"reason\":\"no active version\",\"limits\":{}";
      }
      auto& list = versions[id];
      std::sort(list.begin(), list.end());
      out += ",\"versions\":[";
      for (size_t i = 0; i < list.size(); ++i) out += (i ? "," : "") + list[i].second;
      out += "],\"routes\":[";
      size_t routes = 0;
      if (auto owned = state.routesByWorker().find(id); owned != state.routesByWorker().end()) {
        for (const auto& key : owned->second) {
          auto route = state.routes().find(key);
          if (route == state.routes().end()) continue;
          out += std::string(routes++ ? "," : "") + "{\"host\":" + quote(route->second.value.host) + ",\"pathPrefix\":" +
              quote(route->second.value.pathPrefix) + ",\"effective\":" + (route->second.effective ? "true" : "false") + '}';
        }
      }
      out += "],\"secrets\":[";
      size_t secrets = 0;
      if (auto names = state.secrets().find(id); names != state.secrets().end()) for (const auto& [name, record] : names->second) {
        out += std::string(secrets++ ? "," : "") + "{\"name\":" + quote(name) + ",\"bytes\":" +
            (record.error.empty() ? std::to_string(record.value.size()) : std::string("null")) + '}';
      }
      return out + "]}";
    };
    if (only) {
      printed << describe(*only);
    } else {
      printed << "{\"tenant\":" << quote(tenant) << ",\"revision\":\"" << raw.revision << "\",\"generation\":"
              << state.generation() << ",\"workers\":[";
      size_t index = 0;
      for (const auto& id : ids) printed << (index++ ? "," : "") << describe(id);
      printed << "]}";
    }
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, {});
  }
  co_return controlSuccess(printed.str());
}

kj::Promise<std::string> controlDeploy(ControlContext& context, std::filesystem::path manifest, std::string workerId,
    std::string versionId, bool activate) {
  std::ostringstream out;
  try {
    const auto tenant = tenantOf(workerId);
    auto input = readDeploymentManifest(manifest, tenant);
    if (input.versions.size() != 1 || !input.versions.contains(workerId + "@" + versionId))
      refuse("the package must hold exactly one version, " + versionId + " of Worker " + workerId);
    if (input.active.size() != 1 || !input.active.contains(workerId) || input.active.at(workerId).versionId != versionId)
      refuse("the package's manifest must name " + versionId + " as the active version of Worker " + workerId);
    for (const auto& route : input.routes)
      if (route.workerId != workerId) refuse("the package's routes may only name Worker " + workerId);
    auto options = controlOptions(context.token);
    options.tenant = tenant;
    options.manifest = manifest;
    options.publishOnly = !activate;
    options.keepRoutes = true;
    TrialContext trials{context.network, context.headers, context.token};
    co_await write(Operation::UPSERT, context.etcd, context.timer, &context.storage, options, trials, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

// Activate a version uploaded before (a rollback is the activation of an older one); the Worker keeps
// its routes unless `routes` is given (PUT routes is the activation of the active version with new routes).
// A version already active, with no routes to change, answers {"unchanged":true}.
kj::Promise<std::string> controlActivate(ControlContext& context, std::string workerId, std::string versionId,
    std::optional<std::vector<RouteDefinition>> routes) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId) || (!versionId.empty() && !deploymentSafeName(versionId))) refuse("invalid workerId or versionId");
    if (versionId.empty() && !routes) refuse("an activation names its version");
    if (routes) for (const auto& route : *routes)
      if (route.workerId != workerId) refuse("the routes may only name Worker " + workerId);
    if (!routes) {
      auto raw = co_await tenantSnapshot(context.etcd, tenantOf(workerId));
      auto state = stateOf(raw);
      const auto* worker = state.worker(workerId);
      if (worker && worker->valid && worker->versionId == versionId) {
        co_return controlSuccess("{\"operation\":\"activate\",\"unchanged\":true,\"workerId\":" + quote(workerId) +
            ",\"versionId\":" + quote(versionId) + ",\"readRevision\":\"" + std::to_string(raw.revision) + "\"}");
      }
    }
    auto options = controlOptions(context.token);
    options.tenant = tenantOf(workerId);
    options.activation = ToolOptions::Activation{workerId, versionId, routes};
    TrialContext trials{context.network, context.headers, context.token};
    co_await write(Operation::UPSERT, context.etcd, context.timer, nullptr, options, trials, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

// DELETE a Worker: its pointer and routes (its definitions stay, as with the tool's remove).
kj::Promise<std::string> controlRemove(ControlContext& context, std::string workerId) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId)) refuse("invalid workerId");
    auto options = controlOptions(context.token);
    options.tenant = tenantOf(workerId);
    options.workers = {workerId};
    TrialContext trials{context.network, context.headers, context.token};
    co_await write(Operation::REMOVE, context.etcd, context.timer, nullptr, options, trials, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

// A Worker's own limits: its pointer rewritten with them, conditioned on the pointer as read.
kj::Promise<std::string> controlLimits(ControlContext& context, std::string workerId, WorkerLimits limits) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId)) refuse("invalid workerId");
    std::mt19937 random(std::random_device{}());
    for (uint32_t attempt = 0;; ++attempt) {
      auto raw = co_await tenantSnapshot(context.etcd, tenantOf(workerId));
      auto state = stateOf(raw);
      const auto pointerKey = deploymentActiveKey(workerId);
      const auto* kv = kvOf(raw, pointerKey);
      const auto* worker = state.worker(workerId);
      if (!kv || !worker) refuse("Worker " + workerId + " is not deployed (its limits travel with its active version)");
      if (!worker->pointerError.empty()) refuse("Worker " + workerId + " has an unusable pointer: " + worker->pointerError);
      Transaction txn;
      txn.unchanged(pointerKey, kv);
      txn.put(pointerKey, activePointerJson(worker->versionId, worker->definitionHash, limits));
      if (auto revision = co_await commitSecret(context.etcd, txn)) {
        out << "{\"operation\":\"limits\",\"workerId\":" << quote(workerId) << ",\"versionId\":" << quote(worker->versionId)
            << ",\"limits\":" << workerLimitsJson(limits) << ",\"previous\":" << workerLimitsJson(worker->limits)
            << ",\"revision\":\"" << revision << "\",\"attempts\":" << attempt + 1 << '}';
        break;
      }
      if (attempt >= 8) refuse("another write changed Worker " + workerId + "'s pointer (" + std::to_string(attempt + 1) + " attempts)");
      co_await context.timer.afterDelay(std::uniform_int_distribution<uint32_t>(5, 40)(random) * kj::MILLISECONDS);
    }
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

namespace {
struct ObjectCleanup { uint64_t deleted = 0, kept = 0, failed = 0; std::string skippedModules; };
std::string cleanupJson(const ObjectCleanup& c) {
  return "{\"deletedObjects\":" + std::to_string(c.deleted) + ",\"keptObjects\":" + std::to_string(c.kept) +
      ",\"failedObjects\":" + std::to_string(c.failed) + ",\"modules\":" + (c.skippedModules.empty() ? std::string("\"checked\"")
      : quote(c.skippedModules)) + '}';
}
// Deletes the objects of `gone` (definitions about to leave, or gone) that no definition of `remaining` references
// (control-plane-api.md section 5: reference counted by hash). A v2 package's modules and assets go only when no
// remaining package lists them; past 1,000 remaining packages the modules are kept (reading every manifest is the
// cost of the count).
kj::Promise<ObjectCleanup> cleanupObjects(ObjectStore& storage, const std::vector<std::shared_ptr<const VersionDefinition>>& gone,
    const std::vector<std::shared_ptr<const VersionDefinition>>& remaining) {
  ObjectCleanup result;
  std::set<std::string> keepDefinitions, keepContent, keepModules;
  std::set<std::string> remainingPackages;
  for (const auto& version : remaining) {
    keepDefinitions.insert(version->definitionHash);
    keepContent.insert(version->contentHash);
    if (version->format == "v2") remainingPackages.insert(version->contentHash);
  }
  std::set<std::string> objects;
  std::set<std::string> gonePackages;
  for (const auto& version : gone) {
    if (!keepDefinitions.contains(version->definitionHash)) objects.insert("definitions/" + version->definitionHash);
    if (keepContent.contains(version->contentHash)) { ++result.kept; continue; }
    if (version->format == "v2") { objects.insert("packages/" + version->contentHash); gonePackages.insert(version->contentHash); }
    else objects.insert("bundles/" + version->contentHash);
  }
  auto manifestHashes = [&](const std::string& packageHash) -> kj::Promise<std::optional<std::set<std::string>>> {
    auto response = co_await storage.get("packages/" + packageHash);
    if (response.status != 200) co_return std::nullopt;
    std::set<std::string> hashes;
    try {
      auto contents = readPackage(response.bytes);
      for (const auto& m : contents.modules) hashes.insert(m.sha256);
      for (const auto& a : contents.assets) hashes.insert(a.sha256);
    } catch (const kj::Exception&) { co_return std::nullopt; }
    co_return hashes;
  };
  if (!gonePackages.empty()) {
    if (remainingPackages.size() > 1000) {
      result.skippedModules = "kept: more than 1000 remaining packages to check";
    } else {
      bool complete = true;
      for (const auto& hash : remainingPackages) {
        auto hashes = co_await manifestHashes(hash);
        if (!hashes) { complete = false; break; }
        keepModules.insert(hashes->begin(), hashes->end());
      }
      if (!complete) result.skippedModules = "kept: a remaining package's manifest could not be read";
      else for (const auto& hash : gonePackages) {
        auto hashes = co_await manifestHashes(hash);
        if (!hashes) continue;  // already gone (a deletion that ran before)
        for (const auto& module : *hashes) if (!keepModules.contains(module)) objects.insert("modules/" + module);
      }
    }
  }
  // Packages last: a manifest stays readable until its modules are gone, so a deletion that stopped half way
  // finds them again when it runs again.
  std::vector<std::string> ordered;
  for (const auto& key : objects) if (!key.starts_with("packages/")) ordered.push_back(key);
  for (const auto& key : objects) if (key.starts_with("packages/")) ordered.push_back(key);
  for (const auto& key : ordered) {
    auto response = co_await storage.request(kj::HttpMethod::DELETE, key);
    if (response.status == 204 || response.status == 200 || response.status == 404) ++result.deleted;
    else ++result.failed;
  }
  co_return result;
}

std::vector<std::shared_ptr<const VersionDefinition>> definitionsOf(const DeploymentState& state,
    const std::function<bool(const std::string&)>& keep) {
  std::vector<std::shared_ptr<const VersionDefinition>> out;
  for (const auto& [label, definition] : state.definitions())
    if (definition.version && keep(definition.version->workerId)) out.push_back(definition.version);
  return out;
}
}  // namespace

// DELETE a version: only one the Worker's pointer does not name; then its objects that nothing else uses.
kj::Promise<std::string> controlDeleteVersion(ControlContext& context, std::string workerId, std::string versionId) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId) || !deploymentSafeName(versionId)) refuse("invalid workerId or versionId");
    std::mt19937 random(std::random_device{}());
    std::shared_ptr<const VersionDefinition> deleted;
    uint64_t revision = 0;
    for (uint32_t attempt = 0;; ++attempt) {
      auto raw = co_await tenantSnapshot(context.etcd, tenantOf(workerId));
      auto state = stateOf(raw);
      const auto key = deploymentDefinitionKey(workerId, versionId);
      const auto* kv = kvOf(raw, key);
      if (!kv) refuse("version " + versionId + " of Worker " + workerId + " was never uploaded");
      const auto pointerKey = deploymentActiveKey(workerId);
      const auto* worker = state.worker(workerId);
      if (worker && worker->versionId == versionId)
        refuse("version " + versionId + " is the active version of Worker " + workerId + ": activate another version first");
      deleted = state.definition(workerId + "@" + versionId);
      Transaction txn;
      txn.unchanged(key, kv);
      // A concurrent flip of the pointer to this version.
      txn.unchanged(pointerKey, kvOf(raw, pointerKey));
      txn.remove(key);
      if ((revision = co_await commitSecret(context.etcd, txn))) break;
      if (attempt >= 8) refuse("another write changed Worker " + workerId + " (" + std::to_string(attempt + 1) + " attempts); nothing deleted");
      co_await context.timer.afterDelay(std::uniform_int_distribution<uint32_t>(5, 40)(random) * kj::MILLISECONDS);
    }
    ObjectCleanup cleanup;
    if (deleted) {
      // Every tenant's definitions count (the same bytes may be another tenant's package too).
      auto all = stateOf(co_await context.etcd.range(DEPLOYMENT_PREFIX));
      cleanup = co_await cleanupObjects(context.storage, {deleted}, definitionsOf(all, [](const std::string&) { return true; }));
    }
    out << "{\"operation\":\"delete version\",\"workerId\":" << quote(workerId) << ",\"versionId\":" << quote(versionId)
        << ",\"revision\":\"" << revision << "\",\"objects\":" << cleanupJson(cleanup) << '}';
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

// Secrets (names and lengths in every answer, never a value).
kj::Promise<std::string> controlSecretPut(ControlContext& context, std::string workerId, std::string name, std::string value) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId)) refuse("invalid workerId (<tenant>/<worker>)");
    if (!environmentName(name)) refuse("invalid secret name (a JavaScript identifier of ASCII letters, digits, '_' and '$')");
    co_await secretPut(context.etcd, context.timer, workerId, name, value, 8, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str(), value);
  }
  co_return controlSuccess(out.str());
}
kj::Promise<std::string> controlSecretDelete(ControlContext& context, std::string workerId, std::string name) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId)) refuse("invalid workerId (<tenant>/<worker>)");
    if (!environmentName(name)) refuse("invalid secret name (a JavaScript identifier of ASCII letters, digits, '_' and '$')");
    co_await secretDelete(context.etcd, context.timer, workerId, name, 8, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}
kj::Promise<std::string> controlSecretList(ControlContext& context, std::string workerId) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId)) refuse("invalid workerId (<tenant>/<worker>)");
    co_await secretList(context.etcd, workerId, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

kj::Promise<TenantCounts> tenantCounts(EtcdClient& client, std::string tenant) {
  auto raw = co_await tenantSnapshot(client, tenant);
  auto state = stateOf(raw);
  TenantCounts counts;
  counts.revision = raw.revision;
  for (const auto& [label, definition] : state.definitions()) {
    const auto at = label.find('@');
    const auto id = label.substr(0, at);
    if (tenantOf(id) == tenant) counts.versions[id].insert(label.substr(at + 1));
  }
  for (const auto& [id, worker] : state.workers()) if (tenantOf(id) == tenant) counts.versions.try_emplace(id);
  for (const auto& [id, keys] : state.routesByWorker()) if (tenantOf(id) == tenant) counts.versions.try_emplace(id);
  for (const auto& [id, names] : state.secrets()) {
    if (tenantOf(id) != tenant) continue;
    for (const auto& [name, record] : names) counts.secrets[id].insert(name);
    counts.secretTotal += names.size();
  }
  co_return counts;
}

// The deletion cascade of a tenant's deployment state (control-plane-api.md section 5), each step idempotent: its
// routes, its pointers (nothing of it is served from here on), the objects only its definitions use, then every key
// below deploy/<tenant>/ (definitions, secrets, the Binding index).
kj::Promise<std::string> controlDeleteTenantState(ControlContext& context, std::string tenant) {
  std::ostringstream out;
  try {
    if (!tenantName(tenant)) refuse("invalid tenant");
    auto raw = co_await tenantSnapshot(context.etcd, tenant);
    auto state = stateOf(raw);
    auto commit = [&](const std::vector<std::string>& keys) -> kj::Promise<uint64_t> {
      uint64_t done = 0;
      for (size_t start = 0; start < keys.size(); start += 100) {
        Transaction txn;
        for (size_t i = start; i < std::min(keys.size(), start + 100); ++i) txn.remove(keys[i]);
        auto revision = co_await commitSecret(context.etcd, txn);
        KJ_REQUIRE(revision, "a deletion transaction failed");
        done += std::min(keys.size(), start + 100) - start;
      }
      co_return done;
    };
    std::vector<std::string> routes, pointers;
    for (const auto& [key, route] : state.routes())
      if (!route.value.workerId.empty() && tenantOf(route.value.workerId) == tenant) routes.push_back("routes/" + key);
    for (const auto& [id, worker] : state.workers()) if (tenantOf(id) == tenant) pointers.push_back(deploymentActiveKey(id));
    const auto deletedRoutes = co_await commit(routes);
    const auto deletedPointers = co_await commit(pointers);
    auto all = stateOf(co_await context.etcd.range(DEPLOYMENT_PREFIX));
    auto cleanup = co_await cleanupObjects(context.storage,
        definitionsOf(all, [&](const std::string& id) { return tenantOf(id) == tenant; }),
        definitionsOf(all, [&](const std::string& id) { return tenantOf(id) != tenant; }));
    // Everything left below the tenant's prefix, in one range deletion.
    const auto prefix = absolute(deploymentTenantPrefix(tenant));
    auto end = prefix;
    ++end.back();
    auto response = co_await context.etcd.request("/v3/kv/deleterange", "{\"key\":" + quote(base64(prefix)) +
        ",\"range_end\":" + quote(base64(end)) + "}");
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    out << "{\"operation\":\"delete tenant state\",\"tenant\":" << quote(tenant) << ",\"routes\":" << deletedRoutes
        << ",\"pointers\":" << deletedPointers << ",\"keys\":" << integer(json, "deleted") << ",\"objects\":"
        << cleanupJson(cleanup) << '}';
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

int runDeploymentTool(int argc, char** argv) {
  KJ_REQUIRE(argc > 0, "usage: coworkerd-apiserver tool <apply|upsert|remove|show|compile> --endpoint URL "
      "[--tenant T] [--manifest PATH] [--worker ID]... [--no-trial | --trial-server URL] [--trial-timeout-ms N] "
      "[--trial-address-map ADVERTISED=DIALED]... [--token TOKEN], or coworkerd-apiserver tool secret <put|delete|list> ..., "
      "or coworkerd-apiserver tool etcd-auth ...");
  const std::string operation = argv[0];
  // Its own parser: no argument of a secret command is ever echoed.
  if (operation == "secret") return runSecretTool(argc - 1, argv + 1);
  if (operation == "etcd-auth") return runEtcdAuthTool(argc - 1, argv + 1);
  std::string endpoint;
  ToolOptions options;
  bool tenantGiven = false;
  std::optional<uint32_t> maxRetries;
  ObjectStoreConfig objectConfig;
  if (auto value = std::getenv("COWORKERD_STORAGE_ENDPOINT")) objectConfig.endpoint = value;
  if (auto value = std::getenv("COWORKERD_MANAGEMENT_TOKEN")) options.token = value;
  if (auto value = std::getenv("COWORKERD_OBJECT_DIRECTORY")) {
    objectConfig.backend = "directory";
    objectConfig.directory = value;
  }
  auto number = [](const char* text, uint64_t limit, const char* name) {
    size_t end = 0;
    auto value = std::stoull(text, &end);
    KJ_REQUIRE(end == std::string(text).size() && value <= limit, "invalid numeric option", name);
    return value;
  };
  for (int index = 1; index < argc; index += 2) {
    std::string flag = argv[index];
    if (flag == "--no-trial") { options.trial = false; --index; continue; }
    KJ_REQUIRE(index + 1 < argc, "missing deployment option value", argv[index]);
    if (flag == "--endpoint") endpoint = argv[index + 1];
    else if (flag == "--tenant") {
      // The tenant whose Workers the manifest names; without it, the default tenant.
      options.tenant = argv[index + 1];
      KJ_REQUIRE(tenantName(options.tenant), "invalid tenant ([a-z0-9][a-z0-9-]{0,62})", argv[index + 1]);
      tenantGiven = true;
    }
    else if (flag == "--manifest") options.manifest = argv[index + 1];
    else if (flag == "--code-directory") options.codeRoot = argv[index + 1];
    else if (flag == "--storage-endpoint") objectConfig.endpoint = argv[index + 1];
    else if (flag == "--storage-bucket") objectConfig.bucket = argv[index + 1];
    else if (flag == "--storage-region") objectConfig.region = argv[index + 1];
    else if (flag == "--object-directory") {
      objectConfig.backend = "directory";
      objectConfig.directory = argv[index + 1];
    }
    else if (flag == "--pause-before-flip-ms") options.pauseMs = number(argv[index + 1], 60000, "pause-before-flip-ms");
    else if (flag == "--max-retries") maxRetries = number(argv[index + 1], 100, "max-retries");
    else if (flag == "--max-txn-ops") options.maxTxnOps = number(argv[index + 1], 100000, "max-txn-ops");
    else if (flag == "--token") options.token = argv[index + 1];
    else if (flag == "--trial-server") {
      const std::string url = argv[index + 1];
      KJ_REQUIRE(url.starts_with("http://") && url.size() > 7, "--trial-server takes an http:// URL", url.c_str());
      options.trialServer = url.ends_with('/') ? url.substr(0, url.size() - 1) : url;
    } else if (flag == "--trial-timeout-ms") {
      options.trialTimeoutMs = static_cast<uint32_t>(number(argv[index + 1], 60000, "trial-timeout-ms"));
      KJ_REQUIRE(options.trialTimeoutMs > 0, "--trial-timeout-ms must be positive");
    } else if (flag == "--trial-address-map") {
      // ADVERTISED=DIALED: a member advertises an address this tool cannot
      // dial (another network, a port forward); the trial goes to DIALED.
      const std::string pair = argv[index + 1];
      const auto equals = pair.find('=');
      KJ_REQUIRE(equals != std::string::npos && equals > 0 && equals + 1 < pair.size(),
          "--trial-address-map takes ADVERTISED=DIALED", pair.c_str());
      options.trialAddressMap[pair.substr(0, equals)] = pair.substr(equals + 1);
    }
    else if (flag == "--worker") {
      KJ_REQUIRE(deploymentSafeName(argv[index + 1]), "invalid workerId", argv[index + 1]);
      options.workers.push_back(argv[index + 1]);
    } else KJ_FAIL_REQUIRE("unknown deployment option", flag.c_str());
  }
  // A Worker's name on the command line is its name in the tenant.
  for (auto& worker : options.workers) worker = workerFullId(options.tenant, worker);
  if (operation == "compile") {
    KJ_REQUIRE(!options.manifest.empty(), "compile requires --manifest");
    auto input = readDeploymentManifest(options.manifest, options.tenant);
    readDeploymentCode(input, options.codeRoot.empty() ? options.manifest.parent_path() : options.codeRoot);
    // A manifest names secrets but holds no values; their presence is checked when deploying.
    auto state = DeploymentState::fromInput(input, 0, 0, false);
    auto problem = state.firstProblem();
    KJ_REQUIRE(problem.empty(), problem.c_str());
    KJ_REQUIRE(!state.routes().empty(), "at least one route is required");
    std::cout << "{\"definitions\":[";
    size_t index = 0;
    for (const auto& [label, version] : input.versions) {
      if (index++) std::cout << ',';
      std::cout << definitionJson(*version);
    }
    std::cout << "]}" << std::endl;
    return 0;
  }
  auto io = kj::setupAsyncIo();
  auto headers = kj::HttpHeaderTable::Builder().build();
  EtcdClient client(endpoint, 750, io.provider->getNetwork(), io.provider->getTimer(), *headers);
  if (operation == "apply" || operation == "upsert") {
    KJ_REQUIRE(!options.manifest.empty(), "apply and upsert require --manifest");
    KJ_REQUIRE(options.workers.empty(), "--worker is only for remove");
    // apply keeps the original whole-state contract: a conflict fails without retry.
    options.maxRetries = maxRetries.value_or(operation == "apply" ? 0 : 8);
    ObjectStore storage(objectConfig, io.provider->getNetwork(), io.provider->getTimer());
    TrialContext trials{io.provider->getNetwork(), *headers, options.token};
    write(operation == "apply" ? Operation::APPLY : Operation::UPSERT, client, io.provider->getTimer(), &storage, options, trials,
        std::cout).wait(io.waitScope);
  } else if (operation == "remove") {
    KJ_REQUIRE(!options.workers.empty(), "remove requires --worker");
    options.maxRetries = maxRetries.value_or(8);
    TrialContext trials{io.provider->getNetwork(), *headers, options.token};
    write(Operation::REMOVE, client, io.provider->getTimer(), nullptr, options, trials, std::cout).wait(io.waitScope);
  } else if (operation == "show") {
    // The whole deployment state, or with --tenant the tenant's (its Workers, and the routes they own).
    auto raw = (tenantGiven ? tenantSnapshot(client, options.tenant) : client.range(DEPLOYMENT_PREFIX)).wait(io.waitScope);
    auto state = stateOf(raw);
    std::string active = "{";
    size_t index = 0;
    for (const auto& [id, worker] : state.workers()) {
      if (!worker.valid) continue;
      if (index++) active += ',';
      active += quote(id) + ':' + activePointerJson(worker.version->versionId, worker.version->definitionHash);
    }
    active += '}';
    std::vector<RouteDefinition> routes;
    for (const auto& [key, route] : state.routes())
      if (route.effective && (!tenantGiven || tenantOf(route.value.workerId) == options.tenant)) routes.push_back(route.value);
    std::cout << "{\"generation\":" << state.generation() << ",\"revision\":\"" << raw.revision
              << "\",\"activeVersions\":" << active << ",\"routes\":" << routesJson(routes)
              << ',' << state.statusFields(STATUS_PROBLEM_LIMIT) << "}" << std::endl;
  } else KJ_FAIL_REQUIRE("unknown deployment operation", operation.c_str());
  return 0;
}
}  // namespace coworkerd

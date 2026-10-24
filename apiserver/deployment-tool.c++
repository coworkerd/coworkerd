#include "control.h"
#include "deployment.h"
#include "coworkerd.capnp.h"
#include "manifests.h"
#include "object-store.h"
#include "object-state.h"
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
using namespace json;
namespace rpc = v1::rpc;

uint64_t nowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

// Per-Worker maps in the full status are bounded; tests that need every
// Worker use the counts and digests.
constexpr size_t STATUS_PROBLEM_LIMIT = 20;
// A secret's value of fewer bytes than this occurs in reasons by chance ("1" in a revision number): only a longer one
// found in a failure's reason withholds the reason.
constexpr size_t WITHHELD_MIN_BYTES = 8;

// ---------------------------------------------------------------------------
// Deployment tool: one tenant's manifest at a time (common/tenant-manifest.h), its next revision written only if
// absent (manifests.h); a change of a tenant is one write, whatever it touches.

// A refusal reads as one sentence: "coworkerd-apiserver failed: <reason>".
[[noreturn]] void refuse(const std::string& reason) {
  kj::throwFatalException(kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::str(reason.c_str())));
}

// The control plane writes below a tenant only while its record says active; the operations tool also below a tenant
// without a record (its default tenant has none), never below one being deleted. The record is in the manifest the
// write changes: a deletion that marked it first makes the write's revision meet the deletion's, read it and refuse.
void requireWritable(const std::string& tenant, const TenantManifest& manifest, bool controlPlane) {
  if (!manifest.record) {
    if (controlPlane) refuse("there is no tenant " + tenant);
    return;
  }
  if (manifest.record->status != "active") refuse("tenant " + tenant + " is being deleted; nothing written");
}

TenantManifest currentOf(const ManifestStore& state, const std::string& tenant) {
  if (const auto* found = state.tenant(tenant)) return *found;
  TenantManifest empty;
  empty.tenant = tenant;
  return empty;
}

enum class Operation { APPLY, UPSERT, REMOVE };

struct ToolOptions {
  // The tenant whose Workers the change names; the manifest's names are read in it.
  std::string tenant = DEFAULT_TENANT;
  std::filesystem::path manifest, codeRoot;
  uint64_t pauseMs = 0;
  uint32_t maxRetries = 0;
  std::vector<std::string> workers;  // remove
  // Trial loads: on unless --no-trial. A named Server (--trial-server
  // URL) or else the cluster's members, Home first; a member's advertised
  // address may be mapped to the address this tool dials (--trial-address-map).
  bool trial = true;
  std::string trialServer;
  uint32_t trialTimeoutMs = 5000;
  std::map<std::string, std::string> trialAddressMap;
  // Where the members are: the heartbeats under members/ in `memberStore`, live within the TTL and grace of the
  // listing's Date.
  uint32_t memberTtlSeconds = 15, memberGraceMs = 10000;
  ObjectStore* memberStore = nullptr;
  // The Servers' management token, sent with every trial load: --token, else
  // COWORKERD_MANAGEMENT_TOKEN.
  std::string token;
  // The control plane (controlOptions): its writes need the tenant's record (active), and a route under the default
  // host * is written as defaultHostPrefix gives it. An upload publishes the manifest's version (objects and the
  // version's entry) and moves no pointer; an activation moves a Worker's pointer to a version published before.
  bool controlPlane = false;
  bool publishOnly = false;
  // An activating upload whose manifest lists no routes keeps the Worker's current ones.
  bool keepRoutes = false;
  // The cleanup's grace (control.cleanupGraceMs): an object found there that is older than half of it is written again.
  uint64_t cleanupGraceMs = 3600000;
  struct Activation {
    // An empty versionId is the version the Worker's pointer names at each attempt (PUT routes).
    std::string workerId, versionId;
    // The Worker's routes after the change; unset keeps the routes it has.
    std::optional<std::vector<RouteDefinition>> routes;
  };
  std::optional<Activation> activation;
};

struct Plan {
  std::vector<std::string> workers;                                 // Workers whose pointers this change writes
  std::vector<std::shared_ptr<const VersionDefinition>> published;  // versions new to the tenant's version tables
  std::set<std::string> removed;                                    // Workers whose pointers and routes this change removes
};

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

// The change, made on `next` (a copy of the tenant's current manifest), and validated with the evaluation the nodes
// use: the state after the change against the state before it -- the tenant's own keys and every tenant's routes.
Plan plan(Operation operation, TenantManifest& next, const ManifestStore& state, const DeploymentInput& desired,
    const ToolOptions& options) {
  Plan result;
  const auto& tenant = options.tenant;
  const TenantManifest base = next;
  const auto before = state.stateOf(tenant, &base);
  auto& removed = result.removed;
  const char* name = operation == Operation::APPLY ? "apply" : operation == Operation::UPSERT ? "upsert" : "remove";
  if (operation == Operation::REMOVE) {
    for (const auto& id : options.workers) {
      auto found = next.workers.find(workerNameOf(id));
      if (found == next.workers.end() || (!found->second.active && found->second.routes.empty()))
        refuse("Worker " + id + " is not deployed");
      found->second.active.reset();
      found->second.routes.clear();
      removed.insert(id);
    }
  } else {
    // Versions are immutable: a label names one definition for as long as it exists.
    for (const auto& [label, version] : desired.versions) {
      auto& worker = next.workers[workerNameOf(version->workerId)];
      if (auto existing = worker.versions.find(version->versionId); existing != worker.versions.end()) {
        if (existing->second.definitionHash != version->definitionHash)
          refuse("deployment label conflict: existing definition bytes differ for " + label + "; publish changes under a new versionId");
        continue;
      }
      worker.versions.emplace(version->versionId, ManifestVersion{version->definitionHash, base.revision + 1});
      result.published.push_back(version);
    }
    if (options.publishOnly) return result;
    std::map<std::string, std::vector<ManifestRoute>> routes;
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
      routes[route.workerId].push_back({route.host, route.pathPrefix});
      claimed[route.workerId].insert(route.host + route.pathPrefix);
    }
    std::set<std::string> batch;
    for (const auto& [id, pin] : desired.active) batch.insert(id);
    if (operation == Operation::APPLY) {
      if (desired.routes.empty()) refuse("at least one route is required");
      // The tenant's whole state: its Workers outside the manifest lose their pointers and routes (their versions and
      // secrets stay, as a remove leaves them).
      for (auto& [workerName, worker] : next.workers) {
        const auto id = workerFullId(tenant, workerName);
        if (batch.contains(id) || (!worker.active && worker.routes.empty())) continue;
        worker.active.reset();
        worker.routes.clear();
        removed.insert(id);
      }
    }
    for (const auto& id : batch) {
      const auto& pin = desired.active.at(id);
      auto& worker = next.workers[workerNameOf(id)];
      // The Worker's own limits stay with its pointer whatever version it names.
      const WorkerLimits limits = worker.active ? worker.active->limits : WorkerLimits{};
      worker.active = ManifestPointer{pin.versionId, pin.definitionHash, limits};
      auto list = routes[id];
      std::sort(list.begin(), list.end());
      worker.routes = std::move(list);
      result.workers.push_back(id);
    }
    // A route keeps its owner unless the owner is part of this change and no longer lists it, or is removed by it. An
    // unreadable value has no owner and may be repaired. Routes are every tenant's: another tenant's is never taken.
    for (const auto& [id, list] : routes) {
      for (const auto& route : list) {
        const auto relative = route.host + route.pathPrefix;
        auto existing = before.routes().find(relative);
        if (existing == before.routes().end()) continue;
        const auto& owner = existing->second.value.workerId;
        if (!(owner.empty() || owner == id || removed.contains(owner) || (batch.contains(owner) &&
            !(claimed.contains(owner) && claimed.at(owner).contains(relative)))))
          refuse("route " + relative + " belongs to Worker " + owner + "; include that Worker in the same upsert to move it");
      }
    }
  }
  // Validate exactly what will be written, with the shared evaluation.
  const auto after = state.stateOf(tenant, &next);
  if (operation == Operation::APPLY) {
    auto problem = after.firstProblem(tenant);
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
  return result;
}

// The immutable objects first (then the trial loads, then the manifest that names them -- a version exists in the
// desired state only once it loads). Content objects are written where missing; a definition is created only if
// absent, and one that exists must hold the same bytes. An object found there that is older than half the cleanup's
// grace is written again (its key is its content's hash): a deletion's cleanup removes only objects older than the grace,
// so none that this upload relies on goes before its manifest names it -- and `reused` lists the keys found there, which
// are checked once more after the manifest is written. A failed PUT leaves only immutable objects, never a partly
// published deployment.
kj::Promise<void> publishObjects(ObjectStore& storage, const DeploymentInput& desired, const DeploymentCode& code,
    uint64_t graceMs, std::ostream& report, std::map<std::string, std::string>* reused = nullptr) {
  const auto uploadStartedAtUnixMs = nowUnixMs();
  uint64_t uploaded = 0, skipped = 0, uploadedModules = 0, skippedModules = 0, uploadedAssets = 0, skippedAssets = 0, refreshed = 0;
  auto stale = [graceMs](const ObjectResponse& found) {
    return found.dateUnixMs && found.lastModifiedUnixMs && found.dateUnixMs > found.lastModifiedUnixMs + graceMs / 2;
  };
  std::map<std::string, std::string> definitions;
  for (const auto& [label, version] : desired.versions)
    definitions.emplace("definitions/" + version->definitionHash, definitionJson(*version));
  for (const auto& [key, bytes] : definitions) {
    auto existing = co_await storage.get(key);
    if (existing.status == 404) {
      auto created = co_await storage.create(key, bytes);
      if (created.created) { ++uploaded; continue; }
      existing = co_await storage.get(key);
    }
    requireStorage(existing.status == 200, "object store read failed: HTTP " + std::to_string(existing.status) + " (" + key + ")");
    if (existing.bytes != bytes)
      refuse(key + " holds other bytes than this definition (a damaged object); nothing published");
    if (stale(existing)) { co_await storage.put(key, bytes); ++refreshed; }
    if (reused) (*reused)[key] = bytes;
    ++skipped;
  }
  // The manifest's code objects: bundles/, packages/ and modules/ (code module
  // and static asset bytes share modules/<SHA-256>).
  for (const auto& [key, bytes] : code.objects) {
    const bool module = key.starts_with("modules/"), asset = code.assetObjects.contains(key);
    auto head = co_await storage.request(kj::HttpMethod::HEAD, key);
    requireStorage(head.status == 200 || head.status == 404, "object HEAD failed: HTTP " + std::to_string(head.status));
    if (head.status == 200) {
      if (stale(head)) { co_await storage.put(key, bytes); ++refreshed; }
      if (reused) (*reused)[key] = bytes;
      ++skipped; skippedModules += module; skippedAssets += asset;
    }
    else { co_await storage.put(key, bytes); ++uploaded; uploadedModules += module; uploadedAssets += asset; }
  }
  const auto uploadFinishedAtUnixMs = nowUnixMs();
  report << ",\"uploadStartedAtUnixMs\":" << uploadStartedAtUnixMs << ",\"uploadFinishedAtUnixMs\":" << uploadFinishedAtUnixMs
         << ",\"uploadedObjects\":" << uploaded << ",\"skippedObjects\":" << skipped << ",\"refreshedObjects\":" << refreshed
         << ",\"uploadedModules\":" << uploadedModules << ",\"skippedModules\":" << skippedModules;
  // Asset objects are reported only for deployments with assets, so the output
  // of code-only deployments keeps its fields.
  if (!code.assetObjects.empty()) report << ",\"uploadedAssets\":" << uploadedAssets << ",\"skippedAssets\":" << skippedAssets;
}

// ---------------------------------------------------------------------------
// Trial loads: after the immutable parts are published and before the
// flip, one Server loads each new version (POST /__coworkerd/trial-load). A
// version that does not load stops the change: nothing is flipped, the
// published objects stay (immutable, harmless).

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

// The cluster's nodes: the live heartbeats on the object store.
kj::Promise<std::vector<TrialMember>> trialMembers(const ToolOptions& options) {
  KJ_REQUIRE(options.memberStore != nullptr, "the members are on the object store: the trial loads need it (--storage-endpoint)");
  std::vector<TrialMember> live;
  for (auto& member : co_await readStoreMembers(*options.memberStore, options.memberTtlSeconds, options.memberGraceMs))
    live.push_back({member.id, member.address});
  co_return live;
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
kj::Promise<void> runTrials(kj::Timer& timer, TrialContext& context, const char* operation,
    const DeploymentInput& desired, const DeploymentState& before, const std::vector<std::string>& workers, uint64_t revision,
    const ToolOptions& options, std::ostream& report, std::ostream& out) {
  if (!options.trial) { report << ",\"trial\":{\"enabled\":false}"; co_return; }
  const auto phaseStarted = nowUnixMs();
  std::string records;
  size_t skipped = 0;
  std::vector<std::shared_ptr<const VersionDefinition>> versions;
  for (const auto& id : workers) {
    const auto& version = desired.versions.at(id + "@" + desired.active.at(id).versionId);
    const auto* current = before.worker(id);
    // Not new: the pointer already names this definition.
    if (current && current->pointerError.empty() && current->definitionHash == version->definitionHash) continue;
    if (!version->hasCode()) { ++skipped; continue; }  // static assets only: nothing to load
    versions.push_back(version);
  }
  std::vector<TrialMember> members;
  if (!options.trialServer.empty()) members.push_back({"", options.trialServer});
  else if (!versions.empty()) members = co_await trialMembers(options);
  auto fail = [&](const std::string& reason, const std::string& record) {
    out << "{\"operation\":\"" << operation << "\",\"flipped\":false,\"trial\":{\"enabled\":true,\"server\":"
        << (options.trialServer.empty() ? std::string("\"members\"") : quote(options.trialServer))
        << ",\"timeoutMs\":" << options.trialTimeoutMs << ",\"versions\":[" << records
        << (records.empty() || record.empty() ? "" : ",") << record << "],\"startedAtUnixMs\":" << phaseStarted
        << ",\"finishedAtUnixMs\":" << nowUnixMs() << "},\"reason\":" << quote(reason) << '}' << std::endl;
    refuse(reason);
  };
  if (!versions.empty() && members.empty())
    fail(options.controlPlane ? "no member of the cluster can run the trial load (no node has registered); start a node first"
        : "no member of the cluster can run the trial load (no node has registered); deploy with --no-trial "
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
              " (" + problems + "); nothing flipped" + (options.controlPlane ? std::string()
              : ". Name a reachable node with --trial-server URL, or deploy with --no-trial")
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

// Activation: the input that moves a Worker's pointer to a version published before -- its definition as stored --
// with the Worker's current usable routes, or the routes the activation names. Built from each attempt's manifest, so
// a retry sees the routes as they are then.
kj::Promise<DeploymentInput> activationInput(ManifestStore& state, const TenantManifest& manifest, const DeploymentState& before,
    ToolOptions::Activation activation) {
  if (activation.versionId.empty()) {
    const auto* worker = before.worker(activation.workerId);
    if (!worker || !worker->pointerError.empty()) refuse("Worker " + activation.workerId + " is not deployed (it has no active version)");
    activation.versionId = worker->versionId;
  }
  auto worker = manifest.workers.find(workerNameOf(activation.workerId));
  if (worker == manifest.workers.end() || !worker->second.versions.contains(activation.versionId))
    refuse("version " + activation.versionId + " of Worker " + activation.workerId + " was never uploaded");
  const auto hash = worker->second.versions.at(activation.versionId).definitionHash;
  co_await state.fetchDefinitions({hash});
  auto version = state.definition(hash);
  if (!version || version->key() != activation.workerId + "@" + activation.versionId) {
    auto failure = state.definitionFailures().find(hash);
    refuse("version " + activation.versionId + " of Worker " + activation.workerId + " has an unusable definition: " +
        (failure != state.definitionFailures().end() ? "definitions/" + hash.substr(0, 12) + " " + failure->second
            : std::string("its definition names another version")));
  }
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
  co_return input;
}

// The generation the nodes report once they applied the index this write named: the sum over the tenants.
uint64_t clusterGeneration(const ManifestStore& state) {
  uint64_t sum = 0;
  for (const auto& [tenant, manifest] : state.tenants()) sum += manifest.generation;
  return sum;
}

std::string conflictWaits(const std::vector<uint64_t>& waits) {
  std::string out = "[";
  for (size_t i = 0; i < waits.size(); ++i) out += (i ? "," : "") + std::to_string(waits[i]);
  return out + ']';
}

// The tool's writes. The record of a change goes to `out` (the tool's standard output); a refusal is an exception with
// one sentence, after a refused trial's record. The order: a dry run of the change on the current manifest (its
// refusals come first), the immutable objects, the trial loads, then the manifest's next revision -- the flip, computed
// again on the manifest as it is then, retried on a newer one, never published or tried again (a version exists in the
// desired state only once it loads).
kj::Promise<void> write(Operation operation, kj::Timer& timer, ManifestStore& state, ObjectStore* storage,
    const ToolOptions& options, TrialContext& trials, std::ostream& out) {
  const auto startedAtUnixMs = nowUnixMs();
  const auto& tenant = options.tenant;
  DeploymentInput desired;
  DeploymentCode code;
  if (operation != Operation::REMOVE && !options.activation) {
    desired = readDeploymentManifest(options.manifest, tenant);
    code = readDeploymentCode(desired, options.codeRoot.empty() ? options.manifest.parent_path() : options.codeRoot);
    for (const auto& [label, version] : desired.versions) state.rememberDefinition(*version);
  }
  const bool manifestRoutes = !desired.routes.empty();
  // The change's input as one manifest reads: an activation's version and routes, the routes an activating upload keeps,
  // the default host's prefixes as the control plane writes them.
  auto input = [&](const TenantManifest& manifest, const DeploymentState& before) -> kj::Promise<DeploymentInput> {
    DeploymentInput result = desired;
    if (options.activation) result = co_await activationInput(state, manifest, before, *options.activation);
    else if (options.keepRoutes && !manifestRoutes) {
      result.routes.clear();
      for (const auto& [id, pin] : result.active) {
        auto owned = before.routesByWorker().find(id);
        if (owned == before.routesByWorker().end()) continue;
        for (const auto& key : owned->second) {
          auto route = before.routes().find(key);
          if (route != before.routes().end() && route->second.error.empty()) result.routes.push_back(route->second.value);
        }
      }
    }
    if (options.controlPlane)
      for (auto& route : result.routes)
        if (route.host == "*") route.pathPrefix = defaultHostPrefix(tenantOf(route.workerId), route.pathPrefix);
    co_return result;
  };
  const char* name = options.activation ? "activate" : options.publishOnly ? "upload"
      : operation == Operation::APPLY ? "apply" : operation == Operation::UPSERT ? "upsert" : "remove";
  // The dry run: the tenant as the mirror holds it now.
  const auto readStarted = std::chrono::steady_clock::now();
  if (!state.tenant(tenant)) co_await state.reloadTenant(tenant);
  auto current = currentOf(state, tenant);
  requireWritable(tenant, current, options.controlPlane);
  co_await state.fetchDefinitions(namedDefinitions(current));
  const auto readRevision = state.index().revision;
  auto before = state.stateOf(tenant, &current);
  auto previewInput = co_await input(current, before);
  Plan preview;
  {
    TenantManifest dry = current;
    preview = plan(operation, dry, state, previewInput, options);
  }
  const auto plannedAt = std::chrono::steady_clock::now();
  std::ostringstream report;
  std::map<std::string, std::string> reused;
  if (operation == Operation::REMOVE) report << ",\"publishedDefinitions\":0";
  else {
    // An activated version was published when it was uploaded: its objects are in the store.
    if (!options.activation) co_await publishObjects(*storage, desired, code, options.cleanupGraceMs, report, &reused);
    // Every new version loads on a node before anything flips (before the manifest names it). An upload without
    // activation moves no pointer: its versions are tried when one is activated.
    if (options.publishOnly) report << ",\"trial\":{\"enabled\":false,\"atActivation\":true}";
    else co_await runTrials(timer, trials, name, previewInput, before, preview.workers, readRevision, options, report, out);
    report << ",\"publishedDefinitions\":" << preview.published.size();
  }
  if (options.pauseMs) co_await timer.afterDelay(options.pauseMs * kj::MILLISECONDS);
  Plan planned;
  const auto submittedAtUnixMs = nowUnixMs();
  auto written = co_await state.writeTenant(tenant, [&](TenantManifest& next) -> kj::Promise<void> {
    requireWritable(tenant, next, options.controlPlane);
    co_await state.fetchDefinitions(namedDefinitions(next));
    const TenantManifest base = next;
    auto attemptInput = co_await input(base, state.stateOf(tenant, &base));
    planned = plan(operation, next, state, attemptInput, options);
  }, !options.publishOnly, options.maxRetries + 1);
  // The objects this write found there, once more now that its manifest names them: one a cleanup removed meanwhile is
  // written again.
  uint64_t rewritten = 0;
  for (const auto& [key, bytes] : reused) {
    if (co_await storage->exists(key)) continue;
    co_await storage->put(key, bytes);
    ++rewritten;
  }
  if (rewritten) report << ",\"rewrittenAfterCommit\":" << rewritten;
  if (options.publishOnly) {
    out << "{\"operation\":\"upload\",\"flipped\":false,\"tenant\":" << quote(tenant) << ",\"revision\":\"" << state.index().revision
        << "\",\"readRevision\":\"" << readRevision << "\",\"manifestRevision\":" << written.manifest.revision
        << ",\"changed\":" << (written.changed ? "true" : "false") << ",\"attempts\":" << written.attempts << ",\"versions\":[";
    size_t index = 0;
    for (const auto& [label, version] : desired.versions) out << (index++ ? "," : "") << quote(label);
    out << "],\"startedAtUnixMs\":" << startedAtUnixMs << ",\"finishedAtUnixMs\":" << nowUnixMs() << report.str() << '}' << std::endl;
    co_return;
  }
  out << "{\"operation\":\"" << name << "\",\"tenant\":" << quote(tenant) << ",\"generation\":" << clusterGeneration(state)
      << ",\"tenantGeneration\":" << written.manifest.generation << ",\"revision\":\"" << state.index().revision
      << "\",\"readRevision\":\"" << readRevision << "\",\"manifestRevision\":" << written.manifest.revision
      << ",\"changed\":" << (written.changed ? "true" : "false") << ",\"indexed\":" << (written.indexed ? "true" : "false")
      << ",\"attempts\":" << written.attempts << ",\"retries\":" << written.attempts - 1
      << ",\"conflictWaitsMs\":" << conflictWaits(written.conflictWaitsMs) << ",\"workers\":[";
  for (size_t i = 0; i < planned.workers.size(); ++i) out << (i ? "," : "") << quote(planned.workers[i]);
  out << "],\"removed\":[";
  size_t index = 0;
  for (const auto& id : planned.removed) out << (index++ ? "," : "") << quote(id);
  out << "],\"startedAtUnixMs\":" << startedAtUnixMs << ",\"submittedAtUnixMs\":" << submittedAtUnixMs
      << ",\"committedAtUnixMs\":" << (written.committedAtUnixMs ? written.committedAtUnixMs : nowUnixMs())
      << ",\"readMicros\":" << std::chrono::duration_cast<std::chrono::microseconds>(plannedAt - readStarted).count()
      << report.str() << '}' << std::endl;
}

// ---------------------------------------------------------------------------
// Secrets: secret put <worker> <name> (value on standard input), secret delete
// <worker> <name>, secret list <worker>. A value is sealed with the cluster's secrets key before it is written into
// the tenant's manifest. Nothing here prints, logs or reports a value: output carries names, lengths and times.

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
        "coworkerd-apiserver tool secret put <worker> <name> [--tenant T] [--storage-endpoint URL] [--secrets-key-file FILE] "
        "< value-file (or type it when prompted)");
  refuse("usage: coworkerd-apiserver tool secret put|delete <worker> <name> [--tenant T] [--storage-endpoint URL] "
      "[--storage-bucket B] [--secrets-key-file FILE] [--max-retries N], or coworkerd-apiserver tool secret list <worker> "
      "[--tenant T] [--storage-endpoint URL] (--endpoint URL is accepted and unused)");
}

kj::Promise<void> secretPut(ManifestStore& state, std::string worker, std::string name, std::string value, uint32_t maxRetries,
    std::ostream& out, bool controlPlane = false) {
  if (auto problem = secretValueProblem(value); !problem.empty()) { value.clear(); refuse(problem + " (" + worker + "/" + name + ")"); }
  if (!state.secretsKey())
    refuse("a secret's value is sealed with the cluster's secrets key: set secrets.keyFile (or COWORKERD_SECRETS_KEY_FILE, "
        "--secrets-key-file)");
  const auto tenant = tenantOf(worker), workerName = workerNameOf(worker);
  bool existed = false, declared = false;
  uint64_t setAtUnixMs = 0;
  const auto readRevision = state.index().revision;
  auto written = co_await state.writeTenant(tenant, [&](TenantManifest& next) -> kj::Promise<void> {
    requireWritable(tenant, next, controlPlane);
    auto& entry = next.workers[workerName];
    existed = entry.secrets.contains(name);
    setAtUnixMs = nowUnixMs();
    entry.secrets[name] = state.secretsKey()->seal(secretAad(tenant, workerName, name), value, setAtUnixMs);
    // The active version, as read: whether it declares the name (a typo shows here).
    declared = false;
    if (entry.active) {
      const auto hash = entry.active->definitionHash;
      co_await state.fetchDefinitions({hash});
      if (auto version = state.definition(hash))
        declared = std::find(version->secrets.begin(), version->secrets.end(), name) != version->secrets.end();
    }
  }, true, maxRetries + 1);
  out << "{\"operation\":\"secret put\",\"workerId\":" << quote(worker) << ",\"name\":" << quote(name)
      << ",\"bytes\":" << value.size() << ",\"created\":" << (existed ? "false" : "true")
      << ",\"declaredByActiveVersion\":" << (declared ? "true" : "false")
      << ",\"setAtUnixMs\":" << setAtUnixMs << ",\"revision\":\"" << state.index().revision
      << "\",\"readRevision\":\"" << readRevision << "\",\"manifestRevision\":" << written.manifest.revision
      << ",\"attempts\":" << written.attempts << '}' << std::endl;
}

kj::Promise<void> secretDelete(ManifestStore& state, std::string worker, std::string name, uint32_t maxRetries, std::ostream& out,
    bool controlPlane = false) {
  const auto tenant = tenantOf(worker), workerName = workerNameOf(worker);
  const auto readRevision = state.index().revision;
  auto written = co_await state.writeTenant(tenant, [&](TenantManifest& next) -> kj::Promise<void> {
    requireWritable(tenant, next, controlPlane);
    auto entry = next.workers.find(workerName);
    if (entry == next.workers.end() || !entry->second.secrets.contains(name))
      refuse("secret " + name + " of Worker " + worker + " is not set");
    co_await state.fetchDefinitions(namedDefinitions(next));
    const TenantManifest base = next;
    next.workers[workerName].secrets.erase(name);
    // The shared evaluation decides: a delete must leave every valid Worker
    // valid (the active version must not declare the name). The Worker's own
    // reason first, then any Worker that binds it.
    const auto before = state.stateOf(tenant, &base), after = state.stateOf(tenant, &next);
    if (before.valid(worker) && !after.valid(worker))
      refuse("secret delete would make valid Worker " + worker + " invalid: " + after.worker(worker)->reason +
          "; deploy a version that does not declare " + name + " first");
    requireNoCollateral(before, after, {}, "secret delete");
  }, true, maxRetries + 1);
  out << "{\"operation\":\"secret delete\",\"workerId\":" << quote(worker) << ",\"name\":" << quote(name)
      << ",\"revision\":\"" << state.index().revision << "\",\"readRevision\":\"" << readRevision
      << "\",\"manifestRevision\":" << written.manifest.revision << ",\"attempts\":" << written.attempts << '}' << std::endl;
}

// Names, lengths and times: never a value. With this process's secrets key, a value that does not open with it is
// listed with its problem (bytes null), as the nodes would find it.
std::string secretEntries(const ManifestStore& state, const std::string& tenant, const std::string& workerName,
    const ManifestWorker& worker, bool times) {
  std::string out;
  size_t index = 0;
  for (const auto& [name, sealed] : worker.secrets) {
    auto length = sealedLength(sealed);
    std::string problem;
    if (!length) problem = "the sealed record cannot be read";
    else if (state.secretsKey()) {
      auto value = state.secretsKey()->open(secretAad(tenant, workerName, name), sealed);
      if (!value) problem = "the value does not open with this process's secrets key";
      else std::fill(value->begin(), value->end(), '\0');
    }
    out += std::string(index++ ? "," : "") + "{\"name\":" + quote(name) + ",\"bytes\":";
    if (problem.empty()) out += std::to_string(*length) + (times ? ",\"setAtUnixMs\":" + std::to_string(sealed.setAtUnixMs) : "");
    else out += std::string("null") + (times ? ",\"setAtUnixMs\":null" : "") + ",\"problem\":" + quote(problem);
    out += '}';
  }
  return out;
}

void secretList(const ManifestStore& state, std::string worker, std::ostream& out) {
  const auto tenant = tenantOf(worker), workerName = workerNameOf(worker);
  out << "{\"workerId\":" << quote(worker) << ",\"revision\":\"" << state.index().revision << "\",\"secrets\":[";
  if (const auto* manifest = state.tenant(tenant)) {
    if (auto found = manifest->workers.find(workerName); found != manifest->workers.end())
      out << secretEntries(state, tenant, workerName, found->second, true);
  }
  out << "]}" << std::endl;
}

// The object store and the secrets key of a tool invocation: the options' values, else the environment's.
struct StoreArguments {
  ObjectStoreConfig config;
  std::string keyFile;
};
StoreArguments storeArguments() {
  StoreArguments arguments;
  if (auto value = std::getenv("COWORKERD_STORAGE_ENDPOINT")) arguments.config.endpoint = value;
  if (auto value = std::getenv("COWORKERD_STORAGE_BUCKET")) arguments.config.bucket = value;
  if (auto value = std::getenv("COWORKERD_OBJECT_DIRECTORY")) {
    arguments.config.backend = "directory";
    arguments.config.directory = value;
  }
  if (auto value = std::getenv("COWORKERD_SECRETS_KEY_FILE"); value != nullptr && *value) arguments.keyFile = value;
  return arguments;
}
std::optional<SecretsKey> loadKey(const std::string& file) {
  if (file.empty()) return std::nullopt;
  return SecretsKey::load(file);
}

int runSecretTool(int argc, char** argv) {
  if (argc < 1) refuseSecretUsage("");
  const std::string operation = argv[0];
  if (operation != "put" && operation != "delete" && operation != "list") refuseSecretUsage("");
  const size_t positionals = operation == "list" ? 1 : 2;
  std::vector<std::string> names;
  std::string tenant = DEFAULT_TENANT;
  auto arguments = storeArguments();
  uint32_t maxRetries = 8;
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    // The address of the store 0.3 kept the deployment state in: accepted, unused.
    if (argument == "--endpoint" && index + 1 < argc) { ++index; continue; }
    if (argument == "--storage-endpoint" && index + 1 < argc) { arguments.config.endpoint = argv[++index]; continue; }
    if (argument == "--storage-bucket" && index + 1 < argc) { arguments.config.bucket = argv[++index]; continue; }
    if (argument == "--storage-region" && index + 1 < argc) { arguments.config.region = argv[++index]; continue; }
    if (argument == "--secrets-key-file" && index + 1 < argc) { arguments.keyFile = argv[++index]; continue; }
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
  if (names.size() != positionals || arguments.config.endpoint.empty()) refuseSecretUsage(operation);
  // Not echoed either: a value typed in place of a name must not reach the output.
  if (!deploymentSafeName(names[0])) refuse("invalid workerId (letters, digits, '-', '_' and '.')");
  if (positionals == 2 && !environmentName(names[1]))
    refuse("invalid secret name (a JavaScript identifier of ASCII letters, digits, '_' and '$')");
  std::string value;
  if (operation == "put") value = readSecretValue();
  auto io = kj::setupAsyncIo();
  ObjectStore storage(arguments.config, io.provider->getNetwork(), io.provider->getTimer());
  ManifestStore state(storage, io.provider->getTimer(), loadKey(arguments.keyFile), std::cerr);
  const auto worker = workerFullId(tenant, names[0]);
  try {
    state.load(false, false).wait(io.waitScope);
    if (operation == "put") secretPut(state, worker, names[1], value, maxRetries, std::cout).wait(io.waitScope);
    else if (operation == "delete") secretDelete(state, worker, names[1], maxRetries, std::cout).wait(io.waitScope);
    else secretList(state, worker, std::cout);
  } catch (const kj::Exception& error) {
    // Defence in depth: a failure description never quotes the value.
    std::string description = error.getDescription().cStr();
    if (operation == "put" && value.size() >= WITHHELD_MIN_BYTES && (description.find(value) != std::string::npos ||
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
  if (withheld.size() >= WITHHELD_MIN_BYTES &&
      (reason.find(withheld) != std::string::npos || reason.find(base64(withheld)) != std::string::npos))
    reason = "secret put failed (detail withheld: it would quote the value)";
  auto record = printed;
  while (!record.empty() && (record.back() == '\n' || record.back() == ' ')) record.pop_back();
  // Only a refused trial prints before it refuses: its record is the last line.
  if (auto line = record.rfind('\n'); line != std::string::npos) record = record.substr(line + 1);
  // The failure's kind decides its answer (main.c++), never its reason's words: the store's, another writer's, or else.
  const char* kind = isStorageFailure(error) ? "storage" : isWriteConflict(error) ? "conflict"
      : error.getType() == kj::Exception::Type::DISCONNECTED ? "disconnected"
      : error.getType() == kj::Exception::Type::OVERLOADED ? "overloaded" : "failed";
  return "{\"ok\":false,\"kind\":\"" + std::string(kind) + "\",\"reason\":" + quote(reason) +
      ",\"record\":" + (record.starts_with("{") ? record : std::string("null")) + '}';
}
ToolOptions controlOptions(ControlContext& context) {
  ToolOptions options;
  options.controlPlane = true;
  options.maxRetries = 8;
  options.token = context.token;
  options.memberTtlSeconds = context.memberTtlSeconds;
  options.memberGraceMs = context.memberGraceMs;
  options.memberStore = &context.storage;
  options.cleanupGraceMs = context.cleanupGraceMs;
  return options;
}
}  // namespace

// The Workers of a tenant its manifest names (a pointer, a version, a route or a secret), with their versions in upload
// order, active version, validity, limits, routes and secrets (names and lengths) -- or one of them.
kj::Promise<std::string> controlWorkers(ManifestStore& state, std::string tenant, std::optional<std::string> only) {
  std::ostringstream printed;
  try {
    const auto manifest = currentOf(state, tenant);
    co_await state.fetchDefinitions(namedDefinitions(manifest));
    auto current = state.stateOf(tenant, &manifest);
    std::set<std::string> ids;
    for (const auto& [name, worker] : manifest.workers) if (!worker.empty()) ids.insert(workerFullId(tenant, name));
    if (only && !ids.contains(*only)) refuse("Worker " + *only + " is not deployed");
    auto describe = [&](const std::string& id) {
      const auto& entry = manifest.workers.at(workerNameOf(id));
      std::string out = "{\"tenant\":" + quote(tenant) + ",\"worker\":" + quote(workerNameOf(id)) + ",\"workerId\":" + quote(id) +
          ",\"activeVersion\":";
      if (const auto* worker = current.worker(id)) {
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
      std::vector<std::pair<std::pair<uint64_t, std::string>, std::string>> versions;
      for (const auto& [versionId, version] : entry.versions) {
        std::string item = "{\"versionId\":" + quote(versionId) + ",\"uploadedRevision\":\"" + std::to_string(version.uploadedRevision) + '"';
        auto definition = state.definition(version.definitionHash);
        if (definition && definition->key() == id + "@" + versionId) {
          item += ",\"definitionHash\":" + quote(definition->definitionHash) + ",\"contentHash\":" + quote(definition->contentHash) +
              ",\"format\":" + quote(definition->format) + ",\"compatibilityDate\":" + quote(definition->compatibilityDate) +
              ",\"mainModule\":" + quote(definition->mainModule) + ",\"assets\":" + (definition->assets ? "true" : "false") + ",\"secrets\":[";
          for (size_t i = 0; i < definition->secrets.size(); ++i) item += (i ? "," : "") + quote(definition->secrets[i]);
          item += ']';
        } else {
          auto failure = state.definitionFailures().find(version.definitionHash);
          item += ",\"definitionHash\":" + quote(version.definitionHash) + ",\"problem\":" +
              quote(definition ? "the definition names another version" : "definitions/" + version.definitionHash.substr(0, 12) + " " +
                  (failure != state.definitionFailures().end() ? failure->second : std::string("not read")));
        }
        versions.push_back({{version.uploadedRevision, versionId}, item + '}'});
      }
      std::sort(versions.begin(), versions.end());
      out += ",\"versions\":[";
      for (size_t i = 0; i < versions.size(); ++i) out += (i ? "," : "") + versions[i].second;
      out += "],\"routes\":[";
      size_t routes = 0;
      if (auto owned = current.routesByWorker().find(id); owned != current.routesByWorker().end()) {
        for (const auto& key : owned->second) {
          auto route = current.routes().find(key);
          if (route == current.routes().end()) continue;
          out += std::string(routes++ ? "," : "") + "{\"host\":" + quote(route->second.value.host) + ",\"pathPrefix\":" +
              quote(route->second.value.pathPrefix) + ",\"effective\":" + (route->second.effective ? "true" : "false") + '}';
        }
      }
      return out + "],\"secrets\":[" + secretEntries(state, tenant, workerNameOf(id), entry, false) + "]}";
    };
    if (only) {
      printed << describe(*only);
    } else {
      printed << "{\"tenant\":" << quote(tenant) << ",\"revision\":\"" << state.index().revision << "\",\"generation\":"
              << current.generation() << ",\"workers\":[";
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
    auto options = controlOptions(context);
    options.tenant = tenant;
    options.manifest = manifest;
    options.publishOnly = !activate;
    options.keepRoutes = true;
    TrialContext trials{context.network, context.headers, context.token};
    co_await write(Operation::UPSERT, context.timer, context.state, &context.storage, options, trials, out);
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
      const auto manifest = currentOf(context.state, tenantOf(workerId));
      co_await context.state.fetchDefinitions(namedDefinitions(manifest));
      auto state = context.state.stateOf(tenantOf(workerId), &manifest);
      const auto* worker = state.worker(workerId);
      if (worker && worker->valid && worker->versionId == versionId) {
        co_return controlSuccess("{\"operation\":\"activate\",\"unchanged\":true,\"workerId\":" + quote(workerId) +
            ",\"versionId\":" + quote(versionId) + ",\"readRevision\":\"" + std::to_string(context.state.index().revision) + "\"}");
      }
    }
    auto options = controlOptions(context);
    options.tenant = tenantOf(workerId);
    options.activation = ToolOptions::Activation{workerId, versionId, routes};
    TrialContext trials{context.network, context.headers, context.token};
    co_await write(Operation::UPSERT, context.timer, context.state, nullptr, options, trials, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

// DELETE a Worker: its pointer and routes (its versions and secrets stay, as with the tool's remove).
kj::Promise<std::string> controlRemove(ControlContext& context, std::string workerId) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId)) refuse("invalid workerId");
    auto options = controlOptions(context);
    options.tenant = tenantOf(workerId);
    options.workers = {workerId};
    TrialContext trials{context.network, context.headers, context.token};
    co_await write(Operation::REMOVE, context.timer, context.state, nullptr, options, trials, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

// A Worker's own limits: its pointer rewritten with them.
kj::Promise<std::string> controlLimits(ControlContext& context, std::string workerId, WorkerLimits limits) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId)) refuse("invalid workerId");
    const auto tenant = tenantOf(workerId);
    std::string versionId;
    WorkerLimits previous;
    auto written = co_await context.state.writeTenant(tenant, [&](TenantManifest& next) -> kj::Promise<void> {
      requireWritable(tenant, next, true);
      auto entry = next.workers.find(workerNameOf(workerId));
      if (entry == next.workers.end() || !entry->second.active)
        refuse("Worker " + workerId + " is not deployed (its limits travel with its active version)");
      versionId = entry->second.active->versionId;
      previous = entry->second.active->limits;
      entry->second.active->limits = limits;
      co_return;
    }, true);
    out << "{\"operation\":\"limits\",\"workerId\":" << quote(workerId) << ",\"versionId\":" << quote(versionId)
        << ",\"limits\":" << workerLimitsJson(limits) << ",\"previous\":" << workerLimitsJson(previous)
        << ",\"revision\":\"" << context.state.index().revision << "\",\"manifestRevision\":" << written.manifest.revision
        << ",\"attempts\":" << written.attempts << '}';
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

namespace {
// What a deletion's cleanup removes, and what it leaves for later: an object younger than the grace (an upload may be
// about to name an object it found there: control.cleanupGraceMs, which uploads keep the objects they find within by
// writing them again), or one whose references cannot be read now (a remaining version's definition, or a remaining
// package's manifest -- or more than 1,000 of them to read). Those are listed in one object per cleanup below
// cluster/cleanup/, named by when it is due (20 digits, Unix ms) and a random value; the control plane's sweep
// (controlSweepCleanup) takes each up once due, removes what is still unreferenced and old enough, and lists the rest
// again. A removal the store refuses is a failure: the deletion answers 503 and, run again, resumes.
constexpr const char* CLEANUP_PREFIX = "cluster/cleanup/";
constexpr size_t MODULE_CHECK_LIMIT = 1000;
struct ObjectCleanup {
  uint64_t deleted = 0, kept = 0, failed = 0, deferred = 0;
  std::string modules = "checked";
  std::vector<std::string> deferredKeys;
};
std::string cleanupJson(const ObjectCleanup& c) {
  return "{\"deletedObjects\":" + std::to_string(c.deleted) + ",\"keptObjects\":" + std::to_string(c.kept) +
      ",\"failedObjects\":" + std::to_string(c.failed) + ",\"deferredObjects\":" + std::to_string(c.deferred) +
      ",\"modules\":" + quote(c.modules) + '}';
}
// What the remaining versions reference: their definitions, their content (packages and bundles), and -- read only when
// a module is a candidate -- the modules their packages list.
struct References {
  std::set<std::string> definitions, content, packages;
  bool complete = true;  // every remaining version's definition was read
  std::optional<std::set<std::string>> modules;
  std::string modulesUnknown;  // why `modules` could not be read
};
References referencesOf(const std::vector<std::shared_ptr<const VersionDefinition>>& remaining, bool complete) {
  References refs;
  refs.complete = complete;
  for (const auto& version : remaining) {
    refs.definitions.insert(version->definitionHash);
    refs.content.insert(version->contentHash);
    if (version->format == "v2") refs.packages.insert(version->contentHash);
  }
  return refs;
}
// A package's modules and assets (its manifest's hashes); none when it cannot be read (`missing`: it is not there).
kj::Promise<std::optional<std::set<std::string>>> packageHashes(ObjectStore& storage, std::string packageHash, bool* missing = nullptr) {
  auto response = co_await storage.get("packages/" + packageHash);
  if (missing) *missing = response.status == 404;
  if (response.status != 200) co_return std::nullopt;
  std::set<std::string> hashes;
  try {
    auto contents = readPackage(response.bytes);
    for (const auto& m : contents.modules) hashes.insert(m.sha256);
    for (const auto& a : contents.assets) hashes.insert(a.sha256);
  } catch (const kj::Exception&) { co_return std::nullopt; }
  co_return hashes;
}
kj::Promise<void> readModules(ObjectStore& storage, References& refs) {
  if (refs.modules || !refs.modulesUnknown.empty()) co_return;
  if (refs.packages.size() > MODULE_CHECK_LIMIT) {
    refs.modulesUnknown = "deferred: more than 1000 remaining packages to check";
    co_return;
  }
  std::set<std::string> modules;
  for (const auto& hash : refs.packages) {
    auto hashes = co_await packageHashes(storage, hash);
    if (!hashes) { refs.modulesUnknown = "deferred: a remaining package's manifest could not be read"; co_return; }
    modules.insert(hashes->begin(), hashes->end());
  }
  refs.modules = std::move(modules);
}
// The candidates removed that no remaining version references and that are older than the grace; the others deferred
// (or, when referenced now, dropped). A package goes only after its modules: they are found through it. Modules first,
// then packages and bundles, then definitions -- a cleanup that stops half way finds the rest again through what is left.
kj::Promise<void> removeUnreferenced(ObjectStore& storage, std::set<std::string> candidates, References& refs, uint64_t graceMs,
    ObjectCleanup& result) {
  auto defer = [&](const std::string& key) { result.deferredKeys.push_back(key); ++result.deferred; };
  if (!refs.complete) {
    // What the remaining versions reference is unknown: nothing goes now.
    for (const auto& key : candidates) defer(key);
    result.modules = "deferred: a remaining version's definition could not be read";
    co_return;
  }
  auto referenced = [&](const std::string& key) {
    const auto slash = key.find('/');
    const auto kind = key.substr(0, slash), hash = key.substr(slash + 1);
    if (kind == "definitions") return refs.definitions.contains(hash);
    if (kind == "packages" || kind == "bundles") return refs.content.contains(hash);
    return refs.modules && refs.modules->contains(hash);
  };
  // An object's age by the store's own clock (its Last-Modified against the answer's Date): young, or gone already.
  enum class Age { GONE, YOUNG, OLD };
  auto age = [&](const std::string& key) -> kj::Promise<Age> {
    auto head = co_await storage.request(kj::HttpMethod::HEAD, key);
    if (head.status == 404) co_return Age::GONE;
    requireStorage(head.status == 200, "object HEAD failed: HTTP " + std::to_string(head.status));
    if (!head.dateUnixMs || !head.lastModifiedUnixMs) co_return graceMs ? Age::YOUNG : Age::OLD;
    co_return head.dateUnixMs >= head.lastModifiedUnixMs + graceMs ? Age::OLD : Age::YOUNG;
  };
  auto remove = [&](const std::string& key) -> kj::Promise<void> {
    auto response = co_await storage.request(kj::HttpMethod::DELETE, key);
    if (response.status == 204 || response.status == 200 || response.status == 404) ++result.deleted;
    else ++result.failed;
  };
  std::vector<std::string> modules, content, definitions;
  for (const auto& key : candidates) {
    if (referenced(key)) continue;
    if (key.starts_with("modules/")) modules.push_back(key);
    else if (key.starts_with("definitions/")) definitions.push_back(key);
    else content.push_back(key);
  }
  // The packages to go now, and their modules: a package found young, or whose modules cannot be checked, stays (with
  // its modules) for the sweep.
  std::vector<std::string> goingContent;
  for (const auto& key : content) {
    const auto state = co_await age(key);
    if (state == Age::GONE) { ++result.deleted; continue; }
    if (state == Age::YOUNG) { defer(key); continue; }
    if (key.starts_with("packages/")) {
      bool missing = false;
      auto hashes = co_await packageHashes(storage, key.substr(9), &missing);
      if (hashes && !hashes->empty()) {
        co_await readModules(storage, refs);
        if (!refs.modules) { result.modules = refs.modulesUnknown; defer(key); continue; }
        for (const auto& hash : *hashes) if (!refs.modules->contains(hash)) modules.push_back("modules/" + hash);
      } else if (!hashes && !missing) {
        result.modules = "deferred: a package's manifest could not be read";
        defer(key);
        continue;
      }
    }
    goingContent.push_back(key);
  }
  std::sort(modules.begin(), modules.end());
  modules.erase(std::unique(modules.begin(), modules.end()), modules.end());
  for (const auto& key : modules) {
    const auto state = co_await age(key);
    if (state == Age::GONE) ++result.deleted;
    else if (state == Age::YOUNG) defer(key);
    else co_await remove(key);
  }
  for (const auto& key : goingContent) co_await remove(key);
  for (const auto& key : definitions) {
    const auto state = co_await age(key);
    if (state == Age::GONE) ++result.deleted;
    else if (state == Age::YOUNG) defer(key);
    else co_await remove(key);
  }
}
// The deferred keys of a cleanup, as an object below cluster/cleanup/ due when the youngest of them is older than the
// grace (the sweep checks each one's age again).
kj::Promise<void> deferCleanup(ObjectStore& storage, const std::vector<std::string>& keys, uint64_t graceMs) {
  if (keys.empty()) co_return;
  const auto due = realClockUnixMs() + std::max<uint64_t>(graceMs, 60000);
  std::string digits = std::to_string(due);
  digits.insert(0, REVISION_DIGITS - std::min(digits.size(), REVISION_DIGITS), '0');
  std::string body = "{\"dueAtUnixMs\":" + std::to_string(due) + ",\"graceMs\":" + std::to_string(graceMs) + ",\"keys\":[";
  for (size_t i = 0; i < keys.size(); ++i) body += (i ? "," : "") + quote(keys[i]);
  co_await storage.put(CLEANUP_PREFIX + digits + "-" + newWriterInstance().substr(0, 12), body + "]}");
}
// Deletes the objects of `gone` (definitions about to leave, or gone) that no definition of `remaining` references
// (control-plane-api.md section 5: reference counted by hash): their definitions, their packages or bundles, and a
// package's modules and assets that no remaining package lists. What cannot go now is deferred (above).
kj::Promise<ObjectCleanup> cleanupObjects(ObjectStore& storage, const std::vector<std::shared_ptr<const VersionDefinition>>& gone,
    const std::vector<std::shared_ptr<const VersionDefinition>>& remaining, bool complete, uint64_t graceMs) {
  ObjectCleanup result;
  auto refs = referencesOf(remaining, complete);
  std::set<std::string> candidates;
  for (const auto& version : gone) {
    if (!refs.definitions.contains(version->definitionHash)) candidates.insert("definitions/" + version->definitionHash);
    if (refs.content.contains(version->contentHash)) { ++result.kept; continue; }
    candidates.insert((version->format == "v2" ? "packages/" : "bundles/") + version->contentHash);
  }
  co_await removeUnreferenced(storage, std::move(candidates), refs, graceMs, result);
  co_await deferCleanup(storage, result.deferredKeys, graceMs);
  co_return result;
}

// Every definition the tenants `keep` selects name, and whether each one was read.
struct Definitions { std::vector<std::shared_ptr<const VersionDefinition>> versions; bool complete = true; };
kj::Promise<Definitions> definitionsOf(ManifestStore& state, std::function<bool(const std::string&)> keep,
    const std::string& except = {}) {
  std::set<std::string> hashes;
  for (const auto& [tenant, manifest] : state.tenants()) if (keep(tenant)) hashes.merge(namedDefinitions(manifest));
  hashes.erase(except);
  co_await state.fetchDefinitions(hashes);
  Definitions result;
  for (const auto& hash : hashes) {
    if (auto version = state.definition(hash)) result.versions.push_back(version);
    else result.complete = false;
  }
  co_return result;
}
}  // namespace

// DELETE a version: only one the Worker's pointer does not name. Its objects that nothing else uses go first, within the
// tenant's turn: a removal the store refuses leaves the version in the manifest (503, nothing committed); then the
// manifest without it.
kj::Promise<std::string> controlDeleteVersion(ControlContext& context, std::string workerId, std::string versionId) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId) || !deploymentSafeName(versionId)) refuse("invalid workerId or versionId");
    const auto tenant = tenantOf(workerId);
    ObjectCleanup cleanup;
    auto written = co_await context.state.writeTenant(tenant, [&](TenantManifest& next) -> kj::Promise<void> {
      requireWritable(tenant, next, true);
      auto entry = next.workers.find(workerNameOf(workerId));
      if (entry == next.workers.end() || !entry->second.versions.contains(versionId))
        refuse("version " + versionId + " of Worker " + workerId + " was never uploaded");
      if (entry->second.active && entry->second.active->versionId == versionId)
        refuse("version " + versionId + " is the active version of Worker " + workerId + ": activate another version first");
      const auto hash = entry->second.versions.at(versionId).definitionHash;
      co_await context.state.fetchDefinitions({hash});
      cleanup = {};
      if (auto deleted = context.state.definition(hash)) {
        // Every tenant's definitions count (the same bytes may be another tenant's package too), this version's aside.
        auto remaining = co_await definitionsOf(context.state, [](const std::string&) { return true; }, hash);
        cleanup = co_await cleanupObjects(context.storage, {deleted}, remaining.versions, remaining.complete, context.cleanupGraceMs);
        if (cleanup.failed)
          failStorage("the object store refused to remove " + std::to_string(cleanup.failed) + " of the version's objects; "
              "the version stays (DELETE it again)");
      }
      entry->second.versions.erase(versionId);
    }, true);
    out << "{\"operation\":\"delete version\",\"workerId\":" << quote(workerId) << ",\"versionId\":" << quote(versionId)
        << ",\"revision\":\"" << context.state.index().revision << "\",\"manifestRevision\":" << written.manifest.revision
        << ",\"objects\":" << cleanupJson(cleanup) << '}';
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
    co_await secretPut(context.state, workerId, name, value, 8, out, true);
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
    co_await secretDelete(context.state, workerId, name, 8, out, true);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}
kj::Promise<std::string> controlSecretList(ControlContext& context, std::string workerId) {
  std::ostringstream out;
  try {
    if (!workerFullName(workerId)) refuse("invalid workerId (<tenant>/<worker>)");
    secretList(context.state, workerId, out);
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

TenantCounts tenantCounts(const ManifestStore& state, const std::string& tenant) {
  TenantCounts counts;
  counts.revision = state.index().revision;
  const auto* manifest = state.tenant(tenant);
  if (!manifest) return counts;
  for (const auto& [name, worker] : manifest->workers) {
    if (worker.empty()) continue;
    const auto id = workerFullId(tenant, name);
    auto& versions = counts.versions[id];
    for (const auto& [versionId, version] : worker.versions) versions.insert(versionId);
    for (const auto& [secret, sealed] : worker.secrets) counts.secrets[id].insert(secret);
    counts.secretTotal += worker.secrets.size();
  }
  return counts;
}

// The objects of a tenant's deletion cascade (control-plane-api.md section 5): what only its definitions reference. A
// removal the store refuses fails it (the deletion answers DELETION_INCOMPLETE and resumes when run again); what cannot
// go now is deferred to the sweep.
kj::Promise<std::string> controlDeleteTenantObjects(ControlContext& context, std::string tenant) {
  std::ostringstream out;
  try {
    if (!tenantName(tenant)) refuse("invalid tenant");
    auto gone = co_await definitionsOf(context.state, [&](const std::string& name) { return name == tenant; });
    auto remaining = co_await definitionsOf(context.state, [&](const std::string& name) { return name != tenant; });
    auto cleanup = co_await cleanupObjects(context.storage, gone.versions, remaining.versions, remaining.complete,
        context.cleanupGraceMs);
    out << "{\"operation\":\"delete tenant objects\",\"tenant\":" << quote(tenant) << ",\"definitions\":" << gone.versions.size()
        << ",\"objects\":" << cleanupJson(cleanup) << '}';
    if (!gone.complete) refuse("a definition of tenant " + tenant + " could not be read, so the objects it names are unknown");
    if (cleanup.failed)
      failStorage("the object store refused to remove " + std::to_string(cleanup.failed) + " of tenant " + tenant + "'s objects");
  } catch (const kj::Exception& error) {
    co_return controlFailure(error, out.str());
  }
  co_return controlSuccess(out.str());
}

// The cleanups deferred below cluster/cleanup/ that are due: each one's keys removed when no remaining version
// references them and they are older than the grace; what is left listed again (a new object), the old one removed.
kj::Promise<std::string> controlSweepCleanup(ControlContext& context) {
  auto listing = co_await context.storage.list(CLEANUP_PREFIX);
  const auto now = realClockUnixMs();
  std::vector<std::string> due;
  for (const auto& object : listing.objects) {
    const auto name = object.key.substr(std::string_view(CLEANUP_PREFIX).size());
    if (name.size() < REVISION_DIGITS || !std::all_of(name.begin(), name.begin() + REVISION_DIGITS, [](unsigned char c) { return std::isdigit(c); }))
      continue;
    if (std::stoull(name.substr(0, REVISION_DIGITS)) <= now) due.push_back(object.key);
  }
  if (due.empty()) co_return std::string();
  auto remaining = co_await definitionsOf(context.state, [](const std::string&) { return true; });
  auto refs = referencesOf(remaining.versions, remaining.complete);
  ObjectCleanup total;
  for (const auto& key : due) {
    auto response = co_await context.storage.get(key);
    if (response.status == 404) continue;
    requireStorage(response.status == 200, "object store read failed: HTTP " + std::to_string(response.status));
    std::set<std::string> candidates;
    try {
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<Json>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.bytes.data(), response.bytes.size()), json);
      auto keys = field(json, "keys");
      if (keys.isArray()) for (auto entry : keys.getArray()) {
        if (!entry.isString()) continue;
        std::string candidate = entry.getString().cStr();
        if (candidate.starts_with("definitions/") || candidate.starts_with("packages/") || candidate.starts_with("bundles/") ||
            candidate.starts_with("modules/")) candidates.insert(std::move(candidate));
      }
    } catch (const kj::Exception&) {}
    ObjectCleanup cleanup;
    co_await removeUnreferenced(context.storage, std::move(candidates), refs, context.cleanupGraceMs, cleanup);
    co_await deferCleanup(context.storage, cleanup.deferredKeys, context.cleanupGraceMs);
    if (!cleanup.failed) co_await context.storage.remove(key);
    total.deleted += cleanup.deleted;
    total.deferred += cleanup.deferred;
    total.failed += cleanup.failed;
  }
  co_return "{\"due\":" + std::to_string(due.size()) + ",\"objects\":" + cleanupJson(total) + "}";
}

int runDeploymentTool(int argc, char** argv) {
  KJ_REQUIRE(argc > 0, "usage: coworkerd-apiserver tool <init|apply|upsert|remove|show|compile> [--storage-endpoint URL] "
      "[--storage-bucket B] [--tenant T] [--manifest PATH] [--worker ID]... [--no-trial | --trial-server URL] "
      "[--trial-timeout-ms N] [--trial-address-map ADVERTISED=DIALED]... [--membership objectStore [--membership-ttl-seconds N] "
      "[--membership-grace-ms N]] [--token TOKEN], or coworkerd-apiserver tool secret <put|delete|list> ...");
  const std::string operation = argv[0];
  // Its own parser: no argument of a secret command is ever echoed.
  if (operation == "secret") return runSecretTool(argc - 1, argv + 1);
  ToolOptions options;
  bool tenantGiven = false;
  std::optional<uint32_t> maxRetries;
  auto arguments = storeArguments();
  auto& objectConfig = arguments.config;
  if (auto value = std::getenv("COWORKERD_MANAGEMENT_TOKEN")) options.token = value;
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
    // The address of the store 0.3 kept the members in: accepted, unused (the members are on the object store).
    if (flag == "--endpoint") continue;
    else if (flag == "--membership") {
      KJ_REQUIRE(std::string(argv[index + 1]) == "objectStore", "--membership takes objectStore (the members are on the object store)",
          argv[index + 1]);
    }
    else if (flag == "--membership-ttl-seconds")
      options.memberTtlSeconds = static_cast<uint32_t>(number(argv[index + 1], 86400, "membership-ttl-seconds"));
    else if (flag == "--membership-grace-ms")
      options.memberGraceMs = static_cast<uint32_t>(number(argv[index + 1], 3600000, "membership-grace-ms"));
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
    else if (flag == "--secrets-key-file") arguments.keyFile = argv[index + 1];
    else if (flag == "--object-directory") {
      objectConfig.backend = "directory";
      objectConfig.directory = argv[index + 1];
    }
    else if (flag == "--pause-before-flip-ms") options.pauseMs = number(argv[index + 1], 60000, "pause-before-flip-ms");
    else if (flag == "--max-retries") maxRetries = number(argv[index + 1], 100, "max-retries");
    // The transaction size of 0.3's deployment state: a tenant's change is one object now, whatever its size. Accepted, unused.
    else if (flag == "--max-txn-ops") number(argv[index + 1], 100000, "max-txn-ops");
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
  KJ_REQUIRE(!objectConfig.endpoint.empty() || objectConfig.backend == "directory",
      "the desired state is in the object store: --storage-endpoint URL (or COWORKERD_STORAGE_ENDPOINT)");
  auto io = kj::setupAsyncIo();
  auto headers = kj::HttpHeaderTable::Builder().build();
  auto& timer = io.provider->getTimer();
  ObjectStore storage(objectConfig, io.provider->getNetwork(), timer);
  ManifestStore state(storage, timer, loadKey(arguments.keyFile), std::cerr);
  options.memberStore = &storage;
  if (operation == "init") {
    // A new cluster's bucket: its marker, and empty indexes so that the nodes' first reads find their hints.
    const bool present = storage.get(CLUSTER_FORMAT_KEY).wait(io.waitScope).status == 200;
    state.load(true, true).wait(io.waitScope);
    std::string written;
    auto empty = [&](const char* prefix, const std::string& bytes) {
      if (readLatestRevision(storage, prefix, indexHintKey(prefix), 0, true).wait(io.waitScope).revision) return;
      if (storage.create(revisionKey(prefix, 1), bytes).wait(io.waitScope).created) {
        publishHint(storage, prefix, indexHintKey(prefix), 1).wait(io.waitScope);
        written += std::string(written.empty() ? "" : ",") + quote(prefix);
      }
    };
    // Each revision names its writer (this run of the tool): a create whose answer is lost is decided by reading it back.
    TenantIndex index;
    index.revision = 1;
    index.writer = newWriter(state.writerInstance());
    index.writtenAtUnixMs = index.writer.writtenAtUnixMs;
    empty(TENANT_INDEX_PREFIX, tenantIndexJson(index));
    CredentialIndex credentials;
    credentials.revision = 1;
    credentials.writer = newWriter(state.writerInstance());
    credentials.writtenAtUnixMs = credentials.writer.writtenAtUnixMs;
    empty(CREDENTIAL_INDEX_PREFIX, credentialIndexJson(credentials));
    std::cout << "{\"operation\":\"init\",\"bucket\":" << quote(objectConfig.bucket) << ",\"format\":\"" << (present ? "present" : "created")
              << "\",\"emptyIndexes\":[" << written << "],\"instance\":" << quote(state.writerInstance()) << ",\"client\":"
              << storage.stats().json() << "}" << std::endl;
    return 0;
  }
  if (operation == "apply" || operation == "upsert") {
    KJ_REQUIRE(!options.manifest.empty(), "apply and upsert require --manifest");
    KJ_REQUIRE(options.workers.empty(), "--worker is only for remove");
    // apply keeps the original whole-state contract: a conflict fails without retry.
    options.maxRetries = maxRetries.value_or(operation == "apply" ? 0 : 8);
    // Only init makes a bucket a cluster's: a write to one without the marker is refused, naming the bucket.
    state.load(false, false).wait(io.waitScope);
    TrialContext trials{io.provider->getNetwork(), *headers, options.token};
    write(operation == "apply" ? Operation::APPLY : Operation::UPSERT, timer, state, &storage, options, trials,
        std::cout).wait(io.waitScope);
  } else if (operation == "remove") {
    KJ_REQUIRE(!options.workers.empty(), "remove requires --worker");
    options.maxRetries = maxRetries.value_or(8);
    state.load(false, false).wait(io.waitScope);
    TrialContext trials{io.provider->getNetwork(), *headers, options.token};
    write(Operation::REMOVE, timer, state, nullptr, options, trials, std::cout).wait(io.waitScope);
  } else if (operation == "show") {
    // The whole desired state, or with --tenant the tenant's (its Workers, and the routes they own).
    state.load(false, false).wait(io.waitScope);
    std::set<std::string> hashes;
    for (const auto& [tenant, manifest] : state.tenants())
      if (!tenantGiven || tenant == options.tenant) hashes.merge(namedDefinitions(manifest));
    state.fetchDefinitions(hashes).wait(io.waitScope);
    // Every tenant's keys at once, as a node reads them.
    ManifestView view;
    for (const auto& [tenant, manifest] : state.tenants()) view.emplace(tenant, &manifest);
    std::vector<DeploymentState::Change> changes;
    SecretOpener open = [](const std::string&, const std::string&, const std::string&, const ManifestSecret& sealed) {
      return sealedLength(sealed) ? std::optional<std::string>(std::string()) : std::nullopt;
    };
    for (const auto& [tenant, manifest] : view)
      for (auto& [key, value] : tenantStateKeys(*manifest, state.definitions(), open)) changes.push_back({key, value});
    for (auto& [key, value] : routeStateKeys(view)) changes.push_back({key, value});
    changes.push_back({"generation", generationValue(view)});
    DeploymentState current;
    current.apply(changes, true, state.index().revision);
    std::string active = "{";
    size_t index = 0;
    for (const auto& [id, worker] : current.workers()) {
      if (!worker.valid || (tenantGiven && tenantOf(id) != options.tenant)) continue;
      if (index++) active += ',';
      active += quote(id) + ':' + activePointerJson(worker.version->versionId, worker.version->definitionHash);
    }
    active += '}';
    std::vector<RouteDefinition> routes;
    for (const auto& [key, route] : current.routes())
      if (route.effective && (!tenantGiven || tenantOf(route.value.workerId) == options.tenant)) routes.push_back(route.value);
    std::cout << "{\"generation\":" << current.generation() << ",\"revision\":\"" << state.index().revision
              << "\",\"activeVersions\":" << active << ",\"routes\":" << routesJson(routes)
              << ',' << current.statusFields(STATUS_PROBLEM_LIMIT) << "}" << std::endl;
  } else KJ_FAIL_REQUIRE("unknown deployment operation", operation.c_str());
  return 0;
}
}  // namespace coworkerd

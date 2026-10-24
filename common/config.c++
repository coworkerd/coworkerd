#include "config.h"
#include "coworkerd.capnp.h"
#include "etcd.h"
#include "support.h"
#include "tenancy.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <optional>
#include <tuple>

namespace coworkerd {
namespace rpc = v1::rpc;
namespace fs = std::filesystem;

void ProcessPoolConfig::validate() const {
  KJ_REQUIRE(count > 0 && count <= 256 && inFlightLimit > 0, "invalid process pool capacity");
  KJ_REQUIRE(quarantineProcesses <= 256, "invalid quarantine process capacity");
  // One spare port beyond the slots: a rotation's next generation listens there while the
  // previous one drains on its own port.
  KJ_REQUIRE(basePort > 0 && uint64_t(basePort) + count + quarantineProcesses + 1 <= 65536, "invalid process port range");
  KJ_REQUIRE(probeIntervalMs > 0 && probeOverdueMs > probeIntervalMs && stuckTimeoutMs > probeOverdueMs,
      "probe interval < overdue threshold < stuck timeout is required");
  KJ_REQUIRE(restartBackoffMs > 0 && restartMaxMs >= restartBackoffMs, "invalid restart backoff");
  KJ_REQUIRE(idleDestroyIntervalMs > 0, "idleDestroyIntervalMs must be positive");
  KJ_REQUIRE(idleEvictMs == 0 || idleEvictMs > heatIdleMs, "idleEvictMs must exceed placement.idleMs");
  KJ_REQUIRE(rotationDrainTimeoutMs > 0 && rotationTermGraceMs > 0 && rotationRetryMs > 0, "rotation periods must be positive");
  KJ_REQUIRE(sandboxMode == "off" || sandboxMode == "launcher" || sandboxMode == "auto",
      "sandbox.mode must be off, launcher or auto", sandboxMode.c_str());
  KJ_REQUIRE(syscallsMode == "off" || syscallsMode == "observe" || syscallsMode == "enforce",
      "sandbox.syscalls.mode must be off, observe or enforce", syscallsMode.c_str());
  KJ_REQUIRE(steadySyscallsMode == "off" || steadySyscallsMode == "observe" || steadySyscallsMode == "enforce",
      "sandbox.syscalls.steady.mode must be off, observe or enforce", steadySyscallsMode.c_str());
}

// A management token: 16 to 4096 printable characters without spaces. Never echoed.
void ServerConfig::checkToken(const std::string& token, const char* origin) {
  KJ_REQUIRE(token.size() >= 16 && token.size() <= 4096 &&
      std::all_of(token.begin(), token.end(), [](unsigned char c) { return c > 0x20 && c < 0x7f; }),
      "the management token must be 16 to 4096 printable characters without spaces", origin);
}

// {"accessKey": "...", "secretKey": "..."}: the Servers' read identity of the object store.
// Errors name the file and the rule, never its content.
std::pair<std::string, std::string> ServerConfig::readCredentials(const fs::path& path) {
  auto text = readFile(path);
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<capnp::JsonValue>();
  try {
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), json);
  } catch (const kj::Exception&) {
    KJ_FAIL_REQUIRE("the storage credentials file is not JSON", path.string().c_str());
  }
  KJ_REQUIRE(json.isObject(), "the storage credentials file must be {\"accessKey\":...,\"secretKey\":...}", path.string().c_str());
  std::string access, secret;
  for (auto field : json.getObject()) {
    const std::string name = field.getName().cStr();
    auto value = field.getValue();
    KJ_REQUIRE((name == "accessKey" || name == "secretKey") && value.isString(),
        "the storage credentials file holds accessKey and secretKey strings only", path.string().c_str());
    (name == "accessKey" ? access : secret) = value.getString().cStr();
  }
  KJ_REQUIRE(!access.empty() && !secret.empty(), "the storage credentials file needs accessKey and secretKey", path.string().c_str());
  return {access, secret};
}

ServerConfig ServerConfig::load(const fs::path& path) {
  if (path.empty()) return {};
  auto text = readFile(path);
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<rpc::ServerConfig>();
  capnp::JsonCodec json;
  json.setRejectUnknownFields(true);
  json.decode(kj::arrayPtr(text.data(), text.size()), root);

  ServerConfig result;
  result.serverId = root.getNodeId().cStr();
  result.serverIdGiven = !result.serverId.empty();
  if (result.serverId.empty()) result.serverId = "local";
  KJ_REQUIRE(safeName(result.serverId), "invalid nodeId");
  result.peerListenAddress = root.getPeerListenAddress().cStr();
  result.initialPlacementHops = root.getInitialPlacementHops();
  result.ingressPlacement = root.getIngressPlacement();
  result.memoryWatermarkEnabled = root.getMemoryWatermarkEnabled();
  result.memoryBudgetBytes = root.getMemoryBudgetBytes();
  result.disableAlreadyPlacedForTest = root.getDisableAlreadyPlacedForTest();
  result.peerTimeoutMs = root.getPeerTimeoutMs();
  result.clockOffsetMsForTest = root.getClockOffsetMsForTest();
  auto placement = root.getPlacement();
  result.rootLimit = placement.getRootLimit();
  result.nestedLimit = placement.getNestedLimit();
  result.idleMs = placement.getIdleMs();
  result.homeProbability = placement.getHomeProbability();
  result.avoidMaxMs = placement.getAvoidMaxMs();
  result.rejectionAvoidMs = placement.getRejectionAvoidMs();
  result.connectBackoffMs = placement.getConnectBackoffMs();
  result.loadBandForTest = placement.getLoadBandForTest().cStr();
  result.equalLimitsForTest = placement.getEqualLimitsForTest();
  KJ_REQUIRE(result.rootLimit > 0 &&
      (result.nestedLimit > result.rootLimit ||
       (result.equalLimitsForTest && result.nestedLimit == result.rootLimit)),
      "nestedLimit must exceed rootLimit (equalLimitsForTest is only for the control)");
  KJ_REQUIRE(result.idleMs > 0 && result.avoidMaxMs > 0 && result.connectBackoffMs > 0,
      "placement durations must be positive");
  KJ_REQUIRE(std::isfinite(result.homeProbability) && result.homeProbability >= 0 && result.homeProbability <= 1,
      "homeProbability must be in [0, 1]");
  KJ_REQUIRE(result.loadBandForTest == "green" || result.loadBandForTest == "yellow" || result.loadBandForTest == "red",
      "invalid loadBandForTest");
  auto pool = root.getProcessPool();
  result.processPool = {pool.getCount(), pool.getInFlightLimit(), pool.getBasePort(),
    pool.getProbeIntervalMs(), pool.getProbeOverdueMs(), pool.getStuckTimeoutMs(),
    pool.getRestartBackoffMs(), pool.getRestartMaxMs(), pool.getWatchdog()};
  result.processPool.cpuLimitMs = root.getCpuLimitMs();
  result.processPool.memoryLimitMiB = root.getMemoryLimitMiB();
  // A limit of 0 leaves the isolates of this node without one, and with it the culprit a stuck or exhausted process
  // names, the suspect it makes and the quarantine processes it would run in: a configuration file alone cannot turn a
  // limit off, as a tenant's default limits and a Worker's own cannot be 0 either (common/tenancy.c++). The
  // environment keeps its meaning -- a measurement, or an older case replayed with a limit off, asks for it there
  // (COWORKERD_CPU_LIMIT_MS, COWORKERD_MEMORY_LIMIT_MIB) -- and the node says so at startup.
  KJ_REQUIRE(result.processPool.cpuLimitMs > 0 || std::getenv("COWORKERD_CPU_LIMIT_MS") != nullptr,
      "cpuLimitMs must be a positive number of milliseconds; 0 is refused (it would turn culprit attribution off; "
      "COWORKERD_CPU_LIMIT_MS=0 asks for it deliberately)");
  KJ_REQUIRE(result.processPool.memoryLimitMiB > 0 || std::getenv("COWORKERD_MEMORY_LIMIT_MIB") != nullptr,
      "memoryLimitMiB must be a positive number of MiB; 0 is refused (it would turn culprit attribution off; "
      "COWORKERD_MEMORY_LIMIT_MIB=0 asks for it deliberately)");
  result.processPool.quarantineProcesses = root.getQuarantineProcesses();
  result.processPool.idleEvictMs = root.getIdleEvictMs();
  result.processPool.idleDestroyIntervalMs = root.getIdleDestroyIntervalMs();
  result.processPool.heatIdleMs = result.idleMs;
  result.suspectBaseMs = root.getSuspectBaseMs();
  result.suspectMaxMs = root.getSuspectMaxMs();
  result.drainTimeoutMs = root.getDrainTimeoutMs();
  result.loadCheck = root.getLoadCheck();
  auto rotation = root.getRotation();
  result.processPool.rotationDrainTimeoutMs = rotation.getDrainTimeoutMs();
  result.processPool.rotationTermGraceMs = rotation.getTermGraceMs();
  result.processPool.rotationBinaryCheckMs = rotation.getBinaryCheckMs();
  result.processPool.rotationMaxAgeMs = rotation.getMaxAgeMs();
  result.processPool.rotationMaxRequests = rotation.getMaxRequests();
  result.processPool.rotationResidentBytes = rotation.getResidentBytes();
  result.processPool.rotationRetryMs = rotation.getRetryMs();
  auto sandbox = root.getSandbox();
  result.processPool.sandboxMode = sandbox.getMode().cStr();
  result.processPool.launcherPath = sandbox.getLauncherPath().cStr();
  result.processPool.syscallsMode = sandbox.getSyscalls().getMode().cStr();
  result.processPool.allowlistPath = sandbox.getSyscalls().getAllowlistPath().cStr();
  result.processPool.steadySyscallsMode = sandbox.getSyscalls().getSteady().getMode().cStr();
  result.processPool.steadyAllowlistPath = sandbox.getSyscalls().getSteady().getAllowlistPath().cStr();
  result.processPool.validate();
  auto membership = root.getMembership();
  result.membership = {
    membership.getBackend().cStr(), membership.getEndpoint().cStr(),
    membership.getAdvertisedAddress().cStr(), membership.getTtlSeconds(),
    membership.getRequestTimeoutMs(), membership.getStartupTimeoutMs(), membership.getRetryMs(),
    membership.getHeartbeatIntervalMs(), membership.getListIntervalMs(), membership.getLivenessGraceMs(),
    membership.getClockSkewToleranceMs(), membership.getCreateSettleMs(),
  };
  auto desired = root.getDesiredState();
  result.desiredState = {
    desired.getBackend().cStr(), desired.getEndpoint().cStr(),
    desired.getCodeDirectory().cStr(), desired.getRequestTimeoutMs(),
    desired.getStartupTimeoutMs(), desired.getRetryMs(),
    desired.getVersionResolutionForTest().cStr(), desired.getDeploymentApplyDelayMsForTest(),
  };
  auto object = desired.getObjectStore();
  result.desiredState.objectStore = {object.getBackend().cStr(), object.getDirectory().cStr(),
    object.getEndpoint().cStr(), object.getBucket().cStr(), object.getRegion().cStr(),
    object.getAccessKey().cStr(), object.getSecretKey().cStr(), object.getTimeoutMs(),
    object.getConnectTimeoutMs(), object.getReadTimeoutMs(), object.getCreateTimeoutMs()};
  auto cache = desired.getCache();
  result.desiredState.cache = {cache.getDirectory().cStr(), cache.getMaxBytes(), cache.getPrefetchConcurrency()};
  result.desiredState.pollIntervalMs = desired.getPollIntervalMs();
  KJ_REQUIRE(result.initialPlacementHops > 0, "initialPlacementHops must be positive");
  KJ_REQUIRE(result.peerTimeoutMs > 0, "peerTimeoutMs must be positive");
  KJ_REQUIRE(
      result.clockOffsetMsForTest >= -86'400'000 &&
          result.clockOffsetMsForTest <= 86'400'000,
      "clockOffsetMsForTest is outside the supported one-day range");

  for (auto peer : root.getPeers()) {
    auto peerId = std::string(peer.getNodeId().cStr());
    auto address = std::string(peer.getAddress().cStr());
    KJ_REQUIRE(safeName(peerId), "invalid peer nodeId", peerId.c_str());
    KJ_REQUIRE(!address.empty(), "peer address is required", peerId.c_str());
    KJ_REQUIRE(result.peers.emplace(peerId, address).second,
        "duplicate peer nodeId", peerId.c_str());
  }
  for (auto placement : root.getPlacements()) {
    auto workerId = std::string(placement.getWorkerId().cStr());
    auto serverId = std::string(placement.getNodeId().cStr());
    // A Worker's full name: <tenant>/<worker>.
    KJ_REQUIRE(workerFullName(workerId), "invalid placement workerId (<tenant>/<worker>)", workerId.c_str());
    KJ_REQUIRE(safeName(serverId), "invalid placement nodeId", serverId.c_str());
    KJ_REQUIRE(result.placements.emplace(workerId, serverId).second,
        "duplicate placement workerId", workerId.c_str());
  }

  // The paths and addresses of `serve`, and the shortcuts. A shortcut and the nested field it
  // fills may both appear only with the same value. Whether a field appears is read from the file
  // itself: a reader returns a field's default, set or not.
  capnp::MallocMessageBuilder rawMessage;
  auto raw = rawMessage.initRoot<capnp::JsonValue>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), raw);
  auto given = [&](std::initializer_list<const char*> path) -> std::optional<std::string> {
    capnp::JsonValue::Reader value = raw.asReader();
    for (auto name : path) {
      if (!value.isObject()) return std::nullopt;
      bool found = false;
      for (auto field : value.getObject()) {
        if (field.getName() == name) { value = field.getValue(); found = true; break; }
      }
      if (!found) return std::nullopt;
    }
    return value.isString() ? std::string(value.getString().cStr()) : std::string();
  };
  result.stackDirectory = root.getStackDirectory().cStr();
  result.workerdPath = root.getWorkerdPath().cStr();
  result.runtimePath = root.getRuntimePath().cStr();
  result.listenAddress = root.getListenAddress().cStr();
  // etcd's backend of the desired state was removed in 0.4: the desired state is the object store's.
  KJ_REQUIRE(given({"desiredState", "backend"}) != std::optional<std::string>("etcd"),
      "desiredState.backend must be file or objectStore: etcd's backend was removed in 0.4 (the desired state is the object "
      "store's; desiredState.backend objectStore with the storage block)");
  if (given({"etcdEndpoint"})) {
    const std::string endpoint = root.getEtcdEndpoint().cStr();
    KJ_REQUIRE(!endpoint.empty(), "etcdEndpoint is empty");
    // etcd keeps the suspects and the usage rows, and the membership unless it lives on the object store
    // (membership.backend objectStore, written out): a configuration that does not say so keeps it in etcd, as before.
    const bool objectStoreMembership = given({"membership", "backend"}) == std::optional<std::string>("objectStore");
    auto backend = given({"membership", "backend"}), nested = given({"membership", "endpoint"});
    KJ_REQUIRE(!backend || *backend == "etcd" || objectStoreMembership, "etcdEndpoint sets the backend to etcd", "membership");
    KJ_REQUIRE(!nested || *nested == endpoint, "etcdEndpoint and the endpoint differ", "membership");
    result.etcdEndpoint = endpoint;
    if (!objectStoreMembership) result.membership.backend = "etcd";
    result.membership.endpoint = endpoint;
  }
  if (given({"advertisedAddress"})) {
    const std::string address = root.getAdvertisedAddress().cStr();
    auto nested = given({"membership", "advertisedAddress"});
    KJ_REQUIRE(!nested || *nested == address, "advertisedAddress and membership.advertisedAddress differ");
    result.membership.advertisedAddress = address;
  }
  if (given({"storage"})) {
    auto storage = root.getStorage();
    KJ_REQUIRE(!given({"desiredState", "objectStore"}), "storage and desiredState.objectStore cannot both name the object store");
    const bool cacheGiven = given({"storage", "cacheDirectory"}) || given({"storage", "cacheMaxBytes"});
    KJ_REQUIRE(!cacheGiven || !given({"desiredState", "cache"}), "storage.cache* and desiredState.cache cannot both be given");
    auto& store = result.desiredState.objectStore;
    store.backend = "s3";
    store.endpoint = storage.getEndpoint().cStr();
    store.bucket = storage.getBucket().cStr();
    store.region = storage.getRegion().cStr();
    store.timeoutMs = storage.getTimeoutMs();
    store.connectTimeoutMs = storage.getConnectTimeoutMs();
    store.readTimeoutMs = storage.getReadTimeoutMs();
    store.createTimeoutMs = storage.getCreateTimeoutMs();
    KJ_REQUIRE(!store.endpoint.empty(), "storage.endpoint is required");
    if (given({"storage", "credentialsFile"})) {
      result.storageCredentialsFile = storage.getCredentialsFile().cStr();
      std::tie(store.accessKey, store.secretKey) = readCredentials(result.storageCredentialsFile);
    }
    if (!given({"desiredState", "cache"})) {
      result.desiredState.cache.maxBytes = storage.getCacheMaxBytes();
      if (given({"storage", "cacheDirectory"})) result.desiredState.cache.directory = storage.getCacheDirectory().cStr();
      else result.cacheFollowsStack = true;
    }
    // The object store holds the desired state unless the configuration says file.
    if (!given({"desiredState", "backend"})) result.desiredState.backend = "objectStore";
  }
  // The cluster's secrets key (secrets.keyFile; the environment's COWORKERD_SECRETS_KEY_FILE over it).
  if (given({"secrets", "keyFile"})) result.secretsKeyFile = root.getSecrets().getKeyFile().cStr();
  if (const char* file = std::getenv("COWORKERD_SECRETS_KEY_FILE"); file != nullptr && *file) result.secretsKeyFile = file;
  result.desiredState.secretsKeyFile = result.secretsKeyFile;
  // The desired state's backend and what objectStore needs: the bucket (storage, or desiredState.objectStore) and the
  // secrets key that opens the secrets' values.
  {
    const auto& d = result.desiredState;
    KJ_REQUIRE(d.backend == "file" || d.backend == "objectStore", "desiredState.backend must be file or objectStore", d.backend.c_str());
    if (d.backend == "objectStore") {
      KJ_REQUIRE(d.objectStore.backend == "s3" && !d.objectStore.endpoint.empty(),
          "desiredState.backend objectStore reads the desired state from the object store: it needs the storage block "
          "(or desiredState.objectStore with an endpoint)");
      KJ_REQUIRE(!result.secretsKeyFile.empty(), "desiredState.backend objectStore needs the cluster's secrets key, which "
          "opens the secrets' values: set secrets.keyFile (or COWORKERD_SECRETS_KEY_FILE)");
      KJ_REQUIRE(d.pollIntervalMs >= 50 && d.pollIntervalMs <= 3600000, "desiredState.pollIntervalMs must be 50 to 3600000");
    }
    const auto& o = d.objectStore;
    KJ_REQUIRE(o.connectTimeoutMs >= 100 && o.connectTimeoutMs <= 60000, "storage.connectTimeoutMs must be 100 to 60000");
    KJ_REQUIRE(o.readTimeoutMs >= 100 && o.readTimeoutMs <= 600000, "storage.readTimeoutMs must be 100 to 600000");
    KJ_REQUIRE(o.createTimeoutMs >= o.readTimeoutMs && o.createTimeoutMs <= 600000,
        "storage.createTimeoutMs must be at least storage.readTimeoutMs and at most 600000");
  }
  // The membership's backend, and the object store's settings it needs (the schema's MembershipConfig).
  {
    const auto& m = result.membership;
    KJ_REQUIRE(m.backend == "file" || m.backend == "etcd" || m.backend == "objectStore",
        "membership.backend must be file, etcd or objectStore", m.backend.c_str());
    if (m.backend == "objectStore") {
      KJ_REQUIRE(given({"storage"}).has_value() && result.desiredState.objectStore.backend == "s3",
          "membership.backend objectStore keeps the members in the object store: it needs the storage block");
      KJ_REQUIRE(m.ttlSeconds >= 2 && m.ttlSeconds <= 86400, "membership.ttlSeconds must be 2 to 86400");
      KJ_REQUIRE(m.heartbeatIntervalMs > 0 && uint64_t(m.heartbeatIntervalMs) * 2 < uint64_t(m.ttlSeconds) * 1000,
          "membership.heartbeatIntervalMs must be positive and less than half of ttlSeconds");
      KJ_REQUIRE(m.livenessGraceMs >= m.heartbeatIntervalMs, "membership.livenessGraceMs must be at least heartbeatIntervalMs");
      KJ_REQUIRE(m.listIntervalMs > 0, "membership.listIntervalMs must be positive");
      KJ_REQUIRE(m.clockSkewToleranceMs > 0, "membership.clockSkewToleranceMs must be positive");
      KJ_REQUIRE(m.createSettleMs <= 60000, "membership.createSettleMs must be at most 60000");
      KJ_REQUIRE(m.startupTimeoutMs > 0 && m.retryMs > 0, "membership timeouts must be positive");
    }
  }
  if (given({"management", "token"})) {
    result.managementToken = root.getManagement().getToken().cStr();
    checkToken(result.managementToken, "management.token");
  }
  auto control = root.getControl();
  result.controlListenAddress = control.getListenAddress().cStr();
  result.controlTemporaryDirectory = control.getTemporaryDirectory().cStr();
  result.controlMaxUploadBytes = control.getMaxUploadBytes();
  result.controlServerTimeoutMs = control.getServerTimeoutMs();
  result.controlPathRouting = control.getPathRouting();
  result.controlPollIntervalMs = control.getPollIntervalMs();
  KJ_REQUIRE(result.controlPollIntervalMs >= 100 && result.controlPollIntervalMs <= 3600000, "control.pollIntervalMs must be 100 to 3600000");
  if (given({"control", "adminCredential"})) {
    result.controlAdminCredential = control.getAdminCredential().cStr();
    checkToken(result.controlAdminCredential, "control.adminCredential");
  }
  KJ_REQUIRE(result.controlMaxUploadBytes >= 1024 * 1024 && result.controlServerTimeoutMs > 0,
      "control.maxUploadBytes must be at least 1 MiB and control.serverTimeoutMs positive");
  auto logs = root.getLogs();
  result.logs = {logs.getTenantBufferBytes(), logs.getTotalBufferBytes(), logs.getLinesPerSecond(), logs.getMaxLineBytes(),
      logs.getWorkerdLogMaxBytes(), logs.getWorkerdLogFiles()};
  auto usage = root.getUsage();
  result.usage = {usage.getFlushIntervalMs(), usage.getRetentionHours(), usage.getSampleIntervalMs()};
  KJ_REQUIRE(result.usage.flushIntervalMs >= 100 && result.usage.sampleIntervalMs >= 100,
      "usage.flushIntervalMs and usage.sampleIntervalMs must be at least 100");
  // Outbound requests. The rules' syntax is checked by the node's egress service when it starts.
  auto egress = root.getEgress();
  auto& e = result.egress;
  e.mode = egress.getMode().cStr();
  e.defaultPolicy = egress.getDefaultPolicy().cStr();
  e.publicPorts.clear();
  for (auto port : egress.getPublicPorts()) {
    KJ_REQUIRE(port > 0, "egress.publicPorts holds ports 1 to 65535");
    e.publicPorts.push_back(port);
  }
  e.timeoutMs = egress.getTimeoutMs();
  e.concurrency = egress.getConcurrency();
  e.requestsPerSecond = egress.getRequestsPerSecond();
  e.dnsCacheMs = egress.getDnsCacheMs();
  e.caFile = egress.getCaFile().cStr();
  e.systemTrustStore = egress.getSystemTrustStore();
  for (auto address : egress.getClusterAddresses()) e.clusterAddresses.push_back(address.cStr());
  for (auto tenant : egress.getTenants()) {
    const std::string name = tenant.getTenant().cStr();
    KJ_REQUIRE(tenantName(name), "egress.tenants: not a tenant's name ([a-z0-9][a-z0-9-]{0,62})", name.c_str());
    EgressTenantPolicy policy;
    for (auto rule : tenant.getAllow()) policy.allow.push_back(rule.cStr());
    for (auto rule : tenant.getDeny()) policy.deny.push_back(rule.cStr());
    for (auto rule : tenant.getAllowInternal()) policy.allowInternal.push_back(rule.cStr());
    policy.concurrency = tenant.getConcurrency();
    policy.requestsPerSecond = tenant.getRequestsPerSecond();
    KJ_REQUIRE(e.tenants.emplace(name, std::move(policy)).second, "egress.tenants: duplicate tenant", name.c_str());
  }
  KJ_REQUIRE(e.mode == "on" || e.mode == "off", "egress.mode must be on or off", e.mode.c_str());
  KJ_REQUIRE(e.defaultPolicy == "public" || e.defaultPolicy == "deny", "egress.defaultPolicy must be public or deny",
      e.defaultPolicy.c_str());
  KJ_REQUIRE(e.timeoutMs > 0 && e.concurrency > 0 && e.requestsPerSecond > 0,
      "egress.timeoutMs, egress.concurrency and egress.requestsPerSecond must be positive");
  // The peer listener's authentication and etcd's credentials.
  result.peerAuth = {root.getPeerAuth().getKeyFile().cStr(), root.getPeerAuth().getWindowMs()};
  KJ_REQUIRE(result.peerAuth.windowMs >= 1000 && result.peerAuth.windowMs <= 600000, "peerAuth.windowMs must be 1000 to 600000");
  result.etcdCredentialsFile = root.getEtcdCredentialsFile().cStr();
  // The process's etcd identity (etcd.h): read now, so that a bad file stops the daemon at its start.
  if (!result.etcdCredentialsFile.empty()) useEtcdCredentialsFile(result.etcdCredentialsFile);
  return result;
}

}  // namespace coworkerd

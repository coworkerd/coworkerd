#include "config.h"
#include "coworkerd.capnp.h"
#include "json.h"
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

// A node stops confirming its Durable Object writes when its own lease ends, at the latest one heartbeat interval after
// the TTL (a heartbeat in flight). The others must wait longer than that before they take its shards: the interval, the
// expiry check (100 ms), the ownership watcher (50 ms) and the store's whole-second Date (1,000 ms). Nodes only: the
// control plane holds no lease (it reads the TTL and the grace to judge the members).
void checkNodeLeaseMargin(const MembershipConfig& m) {
  if (m.backend != "objectStore") return;
  if (uint64_t(m.livenessGraceMs) <= uint64_t(m.heartbeatIntervalMs) + LIVENESS_GRACE_MARGIN_MS) {
    const auto values = "membership.livenessGraceMs must be greater than membership.heartbeatIntervalMs + " +
        std::to_string(LIVENESS_GRACE_MARGIN_MS) + " ms: livenessGraceMs " + std::to_string(m.livenessGraceMs) +
        ", heartbeatIntervalMs " + std::to_string(m.heartbeatIntervalMs) + ", ttlSeconds " + std::to_string(m.ttlSeconds) +
        " (the smallest livenessGraceMs for this interval is " +
        std::to_string(uint64_t(m.heartbeatIntervalMs) + LIVENESS_GRACE_MARGIN_MS + 1) + ")";
    kj::throwFatalException(kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::heapString(values.c_str())));
  }
}

void DurableLogConfig::validate() const {
  KJ_REQUIRE(alarmDiscoveryIntervalMs >= 200, "durableObjects.alarmDiscoveryIntervalMs is at least 200");
  KJ_REQUIRE(alarmTakeoverConcurrency > 0 && alarmTakeoverConcurrency <= 4, "durableObjects.alarmTakeoverConcurrency is between 1 and 4");
  KJ_REQUIRE(versionDrainTimeoutMs > 0, "durableObjects.versionDrainTimeoutMs must be positive");
  KJ_REQUIRE(peers <= 8, "durableObjects.peers is at most 8");
  KJ_REQUIRE(confirmTimeoutMs >= 100 && confirmTimeoutMs <= 29000,
      "durableObjects.confirmTimeoutMs is between 100 and 29000 (workerd resets the object at 30 s)");
  KJ_REQUIRE(recoverTimeoutMs >= 1000, "durableObjects.recoverTimeoutMs is at least 1000");
  KJ_REQUIRE(segmentIntervalMs >= 100, "durableObjects.segmentIntervalMs is at least 100");
  KJ_REQUIRE(segmentBytes >= (1u << 16), "durableObjects.segmentBytes is at least 65536");
  KJ_REQUIRE(snapshotBytes >= (1u << 20), "durableObjects.snapshotBytes is at least 1048576");
  KJ_REQUIRE(snapshotPartBytes >= (1u << 16) && snapshotPartBytes <= (256u << 20),
      "durableObjects.snapshotPartBytes is between 65536 and 268435456");
  KJ_REQUIRE(peerBufferBytes >= (1u << 20), "durableObjects.peerBufferBytes is at least 1048576");
  KJ_REQUIRE(objectBytes >= (1u << 20), "durableObjects.objectBytes is at least 1048576");
  KJ_REQUIRE(checkpointBytes >= (1u << 16), "durableObjects.checkpointBytes is at least 65536");
  KJ_REQUIRE(sqlDatabaseSpillBytes >= (1ull << 16) && sqlDatabaseSpillBytes <= (1ull << 40),
      "durableObjects.sqlDatabaseSpillBytes is between 64 KiB and 1 TiB");
  KJ_REQUIRE(sqlProcessSpillBytes == 0 ||
      (sqlProcessSpillBytes >= (1ull << 16) && sqlProcessSpillBytes <= (4ull << 40)),
      "durableObjects.sqlProcessSpillBytes is zero or between 64 KiB and 4 TiB");
  KJ_REQUIRE(recoverParallelism > 0 && recoverParallelism <= 16,
      "durableObjects.recoverParallelism is between 1 and 16");
  KJ_REQUIRE(rebuildConcurrency > 0 && rebuildConcurrency <= 64,
      "durableObjects.rebuildConcurrency is between 1 and 64");
  KJ_REQUIRE(uploadRetryMs > 0 && uploadRetryMaxMs >= uploadRetryMs,
      "durableObjects.uploadRetryMaxMs is at least uploadRetryMs");
}

void ServerConfig::validatePlacementLimits() const {
  KJ_REQUIRE(rootLimit > 0, "placement.rootLimit must be positive");
  KJ_REQUIRE(nestedLimit > rootLimit, "placement.nestedLimit must exceed placement.rootLimit", rootLimit, nestedLimit);
  KJ_REQUIRE(tenantRootLimit <= rootLimit && tenantNestedLimit <= nestedLimit,
      "per-tenant placement limits must not exceed their effective node-wide limits",
      tenantRootLimit, rootLimit, tenantNestedLimit, nestedLimit);
}

void ServerConfig::resolvePlacementLimits() {
  const auto ordinarySlots = processPool.inFlightLimit * processPool.count;
  // A durable-only node has no ordinary capacity from which to derive placement defaults.
  if (ordinarySlots > 0) {
    KJ_REQUIRE(ordinarySlots > 1 || (rootLimitGiven && nestedLimitGiven),
        "one ordinary call slot needs explicit placement.rootLimit and placement.nestedLimit");
    if (!nestedLimitGiven) nestedLimit = ordinarySlots;
    if (!rootLimitGiven) rootLimit = std::min(ordinarySlots, nestedLimit) * 3 / 4;
  }
  validatePlacementLimits();
}

void ProcessPoolConfig::validate() const {
  KJ_REQUIRE((count > 0 || threadedDurableTestStorage) && count <= 256 && inFlightLimit > 0,
      "invalid process pool capacity");
  KJ_REQUIRE(ordinaryServer == "threaded",
      "processPool.ordinaryServer must be threaded; the ordinary upstream backend has been removed");
  KJ_REQUIRE(ordinaryServer != "threaded" || inFlightLimit <= 16384,
      "threaded process inFlightLimit must not exceed 16384");
  KJ_REQUIRE(tenantInFlightLimit <= 16384,
      "processPool.tenantInFlightLimit must be between 0 and 16384");
  KJ_REQUIRE(tenantAdmissionWaitMs <= 60000, "processPool.tenantAdmissionWaitMs is at most 60000");
  KJ_REQUIRE(threadedResidentLimit > 0 && threadedResidentLimit <= 1840,
      "processPool.threadedResidentLimit must be between 1 and 1840");
  KJ_REQUIRE(quarantineProcesses <= 256, "invalid quarantine process capacity");
  KJ_REQUIRE(durableProcesses <= 64, "durableObjects.processes is at most 64");
  // One spare port beyond the slots: a rotation's next generation listens there while the
  // previous one drains on its own port.
  KJ_REQUIRE(basePort > 0 && uint64_t(basePort) + count + quarantineProcesses + 1 <= 65536, "invalid process port range");
  KJ_REQUIRE(probeIntervalMs > 0 && stuckTimeoutMs > probeIntervalMs,
      "probe interval must be below the probe timeout");
  KJ_REQUIRE(restartBackoffMs > 0 && restartMaxMs >= restartBackoffMs, "invalid restart backoff");
  KJ_REQUIRE(idleEvictMs == 0 || idleEvictMs > heatIdleMs, "idleEvictMs must exceed placement.idleMs");
  KJ_REQUIRE(rotationDrainTimeoutMs > 0 && rotationTermGraceMs > 0 && rotationRetryMs > 0, "rotation periods must be positive");
  KJ_REQUIRE(sandboxMode == "off" || sandboxMode == "launcher" || sandboxMode == "auto",
      "sandbox.mode must be off, launcher or auto", sandboxMode.c_str());
  KJ_REQUIRE(protectionKeys == "auto" || protectionKeys == "off" || protectionKeys == "required",
      "sandbox.protectionKeys must be auto, off or required", protectionKeys.c_str());
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
  // The file as plain JSON first: whether a field appears is read from the file itself (a reader returns a field's
  // default, set or not), and what 0.4 removed is named before the schema's decoding refuses it as an unknown field.
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
  // 0.4 has no etcd: a configuration that still names what it had is refused, in one sentence.
  auto refuse = [](const std::string& what) {
    kj::throwFatalException(kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::str("0.4 has no etcd: ",
        what.c_str(), " (the members, the desired state, the suspects and the usage are on the object store: the storage block)")));
  };
  for (auto name : {"etcdEndpoint", "etcdCredentialsFile"})
    if (given({name})) refuse(std::string(name) + " is not a setting any more");
  for (auto name : {"membership", "desiredState"})
    if (given({name, "backend"}) == std::optional<std::string>("etcd")) refuse(std::string(name) + ".backend must be file or objectStore");
  KJ_REQUIRE(!given({"ingress", "webSocketsPerTenant"}),
      "ingress.webSocketsPerTenant was removed. Delete it to use the default node limit, "
      "or set ingress.webSocketsPerNode to a shared limit.");
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<rpc::ServerConfig>();
  capnp::JsonCodec json;
  json.setRejectUnknownFields(true);
  json.decode(kj::arrayPtr(text.data(), text.size()), root);

  ServerConfig result;
  result.serverId = root.getNodeId().cStr();
  result.serverIdGiven = !result.serverId.empty();
  if (result.serverId.empty()) result.serverId = "local";
  KJ_REQUIRE(nodeIdName(result.serverId),
      "invalid nodeId: letters, digits, '-', '_' and '.', at most 255, not . or .. (it names the node's object keys)");
  result.peerListenAddress = root.getPeerListenAddress().cStr();
  result.initialPlacementHops = root.getInitialPlacementHops();
  result.ingressPlacement = root.getIngressPlacement();
  result.memoryWatermarkEnabled = root.getMemoryWatermarkEnabled();
  result.memoryBudgetBytes = root.getMemoryBudgetBytes();
  result.hostCheck = {root.getHostCheck().getIntervalMs(), root.getHostCheck().getSampleMiB()};
  KJ_REQUIRE(result.hostCheck.intervalMs == 0 ||
      (result.hostCheck.intervalMs >= 1000 && result.hostCheck.intervalMs <= 86400000),
      "hostCheck.intervalMs must be 0 or between 1000 and 86400000");
  KJ_REQUIRE(result.hostCheck.sampleMiB >= 1 && result.hostCheck.sampleMiB <= 4096,
      "hostCheck.sampleMiB must be between 1 and 4096");
  result.disableAlreadyPlacedForTest = root.getDisableAlreadyPlacedForTest();
  result.peerTimeoutMs = root.getPeerTimeoutMs();
  result.clockOffsetMsForTest = root.getClockOffsetMsForTest();
  auto placement = root.getPlacement();
  result.rootLimit = placement.getRootLimit();
  result.nestedLimit = placement.getNestedLimit();
  result.rootLimitGiven = given({"placement", "rootLimit"}).has_value();
  result.nestedLimitGiven = given({"placement", "nestedLimit"}).has_value();
  result.tenantRootLimit = placement.getTenantRootLimit();
  result.tenantNestedLimit = placement.getTenantNestedLimit();
  result.tenantBorrowBelowPercent = placement.getTenantBorrowBelowPercent();
  result.idleMs = placement.getIdleMs();
  result.homeProbability = placement.getHomeProbability();
  result.avoidMaxMs = placement.getAvoidMaxMs();
  result.rejectionAvoidMs = placement.getRejectionAvoidMs();
  result.connectBackoffMs = placement.getConnectBackoffMs();
  result.loadBandForTest = placement.getLoadBandForTest().cStr();
  result.subcalls = placement.getSubcalls().cStr();
  KJ_REQUIRE(result.subcalls == "home" || result.subcalls == "local",
      "placement.subcalls is \"home\" or \"local\"", result.subcalls.c_str());
  KJ_REQUIRE(!result.rootLimitGiven || result.rootLimit > 0, "placement.rootLimit must be positive");
  KJ_REQUIRE(!result.nestedLimitGiven || result.nestedLimit > 1, "placement.nestedLimit must be at least 2");
  if (result.rootLimitGiven && result.nestedLimitGiven) result.validatePlacementLimits();
  KJ_REQUIRE((!result.rootLimitGiven || result.tenantRootLimit <= result.rootLimit) &&
      (!result.nestedLimitGiven || result.tenantNestedLimit <= result.nestedLimit),
      "per-tenant placement limits must not exceed their node-wide limits");
  KJ_REQUIRE(result.tenantBorrowBelowPercent <= 100, "placement.tenantBorrowBelowPercent must be in [0, 100]");
  KJ_REQUIRE(result.idleMs > 0 && result.avoidMaxMs > 0 && result.connectBackoffMs > 0,
      "placement durations must be positive");
  KJ_REQUIRE(std::isfinite(result.homeProbability) && result.homeProbability >= 0 && result.homeProbability <= 1,
      "homeProbability must be in [0, 1]");
  KJ_REQUIRE(result.loadBandForTest == "green" || result.loadBandForTest == "yellow" || result.loadBandForTest == "red",
      "invalid loadBandForTest");
  auto pool = root.getProcessPool();
  result.processPool = {pool.getCount(), pool.getInFlightLimit(), pool.getBasePort(),
    pool.getProbeIntervalMs(), pool.getStuckTimeoutMs(),
    pool.getRestartBackoffMs(), pool.getRestartMaxMs(), pool.getWatchdog()};
  result.processPool.inFlightLimitGiven = given({"processPool", "inFlightLimit"}).has_value();
  result.processPool.ordinaryServer = pool.getOrdinaryServer().cStr();
  result.processPool.threadedExecutable = pool.getThreadedExecutable().cStr();
  if (given({"processPool", "tenantInFlightLimit"})) result.processPool.tenantInFlightLimit = pool.getTenantInFlightLimit();
  result.processPool.tenantAdmissionWaitMs = pool.getTenantAdmissionWaitMs();
  result.processPool.threadedMemoryBytes = pool.getThreadedMemoryBytes();
  result.processPool.threadedResidentLimit = pool.getThreadedResidentLimit();
  result.processPool.threadedThreadIdleMs = pool.getThreadedThreadIdleMs();
  result.processPool.cpuLimitMs = root.getCpuLimitMs();
  if (given({"cpuLimitMs"})) result.processPool.durableCpuLimitMs = root.getCpuLimitMs();
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
  auto durable = root.getDurableObjects();
  result.processPool.durableProcesses = durable.getProcesses();
  result.durableSocketFrameBytesPerNode = durable.getSocketFrameBytesPerNode();
  result.durableLog.peers = durable.getPeers();
  result.durableLog.confirmTimeoutMs = durable.getConfirmTimeoutMs();
  result.durableLog.versionDrainTimeoutMs = durable.getVersionDrainTimeoutMs();
  result.durableLog.recoverTimeoutMs = durable.getRecoverTimeoutMs();
  result.durableLog.segmentIntervalMs = durable.getSegmentIntervalMs();
  result.durableLog.segmentBytes = durable.getSegmentBytes();
  result.durableLog.snapshotBytes = durable.getSnapshotBytes();
  result.durableLog.snapshotIntervalMs = durable.getSnapshotIntervalMs();
  result.durableLog.snapshotPartBytes = durable.getSnapshotPartBytes();
  result.durableLog.diskBytes = durable.getDiskBytes();
  result.durableLog.diskBytesDefault = !given({"durableObjects", "diskBytes"}).has_value();
  result.durableLog.peerBufferBytes = durable.getPeerBufferBytes();
  result.durableLog.objectBytes = durable.getObjectBytes();
  result.durableLog.checkpointBytes = durable.getCheckpointBytes();
  result.durableLog.sqlDatabaseSpillBytes = durable.getSqlDatabaseSpillBytes();
  result.durableLog.sqlProcessSpillBytes = durable.getSqlProcessSpillBytes();
  result.durableLog.idleCheckpointMs = durable.getIdleCheckpointMs();
  result.durableLog.uploadRetryMs = durable.getUploadRetryMs();
  result.durableLog.uploadRetryMaxMs = durable.getUploadRetryMaxMs();
  result.durableLog.recoverParallelism = durable.getRecoverParallelism();
  result.durableLog.rebuildConcurrency = durable.getRebuildConcurrency();
  result.durableLog.residentObjects = durable.getResidentObjects();
  result.processPool.threadedDurableTestStorage = durable.getThreadedTestStorage();
  result.durableLog.usagePublishMs = durable.getUsagePublishMs();
  result.durableLog.alarmDiscoveryIntervalMs = durable.getAlarmDiscoveryIntervalMs();
  result.durableLog.alarmTakeoverConcurrency = durable.getAlarmTakeoverConcurrency();
  result.processPool.idleEvictMs = root.getIdleEvictMs();
  result.processPool.heatIdleMs = result.idleMs;
  result.suspectBaseMs = root.getSuspectBaseMs();
  result.suspectMaxMs = root.getSuspectMaxMs();
  result.suspectProbation = root.getSuspectProbation();
  result.drainTimeoutMs = root.getDrainTimeoutMs();
  result.loadCheck = root.getLoadCheck();
  result.webSocketsPerNode = root.getIngress().getWebSocketsPerNode();
  const auto keepalive = root.getIngress().getTcpKeepalive().asReader();
  KJ_REQUIRE(keepalive.getIdleSeconds() > 0 && keepalive.getIdleSeconds() <= 32767 &&
      keepalive.getIntervalSeconds() > 0 && keepalive.getIntervalSeconds() <= 32767 &&
      keepalive.getProbes() > 0 && keepalive.getProbes() <= 127, "invalid ingress.tcpKeepalive settings");
  result.ingressKeepalive = {keepalive.getIdleSeconds(), keepalive.getIntervalSeconds(), keepalive.getProbes()};
  auto rotation = root.getRotation();
  result.processPool.rotationDrainTimeoutMs = rotation.getDrainTimeoutMs();
  result.processPool.rotationTermGraceMs = rotation.getTermGraceMs();
  result.processPool.rotationBinaryCheckMs = rotation.getBinaryCheckMs();
  result.processPool.rotationMaxAgeMs = rotation.getMaxAgeMs();
  result.processPool.rotationMaxRequests = rotation.getMaxRequests();
  result.processPool.rotationResidentBytes = rotation.getResidentBytes();
  result.processPool.rotationResidentBytesDefault = !given({"rotation", "residentBytes"}).has_value();
  result.processPool.rotationRetryMs = rotation.getRetryMs();
  auto sandbox = root.getSandbox();
  result.processPool.sandboxMode = sandbox.getMode().cStr();
  result.processPool.launcherPath = sandbox.getLauncherPath().cStr();
  result.processPool.protectionKeys = sandbox.getProtectionKeys().cStr();
  result.processPool.syscallsMode = sandbox.getSyscalls().getMode().cStr();
  result.processPool.allowlistPath = sandbox.getSyscalls().getAllowlistPath().cStr();
  result.processPool.steadySyscallsMode = sandbox.getSyscalls().getSteady().getMode().cStr();
  result.processPool.steadyAllowlistPath = sandbox.getSyscalls().getSteady().getAllowlistPath().cStr();
  result.processPool.validate();
  auto membership = root.getMembership();
  result.membership = {
    membership.getBackend().cStr(),
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
  // fills may both appear only with the same value.
  result.stackDirectory = root.getStackDirectory().cStr();
  result.workerdPath = root.getWorkerdPath().cStr();
  result.listenAddress = root.getListenAddress().cStr();
  if (result.processPool.threadedDurableTestStorage) {
    KJ_REQUIRE(!given({"storage"}) && !given({"desiredState", "objectStore"}) &&
        result.membership.backend == "file" && result.desiredState.backend == "file" &&
        result.peers.empty() && !result.servePathsGiven(),
        "durableObjects.threadedTestStorage requires the positional single-node fixture and disposable local storage");
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
    // The object store holds the desired state and the members unless the configuration says file.
    if (!given({"desiredState", "backend"})) result.desiredState.backend = "objectStore";
    if (!given({"membership", "backend"})) result.membership.backend = "objectStore";
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
    KJ_REQUIRE(m.backend == "file" || m.backend == "objectStore", "membership.backend must be file or objectStore", m.backend.c_str());
    if (m.backend == "objectStore") {
      KJ_REQUIRE(result.desiredState.objectStore.backend == "s3" && !result.desiredState.objectStore.endpoint.empty(),
          "membership.backend objectStore keeps the members in the object store: it needs the storage block (or "
          "desiredState.objectStore with an endpoint)");
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
  result.managementAllowWithoutToken = root.getManagement().getAllowWithoutToken();
  auto control = root.getControl();
  result.controlListenAddress = control.getListenAddress().cStr();
  result.controlTemporaryDirectory = control.getTemporaryDirectory().cStr();
  result.controlMaxUploadBytes = control.getMaxUploadBytes();
  result.controlServerTimeoutMs = control.getServerTimeoutMs();
  result.controlPathRouting = control.getPathRouting();
  result.controlPollIntervalMs = control.getPollIntervalMs();
  KJ_REQUIRE(result.controlPollIntervalMs >= 100 && result.controlPollIntervalMs <= 3600000, "control.pollIntervalMs must be 100 to 3600000");
  result.controlCleanupGraceMs = control.getCleanupGraceMs();
  KJ_REQUIRE(result.controlCleanupGraceMs <= 7 * 86400000ull, "control.cleanupGraceMs must be at most 604800000 (a week)");
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
  KJ_REQUIRE(e.timeoutMs > 0, "egress.timeoutMs must be positive");
  // The peer listener's authentication.
  result.peerAuth = {root.getPeerAuth().getKeyFile().cStr(), root.getPeerAuth().getWindowMs()};
  KJ_REQUIRE(result.peerAuth.windowMs >= 1000 && result.peerAuth.windowMs <= 600000, "peerAuth.windowMs must be 1000 to 600000");
  return result;
}

}  // namespace coworkerd

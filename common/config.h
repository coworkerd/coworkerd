#pragma once

#include "object-store.h"

#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <utility>
#include <vector>

// The configuration file both daemons read (one file, the same keys and defaults) and the settings of the
// node's parts it fills (the Server's main.c++, process-pool.h,
// membership.h, async-log.h, bundle-cache.h and deployment.h).
namespace coworkerd {

struct ProcessPoolConfig {
  uint32_t count = 1;
  uint32_t inFlightLimit = 512;
  uint32_t basePort = 9000;  // ignored (workerd listens on an inherited Unix socket)
  uint32_t probeIntervalMs = 50;
  uint32_t probeOverdueMs = 400;
  uint32_t stuckTimeoutMs = 3000;
  uint32_t restartBackoffMs = 100;
  uint32_t restartMaxMs = 2000;
  bool watchdog = true;
  uint32_t cpuLimitMs = 200;
  uint32_t memoryLimitMiB = 128;
  uint32_t quarantineProcesses = 2;  // sharded by tenant, and the trial loads' processes
  uint32_t idleEvictMs = 300000;
  uint32_t idleDestroyIntervalMs = 10;
  uint32_t heatIdleMs = 30000;
  // The memory band's shared memory (MemoryWatermark), passed to every workerd; -1 when off.
  int memoryBandFd = -1;
  // Process rotation: a slot's next generation takes new calls while the previous one drains.
  // Set by ServerConfig.rotation; environment overrides: COWORKERD_ROTATION_{DRAIN_TIMEOUT_MS,
  // TERM_GRACE_MS,BINARY_CHECK_MS,MAX_AGE_MS,MAX_REQUESTS,RESIDENT_BYTES,RETRY_MS}.
  uint32_t rotationDrainTimeoutMs = 30000;
  uint32_t rotationTermGraceMs = 5000;
  uint32_t rotationBinaryCheckMs = 1000;    // 0 disables the executable-change trigger
  uint64_t rotationMaxAgeMs = 3600000;      // 0 = off; an hour by default
  uint64_t rotationMaxRequests = 0;         // 0 = off
  uint64_t rotationResidentBytes = 0;       // memory trigger interface; 0 = off
  uint32_t rotationRetryMs = 5000;          // first retry after a failed automatic rotation (then doubling)
  // The sandbox: each workerd started through coworkerd-launcher in its least-privilege environment
  // (launcher/PROTOCOL.md). "off" starts workerd directly; "launcher" through the launcher, and the
  // pool refuses to start when the launcher's check fails here; "auto" through the launcher when its check
  // passes, else directly with a warning. Set by ServerConfig.sandbox; COWORKERD_SANDBOX_MODE overrides it.
  // `launcherPath` is the launcher's file (the Server fills in the default: beside its own executable).
  std::string sandboxMode = "auto";
  std::string launcherPath;
  // The system call allowlist, installed by the launcher before it executes workerd. The list is the
  // build's (scripts/syscall-allowlist.py: generated from a profile of the workerd it ships and delivered
  // in the same package), and it names the workerd it belongs to: a list that names other bytes than the
  // ones a launch runs is refused by the launcher, which reports it as a launch failure.
  //   off      no filter.
  //   observe  a call outside the list is reported to the Server and then proceeds, so the process behaves
  //            as it does without the filter (what to run before the enforce mode).
  //   enforce  a call outside the list ends the process (exit category syscall-denied).
  // Set by ServerConfig.sandbox.syscalls; COWORKERD_SANDBOX_SYSCALLS and COWORKERD_SANDBOX_ALLOWLIST
  // override them. `allowlistPath` default: coworkerd-syscalls-startup.txt beside the Server's executable.
  std::string syscallsMode = "observe";
  std::string allowlistPath;
  // The steady-state table, installed by workerd itself at its first request, together with the
  // dumpable bit the execve reset: the same three modes, over the narrower list of the same
  // build (coworkerd-syscalls-steady.txt). Default off -- run observe over real traffic first, as for the
  // startup table. Set by ServerConfig.sandbox.syscalls.steady; COWORKERD_SANDBOX_SYSCALLS_STEADY and
  // COWORKERD_SANDBOX_STEADY_ALLOWLIST override them.
  std::string steadySyscallsMode = "off";
  std::string steadyAllowlistPath;
  void validate() const;
};

struct MembershipConfig {
  std::string backend = "file";  // file, etcd or objectStore
  std::string endpoint;
  std::string advertisedAddress;
  uint32_t ttlSeconds = 15;
  uint32_t requestTimeoutMs = 750;
  uint32_t startupTimeoutMs = 15000;
  uint32_t retryMs = 250;
  // objectStore (the schema's comments; validated by ServerConfig::load).
  uint32_t heartbeatIntervalMs = 5000;
  uint32_t listIntervalMs = 10000;
  uint32_t livenessGraceMs = 10000;
  uint32_t clockSkewToleranceMs = 2000;
  uint32_t createSettleMs = 1000;
};

// The Workers' logs (ServerConfig.logs). The drainer keeps every tenant line it can attribute in
// memory, per tenant, within the quota; the sink file keeps every line and rotates
// by size. A tenant's own quota (its record's logLinesPerSecond and logBufferBytes) replaces the first two.
struct WorkerLogSettings {
  uint64_t tenantBufferBytes = 1 << 20;
  uint64_t totalBufferBytes = 64ull << 20;
  uint32_t linesPerSecond = 100;
  uint32_t maxLineBytes = 4096;
  // The sink is renamed to <sink>.1 (.1 to .2, ...) once it reaches sinkMaxBytes; sinkFiles files in
  // all. 0 bytes: no rotation. A sink that is not a regular file (a FIFO) never rotates.
  uint64_t sinkMaxBytes = 256ull << 20;
  uint32_t sinkFiles = 4;
};

// Per-tenant usage (ServerConfig.usage): counted on the node, the current hour written to its own etcd
// prefix every flushIntervalMs, rows older than retentionHours deleted (0 keeps them).
struct UsageSettings {
  uint32_t flushIntervalMs = 60000;
  uint32_t retentionHours = 744;
  uint32_t sampleIntervalMs = 1000;
};

struct BundleCacheConfig {
  std::filesystem::path directory;
  uint64_t maxBytes = 64 * 1024 * 1024;
  uint32_t prefetchConcurrency = 2;
};

struct DesiredStateConfig {
  std::string backend = "file";  // file or objectStore
  std::string endpoint;          // unused since 0.4
  std::filesystem::path codeDirectory;
  uint32_t requestTimeoutMs = 750;
  uint32_t startupTimeoutMs = 15000;
  uint32_t retryMs = 250;
  std::string versionResolutionForTest;
  uint32_t deploymentApplyDelayMsForTest = 0;
  ObjectStoreConfig objectStore;
  BundleCacheConfig cache;
  uint32_t pollIntervalMs = 5000;
  // The secrets key's file (ServerConfig::secretsKeyFile, after COWORKERD_SECRETS_KEY_FILE).
  std::string secretsKeyFile;
};

// Tenant Workers' outbound requests (ServerConfig.egress, the schema's comments). The rules are kept as written
// here; the node's egress service parses and checks them when it starts.
struct EgressTenantPolicy {
  std::vector<std::string> allow, deny, allowInternal;
  uint32_t concurrency = 0, requestsPerSecond = 0;  // 0: the cluster's
};
struct EgressConfig {
  std::string mode = "on";
  std::string defaultPolicy = "public";
  std::vector<uint16_t> publicPorts = {80, 443};
  uint32_t timeoutMs = 30000;
  uint32_t concurrency = 32;
  uint32_t requestsPerSecond = 100;
  uint32_t dnsCacheMs = 10000;
  std::string caFile;
  bool systemTrustStore = true;
  std::vector<std::string> clusterAddresses;
  std::map<std::string, EgressTenantPolicy> tenants;
  bool on() const { return mode == "on"; }
};

// The peer listener's authentication (ServerConfig.peerAuth).
struct PeerAuthConfig {
  std::string keyFile;
  uint32_t windowMs = 30000;
};

struct ServerConfig {
  // The node's id: `nodeId` in the configuration file, the last segment of its
  // member key /coworkerd/v2/nodes/<nodeId>. The C++ name is `serverId` (the code calls the node's id this throughout).
  std::string serverId = "local";
  std::string peerListenAddress;
  std::map<std::string, std::string> peers;
  std::map<std::string, std::string> placements;
  uint8_t initialPlacementHops = 8;
  bool disableAlreadyPlacedForTest = false;
  uint32_t peerTimeoutMs = 2000;
  int64_t clockOffsetMsForTest = 0;
  uint32_t rootLimit = 128;
  uint32_t nestedLimit = 384;
  uint32_t idleMs = 30000;
  double homeProbability = 0.05;
  uint32_t avoidMaxMs = 2000;
  uint32_t rejectionAvoidMs = 100;
  uint32_t connectBackoffMs = 100;
  std::string loadBandForTest = "green";
  bool memoryWatermarkEnabled = true;
  uint64_t memoryBudgetBytes = 0;
  bool equalLimitsForTest = false;
  bool ingressPlacement = true;
  coworkerd::ProcessPoolConfig processPool;
  coworkerd::MembershipConfig membership;
  uint32_t suspectBaseMs = 600000, suspectMaxMs = 3600000;
  coworkerd::DesiredStateConfig desiredState;
  // The Server's exit; COWORKERD_DRAIN_TIMEOUT_MS overrides it. The test knob delays
  // the authority write of the announcement (the Server refuses new work at once), widening
  // the window in which other Servers still place onto it.
  uint32_t drainTimeoutMs = 30000;
  uint32_t drainAnnounceDelayMsForTest = 0;
  // The load check before a version's first dispatch to a process; COWORKERD_LOAD_CHECK overrides it.
  bool loadCheck = true;
  // The configuration file of `serve`. The four paths and addresses are serve only (the
  // positional form takes them as arguments); the shortcuts below them fill the nested fields.
  std::string stackDirectory, workerdPath, runtimePath, listenAddress;
  std::string etcdEndpoint, storageCredentialsFile;
  bool serverIdGiven = false, cacheFollowsStack = false;
  // COWORKERD_MANAGEMENT_TOKEN overrides the file's management.token (run()).
  std::string managementToken;
  // The control plane (`control`) and the Workers' logs (each Server's workerd output reader).
  std::string controlListenAddress, controlTemporaryDirectory;
  uint64_t controlMaxUploadBytes = 167772160;
  uint32_t controlServerTimeoutMs = 2000;
  uint32_t controlPollIntervalMs = 5000;
  // The administrator's credential of /v1 (COWORKERD_ADMIN_CREDENTIAL overrides it) and path routing
  // under the default host.
  std::string controlAdminCredential;
  bool controlPathRouting = true;
  coworkerd::WorkerLogSettings logs;
  coworkerd::UsageSettings usage;
  // Outbound requests, the peer listener's authentication, etcd's credentials (the file this configuration
  // names; COWORKERD_ETCD_CREDENTIALS_FILE overrides it, see etcd.h).
  coworkerd::EgressConfig egress;
  coworkerd::PeerAuthConfig peerAuth;
  std::string etcdCredentialsFile;
  // The cluster's secrets key (secrets.keyFile; COWORKERD_SECRETS_KEY_FILE overrides it).
  std::string secretsKeyFile;

  // A management token: 16 to 4096 printable characters without spaces. Never echoed.
  static void checkToken(const std::string& token, const char* origin);

  // {"accessKey": "...", "secretKey": "..."}: the Servers' read identity of the object store.
  // Errors name the file and the rule, never its content.
  static std::pair<std::string, std::string> readCredentials(const std::filesystem::path& path);

  static ServerConfig load(const std::filesystem::path& path);

  bool servePathsGiven() const {
    return !stackDirectory.empty() || !workerdPath.empty() || !runtimePath.empty() || !listenAddress.empty();
  }

  bool hasPlacement(const std::string& workerId) const {
    return placements.contains(workerId);
  }

  std::string targetServer(const std::string& workerId) const {
    auto found = placements.find(workerId);
    return found == placements.end() ? serverId : found->second;
  }
};

}  // namespace coworkerd

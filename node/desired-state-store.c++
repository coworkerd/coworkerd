#include "desired-state.h"
#include "coworkerd.capnp.h"
#include "object-state.h"
#include "object-store.h"
#include "code-package.h"
#include "bundle-cache.h"
#include "tenant-manifest.h"

#include <capnp/message.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <sstream>

// The desired state on the object store (common/tenant-manifest.h): every pollIntervalMs one GET of the tenant index's
// hint; when it names another revision, the index, then only the manifests whose revision the index changed, the
// definitions they name by hash (immutable: each read once and verified), the secrets' values opened with the cluster's
// secrets key -- all of it turned into the deployment state's keys and applied at the index's revision, so the
// evaluation (DeploymentState) is the one 0.3's keys had.
namespace coworkerd {
namespace {
using json::quote;

uint64_t nowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string activeVersionsJson(const DeploymentState& state, bool details, size_t limit) {
  std::string out = "{";
  size_t index = 0;
  for (const auto& [id, worker] : state.workers()) {
    if (!worker.valid) continue;
    if (index == limit) break;
    if (index++) out += ',';
    // Details carry a declared secret's name and length, never its value.
    out += quote(id) + ':' + (details ? versionStatusJson(*worker.version, state.secretsStatusField(*worker.version))
        : activePointerJson(worker.version->versionId, worker.version->definitionHash));
  }
  return out + '}';
}

// Per-Worker maps in the full status are bounded; tests that need every
// Worker use the counts and digests.
constexpr size_t STATUS_ACTIVE_LIMIT = 256;
constexpr size_t STATUS_PROBLEM_LIMIT = 20;
constexpr size_t FETCH_CONCURRENCY = 8;
// A definition that could not be read is tried again at the next poll, but not more often than this.
constexpr uint64_t DEFINITION_RETRY_MS = 2000;

class ObjectStoreDesiredState final: public DesiredState, private kj::TaskSet::ErrorHandler {
public:
  ObjectStoreDesiredState(DesiredStateConfig config, kj::Network& network, kj::Timer& timer)
      : settings(std::move(config)), timer(timer), storage(settings.objectStore, network, timer),
        cache(storage, settings.cache, timer), definitionTasks(*this), key(SecretsKey::load(settings.secretsKeyFile)) {
    KJ_REQUIRE(settings.startupTimeoutMs > 0 && settings.retryMs > 0 && settings.pollIntervalMs > 0,
        "deployment timeouts must be positive");
  }
  kj::Promise<void> start() override {
    try {
      co_await timer.timeoutAfter(settings.startupTimeoutMs * kj::MILLISECONDS, startRetry());
    } catch (const kj::Exception& error) {
      if (refusal.empty()) KJ_FAIL_REQUIRE("the desired state could not be read from the object store before serving",
          lastPollError.c_str(), error.getDescription());
      KJ_FAIL_REQUIRE(refusal.c_str());
    }
  }
  kj::Promise<void> run() override {
    while (true) {
      co_await timer.afterDelay(settings.pollIntervalMs * kj::MILLISECONDS);
      co_await pollShared();
    }
  }
  kj::Promise<void> refresh() override {
    // A poll already running may have read the hint before the write the caller knows of: wait for it, then read again.
    if (pollFlight) co_await pollFlight->addBranch();
    ++polls.refreshes;
    co_await pollShared();
  }
  void reload() override { KJ_FAIL_REQUIRE("reload is only available for the file backend"); }
  const DeploymentState& state() const override { return current; }
  std::shared_ptr<const VersionDefinition> find(const VersionPin& pin, std::string& reason) const override {
    return index.find(pin, reason);
  }
  std::shared_ptr<const VersionDefinition> exact(const std::string& label, const std::string& hash) const override {
    return index.exact(label, hash);
  }
  std::shared_ptr<const VersionDefinition> byHash(const std::string& hash) const override { return index.byHash(hash); }
  kj::Promise<std::shared_ptr<const VersionDefinition>> resolve(
      VersionPin pin, double remainingMs, std::string& reason) override {
    auto known = index.find(pin, reason);
    if (known || reason == "定义不一致") co_return known;
    auto found = definitionFlights.find(pin.definitionHash);
    if (found == definitionFlights.end()) {
      auto hash = pin.definitionHash;
      auto flight = fetchDefinition(hash).then([this, hash](DefinitionResult result) {
        definitionFlights.erase(hash);
        return result;
      }).fork();
      found = definitionFlights.emplace(hash,
          std::make_unique<kj::ForkedPromise<DefinitionResult>>(kj::mv(flight))).first;
      definitionTasks.add(found->second->addBranch().ignoreResult());
    }
    auto result = co_await found->second->addBranch().exclusiveJoin(
        timer.afterDelay(static_cast<int64_t>(std::max(0.0, remainingMs) * 1000) * kj::MICROSECONDS)
            .then([] { return DefinitionResult{nullptr, "定义未知（拉取超时）"}; }));
    if (!result.version) { reason = result.reason; co_return nullptr; }
    // Concurrent applies or different callers may have learned the hash while
    // the GET was in progress. Recheck identity before execution.
    auto resolved = index.find(pin, reason);
    co_return resolved;
  }
  kj::Promise<std::string> ensureCode(const VersionDefinition& version, double remainingMs) override {
    if (version.format == "v1") co_return co_await cache.ensure(version.contentHash, remainingMs);
    initializePackageCaches();
    const auto deadline = timer.now() + static_cast<int64_t>(std::max(0.0, remainingMs) * 1000) * kj::MICROSECONDS;
    auto remaining = [&] { return std::max(0.0, static_cast<double>((deadline - timer.now()) / kj::MICROSECONDS) / 1000); };
    // Every package's manifest is read from the local cache, checked against its
    // hash and parsed once; later calls use the parsed copy (no manifest file
    // read per call). Module bytes are still verified whenever they are read.
    auto read = co_await packageManifest(version.contentHash, remaining());
    if (!read.package) co_return read.reason;
    // Code modules only: asset bytes are fetched when first requested.
    for (const auto& m : read.package->modules) {
      auto reason = co_await modules->ensure(m.sha256, remaining());
      if (!reason.empty()) co_return reason;
    }
    co_return std::string();
  }
  kj::Promise<SharedAssetIndex> assets(const VersionDefinition& version, double remainingMs, std::string& reason) override {
    auto read = co_await packageManifest(version.contentHash, remainingMs);
    if (read.package) co_return read.package->assets;
    ++assetCounters.manifestFailures;
    const auto detail = unprefixed(read.reason);
    reason = "资源不可用：" + (detail.starts_with("包清单") ? detail : "包清单" + detail);
    co_return nullptr;
  }
  kj::Promise<AssetRead> assetBytes(const CodeAsset& asset, double remainingMs) override {
    initializePackageCaches();
    auto read = co_await modules->read(asset.sha256, remainingMs);
    if (!read.reason.empty()) {
      ++assetCounters.fetchFailures;
      co_return AssetRead{{}, "资源不可用：" + unprefixed(read.reason)};
    }
    co_return AssetRead{std::move(read.bytes), {}};
  }
  kj::Promise<std::string> source(const VersionDefinition& version) override {
    if (version.format == "v1") co_return co_await cache.source(version.contentHash);
    initializePackageCaches();
    auto manifest = co_await packages->source(version.contentHash);
    std::map<std::string, std::string> bytes;
    for (const auto& m : readCodePackage(manifest)) {
      auto content = co_await modules->source(m.sha256);
      KJ_REQUIRE(content.size() == m.bytes, "module byte count mismatch");
      bytes.emplace(m.sha256, std::move(content));
    }
    co_return packageSource(manifest, bytes);
  }
  std::string bundlesJson() const override {
    auto result = cache.json();
    result.pop_back();
    return result + ",\"modules\":" + cacheStatus(modules) + ",\"packages\":" + cacheStatus(packages) +
        ",\"packagePrefetchPending\":" + std::to_string(packagePrefetchPending) +
        ",\"outOfBandDefinitions\":" + std::to_string(outOfBandDefinitions) +
        ",\"definitionFetchFailures\":" + std::to_string(definitionFetchFailures) +
        ",\"assets\":{\"indexes\":" + std::to_string(std::count_if(packageCache.begin(), packageCache.end(),
            [](const auto& entry) { return !entry.second.package->assets->byPath.empty(); })) +
        ",\"indexedAssets\":" + std::to_string(packageCacheEntries) +
        ",\"manifestFailures\":" + std::to_string(assetCounters.manifestFailures) +
        ",\"fetchFailures\":" + std::to_string(assetCounters.fetchFailures) + "}" +
        ",\"packageManifests\":{\"entries\":" + std::to_string(packageCache.size()) +
        ",\"limit\":" + std::to_string(PACKAGE_CACHE_LIMIT) + ",\"assetEntries\":" + std::to_string(packageCacheEntries) +
        ",\"assetLimit\":" + std::to_string(PACKAGE_CACHE_ASSET_LIMIT) + ",\"hits\":" + std::to_string(manifestCounters.hits) +
        ",\"parses\":" + std::to_string(manifestCounters.parses) + ",\"merged\":" + std::to_string(manifestCounters.merged) +
        ",\"evictions\":" + std::to_string(manifestCounters.evictions) +
        ",\"invalidations\":" + std::to_string(manifestCounters.invalidations) + ",\"packages\":" + cachedPackagesJson() + "}}";
  }
  std::string json(bool full) const override {
    std::ostringstream out;
    out << "{\"backend\":\"objectStore\",\"revision\":\"" << current.revision()
        << "\",\"generation\":" << current.generation();
    if (full) {
      out << ",\"activeVersions\":" << activeVersionsJson(current, false, STATUS_ACTIVE_LIMIT)
          << ",\"activeVersionDetails\":" << activeVersionsJson(current, true, STATUS_ACTIVE_LIMIT)
          << ",\"activeVersionsTruncated\":" << (current.validCount() > STATUS_ACTIVE_LIMIT ? "true" : "false");
    }
    out << ',' << current.statusFields(STATUS_PROBLEM_LIMIT)
        << ",\"rejectedRevisions\":" << rejectedRevisions << ",\"lastRejectedRevision\":\"" << lastRejectedRevision
        << "\",\"lastRejectedReason\":" << quote(lastRejectedReason)
        // The fields 0.3 reported for its watch, as the poll has them: a full read at the start, no compaction; connected while
        // the last poll reached the store.
        << ",\"fullRereads\":" << (started ? 1 : 0) << ",\"compactions\":0"
        << ",\"watchConnected\":" << (lastPollOk ? "true" : "false")
        << ",\"watchRevision\":\"" << current.revision() << "\",\"watchLastError\":" << quote(lastPollError)
        << ",\"apply\":{\"count\":" << applies.count << ",\"lastMicros\":" << applies.lastMicros
        << ",\"maxMicros\":" << applies.maxMicros << ",\"totalMicros\":" << applies.totalMicros
        << ",\"lastKeys\":" << applies.lastKeys << ",\"lastWorkers\":" << applies.lastWorkers
        << ",\"lastEvaluated\":" << applies.lastEvaluated << ",\"lastRoutes\":" << applies.lastRoutes
        << ",\"lastFull\":" << (applies.lastFull ? "true" : "false") << '}'
        << ",\"pollFailures\":" << polls.failures << ",\"lastPollAtUnixMs\":" << polls.lastAtUnixMs
        << ",\"poll\":{\"intervalMs\":" << settings.pollIntervalMs << ",\"polls\":" << polls.count
        << ",\"lastOkAtUnixMs\":" << polls.lastOkAtUnixMs << ",\"lastChangeAtUnixMs\":" << polls.lastChangeAtUnixMs
        << ",\"hintReads\":" << polls.hints << ",\"indexReads\":" << polls.indexes << ",\"listings\":" << polls.listings
        << ",\"manifestReads\":" << polls.manifests << ",\"definitionReads\":" << polls.definitions
        << ",\"refreshes\":" << polls.refreshes
        << ",\"tenants\":" << tenants.size() << ",\"definitions\":" << definitionBytes.size()
        << ",\"missingDefinitions\":" << missingDefinitionsJson() << ",\"undecryptableSecrets\":" << undecryptableJson()
        << ",\"routeConflicts\":" << routeConflicts << ",\"secretsKey\":" << quote(key.fingerprint())
        << ",\"bucket\":" << quote(storage.config().bucket) << ",\"client\":" << storage.stats().json() << '}'
        << ",\"versionResolutionForTest\":" << quote(settings.versionResolutionForTest)
        << ",\"deploymentApplyDelayMsForTest\":" << settings.deploymentApplyDelayMsForTest << '}';
    return out.str();
  }
  const DesiredStateConfig& config() const override { return settings; }
  void onTenants(std::function<void(const std::map<std::string, std::optional<TenantRecord>>&, uint64_t, bool)> sink) override {
    tenantSink = std::move(sink);
  }

private:
  DesiredStateConfig settings;
  kj::Timer& timer;
  ObjectStore storage;
  BundleCache cache;
  std::unique_ptr<BundleCache> packages, modules;
  void initializePackageCaches() {
    // A v1-only deployment retains its original flat cache directory.
    if (packages) return;
    packages = std::make_unique<BundleCache>(storage, BundleCacheConfig{
        settings.cache.directory / "packages", settings.cache.maxBytes, settings.cache.prefetchConcurrency}, timer, "packages/");
    modules = std::make_unique<BundleCache>(storage, BundleCacheConfig{
        settings.cache.directory / "modules", settings.cache.maxBytes, settings.cache.prefetchConcurrency}, timer, "modules/");
  }
  std::string cacheStatus(const std::unique_ptr<BundleCache>& value) const {
    if (value) return value->json();
    return "{\"backend\":" + quote(storage.config().backend) + ",\"entries\":0,\"bytes\":0,\"maxBytes\":" +
        std::to_string(settings.cache.maxBytes) +
        ",\"hits\":0,\"misses\":0,\"fetchStarted\":0,\"fetchSucceeded\":0,\"singleFlightMerged\":0,\"evictions\":0,"
        "\"prefetchQueueLength\":0,\"prefetchFailures\":0,\"lastError\":\"\",\"failures\":{}}";
  }
  // Verified, parsed manifests of every package, by package hash: the code
  // module list and the asset index (empty without assets). A manifest is read
  // and parsed once (concurrent first uses share that read); bounded by package
  // count and by the number of indexed assets, least recently used out; the
  // packages no Worker serves any more are dropped when a revision is applied
  // (a call still pinned to one reads and verifies its manifest again).
  static constexpr size_t PACKAGE_CACHE_LIMIT = 128;
  static constexpr size_t PACKAGE_CACHE_ASSET_LIMIT = 200000;
  struct ParsedPackage { CodePackage modules; SharedAssetIndex assets; };
  using SharedPackage = std::shared_ptr<const ParsedPackage>;
  struct CachedPackage { SharedPackage package; uint64_t used = 0; };
  std::map<std::string, CachedPackage> packageCache;
  uint64_t packageCacheSequence = 0, packageCacheEntries = 0;
  struct { uint64_t hits = 0, parses = 0, merged = 0, evictions = 0, invalidations = 0; } manifestCounters;
  struct { uint64_t manifestFailures = 0, fetchFailures = 0; } assetCounters;
  struct ManifestRead { SharedPackage package; std::string reason; };
  std::map<std::string, std::unique_ptr<kj::ForkedPromise<ManifestRead>>> manifestFlights;
  // The package hashes whose parsed manifests are held (at most PACKAGE_CACHE_LIMIT).
  std::string cachedPackagesJson() const {
    std::string out = "[";
    for (const auto& [hash, entry] : packageCache) { if (out.size() > 1) out += ','; out += quote(hash); }
    return out + ']';
  }
  kj::Promise<ManifestRead> packageManifest(std::string packageHash, double remainingMs) {
    initializePackageCaches();
    if (auto found = packageCache.find(packageHash); found != packageCache.end()) {
      found->second.used = ++packageCacheSequence;
      ++manifestCounters.hits;
      return ManifestRead{found->second.package, {}};
    }
    auto flight = manifestFlights.find(packageHash);
    if (flight == manifestFlights.end()) {
      auto read = readManifest(packageHash).then([this, packageHash](ManifestRead result) {
        manifestFlights.erase(packageHash);
        return result;
      }).fork();
      flight = manifestFlights.emplace(packageHash, std::make_unique<kj::ForkedPromise<ManifestRead>>(kj::mv(read))).first;
      definitionTasks.add(flight->second->addBranch().ignoreResult());
    } else {
      ++manifestCounters.merged;
    }
    // Each caller keeps its own deadline; the read itself has the object store's.
    auto delay = static_cast<int64_t>(std::max(0.0, std::min(remainingMs, 86400000.0)) * 1000);
    return flight->second->addBranch().exclusiveJoin(timer.afterDelay(delay * kj::MICROSECONDS).then([] {
      return ManifestRead{nullptr, "代码不可用：拉取超时（调用截止时间）"};
    }));
  }
  kj::Promise<ManifestRead> readManifest(std::string packageHash) {
    auto reason = co_await packages->ensure(packageHash, settings.objectStore.timeoutMs + 1);
    if (!reason.empty()) co_return ManifestRead{nullptr, reason};
    try {
      auto manifest = co_await packages->source(packageHash);
      auto contents = readPackage(manifest, limits);
      KJ_REQUIRE(packageJson(contents) == manifest, "package is not canonical");
      co_return ManifestRead{rememberPackage(packageHash, std::move(contents)), {}};
    } catch (const kj::Exception&) {
      co_return ManifestRead{nullptr, "代码不可用：包清单校验失败"};
    }
  }
  void dropUnservedPackages() {
    for (auto entry = packageCache.begin(); entry != packageCache.end();) {
      if (servedPackages.contains(entry->first)) { ++entry; continue; }
      packageCacheEntries -= entry->second.package->assets->byPath.size();
      entry = packageCache.erase(entry);
      ++manifestCounters.invalidations;
    }
  }
  AssetLimits limits = AssetLimits::fromEnvironment();
  static std::string unprefixed(const std::string& reason) {
    const std::string prefix = "代码不可用：";
    return reason.starts_with(prefix) ? reason.substr(prefix.size()) : reason;
  }
  SharedPackage rememberPackage(const std::string& packageHash, PackageContents contents) {
    if (auto existing = packageCache.find(packageHash); existing != packageCache.end()) return existing->second.package;
    ++manifestCounters.parses;
    auto index = std::make_shared<AssetIndex>();
    for (auto& asset : contents.assets) index->byPath.emplace(asset.path, std::move(asset));
    index->bytes = contents.assetBytes;
    while (!packageCache.empty() && (packageCache.size() >= PACKAGE_CACHE_LIMIT ||
        packageCacheEntries + index->byPath.size() > PACKAGE_CACHE_ASSET_LIMIT)) {
      auto oldest = std::min_element(packageCache.begin(), packageCache.end(),
          [](const auto& a, const auto& b) { return a.second.used < b.second.used; });
      packageCacheEntries -= oldest->second.package->assets->byPath.size();
      packageCache.erase(oldest);
      ++manifestCounters.evictions;
    }
    packageCacheEntries += index->byPath.size();
    auto package = std::make_shared<const ParsedPackage>(ParsedPackage{std::move(contents.modules), std::move(index)});
    packageCache[packageHash] = {package, ++packageCacheSequence};
    return package;
  }
  uint64_t packagePrefetchPending = 0, packagePrefetchEpoch = 0;
  kj::Promise<void> prefetchPackages(std::vector<std::shared_ptr<const VersionDefinition>> versions, uint64_t epoch) {
    for (const auto& version : versions) {
      uint32_t backoff = 100;
      while (epoch == packagePrefetchEpoch) {
        auto reason = co_await ensureCode(*version, settings.objectStore.timeoutMs + 1);
        if (epoch != packagePrefetchEpoch) co_return;
        if (reason.empty()) { --packagePrefetchPending; break; }
        co_await timer.afterDelay(backoff * kj::MILLISECONDS);
        backoff = std::min<uint32_t>(backoff * 2, 5000);
      }
    }
  }
  struct DefinitionResult { std::shared_ptr<const VersionDefinition> version; std::string reason; };
  std::map<std::string, std::unique_ptr<kj::ForkedPromise<DefinitionResult>>> definitionFlights;
  uint64_t outOfBandDefinitions = 0, definitionFetchFailures = 0;
  kj::TaskSet definitionTasks;
  void taskFailed(kj::Exception&&) override { ++definitionFetchFailures; }
  // A definition a call tree's lock names that this node's state does not (its view lags, or the version left): by
  // hash, verified, kept.
  kj::Promise<DefinitionResult> fetchDefinition(std::string hash) {
    DefinitionResult result;
    try {
      std::string bytes;
      if (auto known = definitionBytes.find(hash); known != definitionBytes.end()) bytes = known->second;
      else {
        auto response = co_await storage.get("definitions/" + hash);
        if (response.status != 200) {
          result.reason = response.status == 404 ? "定义未知（对象不存在）" : "定义未知（拉取失败）";
        }
        bytes = std::move(response.bytes);
      }
      if (result.reason.empty()) {
        auto version = decodeDeploymentDefinition(bytes);
        if (version->definitionHash != hash || deploymentSha256(definitionEncoding(*version)) != hash) {
          result.reason = "定义未知（内容哈希不符）";
        } else {
          index.remember(version);
          ++outOfBandDefinitions;
          result.version = std::move(version);
        }
      }
    } catch (const kj::Exception&) { result.reason = "定义未知（拉取失败或校验失败）"; }
    if (!result.version) ++definitionFetchFailures;
    co_return result;
  }
  DeploymentState current;
  DefinitionIndex index;
  SecretsKey key;
  bool started = false;
  uint64_t rejectedRevisions = 0;
  uint64_t lastRejectedRevision = 0;
  std::string lastRejectedReason;
  // A start that must not be retried (the bucket is not a cluster's): its reason.
  std::string refusal;
  // The applied view: each tenant's manifest and the keys it contributed, every route key, the generation.
  std::map<std::string, TenantManifest> tenants;
  std::map<std::string, std::map<std::string, std::string>> tenantKeys;
  std::map<std::string, std::string> routeKeys;
  std::string appliedGeneration;
  size_t routeConflicts = 0;
  // Definitions by hash: verified bytes, and the ones that could not be read with why.
  DefinitionBytes definitionBytes;
  std::map<std::string, std::string> missingDefinitions;
  uint64_t lastDefinitionRetryAtUnixMs = 0;
  std::set<std::string> undecryptable;  // <tenant>/<worker>/<name>
  std::function<void(const std::map<std::string, std::optional<TenantRecord>>&, uint64_t, bool)> tenantSink;
  bool lastPollOk = false;
  std::string lastPollError;
  struct {
    uint64_t count = 0, failures = 0, lastAtUnixMs = 0, lastOkAtUnixMs = 0, lastChangeAtUnixMs = 0;
    uint64_t hints = 0, indexes = 0, listings = 0, manifests = 0, definitions = 0, refreshes = 0;
  } polls;
  // One poll at a time: the periodic one and the ones asked for share it.
  std::unique_ptr<kj::ForkedPromise<void>> pollFlight;
  kj::Promise<void> pollShared() {
    if (!pollFlight) {
      pollFlight = std::make_unique<kj::ForkedPromise<void>>(pollGuarded().then([this]() { pollFlight.reset(); }).fork());
      // A branch held by the task set keeps the fork alive while it resolves and its owner is reset.
      definitionTasks.add(pollFlight->addBranch());
    }
    return pollFlight->addBranch();
  }
  // Code of the served Workers, kept current from each batch's served changes
  // so that background prefetch never rescans every Worker.
  std::map<std::string, size_t> servedBundles;
  std::map<std::string, std::pair<size_t, std::shared_ptr<const VersionDefinition>>> servedPackages;
  struct {
    uint64_t count = 0, lastMicros = 0, maxMicros = 0, totalMicros = 0;
    size_t lastKeys = 0, lastWorkers = 0, lastEvaluated = 0, lastRoutes = 0;
    bool lastFull = false;
  } applies;

  std::string missingDefinitionsJson() const {
    std::string out = "{";
    size_t index = 0;
    for (const auto& [hash, reason] : missingDefinitions) {
      if (index == STATUS_PROBLEM_LIMIT) break;
      out += (index++ ? "," : "") + quote(hash) + ":" + quote(reason);
    }
    return out + "}";
  }
  std::string undecryptableJson() const {
    std::string out = "[";
    size_t index = 0;
    for (const auto& name : undecryptable) {
      if (index == STATUS_PROBLEM_LIMIT) break;
      out += (index++ ? "," : "") + quote(name);
    }
    return out + "]";
  }

  kj::Promise<void> startRetry() {
    const auto begin = std::chrono::steady_clock::now();
    // The cluster's marker: a bucket without it is not a cluster's, and serving it as an empty cluster would be wrong.
    while (true) {
      std::string problem;
      try {
        auto marker = co_await storage.get(CLUSTER_FORMAT_KEY);
        if (marker.status == 404) {
          refusal = "the bucket " + storage.config().bucket + " at " + storage.config().endpoint + " holds no " + CLUSTER_FORMAT_KEY +
              ": it is not a Coworkerd cluster's bucket (the cluster's init writes the marker); not starting";
          std::cerr << "[deployment] " << refusal << std::endl;
          KJ_FAIL_REQUIRE(refusal.c_str());
        }
        KJ_REQUIRE(marker.status == 200, "object store read failed", marker.status);
        break;
      } catch (const kj::Exception& error) {
        if (!refusal.empty()) throw;
        problem = error.getDescription().cStr();
      }
      lastPollError = problem;
      co_await timer.afterDelay(settings.retryMs * kj::MILLISECONDS);
    }
    while (!started) {
      co_await pollGuarded();
      if (started) {
        // The initial read: the index, every manifest it names, their definitions, plus the full evaluation.
        std::cout << "[deployment] startup read micros=" << std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin).count() << " backend=objectStore revision=" << current.revision()
            << " tenants=" << tenants.size() << " workers=" << current.workers().size()
            << " routes=" << current.routes().size() << " definitions=" << current.definitions().size() << std::endl;
      } else {
        co_await timer.afterDelay(settings.retryMs * kj::MILLISECONDS);
      }
    }
  }

  kj::Promise<void> pollGuarded() {
    ++polls.count;
    polls.lastAtUnixMs = nowUnixMs();
    std::string failure;
    try {
      co_await poll();
      lastPollOk = true;
      lastPollError.clear();
      polls.lastOkAtUnixMs = nowUnixMs();
    } catch (const kj::Exception& error) {
      failure = error.getDescription().cStr();
    }
    if (!failure.empty()) {
      // The store cannot be read (or what it holds cannot): the node serves the state it has.
      lastPollOk = false;
      lastPollError = failure;
      if (++polls.failures == 1 || polls.failures % 60 == 0)
        std::cerr << "[deployment] poll failed failures=" << polls.failures << " reason=" << failure << std::endl;
    }
  }

  // The index's revision `revision` (when the hint named one), else the largest a listing names.
  kj::Promise<std::pair<uint64_t, std::string>> readIndex(std::optional<uint64_t> hinted) {
    uint64_t candidate = 0;
    bool listed = false;
    auto list = [&]() -> kj::Promise<uint64_t> {
      listed = true;
      ++polls.listings;
      co_return latestRevision(co_await storage.list(std::string(TENANT_INDEX_PREFIX) + '/'), TENANT_INDEX_PREFIX);
    };
    // A hint behind the revision applied (another writer's late hint) is not followed backwards.
    candidate = hinted && *hinted >= current.revision() ? *hinted : co_await list();
    if (!candidate) co_return std::pair<uint64_t, std::string>{0, {}};
    ++polls.indexes;
    auto object = co_await storage.get(revisionKey(TENANT_INDEX_PREFIX, candidate));
    if (object.status == 404 && !listed) {
      candidate = co_await list();
      if (!candidate) co_return std::pair<uint64_t, std::string>{0, {}};
      ++polls.indexes;
      object = co_await storage.get(revisionKey(TENANT_INDEX_PREFIX, candidate));
    }
    KJ_REQUIRE(object.status == 200, "object store read failed", object.status, revisionKey(TENANT_INDEX_PREFIX, candidate).c_str());
    co_return std::pair<uint64_t, std::string>{candidate, std::move(object.bytes)};
  }

  // The definitions the manifests name that this node has not verified yet, read by hash with a bounded number in
  // flight. One that is missing or does not verify stays out (the Worker that names it is not valid: "active Worker
  // Version is missing") and is tried again at a later poll.
  kj::Promise<bool> fetchNamedDefinitions(const std::map<std::string, TenantManifest>& view) {
    std::set<std::string> wanted;
    for (const auto& [tenant, manifest] : view)
      for (const auto& hash : namedDefinitions(manifest)) if (!definitionBytes.contains(hash)) wanted.insert(hash);
    bool added = false;
    std::vector<std::string> list(wanted.begin(), wanted.end());
    for (size_t start = 0; start < list.size(); start += FETCH_CONCURRENCY) {
      kj::Vector<kj::Promise<void>> reads;
      for (size_t i = start; i < std::min(list.size(), start + FETCH_CONCURRENCY); ++i) {
        reads.add([](ObjectStoreDesiredState& self, std::string hash, bool& added) -> kj::Promise<void> {
          ++self.polls.definitions;
          std::string problem;
          try {
            auto response = co_await self.storage.get("definitions/" + hash);
            if (response.status == 404) problem = "missing";
            else if (response.status != 200) problem = "read failed: HTTP " + std::to_string(response.status);
            else if (!definitionMatches(hash, response.bytes)) problem = "content hash mismatch";
            else {
              self.definitionBytes[hash] = std::move(response.bytes);
              self.missingDefinitions.erase(hash);
              added = true;
              co_return;
            }
          } catch (const kj::Exception&) {
            problem = "read failed";
          }
          self.missingDefinitions[hash] = problem;
        }(*this, list[i], added));
      }
      co_await kj::joinPromises(reads.releaseAsArray());
    }
    co_return added;
  }

  kj::Promise<void> poll() {
    ++polls.hints;
    auto hinted = co_await readHintRevision(storage, indexHintKey(TENANT_INDEX_PREFIX));
    const bool sameIndex = started && hinted && *hinted == current.revision();
    std::map<std::string, TenantManifest> next = tenants;
    TenantIndex index;
    uint64_t revision = current.revision();
    std::set<std::string> recompute;
    if (sameIndex) {
      // Nothing new (one GET); only a definition that could not be read before is tried again.
      const auto now = nowUnixMs();
      if (missingDefinitions.empty() || now - lastDefinitionRetryAtUnixMs < DEFINITION_RETRY_MS) co_return;
      lastDefinitionRetryAtUnixMs = now;
      if (!co_await fetchNamedDefinitions(next)) co_return;
      for (const auto& [tenant, manifest] : next) recompute.insert(tenant);
    } else {
      auto [read, bytes] = co_await readIndex(hinted);
      if (read) {
        index = decodeTenantIndex(bytes);
        KJ_REQUIRE(index.revision == read, "the tenant index names another revision than its key", read);
      }
      revision = read;
      if (started && revision == current.revision() && missingDefinitions.empty()) co_return;
      // The manifests whose revision the index changed, a bounded number in flight; a tenant the index no longer names
      // leaves.
      std::vector<std::pair<std::string, uint64_t>> wanted;
      for (const auto& [tenant, entry] : index.tenants) {
        auto known = next.find(tenant);
        if (known == next.end() || known->second.revision != entry.manifestRev) wanted.emplace_back(tenant, entry.manifestRev);
      }
      for (auto it = next.begin(); it != next.end();) {
        if (!index.tenants.contains(it->first)) { recompute.insert(it->first); it = next.erase(it); }
        else ++it;
      }
      for (size_t start = 0; start < wanted.size(); start += FETCH_CONCURRENCY) {
        kj::Vector<kj::Promise<TenantManifest>> reads;
        for (size_t i = start; i < std::min(wanted.size(), start + FETCH_CONCURRENCY); ++i) {
          reads.add([](ObjectStoreDesiredState& self, std::string tenant, uint64_t manifestRev) -> kj::Promise<TenantManifest> {
            ++self.polls.manifests;
            const auto objectKey = revisionKey(tenantManifestPrefix(tenant), manifestRev);
            auto response = co_await self.storage.get(objectKey);
            KJ_REQUIRE(response.status == 200, "object store read failed", response.status, objectKey.c_str());
            auto manifest = decodeTenantManifest(response.bytes);
            KJ_REQUIRE(manifest.tenant == tenant && manifest.revision == manifestRev,
                "a manifest names another tenant or revision than its key", objectKey.c_str());
            co_return manifest;
          }(*this, wanted[i].first, wanted[i].second));
        }
        for (auto& manifest : co_await kj::joinPromises(reads.releaseAsArray())) {
          recompute.insert(manifest.tenant);
          next[manifest.tenant] = std::move(manifest);
        }
      }
      lastDefinitionRetryAtUnixMs = nowUnixMs();
      if (co_await fetchNamedDefinitions(next)) for (const auto& [tenant, manifest] : next) recompute.insert(tenant);
    }
    co_await applyView(std::move(next), std::move(recompute), revision);
  }

  void serve(const std::shared_ptr<const VersionDefinition>& version, int delta) {
    if (!version) return;
    if (version->format == "v2") {
      auto& entry = servedPackages[version->contentHash];
      entry.first += delta;
      if (delta > 0) entry.second = version;
      if (entry.first == 0) servedPackages.erase(version->contentHash);
    } else {
      auto& count = servedBundles[version->contentHash];
      count += delta;
      if (count == 0) servedBundles.erase(version->contentHash);
    }
  }

  // The keys of the view against the keys applied: what changed is one batch at the index's revision.
  kj::Promise<void> applyView(std::map<std::string, TenantManifest> next, std::set<std::string> recompute, uint64_t revision) {
    if (settings.deploymentApplyDelayMsForTest > 0) {
      co_await timer.afterDelay(settings.deploymentApplyDelayMsForTest * kj::MILLISECONDS);
    }
    const auto begin = std::chrono::steady_clock::now();
    const bool full = !started;
    std::vector<DeploymentState::Change> changes;
    std::map<std::string, std::map<std::string, std::string>> nextKeys = tenantKeys;
    std::set<std::string> nextUndecryptable;
    for (const auto& name : undecryptable) {
      if (!recompute.contains(name.substr(0, name.find('/')))) nextUndecryptable.insert(name);
    }
    SecretOpener open = [&](const std::string& tenant, const std::string& worker, const std::string& secret,
        const ManifestSecret& sealed) -> std::optional<std::string> {
      auto value = key.open(secretAad(tenant, worker, secret), sealed);
      if (!value) nextUndecryptable.insert(secretAad(tenant, worker, secret));
      return value;
    };
    for (const auto& tenant : recompute) {
      std::map<std::string, std::string> keys;
      if (auto manifest = next.find(tenant); manifest != next.end()) keys = tenantStateKeys(manifest->second, definitionBytes, open);
      const auto& previous = tenantKeys[tenant];
      for (const auto& [stateKey, value] : previous)
        if (!keys.contains(stateKey)) changes.push_back({stateKey, std::nullopt});
      for (const auto& [stateKey, value] : keys) {
        auto old = previous.find(stateKey);
        if (old == previous.end() || old->second != value) changes.push_back({stateKey, value});
      }
      if (keys.empty()) nextKeys.erase(tenant); else nextKeys[tenant] = std::move(keys);
    }
    ManifestView view;
    for (const auto& [tenant, manifest] : next) view.emplace(tenant, &manifest);
    size_t conflicts = 0;
    auto routes = routeStateKeys(view, &conflicts);
    for (const auto& [stateKey, value] : routeKeys)
      if (!routes.contains(stateKey)) changes.push_back({stateKey, std::nullopt});
    for (const auto& [stateKey, value] : routes) {
      auto old = routeKeys.find(stateKey);
      if (old == routeKeys.end() || old->second != value) changes.push_back({stateKey, value});
    }
    const auto generation = generationValue(view);
    if (generation != appliedGeneration || full) changes.push_back({"generation", generation});
    // Nothing for the deployment state (a definition tried again in vain): no batch.
    if (started && changes.empty() && revision == current.revision()) co_return;
    DeploymentState::ApplyStats stats;
    try {
      stats = current.apply(changes, full, revision);
      for (const auto& version : stats.definitions) index.remember(version);
      if (full) { servedBundles.clear(); servedPackages.clear(); }
      for (const auto& change : stats.served) {
        if (!full) serve(change.before, -1);
        serve(change.after, 1);
      }
      if (!stats.served.empty() || full) dropUnservedPackages();
    } catch (const kj::Exception& error) {
      if (lastRejectedRevision != revision) ++rejectedRevisions;
      lastRejectedRevision = revision;
      lastRejectedReason = error.getDescription().cStr();
      std::cerr << "[deployment] apply failed revision=" << revision << " reason=" << lastRejectedReason << std::endl;
      throw;
    }
    tenants = std::move(next);
    tenantKeys = std::move(nextKeys);
    routeKeys = std::move(routes);
    appliedGeneration = generation;
    routeConflicts = conflicts;
    undecryptable = std::move(nextUndecryptable);
    if (!changes.empty()) polls.lastChangeAtUnixMs = nowUnixMs();
    // The tenants' records: deletion, log quotas, default limits, hostnames (node/tenants.h).
    if (tenantSink) {
      std::map<std::string, std::optional<TenantRecord>> records;
      for (const auto& [tenant, manifest] : tenants) records.emplace(tenant, manifest.record);
      tenantSink(records, revision, true);
    }
    // Background prefetch of the served code, as after every applied revision.
    std::vector<std::string> bundles;
    bundles.reserve(servedBundles.size());
    for (const auto& [hash, count] : servedBundles) bundles.push_back(hash);
    cache.prefetch(std::move(bundles));
    std::vector<std::shared_ptr<const VersionDefinition>> v2;
    for (const auto& [hash, entry] : servedPackages) v2.push_back(entry.second);
    packagePrefetchPending = v2.size(); ++packagePrefetchEpoch;
    if (!v2.empty()) definitionTasks.add(prefetchPackages(std::move(v2), packagePrefetchEpoch));
    const auto micros = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - begin).count());
    ++applies.count; applies.lastMicros = micros; applies.maxMicros = std::max(applies.maxMicros, micros);
    applies.totalMicros += micros; applies.lastKeys = stats.keys; applies.lastWorkers = stats.workers;
    applies.lastEvaluated = stats.evaluated; applies.lastRoutes = stats.routes; applies.lastFull = full;
    started = true;
    // Secret keys of the batch, by name and length: never a value.
    for (const auto& change : changes) {
      // <tenant>/secrets/<worker>/<name>.
      const auto tenantEnd = change.key.find('/');
      if (tenantEnd == std::string::npos || change.key.compare(tenantEnd, 9, "/secrets/") != 0) continue;
      const auto rest = change.key.substr(tenantEnd + 9);
      const auto slash = rest.find('/');
      if (slash == std::string::npos) continue;
      const auto worker = workerFullId(change.key.substr(0, tenantEnd), rest.substr(0, slash)), name = rest.substr(slash + 1);
      if (!workerFullName(worker) || !environmentName(name)) continue;  // an unknown key, in the status
      const SecretValue* record = nullptr;
      if (auto names = current.secrets().find(worker); names != current.secrets().end()) {
        if (auto found = names->second.find(name); found != names->second.end()) record = &found->second;
      }
      std::cout << "[deployment] secret revision=" << revision << " worker=" << worker << " name=" << name;
      if (!record) std::cout << " state=deleted";
      else if (record->error.empty()) std::cout << " state=set bytes=" << record->value.size();
      else if (undecryptable.contains(worker + "/" + name)) std::cout << " state=undecryptable";
      else std::cout << " state=unusable";
      std::cout << std::endl;
    }
    std::cout << "[deployment] applied revision=" << current.revision() << " generation=" << current.generation()
              << " full=" << (full ? 1 : 0) << " keys=" << stats.keys << " workers=" << stats.workers
              << " evaluated=" << stats.evaluated << " routes=" << stats.routes
              << " valid=" << current.validCount() << '/' << current.workers().size()
              << " tenants=" << tenants.size() << " missingDefinitions=" << missingDefinitions.size()
              << " applyMicros=" << micros << " atUnixMs=" << nowUnixMs() << std::endl;
  }
};
}  // namespace

kj::Own<DesiredState> newObjectStoreDesiredState(const DesiredStateConfig& config, kj::Network& network, kj::Timer& timer) {
  return kj::heap<ObjectStoreDesiredState>(config, network, timer);
}
}  // namespace coworkerd

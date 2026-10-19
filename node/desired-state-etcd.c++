#include "desired-state.h"
#include "coworkerd.capnp.h"
#include "object-store.h"
#include "code-package.h"
#include "bundle-cache.h"

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

class EtcdDesiredState final: public DesiredState, private kj::TaskSet::ErrorHandler {
public:
  EtcdDesiredState(DesiredStateConfig config, kj::Network& network, kj::Timer& timer,
      const kj::HttpHeaderTable& headers)
      : settings(std::move(config)), timer(timer),
        storage(settings.objectStore, network, timer), cache(storage, settings.cache, timer),
        definitionTasks(*this), authority(settings.endpoint, settings.requestTimeoutMs, network, timer, headers),
        watcher(authority, DEPLOYMENT_PREFIX, timer, settings.retryMs,
            EtcdWatcher::ApplyChanges([this](const EtcdBatch& batch) { return apply(batch); })) {
    KJ_REQUIRE(settings.startupTimeoutMs > 0 && settings.retryMs > 0, "deployment timeouts must be positive");
  }
  kj::Promise<void> start() override {
    try {
      co_await timer.timeoutAfter(settings.startupTimeoutMs * kj::MILLISECONDS, startRetry());
    } catch (const kj::Exception& error) {
      KJ_FAIL_REQUIRE("etcd deployment startup failed before membership registration and serving",
          lastRejectedReason.c_str(), error.getDescription());
    }
  }
  kj::Promise<void> run() override { return watcher.run(); }
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
    const auto& watch = watcher.view();
    out << "{\"backend\":\"etcd\",\"revision\":\"" << current.revision()
        << "\",\"generation\":" << current.generation();
    if (full) {
      out << ",\"activeVersions\":" << activeVersionsJson(current, false, STATUS_ACTIVE_LIMIT)
          << ",\"activeVersionDetails\":" << activeVersionsJson(current, true, STATUS_ACTIVE_LIMIT)
          << ",\"activeVersionsTruncated\":" << (current.validCount() > STATUS_ACTIVE_LIMIT ? "true" : "false");
    }
    out << ',' << current.statusFields(STATUS_PROBLEM_LIMIT)
        << ",\"rejectedRevisions\":" << rejectedRevisions << ",\"lastRejectedRevision\":\"" << lastRejectedRevision
        << "\",\"lastRejectedReason\":" << quote(lastRejectedReason)
        << ",\"fullRereads\":" << watch.fullRereads << ",\"compactions\":" << watch.compactions
        << ",\"watchConnected\":" << (watch.connected ? "true" : "false")
        << ",\"watchRevision\":\"" << watch.revision << "\",\"watchLastError\":" << quote(watch.lastError)
        << ",\"apply\":{\"count\":" << applies.count << ",\"lastMicros\":" << applies.lastMicros
        << ",\"maxMicros\":" << applies.maxMicros << ",\"totalMicros\":" << applies.totalMicros
        << ",\"lastKeys\":" << applies.lastKeys << ",\"lastWorkers\":" << applies.lastWorkers
        << ",\"lastEvaluated\":" << applies.lastEvaluated << ",\"lastRoutes\":" << applies.lastRoutes
        << ",\"lastFull\":" << (applies.lastFull ? "true" : "false") << '}'
        << ",\"versionResolutionForTest\":" << quote(settings.versionResolutionForTest)
        << ",\"deploymentApplyDelayMsForTest\":" << settings.deploymentApplyDelayMsForTest << '}';
    return out.str();
  }
  const DesiredStateConfig& config() const override { return settings; }

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
  kj::Promise<DefinitionResult> fetchDefinition(std::string hash) {
    DefinitionResult result;
    try {
      auto response = co_await storage.get("definitions/" + hash);
      if (response.status != 200) {
        result.reason = response.status == 404 ? "定义未知（对象不存在）" : "定义未知（拉取失败）";
      } else {
        auto version = decodeDeploymentDefinition(response.bytes);
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
  EtcdClient authority;
  EtcdWatcher watcher;
  DeploymentState current;
  DefinitionIndex index;
  bool started = false;
  uint64_t rejectedRevisions = 0;
  uint64_t lastRejectedRevision = 0;
  std::string lastRejectedReason;
  // Code of the served Workers, kept current from each batch's served changes
  // so that background prefetch never rescans every Worker.
  std::map<std::string, size_t> servedBundles;
  std::map<std::string, std::pair<size_t, std::shared_ptr<const VersionDefinition>>> servedPackages;
  struct {
    uint64_t count = 0, lastMicros = 0, maxMicros = 0, totalMicros = 0;
    size_t lastKeys = 0, lastWorkers = 0, lastEvaluated = 0, lastRoutes = 0;
    bool lastFull = false;
  } applies;

  kj::Promise<void> startRetry() {
    while (!started) {
      const auto begin = std::chrono::steady_clock::now();
      try { co_await watcher.fullRead(); }
      catch (const kj::Exception& error) { lastRejectedReason = error.getDescription().cStr(); }
      if (started) {
        // The initial read: etcd range (all pages) plus the full evaluation.
        std::cout << "[deployment] startup read micros=" << std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - begin).count() << " workers=" << current.workers().size()
            << " routes=" << current.routes().size() << " definitions=" << current.definitions().size() << std::endl;
      } else {
        co_await timer.afterDelay(settings.retryMs * kj::MILLISECONDS);
      }
    }
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
  kj::Promise<void> apply(const EtcdBatch& batch) {
    if (started && batch.revision <= current.revision()) co_return;
    if (settings.deploymentApplyDelayMsForTest > 0) {
      co_await timer.afterDelay(settings.deploymentApplyDelayMsForTest * kj::MILLISECONDS);
    }
    const auto begin = std::chrono::steady_clock::now();
    DeploymentState::ApplyStats stats;
    std::vector<DeploymentState::Change> changes;
    try {
      // Nothing has ever been deployed (or the endpoint is wrong): do not
      // start serving. A running Server applies whatever the prefix holds.
      KJ_REQUIRE(started || !batch.full || !batch.changes.empty(),
          "deployment state is empty or incomplete: no keys under /coworkerd/v2/deploy/");
      const std::string prefix = DEPLOYMENT_PREFIX;
      changes.reserve(batch.changes.size());
      for (const auto& change : batch.changes) {
        KJ_REQUIRE(change.key.starts_with(prefix), "unexpected deployment key");
        changes.push_back({change.key.substr(prefix.size()),
            change.kv ? std::optional<std::string>(change.kv->value) : std::nullopt});
      }
      stats = current.apply(changes, batch.full, batch.revision);
      for (const auto& version : stats.definitions) index.remember(version);
      if (batch.full) { servedBundles.clear(); servedPackages.clear(); }
      for (const auto& change : stats.served) {
        if (!batch.full) serve(change.before, -1);
        serve(change.after, 1);
      }
      if (!stats.served.empty() || batch.full) dropUnservedPackages();
    } catch (const kj::Exception& error) {
      if (lastRejectedRevision != batch.revision) ++rejectedRevisions;
      lastRejectedRevision = batch.revision;
      lastRejectedReason = error.getDescription().cStr();
      std::cerr << "[deployment] apply failed revision=" << batch.revision << " reason=" << lastRejectedReason << std::endl;
      throw;
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
    applies.lastEvaluated = stats.evaluated; applies.lastRoutes = stats.routes; applies.lastFull = batch.full;
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
      std::cout << "[deployment] secret revision=" << batch.revision << " worker=" << worker << " name=" << name;
      if (!record) std::cout << " state=deleted";
      else if (record->error.empty()) std::cout << " state=set bytes=" << record->value.size();
      else std::cout << " state=unusable";
      std::cout << std::endl;
    }
    std::cout << "[deployment] applied revision=" << current.revision() << " generation=" << current.generation()
              << " full=" << (batch.full ? 1 : 0) << " keys=" << stats.keys << " workers=" << stats.workers
              << " evaluated=" << stats.evaluated << " routes=" << stats.routes
              << " valid=" << current.validCount() << '/' << current.workers().size()
              << " applyMicros=" << micros << " atUnixMs=" << nowUnixMs() << std::endl;
  }
};
}  // namespace

kj::Own<DesiredState> newEtcdDesiredState(const DesiredStateConfig& config,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headers) {
  return kj::heap<EtcdDesiredState>(config, network, timer, headers);
}
}  // namespace coworkerd

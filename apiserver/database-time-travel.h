#pragma once
#include "control.h"
#include <memory>
#include <deque>

namespace coworkerd {

// Immutable history is the receipt. Jobs below are only a bounded memory cache;
// a client persists its operation id and resolved bookmark and resubmits them.
class DatabaseTimeTravel {
public:
  explicit DatabaseTimeTravel(ControlContext& context): context(context) {}
  kj::Promise<ApiAnswer> info(std::string tenant, std::string database,
      const std::map<std::string, std::string>& selector);
  kj::Promise<ApiAnswer> start(std::string tenant, std::string database, std::string body);
  kj::Promise<ApiAnswer> progress(std::string tenant, std::string operationId);
private:
  struct Point {
    std::string namespaceId, objectId, sha256;
    uint64_t revision = 0, writtenAtUnixMs = 0, shardEpoch = 0, bytes = 0;
    std::string bookmark() const;
    std::string json() const;
  };
  struct Intent {
    std::string tenant, sourceDatabase, name, operationId, namespaceId, result;
    Point point;
    bool registering = false, accepted = false;
    uint32_t starting = 0;
    double touchedAtMs = 0;
  };
  struct LookupProgress { uint64_t nextEnd = 0, latestEpoch = 0, touched = 0; };
  struct HistoryCache {
    std::string tenant, database, namespaceId;
    std::optional<Point> lastKnown;
    std::map<uint64_t, Point> points;
    std::deque<uint64_t> pointOrder, ownerOrder;
    std::map<uint64_t, uint64_t> ownerStarts;
    std::map<uint64_t, std::shared_ptr<LookupProgress>> searches;
    uint64_t touched = 0;
  };
  void pruneHistoryCaches();
  std::shared_ptr<HistoryCache> historyCache(const std::string& tenant, const std::string& database, const std::string& ns);
  void expireJobs();
  void remember(const std::string& key, std::shared_ptr<Intent> intent);
  std::string source(const std::string& tenant, const std::string& database) const;
  kj::Promise<std::optional<Point>> read(std::string ns, uint64_t revision, uint32_t& reads);
  kj::Promise<Point> resolve(std::string ns, const std::map<std::string, std::string>& selector,
      const std::string& tenant, const std::string& database);
  kj::Promise<uint64_t> usage(std::string tenant);
  void precheck(const TenantManifest& tenant, const std::string& name, const std::string& ns,
      uint64_t bytes, uint64_t reported) const;
  std::string destination(const Intent& intent) const;
  std::optional<std::string> registered(const Intent& intent) const;
  kj::Promise<std::optional<std::string>> recognize(std::shared_ptr<Intent> intent);
  kj::Promise<ApiAnswer> finish(std::shared_ptr<Intent> intent, std::string manifestSha256, bool verifyContent = false);
  struct NodeAnswer { uint status = 0; std::string body; };
  kj::Promise<NodeAnswer> node(std::string address, bool start, std::string bodyOrNamespace);
  ControlContext& context;
  std::map<std::string, std::shared_ptr<Intent>> jobs;
  std::map<std::string, std::shared_ptr<HistoryCache>> histories;
  uint64_t historyClock = 0;
};

kj::Promise<uint64_t> verifySeedFirstManifest(ObjectStore& store, const std::string& namespaceId,
    const std::string& expectedManifestSha256, bool verifyContent = false);
} // namespace coworkerd

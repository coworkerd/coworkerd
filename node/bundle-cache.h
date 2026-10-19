#pragma once

#include "config.h"
#include "object-store.h"
#include <kj/async.h>

#include <map>
#include <memory>
#include <vector>

namespace coworkerd {

class BundleCache final: private kj::TaskSet::ErrorHandler {
public:
  BundleCache(ObjectStore& storage, BundleCacheConfig config, kj::Timer& timer,
      std::string prefix = "bundles/");
  kj::Promise<std::string> ensure(std::string hash, double remainingMs);
  kj::Promise<std::string> source(std::string hash);
  // The verified bytes of one object, fetched on first use. A cached copy that
  // fails verification is dropped and reported, not fetched again in this call.
  struct Read { std::string bytes; std::string reason; };
  kj::Promise<Read> read(std::string hash, double remainingMs);
  void prefetch(std::vector<std::string> hashes);
  std::string json() const;

private:
  struct Entry { uint64_t bytes; uint64_t used; };
  ObjectStore& storage;
  std::string prefix;
  BundleCacheConfig settings;
  kj::Timer& timer;
  std::map<std::string, Entry> index;
  std::map<std::string, std::unique_ptr<kj::ForkedPromise<std::string>>> flights;
  std::vector<std::string> prefetchHashes;
  uint64_t bytes = 0, sequence = 0, hits = 0, misses = 0, started = 0, succeeded = 0;
  uint64_t merged = 0, evictions = 0, prefetchFailures = 0, prefetchPending = 0;
  uint64_t prefetchEpoch = 0;
  uint32_t prefetchActive = 0;
  std::map<std::string, uint64_t> failures;
  std::string lastError;
  kj::TaskSet tasks;
  void taskFailed(kj::Exception&& error) override;
  std::filesystem::path path(const std::string& hash) const;
  void invalidate(const std::string& hash);
  void makeRoom(uint64_t size);
  std::string fail(const std::string& category, const std::string& reason);
  kj::Promise<std::string> fetch(std::string hash);
  kj::Promise<void> prefetchLoop();
};
}  // namespace coworkerd

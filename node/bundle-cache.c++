#include "reasons.h"
#include "bundle-cache.h"
#include "json.h"

#include <kj/debug.h>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <sstream>

namespace coworkerd {
namespace {
bool validHash(const std::string& hash) {
  return hash.size() == 64 && std::all_of(hash.begin(), hash.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}
std::string readFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  KJ_REQUIRE(input.good(), "cache read failed");
  std::ostringstream output;
  output << input.rdbuf();
  KJ_REQUIRE(input.good() || input.eof(), "cache read failed");
  return output.str();
}
}  // namespace

BundleCache::BundleCache(ObjectStore& storage, BundleCacheConfig config, kj::Timer& timer, std::string prefix)
    : storage(storage), prefix(std::move(prefix)), settings(std::move(config)), timer(timer), tasks(*this) {
  KJ_REQUIRE(!settings.directory.empty() && settings.maxBytes > 0 &&
      settings.prefetchConcurrency > 0 && settings.prefetchConcurrency <= 32, "invalid bundle cache configuration");
  std::filesystem::create_directories(settings.directory);
  for (const auto& file : std::filesystem::directory_iterator(settings.directory)) {
    auto hash = file.path().filename().string();
    if (hash.ends_with(".tmp")) { std::filesystem::remove(file.path()); continue; }
    if (!validHash(hash) || !file.is_regular_file()) continue;
    // Recovered entries are verified before indexing; no object store request.
    auto content = readFile(file.path());
    if (content.size() > settings.maxBytes || objectSha256(content) != hash) {
      std::filesystem::remove(file.path()); continue;
    }
    index.emplace(hash, Entry{content.size(), ++sequence});
    bytes += content.size();
  }
  makeRoom(0);
}

void BundleCache::taskFailed(kj::Exception&&) { lastError = "background cache task failed"; }
std::filesystem::path BundleCache::path(const std::string& hash) const { return settings.directory / hash; }
void BundleCache::invalidate(const std::string& hash) {
  auto found = index.find(hash);
  if (found != index.end()) { bytes -= found->second.bytes; index.erase(found); }
  std::filesystem::remove(path(hash));
}
void BundleCache::makeRoom(uint64_t size) {
  while (bytes > settings.maxBytes - size && !index.empty()) {
    auto oldest = std::min_element(index.begin(), index.end(), [](const auto& a, const auto& b) {
      return a.second.used < b.second.used;
    });
    auto hash = oldest->first;
    invalidate(hash);
    ++evictions;
  }
}
std::string BundleCache::fail(const std::string& category, const std::string& reason) {
  ++failures[category];
  lastError = reason;
  return (coworkerd::reasons::CODE_UNAVAILABLE + ": ") + reason;
}
kj::Promise<std::string> BundleCache::fetch(std::string hash) {
  const auto temporary = path(hash).string() + ".tmp";
  try {
    auto result = co_await storage.get(prefix + hash);
    if (result.status == 404) co_return fail("notFound", "object not found");
    if (result.status != 200) co_return fail("fetchFailed", "fetch failed HTTP " + std::to_string(result.status));
    if (result.bytes.size() > settings.maxBytes) co_return fail("overLimit", "cache size limit exceeded");
    // Staging and commit are synchronous on the event loop: at most one disk
    // staging object exists, even when network downloads overlap.
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    output.write(result.bytes.data(), result.bytes.size());
    output.close();
    KJ_REQUIRE(output.good(), "cache staging write failed");
    auto staged = readFile(temporary);
    if (objectSha256(staged) != hash) {
      std::filesystem::remove(temporary);
      co_return fail("hashMismatch", "content hash mismatch");
    }
    makeRoom(staged.size());
    std::filesystem::rename(temporary, path(hash));
    index.emplace(hash, Entry{staged.size(), ++sequence});
    bytes += staged.size();
    ++succeeded;
    co_return std::string();
  } catch (const kj::Exception& error) {
    std::filesystem::remove(temporary);
    const bool timedOut = error.getType() == kj::Exception::Type::OVERLOADED;
    co_return fail(timedOut ? "fetchTimeout" : "fetchFailed", timedOut ? "fetch timed out" : "fetch failed");
  } catch (const std::exception&) {
    std::filesystem::remove(temporary);
    co_return fail("fetchFailed", "fetch failed (cache I/O)");
  }
}

kj::Promise<std::string> BundleCache::ensure(std::string hash, double remainingMs) {
  KJ_REQUIRE(validHash(hash), "invalid bundle hash");
  auto cached = index.find(hash);
  if (cached != index.end()) {
    cached->second.used = ++sequence;
    ++hits;
    return std::string();
  }
  ++misses;
  auto found = flights.find(hash);
  if (found == flights.end()) {
    ++started;
    auto flight = fetch(hash).then([this, hash](std::string reason) {
      flights.erase(hash);
      return reason;
    }).fork();
    auto inserted = flights.emplace(hash, std::make_unique<kj::ForkedPromise<std::string>>(kj::mv(flight))).first;
    tasks.add(inserted->second->addBranch().ignoreResult());
    found = inserted;
  } else ++merged;
  auto branch = found->second->addBranch();
  auto delay = static_cast<int64_t>(std::max(0.0, std::min(remainingMs, 86400000.0)) * 1000);
  return branch.exclusiveJoin(timer.afterDelay(delay * kj::MICROSECONDS).then([this] {
    return fail("waitTimeout", "fetch timed out (call deadline)");
  }));
}

kj::Promise<BundleCache::Read> BundleCache::read(std::string hash, double remainingMs) {
  auto reason = co_await ensure(hash, remainingMs);
  if (!reason.empty()) co_return Read{{}, std::move(reason)};
  std::string content;
  bool valid = false;
  try {
    content = readFile(path(hash));
    valid = objectSha256(content) == hash;
  } catch (const kj::Exception&) {}
  if (valid) co_return Read{std::move(content), {}};
  // Never serve bytes that failed verification; the next read fetches again.
  invalidate(hash);
  co_return Read{{}, fail("diskInvalidated", "cached content hash mismatch")};
}

kj::Promise<std::string> BundleCache::source(std::string hash) {
  while (true) {
    auto reason = co_await ensure(hash, storage.config().timeoutMs + 1);
    KJ_REQUIRE(reason.empty(), "bundle source unavailable", reason.c_str());
    std::string content;
    bool valid = false;
    try {
      content = readFile(path(hash));
      valid = objectSha256(content) == hash;
    } catch (const kj::Exception&) {}
    if (valid) co_return content;
    // Never return bytes read from disk until the exact buffer being returned
    // has passed SHA-256. Eviction or corruption triggers the same single flight.
    invalidate(hash);
    ++failures["diskInvalidated"];
  }
}
void BundleCache::prefetch(std::vector<std::string> hashes) {
  // A deployment may reuse every bundle, or leave only one cache miss.
  // KJ coroutines start synchronously, so an empty worker cannot consume a
  // concurrency slot: launching must also require queued work.
  std::erase_if(hashes, [this](const auto& hash) { return index.contains(hash); });
  std::sort(hashes.begin(), hashes.end());
  hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
  prefetchHashes = std::move(hashes);
  ++prefetchEpoch;
  prefetchPending = prefetchHashes.size();
  while (prefetchActive < settings.prefetchConcurrency && !prefetchHashes.empty()) {
    ++prefetchActive;
    tasks.add(prefetchLoop());
  }
}
kj::Promise<void> BundleCache::prefetchLoop() {
  while (!prefetchHashes.empty()) {
    const auto epoch = prefetchEpoch;
    auto hash = std::move(prefetchHashes.back());
    prefetchHashes.pop_back();
    uint32_t backoff = 100;
    while (epoch == prefetchEpoch) {
      auto reason = co_await ensure(hash, storage.config().timeoutMs + 1);
      if (epoch != prefetchEpoch) break;
      if (reason.empty()) { --prefetchPending; break; }
      ++prefetchFailures;
      co_await timer.afterDelay(backoff * kj::MILLISECONDS);
      backoff = std::min<uint32_t>(backoff * 2, 5000);
    }
  }
  --prefetchActive;
}
std::string BundleCache::json() const {
  std::ostringstream out;
  out << "{\"backend\":" << json::quote(storage.config().backend) << ",\"entries\":" << index.size()
      << ",\"bytes\":" << bytes << ",\"maxBytes\":" << settings.maxBytes << ",\"hits\":" << hits
      << ",\"misses\":" << misses << ",\"fetchStarted\":" << started << ",\"fetchSucceeded\":" << succeeded
      << ",\"singleFlightMerged\":" << merged << ",\"evictions\":" << evictions
      << ",\"prefetchQueueLength\":" << prefetchPending << ",\"prefetchFailures\":" << prefetchFailures
      << ",\"lastError\":" << json::quote(lastError) << ",\"failures\":{";
  size_t i = 0;
  for (const auto& [reason, count] : failures) { if (i++) out << ','; out << json::quote(reason) << ':' << count; }
  out << "}}";
  return out.str();
}
}  // namespace coworkerd

#include "recovery-history.h"
#include "durable-shards.h"
#include "json.h"
#include "support.h"
#include <kj/debug.h>
#include <openssl/evp.h>
#include <algorithm>
#include <array>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <memory>
#include <tuple>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <system_error>
namespace coworkerd {
using json::quote;
namespace {
// The last reference to a decoded segment hands it to this thread, which frees it. One thread for the process,
// started on first use and never stopped; without it (it could not start) a segment is freed where it is dropped.
class SegmentReleaser {
public:
  static SegmentReleaser& instance() { static auto* releaser = new SegmentReleaser(); return *releaser; }
  void release(const DecodedSegment* segment) {
    if (!started) { delete segment; return; }
    { std::lock_guard<std::mutex> lock(mutex); queue.push_back(segment); }
    wake.notify_one();
  }
  std::atomic<uint64_t> released{0};
private:
  SegmentReleaser() {
    try { std::thread([this]() { loop(); }).detach(); started = true; } catch (const std::system_error&) {}
  }
  void loop() {
    for (;;) {
      const DecodedSegment* next;
      {
        std::unique_lock<std::mutex> lock(mutex);
        wake.wait(lock, [this]() { return !queue.empty(); });
        next = queue.front(); queue.pop_front();
      }
      delete next;
      released.fetch_add(1);
    }
  }
  bool started = false;
  std::mutex mutex;
  std::condition_variable wake;
  std::deque<const DecodedSegment*> queue;
};
std::shared_ptr<DecodedSegment> newDecodedSegment() {
  return std::shared_ptr<DecodedSegment>(new DecodedSegment(),
      [](DecodedSegment* segment) { SegmentReleaser::instance().release(segment); });
}
constexpr uint32_t RECOVERY_SYNC_THREADS = 128;
auto recoverySyncThreads = std::make_shared<std::atomic<uint32_t>>(0);
struct SegmentDecodeResult { std::optional<SegmentContents> contents; std::string error; };
kj::Promise<SegmentDecodeResult> decodeRecoverySegmentAsync(std::string bytes) {
  auto pair = kj::newPromiseAndCrossThreadFulfiller<SegmentDecodeResult>();
  auto fulfiller = std::make_shared<kj::Own<kj::CrossThreadPromiseFulfiller<SegmentDecodeResult>>>(kj::mv(pair.fulfiller));
  try {
    std::thread([bytes = std::move(bytes), fulfiller]() {
      try {
        SegmentDecodeResult result; result.contents = decodeSegment(bytes, &result.error);
        (*fulfiller)->fulfill(kj::mv(result));
      } catch (kj::Exception& error) { (*fulfiller)->reject(kj::mv(error)); }
      catch (const std::exception&) { (*fulfiller)->reject(KJ_EXCEPTION(FAILED, "cannot decode recovery segment")); }
    }).detach();
  } catch (const std::system_error&) { KJ_FAIL_REQUIRE("cannot start recovery segment decoder"); }
  co_return co_await kj::mv(pair.promise);
}
// The digest check, the decode and the grouping by object, on the calling (worker) thread. The encoded copy is
// released there too.
struct GroupedDecode { std::shared_ptr<DecodedSegment> segment; std::string error; uint status = 200; double downloadMs = 0; };
GroupedDecode decodeGrouped(std::string bytes) {
  GroupedDecode result;
  const auto began = monotonicTimeMs();
  auto contents = decodeSegment(bytes, &result.error);
  const auto decoded = monotonicTimeMs();
  if (contents) {
    auto segment = newDecodedSegment();
    segment->encodedBytes = bytes.size();
    segment->header = std::move(contents->header);
    segment->records = contents->records.size();
    for (auto& record : contents->records) {
      segment->bytes += sizeof(LogRecord) + record.frameBytes.size() + record.header.size() +
          record.objectName.size() + record.storageId.size() + record.facetName.size();
      auto group = segment->byObject.find(record.objectName);
      if (group == segment->byObject.end()) {
        group = segment->byObject.emplace(record.objectName, std::vector<LogRecord>()).first;
        segment->bytes += group->first.size() + 64;
      }
      group->second.push_back(std::move(record));
    }
    segment->decodeMs = decoded - began;
    segment->groupMs = monotonicTimeMs() - decoded;
    result.segment = std::move(segment);
  }
  contents.reset();
  std::string().swap(bytes);
  return result;
}
// `fetch` (optional) downloads on the worker thread first; otherwise `bytes` is what the event loop downloaded.
kj::Promise<GroupedDecode> decodeGroupedSegmentAsync(std::string bytes, HistoryBlockingGet fetch = {}, std::string key = {}) {
  auto pair = kj::newPromiseAndCrossThreadFulfiller<GroupedDecode>();
  auto fulfiller = std::make_shared<kj::Own<kj::CrossThreadPromiseFulfiller<GroupedDecode>>>(kj::mv(pair.fulfiller));
  try {
    std::thread([bytes = std::move(bytes), fetch = std::move(fetch), key = std::move(key), fulfiller]() mutable {
      try {
        if (fetch) {
          const auto began = monotonicTimeMs();
          auto response = fetch(key);
          const auto downloadMs = monotonicTimeMs() - began;
          GroupedDecode result;
          if (response.status == 200) result = decodeGrouped(std::move(response.bytes));
          result.status = response.status;
          result.downloadMs = downloadMs;
          (*fulfiller)->fulfill(kj::mv(result));
        } else {
          (*fulfiller)->fulfill(decodeGrouped(std::move(bytes)));
        }
      } catch (kj::Exception& error) { (*fulfiller)->reject(kj::mv(error)); }
      catch (const std::exception&) { (*fulfiller)->reject(KJ_EXCEPTION(FAILED, "cannot decode recovery segment")); }
    }).detach();
  } catch (const std::system_error&) { KJ_FAIL_REQUIRE("cannot start recovery segment decoder"); }
  co_return co_await kj::mv(pair.promise);
}
} // namespace
uint64_t segmentsReleasedOffLoop() { return SegmentReleaser::instance().released.load(); }
kj::Promise<bool> synchronizeRecoveryFile(int fd) {
  auto counter = recoverySyncThreads;
  if (counter->fetch_add(1) >= RECOVERY_SYNC_THREADS) { counter->fetch_sub(1); co_return false; }
  auto permit = std::shared_ptr<int>(new int(0), [counter](int* value) { delete value; counter->fetch_sub(1); });
  const int copy = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
  if (copy < 0) co_return false;
  auto file = std::shared_ptr<int>(new int(copy), [](int* fd) { ::close(*fd); delete fd; });
  auto pair = kj::newPromiseAndCrossThreadFulfiller<bool>();
  auto fulfiller = std::make_shared<kj::Own<kj::CrossThreadPromiseFulfiller<bool>>>(kj::mv(pair.fulfiller));
  try {
    std::thread([file, fulfiller, permit]() {
      const bool synced = ::fsync(*file) == 0;
      (*fulfiller)->fulfill(bool(synced));
    }).detach();
  } catch (const std::system_error&) { co_return false; }
  permit.reset();
  co_return co_await kj::mv(pair.promise);
}
void HistoryBudget::memory(uint64_t required) {
  memoryHighWater = std::max(memoryHighWater, required);
  if (required > memoryLimit) { if (refuse) refuse(required, memoryLimit, "memoryBytes"); KJ_FAIL_REQUIRE("recovery memory budget exceeded", required, memoryLimit); }
}
void HistoryBudget::transient(uint64_t required) {
  transientHighWater = std::max(transientHighWater, required);
  if (required > transientLimit) { if (refuse) refuse(required, transientLimit, "segmentDecodeBytes"); KJ_FAIL_REQUIRE("shared segment decode budget exceeded", required, transientLimit); }
  memory(payloadBytes + records * (sizeof(LogRecord) * 2 + 192) + required + (16ull << 20));
}
void HistoryBudget::add(const LogRecord& record) {
  const uint64_t bytes = record.frameBytes.size() + record.header.size() + record.objectName.size() + record.storageId.size() + record.facetName.size();
  if (++records > recordLimit) { if (refuse) refuse(records, recordLimit, "records"); KJ_FAIL_REQUIRE("recovery record limit exceeded", records, recordLimit); }
  payloadBytes += bytes;
  if (payloadBytes > payloadLimit) { if (refuse) refuse(payloadBytes, payloadLimit, "payloadBytes"); KJ_FAIL_REQUIRE("recovery payload limit exceeded", payloadBytes, payloadLimit); }
  memory(payloadBytes + records * (sizeof(LogRecord) * 2 + 192) + transientHighWater + (16ull << 20));
}
FileDigest recoveryFileDigest(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY); KJ_REQUIRE(fd >= 0, "cannot read recovery image");
  auto close = kj::defer([&]() { ::close(fd); });
  auto* digest = EVP_MD_CTX_new(); KJ_REQUIRE(digest && EVP_DigestInit_ex(digest, EVP_sha256(), nullptr) == 1, "cannot hash recovery image");
  auto release = kj::defer([&]() { EVP_MD_CTX_free(digest); });
  std::array<unsigned char, 65536> block; FileDigest result;
  for (;;) { auto n = ::read(fd, block.data(), block.size()); if (n < 0 && errno == EINTR) continue; KJ_REQUIRE(n >= 0, "cannot read recovery image"); if (!n) break;
    result.bytes += n; KJ_REQUIRE(EVP_DigestUpdate(digest, block.data(), size_t(n)) == 1, "cannot hash recovery image"); }
  unsigned char bytes[32]; unsigned int length; KJ_REQUIRE(EVP_DigestFinal_ex(digest, bytes, &length) == 1 && length == 32, "cannot finish image digest");
  constexpr char HEX[] = "0123456789abcdef"; for (auto byte : bytes) { result.sha256 += HEX[byte >> 4]; result.sha256 += HEX[byte & 15]; } return result;
}
kj::Promise<FileDigest> recoveryFileDigestAsync(const std::filesystem::path& path, HistoryYield yield, HistoryCheck check) {
  if (check) check();
  const int fd = ::open(path.c_str(), O_RDONLY); KJ_REQUIRE(fd >= 0, "cannot read recovery image");
  auto close = kj::defer([&]() { ::close(fd); });
  auto* digest = EVP_MD_CTX_new(); KJ_REQUIRE(digest && EVP_DigestInit_ex(digest, EVP_sha256(), nullptr) == 1, "cannot hash recovery image");
  auto release = kj::defer([&]() { EVP_MD_CTX_free(digest); });
  std::array<unsigned char, 65536> block; FileDigest result;
  auto began = monotonicTimeMs();
  for (;;) { auto n = ::read(fd, block.data(), block.size()); if (n < 0 && errno == EINTR) continue; KJ_REQUIRE(n >= 0, "cannot read recovery image"); if (!n) break;
    result.bytes += n; KJ_REQUIRE(EVP_DigestUpdate(digest, block.data(), size_t(n)) == 1, "cannot hash recovery image");
    if (yield && monotonicTimeMs() - began >= 2) {
      co_await yield(); if (check) check(); began = monotonicTimeMs();
    }
  }
  if (check) check();
  unsigned char bytes[32]; unsigned int length; KJ_REQUIRE(EVP_DigestFinal_ex(digest, bytes, &length) == 1 && length == 32, "cannot finish image digest");
  constexpr char HEX[] = "0123456789abcdef"; for (auto byte : bytes) { result.sha256 += HEX[byte >> 4]; result.sha256 += HEX[byte & 15]; } co_return result;
}
kj::Promise<std::vector<LogRecord>> readRecoverySegments(HistoryGet get, const ObjectManifest& manifest,
    const std::string& objectName, uint32_t parallelism, HistoryCheck check, HistoryLog log, HistoryBudget* budget, HistoryYield yield,
    SegmentReadStats* stats) {
  if (budget) parallelism = 1;
  std::vector<LogRecord> out;
  std::map<uint64_t, std::optional<ShardEpoch>> owners;
  const std::set<std::pair<uint64_t, uint64_t>> exclusions(manifest.isolated.begin(), manifest.isolated.end());
  std::vector<ManifestSegment> wanted = manifest.segments;
  std::sort(wanted.begin(), wanted.end(), [](const ManifestSegment& a, const ManifestSegment& b) {
    return std::tie(a.shardEpoch, a.nodeEpoch, a.segment) < std::tie(b.shardEpoch, b.nodeEpoch, b.segment);
  });
  for (size_t at = 0; at < wanted.size();) {
    kj::Vector<kj::Promise<std::string>> running;
    const size_t until = std::min<size_t>(wanted.size(), at + parallelism);
    for (size_t i = at; i < until; ++i) {
      const auto key = nodeSegmentKey(wanted[i].nodeId, wanted[i].nodeEpoch, wanted[i].segment);
      running.add(get(key).then([key](ObjectResponse response) {
        KJ_REQUIRE(response.status == 200, "a segment of a Durable Object's log is not on the store", key.c_str(),
            response.status);
        return std::move(response.bytes);
      }));
    }
    const auto downloadBegan = monotonicTimeMs();
    auto bodies = co_await kj::joinPromises(running.releaseAsArray());
    if (stats) { stats->downloadMs += monotonicTimeMs() - downloadBegan; stats->waitMs += monotonicTimeMs() - downloadBegan; }
    if (check) check();
    for (size_t i = 0; i < bodies.size(); ++i) {
      if (budget) budget->transient(bodies[i].size() * 6);
      if (stats) { ++stats->segments; stats->bytes += bodies[i].size(); ++stats->cacheMisses; }
      std::string error;
      std::optional<SegmentContents> contents;
      const auto decodeBegan = monotonicTimeMs();
      if (yield) {
        // The seed's existing one-segment budget covers the worker's owned input
        // and decoded records. Ordinary recovery keeps its original reader.
        auto decoded = co_await decodeRecoverySegmentAsync(std::move(bodies[i]));
        if (check) check();
        contents = std::move(decoded.contents); error = std::move(decoded.error);
      } else contents = decodeSegment(bodies[i], &error);
      if (stats) { stats->decodeMs += monotonicTimeMs() - decodeBegan; stats->waitMs += monotonicTimeMs() - decodeBegan; }
      KJ_REQUIRE(contents.has_value(), "a segment of a Durable Object's log does not add up", error.c_str());
      const auto selectBegan = monotonicTimeMs();
      auto began = monotonicTimeMs();
      for (auto& record : contents->records) {
        if (yield && monotonicTimeMs() - began >= 2) { co_await yield(); if (check) check(); began = monotonicTimeMs(); }
        if (record.objectName != objectName) continue;
        const auto& header = contents->header;
        const auto& ref = wanted[at + i];
        if (exclusions.contains({record.shardEpoch, record.nodeSeq}) && header.reason != "earlier-epoch" &&
            header.reason != "recovery-tail" && header.nodeId == ref.nodeId &&
            header.nodeEpoch == ref.nodeEpoch && header.segment == ref.segment) {
          auto [owner, fresh] = owners.try_emplace(record.shardEpoch);
          if (fresh) {
            auto response = co_await get(shardEpochKey(manifest.namespaceId, record.shard, record.shardEpoch));
            if (check) check();
            requireStorage(response.status == 200, "an excluded record's original shard owner is unavailable");
            owner->second = parseShardEpoch(response.bytes);
            requireStorage(owner->second && owner->second->namespaceId == manifest.namespaceId &&
                owner->second->shard == record.shard && owner->second->epoch == record.shardEpoch,
                "an excluded record's original shard owner is invalid");
          }
          record.ownerSealed = owner->second && owner->second->nodeId == header.nodeId &&
              owner->second->nodeEpoch == header.nodeEpoch && record.nodeEpoch == header.nodeEpoch;
          if (record.ownerSealed) if (log) log("foreign-isolation-ignored", "\"object\":" + quote(objectName) +
              ",\"shardEpoch\":\"" + std::to_string(record.shardEpoch) + "\",\"nodeSeq\":\"" +
              std::to_string(record.nodeSeq) + "\"");
        }
        if (budget) budget->add(record);
        if (stats) ++stats->records;
        out.push_back(std::move(record));
      }
      if (stats) stats->selectMs += monotonicTimeMs() - selectBegan;
    }
    at = until;
  }
  co_return out;
}
SegmentDecodeCache::SegmentDecodeCache(HistoryGet get, uint64_t limitBytes, double idleMs, Decoded decoded,
    HistoryBlockingGet blockingGet, uint32_t maxLoadsParam)
    : get(std::move(get)), blockingGet(std::move(blockingGet)), limit(limitBytes), idle(idleMs),
      decodedHook(std::move(decoded)), maxLoads(std::max<uint32_t>(1, maxLoadsParam)) {}
void SegmentDecodeCache::taskFailed(kj::Exception&&) {}
kj::Promise<std::shared_ptr<const DecodedSegment>> SegmentDecodeCache::load(std::string key) {
  // At most maxLoads loads at once. A finishing load wakes every waiting one and each checks again, so a waiter that
  // is gone never holds a slot.
  while (loading >= maxLoads) {
    ++loadWaits;
    auto turn = kj::newPromiseAndFulfiller<void>();
    waiting.push_back(kj::mv(turn.fulfiller));
    co_await turn.promise;
  }
  ++loading;
  KJ_DEFER({
    --loading;
    auto woken = std::move(waiting);
    waiting.clear();
    for (auto& next : woken) if (next->isWaiting()) next->fulfill();
  });
  GroupedDecode decoded;
  if (blockingGet) {
    // Download, digest, decode and grouping all on one worker thread.
    decoded = co_await decodeGroupedSegmentAsync({}, blockingGet, key);
    KJ_REQUIRE(decoded.status == 200, "a segment of a Durable Object's log is not on the store", key.c_str(),
        decoded.status);
  } else {
    const auto began = monotonicTimeMs();
    auto response = co_await get(key);
    KJ_REQUIRE(response.status == 200, "a segment of a Durable Object's log is not on the store", key.c_str(),
        response.status);
    const auto downloadMs = monotonicTimeMs() - began;
    decoded = co_await decodeGroupedSegmentAsync(std::move(response.bytes));
    decoded.downloadMs = downloadMs;
  }
  KJ_REQUIRE(decoded.segment != nullptr, "a segment of a Durable Object's log does not add up", decoded.error.c_str());
  decoded.segment->downloadMs = decoded.downloadMs;
  ++decodes;
  if (decodedHook) decodedHook(key, *decoded.segment);
  co_return std::shared_ptr<const DecodedSegment>(std::move(decoded.segment));
}
kj::Promise<std::shared_ptr<const DecodedSegment>> SegmentDecodeCache::segment(const ManifestSegment& ref,
    SegmentSource* source) {
  auto storeKey = nodeSegmentKey(ref.nodeId, ref.nodeEpoch, ref.segment);
  auto key = keyForTest ? keyForTest(ref) : storeKey;
  const auto now = monotonicTimeMs();
  auto entry = table.find(key);
  if (entry != table.end() && entry->second.ready) {
    ++hits;
    if (source) *source = SegmentSource::HIT;
    entry->second.lastUsedMs = now;
    return std::shared_ptr<const DecodedSegment>(entry->second.ready);
  }
  if (entry == table.end()) {
    ++misses;
    if (source) *source = SegmentSource::LOAD;
    auto forked = load(std::move(storeKey)).fork();
    // Finish the entry even when every reader that waited for it was canceled: its bytes are counted once, and a
    // failure is not kept.
    tasks.add(forked.addBranch().then([this, key](std::shared_ptr<const DecodedSegment> value) {
      auto at = table.find(key);
      if (at == table.end()) return;
      at->second.ready = std::move(value);
      at->second.pending.reset();
      // Just decoded counts as just used: the readers that waited read it now, and a load can take longer than
      // the idle time or than the gap to the next batch of rebuilds that need the segment.
      at->second.lastUsedMs = monotonicTimeMs();
      held += at->second.ready->bytes;
      trim();
    }, [this, key](kj::Exception&&) {
      ++failures;
      table.erase(key);
    }));
    entry = table.emplace(key, Entry{}).first;
    entry->second.pending.emplace(kj::mv(forked));
  } else {
    ++joins;
    if (source) *source = SegmentSource::JOIN;
  }
  entry->second.lastUsedMs = now;
  return entry->second.pending->addBranch();
}
void SegmentDecodeCache::trim() {
  while (held > limit) {
    auto victim = table.end();
    for (auto at = table.begin(); at != table.end(); ++at)
      if (at->second.ready && (victim == table.end() || at->second.lastUsedMs < victim->second.lastUsedMs)) victim = at;
    if (victim == table.end()) break;
    held -= std::min(held, victim->second.ready->bytes);
    table.erase(victim);
    ++evictions;
  }
}
bool SegmentDecodeCache::sweep() {
  const auto now = monotonicTimeMs();
  for (auto at = table.begin(); at != table.end();) {
    if (at->second.ready && now - at->second.lastUsedMs >= idle) {
      held -= std::min(held, at->second.ready->bytes);
      at = table.erase(at);
      ++evictions;
    } else {
      ++at;
    }
  }
  return !table.empty();
}
kj::Promise<std::vector<LogRecord>> readRecoverySegmentsDecoded(SegmentDecodeCache& cache, HistoryGet get,
    const ObjectManifest& manifest, const std::string& objectName, uint32_t parallelism, HistoryLog log,
    SegmentReadStats* stats) {
  std::vector<LogRecord> out;
  std::map<uint64_t, std::optional<ShardEpoch>> owners;
  const std::set<std::pair<uint64_t, uint64_t>> exclusions(manifest.isolated.begin(), manifest.isolated.end());
  std::vector<ManifestSegment> wanted = manifest.segments;
  std::sort(wanted.begin(), wanted.end(), [](const ManifestSegment& a, const ManifestSegment& b) {
    return std::tie(a.shardEpoch, a.nodeEpoch, a.segment) < std::tie(b.shardEpoch, b.nodeEpoch, b.segment);
  });
  const size_t width = std::max<uint32_t>(1, parallelism);
  for (size_t at = 0; at < wanted.size();) {
    kj::Vector<kj::Promise<std::shared_ptr<const DecodedSegment>>> running;
    const size_t until = std::min<size_t>(wanted.size(), at + width);
    auto sources = std::make_unique<SegmentSource[]>(until - at);
    for (size_t i = at; i < until; ++i) running.add(cache.segment(wanted[i], &sources[i - at]));
    const auto waitBegan = monotonicTimeMs();
    auto segments = co_await kj::joinPromises(running.releaseAsArray());
    if (stats) stats->waitMs += monotonicTimeMs() - waitBegan;
    for (size_t i = 0; i < segments.size(); ++i) {
      const auto& contents = *segments[i];
      if (stats) {
        ++stats->segments;
        if (sources[i] == SegmentSource::LOAD) {
          ++stats->cacheMisses; stats->bytes += contents.encodedBytes;
          stats->downloadMs += contents.downloadMs; stats->decodeMs += contents.decodeMs; stats->groupMs += contents.groupMs;
        } else if (sources[i] == SegmentSource::JOIN) {
          ++stats->cacheJoins;
        } else {
          ++stats->cacheHits;
        }
      }
      const auto selectBegan = monotonicTimeMs();
      auto group = contents.byObject.find(objectName);
      if (group != contents.byObject.end()) {
        for (const auto& kept : group->second) {
          LogRecord record = kept;
          const auto& header = contents.header;
          const auto& ref = wanted[at + i];
          if (exclusions.contains({record.shardEpoch, record.nodeSeq}) && header.reason != "earlier-epoch" &&
              header.reason != "recovery-tail" && header.nodeId == ref.nodeId &&
              header.nodeEpoch == ref.nodeEpoch && header.segment == ref.segment) {
            auto [owner, fresh] = owners.try_emplace(record.shardEpoch);
            if (fresh) {
              auto response = co_await get(shardEpochKey(manifest.namespaceId, record.shard, record.shardEpoch));
              requireStorage(response.status == 200, "an excluded record's original shard owner is unavailable");
              owner->second = parseShardEpoch(response.bytes);
              requireStorage(owner->second && owner->second->namespaceId == manifest.namespaceId &&
                  owner->second->shard == record.shard && owner->second->epoch == record.shardEpoch,
                  "an excluded record's original shard owner is invalid");
            }
            record.ownerSealed = owner->second && owner->second->nodeId == header.nodeId &&
                owner->second->nodeEpoch == header.nodeEpoch && record.nodeEpoch == header.nodeEpoch;
            if (record.ownerSealed) if (log) log("foreign-isolation-ignored", "\"object\":" + quote(objectName) +
                ",\"shardEpoch\":\"" + std::to_string(record.shardEpoch) + "\",\"nodeSeq\":\"" +
                std::to_string(record.nodeSeq) + "\"");
          }
          if (stats) ++stats->records;
          out.push_back(std::move(record));
        }
      }
      if (stats) stats->selectMs += monotonicTimeMs() - selectBegan;
    }
    at = until;
  }
  co_return out;
}
kj::Promise<void> downloadRecoverySnapshot(HistoryGet get, const ObjectManifest& manifest, const RootStorageRef& ref, const std::filesystem::path& stack,
    const std::string& staging, uint64_t partBytes, uint32_t parallelism, HistoryCheck check, HistoryYield yield, std::function<void(double)> observeFsync) {
  for (const auto& facet : manifest.facets) {
    const auto path = ref.databasePath(stack, facet.facetId, "", staging);
    std::error_code code;
    std::filesystem::create_directories(path.parent_path(), code);
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    KJ_REQUIRE(fd >= 0, "cannot write a Durable Object database", path.string().c_str());
    const auto close = kj::defer([fd]() { ::close(fd); });
    kj::Vector<kj::Promise<void>> running;
    for (uint64_t part = 0; part < facet.parts; ++part) {
      running.add(downloadRecoveryPart(get, manifest, facet.facetId, part, fd, off_t(part * partBytes), check));
      if (running.size() >= parallelism) {
        co_await kj::joinPromises(running.releaseAsArray());
        if (check) check();
        running.clear();
      }
    }
    if (running.size() > 0) co_await kj::joinPromises(running.releaseAsArray());
        if (check) check();
    const auto syncStarted = monotonicTimeMs();
    const auto synced = co_await synchronizeRecoveryFile(fd);
    if (observeFsync) observeFsync(monotonicTimeMs() - syncStarted);
    if (check) check();
    KJ_REQUIRE(synced, "cannot synchronize a Durable Object snapshot", path.string().c_str());
    // The file must be the one the snapshot names: a wrong digest would be read as the object's state.
    const auto bytes = co_await recoveryFileDigestAsync(path, yield, check);
    KJ_REQUIRE(bytes.bytes == facet.bytes && bytes.sha256 == facet.sha256,
        "a snapshot of a Durable Object does not add up", path.string().c_str(), facet.bytes, bytes.bytes);
  }
  co_return;
}

kj::Promise<void> downloadRecoveryPart(HistoryGet get, const ObjectManifest& manifest, uint32_t facetId, uint64_t part,
    int fd, off_t offset, HistoryCheck check) {
  const auto key = objectSnapshotPartKey(manifest.namespaceId, manifest.objectId, manifest.snapshotSequence, facetId,
      part);
  auto object = co_await get(key);
  if (check) check();
  KJ_REQUIRE(object.status == 200, "a part of a Durable Object's snapshot is not on the store", key.c_str(),
      object.status);
  size_t written = 0;
  while (written < object.bytes.size()) {
    auto wrote = ::pwrite(fd, object.bytes.data() + written, object.bytes.size() - written, offset + off_t(written));
    if (wrote < 0 && errno == EINTR) continue;
    KJ_REQUIRE(wrote > 0, "cannot write a part of a Durable Object's snapshot");
    written += size_t(wrote);
  }
  co_return;
}


} // namespace coworkerd

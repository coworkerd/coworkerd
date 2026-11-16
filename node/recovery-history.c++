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
#include <system_error>
namespace coworkerd {
using json::quote;
namespace {
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
} // namespace
kj::Promise<bool> synchronizeRecoveryFile(int fd) {
  const int copy = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
  if (copy < 0) co_return false;
  auto file = std::shared_ptr<int>(new int(copy), [](int* fd) { ::close(*fd); delete fd; });
  auto pair = kj::newPromiseAndCrossThreadFulfiller<bool>();
  auto fulfiller = std::make_shared<kj::Own<kj::CrossThreadPromiseFulfiller<bool>>>(kj::mv(pair.fulfiller));
  try {
    std::thread([file, fulfiller]() {
      const bool synced = ::fsync(*file) == 0;
      (*fulfiller)->fulfill(bool(synced));
    }).detach();
  } catch (const std::system_error&) { co_return false; }
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
    const std::string& objectName, uint32_t parallelism, HistoryCheck check, HistoryLog log, HistoryBudget* budget, HistoryYield yield) {
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
    auto bodies = co_await kj::joinPromises(running.releaseAsArray());
    if (check) check();
    for (size_t i = 0; i < bodies.size(); ++i) {
      if (budget) budget->transient(bodies[i].size() * 6);
      std::string error;
      std::optional<SegmentContents> contents;
      if (yield) {
        // The seed's existing one-segment budget covers the worker's owned input
        // and decoded records. Ordinary recovery keeps its original reader.
        auto decoded = co_await decodeRecoverySegmentAsync(std::move(bodies[i]));
        if (check) check();
        contents = std::move(decoded.contents); error = std::move(decoded.error);
      } else contents = decodeSegment(bodies[i], &error);
      KJ_REQUIRE(contents.has_value(), "a segment of a Durable Object's log does not add up", error.c_str());
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
        out.push_back(std::move(record));
      }
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

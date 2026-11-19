#pragma once

#include "durable-log.h"
#include <functional>

namespace coworkerd {

// The history reader has only GET capability. Its identity always names the
// source; the RootStorageRef passed to the writer always names the destination.
using HistoryGet = std::function<kj::Promise<ObjectResponse>(std::string)>;
using HistoryYield = std::function<kj::Promise<void>()>;
using HistoryCheck = std::function<void()>;
using HistoryLog = std::function<void(const std::string&, const std::string&)>;
// A GET that runs to completion on the calling worker thread (with an event loop and connection of its own), so a
// download does not touch the node's event loop.
using HistoryBlockingGet = std::function<ObjectResponse(const std::string&)>;
struct HistoryBudget {
  uint64_t payloadLimit = 128ull << 20, recordLimit = 100000, memoryLimit = 512ull << 20;
  uint64_t transientLimit = 384ull << 20, transientHighWater = 0;
  void transient(uint64_t required);
  uint64_t payloadBytes = 0, records = 0, memoryHighWater = 0;
  std::function<void(uint64_t, uint64_t, const std::string&)> refuse;
  void memory(uint64_t required);
  void add(const LogRecord& record);
};

// Where reading a manifest's segments spent its time, for the timing a rebuild logs. `downloadMs` and `decodeMs`
// are this read's own work; `waitMs` is the time it waited for segment contents in all.
struct SegmentReadStats {
  uint64_t segments = 0, bytes = 0, records = 0, cacheHits = 0, cacheMisses = 0, cacheJoins = 0;
  double downloadMs = 0, decodeMs = 0, groupMs = 0, waitMs = 0, selectMs = 0;
};

struct FileDigest { uint64_t bytes = 0; std::string sha256; };
// Owns a duplicate until the worker finishes, even if its waiter is canceled.
kj::Promise<bool> synchronizeRecoveryFile(int fd);
FileDigest recoveryFileDigest(const std::filesystem::path& path);
kj::Promise<FileDigest> recoveryFileDigestAsync(const std::filesystem::path& path, HistoryYield yield, HistoryCheck check = {});
kj::Promise<std::vector<LogRecord>> readRecoverySegments(HistoryGet get, const ObjectManifest& manifest,
    const std::string& objectName, uint32_t parallelism, HistoryCheck check = {}, HistoryLog log = {},
    HistoryBudget* budget = nullptr, HistoryYield yield = {}, SegmentReadStats* stats = nullptr);
// One segment of the store, downloaded once, its digest and records checked and decoded on a worker thread, and its
// records grouped by object in segment order. Shared read-only by every rebuild that names the segment.
struct DecodedSegment {
  SegmentHeader header;
  std::map<std::string, std::vector<LogRecord>, std::less<>> byObject;
  uint64_t encodedBytes = 0, bytes = 0, records = 0;
  double downloadMs = 0, decodeMs = 0, groupMs = 0;
};
// The segments a node's rebuilds read, each downloaded and decoded once while rebuilds still need it, instead of once
// per object. Readers waiting for the same segment share one download and one decode. An entry is dropped when no
// read has used it for `idleMs` (see sweep()), and the least recently used go first when the decoded records held
// pass `limitBytes`. A failed download or decode is not kept, so the next read tries again. A segment never changes
// once written (a second write of the same key is refused), so a held entry cannot be stale.
class SegmentDecodeCache final: private kj::TaskSet::ErrorHandler {
public:
  using Decoded = std::function<void(const std::string& key, const DecodedSegment& segment)>;
  // With `blockingGet`, the download runs on the decoding worker thread too; without it, on the event loop.
  SegmentDecodeCache(HistoryGet get, uint64_t limitBytes, double idleMs, Decoded decoded = {},
      HistoryBlockingGet blockingGet = {});
  // `loaded` (optional) is set when this call started the download and decode.
  kj::Promise<std::shared_ptr<const DecodedSegment>> segment(const ManifestSegment& ref, bool* loaded = nullptr);
  // Drops the entries no read has used for idleMs; whether any entry is left.
  bool sweep();
  uint64_t heldBytes() const { return held; }
  uint64_t limitBytes() const { return limit; }
  size_t entries() const { return table.size(); }
  uint64_t decodes = 0, hits = 0, misses = 0, joins = 0, evictions = 0, failures = 0;
private:
  struct Entry {
    std::optional<kj::ForkedPromise<std::shared_ptr<const DecodedSegment>>> pending;
    std::shared_ptr<const DecodedSegment> ready;
    double lastUsedMs = 0;
  };
  void taskFailed(kj::Exception&& exception) override;
  kj::Promise<std::shared_ptr<const DecodedSegment>> load(std::string key);
  void trim();
  HistoryGet get;
  HistoryBlockingGet blockingGet;
  uint64_t limit, held = 0;
  double idle;
  Decoded decodedHook;
  std::map<std::string, Entry> table;
  kj::TaskSet tasks{*this};
};
// readRecoverySegments' records for one object -- the same records, in the same order, with the same exclusion
// evidence and the same errors -- with each segment taken from `cache` instead of downloaded and decoded per object.
kj::Promise<std::vector<LogRecord>> readRecoverySegmentsDecoded(SegmentDecodeCache& cache, HistoryGet get,
    const ObjectManifest& manifest, const std::string& objectName, uint32_t parallelism, HistoryLog log = {},
    SegmentReadStats* stats = nullptr);
kj::Promise<void> downloadRecoveryPart(HistoryGet get, const ObjectManifest& manifest,
    uint32_t facetId, uint64_t part, int fd, off_t offset, HistoryCheck check = {});
kj::Promise<void> downloadRecoverySnapshot(HistoryGet get, const ObjectManifest& manifest,
    const RootStorageRef& destination, const std::filesystem::path& stack, const std::string& staging,
    uint64_t partBytes, uint32_t parallelism, HistoryCheck check = {}, HistoryYield yield = {},
    std::function<void(double)> observeFsync = {});

}  // namespace coworkerd

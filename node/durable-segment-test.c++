// b57: reading an object's segments through the shared decode cache gives each object exactly what the old
// per-object reader gave it -- the same records, order, exclusion evidence, log lines and errors -- while each
// segment is downloaded and decoded once. The negative controls check that these assertions catch a wrong answer.
#include "durable-log.h"
#include "durable-shards.h"
#include "recovery-history.h"
#include <kj/async-io.h>
#include <algorithm>
#include <cstdlib>
#include <random>
#include <unistd.h>

namespace coworkerd {
namespace {

enum class SegmentControl { NONE, DROP_RECORD, SWAP_RECORDS, CLEAR_OWNER_SEALED, LEAK_OTHER_OBJECT, DECODE_PER_READ,
    REVERSE_SEGMENTS };

struct Fixture {
  std::string ns = std::string(64, 'a');
  std::vector<std::string> objects;
  std::vector<ManifestSegment> segments;   // every segment written
  std::map<std::string, ObjectManifest> manifests;
};

std::string objectName(const std::string& ns, unsigned n) { return ns + "/" + std::string(62, '0') + char('a' + n / 10) + char('0' + n % 10); }

// Segments from two nodes and three node epochs, ordinary, earlier-epoch and recovery-tail, with records of several
// objects, shard epochs and node epochs; each object's manifest names the segments in a shuffled order (the reader
// sorts them), one segment without any of its records, and excluded positions both owned and not owned by the
// segment's writer.
kj::Promise<Fixture> prepare(ObjectStore& store, std::mt19937& random) {
  Fixture fixture;
  const unsigned objectCount = 5 + random() % 4;
  for (unsigned i = 0; i < objectCount; ++i) fixture.objects.push_back(objectName(fixture.ns, i));
  struct Writer { std::string nodeId; uint64_t nodeEpoch; };
  const std::vector<Writer> writers{{"node-a", 1}, {"node-b", 1}, {"node-a", 2}};
  const std::vector<std::string> reasons{"", "earlier-epoch", "recovery-tail", ""};
  // The owner of each shard epoch: node-a/1 for 1 and 3, node-b/1 for 2, node-a/2 for 4.
  const std::map<uint64_t, Writer> owners{{1, {"node-a", 1}}, {2, {"node-b", 1}}, {3, {"node-a", 1}}, {4, {"node-a", 2}}};
  for (const auto& [epoch, owner] : owners) {
    ShardEpoch record;
    record.namespaceId = fixture.ns; record.shard = 0; record.epoch = epoch;
    record.nodeId = owner.nodeId; record.nodeEpoch = owner.nodeEpoch; record.dataEpoch = epoch; record.reason = "first";
    auto written = co_await store.put(shardEpochKey(fixture.ns, 0, epoch), shardEpochJson(record));
    KJ_REQUIRE(written.status / 100 == 2, "cannot write a shard epoch");
  }
  std::map<std::string, std::vector<std::pair<uint64_t, uint64_t>>> positions;
  uint64_t sequence = 1;
  const unsigned segmentCount = 5 + random() % 3;
  for (unsigned s = 0; s < segmentCount; ++s) {
    const auto& writer = writers[s % writers.size()];
    SegmentHeader header;
    header.nodeId = writer.nodeId; header.nodeEpoch = writer.nodeEpoch; header.segment = 1 + s / writers.size();
    header.reason = reasons[random() % reasons.size()];
    header.firstSeq = sequence;
    std::vector<std::string> encoded;
    std::map<std::string, SegmentIndexEntry> index;
    const unsigned count = 20 + random() % 120;
    const unsigned skipped = random() % objectCount;  // one object is absent from this segment
    for (unsigned r = 0; r < count; ++r) {
      unsigned obj = random() % objectCount;
      if (obj == skipped) obj = (obj + 1) % objectCount;
      LogRecord record;
      record.kind = (r % 3) ? LogRecordKind::RESET : LogRecordKind::DELETED;
      record.nodeSeq = sequence++;
      record.nodeEpoch = (random() % 7 == 0) ? writer.nodeEpoch + 1 : writer.nodeEpoch;
      record.shardEpoch = 1 + random() % 4;
      record.shard = 0;
      record.objectName = fixture.objects[obj];
      record.storageId = std::string(random() % 40, char('a' + random() % 26));
      record.facetId = random() % 3;
      record.facetName = record.facetId ? "facet-" + std::to_string(record.facetId) : std::string();
      record.txn = record.nodeSeq;
      record.dataEpoch = record.shardEpoch;
      encoded.push_back(encodeLogRecord(record));
      positions[record.objectName].emplace_back(record.shardEpoch, record.nodeSeq);
      auto& entry = index[record.objectName];
      entry.objectName = record.objectName; ++entry.records;
      if (!entry.firstTxn) entry.firstTxn = record.txn;
      entry.lastTxn = record.txn;
    }
    header.lastSeq = sequence - 1;
    header.records = encoded.size();
    for (auto& [name, entry] : index) header.objects.push_back(entry);
    auto bytes = encodeSegment(header, encoded);
    header.bytes = bytes.size();
    auto written = co_await store.put(nodeSegmentKey(header.nodeId, header.nodeEpoch, header.segment), std::move(bytes));
    KJ_REQUIRE(written.status / 100 == 2, "cannot write a segment");
    ManifestSegment ref;
    ref.nodeId = header.nodeId; ref.nodeEpoch = header.nodeEpoch; ref.segment = header.segment;
    ref.firstSeq = header.firstSeq; ref.lastSeq = header.lastSeq; ref.shardEpoch = 1 + s % 4;
    fixture.segments.push_back(ref);
  }
  for (const auto& name : fixture.objects) {
    ObjectManifest manifest;
    manifest.namespaceId = fixture.ns; manifest.objectId = name.substr(fixture.ns.size() + 1);
    manifest.revision = 1; manifest.shardEpoch = 4; manifest.shard = 0;
    manifest.segments = fixture.segments;
    std::shuffle(manifest.segments.begin(), manifest.segments.end(), random);
    // Excluded positions: some of this object's own (owned and not owned by the writer), some of another object's.
    auto& own = positions[name];
    for (size_t i = 0; i < own.size(); i += 1 + random() % 4) manifest.isolated.push_back(own[i]);
    const auto& other = positions[fixture.objects[(&name - fixture.objects.data() + 1) % fixture.objects.size()]];
    for (size_t i = 0; i < other.size(); i += 5) manifest.isolated.push_back(other[i]);
    fixture.manifests.emplace(name, std::move(manifest));
  }
  co_return fixture;
}

struct Read { std::vector<LogRecord> records; std::vector<std::string> lines; };

kj::Promise<Read> readOld(HistoryGet get, const ObjectManifest& manifest, std::string name) {
  Read read;
  read.records = co_await readRecoverySegments(get, manifest, name, 2, {},
      [&read](const std::string& event, const std::string& fields) { read.lines.push_back(event + " " + fields); });
  co_return read;
}

kj::Promise<Read> readNew(SegmentDecodeCache& cache, HistoryGet get, const ObjectManifest& manifest, std::string name) {
  Read read;
  read.records = co_await readRecoverySegmentsDecoded(cache, get, manifest, name, 2,
      [&read](const std::string& event, const std::string& fields) { read.lines.push_back(event + " " + fields); });
  co_return read;
}

void compare(const Fixture& fixture, const std::map<std::string, Read>& before, std::map<std::string, Read> after,
    SegmentControl control, uint64_t decodes, uint64_t expectedDecodes) {
  if (control != SegmentControl::NONE && control != SegmentControl::DECODE_PER_READ) {
    for (auto& [name, read] : after) {
      if (read.records.size() < 2) continue;
      if (control == SegmentControl::DROP_RECORD) read.records.pop_back();
      if (control == SegmentControl::SWAP_RECORDS) std::swap(read.records.front(), read.records.back());
      if (control == SegmentControl::REVERSE_SEGMENTS) std::reverse(read.records.begin(), read.records.end());
      if (control == SegmentControl::LEAK_OTHER_OBJECT) {
        for (const auto& [other, otherRead] : before)
          if (other != name && !otherRead.records.empty()) { read.records.push_back(otherRead.records.front()); break; }
      }
      if (control == SegmentControl::CLEAR_OWNER_SEALED) {
        bool flipped = false;
        for (auto& [any, anyRead] : after)
          for (auto& record : anyRead.records) if (record.ownerSealed && !flipped) { record.ownerSealed = false; flipped = true; }
        KJ_REQUIRE(flipped, "the fixture produced no owner-sealed record");
      }
      break;
    }
  }
  bool sawOwnerSealed = false, sawExcludedKept = false;
  for (const auto& name : fixture.objects) {
    const auto& old = before.at(name).records;
    const auto& got = after.at(name).records;
    KJ_REQUIRE(old.size() == got.size(), "records match the per-object decode", name.c_str(), old.size(), got.size());
    for (size_t i = 0; i < old.size(); ++i) {
      KJ_REQUIRE(old[i].objectName == name, "the old reader returned another object's record");
      KJ_REQUIRE(encodeLogRecord(old[i]) == encodeLogRecord(got[i]), "records match the per-object decode", name.c_str(), i);
      KJ_REQUIRE(old[i].ownerSealed == got[i].ownerSealed, "owner evidence matches", name.c_str(), i);
      sawOwnerSealed = sawOwnerSealed || old[i].ownerSealed;
      const std::pair<uint64_t, uint64_t> position{old[i].shardEpoch, old[i].nodeSeq};
      const auto& isolated = fixture.manifests.at(name).isolated;
      sawExcludedKept = sawExcludedKept || std::find(isolated.begin(), isolated.end(), position) != isolated.end();
    }
    KJ_REQUIRE(before.at(name).lines == after.at(name).lines, "log lines match", name.c_str());
  }
  KJ_REQUIRE(sawOwnerSealed && sawExcludedKept, "the fixture covers owner-sealed and excluded records");
  KJ_REQUIRE(decodes == expectedDecodes, "each segment decoded once", decodes, expectedDecodes);
}

void runSegmentCase(unsigned seed, SegmentControl control) {
  auto temp = (std::filesystem::temp_directory_path() / "b57-segment-XXXXXX").string();
  KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
  KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
  auto io = kj::setupAsyncIo();
  ObjectStoreConfig config;
  config.backend = "directory"; config.directory = temp; config.endpoint = "unused";
  ObjectStore store(config, io.provider->getNetwork(), io.provider->getTimer());
  uint64_t gets = 0;
  HistoryGet get = [&store, &gets](std::string key) { ++gets; return store.get(std::move(key)); };
  // The node's way: the download runs on the decoder's worker thread with a store client of its own.
  HistoryBlockingGet blocking = [config](const std::string& key) {
    auto threadIo = kj::setupAsyncIo();
    ObjectStore threadStore(config, threadIo.provider->getNetwork(), threadIo.provider->getTimer());
    return threadStore.get(key).wait(threadIo.waitScope);
  };
  std::mt19937 random(seed);
  auto fixture = prepare(store, random).wait(io.waitScope);
  std::map<std::string, Read> before;
  for (const auto& name : fixture.objects) before.emplace(name, readOld(get, fixture.manifests.at(name), name).wait(io.waitScope));
  // All objects at once, as concurrent rebuilds would: they share each segment's download and decode. The
  // DECODE_PER_READ control gives every read a cache of its own, which is what decoding per object amounts to.
  const uint64_t limit = 256ull << 20;
  SegmentDecodeCache cache(get, limit, 30000, {}, blocking);
  std::vector<std::unique_ptr<SegmentDecodeCache>> own;
  kj::Vector<kj::Promise<Read>> reads;
  for (const auto& name : fixture.objects) {
    auto* used = &cache;
    if (control == SegmentControl::DECODE_PER_READ)
      used = own.emplace_back(std::make_unique<SegmentDecodeCache>(get, limit, 30000, SegmentDecodeCache::Decoded(), blocking)).get();
    reads.add(readNew(*used, get, fixture.manifests.at(name), name));
  }
  auto results = kj::joinPromises(reads.releaseAsArray()).wait(io.waitScope);
  std::map<std::string, Read> after;
  for (size_t i = 0; i < fixture.objects.size(); ++i) after.emplace(fixture.objects[i], kj::mv(results[i]));
  uint64_t decodes = cache.decodes;
  for (const auto& extra : own) decodes += extra->decodes;
  compare(fixture, before, std::move(after), control, decodes, fixture.segments.size());
  if (control != SegmentControl::NONE) return;
  // Read again: every segment now comes from the cache, nothing is downloaded or decoded again.
  const auto getsBefore = gets;
  for (const auto& name : fixture.objects) {
    auto again = readNew(cache, get, fixture.manifests.at(name), name).wait(io.waitScope);
    KJ_REQUIRE(again.lines == before.at(name).lines && again.records.size() == before.at(name).records.size(),
        "a cached read matches");
  }
  KJ_REQUIRE(cache.decodes == fixture.segments.size() && gets - getsBefore <= fixture.objects.size() * 4,
      "cached reads decode nothing again", cache.decodes, gets - getsBefore);
  KJ_REQUIRE(cache.heldBytes() > 0 && cache.heldBytes() <= limit, "held bytes are counted and bounded", cache.heldBytes());
  // A tiny bound evicts between readers; the answers stay the same. This one downloads on the event loop.
  SegmentDecodeCache tiny(get, 1, 30000);
  for (const auto& name : fixture.objects) {
    auto read = readNew(tiny, get, fixture.manifests.at(name), name).wait(io.waitScope);
    KJ_REQUIRE(read.records.size() == before.at(name).records.size() && read.lines == before.at(name).lines,
        "a bounded cache gives the same answer");
    for (size_t i = 0; i < read.records.size(); ++i)
      KJ_REQUIRE(encodeLogRecord(read.records[i]) == encodeLogRecord(before.at(name).records[i]), "a bounded cache gives the same records");
  }
  KJ_REQUIRE(tiny.heldBytes() <= 1 && tiny.evictions > 0, "the bound is kept", tiny.heldBytes(), tiny.evictions);
  // Idle entries are released.
  SegmentDecodeCache idle(get, 256ull << 20, 0);
  readNew(idle, get, fixture.manifests.at(fixture.objects.front()), fixture.objects.front()).wait(io.waitScope);
  KJ_REQUIRE(idle.entries() > 0);
  KJ_REQUIRE(!idle.sweep() && idle.entries() == 0 && idle.heldBytes() == 0, "an idle cache is emptied");
  // Errors: a missing segment and a corrupted one fail both readers the same way, and a failure is not kept.
  auto expectSame = [&](const ObjectManifest& manifest, const std::string& name, const char* expected) {
    std::string oldError, newError;
    try { readOld(get, manifest, name).wait(io.waitScope); } catch (const kj::Exception& e) { oldError = e.getDescription().cStr(); }
    SegmentDecodeCache fresh(get, 256ull << 20, 30000, {}, blocking);
    for (int round = 0; round < 2; ++round) {
      try { readNew(fresh, get, manifest, name).wait(io.waitScope); } catch (const kj::Exception& e) { newError = e.getDescription().cStr(); }
      KJ_REQUIRE(oldError.find(expected) != std::string::npos && newError.find(expected) != std::string::npos,
          "both readers refuse the same way", oldError.c_str(), newError.c_str());
      KJ_REQUIRE(fresh.failures == uint64_t(round + 1), "a failure is not kept", fresh.failures);
    }
  };
  auto missing = fixture.manifests.at(fixture.objects.front());
  missing.segments.push_back(ManifestSegment{"node-c", 9, 1, 1, 1, 4});
  expectSame(missing, fixture.objects.front(), "is not on the store");
  auto broken = fixture.manifests.at(fixture.objects.front());
  const auto brokenKey = nodeSegmentKey("node-c", 9, 2);
  auto bytes = store.get(nodeSegmentKey(fixture.segments.front().nodeId, fixture.segments.front().nodeEpoch,
      fixture.segments.front().segment)).wait(io.waitScope).bytes;
  bytes[bytes.size() / 2] ^= 0x5a;
  store.put(brokenKey, std::move(bytes)).wait(io.waitScope);
  broken.segments.push_back(ManifestSegment{"node-c", 9, 2, 1, 1, 4});
  expectSame(broken, fixture.objects.front(), "does not add up");
}

}  // namespace

void runDurableSegmentTests(bool negativeControls) {
  if (!negativeControls) {
    for (unsigned seed = 201; seed < 233; ++seed) runSegmentCase(seed, SegmentControl::NONE);
    return;
  }
  const std::pair<SegmentControl, std::string_view> controls[]{
    {SegmentControl::DROP_RECORD, "records match the per-object decode"},
    {SegmentControl::SWAP_RECORDS, "records match the per-object decode"},
    {SegmentControl::REVERSE_SEGMENTS, "records match the per-object decode"},
    {SegmentControl::LEAK_OTHER_OBJECT, "records match the per-object decode"},
    {SegmentControl::CLEAR_OWNER_SEALED, "owner evidence matches"},
    {SegmentControl::DECODE_PER_READ, "each segment decoded once"},
  };
  for (const auto& [control, expected] : controls) {
    bool rejected = false;
    try {
      runSegmentCase(201, control);
    } catch (const kj::Exception& ex) {
      KJ_REQUIRE(std::string_view(ex.getDescription().cStr()).find(expected) != std::string_view::npos,
          "negative control failed for a different reason", ex.getDescription());
      rejected = true;
    }
    KJ_REQUIRE(rejected, "regression was not detected", expected.data());
  }
}

}  // namespace coworkerd

#pragma once

#include "config.h"
#include "coworkerd.capnp.h"
#include "membership.h"
#include "object-store.h"

#include <kj/async-io.h>

#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

// A Durable Object's data does not stay on the node that owns it (docs/cluster-state.md, section 12). The durable
// processes ship the WAL frames of every committed transaction to this node's replicator over the descriptor they
// inherit (patches/workerd 0013); the replicator sends each record to K peers over the signed peer session and the
// object's answer waits for their confirmation, merges the records of all its objects into segments of about 16 MiB
// on the object store (nodes/<id>/wal/<epoch>/<segment>), and keeps each object's manifest
// (do/<ns>/objects/<id>/manifest/<rev>) naming its latest snapshot and the segments after it. A node that takes a
// shard over rebuilds its objects from that history -- snapshot, segments, and what the peers still hold of the
// previous owner's log -- and never reads the previous owner's files. This machine's files are a cache: they are
// reused after a restart of the daemon or a durable process, and thrown away when the machine itself restarted
// unclean.
namespace coworkerd {

// The unique key of the durable processes' Host namespace (process-pool.h keeps the same value).
constexpr const char* DURABLE_HOST_UNIQUE_KEY = "coworkerd-do-host-v1";

// ---- The objects of the store ----
//   nodes/<nodeId>/wal/<nodeEpoch>/<segment>           a segment of the node's log (immutable, 20 digits wide)
//   do/<ns>/objects/<id>/manifest/<rev> (+ /current)   the object's manifest (immutable revisions, created only if absent)
//   do/<ns>/objects/<id>/snapshot/<seq>/index          a snapshot's index, written after all of its parts
//   do/<ns>/objects/<id>/snapshot/<seq>/<part>         one part of a snapshot (immutable)
std::string nodeLogPrefix(const std::string& nodeId);                                    // nodes/<id>/wal/
std::string nodeLogEpochPrefix(const std::string& nodeId, uint64_t nodeEpoch);
std::string nodeSegmentKey(const std::string& nodeId, uint64_t nodeEpoch, uint64_t segment);
std::string objectPrefix(const std::string& namespaceId, const std::string& objectId);   // do/<ns>/objects/<id>/
std::string objectManifestPrefix(const std::string& namespaceId, const std::string& objectId);
std::string objectManifestKey(const std::string& namespaceId, const std::string& objectId, uint64_t revision);
std::string objectManifestHintKey(const std::string& namespaceId, const std::string& objectId);
std::string objectSnapshotPrefix(const std::string& namespaceId, const std::string& objectId, uint64_t sequence);
std::string objectSnapshotIndexKey(const std::string& namespaceId, const std::string& objectId, uint64_t sequence);
std::string objectSnapshotPartKey(const std::string& namespaceId, const std::string& objectId, uint64_t sequence,
    uint32_t facetId, uint64_t part);

// The id of an object's Host, as workerd derives it from the namespace's unique key and the name
// (src/workerd/server/actor-id-impl.c++): HMAC-SHA256 of the key over the name, its first 16 bytes, then the HMAC of
// those -- 64 hexadecimal digits, which name the object's files.
std::string durableHostId(const std::string& uniqueKey, const std::string& name);
// The name an object's Host is made with: "<namespace>/<object>/<data epoch>" (runtime.js).
std::string durableHostName(const std::string& namespaceId, const std::string& objectId, uint64_t dataEpoch);
// The three parts of such a name.
struct DurableObjectRef {
  std::string namespaceId, objectId;
  uint64_t dataEpoch = 0;
};
std::optional<DurableObjectRef> parseDurableHostName(const std::string& name);

// ---- The records of the node's log ----
// A record carries the node's epoch and the shard's, so a fenced owner's records are confirmed by nobody and reach no
// manifest; the object, which of its databases (facet 0 is the object itself) and, for a facet, its name and parent,
// from which a rebuild writes the facet tree index; and, for FRAMES, the bytes of the WAL frames exactly as the
// database wrote them.
enum class LogRecordKind : uint8_t {
  FRAMES = 1,   // the frames of one or more transactions, whole
  RESET = 2,    // the object's database was emptied (deleteAll())
  DELETED = 3,  // a facet's files were removed outside its own transactions
  SEAL = 4,     // this node's own log file only: everything up to `nodeSeq` is in the segment `segment`
};

struct LogRecord {
  LogRecordKind kind = LogRecordKind::FRAMES;
  uint64_t nodeSeq = 0, nodeEpoch = 0, shardEpoch = 0;
  uint32_t shard = 0;
  std::string objectName, storageId, facetName;
  uint32_t facetId = 0, parentFacetId = 0;
  uint64_t txn = 0;              // the object's transaction number this record starts at
  // Which copy of the object's files on the owner's disk the frames came from: the data epoch is part of the name the
  // object's Host was made with, but not of the object's history -- a node that takes the shard over writes the same
  // history into files of its own data epoch.
  uint64_t dataEpoch = 0;
  uint64_t sentAtMonotonicNs = 0;
  // FRAMES: the WAL generation and its frames.
  uint64_t generation = 0;
  uint32_t salt1 = 0, salt2 = 0, pageSize = 0, firstFrame = 0, frames = 0, commits = 0;
  std::string header;            // the generation's 32 byte WAL header, with its first record
  std::string frameBytes;        // frames x (24 byte frame header + one page)
  uint64_t segment = 0;          // SEAL
  // The object in the history: "<namespace>/<object>", without the data epoch.
  std::string key() const { return objectName + "/" + std::to_string(facetId); }
  // The database as the durable process names it: the Host's name, which carries the data epoch, and the facet.
  std::string shipperKey() const {
    return objectName + "/" + std::to_string(dataEpoch) + "/" + std::to_string(facetId);
  }
};
// The record, with its CRC32C: a peer refuses a record whose bytes do not add up rather than letting a rebuild find
// out (experiment 25, B.7).
std::string encodeLogRecord(const LogRecord& record);
std::optional<LogRecord> decodeLogRecord(std::string_view bytes, std::string* error = nullptr);
// Records one after another, each behind its length (UInt32, little endian), as a stream carries them.
std::string frameRecords(const std::vector<std::string>& encoded);
std::vector<std::string> splitRecords(std::string_view bytes, std::string* error = nullptr);
uint32_t crc32c(std::string_view bytes);

// ---- Segments ----
// "CWSEG001" | UInt32 header length | the header as JSON | (UInt32 length + record) x N | SHA-256 of all of it.
struct SegmentIndexEntry {
  std::string objectName;
  uint32_t facetId = 0;
  uint64_t records = 0, firstTxn = 0, lastTxn = 0;
};
struct SegmentHeader {
  std::string nodeId, reason;
  uint64_t nodeEpoch = 0, segment = 0, firstSeq = 0, lastSeq = 0, records = 0, bytes = 0, sealedAtUnixMs = 0;
  std::vector<SegmentIndexEntry> objects;
};
std::string encodeSegment(const SegmentHeader& header, const std::vector<std::string>& records);
// The records of a segment, with its header; none when the magic, the length or the digest do not hold.
struct SegmentContents {
  SegmentHeader header;
  std::vector<LogRecord> records;
};
std::optional<SegmentContents> decodeSegment(const std::string& bytes, std::string* error = nullptr);

// ---- Manifests ----
struct ManifestFacet {
  uint32_t facetId = 0, parentFacetId = 0;
  std::string name, sha256;
  uint64_t bytes = 0, parts = 0;
  // The node sequence of the snapshot's own shard epoch this database already holds: the records up to it were in
  // the file when it was copied, and a rebuild replays only what follows.
  uint64_t upToSeq = 0;
};
struct ManifestSegment {
  std::string nodeId;
  uint64_t nodeEpoch = 0, segment = 0, firstSeq = 0, lastSeq = 0, shardEpoch = 0;
};
struct ObjectManifest {
  std::string namespaceId, objectId;
  uint64_t revision = 0, shardEpoch = 0;
  uint32_t shard = 0;
  // The snapshot every segment below follows, if there is one.
  bool hasSnapshot = false;
  uint64_t snapshotSequence = 0, snapshotParts = 0, snapshotBytes = 0, snapshotAtUnixMs = 0;
  std::string snapshotNodeId;
  uint64_t snapshotNodeEpoch = 0, snapshotShardEpoch = 0;
  std::vector<ManifestFacet> facets;
  std::vector<ManifestSegment> segments;
  // Records this object's owner isolated: never confirmed, never in a segment, and left out of a rebuild even when a
  // peer still holds them.
  std::vector<std::pair<uint64_t, uint64_t>> isolated;  // (shardEpoch, nodeSeq)
  // Who wrote this revision: the node, its membership epoch, the process and a random value of the write, so that a
  // create whose answer was lost is decided by reading the bytes back (R5).
  std::string writerNodeId, instance, nonce;
  uint64_t writerNodeEpoch = 0, writtenAtUnixMs = 0, databaseBytes = 0;
};
std::string objectManifestJson(const ObjectManifest& manifest);
std::optional<ObjectManifest> parseObjectManifest(const std::string& bytes);

// ---- Replaying records into database files ----
// One database being rebuilt: the frames of its records are written at their pages' offsets and the file truncated at
// every commit, which gives the same bytes as the owner's file after a checkpoint (experiment 25, B.1/B.4).
class DatabaseReplay {
public:
  explicit DatabaseReplay(std::filesystem::path path, bool fromEmpty);
  ~DatabaseReplay();
  // Applies one record; false (with the reason) when its frames do not check out, which ends the replay there.
  bool apply(const LogRecord& record, std::string* error);
  void finish();
  uint64_t appliedRecords() const { return records; }
  uint64_t appliedFrames() const { return framesApplied; }

private:
  std::filesystem::path path;
  int fd = -1;
  uint64_t records = 0, framesApplied = 0;
  // The generation being applied: its header's values and the running checksum.
  bool haveHeader = false;
  uint32_t magic = 0, pageSize = 0, salt1 = 0, salt2 = 0, cksum1 = 0, cksum2 = 0, nextFrame = 0;
};

// ---- The replicator ----
// What a node asks another for when it rebuilds an object of a shard it took over.
struct DurableTailAsk {
  std::string namespaceId, ownerNodeId;
  uint32_t shard = 0;
  uint64_t shardEpoch = 0, ownerNodeEpoch = 0, afterSeq = 0;
};
struct DurableTailAnswer {
  std::vector<LogRecord> records;
  uint64_t firstSeq = 0, lastSeq = 0, droppedTo = 0;
  uint32_t count = 0;
  bool more = false;
  std::string refusal;
};

// One node's replicator: the channels to its durable processes, its log, its peers, its segments, the manifests of
// its objects, their snapshots, and the rebuild of an object from all of it.
class DurableLog final: private kj::TaskSet::ErrorHandler {
public:
  // One peer's end of a log stream (node/main.c++ carries the calls over the signed peer session).
  class PeerSink {
  public:
    virtual ~PeerSink() noexcept(false) = default;
    // The records, each behind its length: the answer is the highest sequence the peer holds of this stream, or a
    // refusal (its buffer is full, or this owner has been fenced).
    struct Accepted {
      uint64_t sequence = 0;
      std::string refusal;
    };
    // `partial`: a piece of a record the calls after it complete (one transaction's frames can be larger than one
    // message may carry); the peer holds the pieces and takes the record whole at the call that is not partial.
    virtual kj::Promise<Accepted> append(std::string records, bool partial) = 0;
    virtual kj::Promise<void> trim(uint64_t upTo) = 0;
    virtual kj::Promise<void> drop(std::vector<uint64_t> sequences) = 0;
  };

  struct Settings {
    std::string nodeId, instance;
    std::filesystem::path stackDirectory;
    std::string hostUniqueKey = DURABLE_HOST_UNIQUE_KEY;
    DurableLogConfig config;
    bool objectStore = true;
  };
  // `openStream` opens a log stream to one peer; `fetchTail` asks a node for what it holds of a previous owner's log
  // for a shard; `knownShardEpoch` is this node's view of who owns a shard, for the fencing of the records it takes
  // as a peer (0: it does not know); `closeObjects` closes every object of this node's durable processes, which is
  // how an object whose write was not confirmed stops being served before its files are thrown away.
  struct Hooks {
    std::function<kj::Promise<kj::Own<PeerSink>>(std::string nodeId, uint64_t firstSeq)> openStream;
    std::function<kj::Promise<DurableTailAnswer>(std::string nodeId, DurableTailAsk ask)> fetchTail;
    std::function<uint64_t(const std::string& namespaceId, uint32_t shard)> knownShardEpoch;
    std::function<kj::Promise<void>(std::string reason)> closeObjects;
  };
  DurableLog(Settings settings, const ObjectStoreConfig& store, kj::Network& network, kj::Timer& timer,
      kj::LowLevelAsyncIoProvider& lowLevel, const MembershipView& membership, Hooks hooks);
  ~DurableLog() noexcept(false);

  // The node's membership epoch, as its records carry it; set at registration and at every new epoch.
  void setNodeEpoch(uint64_t nodeEpoch);
  // Reads this machine's Durable Object directory: the files are reused when the machine did not restart unclean
  // (the boot id beside them, and the marker a clean exit writes), and otherwise thrown away. Then this node's own
  // log files are read and the records no segment covers are replayed into the files they belong to, so that what a
  // durable process committed without getting it confirmed before it died is gone and what it did get confirmed is
  // there.
  kj::Promise<void> start();
  // The segments, the snapshots, the uploads and the idle checkpoints.
  kj::Promise<void> run();
  // A clean exit: the segment in hand is sealed and uploaded (bounded by `deadlineMonotonicMs`) and the marker
  // written, so the next start reuses this machine's files.
  kj::Promise<void> stop(double deadlineMonotonicMs);

  // ---- The durable processes ----
  // The node's end of a durable process's channel is kept and read; the descriptor returned is the one the process
  // inherits, or -1 when this node ships nothing.
  int openChannel(const std::string& processId);
  void closeChannel(const std::string& processId);

  // ---- The objects ----
  struct ObjectIdentity {
    std::string namespaceId, objectId;
    uint64_t dataEpoch = 0, shardEpoch = 0;
    uint32_t shard = 0;
  };
  struct ObjectReady {
    bool ready = true;
    uint status = 503;
    std::string code, reason;
    double elapsedMs = 0;
    bool rebuilt = false;
  };
  // Before a request reaches a durable process: the object's files are brought up to date with its replicated
  // history when this node has not served it at this data epoch (a shard taken over, or a repair), and the answer
  // says what to tell the caller when they cannot be.
  kj::Promise<ObjectReady> openObject(ObjectIdentity identity);
  // The shard is this node's no longer (released, lost or dropped): its objects' records stop going out.
  void shardGone(const std::string& namespaceId, uint32_t shard, const std::string& reason);

  // ---- As a peer ----
  // A log stream another node opens here: "" and a stream id, or the refusal.
  std::string openIncoming(const std::string& nodeId, uint64_t nodeEpoch, const std::string& instance,
      const std::string& destinationEpoch, uint64_t firstSeq, uint64_t& streamId);
  struct AppendResult {
    uint64_t accepted = 0;
    std::string refusal;
  };
  AppendResult appendIncoming(uint64_t streamId, std::string_view records, bool partial = false);
  void trimIncoming(uint64_t streamId, uint64_t upTo);
  void dropIncoming(uint64_t streamId, const std::vector<uint64_t>& sequences);
  void closeIncoming(uint64_t streamId);
  // What this node holds of another node's log for one shard.
  DurableTailAnswer tailFor(const DurableTailAsk& ask);

  // The state endpoint's "durableLog" section.
  std::string json() const;
  bool enabled() const { return settings.objectStore && store.has_value(); }

private:
  // One database of one object as this node's log knows it.
  struct Facet {
    uint32_t facetId = 0, parentFacetId = 0;
    std::string facetName;
    uint64_t txn = 0;              // transactions shipped
    uint64_t generation = 0, lastSeq = 0;
    // The last frame of the WAL generation this node has a record of, and the generation's salts: a database that
    // holds more than this when it is opened again holds frames nobody confirmed.
    uint32_t lastFrame = 0, salt1 = 0, salt2 = 0;
    // The record whose frames the last checkpoint moved into the database file: a snapshot taken after it needs only
    // the records that follow.
    uint64_t checkpointedSeq = 0;
    uint64_t databaseBytes = 0;
    bool open = false;             // a durable process has it open
  };
  enum class ObjectState { UNKNOWN, REBUILDING, LOCAL, REPAIR, GONE };
  struct Object {
    std::string namespaceId, objectId, storageId, objectName;
    uint64_t dataEpoch = 0, shardEpoch = 0;
    uint32_t shard = 0;
    ObjectState state = ObjectState::UNKNOWN;
    std::map<uint32_t, Facet> facets;
    uint64_t manifestRevision = 0, snapshotSequence = 0, snapshotParts = 0, snapshotBytes = 0;
    uint64_t sinceSnapshotBytes = 0, snapshotAtUnixMs = 0;
    std::vector<ManifestFacet> snapshotFacets;
    std::vector<ManifestSegment> segments;   // sealed here and not yet in a manifest revision
    bool snapshotting = false;
    uint64_t bytes = 0;            // the databases together, as the last checkpoint reported
    bool overLimit = false;
    double lastAccessMs = 0, lastCheckpointAskMs = 0;
    bool manifestDirty = false;
    std::string repairReason;
    uint64_t rebuilds = 0, repairs = 0;
    double lastRebuildMs = 0;
  };
  // A record this node sent and is waiting for its peers on.
  struct InFlight {
    uint64_t nodeSeq = 0, request = 0;
    std::string processId, objectKey, shipperKey;
    std::vector<std::string> peers;
    double sentAtMs = 0;
    size_t bytes = 0;
    uint32_t endsAtFrame = 0;
    uint64_t generation = 0;
  };
  // A durable process's channel.
  struct Channel {
    std::string processId;
    kj::Own<kj::AsyncIoStream> stream;
    std::string inbox;
    uint64_t pid = 0, messages = 0, frames = 0, bytes = 0;
    bool open = false;
    kj::Promise<void> writeChain = nullptr;
    kj::Promise<void> reader = nullptr;
  };
  // A stream to one peer. One record can be larger than a message may carry, so it goes in pieces, and at most
  // APPEND_WINDOW_BYTES are in flight at a time: the pieces queued here wait for the answers of the ones before them
  // (an operating system refuses one write of many tens of megabytes).
  struct Queued {
    uint64_t sequence = 0;
    std::string bytes;
    bool partial = false;
  };
  struct PeerStream {
    std::string nodeId;
    kj::Maybe<kj::Own<PeerSink>> sink;
    bool connecting = false, broken = false;
    uint64_t firstSeq = 0, ackedSeq = 0, sentSeq = 0, appends = 0, refusals = 0;
    std::string lastRefusal;
    double openedAtMs = 0, lastAckMs = 0, attemptedAtMs = 0;
    std::deque<Queued> queue;
    uint64_t inFlightBytes = 0, queuedBytes = 0;
  };
  // What this node buffers for one owner it is a peer of.
  struct PeerBuffer {
    std::string nodeId, instance;
    uint64_t nodeEpoch = 0, streamId = 0;
    uint64_t acceptedSeq = 0, droppedTo = 0, bytes = 0, records = 0, refusedRecords = 0;
    double openedAtMs = 0, lastAppendMs = 0;
    std::string fragment;                  // the pieces of a record whose last call has not come yet
    std::map<uint64_t, std::string> held;  // nodeSeq -> the encoded record
  };

  void taskFailed(kj::Exception&& error) override;
  void log(const std::string& event, const std::string& fields) const;
  std::filesystem::path durableDirectory() const;   // <stack>/do/<host unique key>
  std::filesystem::path logDirectory() const;       // <stack>/do/log
  std::filesystem::path databasePath(const std::string& storageId, uint32_t facetId, const char* suffix = "") const;
  // The boot id of this machine ("" where it cannot be read) and the marker a clean exit leaves.
  std::string bootId() const;
  bool readCacheMarker(std::string& reason);
  void writeCacheMarker(bool clean);
  // The records this node's own log files hold, and each database cut back to the position they name.
  kj::Promise<void> readOwnLog();
  void cutBackWal(const std::string& storageId, uint32_t facetId, uint32_t salt1, uint32_t salt2, uint32_t pageSize,
      uint32_t lastFrame);

  // ---- the channel to a durable process ----
  kj::Promise<void> readChannel(Channel& channel);
  void onMessage(Channel& channel, std::string_view message);
  void sendToChannel(Channel& channel, std::string message);
  void answer(Channel& channel, uint64_t request, uint8_t status);
  void tellSealed(const std::string& key, uint32_t salt1, uint32_t salt2, uint32_t upToFrame);
  void tellBreak(const std::string& key, const std::string& reason);
  void askCheckpoint(const std::string& key);

  // ---- the log ----
  Object* objectOf(const std::string& objectName);
  void appendToLogFile(const std::string& encoded);
  void takeRecord(Channel& channel, LogRecord&& record, uint64_t request);
  void sendOne(PeerStream& peer, uint64_t sequence, const std::string& encoded);
  void pumpPeer(PeerStream& peer);
  std::vector<std::string> peersOf(const std::string& namespaceId, uint32_t shard) const;
  uint32_t peerCount() const;
  void onPeerAck(const std::string& nodeId, uint64_t accepted);
  void checkConfirmations();
  void isolate(const std::string& objectKey, uint64_t fromSeq, const std::string& reason);
  void markRepair(Object& object, const std::string& reason);
  kj::Promise<void> openPeer(std::string nodeId);

  // ---- segments, manifests, snapshots ----
  kj::Promise<void> sealLoop();
  kj::Promise<void> sealNow(std::string reason);
  kj::Promise<void> uploadSegment(const SegmentHeader& header, const std::string& body);
  kj::Promise<bool> writeManifest(Object& object, bool withSnapshot);
  kj::Promise<void> snapshotIfDue(std::string objectName);
  kj::Promise<void> uploadSnapshotPart(std::string namespaceId, std::string objectId, uint64_t sequence,
      uint32_t facetId, uint64_t part, std::string bytes);
  kj::Promise<void> housekeeping();
  kj::Promise<void> resealEarlier();
  kj::Promise<void> dropColdFiles();

  // ---- rebuilding an object ----
  kj::Promise<void> rebuild(std::string objectName, std::string reason);
  kj::Promise<std::optional<ObjectManifest>> readManifest(std::string namespaceId, std::string objectId);
  kj::Promise<void> downloadSnapshot(const ObjectManifest& manifest, const std::string& storageId);
  kj::Promise<void> downloadSnapshotPart(const ObjectManifest& manifest, uint32_t facetId, uint64_t part, int fd,
      off_t offset);
  kj::Promise<std::vector<LogRecord>> readSegments(const ObjectManifest& manifest, const std::string& objectName);
  kj::Promise<std::vector<LogRecord>> fetchTails(Object& object);
  void applyRecords(Object& object, std::vector<LogRecord> records, const std::vector<ManifestFacet>& facets,
      uint64_t snapshotShardEpoch, const std::set<std::pair<uint64_t, uint64_t>>& drop);
  void writeFacetIndex(const std::string& storageId, const std::vector<ManifestFacet>& facets);
  void removeObjectFiles(const Object& object);

  Settings settings;
  std::optional<ObjectStore> store;
  kj::Network& network;
  kj::Timer& timer;
  kj::LowLevelAsyncIoProvider& lowLevel;
  const MembershipView& membership;
  Hooks hooks;

  uint64_t nodeEpoch = 0;
  uint64_t nextSeq = 0;             // the node sequence of the next record
  uint64_t sealedSeq = 0;           // every record up to here is in a segment
  uint64_t segmentNumber = 0;
  int logFd = -1;                   // the append file of this node epoch's log
  std::filesystem::path logPath;
  // The records this node holds: those not yet in a segment (by sequence), and how many bytes they take.
  std::map<uint64_t, std::string> records;
  std::map<uint64_t, LogRecord> parsed;
  uint64_t recordBytes = 0;
  // Records confirmed by the peers, and records isolated: never confirmed, never sealed.
  std::set<uint64_t> confirmed;
  std::set<uint64_t> isolated;
  std::vector<std::pair<uint64_t, uint64_t>> isolatedReported;  // (shardEpoch, nodeSeq), for the manifest
  // The records of earlier epochs of this node that no segment covers (read at startup), by epoch.
  std::map<uint64_t, std::map<uint64_t, LogRecord>> earlier;

  std::map<std::string, Object> objects;         // objectName -> object
  std::map<std::string, Channel> channels;       // processId -> channel
  std::map<std::string, PeerStream> peers;       // nodeId -> stream
  std::map<uint64_t, InFlight> inFlight;         // nodeSeq -> what waits for its peers
  std::map<uint64_t, PeerBuffer> incoming;       // streamId -> what this node buffers as a peer
  uint64_t nextStreamId = 0, incomingBytes = 0;
  // A rebuild in flight per object; the requests of that object wait on a branch of it. A settled one is released by
  // the next openObject(), never from its own continuation (the ForkHub would be destroyed while it runs).
  std::map<std::string, kj::ForkedPromise<void>> rebuilding;
  std::set<std::string> settledRebuilds;

  // Counters for the state endpoint.
  uint64_t confirmations = 0, confirmFailures = 0, refusals = 0, isolations = 0, segments = 0, segmentBytes = 0;
  uint64_t uploads = 0, uploadFailures = 0, manifestWrites = 0, manifestConflicts = 0, manifestFailures = 0;
  uint64_t snapshots = 0, snapshotFailures = 0, snapshotBytesWritten = 0, checkpoints = 0, checkpointAsks = 0;
  uint64_t rebuilds = 0, rebuildFailures = 0, repairs = 0, filesDropped = 0, peerSwitches = 0, tailFetches = 0;
  uint64_t recordsTaken = 0, framesTaken = 0, bytesTaken = 0, overLimitAnswers = 0, recoverTimeouts = 0;
  uint64_t tailAnswers = 0, tailRefusals = 0, fencedRecords = 0, bufferRefusals = 0;
  uint64_t walCutBacks = 0, positionMismatches = 0, manifestFences = 0, replayStops = 0, diskUsedBytes = 0;
  bool sealing = false, noPeerWarned = false;
  double confirmMsTotal = 0, lastSegmentAtMs = 0, lastUploadAtMs = 0, lastRebuildMs = 0;
  std::vector<double> confirmSamples;
  std::string lastUploadError, lastManifestError, lastRebuildError, startupNote;
  bool diskTrusted = true;
  uint64_t diskLimitBytes = 0;
  kj::TaskSet tasks{*this};
};

}  // namespace coworkerd

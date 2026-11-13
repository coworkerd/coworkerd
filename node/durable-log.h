#pragma once

#include "config.h"
#include "coworkerd.capnp.h"
#include "membership.h"
#include "object-store.h"

#include <kj/async-io.h>
#include <kj/refcount.h>

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
  // Read-time evidence only; never encoded. The original shard owner put this
  // record in a normal segment, so another object's old exclusion cannot drop it.
  bool ownerSealed = false;
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
  explicit DatabaseReplay(std::filesystem::path path, bool fromEmpty, bool validateOnly = false);
  ~DatabaseReplay();
  // Applies one record; false (with the reason) when its frames do not check out, which ends the replay there.
  bool apply(const LogRecord& record, std::string* error);
  void finish();
  uint64_t appliedRecords() const { return records; }
  uint64_t appliedFrames() const { return framesApplied; }

private:
  std::filesystem::path path;
  int fd = -1;
  bool validateOnly = false;
  uint64_t records = 0, framesApplied = 0;
  // The generation being applied: its header's values and the running checksum.
  bool haveHeader = false;
  std::string walHeader;
  uint32_t magic = 0, pageSize = 0, salt1 = 0, salt2 = 0, cksum1 = 0, cksum2 = 0, nextFrame = 0;
};

// ---- Alarms ----
//   do/<ns>/shards/<s>/alarms/<objectId>   a hint: the next alarm of that object, so the node that takes the shard
//                                          over knows which objects to wake. The time itself is in the object's own
//                                          database (SQLite's _cf_METADATA), replicated and recovered with it, so
//                                          this object is only ever a hint -- it is never read for correctness.
std::string shardAlarmPrefix(const std::string& namespaceId, uint32_t shard);
std::string shardAlarmKey(const std::string& namespaceId, uint32_t shard, const std::string& objectId);
// What one node's objects hold, by tenant (nodes/<id>/durable-usage, overwritable): every node publishes its own
// every hour and reads the others' for the durableObjectBytes quota.
std::string nodeDurableUsageKey(const std::string& nodeId);

// One database's alarm, as the durable process reports it (patches/workerd 0014).
struct DurableAlarm {
  std::string namespaceId, objectId, tenant;
  uint32_t shard = 0, facetId = 0;
  uint64_t dataEpoch = 0, shardEpoch = 0, atUnixMs = 0;
  // How many times the handler has already been tried, for alarmInfo.retryCount.
  uint32_t retryCount = 0;
};
// What firing one came to. FIRED: the handler ran and the object cleared its own alarm. NOT_DUE: the object's own
// database holds another time (`atUnixMs`), which is the truth. FAILED: the handler threw, so it is tried again with
// Cloudflare's backoff. UNAVAILABLE: the object could not be reached at all (this node no longer owns the shard, the
// store is down), so nothing is counted against the retry limit.
enum class AlarmOutcome : uint8_t { FIRED, NOT_DUE, FAILED, UNAVAILABLE };
struct AlarmResult {
  AlarmOutcome outcome = AlarmOutcome::FIRED;
  uint64_t atUnixMs = 0;
  std::string reason;
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
  // Local metadata, negotiated by the authenticated CONNECT response, never a wire field.
  bool ownerConfirmed = false;
};

// A recovery destination may have expired out of live membership. Its old epoch
// is still addressed: lease expiry alone does not destroy a process's tail.
struct DurableTailTarget {
  std::string nodeId, address, destinationEpoch, instance;
  // Store-clock observations for this recovery attempt, never wire fields.
  uint64_t heartbeatUnixMs = 0, registeredUnixMs = 0, successorRegisteredUnixMs = 0;
  uint32_t durableProcesses = 1;
  bool timeVerified = false;
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
    uint32_t tailWaitMs = 25000, peerTimeoutMs = 2000;
  };
  // `openStream` opens a log stream to one peer; `fetchTail` asks a node for what it holds of a previous owner's log
  // for a shard; `knownShardEpoch` is this node's view of who owns a shard, for the fencing of the records it takes
  // as a peer (0: it does not know); `closeObjects` closes every object of this node's durable processes, which is
  // how an object whose write was not confirmed stops being served before its files are thrown away.
  struct Hooks {
    std::function<kj::Promise<kj::Own<PeerSink>>(std::string nodeId, uint64_t ownerEpoch, uint64_t firstSeq)> openStream;
    std::function<kj::Promise<DurableTailAnswer>(std::string nodeId, DurableTailAsk ask)> fetchTail;
    std::function<uint64_t(const std::string& namespaceId, uint32_t shard)> knownShardEpoch;
    std::function<kj::Promise<void>(std::string reason)> closeObjects;
    // Runs one object's alarm handler as an event of its Worker (its CPU topped up, its lease renewed, its lines
    // kept): the firing of an alarm is a request to the object like any other, so only the node can do it.
    std::function<kj::Promise<AlarmResult>(DurableAlarm alarm)> fireAlarm;
    // The tenant's durableObjectBytes quota (0: none), as this node's deployment state holds it.
    std::function<uint64_t(const std::string& tenant)> tenantQuotaBytes;
    // Whether the namespace is still a tenant's active one: a deleted class's or a deleted database's objects are
    // not served and stop counting against the tenant's storage.
    std::function<bool(const std::string& namespaceId)> namespaceActive;
    std::function<kj::Promise<DurableTailAnswer>(DurableTailTarget target, DurableTailAsk ask)> fetchTailAt;
  };
  DurableLog(Settings settings, const ObjectStoreConfig& store, kj::Network& network, kj::Timer& timer,
      kj::LowLevelAsyncIoProvider& lowLevel, const MembershipView& membership, Hooks hooks);
  ~DurableLog() noexcept(false);

  // The node's membership epoch, as its records carry it; set at registration and at every new epoch.
  void setNodeEpoch(uint64_t nodeEpoch);
  // The two hooks that need the dispatcher, which is built after this: firing an object's alarm as a request of its
  // Worker, and the tenant's durableObjectBytes quota.
  void setAlarmHook(std::function<kj::Promise<AlarmResult>(DurableAlarm alarm)> hook) {
    hooks.fireAlarm = std::move(hook);
  }
  void setQuotaHook(std::function<uint64_t(const std::string& tenant)> hook) {
    hooks.tenantQuotaBytes = std::move(hook);
  }
  void setNamespaceHook(std::function<bool(const std::string& namespaceId)> hook) {
    hooks.namespaceActive = std::move(hook);
  }
  void setOwnerFenceHook(std::function<void(const std::string&, uint32_t, uint64_t)> hook) {
    ownerFenceHook = std::move(hook);
  }
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
    std::string namespaceId, objectId, tenant;
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
  // This node has taken the shard: the alarm hints of its objects are read, so the alarms the node before it had
  // scheduled go on being fired here. Each one is checked against the object's own database when it fires.
  void shardTaken(const std::string& namespaceId, uint32_t shard, uint64_t shardEpoch, const std::string& tenant);

  // ---- Durable Object storage per tenant (the durableObjectBytes quota) ----
  // What this node's objects of the tenant hold, and what the cluster holds as the nodes last published it.
  uint64_t tenantBytesHere(const std::string& tenant) const;
  uint64_t tenantBytes(const std::string& tenant) const;
  // The tenant's namespaces this node owns shards of, with their objects and bytes (the /v1 namespace view).
  std::string namespacesJson(const std::string& tenant) const;

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
  DurableTailAnswer tailFor(const DurableTailAsk& ask, bool remote = true);
  bool canAnswerRestoredTail(const DurableTailAsk& ask, const std::string& destinationEpoch,
      std::string* refusal = nullptr);
  void setTailAtHook(std::function<kj::Promise<DurableTailAnswer>(DurableTailTarget, DurableTailAsk)> hook) {
    hooks.fetchTailAt = std::move(hook);
  }
  void setHealOwnershipHook(std::function<kj::Promise<std::optional<uint64_t>>(ObjectIdentity)> hook) {
    healOwnership = std::move(hook);
  }
  void objectFailed(const std::string& namespaceId, const std::string& objectId, uint64_t shardEpoch,
      const std::string& step, const std::string& reason);

  // The state endpoint's "durableLog" section.
  std::string json() const;
  std::string memoryJson() const;
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
    bool deleted = false;         // latest local log record explicitly deleted this facet
    bool open = false;             // a durable process has it open
    std::string processId;
    uint64_t openEpoch = 0, openGeneration = 0;
    uint64_t loggedNodeEpoch = 0, loggedGeneration = 0, loggedSeq = 0;
    uint32_t loggedFrame = 0, loggedSalt1 = 0, loggedSalt2 = 0;
    uint32_t loggedPageSize = 0;
    uint64_t checkpointGeneration = 0;
    uint32_t checkpointFrames = 0, checkpointSalt1 = 0, checkpointSalt2 = 0;
    bool checkpointSafe = true;
    bool loggedCheckpointCovered = false;
  };
  enum class ObjectState { UNKNOWN, REBUILDING, LOCAL, REPAIR, GONE };
  struct Object {
    std::string namespaceId, objectId, storageId, objectName, tenant;
    uint64_t dataEpoch = 0, shardEpoch = 0;
    uint32_t shard = 0;
    ObjectState state = ObjectState::UNKNOWN;
    std::map<uint32_t, Facet> facets;
    uint64_t manifestRevision = 0, snapshotSequence = 0, snapshotParts = 0, snapshotBytes = 0;
    uint64_t sinceSnapshotBytes = 0, snapshotAtUnixMs = 0;
    std::vector<ManifestFacet> snapshotFacets;
    std::vector<ManifestSegment> segments;   // sealed here and not yet in a manifest revision
    std::vector<std::pair<uint64_t, uint64_t>> isolatedRecords;
    bool snapshotting = false;
    uint64_t snapshotVersion = 0;  // changed content, checkpoint, isolation or rebuild invalidates a local image
    uint64_t bytes = 0;            // the databases together, as the last checkpoint reported
    bool overLimit = false;
    double lastAccessMs = 0, lastCheckpointAskMs = 0;
    bool manifestDirty = false;
    std::string repairReason;
    std::pair<uint64_t, uint64_t> repairGeneration, rebuildGeneration, imageRepairGeneration;
    uint64_t openEpoch = 0, openGeneration = 0, positionNodeEpoch = 0, cacheShardEpoch = 0;
    bool cacheOpen = false, positionRead = false, recoveryPublished = false, healingInProgress = false;
    bool cacheInvalid = false, cacheHold = false;
    bool cacheAdmissionPending = false;
    uint64_t rebuilds = 0, repairs = 0;
    double lastRebuildMs = 0;
    // Request timeouts and storage retries share one tail deadline at this ownership epoch.
    double recoveryStartedMs = 0;
    uint64_t recoveryShardEpoch = 0;
    std::set<std::pair<uint64_t, uint64_t>> lateTails;
    // Verified bytes survive failed publication and retries; they are released only after publication.
    std::map<std::pair<uint64_t, uint64_t>, LogRecord> recoveryTails;
    std::optional<uint64_t> recoveryManifestRevision;
    bool recoveryRevisionChanged = false;
    std::map<std::string, DurableTailTarget> recoveryAddresses;
    std::set<std::string> recoveryUnknownKeys;
    bool recoveryDiscoveryCompleted = false;
    std::map<uint64_t, std::pair<std::string, uint64_t>> recoveryEpochReads;
    // Startup's trusted cache is checked before any workerd opens these files.
    bool healPending = false, healRequested = false, healError = false;
    double healRetryAfterMs = 0;
    // What each of its databases holds as its alarm (0: none), as the durable process reports it.
    std::map<uint32_t, uint64_t> alarms;
  };
  // One alarm this node has to fire, with its retries. Cloudflare's backoff: the first retry two seconds later,
  // doubling, a quarter of jitter, and once the counted retries run out the delay doubles to about seventeen
  // minutes. The time itself is the object's; this is only the schedule.
  struct AlarmState {
    std::string objectName, namespaceId, objectId, tenant;
    uint32_t shard = 0, facetId = 0;
    uint64_t dataEpoch = 0, shardEpoch = 0, atUnixMs = 0;
    double notBeforeMs = 0;   // monotonic: a retry waits for it
    uint32_t retries = 0, countedRetries = 0, backoff = 0;
    bool firing = false;
    // Read from the hint in the store rather than reported by the object: the object's own database decides when it
    // is opened, and a hint that turns out to be stale is dropped.
    bool fromHint = false;
  };
  // What one durable process reports of its resident objects (patches/workerd 0014).
  struct Residency {
    uint64_t resident = 0, limit = 0, evicted = 0, woken = 0, wakes = 0;
    // As the process reports them, in microseconds: waking an object whose class is still loaded is sub-millisecond.
    uint64_t wakeUsTotal = 0, wakeUsMax = 0;
    double atMs = 0;
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
    std::map<std::string, std::pair<uint64_t, uint64_t>> openFacets;
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
    // Requests retain their sink until they settle, even after this session has been retired.
    struct Sink: kj::Refcounted {
      explicit Sink(kj::Own<PeerSink> value): value(kj::mv(value)) {}
      kj::Own<PeerSink> value;
    };
    std::string nodeId, identity;
    uint64_t session = 0, ownerEpoch = 0;
    kj::Maybe<kj::Rc<Sink>> sink;
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
    std::map<uint64_t, LogRecord> positions;  // validated metadata without frame/header bytes
    std::set<uint64_t> isolated;          // a late append cannot undo DROP on a replacement stream
  };

  void taskFailed(kj::Exception&& error) override;
  void log(const std::string& event, const std::string& fields) const;
  std::filesystem::path durableDirectory() const;   // <stack>/do/<host unique key>
  std::filesystem::path logDirectory() const;       // <stack>/do/log
  std::filesystem::path databasePath(const std::string& storageId, uint32_t facetId, const char* suffix = "") const;
  // The boot id of this machine ("" where it cannot be read) and the marker a clean exit leaves.
  std::string bootId() const;
  bool readCacheMarker(std::string& reason);
  bool writeCacheMarker(bool clean);
  enum class RecoveryKind : uint8_t {
    HEADER, REPAIR_BEGIN, ISOLATED, REPAIR_DONE, REBUILD_BEGIN, REBUILD_DONE,
    PUBLISHED, HEAL_BEGIN, HEAL_DONE
  };
  using RecoveryGeneration = std::pair<uint64_t, uint64_t>;
  struct RecoveryEvent {
    RecoveryKind kind = RecoveryKind::HEADER;
    std::string objectName;
    RecoveryGeneration generation;
    uint64_t shardEpoch = 0, nodeSeq = 0, dataEpoch = 0;
    uint32_t shard = 0;
  };
  struct RecoveryState {
    RecoveryGeneration repair, rebuild, repaired;
    bool repairPending = false, rebuildPending = false, published = false, healing = false;
    std::map<std::pair<uint64_t, uint64_t>, RecoveryGeneration> exclusions;
  };
  struct RecoveryEpoch {
    bool modern = false, complete = false, crossedBoot = false, present = false;
  };
  bool initializeRecovery();
  void readRecovery();
  void applyRecovery(const RecoveryEvent& event);
  bool appendRecovery(const std::vector<RecoveryEvent>& events, bool sync);
  bool beginRepair(Object& object);
  RecoveryEvent recoveryEvent(RecoveryKind kind, const Object& object, RecoveryGeneration generation) const;
  void failRecovery(const std::string& reason);
  bool markCrossedBoot(uint64_t epoch);
  // The records this node's own log files hold, and each database cut back to the position they name.
  kj::Promise<void> readOwnLog();
  kj::Promise<void> classifyEarlier();
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
  bool appendToLogFile(const std::string& encoded);
  uint64_t recordLimitBytes() const;
  void takeRecord(Channel& channel, LogRecord&& record, uint64_t request);
  void sendOne(PeerStream& peer, uint64_t sequence, const std::string& encoded);
  void pumpPeer(PeerStream& peer);
  std::string peerIdentity(const std::string& nodeId) const;
  PeerStream* currentPeer(const std::string& nodeId, uint64_t session);
  void retirePeer(PeerStream& peer);
  void drivePeers();
  kj::Promise<void> peerLoop();
  std::vector<std::string> peersOf(const std::string& namespaceId, uint32_t shard) const;
  uint32_t peerCount() const;
  void onPeerAck(const std::string& nodeId, uint64_t accepted);
  void checkConfirmations();
  void isolate(const std::string& objectKey, uint64_t fromSeq, const std::string& reason);
  void isolateRecords(const std::vector<uint64_t>& dropped, const std::string& key, const std::string& reason);
  void isolateObjectPending(Object& object, const std::string& reason);
  void markRepair(Object& object, const std::string& reason);
  bool ownerFenced(const std::string& namespaceId, uint32_t shard, uint64_t epoch) const;
  void fenceOwner(const std::string& namespaceId, uint32_t shard, uint64_t through,
      const std::string& reason, bool notifyShard);
  kj::Promise<void> openPeer(std::string nodeId);

  // ---- segments, manifests, snapshots ----
  struct SnapshotImage {
    uint64_t sequence = 0, parts = 0, bytes = 0, atUnixMs = 0;
    uint64_t nodeEpoch = 0, shardEpoch = 0, dataEpoch = 0, version = 0;
    ObjectState state = ObjectState::REBUILDING;
    std::vector<ManifestFacet> facets;
  };
  kj::Promise<void> sealLoop();
  kj::Promise<void> sealNow(std::string reason);
  kj::Promise<bool> uploadSegment(const SegmentHeader& header, const std::string& body);
  kj::Promise<bool> writeManifest(Object& object, bool withSnapshot,
      std::optional<uint64_t> expectedRevision = std::nullopt, const SnapshotImage* image = nullptr);
  kj::Promise<void> snapshotIfDue(std::string objectName);
  kj::Promise<void> uploadSnapshotPart(std::string namespaceId, std::string objectId, uint64_t sequence,
      uint32_t facetId, uint64_t part, std::string bytes);
  kj::Promise<void> housekeeping();
  kj::Promise<void> resealEarlier();
  void noteEarlierRetired(uint64_t epoch, const LogRecord& record);
  void persistEarlierSeals();
  void cleanupClosedEpoch();
  kj::Promise<void> dropColdFiles();

  // The object's size from its databases, and the single-object limit judged on it.
  void sized(Object& object);
  bool namespaceCounts(const std::string& namespaceId) const;

  // ---- alarms ----
  void alarmReported(const std::string& objectName, uint32_t facetId, uint64_t atUnixMs);
  kj::Promise<void> writeAlarmHint(std::string namespaceId, std::string objectId, uint32_t shard, uint64_t atUnixMs,
      uint32_t facetId);
  kj::Promise<void> readAlarmHints(std::string namespaceId, uint32_t shard, uint64_t shardEpoch, std::string tenant);
  kj::Promise<void> alarmLoop();
  kj::Promise<void> fireAlarm(std::string key);

  // ---- what this machine's files are at, so the same boot reuses them ----
  std::filesystem::path positionPath(const std::string& storageId) const;
  bool writePosition(const Object& object);
  void readPositions();
  void admitPositions();
  bool prepareOpen(Object& object);
  kj::Promise<void> admitLegacyCache(std::string objectName);
  bool hasLiveOpenFacets(const Object& object) const;
  void channelEnded(Channel& channel, const std::string& reason);
  void maybeCloseMarker(Object& object);

  // ---- Durable Object storage per tenant ----
  kj::Promise<void> usageLoop();
  kj::Promise<void> publishUsage();
  kj::Promise<void> readUsage();

  // ---- rebuilding an object ----
  kj::Promise<void> rebuild(std::string objectName, std::string reason);
  kj::Promise<void> rebuildAttempt(std::string objectName, std::string reason);
  kj::Promise<std::optional<ObjectManifest>> readManifest(std::string namespaceId, std::string objectId);
  kj::Promise<uint64_t> recoveryLowerBound(const std::string& objectName, const std::optional<ObjectManifest>& manifest);
  kj::Promise<void> downloadSnapshot(const ObjectManifest& manifest, const std::string& storageId);
  kj::Promise<void> downloadSnapshotPart(const ObjectManifest& manifest, uint32_t facetId, uint64_t part, int fd,
      off_t offset);
  kj::Promise<std::vector<LogRecord>> readSegments(const ObjectManifest& manifest, const std::string& objectName);
  struct TailRecovery {
    std::vector<LogRecord> records;
    bool cut = false, ownerSilent = false, sourceLogUnavailable = false;
    double collectionMs = 0;
  };
  kj::Promise<TailRecovery> fetchTails(ObjectIdentity identity, uint64_t snapshotShardEpoch, double startedMs);
  void noteLateTail(const std::string& objectName, const LogRecord& record, uint64_t cutEpoch);
  kj::Promise<void> publishRecovery(std::string objectName, const std::vector<LogRecord>& records);
  kj::Promise<void> forceRecoverySnapshot(std::string objectName, bool healing = false, bool local = false);
  kj::Promise<void> healLoop();
  kj::Promise<void> healObject(std::string objectName);
  kj::Promise<void> materializeTrustedWal(std::string objectName, uint64_t shardEpoch);
  kj::Promise<void> retireFencedTails(Object& object, const ObjectManifest& manifest);
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
  std::function<kj::Promise<std::optional<uint64_t>>(ObjectIdentity)> healOwnership;

  uint64_t nodeEpoch = 0;
  uint64_t nextSeq = 0;             // the node sequence of the next record
  uint64_t sealedSeq = 0;           // every record up to here is in a segment
  uint64_t segmentNumber = 0;
  int logFd = -1;                   // the append file of this node epoch's log
  bool logAppendFailed = false;     // no more appends after a possibly partial record
  int recoveryFd = -1;
  bool recoveryWriteFailed = false;
  uint64_t recoverySerial = 0, recoveryWriteFailures = 0;
  std::map<uint64_t, RecoveryEpoch> recoveryEpochs;
  std::map<std::string, RecoveryState> recoveryStates;
  std::filesystem::path logPath;
  // The records this node holds: those not yet in a segment (by sequence), and how many bytes they take.
  std::map<uint64_t, std::string> records;
  std::map<uint64_t, LogRecord> parsed;
  uint64_t recordBytes = 0;
  // Records confirmed by the peers, and records isolated: never confirmed, never sealed.
  std::set<uint64_t> confirmed;
  std::set<uint64_t> isolated;
  // The records of earlier epochs of this node that no segment covers (read at startup), by epoch.
  std::map<uint64_t, std::map<uint64_t, LogRecord>> earlier;
  std::map<uint64_t, std::map<uint64_t, LogRecord>> earlierExcluded;
  // Startup's old log has no per-record confirmation marker. It cannot certify
  // a complete answer on behalf of the predecessor process (design (a')).
  std::set<uint64_t> restoredEpochs;
  struct PredecessorLog {
    bool complete = false;
    uint64_t sealedTo = 0, retiredTo = 0, maxSeq = 0;
    bool closed = false;
    std::set<std::string> objects;
    std::map<std::string, uint64_t> objectEpochs;
    std::set<std::pair<std::string, uint32_t>> shards;
    uint32_t sealFailures = 0;
    // Immutable startup observation, including records subsequently published.
    std::set<std::pair<std::string, uint64_t>> startupUnsealed;
    // Publication is tracked per shard, independently of the current batch.
    std::map<std::pair<std::string, uint32_t>, uint64_t> unpublishedShards;
  };
  std::map<uint64_t, PredecessorLog> predecessorLogs;
  std::map<std::string, std::pair<uint64_t, uint32_t>> fencedTailReadAttempts;
  std::map<uint64_t, ManifestSegment> earlierUploads;
  std::map<uint64_t, std::map<std::string, uint32_t>> earlierPublishAttempts;
  std::map<uint64_t, double> earlierRetryAfter;
  uint64_t earlierCursor = 0;
  std::deque<uint64_t> earlierPriority;
  std::set<std::string> carriedRecoveryObjects;

  std::map<std::string, Object> objects;         // objectName -> object
  // A complete remote owner proof freezes every earlier ownership in this
  // process. Keep the boundary even when no object of an epoch is resident.
  std::map<std::pair<std::string, uint32_t>, uint64_t> ownerFenceThrough;
  std::function<void(const std::string&, uint32_t, uint64_t)> ownerFenceHook;
  uint64_t ownerFenceEvents = 0;
  std::map<std::string, AlarmState> alarms;      // "<objectName>/<facetId>" -> the alarm to fire
  std::map<std::string, Residency> residency;    // processId -> its resident objects
  // What each node published of its tenants' Durable Object bytes, and what this node published last.
  std::map<std::string, std::map<std::string, uint64_t>> publishedBytes;  // nodeId -> tenant -> bytes
  double lastUsagePublishMs = 0, lastUsageReadMs = 0;
  uint64_t usagePublishes = 0, usagePublishFailures = 0, usageReads = 0;
  std::map<std::string, Channel> channels;       // processId -> channel
  std::map<std::string, PeerStream> peers;       // nodeId -> stream
  uint64_t nextPeerSession = 0;
  bool stopped = false;
  std::map<uint64_t, InFlight> inFlight;         // nodeSeq -> what waits for its peers
  std::map<uint64_t, PeerBuffer> incoming;       // streamId -> what this node buffers as a peer
  uint64_t nextStreamId = 0, incomingBytes = 0;
  // All objects share the transfer concurrency used to size a tail page's budget.
  uint32_t activeTailFetches = 0;
  struct TailFetchWaiter {
    kj::Own<kj::PromiseFulfiller<void>> ready;
    bool* acquired = nullptr;  // Valid while ready has a waiting observer.
  };
  std::deque<TailFetchWaiter> tailFetchWaiters;
  // Healing pins this logical object's manifest revision. Existing local
  // background writes finish first; new ones keep their records for resealing.
  struct ManifestGate {
    uint32_t writers = 0, healers = 0;
    std::vector<kj::Own<kj::PromiseFulfiller<void>>> drained;
  };
  std::map<std::pair<std::string, std::string>, ManifestGate> manifestGates;
  bool healingManifest(const Object& object) const;

  // The promises' cancellation callbacks use the gate and settled maps: both
  // must outlive the last pending operation during process shutdown.
  std::set<std::string> settledRebuilds;
  std::map<std::string, kj::ForkedPromise<void>> rebuilding;
  std::map<std::string, std::unique_ptr<kj::Canceler>> rebuildCancelers;

  // Counters for the state endpoint.
  uint64_t confirmations = 0, confirmFailures = 0, refusals = 0, isolations = 0, segments = 0, segmentBytes = 0;
  uint64_t uploads = 0, uploadFailures = 0, manifestWrites = 0, manifestConflicts = 0, manifestFailures = 0;
  uint64_t snapshots = 0, snapshotFailures = 0, snapshotBytesWritten = 0, checkpoints = 0, checkpointAsks = 0;
  uint64_t rebuilds = 0, rebuildFailures = 0, repairs = 0, filesDropped = 0, peerSwitches = 0, tailFetches = 0;
  uint64_t recordsTaken = 0, framesTaken = 0, bytesTaken = 0, overLimitAnswers = 0, recoverTimeouts = 0;
  uint64_t tailRecoveryBounded = 0, ownerSilentRecoveries = 0, lateTailRecords = 0;
  uint64_t transferredTailRecords = 0, unaccountedTailRecords = 0, unaccountedTailBytes = 0;
  uint64_t restoredUnaccountedRecords = 0, restoredUnaccountedBytes = 0;
  uint64_t restoredEmptyAnswers = 0, restoredLogUnavailableAnswers = 0, localLogEmptyClosures = 0;
  uint64_t ownerLogUnavailableRecoveries = 0;
  uint64_t healSnapshots = 0, healFailures = 0, healedWalFrames = 0, objectFailures = 0;
  bool healBusy = false;
  uint32_t requestedHealWaiters = 0;
  uint64_t tailAnswers = 0, tailRefusals = 0, fencedRecords = 0, bufferRefusals = 0, recordLimitRefusals = 0;
  uint64_t walCutBacks = 0, positionMismatches = 0, manifestFences = 0, replayStops = 0, diskUsedBytes = 0;
  uint64_t alarmsSet = 0, alarmsCleared = 0, alarmsFired = 0, alarmFailures = 0, alarmRetries = 0;
  uint64_t alarmsNotDue = 0, alarmsAdopted = 0, alarmHintWrites = 0, alarmHintFailures = 0, alarmsDropped = 0;
  uint64_t positionsReused = 0, positionWrites = 0, quotaRefusals = 0;
  bool sealing = false, noPeerWarned = false;
  bool sealingForRecovery = false;
  uint32_t recoveryPublishWaiters = 0;
  kj::Canceler uploadRetryCanceler;
  double confirmMsTotal = 0, lastSegmentAtMs = 0, lastUploadAtMs = 0, lastRebuildMs = 0;
  std::vector<double> confirmSamples;
  std::string lastUploadError, lastManifestError, lastRebuildError, startupNote;
  bool diskTrusted = true;
  bool sameBootCache = false;
  std::string previousCacheBootId;
  std::optional<std::string> cacheMarkerBootOverride;
  bool startupAttempted = false, startupReady = false, startupMarkerRead = false, startupInitializing = false;
  bool recoveryReadIncomplete = false;
  uint64_t startupMarkerFailures = 0;
  uint64_t startupCleanupFailures = 0;
  uint64_t startupHistoryRecords = 0, startupFencedRecords = 0, startupAdoptRecords = 0, startupReadFallbackRecords = 0;
  uint64_t startupSegmentReads = 0, startupOldManifestReads = 0;
  double startupClassificationMs = 0;
  double startupLogReadMs = 0;
  uint64_t startupLogPeakRssBytes = 0, startupPeakRssBytes = 0;
  double startupRetryAfterMs = 0;
  uint64_t diskLimitBytes = 0;
  kj::TaskSet tasks{*this};
};

}  // namespace coworkerd

#pragma once

#include "object-store.h"

#include <kj/async-io.h>

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

// The cluster's state on the object store, above the client (docs/s3-truth-and-durable-objects.md, sections 1 and 2):
// revision sequences of immutable objects, the clock check that liveness by the store's own clock relies on, the
// membership's objects and how a listing of them reads, and the measurement of whether the store's create-if-absent
// holds under concurrency.
namespace coworkerd {

// A revision sequence: immutable objects <prefix>/<n>, n = 1, 2, ..., the number 20 decimal digits wide so that key
// order is revision order, each written only if absent -- whoever creates <prefix>/<n> is its n-th writer.
constexpr size_t REVISION_DIGITS = 20;
std::string revisionKey(const std::string& prefix, uint64_t revision);
// The revision of <prefix>/<n> (exactly 20 digits, n > 0); none for any other key.
std::optional<uint64_t> parseRevision(const std::string& key, const std::string& prefix);
// The largest revision of the prefix in a listing; 0 when it has none.
uint64_t latestRevision(const ObjectListing& listing, const std::string& prefix);

struct AppendResult {
  uint64_t revision = 0;
  uint32_t attempts = 0;
  uint64_t dateUnixMs = 0;
};
// Lists the prefix and creates <prefix>/<latest + 1>; when another writer created that one first, waits a jittered
// 5 to 160 ms, lists again and retries, at most `maxAttempts` times in all. Correct only where the store's
// create-if-absent holds under concurrency (measureConditionalPut).
kj::Promise<AppendResult> appendRevision(ObjectStore& store, kj::Timer& timer, std::string prefix, std::string bytes,
    uint32_t maxAttempts = 16);

// A revision sequence with a hint (the desired state, tenant-manifest.h): the hint <...>/current names the current
// revision, and only says so -- a reader reads that revision's object, and lists the prefix when the hint is missing or
// behind what the reader already knows.
struct LatestRevision {
  uint64_t revision = 0;  // 0: the sequence has no revision
  std::string bytes;
  bool listed = false;    // the hint was missing or behind: a listing found the revision
};
// The revision a hint names; none when the hint is missing or does not read as one.
kj::Promise<std::optional<uint64_t>> readHintRevision(ObjectStore& store, std::string hintKey);
// The sequence's latest revision and its bytes: the hint's when it names at least `floor`, else the largest a listing
// names. With `probe`, later revisions are looked for one by one (a writer's hint may lag its revision): GET n + 1 until
// one is missing.
kj::Promise<LatestRevision> readLatestRevision(ObjectStore& store, std::string prefix, std::string hintKey,
    uint64_t floor = 0, bool probe = false);
// The hint after revision `revision` was created: written, then whether a later revision exists is checked (another
// writer may have created one and written its hint before this one), and the hint rewritten to the latest if so.
// Returns the revision the hint names in the end.
kj::Promise<uint64_t> publishHint(ObjectStore& store, std::string prefix, std::string hintKey, uint64_t revision);
// Old revisions go: revision - keep is removed (one per write keeps a sequence at `keep` objects).
kj::Promise<void> pruneRevision(ObjectStore& store, std::string prefix, uint64_t revision, uint64_t keep);

// This machine's real clock, as Unix milliseconds: never the test offset (clockOffsetMsForTest).
uint64_t realClockUnixMs();

// One response of the store against the real clock: when its request was sent and its answer came by this machine's
// clock, and the Date header it carried (whole seconds).
struct ClockSample {
  uint64_t sentUnixMs = 0, receivedUnixMs = 0, dateUnixMs = 0;
};
// The store's clock minus this machine's. The store stamped its Date some time between the request and the answer
// and dropped the milliseconds, so the difference lies between Date - received and Date + 1000 - sent: `skewMs` is
// the middle of that range, and the check fails only when the whole range lies beyond the tolerance, so that a
// clock within it is never refused for the Date's lost milliseconds or the round trip.
struct ClockCheck {
  int64_t skewMs = 0, minSkewMs = 0, maxSkewMs = 0;
  bool exceeds = false;
  bool readable = false;  // the answer carried a Date header
};
ClockCheck checkClock(const ClockSample& sample, uint32_t toleranceMs);

// ---- The membership's objects ----
//   members/<nodeId>/epochs/<epoch>     an epoch's registration: created only if absent, then read back
//   members/<nodeId>/<epoch>            the epoch's heartbeat, overwritten every heartbeat interval
//   members/<nodeId>/<epoch>.draining   its drain marker (seen in a listing; nobody reads its bytes)
//   members/<nodeId>/<epoch>.suspects   its suspects (node/suspects.h): written while it has any, removed when it has none;
//                                       the other members read it in their listing cycle when its ETag changed
//   members/<nodeId>/probe/...          the node's own create-if-absent measurement at startup, removed after
// <epoch> is written 20 digits wide in keys; the epoch is the positive integer (in call ids, `<nodeId>.<epoch>-...`).
constexpr const char* MEMBERS_PREFIX = "members/";
std::string memberPrefix(const std::string& nodeId);  // members/<nodeId>/
std::string memberEpochsPrefix(const std::string& nodeId);  // members/<nodeId>/epochs (a revision sequence)
std::string memberRegistrationKey(const std::string& nodeId, uint64_t epoch);
std::string memberHeartbeatKey(const std::string& nodeId, uint64_t epoch);
std::string memberDrainKey(const std::string& nodeId, uint64_t epoch);
std::string memberSuspectsKey(const std::string& nodeId, uint64_t epoch);
std::string memberProbePrefix(const std::string& nodeId);  // members/<nodeId>/probe/

// What a listing of members/ says about one epoch of one node: when its registration was created, when its heartbeat
// was last written, whether it announced its drain -- all by the store's clock -- and the Date of the page that
// listed its heartbeat.
struct ListedEpoch {
  uint64_t epoch = 0;
  uint64_t registeredUnixMs = 0, heartbeatUnixMs = 0, pageDateUnixMs = 0;
  bool registered = false, heartbeat = false, draining = false, suspects = false;
  uint64_t drainKeyUnixMs = 0, suspectsUnixMs = 0;
  std::string suspectsEtag;
};
struct ListedNode {
  std::string id;
  std::map<uint64_t, ListedEpoch> epochs;
  uint64_t latest() const { return epochs.empty() ? 0 : epochs.rbegin()->first; }
};
// Every node of a listing of members/ (or of one node's prefix), its epochs in order. Keys of any other shape are
// not members' and are counted in `ignored`.
std::map<std::string, ListedNode> readMemberListing(const ObjectListing& listing, uint64_t* ignored = nullptr);

// Liveness by the store's clock: a heartbeat whose LastModified is more than `ttlSeconds` + `graceMs` behind the
// Date of the page that listed it is out. A node is its newest live epoch.
struct LiveMember {
  std::string id;
  uint64_t epoch = 0;
  uint64_t heartbeatUnixMs = 0;
  int64_t ageMs = 0;
  bool draining = false;
};
std::vector<LiveMember> liveMembers(const std::map<std::string, ListedNode>& nodes, uint32_t ttlSeconds, uint32_t graceMs);

// Whether an identity is in use: one of its heartbeats, or its newest registration, is less than `ttlSeconds` old
// by the listing's Date. `freshestUnixMs` is the newest of those times and `ageMs` its age.
struct IdentityUse {
  bool inUse = false;
  uint64_t freshestUnixMs = 0;
  int64_t ageMs = 0;
  uint64_t freshestEpoch = 0;
};
IdentityUse identityUse(const ListedNode& node, uint64_t dateUnixMs, uint32_t ttlSeconds);

// A heartbeat's value (and, without address and state, a registration's). `instance` names the process that wrote
// it: a random value of each Membership, never reused, so that a process can tell its own objects from another's.
struct MemberRecord {
  std::string nodeId, address, state = "active", instance;
  uint64_t epoch = 0, startedAtUnixMs = 0, writtenAtUnixMs = 0;
};
std::string memberRecordJson(const MemberRecord& record);
// The record in `bytes`, or none when they are not one (the reason in `error`).
std::optional<MemberRecord> parseMemberRecord(const std::string& bytes, std::string* error = nullptr);

// The live members as the control plane and the deployment tool read them: one listing of members/ and, for each live
// (node, epoch), its heartbeat's record for the address -- read once per epoch when a cache is given. Records that
// cannot be read, or name another node or epoch, leave their member out.
struct StoreMember {
  std::string id, address, state;
  uint64_t epoch = 0, startedAtUnixMs = 0, heartbeatUnixMs = 0;
  int64_t heartbeatAgeMs = 0;
};
using StoreMemberCache = std::map<std::pair<std::string, uint64_t>, MemberRecord>;
kj::Promise<std::vector<StoreMember>> readStoreMembers(ObjectStore& store, uint32_t ttlSeconds, uint32_t graceMs,
    StoreMemberCache* cache = nullptr);

// ---- Whether the store's create-if-absent holds under concurrency ----
// `writers` concurrent creates of one key, in `rounds` rounds over as many keys under `prefix` (every round at once:
// a store that serializes the writers of one key lets them in one by one, and the rounds do not wait for each other),
// then every key removed. Atomic when every round had exactly one writer told "created".
struct ConditionalPutMeasurement {
  uint32_t writers = 0, rounds = 0;
  uint32_t roundsWithExactlyOne = 0, roundsWithMore = 0, roundsWithNone = 0;
  std::vector<uint32_t> createdPerRound;
  std::map<std::string, uint64_t> statuses;  // "200", "412", "409", "error"
  uint64_t elapsedMs = 0;
  bool atomic() const { return rounds > 0 && roundsWithExactlyOne == rounds; }
  std::string json() const;
};
kj::Promise<ConditionalPutMeasurement> measureConditionalPut(ObjectStore& store, std::string prefix, uint32_t writers,
    uint32_t rounds);

}  // namespace coworkerd

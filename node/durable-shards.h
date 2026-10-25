#pragma once

#include "membership.h"
#include "object-store.h"

#include <kj/async-io.h>

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>

// Durable Object shards and who owns them (docs/cluster-state.md, section 11). Every namespace has a fixed number of
// shards; an object falls into one by its id. A shard has one owner in the cluster at a time: the node that created the
// shard's latest epoch, an object of the store created only if absent, and nothing else. A node takes a shard when a
// request to it first comes and no live node owns it; it serves the shard's objects on its own durable process while it
// holds it, lets it go when it drains (a release marker beside the epoch) and gives every shard up at once when its own
// membership expires. Other nodes send the shard's requests to the owner with the epoch they expect, which the owner
// checks. The objects' data stays on the owner's disk: a shard another node takes over starts its objects from empty.
namespace coworkerd {

// The shard of an object: the first 8 bytes of its id (64 hexadecimal digits) as a big-endian number, modulo the
// namespace's shards -- the same on every node.
uint32_t durableShardOf(const std::string& objectId, uint32_t shards);

// ---- The shards' objects ----
//   do/<ns>/shards/<s>/epochs/<n>            the shard's n-th ownership, created only if absent (n 20 digits wide)
//   do/<ns>/shards/<s>/epochs/<n>.released   its release marker: the owner of epoch n let the shard go (overwritten)
//   do/<ns>/shards/<s>/owner                 the hint: the latest epoch, its owner and whether it was released
std::string shardEpochsPrefix(const std::string& namespaceId, uint32_t shard);  // do/<ns>/shards/<s>/epochs
std::string shardEpochKey(const std::string& namespaceId, uint32_t shard, uint64_t epoch);
std::string shardReleasedKey(const std::string& namespaceId, uint32_t shard, uint64_t epoch);
std::string shardOwnerKey(const std::string& namespaceId, uint32_t shard);

// One ownership of a shard: who took it -- the node and its membership epoch --, when, why ("first", "timeout",
// "released", "restart"), and who wrote it, the process's instance and a random value of the write (a create whose answer
// was lost is decided by reading these bytes back); the epoch before it and its owner; and `dataEpoch`, the epoch since
// which the owner's own copy of the shard's objects is the one: the previous epoch's when that one was the same node's,
// else this one.
struct ShardEpoch {
  std::string namespaceId, nodeId, reason, instance, nonce, previousNodeId;
  uint32_t shard = 0;
  uint64_t epoch = 0, nodeEpoch = 0, dataEpoch = 0, acquiredAtUnixMs = 0, previousEpoch = 0, previousNodeEpoch = 0;
  // The store's LastModified of the object (not in its bytes).
  uint64_t lastModifiedUnixMs = 0;
};
std::string shardEpochJson(const ShardEpoch& record);
std::optional<ShardEpoch> parseShardEpoch(const std::string& bytes);

// What a node decided for a request to an object of one shard.
struct ShardRoute {
  enum class Kind { LOCAL, FORWARD, REFUSE };
  Kind kind = Kind::REFUSE;
  // LOCAL: the epoch this node holds the shard at and its data epoch. FORWARD: the node, its membership epoch, and the
  // shard's epoch it is expected to hold (0: it is asked to take the shard).
  std::string nodeId;
  uint64_t nodeEpoch = 0, epoch = 0, dataEpoch = 0;
  // REFUSE: the answer.
  uint status = 503;
  std::string code, reason;
  // How the owner was found: "held", "cache", "hint", "listing", "acquired", "designated".
  std::string source;
  static ShardRoute refuse(uint status, std::string code, std::string reason) {
    ShardRoute route;
    route.status = status; route.code = std::move(code); route.reason = std::move(reason);
    return route;
  }
};

class DurableShards final: private kj::TaskSet::ErrorHandler {
public:
  struct Settings {
    std::string nodeId;
    // This node's durable processes: none, and it takes no shard and is no shard's default owner.
    uint32_t processes = 1;
    // The membership is on the object store (a shard's epochs need it).
    bool objectStore = true;
    uint32_t ttlSeconds = 15, livenessGraceMs = 10000;
  };
  DurableShards(Settings settings, const ObjectStoreConfig& store, kj::Network& network, kj::Timer& timer,
      const MembershipView& membership, std::function<bool()> draining);
  ~DurableShards() noexcept(false);

  // Where a request to an object of shard `shard` goes, decided at the node where its caller runs. `retry`: the first
  // attempt went to `failedNode` and was refused, or could not be sent -- the shard is read from the store again (its
  // epochs listed) and its owner's liveness checked against the store. `designated`: the first attempt asked
  // `failedNode`, the shard's default owner, to take the shard and it could not: this node waits its turn (200 to 500 ms
  // when the shard has no epoch, its rank among the candidates times 1 s when its owner is gone), reads again and takes
  // the shard itself when it still has no live owner.
  kj::Promise<ShardRoute> route(std::string namespaceId, uint32_t shard, bool retry = false, std::string failedNode = {},
      bool designated = false);
  // The decision at the node another node sent such a request to, expecting it to hold the shard at `expectedEpoch` (0:
  // asked to take it).
  kj::Promise<ShardRoute> accept(std::string namespaceId, uint32_t shard, uint64_t expectedEpoch, std::string fromNode);
  // A request sent with `epoch` was accepted by its owner (the cache entry is current), or refused with `code`.
  void confirmed(const std::string& namespaceId, uint32_t shard, uint64_t epoch);
  void refusedBy(const std::string& namespaceId, uint32_t shard, const std::string& code);
  // A request was sent to another node (`designated`: asking it to take the shard), and how it ended.
  void forwarded(bool designated, const std::string& outcome);
  void countRefusal(const std::string& code);

  // This node's membership expired, or another process took its id: every shard it holds is given up -- nothing is
  // written, the store may be out of reach -- and the cache is emptied. Returns how many shards it held.
  uint32_t dropAll(const std::string& reason);
  // The drain: a release marker for every shard this node holds, and the hint, then none is held. Bounded by
  // `deadlineMonotonicMs`; a shard whose marker was not written is taken over once this node's membership is gone.
  struct Released {
    uint32_t shards = 0, written = 0, failed = 0;
    double elapsedMs = 0;
    uint64_t atUnixMs = 0;
    std::string json() const;
  };
  kj::Promise<Released> releaseAll(double deadlineMonotonicMs);

  // What this node knows of a shard's current epoch -- from what it holds, its cache or the hint it last read, never
  // a read of its own; 0 when it knows nothing. The records it takes as another node's peer are judged by it: an
  // owner whose epoch is older than this has been fenced (node/durable-log.h).
  uint64_t knownShardEpoch(const std::string& namespaceId, uint32_t shard) const;
  // Called for every shard this node stops holding (released, lost or dropped), so its objects' data is left alone.
  void onShardGone(std::function<void(const std::string& namespaceId, uint32_t shard, const std::string& reason)> callback);

  // Whether this node may hold shards ("" when it may): processes, the store's create-if-absent, the membership.
  std::string unavailable() const;
  size_t heldCount() const { return held.size(); }
  bool holds(const std::string& namespaceId, uint32_t shard) const { return held.contains({namespaceId, shard}); }
  // The state endpoint's section: the shards held, by namespace, the takeovers by reason, the releases, the refusals
  // by code, the forwarding, the cache and the store's work.
  std::string json() const;

private:
  using Key = std::pair<std::string, uint32_t>;
  struct Held {
    uint64_t epoch = 0, dataEpoch = 0, nodeEpoch = 0, acquiredAtUnixMs = 0;
    std::string reason;
  };
  struct Cached {
    ShardEpoch record;
    bool released = false;
    std::string source;
    kj::TimePoint verifiedAt = kj::origin<kj::TimePoint>();
  };
  // The shard's latest epoch as read from the store: none, or its record, and whether its owner released it.
  struct Latest {
    std::optional<ShardEpoch> record;
    bool released = false, listed = false;
    std::string source;
  };
  enum class Owner { ALIVE, DRAINING, GONE, SELF };

  void taskFailed(kj::Exception&& error) override;
  bool storeAtomic() const;
  // This node may take a shard now.
  bool eligible() const;
  uint64_t ownEpoch() const;
  // The shard's owner as this node's member table sees it.
  Owner ownerOf(const ShardEpoch& record) const;
  // The same, from a listing of the owner's own members/<id>/ now (the store's clock), for an owner the member table
  // still counts live but a request could not reach; the recovery anchor is kept.
  kj::Promise<Owner> probeOwner(const ShardEpoch& record);
  // The candidates for the shard's ownership, its default owner first: the members with durable processes that are
  // not draining, but `exclude`, in the rendezvous order of <ns>/<s> (Workers' homes are chosen the same way).
  std::vector<std::string> candidates(const std::string& namespaceId, uint32_t shard, const std::string& exclude) const;
  kj::Promise<Latest> readLatest(std::string namespaceId, uint32_t shard, bool viaHint);
  kj::Promise<ShardRoute> decide(std::string namespaceId, uint32_t shard, Latest latest, bool retry, std::string failedNode,
      bool designated, bool forwardedHere);
  kj::Promise<ShardRoute> takeOrForward(std::string namespaceId, uint32_t shard, Latest latest, std::string reason,
      std::string exclude, bool designated, bool forwardedHere);
  kj::Promise<ShardRoute> acquire(std::string namespaceId, uint32_t shard, Latest latest, std::string reason);
  kj::Promise<ShardRoute> acquireNow(std::string namespaceId, uint32_t shard, Latest latest, std::string reason);
  kj::Promise<ShardRoute> waitForAtomic();
  void writeHint(const std::string& namespaceId, uint32_t shard, const ShardEpoch& record, bool released);
  ShardRoute forwardTo(const ShardEpoch& record, const std::string& source);
  void log(const std::string& event, const std::string& namespaceId, uint32_t shard, const std::string& fields);

  Settings settings;
  // The node's own identity on the store (none without the object store membership).
  std::optional<ObjectStore> store;
  kj::Timer& timer;
  const MembershipView& membership;
  std::function<bool()> draining;
  std::map<Key, Held> held;
  std::map<Key, Cached> cache;
  // A first resolution of a shard in flight, and one after a refusal: requests for the same shard wait for it.
  std::map<Key, kj::ForkedPromise<ShardRoute>> resolving, retrying;
  // An acquisition of a shard in flight on this node.
  std::map<Key, kj::ForkedPromise<ShardRoute>> acquiring;
  // An owner's liveness probed against the store: (node, membership epoch) -> verdict and when.
  struct Probe {
    Owner owner = Owner::ALIVE;
    kj::TimePoint at = kj::origin<kj::TimePoint>();
  };
  std::map<std::pair<std::string, uint64_t>, Probe> probes;
  std::map<std::string, uint64_t> takeovers, refusals, forwards;
  std::function<void(const std::string&, uint32_t, const std::string&)> shardGone;
  uint64_t acquireAttempts = 0, acquireConflicts = 0, acquireFailures = 0, acquiredAfterReadBack = 0;
  uint64_t releases = 0, releaseFailures = 0, dropped = 0, drops = 0;
  uint64_t hintReads = 0, hintMisses = 0, hintsWritten = 0, hintsRepaired = 0, hintFailures = 0;
  uint64_t listings = 0, recordReads = 0, ownerProbes = 0, cacheHits = 0, cacheMisses = 0, waits = 0;
  std::string lastRelease = "null", lastDrop = "null";
  kj::TaskSet tasks{*this};
};

}  // namespace coworkerd

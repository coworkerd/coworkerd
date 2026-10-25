#include "durable-shards.h"
#include "json.h"
#include "object-state.h"
#include "support.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <openssl/rand.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace coworkerd {
namespace {
using json::quote;

// A cache entry no request confirmed for this long is read from the store again.
constexpr uint64_t CACHE_VERIFIED_MS = 60000;
// An owner's liveness probed against the store is believed for this long.
constexpr uint64_t PROBE_FRESH_MS = 1000;
// Creating the shard's next epoch: attempts before the node gives up (another writer kept creating it first).
constexpr uint32_t MAX_ACQUIRE_ATTEMPTS = 4;
// A request waits this long for the node's measurement of the store's create-if-absent, made at startup.
constexpr uint64_t ATOMIC_WAIT_MS = 10000;
// Release markers written at once.
constexpr size_t RELEASE_PARALLELISM = 8;

std::string randomHex(size_t bytes) {
  std::string value(bytes, '\0');
  KJ_REQUIRE(RAND_bytes(reinterpret_cast<unsigned char*>(value.data()), static_cast<int>(bytes)) == 1, "cannot draw random bytes");
  std::string out;
  for (unsigned char byte : value) { out += "0123456789abcdef"[byte >> 4]; out += "0123456789abcdef"[byte & 15]; }
  return out;
}
uint32_t randomBelow(uint32_t bound) {
  uint32_t value = 0;
  KJ_REQUIRE(RAND_bytes(reinterpret_cast<unsigned char*>(&value), sizeof(value)) == 1, "cannot draw random bytes");
  return bound ? value % bound : 0;
}
// A positive decimal number without a sign; 0 for anything else.
uint64_t decimal(const std::string& text) {
  if (text.empty() || text.size() > 20 || !std::all_of(text.begin(), text.end(), [](unsigned char c) { return std::isdigit(c); }))
    return 0;
  try { return std::stoull(text); } catch (...) { return 0; }
}
uint32_t hexDigit(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  KJ_FAIL_REQUIRE("not a hexadecimal digit");
}
double monotonicMs(kj::Timer& timer) { return (timer.now() - kj::origin<kj::TimePoint>()) / kj::NANOSECONDS / 1e6; }
std::string shortId(const std::string& namespaceId) { return namespaceId.substr(0, 12); }

struct Hint {
  uint64_t epoch = 0, nodeEpoch = 0;
  std::string nodeId;
  bool released = false;
};
std::string hintJson(const ShardEpoch& record, bool released) {
  return "{\"epoch\":" + quote(std::to_string(record.epoch)) + ",\"nodeId\":" + quote(record.nodeId) + ",\"nodeEpoch\":" +
      quote(std::to_string(record.nodeEpoch)) + ",\"released\":" + (released ? "true" : "false") + ",\"writtenAtUnixMs\":" +
      std::to_string(realClockUnixMs()) + "}";
}
std::optional<Hint> parseHint(const std::string& bytes) {
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
    auto reader = value.asReader();
    Hint hint;
    hint.epoch = decimal(json::text(reader, "epoch"));
    hint.nodeEpoch = decimal(json::text(reader, "nodeEpoch"));
    hint.nodeId = json::text(reader, "nodeId");
    hint.released = json::boolean(reader, "released");
    if (!hint.epoch || !hint.nodeEpoch || !safeName(hint.nodeId)) return std::nullopt;
    return hint;
  } catch (const kj::Exception&) {
    return std::nullopt;
  }
}
}  // namespace

uint32_t durableShardOf(const std::string& objectId, uint32_t shards) {
  KJ_REQUIRE(shards > 0 && objectId.size() >= 16, "an object id has 64 hexadecimal digits and a namespace at least one shard");
  uint64_t value = 0;
  for (size_t i = 0; i < 16; ++i) value = value << 4 | hexDigit(objectId[i]);
  return static_cast<uint32_t>(value % shards);
}

std::string shardEpochsPrefix(const std::string& namespaceId, uint32_t shard) {
  return "do/" + namespaceId + "/shards/" + std::to_string(shard) + "/epochs";
}
std::string shardEpochKey(const std::string& namespaceId, uint32_t shard, uint64_t epoch) {
  return revisionKey(shardEpochsPrefix(namespaceId, shard), epoch);
}
std::string shardReleasedKey(const std::string& namespaceId, uint32_t shard, uint64_t epoch) {
  return shardEpochKey(namespaceId, shard, epoch) + ".released";
}
std::string shardOwnerKey(const std::string& namespaceId, uint32_t shard) {
  return "do/" + namespaceId + "/shards/" + std::to_string(shard) + "/owner";
}

std::string shardEpochJson(const ShardEpoch& record) {
  std::string out = "{\"namespaceId\":" + quote(record.namespaceId) + ",\"shard\":" + std::to_string(record.shard) +
      ",\"epoch\":" + quote(std::to_string(record.epoch)) + ",\"nodeId\":" + quote(record.nodeId) + ",\"nodeEpoch\":" +
      quote(std::to_string(record.nodeEpoch)) + ",\"acquiredAtUnixMs\":" + std::to_string(record.acquiredAtUnixMs) +
      ",\"reason\":" + quote(record.reason) + ",\"dataEpoch\":" + quote(std::to_string(record.dataEpoch));
  if (record.previousEpoch)
    out += ",\"previous\":{\"epoch\":" + quote(std::to_string(record.previousEpoch)) + ",\"nodeId\":" + quote(record.previousNodeId) +
        ",\"nodeEpoch\":" + quote(std::to_string(record.previousNodeEpoch)) + "}";
  return out + ",\"instance\":" + quote(record.instance) + ",\"nonce\":" + quote(record.nonce) + "}";
}

std::optional<ShardEpoch> parseShardEpoch(const std::string& bytes) {
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
    auto reader = value.asReader();
    ShardEpoch record;
    record.namespaceId = json::text(reader, "namespaceId");
    auto shard = json::field(reader, "shard");
    KJ_REQUIRE(shard.isNumber() && shard.getNumber() >= 0 && shard.getNumber() < 4294967296.0, "a shard is a number");
    record.shard = static_cast<uint32_t>(shard.getNumber());
    record.epoch = decimal(json::text(reader, "epoch"));
    record.nodeId = json::text(reader, "nodeId");
    record.nodeEpoch = decimal(json::text(reader, "nodeEpoch"));
    record.dataEpoch = decimal(json::text(reader, "dataEpoch"));
    record.reason = json::text(reader, "reason");
    record.instance = json::text(reader, "instance");
    record.nonce = json::text(reader, "nonce");
    if (auto at = json::field(reader, "acquiredAtUnixMs"); at.isNumber() && at.getNumber() > 0)
      record.acquiredAtUnixMs = static_cast<uint64_t>(at.getNumber());
    if (auto previous = json::field(reader, "previous"); previous.isObject()) {
      record.previousEpoch = decimal(json::text(previous, "epoch"));
      record.previousNodeId = json::text(previous, "nodeId");
      record.previousNodeEpoch = decimal(json::text(previous, "nodeEpoch"));
    }
    if (!record.epoch || !record.nodeEpoch || !record.dataEpoch || record.dataEpoch > record.epoch || !safeName(record.nodeId))
      return std::nullopt;
    return record;
  } catch (const kj::Exception&) {
    return std::nullopt;
  }
}

DurableShards::DurableShards(Settings settings, const ObjectStoreConfig& storeConfig, kj::Network& network, kj::Timer& timer,
    const MembershipView& membership, std::function<bool()> draining)
    : settings(std::move(settings)), timer(timer), membership(membership), draining(std::move(draining)) {
  if (this->settings.objectStore) store.emplace(storeConfig, network, timer);
}
DurableShards::~DurableShards() noexcept(false) = default;

void DurableShards::taskFailed(kj::Exception&& error) {
  std::cerr << "[do-shard] background write failed node=" << settings.nodeId << " error=" << error.getDescription().cStr() << std::endl;
}

void DurableShards::log(const std::string& event, const std::string& namespaceId, uint32_t shard, const std::string& fields) {
  std::cout << "[do-shard] {\"nodeId\":" << quote(settings.nodeId) << ",\"event\":" << quote(event) << ",\"namespaceId\":"
            << quote(shortId(namespaceId)) << ",\"shard\":" << shard << (fields.empty() ? "" : ",") << fields
            << ",\"observedAtUnixMs\":" << realClockUnixMs() << "}" << std::endl;
}

bool DurableShards::storeAtomic() const { return membership.conditionalPutAtomic == "true"; }

uint64_t DurableShards::ownEpoch() const { return decimal(membership.epoch); }

bool DurableShards::eligible() const {
  return settings.objectStore && store && settings.processes > 0 && storeAtomic() && !draining() && !membership.selfExpired &&
      !membership.identityTaken && ownEpoch() > 0;
}

std::string DurableShards::unavailable() const {
  if (!settings.objectStore) return "this node's membership is not on the object store, which a shard's ownership needs";
  if (settings.processes == 0) return "this node runs no durable process (durableObjects.processes 0)";
  if (membership.conditionalPutAtomic == "false")
    return "the object store's create-if-absent did not hold under concurrency (conditionalPutAtomic false)";
  if (!storeAtomic()) return "the object store's create-if-absent has not been measured yet";
  return {};
}

DurableShards::Owner DurableShards::ownerOf(const ShardEpoch& record) const {
  if (record.nodeId == settings.nodeId) return Owner::SELF;
  auto found = membership.members.find(record.nodeId);
  if (found != membership.members.end()) {
    const auto epoch = decimal(found->second.epoch);
    if (epoch == record.nodeEpoch) return found->second.state == "draining" ? Owner::DRAINING : Owner::ALIVE;
    // A newer epoch of the node replaced the owner's; an older one means the table has not seen the owner's yet.
    return epoch > record.nodeEpoch ? Owner::GONE : Owner::ALIVE;
  }
  // Not in the table: gone when the table was listed after the epoch was created -- a node that took a shard had a heartbeat
  // then and keeps it live a TTL plus the grace after its last one --, else the table may not know the node yet.
  if (!record.lastModifiedUnixMs) return Owner::ALIVE;
  return membership.lastListDateUnixMs >= record.lastModifiedUnixMs + 1000 ? Owner::GONE : Owner::ALIVE;
}

kj::Promise<DurableShards::Owner> DurableShards::probeOwner(const ShardEpoch& record) {
  const std::pair<std::string, uint64_t> key{record.nodeId, record.nodeEpoch};
  if (auto found = probes.find(key); found != probes.end() && timer.now() - found->second.at < PROBE_FRESH_MS * kj::MILLISECONDS)
    co_return found->second.owner;
  ++ownerProbes;
  auto listing = co_await store->list(memberPrefix(record.nodeId));
  auto nodes = readMemberListing(listing);
  Owner verdict = Owner::GONE;
  const int64_t limit = static_cast<int64_t>(settings.ttlSeconds) * 1000 + settings.livenessGraceMs;
  if (auto node = nodes.find(record.nodeId); node != nodes.end()) {
    bool replaced = false;
    for (const auto& [number, listed] : node->second.epochs) {
      if (number <= record.nodeEpoch || !listed.heartbeat) continue;
      const int64_t age = listed.pageDateUnixMs ? static_cast<int64_t>(listed.pageDateUnixMs) - static_cast<int64_t>(listed.heartbeatUnixMs) : 0;
      if (age <= limit) replaced = true;
    }
    auto own = node->second.epochs.find(record.nodeEpoch);
    if (!replaced && own != node->second.epochs.end() && own->second.heartbeat) {
      const auto date = own->second.pageDateUnixMs;
      const int64_t age = date ? static_cast<int64_t>(date) - static_cast<int64_t>(own->second.heartbeatUnixMs) : 0;
      // The recovery anchor: this node was blind longer than the TTL and recovered; the staleness of that time is not
      // counted until the anchor passes (the member table keeps such a member the same way).
      const bool anchored = membership.recoveredUntilUnixMs && date && date < membership.recoveredUntilUnixMs;
      if (age <= limit || anchored) verdict = own->second.draining ? Owner::DRAINING : Owner::ALIVE;
    }
  }
  probes[key] = Probe{verdict, timer.now()};
  if (probes.size() > 1024) probes.erase(probes.begin());
  co_return verdict;
}

std::vector<std::string> DurableShards::candidates(const std::string& namespaceId, uint32_t shard, const std::string& exclude) const {
  std::vector<std::string> ids;
  for (const auto& [id, member] : membership.members) {
    if (id == exclude || member.durableProcesses == 0 || member.state == "draining") continue;
    if (id == settings.nodeId && !eligible()) continue;
    ids.push_back(id);
  }
  return rendezvousOrder(namespaceId + "/" + std::to_string(shard), ids);
}

kj::Promise<ShardRoute> DurableShards::waitForAtomic() {
  if (!settings.objectStore || !store)
    co_return ShardRoute::refuse(503, "DO_UNSUPPORTED_STORE", unavailable());
  const auto until = timer.now() + ATOMIC_WAIT_MS * kj::MILLISECONDS;
  while (membership.conditionalPutAtomic != "true" && membership.conditionalPutAtomic != "false" && timer.now() < until)
    co_await timer.afterDelay(50 * kj::MILLISECONDS);
  if (membership.conditionalPutAtomic == "false")
    co_return ShardRoute::refuse(503, "DO_UNSUPPORTED_STORE", "Durable Objects are not supported on this object store: " + unavailable() +
        "; a shard's single owner relies on it");
  if (membership.conditionalPutAtomic != "true")
    co_return ShardRoute::refuse(503, "STORAGE_UNAVAILABLE", unavailable() + " (" + membership.conditionalPutAtomic + ")");
  co_return ShardRoute{ShardRoute::Kind::LOCAL};
}

ShardRoute DurableShards::forwardTo(const ShardEpoch& record, const std::string& source) {
  ShardRoute route;
  route.kind = ShardRoute::Kind::FORWARD;
  route.nodeId = record.nodeId;
  route.nodeEpoch = record.nodeEpoch;
  route.epoch = record.epoch;
  route.source = source;
  return route;
}

kj::Promise<ShardRoute> DurableShards::route(std::string namespaceId, uint32_t shard, bool retry, std::string failedNode,
    bool designated) {
  const Key key{namespaceId, shard};
  try {
    if (auto atomic = co_await waitForAtomic(); atomic.kind == ShardRoute::Kind::REFUSE) co_return atomic;
    if (membership.selfExpired || membership.identityTaken)
      co_return ShardRoute::refuse(503, "DO_OWNER_EXPIRED", "this node's membership expired: it holds no shard until it registers again");
    if (auto found = held.find(key); found != held.end()) {
      if (draining()) co_return ShardRoute::refuse(503, "DO_OWNER_DRAINING", "this node, the shard's owner, is draining");
      ShardRoute route;
      route.kind = ShardRoute::Kind::LOCAL;
      route.nodeId = settings.nodeId; route.nodeEpoch = found->second.nodeEpoch;
      route.epoch = found->second.epoch; route.dataEpoch = found->second.dataEpoch; route.source = "held";
      co_return route;
    }
    if (!retry && !designated) {
      if (auto cached = cache.find(key); cached != cache.end()) {
        const auto owner = ownerOf(cached->second.record);
        const bool fresh = timer.now() - cached->second.verifiedAt <= CACHE_VERIFIED_MS * kj::MILLISECONDS;
        if (fresh && !cached->second.released && (owner == Owner::ALIVE || owner == Owner::DRAINING)) {
          ++cacheHits;
          co_return forwardTo(cached->second.record, "cache");
        }
        cache.erase(cached);
      }
      ++cacheMisses;
      // Requests for one shard share its first resolution.
      if (auto running = resolving.find(key); running != resolving.end()) {
        ++waits;
        co_return co_await running->second.addBranch();
      }
      auto first = [](DurableShards& self, std::string namespaceId, uint32_t shard) -> kj::Promise<ShardRoute> {
        auto latest = co_await self.readLatest(namespaceId, shard, true);
        co_return co_await self.decide(namespaceId, shard, std::move(latest), false, {}, false, false);
      }(*this, namespaceId, shard).fork();
      auto branch = first.addBranch();
      resolving.emplace(key, kj::mv(first));
      KJ_DEFER(resolving.erase(key));
      co_return co_await branch;
    }
    cache.erase(key);
    // Requests for one shard that a refusal sent back share one reading of it too (a dead owner's shard is not listed once a
    // request).
    if (auto running = retrying.find(key); running != retrying.end()) {
      ++waits;
      co_return co_await running->second.addBranch();
    }
    auto again = [](DurableShards& self, std::string namespaceId, uint32_t shard, std::string failedNode, bool designated)
        -> kj::Promise<ShardRoute> {
      auto latest = co_await self.readLatest(namespaceId, shard, false);
      co_return co_await self.decide(namespaceId, shard, std::move(latest), true, failedNode, designated, false);
    }(*this, namespaceId, shard, failedNode, designated).fork();
    auto branch = again.addBranch();
    retrying.emplace(key, kj::mv(again));
    KJ_DEFER(retrying.erase(key));
    co_return co_await branch;
  } catch (const kj::Exception& error) {
    ++acquireFailures;
    const bool storage = isStorageFailure(error);
    log("unresolved", namespaceId, shard, "\"code\":\"" + std::string(storage ? "STORAGE_UNAVAILABLE" : "DO_UNAVAILABLE") +
        "\",\"error\":" + quote(error.getDescription().cStr()));
    co_return ShardRoute::refuse(503, storage ? "STORAGE_UNAVAILABLE" : "DO_UNAVAILABLE",
        std::string("the owner of the object's shard could not be found: ") + error.getDescription().cStr());
  }
}

kj::Promise<ShardRoute> DurableShards::accept(std::string namespaceId, uint32_t shard, uint64_t expectedEpoch, std::string fromNode) {
  const Key key{namespaceId, shard};
  try {
    if (auto atomic = co_await waitForAtomic(); atomic.kind == ShardRoute::Kind::REFUSE) co_return atomic;
    if (settings.processes == 0)
      co_return ShardRoute::refuse(503, "DO_UNAVAILABLE", "this node runs no durable process (durableObjects.processes 0)");
    if (membership.selfExpired || membership.identityTaken)
      co_return ShardRoute::refuse(503, "DO_OWNER_EXPIRED", "this node's membership expired: it holds no shard until it registers again");
    auto found = held.find(key);
    if (expectedEpoch) {
      if (found != held.end() && found->second.epoch == expectedEpoch) {
        if (draining()) co_return ShardRoute::refuse(503, "DO_OWNER_DRAINING", "this node, the shard's owner, is draining");
        ShardRoute route;
        route.kind = ShardRoute::Kind::LOCAL;
        route.nodeId = settings.nodeId; route.nodeEpoch = found->second.nodeEpoch;
        route.epoch = found->second.epoch; route.dataEpoch = found->second.dataEpoch; route.source = "held";
        co_return route;
      }
      co_return ShardRoute::refuse(409, "DO_SHARD_EPOCH_MISMATCH", "node " + settings.nodeId + (found == held.end()
          ? std::string(" does not hold the shard") : " holds the shard at epoch " + std::to_string(found->second.epoch)) +
          ", not at epoch " + std::to_string(expectedEpoch));
    }
    // Asked to take the shard: its default owner as the sender sees the members.
    if (draining()) co_return ShardRoute::refuse(503, "DO_OWNER_DRAINING", "this node is draining: it takes no shard");
    if (found != held.end()) {
      ShardRoute route;
      route.kind = ShardRoute::Kind::LOCAL;
      route.nodeId = settings.nodeId; route.nodeEpoch = found->second.nodeEpoch;
      route.epoch = found->second.epoch; route.dataEpoch = found->second.dataEpoch; route.source = "held";
      co_return route;
    }
    auto latest = co_await readLatest(namespaceId, shard, false);
    co_return co_await decide(namespaceId, shard, std::move(latest), false, fromNode, false, true);
  } catch (const kj::Exception& error) {
    ++acquireFailures;
    const bool storage = isStorageFailure(error);
    log("unresolved", namespaceId, shard, "\"code\":\"" + std::string(storage ? "STORAGE_UNAVAILABLE" : "DO_UNAVAILABLE") +
        "\",\"error\":" + quote(error.getDescription().cStr()) + ",\"from\":" + quote(fromNode));
    co_return ShardRoute::refuse(503, storage ? "STORAGE_UNAVAILABLE" : "DO_UNAVAILABLE",
        std::string("the shard could not be taken here: ") + error.getDescription().cStr());
  }
}

kj::Promise<DurableShards::Latest> DurableShards::readLatest(std::string namespaceId, uint32_t shard, bool viaHint) {
  Latest latest;
  std::optional<Hint> hint;
  if (viaHint) {
    ++hintReads;
    auto answer = co_await store->get(shardOwnerKey(namespaceId, shard));
    requireStorage(answer.status == 200 || answer.status == 404, "the shard's owner hint could not be read: HTTP " +
        std::to_string(answer.status));
    if (answer.status == 200) hint = parseHint(answer.bytes);
    if (!hint) ++hintMisses;
    if (hint) {
      ++recordReads;
      auto object = co_await store->get(shardEpochKey(namespaceId, shard, hint->epoch));
      requireStorage(object.status == 200 || object.status == 404, "the shard's epoch could not be read: HTTP " +
          std::to_string(object.status));
      std::optional<ShardEpoch> record;
      if (object.status == 200) record = parseShardEpoch(object.bytes);
      if (record && record->epoch == hint->epoch && record->namespaceId == namespaceId && record->shard == shard) {
        record->lastModifiedUnixMs = object.lastModifiedUnixMs;
        // A hint that names another owner than the epoch does: the epoch is the truth, and the hint is written again.
        if (hint->nodeId != record->nodeId || hint->nodeEpoch != record->nodeEpoch) {
          ++hintsRepaired;
          writeHint(namespaceId, shard, *record, hint->released);
        }
        latest.record = std::move(record);
        latest.released = hint->released;
        latest.source = "hint";
        co_return latest;
      }
      ++hintMisses;
    }
  }
  ++listings;
  const auto prefix = shardEpochsPrefix(namespaceId, shard);
  auto listing = co_await store->list(prefix + "/");
  uint64_t newest = 0, newestModified = 0;
  std::set<uint64_t> released;
  for (const auto& object : listing.objects) {
    if (auto epoch = parseRevision(object.key, prefix)) {
      if (*epoch > newest) { newest = *epoch; newestModified = object.lastModifiedUnixMs; }
    } else if (object.key.ends_with(".released")) {
      if (auto epoch = parseRevision(object.key.substr(0, object.key.size() - 9), prefix)) released.insert(*epoch);
    }
  }
  latest.listed = true;
  latest.source = "listing";
  if (!newest) co_return latest;
  ++recordReads;
  auto object = co_await store->get(shardEpochKey(namespaceId, shard, newest));
  requireStorage(object.status == 200, "the shard's latest epoch could not be read: HTTP " + std::to_string(object.status));
  auto record = parseShardEpoch(object.bytes);
  requireStorage(record && record->epoch == newest && record->namespaceId == namespaceId && record->shard == shard,
      "the shard's latest epoch does not read as one");
  record->lastModifiedUnixMs = newestModified;
  latest.released = released.contains(newest);
  // The hint follows what the listing found when it was missing, unreadable or named another epoch.
  if (!hint || hint->epoch != newest || hint->nodeId != record->nodeId || hint->released != latest.released) {
    if (viaHint) ++hintsRepaired;
    writeHint(namespaceId, shard, *record, latest.released);
  }
  latest.record = std::move(record);
  co_return latest;
}

kj::Promise<ShardRoute> DurableShards::decide(std::string namespaceId, uint32_t shard, Latest latest, bool retry,
    std::string failedNode, bool designated, bool forwardedHere) {
  if (!latest.record) co_return co_await takeOrForward(namespaceId, shard, std::move(latest), "first", {}, designated, forwardedHere);
  // Taking a shard over is decided on a listing only: a hint can lag its latest epoch.
  if (!latest.listed && (latest.released || ownerOf(*latest.record) != Owner::ALIVE)) {
    latest = co_await readLatest(namespaceId, shard, false);
    co_return co_await decide(namespaceId, shard, std::move(latest), retry, failedNode, designated, forwardedHere);
  }
  const auto record = *latest.record;
  if (latest.released)
    co_return co_await takeOrForward(namespaceId, shard, std::move(latest), "released", record.nodeId, designated, forwardedHere);
  auto owner = ownerOf(record);
  if (owner == Owner::SELF) {
    // An epoch of this node's id it does not hold (the holdings were checked first): an earlier membership epoch -- this
    // process expired and registered again, or the node started again --, taken back at once with a new epoch.
    if (forwardedHere || eligible()) co_return co_await acquire(namespaceId, shard, std::move(latest), "restart");
    co_return co_await takeOrForward(namespaceId, shard, std::move(latest), "restart", record.nodeId, designated, forwardedHere);
  }
  // An owner the member table counts live that a request could not reach, or that this node was asked to replace: its
  // liveness from the store now.
  if (owner == Owner::ALIVE && (forwardedHere || (retry && failedNode == record.nodeId))) owner = co_await probeOwner(record);
  if (owner == Owner::ALIVE || owner == Owner::DRAINING) {
    if (forwardedHere)
      co_return ShardRoute::refuse(409, "DO_SHARD_EPOCH_MISMATCH", "the shard is held by node " + record.nodeId + " at epoch " +
          std::to_string(record.epoch));
    cache[{namespaceId, shard}] = {record, false, latest.source, timer.now()};
    co_return forwardTo(record, latest.source);
  }
  co_return co_await takeOrForward(namespaceId, shard, std::move(latest), "timeout", record.nodeId, designated, forwardedHere);
}

kj::Promise<ShardRoute> DurableShards::takeOrForward(std::string namespaceId, uint32_t shard, Latest latest, std::string reason,
    std::string exclude, bool designated, bool forwardedHere) {
  if (forwardedHere) co_return co_await acquire(namespaceId, shard, std::move(latest), reason);
  auto order = candidates(namespaceId, shard, exclude);
  if (designated) {
    // The default owner could not take the shard: this node's turn, then a read of the shard first.
    auto at = std::find(order.begin(), order.end(), settings.nodeId);
    if (at == order.end())
      co_return ShardRoute::refuse(503, "DO_UNAVAILABLE", "the shard's default owner could not take it, and this node cannot (" +
          (eligible() ? std::string("not a candidate") : unavailable().empty() ? std::string("draining") : unavailable()) + ")");
    const uint64_t pause = reason == "first" ? 200 + randomBelow(301) : static_cast<uint64_t>(at - order.begin()) * 1000;
    ++waits;
    co_await timer.afterDelay(pause * kj::MILLISECONDS);
    latest = co_await readLatest(namespaceId, shard, false);
    if (latest.record && !latest.released) {
      const auto owner = ownerOf(*latest.record);
      if (owner == Owner::ALIVE || owner == Owner::DRAINING) {
        cache[{namespaceId, shard}] = {*latest.record, false, latest.source, timer.now()};
        co_return forwardTo(*latest.record, latest.source);
      }
      if (owner == Owner::SELF) reason = "restart";
    } else if (latest.record) {
      reason = "released";
    }
    co_return co_await acquire(namespaceId, shard, std::move(latest), reason);
  }
  if (order.empty())
    co_return ShardRoute::refuse(503, "DO_UNAVAILABLE", "no member that runs durable processes can own the shard");
  if (order.front() == settings.nodeId) co_return co_await acquire(namespaceId, shard, std::move(latest), reason);
  // The default owner is asked to take it.
  const auto& member = membership.members.at(order.front());
  ShardRoute route;
  route.kind = ShardRoute::Kind::FORWARD;
  route.nodeId = order.front();
  route.nodeEpoch = decimal(member.epoch);
  route.epoch = 0;
  route.source = "designated";
  route.reason = reason;
  co_return route;
}

kj::Promise<ShardRoute> DurableShards::acquire(std::string namespaceId, uint32_t shard, Latest latest, std::string reason) {
  // This node's requests for one shard -- its own callers' and those other nodes asked it to take -- share one acquisition.
  const Key key{namespaceId, shard};
  if (auto running = acquiring.find(key); running != acquiring.end()) {
    ++waits;
    co_return co_await running->second.addBranch();
  }
  auto forked = acquireNow(namespaceId, shard, std::move(latest), reason).fork();
  auto branch = forked.addBranch();
  acquiring.emplace(key, kj::mv(forked));
  KJ_DEFER(acquiring.erase(key));
  co_return co_await branch;
}

kj::Promise<ShardRoute> DurableShards::acquireNow(std::string namespaceId, uint32_t shard, Latest latest, std::string reason) {
  const Key key{namespaceId, shard};
  if (!eligible())
    co_return membership.selfExpired || membership.identityTaken
        ? ShardRoute::refuse(503, "DO_OWNER_EXPIRED", "this node's membership expired: it takes no shard")
        : draining() ? ShardRoute::refuse(503, "DO_OWNER_DRAINING", "this node is draining: it takes no shard")
        : ShardRoute::refuse(503, "DO_UNAVAILABLE", "this node takes no shard: " + unavailable());
  const auto started = monotonicMs(timer);
  for (uint32_t attempt = 1; attempt <= MAX_ACQUIRE_ATTEMPTS; ++attempt) {
    if (auto found = held.find(key); found != held.end()) {
      ShardRoute route;
      route.kind = ShardRoute::Kind::LOCAL;
      route.nodeId = settings.nodeId; route.nodeEpoch = found->second.nodeEpoch;
      route.epoch = found->second.epoch; route.dataEpoch = found->second.dataEpoch; route.source = "held";
      co_return route;
    }
    ++acquireAttempts;
    ShardEpoch record;
    record.namespaceId = namespaceId;
    record.shard = shard;
    record.epoch = (latest.record ? latest.record->epoch : 0) + 1;
    record.nodeId = settings.nodeId;
    record.nodeEpoch = ownEpoch();
    record.acquiredAtUnixMs = realClockUnixMs();
    record.reason = reason;
    record.instance = membership.instance;
    record.nonce = randomHex(16);
    if (latest.record) {
      record.previousEpoch = latest.record->epoch;
      record.previousNodeId = latest.record->nodeId;
      record.previousNodeEpoch = latest.record->nodeEpoch;
    }
    // This node's own copy of the shard's objects is the one only when the epoch before was this node's.
    record.dataEpoch = latest.record && latest.record->nodeId == settings.nodeId ? latest.record->dataEpoch : record.epoch;
    auto created = co_await store->create(shardEpochKey(namespaceId, shard, record.epoch), shardEpochJson(record));
    if (created.created) {
      const auto elapsed = monotonicMs(timer) - started;
      // The membership expired, or the drain began, while the create was in flight: the epoch names this node's membership
      // epoch, which no longer serves; it is taken back, or over, later.
      if (!eligible() || ownEpoch() != record.nodeEpoch) {
        log("acquired-unused", namespaceId, shard, "\"epoch\":\"" + std::to_string(record.epoch) + "\",\"reason\":" + quote(reason));
        co_return membership.selfExpired || membership.identityTaken || ownEpoch() != record.nodeEpoch
            ? ShardRoute::refuse(503, "DO_OWNER_EXPIRED", "this node's membership expired while it took the shard")
            : ShardRoute::refuse(503, "DO_OWNER_DRAINING", "this node began draining while it took the shard");
      }
      held[key] = {record.epoch, record.dataEpoch, record.nodeEpoch, record.acquiredAtUnixMs, reason};
      cache.erase(key);
      ++takeovers[reason];
      if (created.readBack) ++acquiredAfterReadBack;
      std::ostringstream fields;
      fields << std::setprecision(6) << "\"epoch\":\"" << record.epoch << "\",\"nodeEpoch\":\"" << record.nodeEpoch << "\",\"reason\":"
             << quote(reason) << ",\"dataEpoch\":\"" << record.dataEpoch << "\",\"previous\":"
             << (latest.record ? "{\"epoch\":\"" + std::to_string(latest.record->epoch) + "\",\"nodeId\":" + quote(latest.record->nodeId) +
                 ",\"nodeEpoch\":\"" + std::to_string(latest.record->nodeEpoch) + "\",\"released\":" + (latest.released ? "true" : "false") + "}"
                 : std::string("null"))
             << ",\"attempts\":" << attempt << ",\"answerReadBack\":" << (created.readBack ? "true" : "false")
             << ",\"elapsedMs\":" << elapsed;
      log("acquired", namespaceId, shard, fields.str());
      writeHint(namespaceId, shard, record, false);
      ShardRoute route;
      route.kind = ShardRoute::Kind::LOCAL;
      route.nodeId = settings.nodeId; route.nodeEpoch = record.nodeEpoch;
      route.epoch = record.epoch; route.dataEpoch = record.dataEpoch; route.source = "acquired";
      co_return route;
    }
    // Another writer created that epoch first: read who, and follow a live owner.
    ++acquireConflicts;
    log("lost", namespaceId, shard, "\"epoch\":\"" + std::to_string(record.epoch) + "\",\"reason\":" + quote(reason) +
        ",\"attempt\":" + std::to_string(attempt));
    latest = co_await readLatest(namespaceId, shard, false);
    if (latest.record && !latest.released) {
      const auto owner = ownerOf(*latest.record);
      if (owner == Owner::ALIVE || owner == Owner::DRAINING) {
        cache[key] = {*latest.record, false, latest.source, timer.now()};
        co_return forwardTo(*latest.record, latest.source);
      }
    }
  }
  co_return ShardRoute::refuse(503, "DO_UNAVAILABLE", "the shard's next epoch was created by another writer at every attempt (" +
      std::to_string(MAX_ACQUIRE_ATTEMPTS) + ")");
}

void DurableShards::writeHint(const std::string& namespaceId, uint32_t shard, const ShardEpoch& record, bool released) {
  if (!store) return;
  tasks.add([](DurableShards& self, std::string key, std::string bytes) -> kj::Promise<void> {
    try {
      co_await self.store->put(std::move(key), std::move(bytes));
      ++self.hintsWritten;
    } catch (const kj::Exception& error) {
      ++self.hintFailures;
      std::cerr << "[do-shard] hint write failed node=" << self.settings.nodeId << " error=" << error.getDescription().cStr() << std::endl;
    }
  }(*this, shardOwnerKey(namespaceId, shard), hintJson(record, released)));
}

void DurableShards::confirmed(const std::string& namespaceId, uint32_t shard, uint64_t epoch) {
  auto found = cache.find({namespaceId, shard});
  if (found != cache.end() && found->second.record.epoch == epoch) found->second.verifiedAt = timer.now();
}

void DurableShards::refusedBy(const std::string& namespaceId, uint32_t shard, const std::string& code) {
  cache.erase({namespaceId, shard});
}

void DurableShards::forwarded(bool designated, const std::string& outcome) {
  ++forwards[std::string(designated ? "designated-" : "") + outcome];
}

void DurableShards::countRefusal(const std::string& code) { ++refusals[code]; }

uint32_t DurableShards::dropAll(const std::string& reason) {
  const auto count = static_cast<uint32_t>(held.size());
  std::ostringstream shards;
  size_t index = 0;
  for (const auto& [key, entry] : held)
    shards << (index++ ? "," : "") << "{\"namespaceId\":" << quote(shortId(key.first)) << ",\"shard\":" << key.second
           << ",\"epoch\":\"" << entry.epoch << "\"}";
  held.clear();
  cache.clear();
  probes.clear();
  dropped += count;
  ++drops;
  lastDrop = "{\"reason\":" + quote(reason) + ",\"shards\":" + std::to_string(count) + ",\"atUnixMs\":" +
      std::to_string(realClockUnixMs()) + "}";
  std::cout << "[do-shard] {\"nodeId\":" << quote(settings.nodeId) << ",\"event\":\"dropped\",\"reason\":" << quote(reason)
            << ",\"shards\":" << count << ",\"held\":[" << shards.str() << "],\"observedAtUnixMs\":" << realClockUnixMs() << "}"
            << std::endl;
  return count;
}

std::string DurableShards::Released::json() const {
  std::ostringstream out;
  out << std::setprecision(6) << "{\"shards\":" << shards << ",\"written\":" << written << ",\"failed\":" << failed
      << ",\"elapsedMs\":" << elapsedMs << ",\"atUnixMs\":" << atUnixMs << "}";
  return out.str();
}

kj::Promise<DurableShards::Released> DurableShards::releaseAll(double deadlineMonotonicMs) {
  Released result;
  const auto started = monotonicMs(timer);
  // A node whose id another process took, or whose membership expired, writes nothing: its shards are given up as they
  // are (the other nodes take them over once its membership is gone).
  if (membership.identityTaken || membership.selfExpired) {
    result.shards = static_cast<uint32_t>(held.size());
    result.failed = result.shards;
    dropAll(membership.identityTaken ? "identity-taken" : "membership-expired");
    result.atUnixMs = realClockUnixMs();
    lastRelease = result.json();
    co_return result;
  }
  std::vector<std::pair<Key, Held>> items(held.begin(), held.end());
  result.shards = static_cast<uint32_t>(items.size());
  auto one = [](DurableShards& self, Key key, Held entry) -> kj::Promise<bool> {
    const auto began = monotonicMs(self.timer);
    bool ok = true;
    std::string error;
    try {
      const auto at = realClockUnixMs();
      co_await self.store->put(shardReleasedKey(key.first, key.second, entry.epoch), "{\"namespaceId\":" + quote(key.first) +
          ",\"shard\":" + std::to_string(key.second) + ",\"epoch\":" + quote(std::to_string(entry.epoch)) + ",\"nodeId\":" +
          quote(self.settings.nodeId) + ",\"nodeEpoch\":" + quote(std::to_string(entry.nodeEpoch)) + ",\"releasedAtUnixMs\":" +
          std::to_string(at) + ",\"reason\":\"drain\"}");
      ShardEpoch record;
      record.epoch = entry.epoch; record.nodeId = self.settings.nodeId; record.nodeEpoch = entry.nodeEpoch;
      co_await self.store->put(shardOwnerKey(key.first, key.second), hintJson(record, true));
      ++self.hintsWritten;
    } catch (const kj::Exception& exception) {
      ok = false;
      error = exception.getDescription().cStr();
    }
    // Not this node's any more either way: the marker, or its membership leaving, lets another node take it.
    self.held.erase(key);
    std::ostringstream fields;
    fields << std::setprecision(6) << "\"epoch\":\"" << entry.epoch << "\",\"markerWritten\":" << (ok ? "true" : "false")
           << ",\"elapsedMs\":" << monotonicMs(self.timer) - began << (ok ? "" : ",\"error\":" + quote(error));
    self.log("released", key.first, key.second, fields.str());
    co_return ok;
  };
  for (size_t at = 0; at < items.size(); at += RELEASE_PARALLELISM) {
    if (store == std::nullopt || monotonicMs(timer) >= deadlineMonotonicMs) {
      for (size_t i = at; i < items.size(); ++i) held.erase(items[i].first);
      result.failed += static_cast<uint32_t>(items.size() - at);
      break;
    }
    kj::Vector<kj::Promise<bool>> batch;
    for (size_t i = at; i < std::min(items.size(), at + RELEASE_PARALLELISM); ++i) batch.add(one(*this, items[i].first, items[i].second));
    for (bool ok : co_await kj::joinPromises(batch.releaseAsArray())) (ok ? result.written : result.failed) += 1;
  }
  held.clear();
  cache.clear();
  releases += result.written;
  releaseFailures += result.failed;
  result.elapsedMs = monotonicMs(timer) - started;
  result.atUnixMs = realClockUnixMs();
  lastRelease = result.json();
  std::cout << "[do-shard] {\"nodeId\":" << quote(settings.nodeId) << ",\"event\":\"released-all\",\"release\":" << lastRelease
            << ",\"observedAtUnixMs\":" << realClockUnixMs() << "}" << std::endl;
  co_return result;
}

std::string DurableShards::json() const {
  auto counts = [](const std::map<std::string, uint64_t>& values) {
    std::string out = "{";
    for (const auto& [name, count] : values) out += (out.size() > 1 ? "," : "") + quote(name) + ":" + std::to_string(count);
    return out + "}";
  };
  std::map<std::string, std::vector<std::pair<uint32_t, Held>>> byNamespace;
  for (const auto& [key, entry] : held) byNamespace[key.first].emplace_back(key.second, entry);
  std::ostringstream out;
  const auto reason = unavailable();
  out << "{\"enabled\":" << (reason.empty() ? "true" : "false") << ",\"unavailable\":" << quote(reason)
      << ",\"processes\":" << settings.processes << ",\"conditionalPutAtomic\":" << quote(membership.conditionalPutAtomic)
      << ",\"held\":{\"count\":" << held.size() << ",\"namespaces\":{";
  size_t n = 0;
  for (const auto& [namespaceId, shards] : byNamespace) {
    out << (n++ ? "," : "") << quote(namespaceId) << ":{\"count\":" << shards.size() << ",\"shards\":[";
    for (size_t i = 0; i < shards.size(); ++i) {
      const auto& [shard, entry] = shards[i];
      out << (i ? "," : "") << "{\"shard\":" << shard << ",\"epoch\":\"" << entry.epoch << "\",\"dataEpoch\":\"" << entry.dataEpoch
          << "\",\"nodeEpoch\":\"" << entry.nodeEpoch << "\",\"reason\":" << quote(entry.reason) << ",\"acquiredAtUnixMs\":"
          << entry.acquiredAtUnixMs << "}";
    }
    out << "]}";
  }
  out << "}},\"takeovers\":" << counts(takeovers) << ",\"acquireAttempts\":" << acquireAttempts << ",\"acquireConflicts\":"
      << acquireConflicts << ",\"acquireFailures\":" << acquireFailures << ",\"acquiredAfterReadBack\":" << acquiredAfterReadBack
      << ",\"releases\":" << releases << ",\"releaseFailures\":" << releaseFailures << ",\"lastRelease\":" << lastRelease
      << ",\"dropped\":" << dropped << ",\"drops\":" << drops << ",\"lastDrop\":" << lastDrop
      << ",\"refusals\":" << counts(refusals) << ",\"forwards\":" << counts(forwards)
      << ",\"cache\":{\"entries\":" << cache.size() << ",\"hits\":" << cacheHits << ",\"misses\":" << cacheMisses << ",\"owners\":[";
  size_t listed = 0;
  for (const auto& [key, entry] : cache) {
    if (listed == 64) break;
    out << (listed++ ? "," : "") << "{\"namespaceId\":" << quote(shortId(key.first)) << ",\"shard\":" << key.second << ",\"nodeId\":"
        << quote(entry.record.nodeId) << ",\"nodeEpoch\":\"" << entry.record.nodeEpoch << "\",\"epoch\":\"" << entry.record.epoch
        << "\",\"source\":" << quote(entry.source) << ",\"verifiedMsAgo\":" << (timer.now() - entry.verifiedAt) / kj::MILLISECONDS << "}";
  }
  out << "]}"
      << ",\"hintReads\":" << hintReads << ",\"hintMisses\":" << hintMisses << ",\"hintsWritten\":" << hintsWritten
      << ",\"hintsRepaired\":" << hintsRepaired << ",\"hintFailures\":" << hintFailures << ",\"listings\":" << listings
      << ",\"recordReads\":" << recordReads << ",\"ownerProbes\":" << ownerProbes << ",\"waits\":" << waits
      << ",\"store\":" << (store ? store->stats().json() : std::string("null")) << "}";
  return out.str();
}

}  // namespace coworkerd

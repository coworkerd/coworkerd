#include "object-state.h"
#include "json.h"
#include "tenant-manifest.h"
#include "support.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <ctime>
#include <set>
#include <sstream>

namespace coworkerd {
namespace {
std::string padded(uint64_t value) {
  const auto digits = std::to_string(value);
  return std::string(REVISION_DIGITS - digits.size(), '0') + digits;
}
// A key segment that is exactly a 20-digit epoch, or none.
std::optional<uint64_t> epochSegment(std::string_view text) {
  if (text.size() != REVISION_DIGITS) return std::nullopt;
  uint64_t value = 0;
  for (char c : text) {
    if (!std::isdigit(static_cast<unsigned char>(c))) return std::nullopt;
    const uint64_t digit = c - '0';
    if (value > (UINT64_MAX - digit) / 10) return std::nullopt;
    value = value * 10 + digit;
  }
  if (value == 0) return std::nullopt;
  return value;
}
}  // namespace

std::string revisionKey(const std::string& prefix, uint64_t revision) {
  return prefix + '/' + padded(revision);
}

std::optional<uint64_t> parseRevision(const std::string& key, const std::string& prefix) {
  if (key.size() != prefix.size() + 1 + REVISION_DIGITS || !key.starts_with(prefix) || key[prefix.size()] != '/')
    return std::nullopt;
  return epochSegment(std::string_view(key).substr(prefix.size() + 1));
}

uint64_t latestRevision(const ObjectListing& listing, const std::string& prefix) {
  uint64_t latest = 0;
  for (const auto& object : listing.objects) {
    if (auto revision = parseRevision(object.key, prefix)) latest = std::max(latest, *revision);
  }
  return latest;
}

kj::Promise<AppendResult> appendRevision(ObjectStore& store, kj::Timer& timer, std::string prefix, std::string bytes,
    uint32_t maxAttempts) {
  KJ_REQUIRE(maxAttempts > 0, "a revision sequence needs at least one attempt");
  for (uint32_t attempt = 1;; ++attempt) {
    auto listing = co_await store.list(prefix + '/');
    const auto next = latestRevision(listing, prefix) + 1;
    auto result = co_await store.create(revisionKey(prefix, next), bytes);
    if (result.created) co_return AppendResult{next, attempt, result.dateUnixMs};
    if (attempt >= maxAttempts)
      failConflict("revision sequence " + prefix + ": every attempt met a revision another writer created first (" +
          std::to_string(attempt) + " attempts)");
    uint32_t random = 0;
    KJ_REQUIRE(RAND_bytes(reinterpret_cast<unsigned char*>(&random), sizeof(random)) == 1, "cannot draw a retry pause");
    const uint32_t ceiling = std::min<uint32_t>(160, 5u << std::min<uint32_t>(attempt, 5));
    co_await timer.afterDelay((5 + random % (ceiling - 4)) * kj::MILLISECONDS);
  }
}

kj::Promise<std::optional<uint64_t>> readHintRevision(ObjectStore& store, std::string hintKey) {
  auto response = co_await store.get(hintKey);
  if (response.status == 404) co_return std::nullopt;
  requireStorage(response.status == 200, "object store read failed: HTTP " + std::to_string(response.status) + " (" + hintKey + ")");
  co_return parseHint(response.bytes);
}

kj::Promise<LatestRevision> readLatestRevision(ObjectStore& store, std::string prefix, std::string hintKey, uint64_t floor,
    bool probe) {
  LatestRevision result;
  auto hint = co_await readHintRevision(store, hintKey);
  auto listLatest = [&]() -> kj::Promise<uint64_t> {
    result.listed = true;
    co_return latestRevision(co_await store.list(prefix + '/'), prefix);
  };
  uint64_t candidate = hint && *hint >= floor ? *hint : co_await listLatest();
  if (candidate) {
    auto object = co_await store.get(revisionKey(prefix, candidate));
    // A hint can name a revision that is gone (pruned after a late hint): the listing has the latest.
    if (object.status == 404 && !result.listed) {
      candidate = co_await listLatest();
      if (candidate) object = co_await store.get(revisionKey(prefix, candidate));
    }
    if (candidate) {
      requireStorage(object.status == 200, "object store read failed: HTTP " + std::to_string(object.status) + " (" +
          revisionKey(prefix, candidate) + ")");
      result.bytes = std::move(object.bytes);
    }
  }
  if (probe) {
    while (true) {
      auto next = co_await store.get(revisionKey(prefix, candidate + 1));
      if (next.status == 404) break;
      requireStorage(next.status == 200, "object store read failed: HTTP " + std::to_string(next.status) + " (" +
          revisionKey(prefix, candidate + 1) + ")");
      ++candidate;
      result.bytes = std::move(next.bytes);
    }
  }
  result.revision = candidate;
  co_return result;
}

kj::Promise<uint64_t> publishHint(ObjectStore& store, std::string prefix, std::string hintKey, uint64_t revision) {
  co_await store.put(hintKey, hintJson(revision));
  uint64_t latest = revision;
  while (co_await store.exists(revisionKey(prefix, latest + 1))) ++latest;
  if (latest != revision) co_await store.put(hintKey, hintJson(latest));
  co_return latest;
}

kj::Promise<void> pruneRevision(ObjectStore& store, std::string prefix, uint64_t revision, uint64_t keep) {
  if (keep == 0 || revision <= keep) co_return;
  co_await store.remove(revisionKey(prefix, revision - keep));
}

uint64_t realClockUnixMs() {
  timespec now{};
  KJ_REQUIRE(clock_gettime(CLOCK_REALTIME, &now) == 0, "cannot read the real-time clock");
  return static_cast<uint64_t>(now.tv_sec) * 1000 + static_cast<uint64_t>(now.tv_nsec) / 1000000;
}

ClockCheck checkClock(const ClockSample& sample, uint32_t toleranceMs) {
  ClockCheck check;
  check.readable = sample.dateUnixMs > 0;
  if (!check.readable) return check;
  const auto date = static_cast<int64_t>(sample.dateUnixMs);
  const auto sent = static_cast<int64_t>(sample.sentUnixMs), received = static_cast<int64_t>(sample.receivedUnixMs);
  check.minSkewMs = date - received;
  check.maxSkewMs = date + 1000 - sent;
  check.skewMs = date + 500 - (sent + received) / 2;
  const auto tolerance = static_cast<int64_t>(toleranceMs);
  check.exceeds = check.minSkewMs > tolerance || check.maxSkewMs < -tolerance;
  return check;
}

std::string memberPrefix(const std::string& nodeId) { return std::string(MEMBERS_PREFIX) + nodeId + '/'; }
std::string memberEpochsPrefix(const std::string& nodeId) { return memberPrefix(nodeId) + "epochs"; }
std::string memberRegistrationKey(const std::string& nodeId, uint64_t epoch) {
  return revisionKey(memberEpochsPrefix(nodeId), epoch);
}
std::string memberHeartbeatKey(const std::string& nodeId, uint64_t epoch) { return memberPrefix(nodeId) + padded(epoch); }
std::string memberDrainKey(const std::string& nodeId, uint64_t epoch) { return memberHeartbeatKey(nodeId, epoch) + ".draining"; }
std::string memberSuspectsKey(const std::string& nodeId, uint64_t epoch) { return memberHeartbeatKey(nodeId, epoch) + ".suspects"; }
std::string memberProbePrefix(const std::string& nodeId) { return memberPrefix(nodeId) + "probe/"; }

std::map<std::string, ListedNode> readMemberListing(const ObjectListing& listing, uint64_t* ignored) {
  std::map<std::string, ListedNode> nodes;
  const std::string_view root(MEMBERS_PREFIX);
  for (const auto& object : listing.objects) {
    const std::string_view key(object.key);
    const auto slash = key.find('/', root.size());
    const bool rooted = key.starts_with(root) && slash != std::string_view::npos;
    const std::string id(rooted ? key.substr(root.size(), slash - root.size()) : std::string_view());
    const auto rest = rooted ? key.substr(slash + 1) : std::string_view();
    std::optional<uint64_t> epoch;
    enum { REGISTRATION, HEARTBEAT, DRAINING, SUSPECTS, NONE } kind = NONE;
    if (rooted && safeName(id) && id != "." && id != "..") {
      if (rest.starts_with("epochs/")) { epoch = epochSegment(rest.substr(7)); kind = REGISTRATION; }
      else if (rest.ends_with(".draining")) { epoch = epochSegment(rest.substr(0, rest.size() - 9)); kind = DRAINING; }
      else if (rest.ends_with(".suspects")) { epoch = epochSegment(rest.substr(0, rest.size() - 9)); kind = SUSPECTS; }
      else if (rest.starts_with("probe/")) { if (ignored) ++*ignored; continue; }
      else { epoch = epochSegment(rest); kind = HEARTBEAT; }
    }
    if (!epoch || kind == NONE) { if (ignored) ++*ignored; continue; }
    auto& node = nodes[id];
    node.id = id;
    auto& listed = node.epochs[*epoch];
    listed.epoch = *epoch;
    if (kind == REGISTRATION) { listed.registered = true; listed.registeredUnixMs = object.lastModifiedUnixMs; }
    else if (kind == HEARTBEAT) {
      listed.heartbeat = true;
      listed.heartbeatUnixMs = object.lastModifiedUnixMs;
      listed.pageDateUnixMs = object.pageDateUnixMs;
    } else if (kind == DRAINING) { listed.draining = true; listed.drainKeyUnixMs = object.lastModifiedUnixMs; }
    else { listed.suspects = true; listed.suspectsUnixMs = object.lastModifiedUnixMs; listed.suspectsEtag = object.etag; }
  }
  return nodes;
}

std::vector<LiveMember> liveMembers(const std::map<std::string, ListedNode>& nodes, uint32_t ttlSeconds, uint32_t graceMs) {
  std::vector<LiveMember> live;
  const auto limit = static_cast<int64_t>(ttlSeconds) * 1000 + graceMs;
  for (const auto& [id, node] : nodes) {
    for (auto it = node.epochs.rbegin(); it != node.epochs.rend(); ++it) {
      const auto& epoch = it->second;
      if (!epoch.heartbeat) continue;
      // Without a Date header there is no age to judge by: the heartbeat counts as live.
      const int64_t age = epoch.pageDateUnixMs
          ? static_cast<int64_t>(epoch.pageDateUnixMs) - static_cast<int64_t>(epoch.heartbeatUnixMs) : 0;
      if (age > limit) continue;
      live.push_back({id, epoch.epoch, epoch.heartbeatUnixMs, age, epoch.draining});
      break;
    }
  }
  return live;
}

IdentityUse identityUse(const ListedNode& node, uint64_t dateUnixMs, uint32_t ttlSeconds) {
  IdentityUse use;
  uint64_t latestRegistered = 0;
  for (const auto& [number, epoch] : node.epochs) {
    if (epoch.heartbeat && epoch.heartbeatUnixMs > use.freshestUnixMs) {
      use.freshestUnixMs = epoch.heartbeatUnixMs;
      use.freshestEpoch = number;
    }
    if (epoch.registered) latestRegistered = number;
  }
  if (latestRegistered) {
    const auto& registered = node.epochs.at(latestRegistered);
    if (registered.registeredUnixMs > use.freshestUnixMs) {
      use.freshestUnixMs = registered.registeredUnixMs;
      use.freshestEpoch = latestRegistered;
    }
  }
  if (!use.freshestUnixMs) return use;
  use.ageMs = static_cast<int64_t>(dateUnixMs) - static_cast<int64_t>(use.freshestUnixMs);
  use.inUse = use.ageMs < static_cast<int64_t>(ttlSeconds) * 1000;
  return use;
}

std::string memberRecordJson(const MemberRecord& record) {
  using json::quote;
  return "{\"nodeId\":" + quote(record.nodeId) + ",\"epoch\":" + quote(std::to_string(record.epoch)) +
      ",\"address\":" + quote(record.address) + ",\"startedAtUnixMs\":" + std::to_string(record.startedAtUnixMs) +
      ",\"state\":" + quote(record.state) + ",\"writtenAtUnixMs\":" + std::to_string(record.writtenAtUnixMs) +
      ",\"instance\":" + quote(record.instance) + "}";
}

std::optional<MemberRecord> parseMemberRecord(const std::string& bytes, std::string* error) {
  try {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), json);
    auto reader = json.asReader();
    MemberRecord record;
    record.nodeId = json::text(reader, "nodeId");
    const auto epoch = json::text(reader, "epoch");
    KJ_REQUIRE(safeName(record.nodeId), "invalid member nodeId");
    KJ_REQUIRE(!epoch.empty() && epoch.size() <= 20 && std::all_of(epoch.begin(), epoch.end(),
        [](unsigned char c) { return std::isdigit(c); }), "invalid member epoch");
    record.epoch = std::stoull(epoch);
    KJ_REQUIRE(record.epoch > 0, "invalid member epoch");
    record.instance = json::text(reader, "instance");
    KJ_REQUIRE(!record.instance.empty(), "a member record without its instance");
    if (auto address = json::field(reader, "address"); address.isString()) record.address = address.getString().cStr();
    if (auto state = json::field(reader, "state"); state.isString()) record.state = state.getString().cStr();
    KJ_REQUIRE(record.state == "active" || record.state == "draining", "invalid member state");
    for (auto [name, target] : {std::pair{"startedAtUnixMs", &record.startedAtUnixMs},
        std::pair{"writtenAtUnixMs", &record.writtenAtUnixMs}}) {
      auto value = json::field(reader, name);
      if (value.isNumber() && value.getNumber() > 0) *target = static_cast<uint64_t>(value.getNumber());
    }
    return record;
  } catch (const kj::Exception& exception) {
    if (error) *error = exception.getDescription().cStr();
    return std::nullopt;
  }
}

kj::Promise<std::vector<StoreMember>> readStoreMembers(ObjectStore& store, uint32_t ttlSeconds, uint32_t graceMs,
    StoreMemberCache* cache) {
  auto listing = co_await store.list(MEMBERS_PREFIX);
  std::vector<StoreMember> members;
  std::set<std::pair<std::string, uint64_t>> seen;
  for (const auto& live : liveMembers(readMemberListing(listing), ttlSeconds, graceMs)) {
    const std::pair<std::string, uint64_t> name{live.id, live.epoch};
    seen.insert(name);
    std::optional<MemberRecord> record;
    if (cache) if (auto found = cache->find(name); found != cache->end()) record = found->second;
    if (!record) {
      auto response = co_await store.get(memberHeartbeatKey(live.id, live.epoch));
      if (response.status == 200) record = parseMemberRecord(response.bytes);
      if (record && (record->nodeId != live.id || record->epoch != live.epoch || record->address.empty() ||
          record->address.find(':') == std::string::npos || record->address.find("://") != std::string::npos)) record.reset();
      if (!record) continue;
      if (cache) (*cache)[name] = *record;
    }
    members.push_back({live.id, record->address, live.draining ? "draining" : "active", live.epoch, record->startedAtUnixMs,
        live.heartbeatUnixMs, live.ageMs});
  }
  if (cache) std::erase_if(*cache, [&](const auto& entry) { return !seen.contains(entry.first); });
  co_return members;
}

std::string ConditionalPutMeasurement::json() const {
  std::ostringstream out;
  out << "{\"conditionalPutAtomic\":" << (atomic() ? "true" : "false") << ",\"writers\":" << writers << ",\"rounds\":" << rounds
      << ",\"roundsWithExactlyOneCreated\":" << roundsWithExactlyOne << ",\"roundsWithMoreThanOne\":" << roundsWithMore
      << ",\"roundsWithNone\":" << roundsWithNone << ",\"createdPerRound\":[";
  for (size_t i = 0; i < createdPerRound.size(); ++i) out << (i ? "," : "") << createdPerRound[i];
  out << "],\"statuses\":{";
  size_t index = 0;
  for (const auto& [status, count] : statuses) out << (index++ ? "," : "") << json::quote(status) << ":" << count;
  out << "},\"elapsedMs\":" << elapsedMs << "}";
  return out.str();
}

namespace {
kj::Promise<std::string> createStatus(ObjectStore& store, std::string key, std::string bytes) {
  try {
    auto result = co_await store.create(std::move(key), std::move(bytes));
    co_return std::to_string(result.status);
  } catch (const kj::Exception&) {
    co_return std::string("error");
  }
}
kj::Promise<void> removeQuietly(ObjectStore& store, std::string key) {
  try { co_await store.remove(std::move(key)); } catch (const kj::Exception&) {}
}
}  // namespace

kj::Promise<ConditionalPutMeasurement> measureConditionalPut(ObjectStore& store, std::string prefix, uint32_t writers,
    uint32_t rounds) {
  KJ_REQUIRE(writers >= 2 && rounds >= 1, "a measurement needs two writers and a round");
  std::array<unsigned char, 6> random;
  KJ_REQUIRE(RAND_bytes(random.data(), random.size()) == 1, "cannot name the measurement's keys");
  std::string run;
  for (auto byte : random) { run += "0123456789abcdef"[byte >> 4]; run += "0123456789abcdef"[byte & 15]; }
  const auto started = std::chrono::steady_clock::now();
  std::vector<std::string> keys;
  kj::Vector<kj::Promise<std::string>> calls;
  for (uint32_t round = 0; round < rounds; ++round) {
    keys.push_back(prefix + run + '-' + std::to_string(round));
    for (uint32_t writer = 0; writer < writers; ++writer) calls.add(createStatus(store, keys.back(), std::to_string(writer)));
  }
  auto outcomes = co_await kj::joinPromises(calls.releaseAsArray());
  ConditionalPutMeasurement measurement;
  measurement.writers = writers;
  measurement.rounds = rounds;
  for (uint32_t round = 0; round < rounds; ++round) {
    uint32_t created = 0;
    for (uint32_t writer = 0; writer < writers; ++writer) {
      const auto& status = outcomes[round * writers + writer];
      ++measurement.statuses[status];
      if (status == "200") ++created;
    }
    measurement.createdPerRound.push_back(created);
    if (created == 1) ++measurement.roundsWithExactlyOne;
    else if (created == 0) ++measurement.roundsWithNone;
    else ++measurement.roundsWithMore;
  }
  measurement.elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
  kj::Vector<kj::Promise<void>> removals;
  for (const auto& key : keys) removals.add(removeQuietly(store, key));
  co_await kj::joinPromises(removals.releaseAsArray());
  co_return measurement;
}

}  // namespace coworkerd

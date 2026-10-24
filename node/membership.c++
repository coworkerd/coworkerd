#include "membership.h"
#include "etcd.h"
#include "object-state.h"
#include "tenancy.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/encoding.h>
#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <iostream>
#include <sstream>
#include <utility>

namespace coworkerd {
namespace {

// The member keys: /coworkerd/v2/nodes/<nodeId>. The keys below one are that node's own records (suspicions,
// usage rows), not members.
constexpr const char* PREFIX = NODES_PREFIX;
bool memberKey(const std::string& key) {
  return key.starts_with(PREFIX) && key.size() > std::string_view(PREFIX).size() &&
      key.find('/', std::string_view(PREFIX).size()) == std::string::npos;
}
using namespace etcd;

uint64_t nowUnixMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

bool safeId(const std::string& value) {
  return !value.empty() && std::all_of(value.begin(), value.end(),
      [](unsigned char c) { return std::isalnum(c) || c == '-' || c == '_' || c == '.'; });
}

bool advertised(const std::string& address) {
  return !address.empty() && address.find(':') != std::string::npos &&
      address.find("://") == std::string::npos &&
      !address.starts_with("0.0.0.0:") && !address.starts_with("[::]:") &&
      !address.starts_with("*:") && !address.ends_with(":0");
}

std::string memberJson(const Member& member) {
  return "{\"nodeId\":" + quote(member.id) + ",\"address\":" + quote(member.address) +
      ",\"epoch\":" + quote(member.epoch) + ",\"startedAtUnixMs\":" +
      std::to_string(member.startedAtUnixMs) + ",\"state\":" + quote(member.state) + "}";
}

// One line per change of the local member table, so that when other Servers saw
// an announcement or a departure can be read from their logs.
void logMemberChanges(const std::string& self, const std::map<std::string, Member>& before,
    const std::map<std::string, Member>& after, uint64_t revision, MembershipView& view) {
  auto line = [&](const Member& member, const char* change) {
    ++view.memberChanges;
    std::cout << "[membership-change] {\"nodeId\":" << quote(self) << ",\"member\":" << quote(member.id)
              << ",\"change\":\"" << change << "\",\"state\":" << quote(member.state) << ",\"epoch\":" << quote(member.epoch)
              << ",\"revision\":" << revision << ",\"observedAtUnixMs\":" << nowUnixMs() << "}" << std::endl;
  };
  for (const auto& [id, member] : before) if (!after.contains(id)) line(member, "removed");
  for (const auto& [id, member] : after) {
    auto found = before.find(id);
    if (found == before.end()) line(member, "added");
    else if (found->second.epoch != member.epoch) line(member, "epoch");
    else if (found->second.state != member.state) line(member, "state");
  }
}

class FileMembership final: public Membership {
public:
  FileMembership(const std::string& serverId, const std::string& address,
      const std::map<std::string, std::string>& peers) {
    state.backend = "file";
    std::array<unsigned char, 16> random;
    KJ_REQUIRE(RAND_bytes(random.data(), random.size()) == 1, "cannot generate membership epoch");
    constexpr const char* HEX = "0123456789abcdef";
    for (auto byte : random) {
      state.epoch += HEX[byte >> 4];
      state.epoch += HEX[byte & 15];
    }
    state.heldEpochs.insert(state.epoch);
    state.leaseStatus = "static";
    for (const auto& [id, peer] : peers) state.members[id] = {id, peer, "static", 0};
    state.members[serverId] = {serverId, address, state.epoch, nowUnixMs()};
  }

  const MembershipView& view() const override { return state; }
  kj::Promise<void> start() override { return kj::READY_NOW; }
  kj::Promise<void> run() override { return kj::NEVER_DONE; }
  kj::Promise<void> stop() override {
    if (state.revokeStatus == "none") { state.revokeStatus = "no-lease"; state.revokedAtUnixMs = nowUnixMs(); }
    return kj::READY_NOW;
  }
  kj::Promise<void> announceDraining() override {
    state.draining = true;
    ++state.drainAnnounceAttempts;
    for (auto& [id, member] : state.members) if (member.epoch == state.epoch) member.state = "draining";
    state.drainAnnounce = "announced";
    state.drainAnnouncedAtUnixMs = nowUnixMs();
    return kj::READY_NOW;
  }

private:
  MembershipView state;
};

class EtcdMembership final: public Membership {
public:
  EtcdMembership(MembershipConfig config, std::string serverId,
      kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable)
      : config(std::move(config)), serverId(std::move(serverId)), network(network),
        timer(timer), headerTable(headerTable), startedAtUnixMs(nowUnixMs()), expiresAt(timer.now()),
        authority(this->config.endpoint, this->config.requestTimeoutMs, network, timer, headerTable),
        // Changes only: the prefix also holds every node's suspicions and usage rows, which no member
        // table needs mirrored.
        watcher(authority, PREFIX, timer, this->config.retryMs,
            EtcdWatcher::ApplyChanges([this](const EtcdBatch& batch) { return applyMembers(batch); })),
        startupDeadline(timer.now()) {
    state.backend = "etcd";
  }

  const MembershipView& view() const override {
    const auto& observed = watcher.view();
    state.watchConnected = observed.connected;
    state.revision = observed.revision;
    state.fullRereads = observed.fullRereads;
    state.compactions = observed.compactions;
    return state;
  }

  kj::Promise<void> start() override {
    try {
      startupDeadline = timer.now() + config.startupTimeoutMs * kj::MILLISECONDS;
      co_await startRetry();
    } catch (const kj::Exception& error) {
      if (conflict) throw;
      KJ_FAIL_REQUIRE("etcd membership startup failed before serving",
          config.endpoint.c_str(), config.startupTimeoutMs, error.getDescription());
    }
  }

  kj::Promise<void> run() override {
    return maintainLease().exclusiveJoin(watcher.run());
  }

  kj::Promise<void> stop() override {
    if (stopping) co_return;
    // Renewal and re-registration end before the revocation: once revoked, the
    // key must not come back from this process.
    stopping = true;
    state.leaseStatus = "stopping";
    if (!state.leaseId.empty()) {
      try {
        co_await request("/v3/lease/revoke", "{\"ID\":" + quote(state.leaseId) + "}");
        state.revokeStatus = "revoked";
        std::cout << "[membership] revoked node=" << serverId
                  << " lease=" << state.leaseId << std::endl;
      } catch (const kj::Exception& error) {
        // A disconnected authority cannot acknowledge graceful revocation.
        // The remaining lease still expires; shutdown is bounded by the RPC timeout.
        state.revokeStatus = std::string("failed: ") + error.getDescription().cStr();
        std::cerr << "[membership] revoke failed node=" << serverId
                  << " error=" << error.getDescription().cStr() << std::endl;
      }
    } else {
      state.revokeStatus = "no-lease";
    }
    state.revokedAtUnixMs = nowUnixMs();
    state.leaseStatus = "stopped";
  }

  kj::Promise<void> announceDraining() override {
    state.draining = true;
    ++state.drainAnnounceAttempts;
    if (state.drainAnnounce == "announced") co_return;
    if (stopping || state.leaseId.empty() || timer.now() >= expiresAt) {
      state.drainAnnounce = stopping ? "failed: membership stopped" : "failed: no live lease";
      KJ_FAIL_REQUIRE("no live lease to announce draining under", serverId.c_str());
    }
    state.drainAnnounce = "pending";
    const auto leaseId = state.leaseId;
    auto key = base64(std::string(PREFIX) + serverId);
    Member member{serverId, config.advertisedAddress, leaseId, startedAtUnixMs, "draining"};
    // Only a key this process holds under this lease is rewritten; the value keeps
    // the lease, so renewal and revocation are unchanged.
    std::string response;
    try {
      response = co_await request("/v3/kv/txn",
          "{\"compare\":[{\"target\":\"LEASE\",\"result\":\"EQUAL\",\"key\":" + quote(key) +
          ",\"lease\":" + quote(leaseId) + "}],\"success\":[{\"request_put\":{\"key\":" + quote(key) +
          ",\"value\":" + quote(base64(memberJson(member))) + ",\"lease\":" + quote(leaseId) + "}}]}");
    } catch (const kj::Exception& error) {
      state.drainAnnounce = std::string("failed: ") + error.getDescription().cStr();
      throw;
    }
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    if (!boolean(json, "succeeded")) {
      state.drainAnnounce = "failed: the member key is not held under this Server's lease";
      KJ_FAIL_REQUIRE("member key is not held under this Server's lease", serverId.c_str(), leaseId.c_str());
    }
    state.drainAnnounce = "announced";
    state.drainAnnouncedAtUnixMs = nowUnixMs();
    std::cout << "[membership] draining announced node=" << serverId << " lease=" << leaseId << std::endl;
  }

private:
  MembershipConfig config;
  std::string serverId;
  kj::Network& network;
  kj::Timer& timer;
  const kj::HttpHeaderTable& headerTable;
  uint64_t startedAtUnixMs;
  mutable MembershipView state;
  bool conflict = false;
  bool stopping = false;
  bool collisionAllowanceUsed = false;
  std::string retiredLeaseId;
  kj::TimePoint expiresAt;
  EtcdClient authority;
  EtcdWatcher watcher;
  kj::TimePoint startupDeadline;


  kj::Promise<std::string> request(std::string path, std::string payload, bool firstFrame = false) {
    return authority.request(std::move(path), std::move(payload), firstFrame);
  }

  kj::Promise<uint32_t> keepAlive() {
    auto response = co_await request("/v3/lease/keepalive",
        "{\"ID\":" + quote(state.leaseId) + "}", true);
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    auto result = field(json, "result");
    auto ttl = integer(result, "TTL");
    KJ_REQUIRE(ttl <= 86400, "etcd returned an unsupported lease TTL");
    if (ttl > 0) {
      state.grantedTtlSeconds = ttl;
      expiresAt = timer.now() + ttl * kj::SECONDS;
      ++state.keepAlives;
    }
    co_return static_cast<uint32_t>(ttl);
  }

  kj::Promise<void> registerOnce(bool starting = false) {
    conflict = false;
    if (!state.leaseId.empty() && co_await keepAlive() == 0) state.leaseId.clear();
    if (state.leaseId.empty()) {
      auto response = co_await request("/v3/lease/grant",
          "{\"TTL\":" + quote(std::to_string(config.ttlSeconds)) + "}");
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<Json>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
      state.leaseId = text(json, "ID");
      auto ttl = integer(json, "TTL");
      KJ_REQUIRE(ttl > 0 && ttl <= 86400, "etcd returned an invalid lease TTL");
      state.grantedTtlSeconds = ttl;
      expiresAt = timer.now() + ttl * kj::SECONDS;
    }
    auto key = base64(std::string(PREFIX) + serverId);
    Member member{serverId, config.advertisedAddress, state.leaseId, startedAtUnixMs};
    // A registration still in flight when stop() revoked the lease must not
    // create the key again.
    KJ_REQUIRE(!stopping, "membership stopped during registration");
    auto response = co_await request("/v3/kv/txn",
        "{\"compare\":[{\"target\":\"VERSION\",\"result\":\"EQUAL\",\"key\":" + quote(key) +
        ",\"version\":\"0\"}],\"success\":[{\"request_put\":{\"key\":" + quote(key) +
        ",\"value\":" + quote(base64(memberJson(member))) + ",\"lease\":" + quote(state.leaseId) +
        "}}],\"failure\":[{\"request_range\":{\"key\":" + quote(key) + "}}]}");
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    if (!boolean(json, "succeeded")) {
      auto responses = field(json, "responses").getArray();
      auto kvs = field(field(responses[0], "response_range"), "kvs");
      // A lost transaction response may already have committed our own lease.
      // Only that exact lease is accepted; another process is never overwritten.
      bool own = kvs.isArray() && kvs.getArray().size() == 1 &&
          text(kvs.getArray()[0], "lease") == state.leaseId &&
          unbase64(text(kvs.getArray()[0], "value")) == memberJson(member);
      if (!own && !retiredLeaseId.empty() && kvs.isArray() && kvs.getArray().size() == 1 &&
          text(kvs.getArray()[0], "lease") == retiredLeaseId) {
        // If etcd restored an old lease after downtime, atomically reattach
        // only our own key. A delete/create gap would defeat static stability
        // during recovery. A competing identity never passes this comparison.
        auto replacement = co_await request("/v3/kv/txn",
            "{\"compare\":[{\"target\":\"LEASE\",\"result\":\"EQUAL\",\"key\":" + quote(key) +
            ",\"lease\":" + quote(retiredLeaseId) + "}],\"success\":[{\"request_put\":{\"key\":" +
            quote(key) + ",\"value\":" + quote(base64(memberJson(member))) +
            ",\"lease\":" + quote(state.leaseId) + "}}]}");
        capnp::MallocMessageBuilder replacementMessage;
        auto replacementJson = replacementMessage.initRoot<Json>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(replacement.data(), replacement.size()), replacementJson);
        own = boolean(replacementJson, "succeeded");
        KJ_REQUIRE(own, "membership changed while retiring its old epoch; retry registration");
      }
      if (!own && starting && kvs.isArray() && kvs.getArray().size() == 1) {
        co_await observeCollision(EtcdClient::decodeKv(kvs.getArray()[0]));
        co_await registerOnce(true);
        co_return;
      }
      if (!own) {
        conflict = true;
        state.leaseStatus = "conflict";
        KJ_FAIL_REQUIRE("membership nodeId conflict: key already exists", serverId.c_str());
      }
    }
    if (state.epoch != state.leaseId) {
      state.epoch = state.leaseId;
      state.heldEpochs.insert(state.epoch);
      ++state.registrations;
      std::cout << "[membership] registered node=" << serverId
                << " epoch=" << state.epoch << " address=" << config.advertisedAddress
                << " ttl-seconds=" << state.grantedTtlSeconds << std::endl;
    }
    state.leaseStatus = "active";
    state.leaseConnected = true;
    if (!retiredLeaseId.empty()) {
      auto oldLease = std::exchange(retiredLeaseId, {});
      try {
        co_await request("/v3/lease/revoke", "{\"ID\":" + quote(oldLease) + "}");
      } catch (const kj::Exception& error) {
        // The key is already attached to the replacement lease; old-lease
        // cleanup cannot delete it, and a failed cleanup expires on its own.
        failed("retired-lease cleanup", error);
      }
    }
  }

  void failed(const char* operation, const kj::Exception& error) {
    state.lastError = error.getDescription().cStr();
    std::cerr << "[membership] " << operation << " failed node=" << serverId
              << " revision=" << state.revision << " error=" << state.lastError << std::endl;
  }

  kj::Promise<void> startRetry() {
    while (true) {
      try {
        KJ_REQUIRE(timer.now() < startupDeadline, "membership startup timeout", state.lastError.c_str());
        co_await registerOnce(true);
        co_await watcher.fullRead();
        co_return;
      } catch (const kj::Exception& error) {
        if (conflict || timer.now() >= startupDeadline) throw;
        state.leaseConnected = false;
        failed("startup", error);
      }
      co_await timer.afterDelay(config.retryMs * kj::MILLISECONDS);
    }
  }

  kj::Promise<void> maintainLease() {
    uint32_t delayMs = state.grantedTtlSeconds * 1000 / 3;
    while (true) {
      co_await timer.afterDelay(delayMs * kj::MILLISECONDS);
      // After stop() nothing renews or registers again; the promise stays pending
      // (it is joined with the watch and the Server's other loops).
      if (stopping) co_await kj::Promise<void>(kj::NEVER_DONE);
      try {
        if (state.draining) {
          // An exiting Server keeps its lease while it drains but takes no new epoch:
          // an expired lease (by the local clock, as below) stays expired.
          if (!state.leaseId.empty() && timer.now() < expiresAt && co_await keepAlive() > 0) {
            state.leaseConnected = true;
          } else if (!state.leaseId.empty()) {
            state.leaseStatus = "expired";
            std::cout << "[membership] lease expired while draining node=" << serverId
                      << " epoch=" << state.epoch << std::endl;
            state.leaseId.clear();
          }
          delayMs = state.leaseId.empty() ? config.retryMs : state.grantedTtlSeconds * 1000 / 3;
          continue;
        }
        if (state.leaseId.empty()) {
          co_await registerOnce();
        } else if (timer.now() >= expiresAt) {
          // A stopped single-node etcd can restore the lease's full TTL on
          // restart. Never resurrect an epoch whose local renewal deadline
          // passed. Conditional registration can replace our restored key.
          state.leaseStatus = "expired";
          retiredLeaseId = std::exchange(state.leaseId, {});
          co_await registerOnce();
        } else if (co_await keepAlive() == 0) {
          state.leaseStatus = "expired";
          state.leaseId.clear();
          std::cout << "[membership] lease expired node=" << serverId
                    << " old-epoch=" << state.epoch << std::endl;
          co_await registerOnce();
        } else if (state.epoch != state.leaseId || state.leaseStatus == "conflict") {
          co_await registerOnce();
        }
        state.leaseConnected = true;
        state.leaseStatus = "active";
        delayMs = state.grantedTtlSeconds * 1000 / 3;
      } catch (const kj::Exception& error) {
        state.leaseConnected = false;
        if (!conflict) state.leaseStatus = timer.now() >= expiresAt ? "expired" : "uncertain";
        failed("keepalive", error);
        delayMs = config.retryMs;
      }
    }
  }

  std::string ttlSequence() const {
    std::ostringstream out;
    out << '[';
    size_t index = 0;
    for (auto ttl : state.startupConflictTtlSequence) { if (index++) out << ','; out << ttl; }
    out << ']';
    return out.str();
  }

  kj::Promise<void> observeCollision(const EtcdKv& held) {
    const auto started = timer.now();
    state.startupConflictTtlSequence.clear();
    state.leaseStatus = "waiting-for-old-lease";
    int64_t previous = -1;
    kj::Maybe<kj::TimePoint> collisionDeadline;
    // etcd truncates the reported TTL. One second covers truncation; two more
    // seconds cover expiration processing and scheduling variation.
    constexpr uint32_t EXPIRATION_MARGIN_MS = 2000;
    while (true) {
      auto response = co_await request("/v3/lease/timetolive", "{\"ID\":" + quote(held.lease) + "}");
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<Json>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
      auto ttlField = field(json, "TTL");
      auto ttl = ttlField.isString() ? std::stoll(ttlField.getString().cStr()) : 0;
      if (collisionDeadline == kj::none && ttl >= 0) {
        collisionDeadline = timer.now() + (ttl + 1) * kj::SECONDS + EXPIRATION_MARGIN_MS * kj::MILLISECONDS;
        // The original startup timeout remains the global bound. A collision
        // receives one finite allowance; repeated collisions never add time.
        if (!collisionAllowanceUsed) {
          collisionAllowanceUsed = true;
          KJ_IF_SOME(deadline, collisionDeadline) {
            startupDeadline = kj::max(startupDeadline, deadline + 2 * config.requestTimeoutMs * kj::MILLISECONDS);
          }
        }
      }
      state.startupConflictTtlSequence.push_back(ttl);
      state.startupCollisionWaitMs = (timer.now() - started) / kj::MILLISECONDS;
      std::cout << "[membership] observe collision node=" << serverId << " held-lease=" << held.lease
                << " ttl-sequence=" << ttlSequence() << " elapsed-ms=" << state.startupCollisionWaitMs << std::endl;
      if (previous >= 0 && ttl > previous) {
        conflict = true;
        state.leaseStatus = "conflict";
        auto reason = "membership nodeId conflict: observed renewed lease TTL sequence=" + ttlSequence();
        KJ_FAIL_REQUIRE(reason.c_str(), serverId.c_str());
      }
      if (ttl < 0) co_return;
      auto current = co_await authority.range(std::string(PREFIX) + serverId);
      auto found = current.values.find(held.key);
      if (found == current.values.end()) co_return;
      if (found->second.lease != held.lease) {
        conflict = true;
        KJ_FAIL_REQUIRE("membership nodeId conflict: holder changed; TTL sequence", ttlSequence().c_str());
      }
      KJ_IF_SOME(deadline, collisionDeadline) {
        if (timer.now() >= deadline) {
          conflict = true;
          state.leaseStatus = "conflict";
          auto reason = "membership nodeId conflict: lease survived time bound; TTL sequence=" + ttlSequence();
          KJ_FAIL_REQUIRE(reason.c_str(), serverId.c_str(), state.startupCollisionWaitMs);
        }
      }
      KJ_REQUIRE(timer.now() < startupDeadline, "membership startup timeout while observing collision");
      previous = ttl;
      co_await timer.afterDelay(250 * kj::MILLISECONDS);
    }
  }

  Member decodeMember(const EtcdKv& kv) {
    const auto& key = kv.key;
    KJ_REQUIRE(key.starts_with(PREFIX), "unexpected membership key");
    const auto& value = kv.value;
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(value.data(), value.size()), json);
    auto id = text(json, "nodeId");
    auto address = text(json, "address");
    auto epoch = text(json, "epoch");
    KJ_REQUIRE(safeId(id) && key == std::string(PREFIX) + id && advertised(address),
        "invalid membership value", key.c_str());
    KJ_REQUIRE(epoch == kv.lease, "membership epoch differs from its lease");
    auto timestamp = field(json, "startedAtUnixMs");
    KJ_REQUIRE(timestamp.isNumber() && timestamp.getNumber() > 0, "invalid member startup time");
    auto stateField = field(json, "state");
    std::string memberState = "active";
    if (!stateField.isNull()) {
      KJ_REQUIRE(stateField.isString(), "invalid member state", key.c_str());
      memberState = stateField.getString().cStr();
      KJ_REQUIRE(memberState == "active" || memberState == "draining", "invalid member state", key.c_str());
    }
    return {id, address, epoch, static_cast<uint64_t>(timestamp.getNumber()), memberState};
  }

  // A value that cannot be read is skipped -- its key is no member while it stays so -- and logged once; it never
  // stops the other members from being applied, nor this node from starting.
  kj::Promise<void> applyMembers(const EtcdBatch& batch) {
    auto next = batch.full ? std::map<std::string, Member>() : state.members;
    auto invalid = batch.full ? std::set<std::string>() : state.invalidMembers;
    for (const auto& change : batch.changes) {
      if (!memberKey(change.key)) continue;
      const auto id = change.key.substr(std::string_view(PREFIX).size());
      if (!change.kv) { next.erase(id); invalid.erase(change.key); continue; }
      try {
        auto member = decodeMember(*change.kv);
        invalid.erase(change.key);
        next[member.id] = std::move(member);
      } catch (const kj::Exception& error) {
        next.erase(id);
        ++state.skippedMemberValues;
        if (invalid.insert(change.key).second)
          std::cout << "[membership] {\"event\":\"invalid-member\",\"nodeId\":" << quote(serverId) << ",\"key\":" << quote(change.key)
                    << ",\"modRevision\":\"" << change.kv->modRevision << "\",\"reason\":" << quote(error.getDescription().cStr())
                    << ",\"revision\":\"" << batch.revision << "\"}" << std::endl;
      }
    }
    state.invalidMembers = std::move(invalid);
    logMemberChanges(serverId, state.members, next, batch.revision, state);
    state.members.swap(next);
    return kj::READY_NOW;
  }

};

// The membership on the object store (docs/s3-truth-and-durable-objects.md section 2; the keys in object-state.h).
// Joining: list members/<id>/, wait while the id is in use (a heartbeat or the newest registration younger than the TTL
// by the listing's Date), create epochs/<max+1> only if absent and read it back after createSettleMs -- another
// process's bytes there mean the registration was lost, and the id is judged again; the store's create-if-absent need
// not hold under concurrency. Then a heartbeat object per epoch, overwritten every heartbeatIntervalMs, and a listing
// of members/ every listIntervalMs: a heartbeat more than TTL + livenessGraceMs behind its listing's Date is out
// (the store's clock both); a new (node, epoch) is read once for its address. Each listing also reads this epoch's
// own heartbeat back: another process's bytes there, or a newer epoch of this id, mean another process holds the id --
// this one drains and exits with 3 (onIdentityTaken). Past the TTL without a heartbeat written, by this node's
// monotonic clock, the epoch is over: no calls from other nodes, and a new epoch once the store answers (v20's rule:
// an expired epoch is never revived). Without the store the last member table stays in use.
class ObjectStoreMembership final: public Membership, private kj::TaskSet::ErrorHandler {
public:
  ObjectStoreMembership(MembershipConfig config, std::string serverId, const ObjectStoreConfig& storeConfig,
      kj::Network& network, kj::Timer& timer)
      : config(std::move(config)), serverId(std::move(serverId)), store(storeConfig, network, timer), timer(timer),
        startedAtUnixMs(nowUnixMs()), lastHeartbeatOk(timer.now()), startupDeadline(timer.now()) {
    std::array<unsigned char, 16> random;
    KJ_REQUIRE(RAND_bytes(random.data(), random.size()) == 1, "cannot name this membership's instance");
    for (auto byte : random) { instance += "0123456789abcdef"[byte >> 4]; instance += "0123456789abcdef"[byte & 15]; }
    state.backend = "objectStore";
    state.storeEndpoint = storeConfig.endpoint;
    state.bucket = storeConfig.bucket;
    state.instance = instance;
    state.grantedTtlSeconds = this->config.ttlSeconds;
    state.heartbeatIntervalMs = this->config.heartbeatIntervalMs;
    state.listIntervalMs = this->config.listIntervalMs;
    state.livenessGraceMs = this->config.livenessGraceMs;
    state.clockSkewToleranceMs = this->config.clockSkewToleranceMs;
    state.createSettleMs = this->config.createSettleMs;
  }

  const MembershipView& view() const override { return state; }

  void onIdentityTaken(std::function<void(std::string)> callback) override {
    identityCallback = std::move(callback);
    if (state.identityTaken && identityCallback) identityCallback(state.identityTakenReason);
  }

  kj::Promise<void> start() override {
    startupDeadline = timer.now() + config.startupTimeoutMs * kj::MILLISECONDS;
    while (true) {
      try {
        KJ_REQUIRE(timer.now() < startupDeadline, "membership startup timeout", state.lastError.c_str());
        if (!epoch) co_await registerEpoch(true);
        co_await writeHeartbeat();
        co_await removeOldObjects();
        co_await listMembers();
        state.leaseStatus = "active";
        co_return;
      } catch (const kj::Exception& error) {
        if (fatal) throw;
        if (timer.now() >= startupDeadline) {
          KJ_FAIL_REQUIRE("object store membership startup failed before serving", state.storeEndpoint.c_str(),
              config.startupTimeoutMs, error.getDescription());
        }
        failed("startup", error);
      }
      co_await timer.afterDelay(config.retryMs * kj::MILLISECONDS);
    }
  }

  kj::Promise<void> run() override {
    tasks.add(measure());
    return heartbeats().exclusiveJoin(listings()).exclusiveJoin(watchExpiry());
  }

  kj::Promise<void> stop() override {
    if (stopping) co_return;
    // Heartbeats and registration end before the objects go: once removed, they never come back from this process.
    stopping = true;
    state.leaseStatus = "stopping";
    for (auto until = timer.now() + boundMs() * kj::MILLISECONDS; writing && timer.now() < until;)
      co_await timer.afterDelay(10 * kj::MILLISECONDS);
    if (state.identityTaken) {
      // The id is another process's now: its epoch's objects are left to it (it removes the old epochs' objects).
      state.revokeStatus = "skipped: another process holds this node's id";
    } else if (!epoch) {
      state.revokeStatus = "no-lease";
    } else {
      try {
        co_await bounded(store.remove(memberHeartbeatKey(serverId, epoch)));
        co_await bounded(store.remove(memberDrainKey(serverId, epoch)));
        state.revokeStatus = "removed";
        std::cout << "[membership] removed node=" << serverId << " epoch=" << epoch << std::endl;
      } catch (const kj::Exception& error) {
        // Without the store the heartbeat stays; the others drop it once it is TTL + grace old.
        state.revokeStatus = std::string("failed: ") + error.getDescription().cStr();
        std::cerr << "[membership] removal failed node=" << serverId << " error=" << error.getDescription().cStr() << std::endl;
      }
    }
    state.revokedAtUnixMs = nowUnixMs();
    state.leaseStatus = "stopped";
  }

  kj::Promise<void> announceDraining() override {
    state.draining = true;
    ++state.drainAnnounceAttempts;
    if (state.drainAnnounce == "announced") co_return;
    if (state.identityTaken) {
      state.drainAnnounce = "skipped: another process holds this node's id";
      co_return;
    }
    if (stopping || !epoch || state.selfExpired) {
      state.drainAnnounce = stopping ? "failed: membership stopped" : "failed: no live lease: the epoch expired";
      KJ_FAIL_REQUIRE("no live epoch to announce draining under", serverId.c_str());
    }
    state.drainAnnounce = "pending";
    auto record = recordOf("draining");
    try {
      co_await bounded(store.put(memberDrainKey(serverId, epoch), memberRecordJson(record)));
    } catch (const kj::Exception& error) {
      state.drainAnnounce = std::string("failed: ") + error.getDescription().cStr();
      throw;
    }
    state.drainAnnounce = "announced";
    state.drainAnnouncedAtUnixMs = nowUnixMs();
    if (auto self = state.members.find(serverId); self != state.members.end()) self->second.state = "draining";
    std::cout << "[membership] draining announced node=" << serverId << " epoch=" << epoch << std::endl;
  }

private:
  static constexpr uint32_t MAX_REGISTRATION_ATTEMPTS = 8;
  static constexpr uint32_t IDENTITY_POLL_MS = 500, EXPIRY_CHECK_MS = 100, CLOCK_RECHECK_MS = 60000;
  MembershipConfig config;
  std::string serverId;
  ObjectStore store;
  kj::Timer& timer;
  uint64_t startedAtUnixMs;
  std::string instance;
  MembershipView state;
  uint64_t epoch = 0;
  kj::TimePoint lastHeartbeatOk;
  kj::TimePoint startupDeadline;
  bool fatal = false, stopping = false, writing = false, collisionAllowanceUsed = false;
  std::function<void(std::string)> identityCallback;
  // The registrations this process created (its epochs, once read back).
  std::set<uint64_t> created;
  // (node, epoch) -> its heartbeat's record, read once for its address.
  std::map<std::pair<std::string, uint64_t>, MemberRecord> addresses;
  kj::TaskSet tasks{*this};

  void taskFailed(kj::Exception&& error) override { failed("background", error); }

  // Every request of the membership is bounded by the heartbeat interval: one that hangs never holds up the next.
  uint32_t boundMs() const { return config.heartbeatIntervalMs; }
  template <typename T> kj::Promise<T> bounded(kj::Promise<T> promise) {
    return timer.timeoutAfter(boundMs() * kj::MILLISECONDS, kj::mv(promise));
  }

  void failed(const char* operation, const kj::Exception& error) {
    state.lastError = error.getDescription().cStr();
    std::cerr << "[membership] " << operation << " failed node=" << serverId << " epoch=" << epoch
              << " error=" << state.lastError << std::endl;
  }

  MemberRecord recordOf(const char* memberState) const {
    MemberRecord record;
    record.nodeId = serverId;
    record.address = config.advertisedAddress;
    record.state = memberState;
    record.instance = instance;
    record.epoch = epoch;
    record.startedAtUnixMs = startedAtUnixMs;
    record.writtenAtUnixMs = nowUnixMs();
    return record;
  }

  Member selfMember() const {
    return {serverId, config.advertisedAddress, std::to_string(epoch), startedAtUnixMs, state.draining ? "draining" : "active",
        state.lastHeartbeatAtUnixMs, 0};
  }

  kj::Promise<std::pair<ObjectListing, ClockSample>> timedList(std::string prefix) {
    ClockSample sample;
    sample.sentUnixMs = realClockUnixMs();
    auto listing = co_await bounded(store.list(std::move(prefix)));
    sample.receivedUnixMs = realClockUnixMs();
    sample.dateUnixMs = listing.dateUnixMs;
    co_return std::make_pair(std::move(listing), sample);
  }

  // The clock against the store's Date header: at startup a difference beyond the tolerance refuses the start (naming
  // it and the store), at runtime it is only warned of; the state endpoint shows the last check either way.
  void noteClock(const ClockSample& sample, bool starting) {
    auto check = checkClock(sample, config.clockSkewToleranceMs);
    ++state.clockChecks;
    state.clockCheckedAtUnixMs = realClockUnixMs();
    if (!check.readable) {
      const std::string reason = "the object store's answers carry no Date header, so this node's clock cannot be checked "
          "against it (storage " + state.storeEndpoint + ")";
      if (starting) { fatal = true; KJ_FAIL_REQUIRE(reason.c_str()); }
      ++state.clockWarnings;
      std::cout << "[membership] {\"event\":\"clock\",\"warning\":" << quote(reason) << "}" << std::endl;
      return;
    }
    state.clockSkewMs = check.skewMs;
    state.clockSkewMinMs = check.minSkewMs;
    state.clockSkewMaxMs = check.maxSkewMs;
    if (!check.exceeds) {
      if (starting) std::cout << "[membership] {\"event\":\"clock\",\"nodeId\":" << quote(serverId) << ",\"skewMs\":" << check.skewMs
                              << ",\"minSkewMs\":" << check.minSkewMs << ",\"maxSkewMs\":" << check.maxSkewMs << ",\"toleranceMs\":"
                              << config.clockSkewToleranceMs << ",\"storage\":" << quote(state.storeEndpoint) << "}" << std::endl;
      return;
    }
    const auto magnitude = check.skewMs < 0 ? -check.skewMs : check.skewMs;
    const std::string reason = "this machine's clock is " + std::to_string(magnitude) + " ms " +
        (check.skewMs < 0 ? "ahead of" : "behind") + " the object store's (the store's Date minus this clock lies between " +
        std::to_string(check.minSkewMs) + " and " + std::to_string(check.maxSkewMs) + " ms), more than membership.clockSkewToleranceMs " +
        std::to_string(config.clockSkewToleranceMs) + " ms; storage " + state.storeEndpoint;
    if (starting) {
      fatal = true;
      std::cout << "[membership] {\"event\":\"clock\",\"refused\":true,\"skewMs\":" << check.skewMs << ",\"reason\":" << quote(reason)
                << "}" << std::endl;
      KJ_FAIL_REQUIRE("membership refuses to start: clock skew", reason.c_str());
    }
    ++state.clockWarnings;
    std::cout << "[membership] {\"event\":\"clock\",\"warning\":" << quote(reason) << ",\"skewMs\":" << check.skewMs << "}" << std::endl;
  }

  // Another process holds this node's id: no more writes from this one; the Server drains and exits with 3.
  void identityTakenBy(std::string reason) {
    if (state.identityTaken) return;
    state.identityTaken = true;
    state.identityTakenReason = reason;
    state.leaseStatus = "identity-taken";
    std::cout << "[membership] {\"event\":\"identity-taken\",\"nodeId\":" << quote(serverId) << ",\"epoch\":\"" << epoch
              << "\",\"instance\":" << quote(instance) << ",\"reason\":" << quote(reason) << ",\"observedAtUnixMs\":" << nowUnixMs()
              << "}" << std::endl;
    if (identityCallback) identityCallback(reason);
  }

  void conflict(bool starting, const std::string& reason) {
    if (starting) {
      fatal = true;
      state.leaseStatus = "conflict";
      KJ_FAIL_REQUIRE(("membership nodeId conflict: " + reason).c_str(), serverId.c_str());
    }
    identityTakenBy(reason);
    KJ_FAIL_REQUIRE("membership identity taken", reason.c_str());
  }

  // Waits while the id is in use: until the heartbeat or registration that makes it so is TTL old. Written again
  // meanwhile, or still in use past TTL + grace + 2 s, it is another process's: a conflict.
  kj::Promise<void> waitForIdentity(IdentityUse first, bool starting) {
    ++state.identityWaits;
    const auto began = timer.now();
    const auto bound = began + (uint64_t(config.ttlSeconds) * 1000 + config.livenessGraceMs + 2000) * kj::MILLISECONDS;
    if (starting && !collisionAllowanceUsed) {
      // The startup timeout stays the global bound; one wait for an identity in use gets its own allowance.
      collisionAllowanceUsed = true;
      startupDeadline = kj::max(startupDeadline, bound + 2 * boundMs() * kj::MILLISECONDS);
    }
    state.leaseStatus = "waiting-for-old-epoch";
    std::cout << "[membership] observe collision node=" << serverId << " epoch=" << first.freshestEpoch
              << " age-ms=" << first.ageMs << std::endl;
    while (true) {
      co_await timer.afterDelay(IDENTITY_POLL_MS * kj::MILLISECONDS);
      KJ_REQUIRE(!stopping, "membership stopped while waiting for its identity");
      auto [listing, sample] = co_await timedList(memberPrefix(serverId));
      auto nodes = readMemberListing(listing);
      auto use = identityUse(nodes[serverId], listing.dateUnixMs, config.ttlSeconds);
      state.startupCollisionWaitMs = (timer.now() - began) / kj::MILLISECONDS;
      if (use.freshestUnixMs > first.freshestUnixMs) {
        conflict(starting, "epoch " + std::to_string(use.freshestEpoch) + " of this id was written again while waiting (" +
            std::to_string(state.startupCollisionWaitMs) + " ms)");
      }
      if (!use.inUse) co_return;
      if (timer.now() >= bound) {
        conflict(starting, "epoch " + std::to_string(use.freshestEpoch) + " of this id stayed in use past the time bound (" +
            std::to_string(state.startupCollisionWaitMs) + " ms)");
      }
    }
  }

  // A new epoch: max + 1 of every epoch this id has, once the id is not in use, created only if absent and read back
  // after createSettleMs -- the read-back decides, so that a store whose create-if-absent does not hold under
  // concurrency still gives an epoch to one process at most.
  kj::Promise<void> registerEpoch(bool starting) {
    for (uint32_t attempt = 1;; ++attempt) {
      KJ_REQUIRE(attempt <= MAX_REGISTRATION_ATTEMPTS, "membership registration did not settle", attempt);
      KJ_REQUIRE(!stopping, "membership stopped during registration");
      ++state.registrationAttempts;
      auto [listing, sample] = co_await timedList(memberPrefix(serverId));
      if (starting || state.clockChecks == 0) noteClock(sample, starting);
      auto nodes = readMemberListing(listing);
      const auto& self = nodes[serverId];
      // The newest registration may be this process's own, created by an attempt whose read-back did not answer: read
      // back again, and when it holds this instance it is this process's epoch (never one it held before: an expired
      // epoch is not revived).
      if (self.latest() > epoch && created.contains(self.latest())) {
        auto back = co_await bounded(store.get(memberRegistrationKey(serverId, self.latest())));
        std::optional<MemberRecord> read;
        if (back.status == 200) read = parseMemberRecord(back.bytes);
        if (read && read->instance == instance) {
          adopt(self.latest(), attempt);
          co_return;
        }
      }
      if (!starting && epoch && self.latest() > epoch) {
        conflict(false, "epoch " + std::to_string(self.latest()) + " of this id was registered by another process");
      }
      auto use = identityUse(self, listing.dateUnixMs, config.ttlSeconds);
      if (use.inUse) {
        co_await waitForIdentity(use, starting);
        continue;
      }
      const uint64_t next = self.latest() + 1;
      const auto key = memberRegistrationKey(serverId, next);
      auto record = recordOf("active");
      record.epoch = next;
      auto result = co_await bounded(store.create(key, memberRecordJson(record)));
      if (!result.created) continue;
      created.insert(next);
      if (config.createSettleMs) co_await timer.afterDelay(config.createSettleMs * kj::MILLISECONDS);
      auto back = co_await bounded(store.get(key));
      std::optional<MemberRecord> read;
      if (back.status == 200) read = parseMemberRecord(back.bytes);
      if (!read || read->instance != instance) {
        ++state.registrationsLost;
        std::cout << "[membership] {\"event\":\"registration-lost\",\"nodeId\":" << quote(serverId) << ",\"epoch\":\"" << next
                  << "\",\"readBackStatus\":" << back.status << ",\"readBackInstance\":" << quote(read ? read->instance : "")
                  << "}" << std::endl;
        continue;
      }
      adopt(next, attempt);
      co_return;
    }
  }

  void adopt(uint64_t number, uint32_t attempt) {
    KJ_REQUIRE(!stopping, "membership stopped during registration");
    epoch = number;
    state.epoch = std::to_string(number);
    // A new epoch replaces any expired one: calls addressed to that one are refused from now on.
    state.heldEpochs = {state.epoch};
    ++state.registrations;
    state.storeClient = store.stats().json();
    std::cout << "[membership] registered node=" << serverId << " epoch=" << epoch << " address=" << config.advertisedAddress
              << " ttl-seconds=" << config.ttlSeconds << " attempts=" << attempt << std::endl;
  }

  kj::Promise<void> writeHeartbeat() {
    KJ_REQUIRE(epoch && !state.identityTaken && !stopping, "no epoch to write a heartbeat for");
    auto record = recordOf(state.draining ? "draining" : "active");
    const auto sent = timer.now();
    writing = true;
    KJ_DEFER(writing = false);
    try {
      co_await bounded(store.put(memberHeartbeatKey(serverId, epoch), memberRecordJson(record)));
    } catch (const kj::Exception&) {
      ++state.heartbeatFailures;
      throw;
    }
    lastHeartbeatOk = sent;
    ++state.heartbeatsWritten;
    state.lastHeartbeatAtUnixMs = record.writtenAtUnixMs;
    state.storeClient = store.stats().json();
    state.leaseConnected = true;
  }

  // The old epochs' heartbeats and markers of this id go once a new epoch holds it; of its registrations the newest two
  // stay. Best effort: what is left is dropped by age and removed by the next epoch.
  kj::Promise<void> removeOldObjects() {
    try {
      auto listing = co_await bounded(store.list(memberPrefix(serverId)));
      auto nodes = readMemberListing(listing);
      const auto& self = nodes[serverId];
      std::vector<uint64_t> registered;
      for (const auto& [number, listed] : self.epochs) {
        if (listed.registered) registered.push_back(number);
        if (number >= epoch) continue;
        if (listed.heartbeat) { co_await bounded(store.remove(memberHeartbeatKey(serverId, number))); ++state.oldObjectsRemoved; }
        if (listed.draining) { co_await bounded(store.remove(memberDrainKey(serverId, number))); ++state.oldObjectsRemoved; }
      }
      for (size_t i = 0; i + 2 < registered.size(); ++i) {
        if (registered[i] >= epoch) break;
        co_await bounded(store.remove(memberRegistrationKey(serverId, registered[i])));
        ++state.oldObjectsRemoved;
      }
    } catch (const kj::Exception& error) {
      failed("old epoch removal", error);
    }
  }

  // A member's address, from its heartbeat's record: read once per (node, epoch).
  kj::Promise<std::optional<MemberRecord>> addressOf(const std::string& id, uint64_t number) {
    auto found = addresses.find({id, number});
    if (found != addresses.end()) co_return found->second;
    const auto key = memberHeartbeatKey(id, number);
    ++state.addressReads;
    auto response = co_await bounded(store.get(key));
    std::string error;
    std::optional<MemberRecord> record;
    if (response.status == 200) record = parseMemberRecord(response.bytes, &error);
    else error = "HTTP " + std::to_string(response.status);
    if (record && (record->nodeId != id || record->epoch != number || !advertised(record->address))) {
      error = "the record names another node, epoch or no dialable address";
      record.reset();
    }
    if (!record) {
      ++state.skippedMemberValues;
      if (state.invalidMembers.insert(key).second)
        std::cout << "[membership] {\"event\":\"invalid-member\",\"nodeId\":" << quote(serverId) << ",\"key\":" << quote(key)
                  << ",\"reason\":" << quote(error) << "}" << std::endl;
      co_return std::nullopt;
    }
    state.invalidMembers.erase(key);
    addresses[{id, number}] = *record;
    co_return record;
  }

  // One listing of members/: the member table, this id's newer epochs, this epoch's own heartbeat, the clock.
  kj::Promise<void> listMembers() {
    auto [listing, sample] = co_await timedList(MEMBERS_PREFIX);
    ++state.lists;
    state.lastListAtUnixMs = nowUnixMs();
    state.storeClient = store.stats().json();
    state.watchConnected = true;
    if (realClockUnixMs() - state.clockCheckedAtUnixMs >= CLOCK_RECHECK_MS) noteClock(sample, false);
    uint64_t ignored = 0;
    auto nodes = readMemberListing(listing, &ignored);
    state.ignoredKeys = ignored;
    // (While the epoch is expired this process registers a new one itself: the registration judges the newer epochs then.)
    if (epoch && !state.identityTaken && !state.selfExpired) {
      if (auto own = nodes.find(serverId); own != nodes.end() && own->second.latest() > epoch && !created.contains(own->second.latest()))
        identityTakenBy("epoch " + std::to_string(own->second.latest()) + " of this id was registered by another process");
    }
    if (epoch && !state.identityTaken && !state.selfExpired) {
      ++state.ownHeartbeatChecks;
      auto own = co_await bounded(store.get(memberHeartbeatKey(serverId, epoch)));
      if (own.status == 404) {
        ++state.ownHeartbeatMissing;
      } else if (own.status == 200) {
        auto record = parseMemberRecord(own.bytes);
        if (!record || record->instance != instance)
          identityTakenBy("the heartbeat of epoch " + std::to_string(epoch) + " holds another process's record (instance " +
              (record ? record->instance : std::string("unreadable")) + ")");
      }
    }
    std::map<std::string, Member> next;
    std::set<std::pair<std::string, uint64_t>> seen;
    for (const auto& live : liveMembers(nodes, config.ttlSeconds, config.livenessGraceMs)) {
      if (live.id == serverId) continue;
      seen.insert({live.id, live.epoch});
      auto record = co_await addressOf(live.id, live.epoch);
      if (!record) continue;
      next[live.id] = {live.id, record->address, std::to_string(live.epoch), record->startedAtUnixMs,
          live.draining ? "draining" : "active", live.heartbeatUnixMs, live.ageMs};
    }
    std::erase_if(addresses, [&](const auto& entry) { return !seen.contains(entry.first); });
    if (epoch && !state.identityTaken) next[serverId] = selfMember();
    state.lastListMembers = next.size();
    logMemberChanges(serverId, state.members, next, state.lists, state);
    state.members.swap(next);
  }

  kj::Promise<void> heartbeats() {
    while (true) {
      co_await timer.afterDelay(config.heartbeatIntervalMs * kj::MILLISECONDS);
      // After stop(), or once the id is another process's, nothing is written; the promise stays pending (it is joined
      // with the Server's other loops).
      if (stopping || state.identityTaken) co_await kj::Promise<void>(kj::NEVER_DONE);
      try {
        if (!state.selfExpired) {
          co_await writeHeartbeat();
        } else if (!state.draining) {
          // A new epoch once the store answers (an exiting Server takes none).
          const auto old = epoch;
          co_await registerEpoch(false);
          co_await writeHeartbeat();
          state.selfExpired = false;
          state.leaseStatus = "active";
          std::cout << "[membership] {\"event\":\"re-registered\",\"nodeId\":" << quote(serverId) << ",\"oldEpoch\":\"" << old
                    << "\",\"epoch\":\"" << epoch << "\",\"observedAtUnixMs\":" << nowUnixMs() << "}" << std::endl;
          state.members[serverId] = selfMember();
          co_await removeOldObjects();
        }
      } catch (const kj::Exception& error) {
        state.leaseConnected = false;
        failed(state.selfExpired ? "re-registration" : "heartbeat", error);
      }
    }
  }

  kj::Promise<void> listings() {
    while (true) {
      co_await timer.afterDelay(config.listIntervalMs * kj::MILLISECONDS);
      if (stopping) co_await kj::Promise<void>(kj::NEVER_DONE);
      try {
        co_await listMembers();
      } catch (const kj::Exception& error) {
        // Static stability: the last member table stays in use.
        ++state.listFailures;
        state.watchConnected = false;
        failed("listing", error);
      }
    }
  }

  // The epoch's own deadline, by this node's monotonic clock: TTL after the last heartbeat it wrote was sent.
  kj::Promise<void> watchExpiry() {
    while (true) {
      co_await timer.afterDelay(EXPIRY_CHECK_MS * kj::MILLISECONDS);
      if (stopping || state.identityTaken || state.selfExpired || !epoch) continue;
      if (timer.now() - lastHeartbeatOk <= config.ttlSeconds * kj::SECONDS) continue;
      state.selfExpired = true;
      ++state.selfExpiries;
      state.selfExpiredAtUnixMs = nowUnixMs();
      state.leaseStatus = "expired";
      state.leaseConnected = false;
      // Never revived: calls addressed to this epoch are refused from now on.
      state.heldEpochs.erase(state.epoch);
      std::cout << "[membership] lease expired node=" << serverId << " old-epoch=" << epoch << std::endl;
      std::cout << "[membership] {\"event\":\"self-expired\",\"nodeId\":" << quote(serverId) << ",\"epoch\":\"" << epoch
                << "\",\"lastHeartbeatAtUnixMs\":" << state.lastHeartbeatAtUnixMs << ",\"observedAtUnixMs\":" << nowUnixMs()
                << "}" << std::endl;
    }
  }

  // Whether the store's create-if-absent holds under concurrency (4 writers, 5 rounds, under members/<id>/probe/):
  // reported in the log and the state endpoint; a store where it does not hold is warned of, not refused.
  kj::Promise<void> measure() {
    try {
      auto measurement = co_await measureConditionalPut(store, memberProbePrefix(serverId), 4, 5);
      state.conditionalPutAtomic = measurement.atomic() ? "true" : "false";
      state.conditionalPutProbe = measurement.json();
      std::cout << "[membership] {\"event\":\"conditional-put\",\"nodeId\":" << quote(serverId) << ",\"measurement\":"
                << state.conditionalPutProbe
                << (measurement.atomic() ? "" : ",\"warning\":\"the object store's create-if-absent did not hold under "
                    "concurrency: the membership does not rely on it (registrations are read back), but a control plane "
                    "with more than one instance and Durable Objects would\"") << "}" << std::endl;
    } catch (const kj::Exception& error) {
      state.conditionalPutAtomic = std::string("failed: ") + error.getDescription().cStr();
      failed("conditional put measurement", error);
    }
  }
};

}  // namespace

std::string MembershipView::json() const {
  std::ostringstream output;
  if (backend == "objectStore") {
    output << "{\"backend\":" << quote(backend) << ",\"epoch\":" << quote(epoch) << ",\"instance\":" << quote(instance)
           << ",\"leaseStatus\":" << quote(leaseStatus) << ",\"grantedTtlSeconds\":" << grantedTtlSeconds
           << ",\"storeConnected\":" << (leaseConnected && watchConnected ? "true" : "false")
           << ",\"heartbeatConnected\":" << (leaseConnected ? "true" : "false")
           << ",\"listConnected\":" << (watchConnected ? "true" : "false")
           << ",\"heartbeatIntervalMs\":" << heartbeatIntervalMs << ",\"listIntervalMs\":" << listIntervalMs
           << ",\"livenessGraceMs\":" << livenessGraceMs << ",\"createSettleMs\":" << createSettleMs
           << ",\"heartbeatsWritten\":" << heartbeatsWritten << ",\"heartbeatFailures\":" << heartbeatFailures
           << ",\"lastHeartbeatAtUnixMs\":" << lastHeartbeatAtUnixMs << ",\"lists\":" << lists << ",\"listFailures\":" << listFailures
           << ",\"lastListAtUnixMs\":" << lastListAtUnixMs << ",\"lastListMembers\":" << lastListMembers
           << ",\"addressReads\":" << addressReads << ",\"ignoredKeys\":" << ignoredKeys
           << ",\"selfExpired\":" << (selfExpired ? "true" : "false") << ",\"selfExpiries\":" << selfExpiries
           << ",\"selfExpiredAtUnixMs\":" << selfExpiredAtUnixMs
           << ",\"clockSkewMs\":" << clockSkewMs << ",\"clockSkewMinMs\":" << clockSkewMinMs << ",\"clockSkewMaxMs\":" << clockSkewMaxMs
           << ",\"clockSkewToleranceMs\":" << clockSkewToleranceMs << ",\"clockCheckedAtUnixMs\":" << clockCheckedAtUnixMs
           << ",\"clockChecks\":" << clockChecks << ",\"clockWarnings\":" << clockWarnings
           << ",\"registrations\":" << registrations << ",\"registrationAttempts\":" << registrationAttempts
           << ",\"registrationsLost\":" << registrationsLost << ",\"identityWaits\":" << identityWaits
           << ",\"startupCollisionWaitMs\":" << startupCollisionWaitMs
           << ",\"identityTaken\":" << (identityTaken ? "true" : "false") << ",\"identityTakenReason\":" << quote(identityTakenReason)
           << ",\"ownHeartbeatChecks\":" << ownHeartbeatChecks << ",\"ownHeartbeatMissing\":" << ownHeartbeatMissing
           << ",\"oldObjectsRemoved\":" << oldObjectsRemoved << ",\"lastError\":" << quote(lastError)
           << ",\"draining\":" << (draining ? "true" : "false") << ",\"drainAnnounce\":" << quote(drainAnnounce)
           << ",\"drainAnnouncedAtUnixMs\":" << drainAnnouncedAtUnixMs << ",\"drainAnnounceAttempts\":" << drainAnnounceAttempts
           << ",\"revokeStatus\":" << quote(revokeStatus) << ",\"revokedAtUnixMs\":" << revokedAtUnixMs
           << ",\"memberChanges\":" << memberChanges << ",\"invalidMembers\":" << invalidMembers.size()
           << ",\"skippedMemberValues\":" << skippedMemberValues
           << ",\"storeClient\":" << (storeClient.empty() ? std::string("null") : storeClient) << ",\"heldEpochs\":[";
    size_t held = 0;
    for (const auto& value : heldEpochs) output << (held++ ? "," : "") << quote(value);
    output << "],\"members\":[";
    size_t index = 0;
    for (const auto& [id, member] : members) {
      auto value = memberJson(member);
      value.pop_back();
      output << (index++ ? "," : "") << value << ",\"heartbeatUnixMs\":" << member.heartbeatUnixMs
             << ",\"heartbeatAgeMs\":" << member.heartbeatAgeMs << "}";
    }
    output << "]}";
    return output.str();
  }
  output << "{\"backend\":" << quote(backend) << ",\"epoch\":" << quote(epoch)
         << ",\"leaseId\":" << quote(leaseId) << ",\"leaseStatus\":" << quote(leaseStatus)
         << ",\"grantedTtlSeconds\":" << grantedTtlSeconds
         << ",\"etcdConnected\":" << (leaseConnected && watchConnected ? "true" : "false")
         << ",\"leaseConnected\":" << (leaseConnected ? "true" : "false")
         << ",\"watchConnected\":" << (watchConnected ? "true" : "false")
         << ",\"revision\":\"" << revision << "\",\"fullRereads\":" << fullRereads
         << ",\"compactions\":" << compactions << ",\"registrations\":" << registrations
         << ",\"keepAlives\":" << keepAlives
         << ",\"startupCollisionWaitMs\":" << startupCollisionWaitMs
         << ",\"startupConflictTtlSequence\":[";
  for (size_t index = 0; index < startupConflictTtlSequence.size(); ++index) {
    if (index) output << ',';
    output << startupConflictTtlSequence[index];
  }
  output << "]" << ",\"lastError\":" << quote(lastError)
         << ",\"draining\":" << (draining ? "true" : "false") << ",\"drainAnnounce\":" << quote(drainAnnounce)
         << ",\"drainAnnouncedAtUnixMs\":" << drainAnnouncedAtUnixMs << ",\"drainAnnounceAttempts\":" << drainAnnounceAttempts
         << ",\"revokeStatus\":" << quote(revokeStatus) << ",\"revokedAtUnixMs\":" << revokedAtUnixMs
         << ",\"memberChanges\":" << memberChanges << ",\"invalidMembers\":" << invalidMembers.size()
         << ",\"skippedMemberValues\":" << skippedMemberValues
         << ",\"heldEpochs\":[";
  size_t heldCount = 0;
  for (const auto& held : heldEpochs) {
    if (heldCount++) output << ',';
    output << quote(held);
  }
  output << "],\"members\":[";
  size_t count = 0;
  for (const auto& [id, member] : members) {
    if (count++ > 0) output << ',';
    output << memberJson(member);
  }
  output << "]}";
  return output.str();
}

std::string MembershipView::storageJson() const {
  std::ostringstream output;
  const char* atomic = conditionalPutAtomic == "true" ? "true" : conditionalPutAtomic == "false" ? "false" : "null";
  output << "{\"backend\":\"s3\",\"endpoint\":" << quote(storeEndpoint) << ",\"bucket\":" << quote(bucket)
         << ",\"conditionalPutAtomic\":" << atomic << ",\"conditionalPutMeasurement\":" << quote(conditionalPutAtomic)
         << ",\"conditionalPutProbe\":" << conditionalPutProbe << ",\"clockSkewMs\":" << clockSkewMs
         << ",\"clockCheckedAtUnixMs\":" << clockCheckedAtUnixMs << ",\"clockSkewToleranceMs\":" << clockSkewToleranceMs << "}";
  return output.str();
}

kj::Own<Membership> newMembership(const MembershipConfig& config, const std::string& serverId,
    const std::string& peerListenAddress, const std::map<std::string, std::string>& staticPeers,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable, const ObjectStoreConfig& store) {
  KJ_REQUIRE(config.backend == "file" || config.backend == "etcd" || config.backend == "objectStore",
      "unknown membership backend");
  if (config.backend == "file") {
    return kj::heap<FileMembership>(serverId,
        config.advertisedAddress.empty() ? peerListenAddress : config.advertisedAddress, staticPeers);
  }
  if (config.backend == "objectStore") {
    KJ_REQUIRE(staticPeers.empty(), "the object store membership forbids static peers");
    KJ_REQUIRE(advertised(config.advertisedAddress), "membership advertisedAddress must be a dialable address");
    KJ_REQUIRE(store.backend == "s3" && !store.endpoint.empty(), "the object store membership needs the storage block");
    KJ_REQUIRE(config.ttlSeconds >= 2 && config.heartbeatIntervalMs > 0 &&
        uint64_t(config.heartbeatIntervalMs) * 2 < uint64_t(config.ttlSeconds) * 1000 &&
        config.livenessGraceMs >= config.heartbeatIntervalMs && config.listIntervalMs > 0 && config.startupTimeoutMs > 0 &&
        config.retryMs > 0, "invalid object store membership timing");
    return kj::heap<ObjectStoreMembership>(config, serverId, store, network, timer);
  }
  KJ_REQUIRE(staticPeers.empty(), "etcd membership forbids static peers");
  KJ_REQUIRE(advertised(config.advertisedAddress), "membership advertisedAddress must be a dialable address");
  KJ_REQUIRE(config.endpoint.starts_with("http://") && !config.endpoint.ends_with('/'),
      "membership endpoint must be an HTTP gateway URL without a trailing slash");
  KJ_REQUIRE(config.ttlSeconds >= 1 && config.ttlSeconds <= 86400, "invalid membership TTL");
  KJ_REQUIRE(config.requestTimeoutMs > 0 && config.retryMs > 0 && config.startupTimeoutMs > 0,
      "membership timeouts must be positive");
  return kj::heap<EtcdMembership>(config, serverId, network, timer, headerTable);
}

}  // namespace coworkerd

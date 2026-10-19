#include "membership.h"
#include "etcd.h"
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

  kj::Promise<void> applyMembers(const EtcdBatch& batch) {
    auto next = batch.full ? std::map<std::string, Member>() : state.members;
    for (const auto& change : batch.changes) {
      if (!memberKey(change.key)) continue;
      if (!change.kv) { next.erase(change.key.substr(std::string_view(PREFIX).size())); continue; }
      auto member = decodeMember(*change.kv);
      next[member.id] = std::move(member);
    }
    logMemberChanges(serverId, state.members, next, batch.revision, state);
    state.members.swap(next);
    return kj::READY_NOW;
  }

};

}  // namespace

std::string MembershipView::json() const {
  std::ostringstream output;
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
         << ",\"memberChanges\":" << memberChanges
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

kj::Own<Membership> newMembership(const MembershipConfig& config, const std::string& serverId,
    const std::string& peerListenAddress, const std::map<std::string, std::string>& staticPeers,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable) {
  KJ_REQUIRE(config.backend == "file" || config.backend == "etcd", "unknown membership backend");
  if (config.backend == "file") {
    return kj::heap<FileMembership>(serverId,
        config.advertisedAddress.empty() ? peerListenAddress : config.advertisedAddress, staticPeers);
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

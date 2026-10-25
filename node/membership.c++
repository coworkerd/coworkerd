#include "membership.h"
#include "json.h"
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
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <utility>

namespace coworkerd {
namespace {

using namespace json;

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

class ObjectStoreMembership final: public Membership, private kj::TaskSet::ErrorHandler {
public:
  ObjectStoreMembership(MembershipConfig config, std::string serverId, const ObjectStoreConfig& storeConfig,
      kj::Network& network, kj::Timer& timer)
      : config(std::move(config)), serverId(std::move(serverId)), store(storeConfig, network, timer), timer(timer),
        startedAtUnixMs(nowUnixMs()), lastHeartbeatOk(timer.now()), lastHeartbeatOkDue(timer.now()), lastListOk(timer.now()),
        startupDeadline(timer.now()) {
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

  void onSuspects(SuspectsCallback callback) override { suspectsCallback = std::move(callback); }

  void setDurableProcesses(uint32_t count) override { state.durableProcesses = count; }

  kj::Promise<void> writeSuspects(std::string bytes) override {
    KJ_REQUIRE(epoch && !stopping && !state.identityTaken && !state.selfExpired, "no live epoch to write the suspects object for",
        serverId.c_str());
    const auto number = epoch;
    const bool removing = bytes.empty();
    ++writing;
    KJ_DEFER(--writing);
    try {
      if (removing) {
        co_await bounded(store.remove(memberSuspectsKey(serverId, number)));
        ++state.suspectsRemoved;
      } else {
        co_await bounded(store.put(memberSuspectsKey(serverId, number), std::move(bytes)));
        ++state.suspectsWritten;
      }
    } catch (const kj::Exception&) {
      ++state.suspectsWriteFailures;
      throw;
    }
    // Written for the epoch this process still holds (a new one gets its object from the next write).
    if (number == epoch) state.suspectsObject = !removing;
  }

  kj::Promise<void> start() override {
    startupDeadline = timer.now() + config.startupTimeoutMs * kj::MILLISECONDS;
    while (true) {
      try {
        KJ_REQUIRE(timer.now() < startupDeadline, "membership startup timeout", state.lastError.c_str());
        if (!epoch) co_await registerEpoch(true);
        if (cadence == kj::none) {
          co_await writeHeartbeat();
          // The cadence starts at this first heartbeat, not once the old epochs' objects are removed and the members
          // listed: on a slow store those take longer than an interval.
          cadence = heartbeats();
        }
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
    serving = true;
    KJ_IF_SOME(running, cadence) {
      auto loop = kj::mv(running);
      cadence = kj::none;
      return loop.exclusiveJoin(listings()).exclusiveJoin(watchExpiry());
    }
    return heartbeats().exclusiveJoin(listings()).exclusiveJoin(watchExpiry());
  }

  kj::Promise<void> stop() override {
    if (stopping) co_return;
    // Heartbeats and registration end before the objects go: once removed, they never come back from this process.
    stopping = true;
    state.leaseStatus = "stopping";
    for (auto until = timer.now() + boundMs() * kj::MILLISECONDS; writing > 0 && timer.now() < until;)
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
        if (state.suspectsObject) {
          co_await bounded(store.remove(memberSuspectsKey(serverId, epoch)));
          state.suspectsObject = false;
          ++state.suspectsRemoved;
        }
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
  // The cadence time of the last heartbeat the store took, and of every heartbeat in flight: one that went out, by the
  // cadence, within the TTL of the last one taken decides the epoch's expiry (expireIfDue()).
  kj::TimePoint lastHeartbeatOkDue;
  std::multiset<kj::TimePoint> dueInFlight;
  // The last listing of members/ that answered, and whether one failed since (the recovery anchor's blindness).
  kj::TimePoint lastListOk;
  bool listFailedSinceOk = false;
  kj::TimePoint startupDeadline;
  bool fatal = false, stopping = false, collisionAllowanceUsed = false;
  // Writes of this epoch's objects in flight (the heartbeats, the suspects object): stop() waits for them.
  uint32_t writing = 0;
  // The heartbeats sent and the newest of them that finished: heartbeatConnected follows that one alone.
  uint64_t heartbeatsSent = 0, heartbeatsSettled = 0;
  // The heartbeats' loop from start()'s first heartbeat on, which run() joins; and whether run() began (only then can
  // the epoch expire).
  kj::Maybe<kj::Promise<void>> cadence;
  bool serving = false;
  std::function<void(std::string)> identityCallback;
  SuspectsCallback suspectsCallback;
  // (node, epoch) -> the ETag of its suspects object as this membership last read it.
  std::map<std::pair<std::string, uint64_t>, std::string> suspectsEtags;
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
    record.durableProcesses = state.durableProcesses;
    return record;
  }

  Member selfMember() const {
    return {serverId, config.advertisedAddress, std::to_string(epoch), startedAtUnixMs, state.draining ? "draining" : "active",
        state.lastHeartbeatAtUnixMs, 0, state.durableProcesses};
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

  // Whether the newest write of this id is a heartbeat of this process's own: under an epoch it held, its instance in the
  // bytes. A heartbeat it gave up on can reach the store after it, also once its epoch expired.
  kj::Promise<bool> ownHeartbeat(const ListedNode& node, const IdentityUse& use) {
    auto listed = node.epochs.find(use.freshestEpoch);
    if (listed == node.epochs.end() || !listed->second.heartbeat || listed->second.heartbeatUnixMs != use.freshestUnixMs ||
        !state.heldEpochs.contains(std::to_string(use.freshestEpoch))) co_return false;
    auto response = co_await bounded(store.get(memberHeartbeatKey(serverId, use.freshestEpoch)));
    if (response.status == 404) co_return false;
    KJ_REQUIRE(response.status == 200, "cannot read this id's heartbeat back", response.status);
    auto record = parseMemberRecord(response.bytes);
    co_return record && record->instance == instance;
  }

  // Waits while the id is in use: until the heartbeat or registration that makes it so is TTL old. Written again
  // meanwhile, or still in use past TTL + grace + 2 s, it is another process's: a conflict -- unless what was written
  // is a late heartbeat of this process's own, which only restarts the wait.
  kj::Promise<void> waitForIdentity(IdentityUse first, bool starting) {
    ++state.identityWaits;
    const auto began = timer.now();
    const auto window = (uint64_t(config.ttlSeconds) * 1000 + config.livenessGraceMs + 2000) * kj::MILLISECONDS;
    auto bound = began + window;
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
        if (co_await ownHeartbeat(nodes[serverId], use)) {
          std::cout << "[membership] {\"event\":\"own-late-heartbeat\",\"nodeId\":" << quote(serverId) << ",\"epoch\":\""
                    << use.freshestEpoch << "\",\"lastModifiedUnixMs\":" << use.freshestUnixMs << ",\"observedAtUnixMs\":"
                    << nowUnixMs() << "}" << std::endl;
          first = use;
          bound = kj::max(bound, timer.now() + window);
        } else {
          conflict(starting, "epoch " + std::to_string(use.freshestEpoch) + " of this id was written again while waiting (" +
              std::to_string(state.startupCollisionWaitMs) + " ms)");
        }
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
    // Beside the epochs this process held before: a stateless call addressed to an expired one is still served (an
    // expired epoch is never renewed; re-registration takes a new one).
    state.heldEpochs.insert(state.epoch);
    ++state.registrations;
    state.storeClient = store.stats().json();
    std::cout << "[membership] registered node=" << serverId << " epoch=" << epoch << " address=" << config.advertisedAddress
              << " ttl-seconds=" << config.ttlSeconds << " attempts=" << attempt << std::endl;
  }

  // `due`: the heartbeat's time on the cadence (start()'s and a re-registration's first heartbeat: when it is sent).
  kj::Promise<void> writeHeartbeat(kj::Maybe<kj::TimePoint> due = kj::none) {
    KJ_REQUIRE(epoch && !state.identityTaken && !stopping, "no epoch to write a heartbeat for");
    auto record = recordOf(state.draining ? "draining" : "active");
    const auto sent = timer.now();
    const auto slot = due.orDefault(sent);
    const auto number = ++heartbeatsSent;
    ++writing;
    KJ_DEFER(--writing);
    auto inFlight = dueInFlight.insert(slot);
    bool settled = false;
    KJ_DEFER(if (!settled) dueInFlight.erase(inFlight));
    try {
      co_await bounded(store.put(memberHeartbeatKey(serverId, epoch), memberRecordJson(record)));
    } catch (const kj::Exception&) {
      settled = true;
      dueInFlight.erase(inFlight);
      ++state.heartbeatFailures;
      if (number > heartbeatsSettled) { heartbeatsSettled = number; state.leaseConnected = false; }
      // It may be the heartbeat the epoch's expiry waited for: judged now, not at the next check.
      expireIfDue();
      throw;
    }
    settled = true;
    dueInFlight.erase(inFlight);
    // Two may be in flight (heartbeats()): one that finishes after a later one moves nothing back.
    if (sent > lastHeartbeatOk) lastHeartbeatOk = sent;
    if (slot > lastHeartbeatOkDue) lastHeartbeatOkDue = slot;
    ++state.heartbeatsWritten;
    state.lastHeartbeatAtUnixMs = std::max(state.lastHeartbeatAtUnixMs, record.writtenAtUnixMs);
    state.storeClient = store.stats().json();
    if (number > heartbeatsSettled) { heartbeatsSettled = number; state.leaseConnected = true; }
  }

  // A member's suspects object, handed to the callback (Suspects adopts what it holds) once it reads; its ETag is
  // remembered so that the next listing reads it again only when it changed.
  kj::Promise<void> readSuspects(std::string id, uint64_t number, std::string etag) {
    if (!suspectsCallback) co_return;
    ++state.suspectsReads;
    ObjectResponse response;
    try {
      response = co_await bounded(store.get(memberSuspectsKey(id, number)));
    } catch (const kj::Exception& error) {
      ++state.suspectsReadFailures;
      failed("suspects read", error);
      co_return;
    }
    if (response.status != 200) {
      // Gone since the listing (its node removed it): the next listing tells.
      if (response.status != 404) ++state.suspectsReadFailures;
      co_return;
    }
    suspectsEtags[{id, number}] = etag;
    if (suspectsCallback) suspectsCallback(id, std::to_string(number), response.bytes);
  }

  // The old epochs' heartbeats, markers and suspects objects of this id go once a new epoch holds it (its suspicions are
  // read first: a process that ended without removing them hands them to its successor); of its registrations the
  // newest two stay. Best effort: what is left is dropped by age and removed by the next epoch.
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
        if (listed.suspects) {
          co_await readSuspects(serverId, number, listed.suspectsEtag);
          co_await bounded(store.remove(memberSuspectsKey(serverId, number)));
          ++state.oldObjectsRemoved;
        }
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
    // The recovery anchor (v54): once this node could not list the store for longer than the TTL, the staleness that
    // built up while it was blind is not counted -- an outage of the whole cluster stopped the others' heartbeats too.
    // Until the recovery + TTL + grace the members it had whose heartbeat objects are still listed stay (a newer epoch
    // of theirs takes their place as it appears); then, and for a member whose heartbeat object is gone, the usual rule.
    const auto listedAt = timer.now();
    if (listFailedSinceOk && listedAt - lastListOk > config.ttlSeconds * kj::SECONDS && sample.dateUnixMs) {
      state.recoveredUntilUnixMs = sample.dateUnixMs + uint64_t(config.ttlSeconds) * 1000 + config.livenessGraceMs;
      ++state.recoveries;
      std::cout << "[membership] {\"event\":\"recovery-anchor\",\"nodeId\":" << quote(serverId) << ",\"blindMs\":"
                << (listedAt - lastListOk) / kj::MILLISECONDS << ",\"untilUnixMs\":" << state.recoveredUntilUnixMs
                << ",\"observedAtUnixMs\":" << nowUnixMs() << "}" << std::endl;
    }
    listFailedSinceOk = false;
    lastListOk = listedAt;
    ++state.lists;
    state.lastListAtUnixMs = nowUnixMs();
    state.lastListDateUnixMs = sample.dateUnixMs;
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
    // A listing that does not hold this node's own live heartbeat is not a listing of the members (v60's core
    // regression: the first second of a stopped store answered 200 with an empty list, and two nodes dropped each
    // other on it). Nothing is judged by it: the member table stays as it was and the listing counts as a failure,
    // so the recovery anchor works exactly as before.
    if (epoch && !state.identityTaken && !state.selfExpired && lastHeartbeatOk != kj::origin<kj::TimePoint>() &&
        timer.now() - lastHeartbeatOk <= config.ttlSeconds * kj::SECONDS) {
      auto own = nodes.find(serverId);
      const bool mine = own != nodes.end() && own->second.epochs.contains(epoch) &&
          own->second.epochs.at(epoch).heartbeat;
      if (!mine) {
        ++state.incompleteListings;
        std::cout << "[membership] {\"event\":\"incomplete-listing\",\"nodeId\":" << quote(serverId)
                  << ",\"epoch\":" << epoch << ",\"keys\":" << listing.objects.size() << ",\"nodes\":" << nodes.size()
                  << ",\"observedAtUnixMs\":" << nowUnixMs()
                  << ",\"reason\":\"the listing does not hold this node's own heartbeat, which it wrote within the TTL\"}"
                  << std::endl;
        KJ_FAIL_REQUIRE("the listing of the members does not hold this node's own heartbeat");
      }
    }
    auto current = liveMembers(nodes, config.ttlSeconds, config.livenessGraceMs);
    state.keptMembers = 0;
    if (state.recoveredUntilUnixMs && sample.dateUnixMs && sample.dateUnixMs < state.recoveredUntilUnixMs) {
      for (const auto& [id, member] : state.members) {
        if (id == serverId || std::any_of(current.begin(), current.end(), [&](const LiveMember& m) { return m.id == id; })) continue;
        const auto number = std::strtoull(member.epoch.c_str(), nullptr, 10);
        auto listed = nodes.find(id);
        if (!number || listed == nodes.end()) continue;
        auto heartbeat = listed->second.epochs.find(number);
        if (heartbeat == listed->second.epochs.end() || !heartbeat->second.heartbeat) continue;
        current.push_back({id, number, heartbeat->second.heartbeatUnixMs,
            static_cast<int64_t>(sample.dateUnixMs) - static_cast<int64_t>(heartbeat->second.heartbeatUnixMs), heartbeat->second.draining});
        ++state.keptMembers;
      }
    } else {
      state.recoveredUntilUnixMs = 0;
    }
    std::map<std::string, Member> next;
    std::set<std::pair<std::string, uint64_t>> seen;
    for (const auto& live : current) {
      if (live.id == serverId) continue;
      seen.insert({live.id, live.epoch});
      auto record = co_await addressOf(live.id, live.epoch);
      if (!record) continue;
      next[live.id] = {live.id, record->address, std::to_string(live.epoch), record->startedAtUnixMs,
          live.draining ? "draining" : "active", live.heartbeatUnixMs, live.ageMs, record->durableProcesses};
    }
    std::erase_if(addresses, [&](const auto& entry) { return !seen.contains(entry.first); });
    // The suspects objects ride this listing: a live member's is read only when its ETag changed since the last read.
    for (const auto& [id, number] : seen) {
      const auto& listed = nodes[id].epochs[number];
      if (!listed.suspects) continue;
      auto known = suspectsEtags.find({id, number});
      if (known != suspectsEtags.end() && known->second == listed.suspectsEtag) continue;
      co_await readSuspects(id, number, listed.suspectsEtag);
    }
    std::erase_if(suspectsEtags, [&](const auto& entry) { return !seen.contains(entry.first); });
    if (epoch && !state.identityTaken) next[serverId] = selfMember();
    state.lastListMembers = next.size();
    logMemberChanges(serverId, state.members, next, state.lists, state);
    state.members.swap(next);
  }

  // The heartbeats of an epoch go out on a fixed cadence: one every interval from its first, whether the one before has
  // finished or not. Each is bounded by the interval and given up as a failure when that runs out, so at most two are in
  // flight and one that hangs never holds up the next. One given up on may still reach the store after a later one:
  // liveness reads only LastModified, and its bytes name this process as the later one's do.
  kj::Promise<void> heartbeats() {
    const auto interval = config.heartbeatIntervalMs * kj::MILLISECONDS;
    // The epoch's first heartbeat is start()'s, just written.
    auto next = lastHeartbeatOk + interval;
    while (true) {
      co_await timer.atTime(next);
      // After stop(), or once the id is another process's, nothing is written; the promise stays pending (it is joined
      // with the Server's other loops).
      if (stopping || state.identityTaken) co_await kj::Promise<void>(kj::NEVER_DONE);
      if (!state.selfExpired) {
        tasks.add(beat(next));
      } else if (!state.draining) {
        // A new epoch once the store answers (an exiting Server takes none); its first heartbeat starts its cadence, and
        // the old epochs' objects go beside it.
        try {
          const auto old = epoch;
          co_await registerEpoch(false);
          co_await writeHeartbeat();
          state.selfExpired = false;
          state.leaseStatus = "active";
          std::cout << "[membership] {\"event\":\"re-registered\",\"nodeId\":" << quote(serverId) << ",\"oldEpoch\":\"" << old
                    << "\",\"epoch\":\"" << epoch << "\",\"observedAtUnixMs\":" << nowUnixMs() << "}" << std::endl;
          state.members[serverId] = selfMember();
          next = lastHeartbeatOkDue;
          tasks.add(removeOldObjects());
        } catch (const kj::Exception& error) {
          state.leaseConnected = false;
          failed("re-registration", error);
        }
      }
      // A time the loop missed -- a re-registration, a stalled event loop -- is not made up: the next heartbeat goes at the
      // first time of the cadence still ahead.
      do next += interval; while (next <= timer.now());
    }
  }

  // One heartbeat of the cadence, on its own (a failure is the store's answer or the interval running out).
  kj::Promise<void> beat(kj::TimePoint due) {
    try {
      co_await writeHeartbeat(due);
    } catch (const kj::Exception& error) {
      failed("heartbeat", error);
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
        listFailedSinceOk = true;
        state.watchConnected = false;
        failed("listing", error);
      }
    }
  }

  kj::Promise<void> watchExpiry() {
    while (true) {
      co_await timer.afterDelay(EXPIRY_CHECK_MS * kj::MILLISECONDS);
      expireIfDue();
    }
  }

  // The epoch's own deadline, by this node's monotonic clock: TTL after the last heartbeat the store took was sent --
  // unless a heartbeat that went out, by the cadence, within that TTL is still in flight: its outcome decides then, and
  // its failure expires the epoch at once (writeHeartbeat()). With a TTL of three intervals, the third heartbeat after
  // two failed ones goes out at the deadline itself.
  void expireIfDue() {
    if (!serving || stopping || state.identityTaken || state.selfExpired || !epoch) return;
    const auto ttl = config.ttlSeconds * kj::SECONDS;
    if (timer.now() - lastHeartbeatOk <= ttl) return;
    if (!dueInFlight.empty() && *dueInFlight.begin() <= lastHeartbeatOkDue + ttl) return;
    state.selfExpired = true;
    ++state.selfExpiries;
    state.selfExpiredAtUnixMs = nowUnixMs();
    state.leaseStatus = "expired";
    state.leaseConnected = false;
    // Never renewed (once the store answers, a new epoch is registered); still held: the stateless calls addressed to it
    // are served.
    std::cout << "[membership] lease expired node=" << serverId << " old-epoch=" << epoch << std::endl;
    std::cout << "[membership] {\"event\":\"self-expired\",\"nodeId\":" << quote(serverId) << ",\"epoch\":\"" << epoch
              << "\",\"lastHeartbeatAtUnixMs\":" << state.lastHeartbeatAtUnixMs << ",\"observedAtUnixMs\":" << nowUnixMs()
              << "}" << std::endl;
  }

  // Whether the store's create-if-absent holds under concurrency (4 writers, 5 rounds, under members/<id>/probe/):
  // reported in the log and the state endpoint; a store where it does not hold is warned of, not refused. A measurement
  // the store failed is tried again every 5 s until one completes (Durable Objects wait for its answer).
  kj::Promise<void> measure() {
    while (!stopping) {
      try {
        auto measurement = co_await measureConditionalPut(store, memberProbePrefix(serverId), 4, 5);
        state.conditionalPutAtomic = measurement.atomic() ? "true" : "false";
        state.conditionalPutProbe = measurement.json();
        std::cout << "[membership] {\"event\":\"conditional-put\",\"nodeId\":" << quote(serverId) << ",\"measurement\":"
                  << state.conditionalPutProbe
                  << (measurement.atomic() ? "" : ",\"warning\":\"the object store's create-if-absent did not hold under "
                      "concurrency: the membership does not rely on it (registrations are read back), but a control plane "
                      "with more than one instance and Durable Objects would: this node takes no Durable Object shard\"")
                  << "}" << std::endl;
        co_return;
      } catch (const kj::Exception& error) {
        state.conditionalPutAtomic = std::string("failed: ") + error.getDescription().cStr();
        failed("conditional put measurement", error);
      }
      co_await timer.afterDelay(5 * kj::SECONDS);
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
           << ",\"lastListAtUnixMs\":" << lastListAtUnixMs << ",\"lastListDateUnixMs\":" << lastListDateUnixMs
           << ",\"lastListMembers\":" << lastListMembers << ",\"durableProcesses\":" << durableProcesses
           << ",\"addressReads\":" << addressReads << ",\"ignoredKeys\":" << ignoredKeys
           << ",\"selfExpired\":" << (selfExpired ? "true" : "false") << ",\"selfExpiries\":" << selfExpiries
           << ",\"selfExpiredAtUnixMs\":" << selfExpiredAtUnixMs << ",\"recoveries\":" << recoveries
           << ",\"recoveredUntilUnixMs\":" << recoveredUntilUnixMs << ",\"keptMembers\":" << keptMembers
           << ",\"clockSkewMs\":" << clockSkewMs << ",\"clockSkewMinMs\":" << clockSkewMinMs << ",\"clockSkewMaxMs\":" << clockSkewMaxMs
           << ",\"clockSkewToleranceMs\":" << clockSkewToleranceMs << ",\"clockCheckedAtUnixMs\":" << clockCheckedAtUnixMs
           << ",\"clockChecks\":" << clockChecks << ",\"clockWarnings\":" << clockWarnings
           << ",\"registrations\":" << registrations << ",\"registrationAttempts\":" << registrationAttempts
           << ",\"registrationsLost\":" << registrationsLost << ",\"identityWaits\":" << identityWaits
           << ",\"startupCollisionWaitMs\":" << startupCollisionWaitMs
           << ",\"identityTaken\":" << (identityTaken ? "true" : "false") << ",\"identityTakenReason\":" << quote(identityTakenReason)
           << ",\"ownHeartbeatChecks\":" << ownHeartbeatChecks << ",\"ownHeartbeatMissing\":" << ownHeartbeatMissing
           << ",\"incompleteListings\":" << incompleteListings
           << ",\"oldObjectsRemoved\":" << oldObjectsRemoved << ",\"suspectsObject\":" << (suspectsObject ? "true" : "false")
           << ",\"suspectsWritten\":" << suspectsWritten << ",\"suspectsRemoved\":" << suspectsRemoved
           << ",\"suspectsWriteFailures\":" << suspectsWriteFailures << ",\"suspectsReads\":" << suspectsReads
           << ",\"suspectsReadFailures\":" << suspectsReadFailures << ",\"lastError\":" << quote(lastError)
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
             << ",\"heartbeatAgeMs\":" << member.heartbeatAgeMs << ",\"durableProcesses\":" << member.durableProcesses << "}";
    }
    output << "]}";
    return output.str();
  }
  // The file membership: static members, this process's random epoch.
  output << "{\"backend\":" << quote(backend) << ",\"epoch\":" << quote(epoch) << ",\"leaseStatus\":" << quote(leaseStatus)
         << ",\"lastError\":" << quote(lastError)
         << ",\"draining\":" << (draining ? "true" : "false") << ",\"drainAnnounce\":" << quote(drainAnnounce)
         << ",\"drainAnnouncedAtUnixMs\":" << drainAnnouncedAtUnixMs << ",\"drainAnnounceAttempts\":" << drainAnnounceAttempts
         << ",\"revokeStatus\":" << quote(revokeStatus) << ",\"revokedAtUnixMs\":" << revokedAtUnixMs
         << ",\"memberChanges\":" << memberChanges << ",\"heldEpochs\":[";
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
  KJ_REQUIRE(config.backend == "file" || config.backend == "objectStore", "unknown membership backend", config.backend.c_str());
  if (config.backend == "file") {
    return kj::heap<FileMembership>(serverId,
        config.advertisedAddress.empty() ? peerListenAddress : config.advertisedAddress, staticPeers);
  }
  KJ_REQUIRE(staticPeers.empty(), "the object store membership forbids static peers");
  KJ_REQUIRE(advertised(config.advertisedAddress), "membership advertisedAddress must be a dialable address");
  KJ_REQUIRE(store.backend == "s3" && !store.endpoint.empty(), "the object store membership needs the object store");
  KJ_REQUIRE(config.ttlSeconds >= 2 && config.heartbeatIntervalMs > 0 &&
      uint64_t(config.heartbeatIntervalMs) * 2 < uint64_t(config.ttlSeconds) * 1000 &&
      config.livenessGraceMs >= config.heartbeatIntervalMs && config.listIntervalMs > 0 && config.startupTimeoutMs > 0 &&
      config.retryMs > 0, "invalid object store membership timing");
  return kj::heap<ObjectStoreMembership>(config, serverId, store, network, timer);
}

}  // namespace coworkerd

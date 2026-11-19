#pragma once

#include "config.h"

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace coworkerd {

struct Member {
  std::string id;
  std::string address;
  std::string epoch;
  uint64_t startedAtUnixMs;
  // "active", or "draining" once the Server announced its exit: it takes no
  // new work, finishes what it has and then removes its objects. A value without the
  // field is active.
  std::string state = "active";
  // objectStore: its heartbeat's LastModified and how far it was behind the Date of the listing that last saw it.
  uint64_t heartbeatUnixMs = 0;
  int64_t heartbeatAgeMs = 0;
  // objectStore: its durable processes (durableObjects.processes, from its heartbeat): a member with none is no Durable
  // Object shard's default owner.
  uint32_t durableProcesses = 1;
  std::string instance = {};
};

// The lease of this process's membership epochs (objectStore), judged synchronously next to every confirmation of a
// Durable Object write (results/r45/DESIGN.md section 10). All times are on the lease clock, leaseClockMs(), in ms.
//
// S and D: the send time and the cadence slot of the last heartbeat whose success was handled before its end; T and I:
// the TTL and the heartbeat interval. The epoch is HELD while now < S + T (equal is not). Past it, a heartbeat of the
// same epoch still in flight whose slot is at most D + T keeps the answer back (WAIT), until it settles or until its
// end, min(sent, slot) + I, which nothing moves -- and so does the last slot within D + T while it has come but its
// heartbeat has not gone out yet; without either the epoch is EXPIRED and fenced: it never holds again in this
// process, whatever answer comes later. A heartbeat's slot after the epoch's first is D plus the cadence's own distance
// from D's slot (cadenceSlot), so that the slot on the TTL is exactly D + T, whatever the clock readings' jitter. A success counts only when it is handled before its heartbeat's end, for
// the epoch it was sent for, and never moves S or D back. Before the first success of an epoch nothing is held
// (NOT_HELD), which fences nothing. A clock that cannot be read or goes back fences the epoch.
class LeaseTracker {
public:
  enum class Verdict : uint8_t { HELD, WAIT, NOT_HELD, EXPIRED };
  struct Judgment {
    Verdict verdict = Verdict::NOT_HELD;
    // HELD: S + T; WAIT: the earliest end of a heartbeat that keeps the answer back.
    double untilMs = 0;
    const char* reason = "";
  };
  LeaseTracker(double ttlMs, double intervalMs): ttlMs(ttlMs), intervalMs(intervalMs) {}
  // A new epoch of this process (a registration): it holds nothing until one of its heartbeats succeeds in time. The
  // answers of the earlier epochs' heartbeats no longer change anything.
  void begin(uint64_t epoch);
  // A heartbeat of the current epoch goes out at `nowMs` for the cadence slot `slotMs` (at most `nowMs`). Its id, or 0
  // when the epoch is not the current one or is fenced: nothing is sent then.
  uint64_t sent(uint64_t epoch, double nowMs, double slotMs);
  // The store's answer to heartbeat `id`, handled at `nowMs`. True when it renewed the lease.
  bool settled(uint64_t id, bool ok, double nowMs);
  // The lease-clock slot of a heartbeat `sinceRenewedSlotMs` after the slot of the heartbeat that last renewed the
  // lease, on the cadence (only meaningful once renewed()).
  double cadenceSlot(double sinceRenewedSlotMs) const { return okSlotMs + sinceRenewedSlotMs; }
  bool renewed() const { return held; }
  // A heartbeat abandoned without an answer (its request was canceled): it keeps nothing back any more.
  void forget(uint64_t id) { pending.erase(id); }
  // The lease of `epoch` at `nowMs`. EXPIRED for the current epoch fences it (the reason is kept).
  Judgment judge(uint64_t epoch, double nowMs);
  void fence(uint64_t epoch, const char* reason);
  bool fenced(uint64_t epoch) const { return epoch != current || currentFenced; }
  uint64_t epoch() const { return current; }
  const char* fenceReason() const { return reason; }
  size_t inFlight() const { return pending.size(); }
  // Successes handled after their heartbeat's end (or for an epoch that changed or was fenced): counted, never renewing.
  uint64_t lateSuccesses = 0, fences = 0;

private:
  struct Pending { uint64_t epoch; double sentMs, slotMs, endMs; };
  double ttlMs, intervalMs;
  uint64_t current = 0, nextId = 0;
  bool currentFenced = false, held = false;
  double okSentMs = 0, okSlotMs = 0, lastNowMs = 0, lastSentSlotMs = -1;
  const char* reason = "";
  std::map<uint64_t, Pending> pending;
};

// All access takes place on the Server's KJ event loop. Request handling only
// reads this local view; it never waits for the authority.
struct MembershipView {
  std::string backend;
  std::string epoch;
  // Only successfully held identities from this Membership instance belong here.
  // Re-registration retains them; a new Server process starts with an empty set.
  // (objectStore: an epoch that expired by this node's own clock stays held -- its stateless calls are served -- and is
  // never renewed: re-registration takes a new one.)
  std::set<std::string> heldEpochs;
  std::string leaseStatus = "starting";
  uint32_t grantedTtlSeconds = 0;
  bool leaseConnected = false;
  bool watchConnected = false;
  uint64_t registrations = 0;
  uint64_t startupCollisionWaitMs = 0;
  std::string lastError;
  // This Server announced its exit; its member value says so once
  // `drainAnnounce` is "announced". No new epoch is registered while draining.
  bool draining = false;
  std::string drainAnnounce = "none";
  uint64_t drainAnnouncedAtUnixMs = 0, drainAnnounceAttempts = 0;
  // The removal of this epoch's objects at exit: "none", "removed", "no-lease" (none held; the file membership's answer),
  // "skipped: ..." (another process holds the id) or "failed: ...".
  std::string revokeStatus = "none";
  uint64_t revokedAtUnixMs = 0;
  uint64_t memberChanges = 0;
  std::map<std::string, Member> members;
  // Recovery may need a process that is no longer a live member. This is an address
  // cache, never an authority for placement or a proof that a missing process died.
  std::map<std::string, Member> recoveryMembers;
  bool recoveryHistoryIncomplete = false;
  void rememberForRecovery(const Member& member);
  // Member keys whose value cannot be read (skipped: the other members are applied as usual), and how many
  // such values were skipped since this process started.
  std::set<std::string> invalidMembers;
  uint64_t skippedMemberValues = 0;
  // objectStore: what this membership's object store client met (ObjectStoreStats), as of its last registration,
  // heartbeat or listing.
  std::string storeClient;

  // ---- objectStore (the json() of the other backends leaves these out) ----
  std::string storeEndpoint, bucket, instance;
  uint32_t heartbeatIntervalMs = 0, listIntervalMs = 0, livenessGraceMs = 0, clockSkewToleranceMs = 0, createSettleMs = 0;
  uint64_t heartbeatsWritten = 0, heartbeatFailures = 0, lists = 0, listFailures = 0, addressReads = 0;
  uint64_t lastHeartbeatAtUnixMs = 0, lastListAtUnixMs = 0, lastListMembers = 0, ignoredKeys = 0;
  // The Date of the store's answer to the last listing that applied (the store's clock): what the member table knows is
  // as of then.
  uint64_t lastListDateUnixMs = 0;
  // This node's durable processes, as its heartbeat announces them.
  uint32_t durableProcesses = 1;
  // Past the TTL without a heartbeat written, by this node's monotonic clock: the others drop it, and it places on
  // itself only when no other member takes a call (the calls addressed to an epoch it held are still served), until it
  // registers a new epoch.
  bool selfExpired = false;
  uint64_t selfExpiries = 0, selfExpiredAtUnixMs = 0;
  // The lease (LeaseTracker): the clock it is judged by ("boottime" on Linux, else "monotonic", for development), why
  // the current epoch was fenced, and the heartbeat successes that came too late to renew it.
  std::string leaseClock, leaseFenceReason;
  uint64_t leaseFences = 0, lateHeartbeats = 0;
  // The recovery anchor: once this node could not list the store for longer than the TTL, the members it had whose
  // heartbeat objects are still listed stay in its table until recoveredUntilUnixMs (the store's clock: the recovery
  // + TTL + grace) unless a newer epoch of theirs appears. recoveries counts the anchors, keptMembers the members the
  // last listing kept that way.
  uint64_t recoveries = 0, recoveredUntilUnixMs = 0, keptMembers = 0;
  // The clock check: the store's clock minus this machine's (the middle of the range a Date header allows) and when.
  int64_t clockSkewMs = 0, clockSkewMinMs = 0, clockSkewMaxMs = 0;
  uint64_t clockCheckedAtUnixMs = 0, clockChecks = 0, clockWarnings = 0;
  // Registration: attempts, registrations whose read-back was another process's (lost), the waits for an identity
  // still in use.
  uint64_t registrationAttempts = 0, registrationsLost = 0, identityWaits = 0;
  // Another process holds this node's id (a newer epoch of it, or another process's bytes in this epoch's heartbeat):
  // this one takes no new work, drains and exits with 3.
  bool identityTaken = false;
  std::string identityTakenReason;
  uint64_t ownHeartbeatChecks = 0, ownHeartbeatMissing = 0;
  // Listings that did not hold this node's own heartbeat although it had written one within the TTL: not a listing
  // of the members, so nobody was dropped by it (v61, requirement 5c).
  uint64_t incompleteListings = 0;
  uint64_t oldObjectsRemoved = 0;
  // This node's suspects object (members/<nodeId>/<epoch>.suspects): writes, removals and whether its current epoch has
  // one; the other members' objects read (each when its ETag changed) and how many GETs that took.
  uint64_t suspectsWritten = 0, suspectsRemoved = 0, suspectsWriteFailures = 0, suspectsReads = 0, suspectsReadFailures = 0;
  bool suspectsObject = false;
  // The node's create-if-absent measurement at startup: "pending", "true", "false" or "failed: ...", and its record.
  std::string conditionalPutAtomic = "pending";
  std::string conditionalPutProbe = "null";

  std::string json() const;
  // objectStore: the state endpoint's "storage" section (the store, its create-if-absent measurement and the clock).
  std::string storageJson() const;
};

class Membership {
public:
  virtual ~Membership() noexcept(false) = default;
  virtual const MembershipView& view() const = 0;
  virtual kj::Promise<void> start() = 0;
  virtual kj::Promise<void> run() = 0;
  // Removes this epoch's heartbeat and drain marker; retains live suspicions for the next epoch. Idempotent.
  // Heartbeats and registration end here, so the removed objects never come back from this process.
  virtual kj::Promise<void> stop() = 0;
  // Marks this Server's membership draining, under its current epoch and only
  // if the epoch is still held by it. Heartbeats continue. Throws when the store did
  // not take it; the caller may retry.
  virtual kj::Promise<void> announceDraining() = 0;
  // objectStore: called once when another process turns out to hold this node's id; the Server drains and exits with 3.
  virtual void onIdentityTaken(std::function<void(std::string)> callback) {}
  // objectStore: this node's suspects object of its current epoch, members/<nodeId>/<epoch>.suspects -- written, or with
  // empty bytes removed -- under the heartbeat's rules: never after stop(), never by a process whose id another process
  // holds, never for an expired epoch (it throws then, as when the store did not take it). The other backends keep
  // nothing.
  virtual kj::Promise<void> writeSuspects(std::string bytes) { return kj::READY_NOW; }
  // objectStore: every live member's suspects object whose ETag changed since this membership last read it, once per
  // listing cycle, and at the start this node's own of its earlier epochs before they are removed. (nullptr: none.)
  using SuspectsCallback = std::function<bool(const std::string& nodeId, const std::string& epoch, const std::string& bytes)>;
  virtual void onSuspects(SuspectsCallback callback) {}
  // The live suspicions owned by this node, used for an atomic handoff between epochs and graceful shutdown.
  virtual void ownSuspects(std::function<std::string()> callback) {}
  // objectStore: the durable processes this node runs, announced in its heartbeats (set before start()).
  virtual void setDurableProcesses(uint32_t count) {}
  // The lease of `epoch` at `nowMs` on the lease clock (LeaseTracker::judge; an EXPIRED current epoch is fenced and
  // marked selfExpired). It calls nothing back, so a caller in the middle of its own work may use it. The file
  // membership has no lease: HELD.
  virtual LeaseTracker::Judgment checkLease(uint64_t epoch, double nowMs) {
    return {LeaseTracker::Verdict::HELD, 0, ""};
  }
  // objectStore: called synchronously, from the membership's own tasks, when the current epoch stops being held
  // (`lost`: it expired, or another process took the id) and whenever one of its heartbeats settles (a WAIT is decided).
  virtual void onLease(std::function<void(uint64_t epoch, bool lost)> callback) {}
};

kj::Own<Membership> newMembership(
    const MembershipConfig& config, const std::string& serverId,
    const std::string& peerListenAddress,
    const std::map<std::string, std::string>& staticPeers,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable,
    const ObjectStoreConfig& store = {});

}  // namespace coworkerd

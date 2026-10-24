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
  // Past the TTL without a heartbeat written, by this node's monotonic clock: the others drop it, and it places on
  // itself only when no other member takes a call (the calls addressed to an epoch it held are still served), until it
  // registers a new epoch.
  bool selfExpired = false;
  uint64_t selfExpiries = 0, selfExpiredAtUnixMs = 0;
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
  // Removes this epoch's objects (the other members drop it at their next listing); idempotent. Heartbeats and
  // registration end here, so the objects never come back from this process.
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
  using SuspectsCallback = std::function<void(const std::string& nodeId, const std::string& epoch, const std::string& bytes)>;
  virtual void onSuspects(SuspectsCallback callback) {}
};

kj::Own<Membership> newMembership(
    const MembershipConfig& config, const std::string& serverId,
    const std::string& peerListenAddress,
    const std::map<std::string, std::string>& staticPeers,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable,
    const ObjectStoreConfig& store = {});

}  // namespace coworkerd

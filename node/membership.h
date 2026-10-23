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
  // new work, finishes what it has and then revokes its lease. A value without the
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
  std::string leaseId;
  // Only successfully held identities from this Membership instance belong here.
  // Re-registration retains them; a new Server process starts with an empty set.
  // (objectStore: an epoch that expired by this node's own clock is never held again, so its calls are refused.)
  std::set<std::string> heldEpochs;
  std::string leaseStatus = "starting";
  uint32_t grantedTtlSeconds = 0;
  bool leaseConnected = false;
  bool watchConnected = false;
  uint64_t revision = 0;
  uint64_t fullRereads = 0;
  uint64_t compactions = 0;
  uint64_t registrations = 0;
  uint64_t keepAlives = 0;
  uint64_t startupCollisionWaitMs = 0;
  std::vector<int64_t> startupConflictTtlSequence;
  std::string lastError;
  // This Server announced its exit; its member value says so once
  // `drainAnnounce` is "announced". No new epoch is registered while draining.
  bool draining = false;
  std::string drainAnnounce = "none";
  uint64_t drainAnnouncedAtUnixMs = 0, drainAnnounceAttempts = 0;
  // The lease revocation at exit: "none", "revoked", "no-lease" or "failed: ...".
  std::string revokeStatus = "none";
  uint64_t revokedAtUnixMs = 0;
  uint64_t memberChanges = 0;
  std::map<std::string, Member> members;
  // Member keys whose value cannot be read (skipped: the other members are applied as usual), and how many
  // such values were skipped since this process started.
  std::set<std::string> invalidMembers;
  uint64_t skippedMemberValues = 0;

  // ---- objectStore (the json() of the other backends leaves these out) ----
  std::string storeEndpoint, bucket, instance;
  uint32_t heartbeatIntervalMs = 0, listIntervalMs = 0, livenessGraceMs = 0, clockSkewToleranceMs = 0, createSettleMs = 0;
  uint64_t heartbeatsWritten = 0, heartbeatFailures = 0, lists = 0, listFailures = 0, addressReads = 0;
  uint64_t lastHeartbeatAtUnixMs = 0, lastListAtUnixMs = 0, lastListMembers = 0, ignoredKeys = 0;
  // Past the TTL without a heartbeat written, by this node's monotonic clock: it takes no calls from other nodes and
  // places on itself only when no other member takes a call, until it registers a new epoch.
  bool selfExpired = false;
  uint64_t selfExpiries = 0, selfExpiredAtUnixMs = 0;
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
  // Revokes the lease (the member key disappears at once); idempotent. Renewal and
  // registration end here, so the key never comes back from this process.
  virtual kj::Promise<void> stop() = 0;
  // Marks this Server's member value draining, under its current lease and only
  // if the key is still held by it. Renewal continues. Throws when the authority did
  // not take it; the caller may retry.
  virtual kj::Promise<void> announceDraining() = 0;
  // objectStore: called once when another process turns out to hold this node's id; the Server drains and exits with 3.
  virtual void onIdentityTaken(std::function<void(std::string)> callback) {}
};

kj::Own<Membership> newMembership(
    const MembershipConfig& config, const std::string& serverId,
    const std::string& peerListenAddress,
    const std::map<std::string, std::string>& staticPeers,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable,
    const ObjectStoreConfig& store = {});

}  // namespace coworkerd

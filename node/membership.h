#pragma once

#include "config.h"

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <cstdint>
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
};

// All access takes place on the Server's KJ event loop. Request handling only
// reads this local view; it never waits for the authority.
struct MembershipView {
  std::string backend;
  std::string epoch;
  std::string leaseId;
  // Only successfully held identities from this Membership instance belong here.
  // Re-registration retains them; a new Server process starts with an empty set.
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

  std::string json() const;
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
};

kj::Own<Membership> newMembership(
    const MembershipConfig& config, const std::string& serverId,
    const std::string& peerListenAddress,
    const std::map<std::string, std::string>& staticPeers,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable);

}  // namespace coworkerd

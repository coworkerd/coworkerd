#pragma once

#include "json.h"
#include "membership.h"

#include <kj/async-io.h>

#include <map>
#include <memory>
#include <string>

namespace coworkerd {

// A suspicion (of a tenant): the Worker whose invocation was the culprit, where and when.
struct Suspect {
  std::string tenant, workerId, sourceServerId, processId, invocationId;
  uint64_t generation = 0, count = 0, recognizedAtUnixMs = 0;
  uint64_t expiresAtUnixMs = 0, durationMs = 0;
  bool synced = false;
  std::string json() const;
};

// The request path reads only this local view. Only a validated watchdog report
// may create a local suspicion; the object store's I/O runs independently of dispatch.
// A suspicion is of a tenant (control-plane-api.md section 1): any Worker of it confirmed as a culprit
// puts every Worker of the tenant into the quarantine processes until the observation period ends. Each node
// writes the suspicions it recognized as one object beside its heartbeat, members/<nodeId>/<epoch>.suspects (through its
// membership: written when they change and every 60 s while there are any, removed once none is left, and with the
// epoch's other objects when the node leaves or registers a new epoch), and adopts every live member's -- its membership
// reads them in its listing cycle, each one only when its ETag changed. An adopted suspicion ends at its own expiry
// (expiresAtUnixMs, against this machine's clock, which the membership's clock check keeps within 2 s of the store's),
// never because its object went.
class Suspects final {
public:
  // Without a membership that keeps objects (the file membership, the self-check) the suspicions stay on this node.
  Suspects(Membership* membership, std::string serverId, uint32_t baseMs, uint32_t maxMs, kj::Timer& timer);
  ~Suspects() noexcept(false);
  bool contains(const std::string& tenant) const;
  // The culprit's Worker (a full name); its tenant is the suspect.
  void confirm(const std::string& worker, const std::string& process,
      uint64_t generation, const std::string& invocation);
  kj::Promise<void> run();
  std::string json() const;
private:
  static constexpr uint32_t CHECK_MS = 250;
  static constexpr uint32_t REWRITE_MS = 60000;
  Membership* membership;
  std::string serverId;
  uint32_t baseMs, maxMs;
  kj::Timer& timer;
  std::map<std::string, Suspect> entries;
  std::map<std::string, uint64_t> counts;
  std::map<std::string, double> deadlines;
  // What this node last wrote: the object's bytes ("" when it has none) and for which epoch, and when.
  std::string writtenBody, writtenEpoch;
  double writtenAt = 0;
  uint64_t writes = 0, removals = 0, writeFailures = 0, adoptions = 0, adoptedObjects = 0, invalidObjects = 0;
  uint64_t lastWriteAtUnixMs = 0;
  std::string lastError;
  kj::Own<kj::PromiseFulfiller<void>> wake;
  bool keepsObjects() const;
  // The live suspicions this node recognized (and restored from its earlier epochs), as its object holds them; "" for none.
  std::string ownObject() const;
  void adopt(const std::string& nodeId, const std::string& epoch, const std::string& bytes);
  kj::Promise<void> maintain();
  void expire();
  void remember(Suspect record, const char* event);
};
}  // namespace coworkerd

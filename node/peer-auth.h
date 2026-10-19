#pragma once

#include "config.h"

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <map>
#include <string>
#include <string_view>
#include <vector>

// The peer listener's authentication (docs/egress-and-auth.md section 4). The cluster shares a key file
// (peerAuth.keyFile; compose's init generates it): {"keys":[{"id":"k2","secret":"<hex>"},{"id":"k1","secret":"..."}]},
// readable by its owner only. A node signs every peer connection it opens -- the Cap'n Proto CONNECT that carries all
// of its calls to that node -- with the first key, and the peer listener accepts a connection signed with either key:
//
//   x-coworkerd-peer-key        the key's id
//   x-coworkerd-peer-time       Unix milliseconds
//   x-coworkerd-peer-nonce      32 random hex digits
//   x-coworkerd-peer-target     the serverId the connection is for
//   x-coworkerd-peer-signature  HMAC-SHA256 (hex) over "coworkerd-peer-v1\n" METHOD "\n" PATH "\n" TARGET "\n" TIME "\n"
//                               NONCE "\n" SHA-256(body, hex)
//
// Refused (401, a [peer-auth] line, a count): no signature, an unknown key, another target, a time more than windowMs
// from this node's clock, a wrong signature, or a nonce already seen within the window. The file is re-read when it
// changes: rotation is adding the new key second on every node, then moving it first on every node, then removing the
// old one; a node whose signing key changes opens its next connections with the new key, and a session accepted with a
// key that left the file is closed windowMs later. Unset: nothing is signed or checked (a startup warning).
namespace coworkerd {

class PeerAuth {
public:
  PeerAuth(const PeerAuthConfig& config, std::string serverId, kj::Timer& timer);
  ~PeerAuth() noexcept(false);
  bool enabled() const { return !keys.empty(); }

  // Adds the signature headers of a request from this node to `targetServerId`.
  void sign(kj::HttpHeaders& headers, kj::StringPtr method, kj::StringPtr path, const std::string& targetServerId,
      std::string_view body = {});
  struct Verdict {
    std::string refusal;  // empty: accepted
    std::string keyId;
    std::string keyTag;   // accepted: the key's id and a hash of its secret (a re-issued id is another key)
  };
  Verdict verify(const kj::HttpHeaders& headers, kj::StringPtr method, kj::StringPtr path, std::string_view body = {});
  // Logs and counts a refused request.
  void refused(const char* listener, kj::StringPtr method, const Verdict& verdict);
  // Resolves windowMs after the key `keyTag` (Verdict::keyTag) left the key file (never, while it stays).
  kj::Promise<void> retired(const std::string& keyTag);
  void sessionRetired(const std::string& keyTag);
  // Changes whenever the signing key does: connections opened with an older one are not reused.
  uint64_t signingGeneration() const { return generation; }

  // Re-reads the key file when it changes; retires removed keys.
  kj::Promise<void> run();
  std::string json() const;

private:
  struct Key {
    std::string id;
    std::string secret;  // the bytes
    std::string tag() const;
  };
  struct Retirement {
    double at = 0;  // monotonic ms; 0 while the key is accepted
    kj::Own<kj::PromiseFulfiller<void>> fulfiller;
    kj::ForkedPromise<void> promise = nullptr;
  };
  std::vector<Key> load() const;
  void apply(std::vector<Key> next);
  std::string fileSignature() const;

  PeerAuthConfig config;
  std::string serverId;
  kj::Timer& timer;
  std::vector<Key> keys;
  std::string signature;  // of the key file as last read
  uint64_t generation = 1, reloads = 0, reloadFailures = 0, signedRequests = 0, retiredSessions = 0;
  std::string lastReloadError;
  std::map<std::string, double> nonces;  // nonce -> when it can be forgotten (ms)
  std::map<std::string, uint64_t> accepted, refusedByReason;
  std::map<std::string, Retirement> retirements;
};

}  // namespace coworkerd

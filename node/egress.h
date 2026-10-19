#pragma once

#include "config.h"

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <functional>
#include <optional>
#include <string>
#include <vector>

// Tenant Workers' outbound requests (docs/egress-and-auth.md). Every tenant Worker's global outbound is the
// runtime's Egress entrypoint (runtime/runtime.js), bound to that Worker's identity when the loader loads it; it hands
// the request to the external service "egress" of the workerd configuration, <stack>/run/egress.sock, in the proxy
// form (the request line carries the whole URL), with the header
//
//   x-coworkerd-egress-source: <the process's egress token> {"workerId":...,"versionId":...}
//
// workerId is the Worker's full name, <tenant>/<worker>; its tenant is the one the limits, the rules and the
// metering are kept for.
// The token is the one this node gave that workerd process (its configuration's EGRESS_TOKEN): only the runtime holds
// it, no tenant code can read it, so a request without a live process's token is not from a runtime and is refused
// (rule unknown-source). The service then, per request: the tenant's limits (in flight, per second); the rules on the
// name; the node's own resolution of the name, every address checked and the connection pinned to the checked
// addresses; TLS by the node (the system's roots and egress.caFile); the bytes of both bodies metered for the tenant.
// A refusal is answered with the header x-coworkerd-egress-error ("EGRESS_DENIED: <rule>" or "EGRESS_FAILED: <class>"),
// which the runtime throws to the tenant as an Error; a destination's own header of that name never reaches it.
namespace coworkerd {

// The process an egress token belongs to.
struct EgressSource {
  std::string processId;
  uint64_t generation = 0;
};

class EgressService final: public kj::HttpService {
public:
  // `clusterEndpoints`: the cluster's own HOST[:PORT]s this node knows (members, etcd, the object store, its own
  // listeners), asked at every refresh. `source`: the process holding an egress token, if any. `metered`: each part of
  // either body as it passes, for the tenant's usage (TenantUsage::egress).
  EgressService(const EgressConfig& config, std::string serverId, kj::Network& network, kj::Timer& timer,
      const kj::HttpHeaderTable& headerTable, std::function<std::vector<std::string>()> clusterEndpoints,
      std::function<std::optional<EgressSource>(const std::string&)> source,
      std::function<void(const std::string& tenant, uint64_t bytes)> metered = {});
  ~EgressService() noexcept(false);

  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody, Response& response) override;

  // Refreshes the cluster's addresses (resolving their names) every few seconds; the first refresh completes before
  // any request is checked.
  kj::Promise<void> run();
  // The state endpoint's `egress` section.
  std::string json() const;
  // The metered bytes of a tenant on this node since it started (both bodies, both directions); `metered` gets the same
  // bytes for the hourly usage rows (egressBytes).
  uint64_t egressBytes(const std::string& tenant) const;

  // Parses every rule of the configuration; throws naming the first bad one (the node's start, `coworkerd check`).
  static void check(const EgressConfig& config);

  struct Impl;

private:
  kj::Own<Impl> impl;
};

}  // namespace coworkerd

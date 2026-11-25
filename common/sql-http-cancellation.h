#pragma once

#include <kj/async-io.h>
#include <kj/compat/http.h>
#include <kj/debug.h>

#include <cstdint>
#include <string_view>

namespace coworkerd {

inline constexpr uint32_t SQL_DEADLINE_MS = 30000;
inline constexpr auto SQL_DEADLINE_REASON = "SQL_LIMIT_EXCEEDED: wall limit 30000ms exceeded";
inline constexpr auto SQL_RESET_REASON = "the database reset while the SQL request was queued";
inline constexpr auto SQL_REMAINING_HEADER = "X-Coworkerd-Sql-Remaining-Ms";

// A SQL caller can disconnect while the server is waiting for the query.
// A read-side EOF is also how a caller half-closes while waiting for its reply.
class SqlHttpCancellation final : public kj::HttpService {
public:
  enum class Route { CONTROL_PLANE, PEER };

  SqlHttpCancellation(kj::HttpService& service, kj::AsyncIoStream& connection, Route route)
      : service(service), connection(connection), route(route) {}

  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body, Response& response) override {
    std::string_view path(url.begin(), url.size());
    path = path.substr(0, path.find('?'));
    const bool sql = path.ends_with("/query") && (route == Route::PEER
        ? path.starts_with("/__coworkerd/databases/")
        : path.starts_with("/v1/tenants/") && path.find("/databases/") != std::string_view::npos);
    auto work = service.request(method, url, headers, body, response);
    if (method == kj::HttpMethod::POST && sql) {
      return work.exclusiveJoin(connection.whenWriteDisconnected().then([] {
        kj::throwRecoverableException(KJ_EXCEPTION(DISCONNECTED, "SQL HTTP client disconnected"));
      }));
    }
    return work;
  }

  kj::Promise<void> connect(kj::StringPtr host, const kj::HttpHeaders& headers, kj::AsyncIoStream& stream,
      ConnectResponse& response, kj::HttpConnectSettings settings) override {
    return service.connect(host, headers, stream, response, kj::mv(settings));
  }

private:
  kj::HttpService& service;
  kj::AsyncIoStream& connection;
  Route route;
};

}  // namespace coworkerd

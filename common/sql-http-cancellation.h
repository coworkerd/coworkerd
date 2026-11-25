#pragma once

#include <kj/async-io.h>
#include <kj/compat/http.h>
#include <kj/debug.h>

#include <cerrno>
#include <cstdint>
#include <poll.h>
#include <string_view>
#include <sys/socket.h>

namespace coworkerd {

inline constexpr uint32_t SQL_DEADLINE_MS = 30000;
inline constexpr auto SQL_DEADLINE_REASON = "SQL_LIMIT_EXCEEDED: wall limit 30000ms exceeded";
inline constexpr auto SQL_REMAINING_HEADER = "X-Coworkerd-Sql-Remaining-Ms";

// A peer can finish uploading a SQL request and disconnect while the server
// waits for the query. Watch incoming EOF so dropping the SQL handler also
// releases its downstream connection.
class SqlHttpCancellation final : public kj::HttpService {
public:
  enum class Route { CONTROL_PLANE, PEER, WORKER };

  SqlHttpCancellation(kj::HttpService& service, kj::AsyncIoStream& connection, kj::Timer& timer, Route route)
      : service(service), connection(connection), timer(timer), route(route) {}

  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body, Response& response) override {
    std::string_view path(url.begin(), url.size());
    path = path.substr(0, path.find('?'));
    const bool sql = path.ends_with("/query") && (route == Route::PEER
        ? path.starts_with("/__coworkerd/databases/")
        : path.starts_with("/v1/tenants/") && path.find("/databases/") != std::string_view::npos);
    auto work = service.request(method, url, headers, body, response);
    // A Worker's pending fetch can be waiting on a D1 Binding. Leave upgrades
    // and management requests with their existing connection lifetimes.
    const bool worker = route == Route::WORKER && !path.starts_with("/__coworkerd/") && !headers.isWebSocket();
    if (worker || (method == kj::HttpMethod::POST && sql)) {
      KJ_IF_SOME(fd, connection.getFd()) {
        return work.exclusiveJoin(peerClosed(fd, worker ? 100 : 25));
      }
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
  kj::Timer& timer;
  Route route;

  kj::Promise<void> peerClosed(int fd, uint32_t intervalMs) {
    for (;;) {
      co_await timer.afterDelay(intervalMs * kj::MILLISECONDS);
#if defined(POLLRDHUP)
      pollfd socket{fd, POLLRDHUP, 0};
      const bool disconnected = poll(&socket, 1, 0) > 0 &&
          (socket.revents & (POLLRDHUP | POLLHUP | POLLERR | POLLNVAL)) != 0;
#else
      char byte;
      const auto result = recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
      const bool disconnected = result == 0 ||
          (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR);
#endif
      if (disconnected) kj::throwRecoverableException(KJ_EXCEPTION(DISCONNECTED, "HTTP request client disconnected"));
    }
  }
};

}  // namespace coworkerd

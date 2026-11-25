#pragma once

#include <kj/async-io.h>
#include <kj/debug.h>

namespace coworkerd {

inline constexpr auto PEER_KEEPALIVE_INTERVAL = 2 * kj::SECONDS;
inline constexpr auto PEER_KEEPALIVE_TIMEOUT = 5 * kj::SECONDS;

// One watchdog for the accepted session. A ping received after expiry cannot revive it.
class PeerReceiveLiveness {
public:
  explicit PeerReceiveLiveness(kj::Timer& timer)
      : timer(timer), deadline(timer.now() + PEER_KEEPALIVE_TIMEOUT) {}

  void receivedPing() {
    const auto now = timer.now();
    KJ_REQUIRE(now < deadline, "peer session already expired");
    deadline = now + PEER_KEEPALIVE_TIMEOUT;
  }

  kj::Promise<void> whenExpired() {
    while (timer.now() < deadline) co_await timer.atTime(deadline);
  }

private:
  kj::Timer& timer;
  kj::TimePoint deadline;
};

// The caller owns one probe at a time and times it against the last acknowledgement.
inline kj::Promise<void> runPeerKeepalive(kj::Timer& timer, kj::Function<kj::Promise<void>()> ping) {
  auto acknowledgedAt = timer.now();
  for (;;) {
    co_await timer.atTime(acknowledgedAt + PEER_KEEPALIVE_INTERVAL);
    const auto deadline = acknowledgedAt + PEER_KEEPALIVE_TIMEOUT;
    if (timer.now() >= deadline) break;
    co_await timer.timeoutAt(deadline, ping());
    if (timer.now() >= deadline) break;
    acknowledgedAt = timer.now();
  }
  throw KJ_EXCEPTION(DISCONNECTED, "peer session keepalive expired");
}

}  // namespace coworkerd

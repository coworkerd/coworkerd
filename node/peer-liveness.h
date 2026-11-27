#pragma once

#include <kj/async-io.h>
#include <kj/debug.h>

namespace coworkerd {

inline constexpr auto PEER_KEEPALIVE_INTERVAL = 2 * kj::SECONDS;
inline constexpr auto PEER_KEEPALIVE_TIMEOUT = 5 * kj::SECONDS;

// One clock per node. Liveness deadlines count time that the local loop had a
// chance to service I/O. A missed sampling interval grants the same time back
// to all sessions, including those whose timers wake before the sampling task.
class PeerLivenessClock {
public:
  explicit PeerLivenessClock(kj::Timer& timer)
      : timer(timer), observedAt(timer.now()), sampling(sample().eagerlyEvaluate(nullptr)) {}

  kj::TimePoint now() {
    const auto wall = timer.now();
    const auto elapsed = wall - observedAt;
    if (elapsed > 2 * SAMPLE_INTERVAL) paused += elapsed - SAMPLE_INTERVAL;
    observedAt = wall;
    return wall - paused;
  }

  kj::Promise<void> processReadyIo(kj::TimePoint deadline) {
    // Give queued RPC progress a turn without letting unrelated traffic hold
    // an expired session open indefinitely.
    return kj::yieldUntilWouldSleep().exclusiveJoin(atTime(deadline + 50 * kj::MILLISECONDS));
  }

  kj::Promise<void> atTime(kj::TimePoint deadline) {
    for (;;) {
      const auto remaining = deadline - now();
      if (remaining <= 0 * kj::MILLISECONDS) co_return;
      co_await timer.afterDelay(remaining);
    }
  }

private:
  static constexpr auto SAMPLE_INTERVAL = 250 * kj::MILLISECONDS;
  kj::Promise<void> sample() {
    for (;;) {
      co_await timer.afterDelay(SAMPLE_INTERVAL);
      now();
    }
  }

  kj::Timer& timer;
  kj::TimePoint observedAt;
  kj::Duration paused = 0 * kj::MILLISECONDS;
  kj::Promise<void> sampling;
};

// One watchdog for the accepted session. Pings already queued by the socket
// count before we decide whether to close it.
class PeerReceiveLiveness {
public:
  explicit PeerReceiveLiveness(PeerLivenessClock& clock)
      : clock(clock), deadline(clock.now() + PEER_KEEPALIVE_TIMEOUT) {}

  void receivedPing() {
    KJ_REQUIRE(!expired, "peer session already expired");
    deadline = clock.now() + PEER_KEEPALIVE_TIMEOUT;
  }

  kj::Promise<void> whenExpired() {
    do {
      co_await clock.atTime(deadline);
      co_await clock.processReadyIo(deadline);
    } while (clock.now() < deadline);
    expired = true;
  }

private:
  PeerLivenessClock& clock;
  kj::TimePoint deadline;
  bool expired = false;
};

inline kj::Promise<void> peerAcknowledgementTimeout(PeerLivenessClock& clock, kj::TimePoint deadline) {
  co_await clock.atTime(deadline);
  co_await clock.processReadyIo(deadline);
  throw KJ_EXCEPTION(DISCONNECTED, "peer session keepalive expired");
}

// The caller owns one probe at a time and times it against the last acknowledgement.
inline kj::Promise<void> runPeerKeepalive(PeerLivenessClock& clock, kj::Function<kj::Promise<void>()> ping) {
  auto acknowledgedAt = clock.now();
  for (;;) {
    co_await clock.atTime(acknowledgedAt + PEER_KEEPALIVE_INTERVAL);
    co_await ping().exclusiveJoin(peerAcknowledgementTimeout(clock, acknowledgedAt + PEER_KEEPALIVE_TIMEOUT));
    acknowledgedAt = clock.now();
  }
}

}  // namespace coworkerd

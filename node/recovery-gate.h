#pragma once

#include <kj/async-io.h>
#include <kj/refcount.h>
#include <algorithm>
#include <cmath>
#include <list>
#include <set>
#include <string>
#include <string_view>

namespace coworkerd {

// Invalidation changes eligibility for reuse. Existing users keep their
// references to the transport until their requests finish.
template <typename Transport>
struct PeerConnectionReuse {
  std::string memberIdentity;
  kj::Maybe<kj::Rc<Transport>> connection;
  uint64_t generation = 0;
  bool stale = false;

  bool markStale(std::string_view identity, uint64_t observedGeneration) {
    if (memberIdentity != identity || generation != observedGeneration) return false;
    stale = true;
    return true;
  }
};

// FIFO within each class. Shared coordination can make progress during a bulk
// read backlog, and the two classes alternate when both can run. A busy
// destination is skipped until its sole permit returns.
class RecoveryGate: private kj::TaskSet::ErrorHandler {
public:
  class Permit {
  public:
    Permit(RecoveryGate& gate, std::string target): gate(gate), target(std::move(target)) {}
    ~Permit() noexcept(false) { gate.release(target); }
    Permit(const Permit&) = delete;
    Permit& operator=(const Permit&) = delete;
  private:
    RecoveryGate& gate;
    const std::string target;
  };

  RecoveryGate(kj::Timer& timer, unsigned limit, unsigned rate = 0, unsigned burst = 0)
      : timer(timer), limit(limit), rate(rate), burst(burst), tokens(burst), updatedAt(timer.now()) {}

  ~RecoveryGate() noexcept(false) { stopping = true; }

  kj::Promise<kj::Own<Permit>> acquire(std::string target = {}) {
    return enqueue(waiters, std::move(target));
  }

  // Discovery and the heartbeat check unblock multiple already waiting
  // recoveries. They use the same tokens and in-flight limit as epoch reads.
  kj::Promise<kj::Own<Permit>> acquireCoordination() {
    return enqueue(coordination, {});
  }

  unsigned active() const { return running; }
  unsigned peakActive() const { return peak; }
  uint64_t started() const { return starts; }

private:
  struct Waiter {
    std::string target;
    kj::Own<kj::PromiseFulfiller<kj::Own<Permit>>> ready;
  };

  kj::Promise<kj::Own<Permit>> enqueue(std::list<Waiter>& queue, std::string target) {
    auto pending = kj::newPromiseAndFulfiller<kj::Own<Permit>>();
    queue.push_back({std::move(target), kj::mv(pending.fulfiller)});
    pump();
    return kj::mv(pending.promise);
  }

  void release(const std::string& target) {
    --running;
    if (!target.empty()) destinations.erase(target);
    if (!stopping) pump();
  }

  void pump() {
    if (running == limit) return;
    if (rate) {
      const auto now = timer.now();
      tokens = std::min(double(burst), tokens + rate * double((now - updatedAt) / kj::MICROSECONDS) / 1000000);
      updatedAt = now;
    }
    const auto candidate = [&](std::list<Waiter>& queue) {
      for (auto it = queue.begin(); it != queue.end();) {
        if (!it->ready->isWaiting()) it = queue.erase(it);
        else if (it->target.empty() || !destinations.contains(it->target)) return it;
        else ++it;
      }
      return queue.end();
    };
    while (running < limit) {
      auto* queue = preferCoordination ? &coordination : &waiters;
      auto it = candidate(*queue);
      if (it == queue->end()) {
        queue = preferCoordination ? &waiters : &coordination;
        it = candidate(*queue);
        if (it == queue->end()) return;
      }
      if (rate && tokens < 1) {
        if (!wakePending) {
          wakePending = true;
          const auto delay = int64_t(std::ceil((1 - tokens) * 1000000 / rate));
          wake.add(timer.afterDelay(delay * kj::MICROSECONDS).then([this]() {
            wakePending = false;
            pump();
          }));
        }
        return;
      }
      if (rate) --tokens;
      auto waiter = std::move(*it);
      queue->erase(it);
      preferCoordination = queue == &waiters;
      if (!waiter.target.empty()) destinations.insert(waiter.target);
      ++starts;
      peak = std::max(peak, ++running);
      waiter.ready->fulfill(kj::heap<Permit>(*this, std::move(waiter.target)));
    }
  }
  void taskFailed(kj::Exception&& error) override { kj::throwRecoverableException(kj::mv(error)); }

  kj::Timer& timer;
  const unsigned limit, rate, burst;
  unsigned running = 0, peak = 0;
  uint64_t starts = 0;
  double tokens;
  kj::TimePoint updatedAt;
  bool wakePending = false, stopping = false, preferCoordination = true;
  std::set<std::string> destinations;
  std::list<Waiter> waiters, coordination;
  kj::TaskSet wake{*this};
};

}  // namespace coworkerd

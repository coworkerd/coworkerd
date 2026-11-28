#pragma once

#include <kj/async-io.h>
#include <limits>

namespace coworkerd {

inline constexpr auto REQUEST_DRAIN_LIMIT = 30 * kj::SECONDS;

class RequestLease {
public:
  explicit RequestLease(kj::Timer& timer): timer(timer) {}
  KJ_DISALLOW_COPY_AND_MOVE(RequestLease);

  bool isClosed() const { return closed; }
  double remainingMs() const {
    if (closed) return 0;
    KJ_IF_SOME(at, deadline) {
      return kj::max(0.0, double((at - timer.now()) / kj::NANOSECONDS) / 1'000'000);
    }
    return std::numeric_limits<double>::infinity();
  }

  kj::Promise<void> whenClosing() { return closing.addBranch(); }
  kj::Promise<void> whenClosed() { return ended.addBranch(); }
  void watchCaller(kj::Promise<void> watch) { caller = kj::mv(watch).eagerlyEvaluate(nullptr); }

  // Only the first transition sets the drain deadline. A repeated notification
  // or a descendant beginning its own cleanup cannot buy another interval.
  void beginDrain(kj::Duration remaining = REQUEST_DRAIN_LIMIT) {
    if (closed || deadline != kj::none) return;
    if (remaining <= 0 * kj::MILLISECONDS) {
      close();
      return;
    }
    setLimit(kj::min(remaining, REQUEST_DRAIN_LIMIT));
    draining = true;
    closingSignal.fulfiller->fulfill();
  }

  void startFixed(kj::Duration limit) { setLimit(limit); }

  void close() {
    if (closed) return;
    closed = true;
    if (!draining) closingSignal.fulfiller->fulfill();
    endSignal.fulfiller->fulfill();
  }

private:
  void setLimit(kj::Duration limit) {
    KJ_REQUIRE(!closed && deadline == kj::none, "lease already has a deadline");
    const auto stopAt = timer.now() + limit;
    deadline = stopAt;
    expire = timer.atTime(stopAt).then([this]() { close(); }).eagerlyEvaluate(nullptr);
  }

  kj::Timer& timer;
  kj::Maybe<kj::TimePoint> deadline;
  bool closed = false;
  bool draining = false;
  kj::PromiseFulfillerPair<void> closingSignal = kj::newPromiseAndFulfiller<void>();
  kj::ForkedPromise<void> closing = closingSignal.promise.fork();
  kj::PromiseFulfillerPair<void> endSignal = kj::newPromiseAndFulfiller<void>();
  kj::ForkedPromise<void> ended = endSignal.promise.fork();
  kj::Promise<void> expire = kj::READY_NOW;
  kj::Promise<void> caller = kj::READY_NOW;
};

}  // namespace coworkerd

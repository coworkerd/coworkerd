#pragma once

#include <kj/async.h>
#include <kj/timer.h>
#include <algorithm>
#include <cstdint>

namespace coworkerd {

// Only node metadata and a timer live here: no process ticket, native call
// thread, Worker preparation or memory reservation is acquired while waiting.
class TenantExecutionQueue {
public:
  enum Result { READY, EXPIRED, FULL };
  uint32_t active() const { return waiting; }

  template<class Limited>
  kj::Promise<Result> wait(kj::Timer& timer, kj::Duration allowance, uint32_t maximum, Limited limited) {
    if (waiting >= maximum) co_return FULL;
    ++waiting;
    KJ_DEFER(--waiting);
    const auto until = timer.now() + allowance;
    for (;;) {
      if (timer.now() > until) co_return EXPIRED;
      if (!limited()) co_return READY;
      const auto remaining = until - timer.now();
      if (remaining <= 0 * kj::MILLISECONDS) co_return EXPIRED;
      co_await timer.afterDelay(kj::min(remaining, 5 * kj::MILLISECONDS));
    }
  }
private:
  uint32_t waiting = 0;
};

}  // namespace coworkerd

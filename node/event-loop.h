#pragma once

#include <kj/async.h>

namespace coworkerd {

// A connection burst can keep the continuation queue runnable for a long time.
// Poll ready I/O between small batches so established connections make progress.
// Polling after almost every continuation also reads the kernel clock and is
// expensive on hosts whose monotonic clock cannot use the vDSO fast path.
inline void configureNodeEventLoop(kj::WaitScope& scope) {
  scope.setBusyPollInterval(7);
}

}  // namespace coworkerd

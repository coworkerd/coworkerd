#pragma once

#include <kj/async.h>

namespace coworkerd {

// A connection burst can keep the continuation queue runnable for a long time.
// Poll ready I/O between small batches so established connections make progress.
inline void configureNodeEventLoop(kj::WaitScope& scope) {
  scope.setBusyPollInterval(1);
}

}  // namespace coworkerd

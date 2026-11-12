#pragma once

#include "config.h"
#include <kj/async.h>
#include <kj/timer.h>
#include <functional>
#include <memory>
#include <string>

namespace coworkerd {

// First-touch diagnostics only: never changes admission, limits, or the host's settings.
int runHostCheck(int argc, char** argv);
int runHostCheckChild(int argc, char** argv);

class HostCheck final {
public:
  HostCheck(HostCheckConfig config, std::function<std::string()> memoryBand);
  ~HostCheck();
  kj::Promise<void> run(kj::Timer& timer);
  std::string json() const;
  // memory.events has no victim pid. An increment while this helper could have been
  // its victim must not become evidence against a workerd process. Called before
  // every pool read, including reads at workerd exit, on the node's event loop.
  bool takeOomExposure();
private:
  class Impl;
  std::unique_ptr<Impl> impl;
};

}  // namespace coworkerd

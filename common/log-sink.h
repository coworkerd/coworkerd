#pragma once

#include <kj/async-io.h>

#include <cstdint>
#include <memory>
#include <string>

// This process's own standard output and error through a shared-memory ring and a writer process, as
// both daemons write them (async-log.h keeps the workerd output).
namespace coworkerd {

// Install before the event loop or any workerd is created. Only serve mode uses
// this sink; deployment-tool stdout remains its synchronous command result.
class AsyncLog {
public:
  AsyncLog();
  ~AsyncLog();
  AsyncLog(const AsyncLog&) = delete;
  AsyncLog& operator=(const AsyncLog&) = delete;

  static std::string stateJson();
  static void updateClock();

private:
  class Impl;
  static Impl* active;
  std::unique_ptr<Impl> impl;
};

// Keeps the sink's clock current (every 10 ms) while the event loop runs.
kj::Promise<void> runLogClock(kj::Timer& timer);

}  // namespace coworkerd

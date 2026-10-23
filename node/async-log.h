#pragma once

#include "config.h"

#include <kj/async-io.h>

#include <cstdint>
#include <memory>
#include <string>

namespace coworkerd {

// Drains the stdout/stderr pipes of every workerd process into one sink file
// from a separate process, so a slow sink never blocks a workerd event loop.
// Install after AsyncLog and before the event loop; destroy after every workerd
// has been killed so the drainer can read each pipe to its end.
//
// Tenant lines: the node hands the drainer each Worker's console lines as that process's runtime sent them over the
// source socket, from the tail channel workerd gives each loaded Worker under the Worker's own identity
// (runtime/runtime.js, TenantLog). The drainer keeps them in a per-tenant buffer: at most linesPerSecond lines per
// tenant and second (the rest dropped and counted in one [dropped] entry), lines cut at maxLineBytes; a tenant's own
// quota replaces both limits. Nothing a workerd process writes on its standard output or error counts as a Worker's
// line, whatever it holds.
class WorkerdOutput {
public:
  explicit WorkerdOutput(std::string sinkPath, WorkerLogSettings settings = {});
  ~WorkerdOutput();
  WorkerdOutput(const WorkerdOutput&) = delete;
  WorkerdOutput& operator=(const WorkerdOutput&) = delete;

  struct Pipes {
    int stdoutRead = -1, stdoutWrite = -1, stderrRead = -1, stderrWrite = -1;
    Pipes() = default;
    Pipes(const Pipes&) = delete;
    Pipes& operator=(const Pipes&) = delete;
    ~Pipes();
  };
  // False when no drainer is installed: the caller keeps its direct file.
  static bool open(Pipes& pipes);
  // Called once the child holds the write ends. Never blocks the caller.
  static void attach(Pipes& pipes, uint32_t slot, const std::string& processId, bool quarantine,
      uint64_t generation, int64_t pid);
  static std::string processJson(uint32_t slot);
  static std::string stateJson();
  // One Worker's kept lines, as the drainer answers them (JSON): those after the cursor `after` (oldest
  // first) or else the newest `limit` from `sinceUnixMs` on. On the event loop; never blocks it.
  static kj::Promise<std::string> workerLogs(kj::LowLevelAsyncIoProvider& provider, const std::string& workerId,
      uint64_t after, uint64_t sinceUnixMs, uint32_t limit);
  // A tenant's own log quota (0 and 0: the configured defaults again), and each tenant's kept and dropped lines
  // since the drainer started ({"epoch","tenants":{"<t>":{"keptLines","droppedLines","bufferBytes"}}}).
  static kj::Promise<std::string> tenantQuota(kj::LowLevelAsyncIoProvider& provider, const std::string& tenant,
      uint64_t linesPerSecond, uint64_t bufferBytes);
  static kj::Promise<std::string> tenantLines(kj::LowLevelAsyncIoProvider& provider);
  // One Worker's lines from one process: `lines` holds one {"workerId","versionId","level","message"} object per line
  // (fields in this order, the message JSON-escaped), each followed by '\n'; answers {"ok","accepted","refused"}.
  static kj::Promise<std::string> appendTenantLines(kj::LowLevelAsyncIoProvider& provider, const std::string& processId,
      const std::string& lines);

private:
  class Impl;
  static Impl* active;
  std::unique_ptr<Impl> impl;
};

}  // namespace coworkerd

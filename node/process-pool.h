#pragma once

#include "coworkerd.capnp.h"
#include "config.h"
#include "suspects.h"
#include "tenancy.h"
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/compat/http.h>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace coworkerd {

struct OpenEvent {
  v1::rpc::EventDispatcher::Client dispatcher;
  kj::Promise<void> ready;
};

struct ProcessGeneration;
class ProcessPool;

class ProcessCall final {
public:
  ~ProcessCall();
  void finish();
  kj::Promise<void> onExit();
  bool cpuLimitExceeded() const;
  bool memoryLimitExceeded() const;
  const std::string processId;
  const uint64_t generation;
  std::string invocationId, treeId, workerId, versionId;
  double startedAtMonotonicMs = 0;
  uint64_t startedAtUnixMs = 0;
  bool dispatched = false;
private:
  friend class ProcessPool;
  explicit ProcessCall(std::shared_ptr<ProcessGeneration> process);
  std::shared_ptr<ProcessGeneration> process;
  bool finished = false;
};

// The Server and its workerd processes talk only over descriptors the Server creates. Each
// generation inherits, under fixed numbers: 1 and 2, its output pipes; 3, the write end of
// its limit event pipe; 4, its listening Unix socket <stack>/run/<slot>-<generation>.sock (probes
// and the runtime RPC); 5, shared memory for its lifecycle snapshot; 6, the memory band's shared
// memory (when the watermark is on). The runtime calls back over <stack>/run/binding.sock and
// source.sock, which `bindingAddress` and `sourceAddress` name. With the sandbox the launcher
// also gets 7 (its status socket) and 8 (the workerd file); it closes both before workerd runs. For
// parent liveness, workerd inherits only the read end at 10; only this node keeps the
// close-on-exec write end, so every process class exits on EOF after node death.
// In the observe mode of the system call allowlist the launcher sends the filter's notification listener
// back over 7, and this Server is the supervisor that answers the notifications.
class ProcessPool final {
public:
  ProcessPool(ProcessPoolConfig config, std::string executable, std::string runtimeFile,
      std::string directory, std::string bindingAddress, std::string sourceAddress,
      kj::AsyncIoProvider& provider, kj::UnixEventPort& eventPort, const kj::HttpHeaderTable& headers,
      Suspects& suspects);
  ~ProcessPool() noexcept(false);
  kj::Promise<void> run();
  bool available(const std::string& worker);
  bool quarantined(const std::string& worker) const;
  // Ready to take work: every ordinary process (a quarantine shard's state does not decide it).
  bool ready() const;
  // The quarantine shards that are not ready: [{"processId","state"}], for the state endpoint.
  std::string quarantineReadinessJson() const;
  uint32_t cpuLimitMs() const;
  uint32_t memoryLimitMiB() const;
  // The limits each Worker names: its own, else its tenant's default, 0 where this node's own values
  // apply. The node sets this once its tenant view exists. The processes apply them per isolate (the runtime
  // bridge puts them in the loader key and workerd's limits take them from there), so the pool uses them to
  // read the limit rows a process writes -- a row names the limit that was in force -- and the largest memory
  // limit to size the process-wide WebAssembly memory cap of a new generation.
  struct WorkerLimitsView {
    std::function<WorkerLimits(const std::string&)> of;
    std::function<uint32_t()> maxMemoryMiB;
  };
  void setWorkerLimits(WorkerLimitsView view);
  // The limits in force for one Worker here: what it names, else this node's own.
  WorkerLimits effectiveLimits(const std::string& workerId) const;
  bool hot(const std::string& worker, const std::string& hash, uint32_t idleMs) const;
  std::shared_ptr<ProcessCall> select(const std::string& worker);
  // A trial load's process: a quarantine process, the tenant's shard.
  std::shared_ptr<ProcessCall> selectTrial(const std::string& worker);
  // Durable Objects: the durable process of an object (by its key, <namespace id>/<object id>: the same one while the
  // durable slots are), whether it takes a call now, and a call on it.
  uint32_t durableProcesses() const;
  // A version executed on a durable process within the placement idle period (the yellow band's test for a cold one).
  bool durableHot(const std::string& hash) const;
  std::string durableProcessFor(const std::string& objectKey) const;
  bool durableAvailable(const std::string& objectKey);
  std::shared_ptr<ProcessCall> selectDurable(const std::string& objectKey);
  // The calls in flight on the durable processes (every live generation).
  uint32_t durableInFlight();
  // The node no longer owns the objects its durable processes run (its membership expired): each live durable generation
  // is killed -- what it runs ends with it, the facets of every object among it -- and a new one starts at once, empty.
  // Not a failure of the process: no backoff, no culprit. Returns how many generations were killed.
  uint32_t resetDurable(const std::string& reason);
  // The drain's close of the durable processes alone (as closeAll() closes every process), before the node lets its
  // shards go: no durable generation starts from then on. Resolves with the record once they exited.
  kj::Promise<std::string> closeDurable();
  // The Durable Object log channel (node/durable-log.h): a durable process inherits one end of a socket under
  // descriptor 9 and ships its objects' WAL frames over it. `open` gives the descriptor the child is to inherit
  // (-1: this node ships nothing), `close` is called when a generation is gone, and the settings the processes read
  // come from the same block.
  void setDurableChannel(std::function<int(const std::string&)> open, std::function<void(const std::string&)> close,
      const DurableLogConfig& settings);
  // Usage: the CPU nanoseconds of each tenant's executions since the last call (from the `completed` rows),
  // and how many isolates of each tenant are resident now (the lifecycle snapshots).
  std::map<std::string, uint64_t> takeCpuUsage();
  std::map<std::string, uint64_t> residentIsolatesByTenant() const;
  // The quarantine process a tenant's trials and (when it is a suspect) calls go to first; "" without one.
  std::string quarantineShard(const std::string& tenant) const;
  OpenEvent open(std::shared_ptr<ProcessCall> call, kj::StringPtr descriptor,
      const std::string& definitionHash);
  std::string json(uint32_t idleMs) const;
  std::string placementJson(const std::vector<std::string>& workers);
  // A rotation's switch from a generation: `callback(processId, generation)` tells that generation's WebSocket
  // connections to go away (1001) and returns how many it told; the rotation's record keeps the number.
  void setRetiring(std::function<uint32_t(const std::string&, uint64_t)> callback);
  // Queues a rotation of each named slot (all slots when empty); returns the request ids as JSON.
  // Throws for an unknown slot. Slots rotate one at a time on this machine.
  std::string requestRotation(const std::vector<std::string>& processIds, const std::string& reason);
  // The rotation state, or with request ids, how far those requests are.
  std::string rotationJson(const std::vector<uint64_t>& requestIds = {}) const;

  // Tenant Workers' outbound requests (egress.h): before run(), the node's egress service at `address`
  // (unix:<stack>/run/egress.sock). Every workerd process then gets the external service "egress" (the proxy form)
  // and its own egress token (EGRESS_TOKEN), which the runtime sends with each request and only the runtime holds.
  void enableEgress(std::string address);
  // The live generation (slot id, generation) a token belongs to, if any.
  std::optional<std::pair<std::string, uint64_t>> egressSource(const std::string& token) const;

  // Server exit. beginShutdown(): no further rotation starts (one in progress
  // finishes). drainState(): the rotation's drain criterion over every live generation, with
  // `sinceMonotonicMs` and `periodMs` standing in for the switch time and drain period
  // where workerd reports no request contexts. killBusy(): at the drain deadline, kills each
  // generation that still has calls or contexts (what is in flight is lost with it). closeAll(): the rotation's
  // close of every generation (connections, SIGTERM, SIGKILL after the grace), with no
  // new generation started from then on; resolves with the record once all exited.
  // killAll(): the emergency exit, a kill of every live generation.
  struct DrainState {
    bool idle = false, contextsVisible = true;
    uint32_t inFlight = 0;
    size_t openContexts = 0, backgroundContexts = 0, liveGenerations = 0;
    uint64_t clearedContexts = 0;
    std::string json;
  };
  void beginShutdown();
  DrainState drainState(double sinceMonotonicMs, uint32_t periodMs);
  struct Killed { uint32_t calls = 0; size_t contexts = 0; uint32_t processes = 0; };
  Killed killBusy(double sinceMonotonicMs, uint32_t periodMs, const std::string& reason);
  kj::Promise<std::string> closeAll();
  Killed killAll(const std::string& reason);
private:
  class Impl;
  std::unique_ptr<Impl> impl;
};

}  // namespace coworkerd

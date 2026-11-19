#pragma once

#include "coworkerd.capnp.h"
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>

namespace coworkerd {

// The current CPU watchdog evidence only matches a live dispatched invocation.
inline bool cpuCulpritMatches(std::string_view event, uint32_t reportedPid, uint32_t pid,
    uint32_t reportedLimit, uint32_t limit, uint64_t usedNs, bool dispatched,
    std::string_view reportedWorker, std::string_view worker,
    std::string_view reportedVersion, std::string_view version) {
  return limit && event == "culprit" && reportedPid == pid && reportedLimit == limit &&
      usedNs >= uint64_t(limit) * 1000000 && dispatched &&
      reportedWorker == worker && reportedVersion == version;
}

// Validate both active and recently completed invocations before attribution.
template <typename Process, typename Limits, typename Labelled, typename Confirmed>
std::string confirmedCpuCulprit(Process& p, Limits limits, Labelled labelled, Confirmed confirmed) {
  p.pullEvents();
  const auto row = p.culpritRow;
  if (row.empty() || row.size() > 16384) return "null";
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<v1::rpc::CpuReport>();
    capnp::JsonCodec().decode(kj::StringPtr(row.c_str()), value);
    auto call = p.calls.find(value.getInvocationId().cStr());
    // The limit this Worker's executions run under (its own where it names one): the row has to be
    // against that limit, and to have spent it.
    const std::string worker = value.getWorkerId().cStr(), version = value.getVersionId().cStr();
    const bool recent = p.threaded && std::any_of(p.recentDispatched.begin(), p.recentDispatched.end(), [&](const auto& completed) {
      return completed.workerId == worker && completed.versionId == version && completed.invocationId == value.getInvocationId().cStr();
    });
    const auto limit = call == p.calls.end() ? (recent ? limits(worker, p.durable).cpuMs : 0u) : limits(call->second->workerId, p.durable).cpuMs;
    if (!limit) return "null";
    const bool live = call != p.calls.end();
    if (!cpuCulpritMatches(value.getEvent().cStr(), value.getPid(), static_cast<uint32_t>(p.pid),
        value.getCpuLimitMs(), limit, value.getCpuUsedNs(), live ? call->second->dispatched : recent,
        worker, live ? call->second->workerId : worker, version, live ? call->second->versionId : version)) return "null";
    if (!labelled(p, value.getIsolate().cStr(), worker, version)) return "null";
    confirmed(worker);
    return capnp::JsonCodec().encode(value.asReader()).cStr();
  } catch (const kj::Exception&) { return "null"; }
}

// A native memory-culprit row carries the heap of the isolate whose allocation failed. Below that
// Worker's own limit the isolate cannot have used up its own V8 headroom (its limit plus one maximum
// object): other isolates filled the shared pointer cage, so the row names no culprit. 0: not reported.
inline bool memoryCulpritOwnsOom(uint64_t oomIsolateHeapBytes, uint32_t memoryLimitMiB) {
  return oomIsolateHeapBytes == 0 || oomIsolateHeapBytes >= (uint64_t(memoryLimitMiB) << 20);
}

// An OOM row can match a completed dispatch in the same process generation.
template<class Calls, class Recent>
bool memoryCulpritDispatch(const Calls& live, const Recent& recent, const std::string& invocation,
    const std::string& worker, const std::string& version) {
  auto current = live.find(invocation);
  if (current != live.end() && current->second->dispatched &&
      current->second->workerId == worker && current->second->versionId == version) return true;
  return std::any_of(recent.begin(), recent.end(), [&](const auto& call) {
    return call.invocationId == invocation && call.workerId == worker && call.versionId == version;
  });
}


}  // namespace coworkerd

#pragma once

#include "ingress-descriptors.h"
#include <algorithm>
#include <cstdint>

namespace coworkerd {

inline constexpr uint32_t LEGACY_CALL_SLOTS = 512;
inline constexpr uint32_t MAX_EAGER_CALL_SLOTS = 16384;

inline constexpr uint64_t remainingBudget(uint64_t budget, uint64_t used) {
  return budget - std::min(budget, used);
}

inline constexpr uint64_t poolDescriptors(uint64_t nativeGenerations, uint64_t durableProcesses,
    uint32_t ordinarySlots, uint32_t durableSlots) {
  const auto child = uint64_t(ordinarySlots) * 8 + 256;
  const auto node = 256 + nativeGenerations * (uint64_t(ordinarySlots) * 4 + 128) +
      durableProcesses * (uint64_t(durableSlots) * 4 + 128);
  return std::max(child, node);
}

struct EagerCallPlan {
  uint32_t requested, slots;
  bool taskLimited, descriptorLimited;
};

// Keep the ingress upgrade budget the previous ordinary default supported on this host.
// Tasks are the available slots after fixed daemon, durable and generation overheads.
inline constexpr EagerCallPlan planEagerCalls(uint64_t processMemory, uint64_t tasks, uint64_t descriptors,
    uint64_t nativeGenerations, uint32_t durableProcesses, uint32_t durableSlots, uint32_t webSockets) {
  const auto requested = static_cast<uint32_t>(std::clamp<uint64_t>(
      processMemory >> 20, LEGACY_CALL_SLOTS, MAX_EAGER_CALL_SLOTS));
  const auto byTasks = tasks / nativeGenerations;
  const auto legacy = poolDescriptors(nativeGenerations, durableProcesses, LEGACY_CALL_SLOTS, durableSlots);
  const auto ingress = planIngressDescriptors(0, descriptors, legacy, webSockets);
  const auto usableDescriptors = remainingBudget(descriptors, OTHER_INGRESS_DESCRIPTORS +
      uint64_t(std::max(ingress.webSockets, 1u)) * DESCRIPTORS_PER_WEBSOCKET);
  const auto child = remainingBudget(usableDescriptors, 256) / 8;
  const auto node = remainingBudget(usableDescriptors,
      256 + nativeGenerations * 128 + uint64_t(durableProcesses) * (uint64_t(durableSlots) * 4 + 128)) /
      (nativeGenerations * 4);
  const auto byDescriptors = std::min(child, node);
  const auto slots = static_cast<uint32_t>(std::min({uint64_t(requested), byTasks, byDescriptors}));
  return {requested, slots, byTasks == slots && slots < requested, byDescriptors == slots && slots < requested};
}

}  // namespace coworkerd

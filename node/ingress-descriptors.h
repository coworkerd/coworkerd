#pragma once

#include <algorithm>
#include <cstdint>

namespace coworkerd {

struct IngressDescriptorPlan {
  uint32_t requested;
  uint32_t webSockets;
  uint64_t softLimit;
};

inline constexpr uint32_t DEFAULT_INGRESS_WEBSOCKETS = 2048;
inline constexpr uint64_t OTHER_INGRESS_DESCRIPTORS = 256;
inline constexpr uint64_t DESCRIPTORS_PER_WEBSOCKET = 2;

// The inputs are effective OS limits. UINT64_MAX represents unlimited. The pool has already been validated.
inline constexpr IngressDescriptorPlan planIngressDescriptors(uint64_t soft, uint64_t hard,
    uint64_t poolDescriptors, uint32_t configured) {
  const auto requested = configured == 0 ? DEFAULT_INGRESS_WEBSOCKETS : configured;
  const auto reserve = poolDescriptors + OTHER_INGRESS_DESCRIPTORS;
  const auto available = (hard - std::min(hard, reserve)) / DESCRIPTORS_PER_WEBSOCKET;
  const auto sockets = static_cast<uint32_t>(std::min<uint64_t>(requested, available));
  const auto needed = sockets > 0 ? reserve + sockets * DESCRIPTORS_PER_WEBSOCKET : 0;
  return {requested, sockets, std::max(soft, needed)};
}

}  // namespace coworkerd

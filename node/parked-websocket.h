#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace coworkerd {

// Shared by all home deliveries on the node's event loop. Charge the received
// payload and room for its copy in HTTP-over-Cap'n Proto.
class DurableSocketFrameBudget {
public:
  class Reservation {
  public:
    Reservation(Reservation&& other) noexcept
        : budget(std::exchange(other.budget, nullptr)), bytes(other.bytes) {}
    ~Reservation() { if (budget) budget->used -= bytes; }

  private:
    friend class DurableSocketFrameBudget;
    Reservation(DurableSocketFrameBudget& budget, uint64_t bytes): budget(&budget), bytes(bytes) {}

    DurableSocketFrameBudget* budget;
    uint64_t bytes;
  };

  explicit DurableSocketFrameBudget(uint64_t limit): limit(limit) {}

  std::optional<Reservation> tryReserve(uint64_t payloadBytes) {
    if (payloadBytes > (limit - used) / 2) {
      ++refused;
      return std::nullopt;
    }
    const auto bytes = payloadBytes * 2;
    used += bytes;
    return Reservation(*this, bytes);
  }

  uint64_t limitBytes() const { return limit; }
  uint64_t usedBytes() const { return used; }
  uint64_t refusals() const { return refused; }

private:
  const uint64_t limit;
  uint64_t used = 0;
  uint64_t refused = 0;
};

// Each relay owns this state on the node's event loop. The transport continues
// to belong to the relay while the attached home and its open are replaced.
class ParkedWebSocket {
public:
  struct Owner {
    uint64_t nodeEpoch, shardEpoch;
    bool operator==(const Owner&) const = default;
  };
  struct Home {
    std::string invocationId;
    uint64_t generation;
    bool operator==(const Home&) const = default;
  };
  struct Snapshot {
    std::vector<std::string> tags;
    std::optional<std::string> attachment;
    std::optional<uint64_t> autoResponseTime;
    uint64_t lastFrame = 0;
    bool closedOutgoing = false;
  };
  struct Frame {
    enum class Action { FORWARD, ANSWER, WAKE, BACKPRESSURE, REFUSE };
    Action action;
    uint64_t position;
    std::string_view answer;
  };

  ParkedWebSocket(Owner owner, Home home): owner(owner), home(std::move(home)) {}

  bool alive(Owner expected) const { return expected == owner && !closed; }

  bool authorized(Owner expected, const Home& from) const {
    return alive(expected) && home == from;
  }

  bool canPark(Owner expected, const Home& from, uint64_t position) const {
    return authorized(expected, from) && position == lastApplicationFrame && !pendingApplication;
  }

  bool canParkOpen(Owner expected, uint64_t generation, uint64_t position) const {
    return home && home->generation == generation && canPark(expected, *home, position);
  }

  bool park(Owner expected, const Home& from, uint64_t position, Snapshot snapshot) {
    if (!canPark(expected, from, position)) return false;
    snapshot.lastFrame = position;
    if (this->snapshot.autoResponseTime > snapshot.autoResponseTime)
      snapshot.autoResponseTime = this->snapshot.autoResponseTime;
    this->snapshot = std::move(snapshot);
    home.reset();
    adopted = false;
    return true;
  }

  void parkOpen(Owner expected, uint64_t generation, Snapshot saved) {
    if (canParkOpen(expected, generation, saved.lastFrame)) {
      auto from = *home;
      const auto position = saved.lastFrame;
      park(expected, from, position, std::move(saved));
    }
  }

  const Snapshot* parked(Owner expected) const {
    return expected == owner && !home && !closed ? &snapshot : nullptr;
  }

  const Snapshot* adoptable(Owner expected, const Home& replacement) const {
    return alive(expected) && (!home || (adopted && home == replacement)) ? &snapshot : nullptr;
  }

  void adopt(Owner expected, Home replacement) {
    if (parked(expected)) {
      home = std::move(replacement);
      adopted = true;
    }
  }

  const auto& automatic() const { return automaticResponse; }
  std::optional<uint64_t> autoResponseTime() const { return snapshot.autoResponseTime; }

  bool configure(Owner expected, const Home& from,
      std::optional<std::pair<std::string, std::string>> response) {
    if (!authorized(expected, from)) return false;
    automaticResponse = std::move(response);
    return true;
  }

  Frame receive(Owner expected, std::optional<std::string_view> text) {
    using Action = Frame::Action;
    if (expected != owner || closed) return {Action::REFUSE, lastFrame, {}};
    // A relay awaits completion before reading again. Follow the same rule
    // when a caller gets here while the earlier frame is still in flight.
    if (pendingApplication || pendingAnswer) return {Action::BACKPRESSURE, lastFrame, {}};
    const auto position = ++lastFrame;
    if (text && automaticResponse && *text == automaticResponse->first) {
      pendingAnswer = position;
      return {Action::ANSWER, position, automaticResponse->second};
    }
    pendingApplication = lastApplicationFrame = position;
    return {home ? Action::FORWARD : Action::WAKE, position, {}};
  }

  bool answered(Owner expected, uint64_t position, uint64_t unixMs) {
    if (expected != owner || pendingAnswer != position || closed) return false;
    snapshot.autoResponseTime = unixMs;
    pendingAnswer.reset();
    return true;
  }

  bool complete(Owner expected, uint64_t position) {
    if (expected != owner || pendingApplication != position || closed) return false;
    pendingApplication.reset();
    return true;
  }

  void fence(Owner expected) {
    if (expected != owner) return;
    closed = true;
    home.reset();
    pendingApplication.reset();
    pendingAnswer.reset();
  }

private:
  Owner owner;
  std::optional<Home> home;
  Snapshot snapshot;
  std::optional<std::pair<std::string, std::string>> automaticResponse;
  uint64_t lastFrame = 0, lastApplicationFrame = 0;
  std::optional<uint64_t> pendingApplication, pendingAnswer;
  bool closed = false;
  bool adopted = false;
};

}  // namespace coworkerd

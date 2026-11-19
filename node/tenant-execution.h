#pragma once

#include "cpu-capacity.h"
#include <kj/async.h>
#include <kj/debug.h>
#include <kj/timer.h>
#include <cstdint>
#include <iterator>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace coworkerd {

// One process generation. Calls and waiters are owned by the node event loop.
class TenantExecutionState {
public:
  explicit TenantExecutionState(uint32_t limit): limit(limit) {}
  TenantExecutionState(const TenantExecutionState&) = delete;
  TenantExecutionState& operator=(const TenantExecutionState&) = delete;
  uint32_t count(const std::string& tenant) const {
    const auto found = tenants.find(tenant);
    return found == tenants.end() ? 0 : found->second.inFlight;
  }
  uint32_t active() const { return activeTenants; }
  uint32_t queued() const { return waiting; }
  bool limited(const std::string& tenant) const {
    return tenantExecutionLimited(count(tenant), activeTenants, limit);
  }
  void enter(const std::string& tenant) {
    auto& entry = tenants[tenant];
    if (entry.inFlight++ == 0) ++activeTenants;
  }
  void leave(const std::string& tenant) {
    auto found = tenants.find(tenant);
    KJ_REQUIRE(found != tenants.end() && found->second.inFlight > 0);
    auto& entry = found->second;
    if (--entry.inFlight == 0) --activeTenants;
    if (activeTenants <= 1) wakeAll();
    else if (entry.inFlight < limit) wakeOne(entry);
    found = tenants.find(tenant);
    if (found != tenants.end() && found->second.inFlight == 0 && found->second.waiters.empty()) tenants.erase(found);
  }
  kj::Promise<void> changed(std::string tenant) {
    auto pair = kj::newPromiseAndFulfiller<void>();
    auto& entry = tenants[tenant];
    Waiter waiter{entry, kj::mv(pair.fulfiller)};
    entry.waiters.push_back(&waiter); waiter.position = std::prev(entry.waiters.end());
    waiter.queued = true; ++waiting;
    if (entry.waiters.size() == 1) {
      waitingTenants.push_back(&entry); entry.position = std::prev(waitingTenants.end());
    }
    KJ_DEFER(remove(waiter));
    co_await pair.promise;
  }
  void wakeAll() {
    // Only queued calls are visited, when borrowing becomes possible again.
    while (!waitingTenants.empty()) wakeOne(*waitingTenants.front());
  }
private:
  struct Waiter;
  struct Tenant {
    uint32_t inFlight = 0;
    std::list<Waiter*> waiters;
    std::list<Tenant*>::iterator position;
  };
  struct Waiter {
    Tenant& owner;
    kj::Own<kj::PromiseFulfiller<void>> fulfiller;
    std::list<Waiter*>::iterator position;
    bool queued = false;
  };
  void remove(Waiter& waiter) {
    if (!waiter.queued) return;
    waiter.queued = false; --waiting;
    auto& entry = waiter.owner;
    entry.waiters.erase(waiter.position);
    if (entry.waiters.empty()) waitingTenants.erase(entry.position);
  }
  void wakeOne(Tenant& entry) {
    if (entry.waiters.empty()) return;
    auto& waiter = *entry.waiters.front();
    remove(waiter);
    waiter.fulfiller->fulfill();
  }
  uint32_t limit, activeTenants = 0, waiting = 0;
  std::unordered_map<std::string, Tenant> tenants;
  std::list<Tenant*> waitingTenants;
};

// A bounded node wait races release events against one deadline timer. It
// reserves no process ticket, native call thread or Worker preparation memory.
class TenantExecutionQueue {
public:
  enum Result { READY, EXPIRED, FULL };
  uint32_t active() const { return waiting; }
  kj::Promise<Result> wait(kj::Timer& timer, kj::Duration allowance, uint32_t maximum,
      std::string tenant, std::vector<std::shared_ptr<TenantExecutionState>> states) {
    if (allowance <= 0 * kj::MILLISECONDS) co_return EXPIRED;
    if (waiting >= maximum) co_return FULL;
    ++waiting;
    KJ_DEFER(--waiting);
    const auto until = timer.now() + allowance;
    kj::Promise<void> changed = kj::NEVER_DONE;
    for (const auto& state : states) {
      if (!state->limited(tenant)) co_return READY;
      changed = kj::mv(changed).exclusiveJoin(state->changed(tenant));
    }
    if (states.empty()) co_return READY;
    auto released = kj::mv(changed).then([&timer, until] { return timer.now() > until ? EXPIRED : READY; });
    auto deadline = timer.atTime(until).then([] { return EXPIRED; });
    co_return co_await kj::mv(released).exclusiveJoin(kj::mv(deadline));
  }
private:
  uint32_t waiting = 0;
};

}  // namespace coworkerd

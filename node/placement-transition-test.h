#pragma once
#include "placement-contract.h"
#include <array>
#include <kj/debug.h>
#include <map>

namespace coworkerd {
// Replays the policy inputs of the old placement scenarios. A synthetic peer reply
// lets the table vary transport outcomes without claiming to cover the transport.
inline void placementTransitions() {
  enum class Reply { Good, Red, Yellow, HotYellow, Saturated, Offline, Expired, Uncertain, Package, FatalLoad };
  using enum Reply;
  struct Row {
    const char* name;
    std::array<Reply, 3> reply; // Home s1, ingress/parent s2, other s3
    const char* tried;
    const char* result;
    uint8_t hops = 8;
    bool root = true;
    bool hotLocal = false;
    bool chooseHome = false;
    const char* avoided = "";
    const char* absent = "";
    bool selfExpired = false;
    uint32_t rootLimit = 2;
    uint32_t nestedLimit = 9;
  };
  constexpr Row rows[] = {
    {"cold placement with three members", {Good,Good,Good}, "1", "s1"},
    {"placement after Home removal", {Good,Good,Good}, "3", "s3", 8,true,false,false,"","1"},
    {"placement after Home rejoins", {Good,Good,Good}, "1", "s1"},
    {"cold binding uses Home", {Good,Good,Good}, "1", "s1", 8,false},
    {"hot binding stays local", {Good,Good,Good}, "2", "s2", 8,false,true},
    {"binding falls back when Home is offline", {Offline,Good,Good}, "12", "s2", 8,false},
    {"subsequent binding call avoids offline Home", {Offline,Good,Good}, "2", "s2", 8,false,false,false,"1"},
    {"binding falls back when Home is red", {Red,Good,Good}, "12", "s2", 8,false},
    {"binding still avoids Home after recovery", {Good,Good,Good}, "2", "s2", 8,false,true,false,"1"},
    {"binding returns to Home after avoidance expires", {Good,Good,Good}, "1", "s1", 8,false},
    {"yellow Home admits a hot binding version", {HotYellow,Good,Good}, "1", "s1", 8,false},
    {"yellow Home refuses a cold binding version", {Yellow,Good,Good}, "12", "s2", 8,false},
    {"binding stops after an uncertain result", {Uncertain,Good,Good}, "1", "RESULT_UNCERTAIN", 8,false},
    {"all candidates reject the binding", {Red,Red,Red}, "123", "PLACEMENT_EXHAUSTED", 8,false},
    {"binding stops after two failed hops", {Red,Red,Red}, "12", "PLACEMENT_EXHAUSTED/hops", 2,false},
    {"binding falls back when snapshot Home is stale", {Offline,Good,Good}, "12", "s2", 8,false},
    {"binding skips the ineligible peer", {Good,Good,Good}, "3", "s3", 8,false,false,false,"","1"},
    {"binding retries after confirmed pre-dispatch failure", {Offline,Good,Good}, "12", "s2", 8,false},
    {"binding stops when dispatch is uncertain", {Uncertain,Good,Good}, "1", "RESULT_UNCERTAIN", 8,false},
    {"binding tries candidates in order when all are avoided", {Offline,Good,Good}, "12", "s2", 8,false,false,false,"123"},
    {"binding starts with local while refused Home is avoided", {Red,Good,Good}, "2", "s2", 8,false,false,false,"1"},
    {"cold binding keeps Home affinity", {Good,Good,Good}, "1", "s1", 8,false},
    {"binding falls back when Home is saturated", {Saturated,Good,Good}, "12", "s2", 8,false},
    {"cold ingress uses Home", {Good,Good,Good}, "1", "s1"},
    {"hot ingress stays local", {Good,Good,Good}, "2", "s2", 8,true,true},
    {"Home policy overrides hot local ingress", {Good,Good,Good}, "1", "s1", 8,true,true,true},
    {"Home policy falls back to ingress when Home is red", {Red,Good,Good}, "12", "s2", 8,true,true,true},
    {"red Home refuses forwarding and red ingress admits its own root", {Red,Red,Good}, "12", "s2"},
    {"root falls back when Home reaches the root cap", {Saturated,Good,Good}, "12", "s2"},
    {"root falls back after a failed connection", {Offline,Good,Good}, "12", "s2"},
    {"nested call falls back from a red Home", {Red,Good,Good}, "12", "s2", 8,false},
    {"nested call falls back when Home reaches the nested cap", {Saturated,Good,Good}, "12", "s2", 8,false},
    {"nested call falls back after a failed connection", {Offline,Good,Good}, "12", "s2", 8,false},
    {"third candidate runs after Home and ingress refuse", {Red,Saturated,Good}, "123", "s3"},
    {"all candidates refuse or are offline", {Red,Saturated,Offline}, "123", "PLACEMENT_EXHAUSTED"},
    {"hop budget runs out before the third candidate", {Red,Saturated,Good}, "12", "PLACEMENT_EXHAUSTED/hops", 2},
    {"one hop is enough for a successful forward", {Good,Good,Good}, "1", "s1", 1},
    {"one failed hop leaves no fallback", {Red,Good,Good}, "1", "PLACEMENT_EXHAUSTED/hops", 1},
    {"forward succeeds on the first of two hops", {Good,Good,Good}, "1", "s1", 2},
    {"two hops allow a fallback after refusal", {Red,Good,Good}, "12", "s2", 2},
    {"expired lease stops retry", {Expired,Good,Good}, "1", "LEASE_EXPIRED"},
    {"missing exact package allows fallback", {Package,Good,Good}, "12", "s2"},
    {"uncertain root result stops retry", {Uncertain,Good,Good}, "1", "RESULT_UNCERTAIN"},
    {"ingress race with confirmed pre-dispatch failure allows retry", {Offline,Good,Good}, "12", "s2"},
    {"yellow Home refuses a forwarded cold root", {Yellow,Good,Good}, "12", "s2"},
    {"yellow Home admits a forwarded hot root", {HotYellow,Good,Good}, "1", "s1"},
    {"root traffic uses the root admission limit", {Saturated,Good,Good}, "12", "s2"},
    {"cold copy uses stable Home", {Good,Good,Good}, "1", "s1"},
    {"stale ingress loses hot-local priority", {Good,Good,Good}, "1", "s1", 8,true,true,false,"","",true},
    {"equal limits leave no nested headroom", {Saturated,Saturated,Saturated}, "123", "PLACEMENT_EXHAUSTED", 8,false,false,false,"","",false,8,8},
  };
  static_assert(std::size(rows) == 50);
  struct Member { std::string state; };
  for (const auto& row : rows) {
    std::map<std::string, Member> members;
    for (char n : std::string_view("123")) if (std::string_view(row.absent).find(n) == std::string_view::npos)
      members.emplace(std::string("s") + n, Member{"active"});
    const auto ranked = rankPlacementMembers("light-0", members, "s2", row.selfExpired);
    auto order = placementOrder(ranked, "s2", row.selfExpired, ranked.empty() ? "" : ranked.front(), members.contains("s2"));
    bool localFirst = chooseHotLocalPlacement(row.selfExpired, members.contains("s2"), [&] { return row.hotLocal; },
        [] { return true; });
    if (localFirst && !order.empty() && order.front() != "s2" && row.chooseHome) localFirst = false;
    if (localFirst) prioritizePlacementNode(order, "s2");
    orderByPlacementAvoidance(order, localFirst, [&](const auto& id) { return std::string_view(row.avoided).find(id.back()) == std::string_view::npos; });
    uint8_t hops = row.hops;
    std::string tried, result, lastRefusal;
    for (const auto& id : order) {
      if (!consumePlacementHop(hops)) break;
      tried += id.back();
      const auto reply = row.reply[id.back() - '1'];
      const auto band = reply == Red ? "red" : (reply == Yellow || reply == HotYellow) ? "yellow" : "green";
      const bool rejectedBand = placementBandRefusal(true, row.root, row.root && id != "s2", band,
          [&] { return reply == HotYellow; }) != nullptr;
      const auto admission = initialPlacementAdmission(reply == Expired ? 0.0 : 500.0,
          reply == Saturated ? row.rootLimit : 0, reply == Saturated && !row.root ? row.nestedLimit - row.rootLimit : 0,
          false, row.root, row.rootLimit, row.nestedLimit);
      lastRefusal = rejectedBand ? "LOAD_BAND_REJECTED" : admission == InitialPlacementAdmission::Expired ? "LEASE_EXPIRED" :
          admission == InitialPlacementAdmission::Saturated ? "OVERLOADED" : reply == Offline ? "PEER_UNAVAILABLE" :
          reply == Uncertain ? "RESULT_UNCERTAIN" : reply == Package || reply == FatalLoad ? "LOAD_FAILED" : "";
      if (lastRefusal.empty()) { result = id; break; }
      if (!placementRetryable(lastRefusal, reply == Package ? PACKAGE_UNAVAILABLE : "") || lastRefusal == "LEASE_EXPIRED") {
        result = lastRefusal; break;
      }
    }
    if (result.empty()) result = std::string_view(placementExhaustionReason(hops)) == "placement hops exhausted" ?
        "PLACEMENT_EXHAUSTED/hops" : "PLACEMENT_EXHAUSTED";
    KJ_REQUIRE(tried == row.tried && result == row.result, row.name, tried.c_str(), result.c_str(), lastRefusal.c_str(), unsigned(hops));
  }
}
} // namespace coworkerd

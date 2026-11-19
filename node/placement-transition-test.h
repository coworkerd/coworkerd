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
    const char* old;
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
    {"v23-mechanism-4.1 / cold three members", {Good,Good,Good}, "1", "s1"},
    {"v23-mechanism-4.1 / Home removed", {Good,Good,Good}, "3", "s3", 8,true,false,false,"","1"},
    {"v23-mechanism-4.1 / Home rejoins", {Good,Good,Good}, "1", "s1"},
    {"v23-mechanism-4.2 / cold binding", {Good,Good,Good}, "1", "s1", 8,false},
    {"v23-mechanism-4.2 / hot binding", {Good,Good,Good}, "2", "s2", 8,false,true},
    {"v23-mechanism-4.3 / Home offline", {Offline,Good,Good}, "12", "s2", 8,false},
    {"v23-mechanism-4.3 / subsequent call avoids Home", {Offline,Good,Good}, "2", "s2", 8,false,false,false,"1"},
    {"v23-mechanism-4.4 / red Home", {Red,Good,Good}, "12", "s2", 8,false},
    {"v23-mechanism-4.4 / still avoided after recovery", {Good,Good,Good}, "2", "s2", 8,false,true,false,"1"},
    {"v23-mechanism-4.4 / avoidance expired", {Good,Good,Good}, "1", "s1", 8,false},
    {"v23-mechanism-4.5 / hot version at yellow Home", {HotYellow,Good,Good}, "1", "s1", 8,false},
    {"v23-mechanism-4.5 / cold version at yellow Home", {Yellow,Good,Good}, "12", "s2", 8,false},
    {"v23-mechanism-4.6 / uncertain result", {Uncertain,Good,Good}, "1", "RESULT_UNCERTAIN", 8,false},
    {"v23-mechanism-4.7 / all reject", {Red,Red,Red}, "123", "PLACEMENT_EXHAUSTED", 8,false},
    {"v23-mechanism-4.7-hops / only two attempts", {Red,Red,Red}, "12", "PLACEMENT_EXHAUSTED/hops", 2,false},
    {"v23-mechanism-4.8-stale / stale Home in snapshot", {Offline,Good,Good}, "12", "s2", 8,false},
    {"v23-mechanism-4.8-exit / peer ineligible", {Good,Good,Good}, "3", "s3", 8,false,false,false,"","1"},
    {"v23-mechanism-4.10 / explicit failure before dispatch", {Offline,Good,Good}, "12", "s2", 8,false},
    {"v23-mechanism-4.10 / committed dispatch", {Uncertain,Good,Good}, "1", "RESULT_UNCERTAIN", 8,false},
    {"v23-all-avoided / still try candidates in order", {Offline,Good,Good}, "12", "s2", 8,false,false,false,"123"},
    {"v23-epoch-refusal-backoff / retry after refusal", {Red,Good,Good}, "2", "s2", 8,false,false,false,"1"},
    {"v23-contraction-formal / stable cold affinity", {Good,Good,Good}, "1", "s1", 8,false},
    {"v23-contraction-saturation / unavailable Home", {Saturated,Good,Good}, "12", "s2", 8,false},
    {"v25-4.1 / cold ingress uses Home", {Good,Good,Good}, "1", "s1"},
    {"v25-4.2-p0 / hot local", {Good,Good,Good}, "2", "s2", 8,true,true},
    {"v25-4.2-p1 / use Home", {Good,Good,Good}, "1", "s1", 8,true,true,true},
    {"v25-4.2-p1 / Home turns red", {Red,Good,Good}, "12", "s2", 8,true,true,true},
    {"v25-4.3-ingress-red / forwarded root", {Red,Red,Good}, "12", "s2"},
    {"v25-4.3-ingress-saturation / root cap", {Saturated,Good,Good}, "12", "s2"},
    {"v25-4.3-ingress-offline / failed connection", {Offline,Good,Good}, "12", "s2"},
    {"v25-4.3-binding-red / nested on red", {Red,Good,Good}, "12", "s2", 8,false},
    {"v25-4.3-binding-saturation / nested cap", {Saturated,Good,Good}, "12", "s2", 8,false},
    {"v25-4.3-binding-offline / failed connection", {Offline,Good,Good}, "12", "s2", 8,false},
    {"v25-4.4-next / Home red and local saturated", {Red,Saturated,Good}, "123", "s3"},
    {"v25-4.4-all / Home red, local saturated, next offline", {Red,Saturated,Offline}, "123", "PLACEMENT_EXHAUSTED"},
    {"v25-4.4-hops / Home red and local saturated", {Red,Saturated,Good}, "12", "PLACEMENT_EXHAUSTED/hops", 2},
    {"v25-4.5-budget-1-forward / one successful hop", {Good,Good,Good}, "1", "s1", 1},
    {"v25-4.5-budget-1-reroute / no fallback hop", {Red,Good,Good}, "1", "PLACEMENT_EXHAUSTED/hops", 1},
    {"v25-4.5-budget-2-forward / two-hop input", {Good,Good,Good}, "1", "s1", 2},
    {"v25-4.5-budget-2-reroute / fallback hop remains", {Red,Good,Good}, "12", "s2", 2},
    {"v25-4.5-deadline / no retry after expiry", {Expired,Good,Good}, "1", "LEASE_EXPIRED"},
    {"v25-4.5-unavailable / exact package missing", {Package,Good,Good}, "12", "s2"},
    {"v25-4.5-uncertain / committed request never repeats", {Uncertain,Good,Good}, "1", "RESULT_UNCERTAIN"},
    {"v25-4.6 ingress race / proven pre-dispatch failure", {Offline,Good,Good}, "12", "s2"},
    {"v25-4.7-band / forwarded cold root on yellow", {Yellow,Good,Good}, "12", "s2"},
    {"v25-4.7-band / forwarded hot root on yellow", {HotYellow,Good,Good}, "1", "s1"},
    {"v25-4.7-headroom / root traffic using first band", {Saturated,Good,Good}, "12", "s2"},
    {"v25-4.9 / cold copy uses stable Home", {Good,Good,Good}, "1", "s1"},
    {"v25-4.10-formal / stale ingress loses hot-first", {Good,Good,Good}, "1", "s1", 8,true,true,false,"","",true},
    {"v25-4.11 / equal root and nested limits", {Saturated,Saturated,Saturated}, "123", "PLACEMENT_EXHAUSTED", 8,false,false,false,"","",false,8,8},
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
    KJ_REQUIRE(tried == row.tried && result == row.result, row.old, tried.c_str(), result.c_str(), lastRefusal.c_str(), unsigned(hops));
  }
}
} // namespace coworkerd

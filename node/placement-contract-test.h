#pragma once
#include "placement-contract.h"
#include "invocation-contract.h"
#include "placement-transition-test.h"
#include <kj/debug.h>
#include <map>

namespace coworkerd {
inline void placementContracts() {
  placementTransitions();
  struct BandCase {
    const char* name;
    bool root, forwarded, hot;
    const char* band;
    const char* reason;
  };
  constexpr BandCase bands[] = {
    {"red band rejects a forwarded root", true, true, false, "red", "red band rejects forwarded roots"},
    {"red band rejects a nested invocation", false, false, false, "red", "red band rejects nested invocations"},
    {"red band admits the local root", true, false, false, "red", nullptr},
    {"green band admits a forwarded root", true, true, false, "green", nullptr},
    {"yellow band rejects a cold forwarded root", true, true, false, "yellow", "yellow band rejects cold version"},
    {"yellow band admits a hot forwarded root", true, true, true, "yellow", nullptr},
    {"yellow band rejects a cold nested invocation", false, false, false, "yellow", "yellow band rejects cold version"},
    {"yellow band admits a hot nested invocation", false, false, true, "yellow", nullptr},
  };
  for (const auto& row : bands) {
    unsigned consulted = 0;
    const char* reason = placementBandRefusal(true, row.root, row.forwarded, row.band,
        [&] { ++consulted; return row.hot; });
    KJ_REQUIRE((reason == nullptr) == (row.reason == nullptr), row.name);
    if (reason) {
      KJ_REQUIRE(std::string_view(reason) == row.reason, row.name);
    }
    KJ_REQUIRE(consulted == unsigned(std::string_view(row.band) == "yellow" && (!row.root || row.forwarded)), row.name);
  }
  using Admission = InitialPlacementAdmission;
  struct AdmissionCase {
    const char* name;
    double leaseMs;
    uint32_t roots, nested;
    bool reserved, root;
    Admission expected;
  };
  constexpr AdmissionCase admissions[] = {
    {"root admission reaches the root limit", 100, 2, 0, false, true, Admission::Saturated},
    {"binding admission reaches the nested limit", 100, 2, 7, false, false, Admission::Saturated},
    {"binding admission uses the nested headroom", 100, 2, 0, false, false, Admission::Allowed},
    {"reserved root checks the count before acquiring its own slot", 100, 2, 0, true, true, Admission::Allowed},
    {"reserved binding checks the count before acquiring its own slot", 100, 2, 7, true, false, Admission::Allowed},
    {"expired lease takes precedence over saturation", 0, 2, 7, false, true, Admission::Expired},
  };
  for (const auto& row : admissions) {
    KJ_REQUIRE(initialPlacementAdmission(row.leaseMs, row.roots, row.nested, row.reserved, row.root, 2, 9) ==
        row.expected, row.name);
  }

  struct BackoffCase {
    const char* name;
    uint32_t prior, advertised, connect, maximum, next, delay;
    bool failedToConnect;
  };
  constexpr BackoffCase backoffs[] = {
    {"first connection failure starts the retry backoff", 0, 0, 2000, 2000, 1, 2000, true},
    {"connection retry grows before reaching the maximum", 1, 0, 300, 2000, 2, 600, true},
    {"consecutive connection failures stay within the maximum", 32, 0, 300, 2000, 32, 2000, true},
    {"peer refusal resets the connection streak", 6, 200, 2000, 2000, 0, 200, false},
    {"advertised refusal delay is capped", 0, 4000, 300, 2000, 0, 2000, false},
  };
  for (const auto& row : backoffs) {
    auto failures = row.prior;
    KJ_REQUIRE(advancePlacementAvoidance(failures, row.advertised, row.failedToConnect, row.connect, row.maximum) ==
        row.delay, row.name);
    KJ_REQUIRE(failures == row.next, row.name);
  }

  struct HopCase {
    const char* name;
    uint8_t initial;
    unsigned candidates, attempts, remaining;
    const char* exhaustedReason;
  };
  constexpr HopCase hopCases[] = {
    {"all three candidates rejected", 8, 3, 3, 5, "all candidates rejected or unavailable"},
    {"two hops stop before the third candidate", 2, 3, 2, 0, "placement hops exhausted"},
    {"no remaining hop allows no attempt", 0, 3, 0, 0, "placement hops exhausted"},
  };
  for (const auto& row : hopCases) {
    auto remaining = row.initial;
    unsigned attempts = 0;
    while (attempts < row.candidates && consumePlacementHop(remaining)) ++attempts;
    KJ_REQUIRE(attempts == row.attempts && remaining == row.remaining, row.name);
    KJ_REQUIRE(std::string_view(placementExhaustionReason(remaining)) == row.exhaustedReason, row.name);
  }

  struct OutcomeCase { const char* name; const char* code; const char* category; bool anotherCandidate; };
  constexpr OutcomeCase outcomes[] = {
    {"load-band refusal allows retry", "LOAD_BAND_REJECTED", "", true},
    {"overload allows retry", "OVERLOADED", "", true},
    {"peer unavailability allows retry", "PEER_UNAVAILABLE", "", true},
    {"uncertain result stops retry", "RESULT_UNCERTAIN", "", false},
    {"missing package allows retry", "LOAD_FAILED", "package-unavailable", true},
    {"load limit stops retry", "LOAD_FAILED", "limit", false},
  };
  for (const auto& row : outcomes)
    KJ_REQUIRE(placementRetryable(row.code, row.category) == row.anotherCandidate, row.name);

  using Nodes = std::vector<std::string>;
  struct Member { std::string state; };
  std::map<std::string, Member> members{{"s1", {"active"}}, {"s2", {"active"}}, {"s3", {"active"}}};
  // An independently enumerated rendezvous ranking for `light-0`.
  KJ_REQUIRE(bool(rankPlacementMembers("light-0", members, "s1", false) == Nodes{"s1", "s3", "s2"}),
      "cold placement uses rendezvous ranking");
  members.at("s2").state = "draining";
  KJ_REQUIRE(bool(rankPlacementMembers("light-0", members, "s1", false) == Nodes{"s1", "s3"}),
      "rendezvous ranking omits draining peer");
  KJ_REQUIRE(bool(rankPlacementMembers("light-0", members, "s3", true) == Nodes{"s1", "s3"}),
      "expired local member is tried after the ranked peers");
  struct OrderCase {
    const char* name;
    Nodes ranked, avoided, expected;
    bool lastResort = false, hotFirst = false;
  };
  const OrderCase orders[] = {
    {"Home, then local, then remaining peer", {"s3", "s2", "s1"}, {}, {"s3", "s1", "s2"}},
    {"avoid rejected Home", {"s3", "s2", "s1"}, {"s3"}, {"s1", "s2", "s3"}},
    {"all avoided candidates keep placement order", {"s3", "s2", "s1"}, {"s3", "s2", "s1"}, {"s3", "s1", "s2"}},
    {"expired local membership keeps self last", {"s3", "s2", "s1"}, {}, {"s3", "s2", "s1"}, true},
    {"hot local remains first during avoidance", {"s1", "s3", "s2"}, {"s1", "s3"}, {"s1", "s2", "s3"}, false, true},
    {"sole eligible peer is tried", {"s3"}, {}, {"s3"}},
  };
  for (const auto& row : orders) {
    auto order = placementOrder(row.ranked, "s1", row.lastResort, row.ranked.front(),
        std::find(row.ranked.begin(), row.ranked.end(), "s1") != row.ranked.end());
    orderByPlacementAvoidance(order, row.hotFirst, [&](const auto& n) {
      return std::find(row.avoided.begin(), row.avoided.end(), n) == row.avoided.end();
    });
    KJ_REQUIRE(bool(order == row.expected), row.name);
  }
}
} // namespace coworkerd

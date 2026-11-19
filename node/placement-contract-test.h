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
    const char* old;
    bool root, forwarded, hot;
    const char* band;
    const char* reason;
  };
  constexpr BandCase bands[] = {
    {"v25-4.3-ingress-red", true, true, false, "red", "red band rejects forwarded roots"},
    {"v25-4.3-binding-red", false, false, false, "red", "red band rejects nested invocations"},
    {"v25-4.4-next / v25-4.4-all / v25-4.4-hops", true, false, false, "red", nullptr},
    {"v25-4.2-p0 / v25-4.2-p1", true, true, false, "green", nullptr},
    {"v25-4.7-band", true, true, false, "yellow", "yellow band rejects cold version"},
    {"v25-4.7-band", true, true, true, "yellow", nullptr},
    {"v25-4.7-band", false, false, false, "yellow", "yellow band rejects cold version"},
    {"v25-4.7-band", false, false, true, "yellow", nullptr},
  };
  for (const auto& row : bands) {
    unsigned consulted = 0;
    const char* reason = placementBandRefusal(true, row.root, row.forwarded, row.band,
        [&] { ++consulted; return row.hot; });
    KJ_REQUIRE((reason == nullptr) == (row.reason == nullptr), row.old);
    if (reason) {
      KJ_REQUIRE(std::string_view(reason) == row.reason, row.old);
    }
    KJ_REQUIRE(consulted == unsigned(std::string_view(row.band) == "yellow" && (!row.root || row.forwarded)), row.old);
  }
  using Admission = InitialPlacementAdmission;
  struct AdmissionCase {
    const char* old;
    double leaseMs;
    uint32_t roots, nested;
    bool reserved, root;
    Admission expected;
  };
  constexpr AdmissionCase admissions[] = {
    {"v25-4.3-ingress-saturation", 100, 2, 0, false, true, Admission::Saturated},
    {"v25-4.3-binding-saturation", 100, 2, 7, false, false, Admission::Saturated},
    {"v25-4.7-headroom", 100, 2, 0, false, false, Admission::Allowed},
    {"reserved root checks the count before acquiring its own slot", 100, 2, 0, true, true, Admission::Allowed},
    {"reserved binding checks the count before acquiring its own slot", 100, 2, 7, true, false, Admission::Allowed},
    {"v25-4.5-deadline: expired lease takes precedence over saturation", 0, 2, 7, false, true, Admission::Expired},
  };
  for (const auto& row : admissions) {
    KJ_REQUIRE(initialPlacementAdmission(row.leaseMs, row.roots, row.nested, row.reserved, row.root, 2, 9) ==
        row.expected, row.old);
  }

  struct BackoffCase {
    const char* old;
    uint32_t prior, advertised, connect, maximum, next, delay;
    bool failedToConnect;
  };
  constexpr BackoffCase backoffs[] = {
    {"v23-mechanism-4.3: disconnected peer", 0, 0, 2000, 2000, 1, 2000, true},
    {"connection retry grows before reaching the maximum", 1, 0, 300, 2000, 2, 600, true},
    {"consecutive connection failures stay within the maximum", 32, 0, 300, 2000, 32, 2000, true},
    {"v23-epoch-refusal-backoff: peer refusal resets the connection streak", 6, 200, 2000, 2000, 0, 200, false},
    {"advertised refusal delay is capped", 0, 4000, 300, 2000, 0, 2000, false},
  };
  for (const auto& row : backoffs) {
    auto failures = row.prior;
    KJ_REQUIRE(advancePlacementAvoidance(failures, row.advertised, row.failedToConnect, row.connect, row.maximum) ==
        row.delay, row.old);
    KJ_REQUIRE(failures == row.next, row.old);
  }

  struct HopCase {
    const char* old;
    uint8_t initial;
    unsigned candidates, attempts, remaining;
    const char* exhaustedReason;
  };
  constexpr HopCase hopCases[] = {
    {"v25-4.4-all: all three candidates rejected", 8, 3, 3, 5, "all candidates rejected or unavailable"},
    {"v25-4.4-hops: third candidate must not be tried", 2, 3, 2, 0, "placement hops exhausted"},
    {"no remaining hop allows no attempt", 0, 3, 0, 0, "placement hops exhausted"},
  };
  for (const auto& row : hopCases) {
    auto remaining = row.initial;
    unsigned attempts = 0;
    while (attempts < row.candidates && consumePlacementHop(remaining)) ++attempts;
    KJ_REQUIRE(attempts == row.attempts && remaining == row.remaining, row.old);
    KJ_REQUIRE(std::string_view(placementExhaustionReason(remaining)) == row.exhaustedReason, row.old);
  }

  struct OutcomeCase { const char* old; const char* code; const char* category; bool anotherCandidate; };
  constexpr OutcomeCase outcomes[] = {
    {"v25-4.3-ingress-red / v25-4.3-binding-red", "LOAD_BAND_REJECTED", "", true},
    {"v25-4.3-ingress-saturation / v25-4.3-binding-saturation", "OVERLOADED", "", true},
    {"v25-4.3-ingress-offline / v25-4.3-binding-offline", "PEER_UNAVAILABLE", "", true},
    {"v25-4.5-uncertain / v23-mechanism-4.10", "RESULT_UNCERTAIN", "", false},
    {"v25-4.5-unavailable", "LOAD_FAILED", "package-unavailable", true},
    {"v32-placement-1", "LOAD_FAILED", "limit", false},
  };
  for (const auto& row : outcomes)
    KJ_REQUIRE(placementRetryable(row.code, row.category) == row.anotherCandidate, row.old);

  using Nodes = std::vector<std::string>;
  struct Member { std::string state; };
  std::map<std::string, Member> members{{"s1", {"active"}}, {"s2", {"active"}}, {"s3", {"active"}}};
  // An independently enumerated rendezvous ranking for `light-0`, in the old 4.1 fixture.
  KJ_REQUIRE(bool(rankPlacementMembers("light-0", members, "s1", false) == Nodes{"s1", "s3", "s2"}),
      "v25-4.1 cold Home from each ingress");
  members.at("s2").state = "draining";
  KJ_REQUIRE(bool(rankPlacementMembers("light-0", members, "s1", false) == Nodes{"s1", "s3"}),
      "v23-mechanism-4.8-exit draining peer disappears");
  KJ_REQUIRE(bool(rankPlacementMembers("light-0", members, "s3", true) == Nodes{"s1", "s3"}),
      "expired local member is tried after the ranked peers");
  struct OrderCase {
    const char* old;
    Nodes ranked, avoided, expected;
    bool lastResort = false, hotFirst = false;
  };
  const OrderCase orders[] = {
    {"v25-4.4-next", {"s3", "s2", "s1"}, {}, {"s3", "s1", "s2"}},
    {"v23-mechanism-4.4: avoid rejected Home", {"s3", "s2", "s1"}, {"s3"}, {"s1", "s2", "s3"}},
    {"v23-all-avoided", {"s3", "s2", "s1"}, {"s3", "s2", "s1"}, {"s3", "s1", "s2"}},
    {"expired local membership keeps self last", {"s3", "s2", "s1"}, {}, {"s3", "s2", "s1"}, true},
    {"v25-4.2-p0 hot local remains first", {"s1", "s3", "s2"}, {"s1", "s3"}, {"s1", "s2", "s3"}, false, true},
    {"v23-mechanism-4.8-exit", {"s3"}, {}, {"s3"}},
  };
  for (const auto& row : orders) {
    auto order = placementOrder(row.ranked, "s1", row.lastResort, row.ranked.front(),
        std::find(row.ranked.begin(), row.ranked.end(), "s1") != row.ranked.end());
    orderByPlacementAvoidance(order, row.hotFirst, [&](const auto& n) {
      return std::find(row.avoided.begin(), row.avoided.end(), n) == row.avoided.end();
    });
    KJ_REQUIRE(bool(order == row.expected), row.old);
  }
}
} // namespace coworkerd

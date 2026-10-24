#include "object-store.h"
#include "object-state.h"
#include "tenant-manifest.h"

#include <kj/debug.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <unistd.h>

namespace coworkerd {
// `coworkerd check storage ENDPOINT BUCKET`: the object store's
// self-check -- the bucket and a signed write with the deploy identity, signed reads with the read identity
// (COWORKERD_READ_ACCESS_KEY, COWORKERD_READ_SECRET_KEY), and unsigned requests refused. Then what the membership on
// the object store needs (docs/s3-truth-and-durable-objects.md section 2), with the deploy identity under check/: a
// create-if-absent twice (the second must find the key existing), a listing whose answer carries a Date header and its
// objects' LastModified, and this machine's clock within COWORKERD_CLOCK_SKEW_TOLERANCE_MS (default 2000) of that Date.
// Reported but not required: a listing of members/ with the read identity (the 0.4 nodes' identity lists and writes
// members/; a 0.3 read-only one does not) and whether create-if-absent holds under concurrency (8 writers on one key,
// 20 rounds at once) -- `conditionalPutAtomic` (SeaweedFS before 4.29 answers false) -- and whether the bucket holds the
// cluster's marker cluster/format/1 (`clusterFormat`: init writes it after this check). One JSON line per request, then
// the contract's line.
// SeaweedFS 4.48, compared with 3.97 (v22, b8): an object PUT into a bucket that does not exist still answers 200 (so the
// bucket comes first, here and in init), but its gateway no longer keeps answering NoSuchBucket once the bucket exists,
// and a read signed in the Authorization header no longer needs Write; it refuses the presigned read that also signs
// x-amz-content-sha256 and x-amz-date, so the client presigns reads the standard way (host only, UNSIGNED-PAYLOAD), which
// both versions take.
int runStorageCheck(int argc, char** argv) {
  try {
    KJ_REQUIRE(argc == 3, "usage: coworkerd check storage ENDPOINT BUCKET");
    auto io = kj::setupAsyncIo();
    coworkerd::ObjectStoreConfig config;
    config.endpoint = argv[1];
    config.bucket = argv[2];
    coworkerd::ObjectStore store(config, io.provider->getNetwork(), io.provider->getTimer());
    auto readerConfig = config;
    if (auto key = std::getenv("COWORKERD_READ_ACCESS_KEY")) readerConfig.accessKey = key;
    if (auto key = std::getenv("COWORKERD_READ_SECRET_KEY")) readerConfig.secretKey = key;
    coworkerd::ObjectStore reader(readerConfig, io.provider->getNetwork(), io.provider->getTimer());
    const std::string bytes = "coworkerd-v22-signature-probe\n";
    const auto digest = coworkerd::objectSha256(bytes);
    const std::string key = "bundles/" + digest;
    auto execute = [&](kj::HttpMethod method, std::string target, std::string payload, bool signedRequest) {
      const auto started = std::chrono::steady_clock::now();
      auto& client = method == kj::HttpMethod::GET || method == kj::HttpMethod::HEAD ? reader : store;
      auto response = client.request(method, std::move(target), std::move(payload), signedRequest).wait(io.waitScope);
      const auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();
      std::cout << "{\"method\":\"" << kj::str(method).cStr() << "\",\"signed\":" << (signedRequest ? "true" : "false")
          << ",\"status\":" << response.status << ",\"bodyBytes\":" << response.bytes.size()
          << ",\"bodySha256\":\"" << coworkerd::objectSha256(response.bytes)
          << "\",\"elapsedMs\":" << elapsed << "}" << std::endl;
      return response;
    };
    auto bucket = execute(kj::HttpMethod::PUT, "", "", true);
    KJ_REQUIRE(bucket.status == 200 || bucket.status == 409, "bucket creation failed");
    KJ_REQUIRE(execute(kj::HttpMethod::PUT, key, bytes, true).status == 200, "signed PUT failed");
    auto get = execute(kj::HttpMethod::GET, key, "", true);
    KJ_REQUIRE(get.status == 200 && get.bytes == bytes, "signed GET failed");
    KJ_REQUIRE(execute(kj::HttpMethod::HEAD, key, "", true).status == 200, "signed HEAD failed");
    for (auto method : {kj::HttpMethod::PUT, kj::HttpMethod::GET, kj::HttpMethod::HEAD}) {
      auto rejected = execute(method, key, method == kj::HttpMethod::PUT ? bytes : "", false);
      KJ_REQUIRE(rejected.status == 403, "STOP: unsigned object request was not rejected");
    }

    // The contract of the membership on the object store.
    uint32_t tolerance = 2000;
    if (auto value = std::getenv("COWORKERD_CLOCK_SKEW_TOLERANCE_MS")) tolerance = static_cast<uint32_t>(std::strtoul(value, nullptr, 10));
    const std::string prefix = "check/storage-" + std::to_string(coworkerd::realClockUnixMs()) + "-" + std::to_string(getpid()) + "/";
    auto first = store.create(prefix + "create", "first\n").wait(io.waitScope);
    auto second = store.create(prefix + "create", "second\n").wait(io.waitScope);
    auto kept = store.get(prefix + "create").wait(io.waitScope);
    coworkerd::ClockSample sample;
    sample.sentUnixMs = coworkerd::realClockUnixMs();
    auto listing = store.list(prefix).wait(io.waitScope);
    sample.receivedUnixMs = coworkerd::realClockUnixMs();
    sample.dateUnixMs = listing.dateUnixMs;
    const auto clock = coworkerd::checkClock(sample, tolerance);
    bool lastModified = !listing.objects.empty();
    for (const auto& object : listing.objects) lastModified = lastModified && object.lastModifiedUnixMs > 0;
    store.remove(prefix + "create").wait(io.waitScope);
    // Reported, not required: a check run with a 0.3 read-only identity (no List) still passes as it did.
    std::string membersListed = "\"ok\":false";
    try {
      auto members = reader.list(coworkerd::MEMBERS_PREFIX).wait(io.waitScope);
      membersListed = "\"objects\":" + std::to_string(members.objects.size()) + ",\"ok\":true";
    } catch (const kj::Exception& error) {
      membersListed = "\"ok\":false,\"reason\":\"" + std::string(error.getDescription().cStr()).substr(0, 200) + "\"";
      for (auto& c : membersListed) if (c == '\n') c = ' ';
    }
    auto measurement = coworkerd::measureConditionalPut(store, prefix + "concurrent-", 8, 20).wait(io.waitScope);
    // Reported, not required (init runs this check before it writes the marker): whether the bucket already holds the
    // cluster's marker, cluster/format/1, without which nodes and the control plane refuse to start.
    const auto marker = reader.get(coworkerd::CLUSTER_FORMAT_KEY).wait(io.waitScope);
    const bool sequential = first.created && !second.created && kept.bytes == "first\n";
    const bool clockOk = clock.readable && !clock.exceeds;
    const bool ok = sequential && listing.dateUnixMs > 0 && lastModified && clockOk;
    std::cout << "{\"contract\":{\"createIfAbsent\":{\"firstStatus\":" << first.status << ",\"secondStatus\":" << second.status
        << ",\"secondFoundTheKey\":" << (second.created ? "false" : "true") << ",\"firstBytesKept\":"
        << (kept.bytes == "first\n" ? "true" : "false") << ",\"ok\":" << (sequential ? "true" : "false")
        << "},\"listing\":{\"objects\":" << listing.objects.size() << ",\"dateHeader\":" << (listing.dateUnixMs > 0 ? "true" : "false")
        << ",\"lastModified\":" << (lastModified ? "true" : "false") << "},\"clock\":{\"skewMs\":" << clock.skewMs
        << ",\"minSkewMs\":" << clock.minSkewMs << ",\"maxSkewMs\":" << clock.maxSkewMs << ",\"toleranceMs\":" << tolerance
        << ",\"ok\":" << (clockOk ? "true" : "false") << "},\"readIdentityLists\":{\"prefix\":\"members/\"," << membersListed
        << "},\"clusterFormat\":{\"key\":\"" << coworkerd::CLUSTER_FORMAT_KEY << "\",\"present\":"
        << (marker.status == 200 ? "true" : "false") << ",\"status\":" << marker.status
        << "},\"conditionalPutAtomic\":" << (measurement.atomic() ? "true" : "false")
        << ",\"concurrency\":" << measurement.json() << ",\"ok\":" << (ok ? "true" : "false") << "}}" << std::endl;
    KJ_REQUIRE(sequential, "the second create-if-absent of a key did not find it existing");
    KJ_REQUIRE(listing.dateUnixMs > 0 && lastModified, "the listing's answer lacks its Date header or LastModified");
    KJ_REQUIRE(clockOk, "this machine's clock differs from the object store's Date by more than the tolerance");
    return 0;
  } catch (const kj::Exception& error) {
    std::cerr << error.getDescription().cStr() << std::endl;
    return 1;
  }
}
}  // namespace coworkerd

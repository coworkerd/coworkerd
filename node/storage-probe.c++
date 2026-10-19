#include "object-store.h"

#include <kj/debug.h>

#include <chrono>
#include <iostream>

namespace coworkerd {
// `coworkerd check storage ENDPOINT BUCKET`: the object store's
// self-check -- the bucket and a signed write with the deploy identity, signed reads with the read identity
// (COWORKERD_READ_ACCESS_KEY, COWORKERD_READ_SECRET_KEY), and unsigned requests refused.
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
    return 0;
  } catch (const kj::Exception& error) {
    std::cerr << error.getDescription().cStr() << std::endl;
    return 1;
  }
}
}  // namespace coworkerd

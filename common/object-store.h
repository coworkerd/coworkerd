#pragma once

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace coworkerd {

struct ObjectStoreConfig {
  std::string backend = "s3";
  std::filesystem::path directory;
  std::string endpoint;
  std::string bucket = "coworkerd";
  std::string region = "us-east-1";
  std::string accessKey;
  std::string secretKey;
  uint32_t timeoutMs = 30000;
};

struct ObjectResponse {
  uint status;
  std::string bytes;
  // The response's Date header as Unix milliseconds: an HTTP-date names whole seconds, so this is a multiple of
  // 1000. 0 when the response carried none that reads as an HTTP-date, and from the directory backend, which has
  // no clock of its own.
  uint64_t dateUnixMs = 0;
};

// One object of a listing, with the Date header of the page that listed it: LastModified and Date both come from
// the store's clock, so their difference is the object's age without trusting this machine's clock.
struct ListedObject {
  std::string key;
  uint64_t lastModifiedUnixMs = 0;
  std::string etag;
  uint64_t size = 0;
  uint64_t pageDateUnixMs = 0;
};

// Every object under a prefix, in key order; the first page's Date header and how many pages it took.
struct ObjectListing {
  std::vector<ListedObject> objects;
  uint64_t dateUnixMs = 0;
  uint32_t pages = 0;
};

// A create-if-absent write: created, or the key existed already (the store kept the bytes it had).
struct CreateResult {
  bool created = false;
  uint status = 0;
  uint64_t dateUnixMs = 0;
};

// Credentials are never included in diagnostics. This client deliberately uses
// only path addressing and signed, single-chunk payloads, without an AWS SDK.
// Keys: content by hash under bundles/, definitions/, modules/ and packages/ (lower-case hexadecimal paths), and
// the cluster's state under members/ (and check/, `coworkerd check`'s probes): '/'-separated segments of letters,
// digits, '.', '_' and '-', none of them "." or "..". Only the S3 backend keeps state: create, list and remove
// refuse the directory backend.
class ObjectStore {
public:
  ObjectStore(ObjectStoreConfig config, kj::Network& network, kj::Timer& timer);
  kj::Promise<bool> exists(std::string key);
  // An overwrite.
  kj::Promise<ObjectResponse> put(std::string key, std::string bytes);
  kj::Promise<ObjectResponse> get(std::string key);
  // PUT with `If-None-Match: *`: written only where the key does not exist. 412 (Precondition Failed) and 409 (a
  // conditional write to the same key in flight) both mean that it exists; any other status is an error.
  kj::Promise<CreateResult> create(std::string key, std::string bytes);
  // Every object whose key starts with `prefix` (ListObjectsV2, page after page until the store says that was the
  // last one); `maxKeys` bounds one page.
  kj::Promise<ObjectListing> list(std::string prefix, uint32_t maxKeys = 1000);
  // DELETE. A key that does not exist is removed as well (S3 answers 204 either way).
  kj::Promise<ObjectResponse> remove(std::string key);
  // Also used by the independent authentication probe (bucket creation and
  // unsigned requests). Normal callers use the operations above.
  kj::Promise<ObjectResponse> request(kj::HttpMethod method, std::string key,
      std::string bytes = {}, bool sign = true);
  const ObjectStoreConfig& config() const { return settings; }

private:
  // One request. Reads (GET, HEAD, a listing) are signed in the query and writes in the Authorization header:
  // SeaweedFS 3.97 wants Write during a header signature's verification even for reads (node/storage-probe.c++).
  struct Call {
    kj::HttpMethod method;
    std::string key;
    std::string bytes;
    bool sign = true;
    // Parameters besides the signature's (a listing's), signed with it.
    std::vector<std::pair<std::string, std::string>> query;
    bool ifNoneMatch = false;
  };
  ObjectStoreConfig settings;
  kj::Network& network;
  kj::Timer& timer;
  kj::Own<kj::HttpHeaderTable> table;
  kj::HttpHeaderId dateHeader;
  kj::HttpHeaderId hashHeader;
  kj::HttpHeaderId authorizationHeader;
  kj::HttpHeaderId ifNoneMatchHeader;
  kj::Promise<ObjectResponse> send(Call call);
  kj::Promise<ObjectResponse> sendInner(Call call);
};

std::string objectSha256(const std::string& bytes);
// "Sun, 06 Nov 1994 08:49:37 GMT" (an IMF-fixdate) as Unix milliseconds; 0 when it does not read as one. A header
// that came twice reaches here joined with ", "; the first date counts.
uint64_t httpDateUnixMs(std::string_view text);
// "2026-10-24T08:49:37.000Z" (a listing's LastModified) as Unix milliseconds; 0 when it does not read as one.
uint64_t isoTimeUnixMs(std::string_view text);
}  // namespace coworkerd

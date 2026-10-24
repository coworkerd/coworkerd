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
  // The longest a prefetch waits for one object (node/bundle-cache.c++). A single request is bounded by the three below.
  uint32_t timeoutMs = 30000;
  // Every request: establishing its connection (resolving the name included), then waiting for the answer and between
  // two reads of its body or two writes of the request's. A create-if-absent waits up to createTimeoutMs for its answer:
  // SeaweedFS 4.48 queues the later writers of one key behind its owner's lock, about 1 s each (R5, experiment 23).
  uint32_t connectTimeoutMs = 2000;
  uint32_t readTimeoutMs = 5000;
  uint32_t createTimeoutMs = 30000;
};

// What the client met: the requests it made (each on a connection of its own), the idempotent ones sent once more on a
// new connection after the first failed (a timeout or a connection that broke before the whole answer came), the
// timeouts by phase, and the creates whose answer was lost, decided by reading the key back.
struct ObjectStoreStats {
  uint64_t requests = 0, retries = 0, connectTimeouts = 0, readTimeouts = 0, failures = 0;
  uint64_t createsReadBack = 0, createsConfirmed = 0, createsFoundOther = 0, createsUnknown = 0;
  std::string json() const;
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
  // The answer to the create did not come back (a timeout, a broken connection) and the key was read back: `created`
  // says whether it held these bytes.
  bool readBack = false;
};

// Credentials are never included in diagnostics. This client deliberately uses
// only path addressing and signed, single-chunk payloads, without an AWS SDK.
// Keys: content by hash under bundles/, definitions/, modules/ and packages/ (lower-case hexadecimal paths), and
// the cluster's state under members/, cluster/ and tenants/ (and check/, `coworkerd check`'s probes): '/'-separated segments of letters,
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
  // conditional write to the same key in flight) both mean that it exists; any other status is an error. A create can
  // take effect while its answer is lost (R5: 20 of 20 under a network cut), and then a second try meets 412: when the
  // answer does not come back, the key is read back -- these very bytes there mean this create made it, which is why
  // every creator's bytes carry its own identity (a process instance and a random value of the write) -- and only an
  // absent key is created once more, on a new connection.
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
  const ObjectStoreStats& stats() const { return statistics; }

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
  ObjectStoreStats statistics;
  kj::Network& network;
  kj::Timer& timer;
  kj::Own<kj::HttpHeaderTable> table;
  kj::HttpHeaderId dateHeader;
  kj::HttpHeaderId hashHeader;
  kj::HttpHeaderId authorizationHeader;
  kj::HttpHeaderId ifNoneMatchHeader;
  // One request, sent once more on a new connection when it is idempotent (GET, HEAD, a listing, DELETE) and the first
  // attempt failed; an overwrite is sent once. Errors never carry the request's URL (a presigned one holds the access
  // key's identifier).
  kj::Promise<ObjectResponse> send(Call call);
  // One attempt on a connection of its own, every phase bounded (connect, write, answer, each read).
  kj::Promise<ObjectResponse> exchange(const Call& call);
  kj::Exception sanitized(const kj::Exception& error);
};

std::string objectSha256(const std::string& bytes);
// "Sun, 06 Nov 1994 08:49:37 GMT" (an IMF-fixdate) as Unix milliseconds; 0 when it does not read as one. A header
// that came twice reaches here joined with ", "; the first date counts.
uint64_t httpDateUnixMs(std::string_view text);
// "2026-10-24T08:49:37.000Z" (a listing's LastModified) as Unix milliseconds; 0 when it does not read as one.
uint64_t isoTimeUnixMs(std::string_view text);
}  // namespace coworkerd

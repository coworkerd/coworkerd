#pragma once

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <cstdint>
#include <deque>
#include <filesystem>
#include <optional>
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
  // Kept-alive connections (not read from a configuration file). After a whole answer a connection is kept for the next
  // request, at most idleConnections of them per lane (the heartbeat's lane keeps two); a read (GET, HEAD, a listing) or
  // a DELETE goes on one kept for at most idleConnectionMs, an overwrite or a create only on one used within
  // writeIdleConnectionMs, else on a new connection. Both well below the stores' own idle timeouts (reported 3-5 s for
  // S3, MinIO's default 30 s, SeaweedFS 4.48's 120 s for `weed s3`): a store that closes a connection as a request goes
  // out on it fails that request, and an overwrite is never sent again.
  uint32_t idleConnections = 64;
  uint32_t idleConnectionMs = 2000;
  uint32_t writeIdleConnectionMs = 1000;
};

// Which connections a request goes on: the shared ones, or the membership heartbeat's own (writeHeartbeat), which no
// other request uses -- however many requests the shared connections carry, a heartbeat neither waits for one of them
// nor goes on one a burst of other requests left behind.
enum class ObjectStoreLane : uint8_t { SHARED = 0, HEARTBEAT = 1 };

// The connections of one lane: open on a request (inUse) or kept for the next (idle); established (created), and the
// requests that went on a kept one (reused). How the others ended: an error, a timeout or a cancellation in the middle
// of a request (discardedOnError: its state unknown, never used again); found closed by the store, or holding bytes no
// request asked for, when taken -- before anything was sent on it (closedByPeer); an answer that said
// `Connection: close` (closedByAnswer); kept longer than the lane allows (closedIdle); more than idleConnections kept
// (closedOverCap). closedByAnswer also counts a request with a body that was answered other than 2xx (the store may not
// have read the whole body). reusedReadFailures and reusedWriteFailures: requests that failed on a kept connection other
// than by a timeout (a read is then sent once more on a new connection; an overwrite or a create follows its own rule
// and is never sent twice).
struct ObjectStoreConnections {
  uint64_t inUse = 0, idle = 0, created = 0, reused = 0, discardedOnError = 0, closedByPeer = 0, closedByAnswer = 0,
      closedIdle = 0, closedOverCap = 0, reusedReadFailures = 0, reusedWriteFailures = 0;
  std::string json() const;
};

// Every ObjectStore of this process, on every thread (the segment decoder's and the off-loop uploads' worker threads
// make one per job): connections established, requests on a kept one, connections discarded on an error.
struct ObjectStoreProcessConnections {
  uint64_t created = 0, reused = 0, discardedOnError = 0;
  std::string json() const;
};
ObjectStoreProcessConnections objectStoreProcessConnections();

// What the client met: the requests it made, the idempotent ones sent once more on a new connection after the first
// failed (a timeout or a connection that broke before the whole answer came), the timeouts by phase, the creates whose
// answer was lost, decided by reading the key back, and the connections of each lane.
struct ObjectStoreStats {
  uint64_t requests = 0, retries = 0, connectTimeouts = 0, readTimeouts = 0, failures = 0;
  uint64_t createsReadBack = 0, createsConfirmed = 0, createsFoundOther = 0, createsUnknown = 0;
  ObjectStoreConnections connections, heartbeatConnections;
  std::string json() const;
};

struct ObjectResponse {
  uint status;
  std::string bytes;
  // The response's Date header as Unix milliseconds: an HTTP-date names whole seconds, so this is a multiple of
  // 1000. 0 when the response carried none that reads as an HTTP-date, and from the directory backend, which has
  // no clock of its own.
  uint64_t dateUnixMs = 0;
  // The object's Last-Modified header (a GET's or HEAD's), the same way; 0 when there is none.
  uint64_t lastModifiedUnixMs = 0;
};

// A failure of the object store -- it could not be reached, did not answer in time, or answered with an error status
// or with something that is not an answer -- as opposed to a refusal of the caller's own. Every exception this client
// and the state layer above it (object-state.h) throw for one carries this detail, and the callers classify by it
// (isStorageFailure), never by the words of a description: one failure, one answer (503 STORAGE_UNAVAILABLE).
constexpr kj::Exception::DetailTypeId STORAGE_FAILURE_DETAIL = 0x636f776b2d73746full;
kj::Exception storageFailure(kj::Exception::Type type, kj::String description);
[[noreturn]] void failStorage(const std::string& description);
inline void requireStorage(bool condition, const std::string& description) { if (!condition) failStorage(description); }
bool isStorageFailure(const kj::Exception& error);
// A write that other writers kept winning: every attempt to create an aggregate's next revision met one another writer
// had created first (409 CONFLICT). The same detail mechanism, so that no reason's words decide it either.
constexpr kj::Exception::DetailTypeId WRITE_CONFLICT_DETAIL = 0x636f776b2d636f6eull;
[[noreturn]] void failConflict(const std::string& description);
bool isWriteConflict(const kj::Exception& error);

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

// One ListObjectsV2 page, including delimiter groups. An empty nextToken is the last page.
struct ObjectListingPage {
  std::vector<ListedObject> objects;
  std::vector<std::string> commonPrefixes;
  std::string nextToken;
  uint64_t dateUnixMs = 0;
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
// the cluster's state under members/, cluster/, tenants/, nodes/, do/ and alarm-wake/by-owner/
// (and check/, `coworkerd check`'s probes): '/'-separated segments of letters,
// digits, '.', '_' and '-', none of them "." or "..". Only the S3 backend keeps state: create, list and remove
// refuse the directory backend.
class ObjectStore {
public:
  ObjectStore(ObjectStoreConfig config, kj::Network& network, kj::Timer& timer);
  // Its kept connections and their sweeps hold `this`; one instance serves the thread that made it. Every promise it
  // returns (a request's coroutine frame) must be done or destroyed before the ObjectStore is: the frame uses its
  // connections, header table and statistics.
  KJ_DISALLOW_COPY_AND_MOVE(ObjectStore);
  ~ObjectStore() noexcept(false);
  kj::Promise<bool> exists(std::string key);
  // S3 object length from one HEAD; nullopt for 404. No body or listing is read.
  kj::Promise<std::optional<uint64_t>> headSize(std::string key);
  // An overwrite, on the lane's connections (the membership heartbeat's: ObjectStoreLane::HEARTBEAT).
  kj::Promise<ObjectResponse> put(std::string key, std::string bytes, ObjectStoreLane lane = ObjectStoreLane::SHARED);
  kj::Promise<ObjectResponse> get(std::string key);
  // PUT with `If-None-Match: *`: written only where the key does not exist. 412 (Precondition Failed) and 409 (a
  // conditional write to the same key in flight) both mean that it exists; any other status is an error. A create can
  // take effect while its answer is lost (R5: 20 of 20 under a network cut), and then a second try meets 412: when the
  // answer does not come back, the key is read back -- these very bytes there mean this create made it, which is why
  // every creator's bytes carry its own identity (a process instance and a random value of the write) -- and only an
  // absent key is created once more, on a new connection.
  kj::Promise<CreateResult> create(std::string key, std::string bytes, std::string payloadSha256 = {});
  // Every object whose key starts with `prefix` (ListObjectsV2, page after page until the store says that was the
  // last one); `maxKeys` bounds one page.
  kj::Promise<ObjectListing> list(std::string prefix, uint32_t maxKeys = 1000);
  kj::Promise<ObjectListingPage> listPage(std::string prefix, uint32_t maxKeys = 1000,
      std::string token = {}, std::string delimiter = {});
  // One LIST page with max-keys=1; does not follow continuation tokens.
  kj::Promise<bool> hasAny(std::string prefix);
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
    // Only headSize supplies this; existing operations do not parse length.
    std::optional<uint64_t>* headLength = nullptr;
    // SHA-256 of the exact bytes when the producer already computed it off the event loop.
    std::string payloadSha256;
    ObjectStoreLane lane = ObjectStoreLane::SHARED;
  };
  // A kept-alive connection: only its stream. Each request makes an HTTP client on it and destroys it after the whole
  // answer, so that no client reads the connection while it is idle (bytes the store sends unasked stay in the socket,
  // where the peek before the next request finds them).
  struct Connection {
    kj::Own<kj::AsyncIoStream> stream;
    kj::TimePoint idleSince = kj::origin<kj::TimePoint>();
  };
  struct Lane {
    ObjectStoreConnections* counts = nullptr;
    uint32_t keep = 0;
    // Oldest first; a request takes the one kept last.
    std::deque<kj::Own<Connection>> idle;
    bool sweeping = false;
    kj::Promise<void> sweep = kj::READY_NOW;
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
  kj::HttpHeaderId lastModifiedHeader;
  // After `table`: the kept clients read their answers' headers with it.
  Lane lanes[2];
  // One request, sent once more on a new connection when it is idempotent (GET, HEAD, a listing, DELETE) and the first
  // attempt failed; an overwrite is sent once. Errors never carry the request's URL (a presigned one holds the access
  // key's identifier).
  kj::Promise<ObjectResponse> send(Call call);
  // One attempt, every phase bounded (connect, write, answer, each read): on a kept connection of the call's lane
  // unless `fresh` (a second attempt) or none is young enough for it, else on a new one, with an HTTP client of its
  // own. Kept again only after its whole answer was read and its client destroyed; any failure on it closes it.
  kj::Promise<ObjectResponse> exchange(const Call& call, bool fresh = false);
  // The newest kept connection of the lane that is younger than `maxIdleMs` and still open, after closing the ones
  // past the lane's limit.
  kj::Maybe<kj::Own<Connection>> takeKept(Lane& lane, uint32_t maxIdleMs);
  void keep(Lane& lane, kj::Own<Connection> connection);
  void closeExpired(Lane& lane);
  kj::Promise<void> sweepKept(Lane& lane);
  kj::Exception sanitized(const kj::Exception& error);
};

std::string objectSha256(const std::string& bytes);
// The code of an error answer's body (<Error><Code>AccessDenied</Code>...), "" when it has none; never the rest of it (a
// signature mismatch echoes the canonical request, with the access key's identifier).
std::string objectErrorCode(const std::string& body);
// "Sun, 06 Nov 1994 08:49:37 GMT" (an IMF-fixdate) as Unix milliseconds; 0 when it does not read as one. A header
// that came twice reaches here joined with ", "; the first date counts.
uint64_t httpDateUnixMs(std::string_view text);
// "2026-10-24T08:49:37.000Z" (a listing's LastModified) as Unix milliseconds; 0 when it does not read as one.
uint64_t isoTimeUnixMs(std::string_view text);
}  // namespace coworkerd

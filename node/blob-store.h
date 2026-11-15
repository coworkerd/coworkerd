#pragma once

#include "object-store.h"

#include <compare>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace coworkerd {

// Only the trusted node dispatcher supplies targets, limits and confirmed positions.
// One current owner uses an instance on one KJ event loop; outstanding calls must finish before destruction.
class NodeBlobStore final {
public:
  enum class Kind { KV, R2 };
  struct Limits {
    std::optional<uint64_t> liveBytes, garbageBytes;
  };
  struct Target {
    std::string tenant, namespaceId, objectId;
    Kind kind = Kind::KV;
    Limits limits;
  };
  struct Position {
    uint64_t shardEpoch = 0, nodeSeq = 0;
    auto operator<=>(const Position&) const = default;
  };
  struct Settings {
    uint64_t retentionMs = 2 * 60 * 60 * 1000;
    uint64_t defaultLiveBytes = 10ull << 30;
    // Unset: the effective live limit, including a target's override.
    std::optional<uint64_t> defaultGarbageBytes;
  };
  static constexpr uint64_t MAX_BLOB_BYTES = 25ull << 20;
  struct Range { uint64_t offset = 0, length = 0; };
  struct Reply {
    uint status = 200;
    std::string code, body;
    uint64_t length = 0, objectBytes = 0;
    // The complete immutable blob's digest, also when body is only a range.
    std::string sha256;
  };
  struct Request {
    enum class Method { GET, PUT, DELETE };
    Method method = Method::GET;
    std::string name, body;
    std::optional<uint64_t> knownLength;
    std::optional<Range> range;
    std::optional<Position> dereferencedAt;
  };
  struct Usage {
    uint64_t liveBytes = 0, garbageBytes = 0;
    bool operator==(const Usage&) const = default;
  };
  struct Sweep {
    uint64_t blobsRemoved = 0, markersRemoved = 0, deferred = 0;
  };
  // Unknown publication must return nullopt, never a guessed sequence. Positions describe the same object's
  // published history, ordered by shard epoch then node sequence, including snapshot coverage.
  using Published = std::function<kj::Promise<std::optional<Position>>(const Target&)>;
  using Clock = std::function<uint64_t()>;

  NodeBlobStore(ObjectStore& store, Settings settings, Published published, Clock clock);
  ~NodeBlobStore() noexcept;
  NodeBlobStore(const NodeBlobStore&) = delete;
  NodeBlobStore& operator=(const NodeBlobStore&) = delete;

  // Values are owned across suspension. Length and range are separate from the payload so a future stream
  // overload can keep the same admission and metadata contract. Names are immutable; callers use fresh names.
  kj::Promise<Reply> put(Target target, std::string name, uint64_t knownLength, std::string body);
  kj::Promise<Reply> get(Target target, std::string name, std::optional<Range> range = std::nullopt);
  kj::Promise<Reply> erase(Target target, std::string name, Position dereferencedAt);
  kj::Promise<Reply> handle(Target target, Request request);
  kj::Promise<Usage> usage(Target target);
  kj::Promise<Usage> rebuild(Target target);
  kj::Promise<Sweep> sweep(Target target);

  static bool validName(std::string_view name);
  static bool validTarget(const Target& target);

private:
  struct Impl;
  std::unique_ptr<Impl> impl;
};

}  // namespace coworkerd

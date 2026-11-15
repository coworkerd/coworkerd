#include "blob-store.h"
#include "json.h"

#include <capnp/message.h>
#include <kj/debug.h>

#include <algorithm>
#include <deque>
#include <limits>
#include <map>

namespace coworkerd {
namespace {
using Target = NodeBlobStore::Target;
using Position = NodeBlobStore::Position;
using Reply = NodeBlobStore::Reply;
bool word(std::string_view value) {
  return !value.empty() && value != "." && value != ".." &&
      std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
      });
}
bool id(std::string_view value) {
  return value.size() == 64 && std::all_of(value.begin(), value.end(), [](char c) {
    return (c >= 'a' && c <= 'f') || (c >= '0' && c <= '9');
  });
}
const char* kindName(NodeBlobStore::Kind kind) { return kind == NodeBlobStore::Kind::KV ? "kv" : "r2"; }
std::string prefix(const Target& target) {
  KJ_REQUIRE(NodeBlobStore::validTarget(target), "invalid blob target");
  return "do/" + target.namespaceId + "/objects/" + target.objectId + "/" + kindName(target.kind) + "/";
}
Reply status(uint code, std::string reason = {}) {
  Reply out; out.status = code; out.code = std::move(reason); return out;
}
Reply metadata(uint code, const std::string& bytes) {
  Reply out = status(code);
  out.length = out.objectBytes = bytes.size(); out.sha256 = objectSha256(bytes);
  return out;
}
void checkRead(const ObjectResponse& answer) {
  requireStorage(answer.status == 200 || answer.status == 404, "blob object could not be read");
}
uint64_t add(uint64_t a, uint64_t b) {
  KJ_REQUIRE(b <= std::numeric_limits<uint64_t>::max() - a, "blob byte/time counter overflow");
  return a + b;
}
struct Marker {
  std::string name;
  Position position;
  uint64_t notBeforeUnixMs = 0;
};
std::string markerJson(const Target& target, const Marker& marker) {
  return "{\"version\":1,\"namespaceId\":" + json::quote(target.namespaceId) +
      ",\"objectId\":" + json::quote(target.objectId) + ",\"kind\":" + json::quote(kindName(target.kind)) +
      ",\"name\":" + json::quote(marker.name) + ",\"shardEpoch\":\"" + std::to_string(marker.position.shardEpoch) +
      "\",\"nodeSeq\":\"" + std::to_string(marker.position.nodeSeq) + "\",\"notBeforeUnixMs\":\"" +
      std::to_string(marker.notBeforeUnixMs) + "\"}\n";
}
Marker parseMarker(const Target& target, const std::string& name, const std::string& bytes) {
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
    auto reader = value.asReader();
    auto version = json::field(reader, "version");
    KJ_REQUIRE(version.isNumber() && version.getNumber() == 1 &&
        json::text(reader, "namespaceId") == target.namespaceId && json::text(reader, "objectId") == target.objectId &&
        json::text(reader, "kind") == kindName(target.kind) && json::text(reader, "name") == name &&
        NodeBlobStore::validName(name), "invalid blob trash marker");
    for (const auto* field : {"shardEpoch", "nodeSeq", "notBeforeUnixMs"})
      KJ_REQUIRE(json::field(reader, field).isString(), "missing blob trash barrier");
    return {name, {json::integer(reader, "shardEpoch"), json::integer(reader, "nodeSeq")},
        json::integer(reader, "notBeforeUnixMs")};
  } catch (const kj::Exception&) {
    failStorage("invalid blob trash marker; no object was reclaimed");
  } catch (const std::exception&) {
    failStorage("invalid blob trash integer; no object was reclaimed");
  }
}
}  // namespace

struct NodeBlobStore::Impl {
  struct Turn;
  struct State {
    std::string tenant;
    bool loaded = false, busy = false;
    std::map<std::string, uint64_t> blobs;
    std::map<std::string, Marker> trash;
    std::deque<kj::Own<kj::PromiseFulfiller<kj::Own<Turn>>>> waiting;
    Usage totals() const {
      Usage out;
      for (const auto& [name, bytes] : blobs) {
        auto& total = trash.contains(name) ? out.garbageBytes : out.liveBytes;
        total = add(total, bytes);
      }
      return out;
    }
  };
  struct Turn {
    State& state;
    explicit Turn(State& state): state(state) {}
    ~Turn() noexcept(false) {
      while (!state.waiting.empty()) {
        auto next = kj::mv(state.waiting.front()); state.waiting.pop_front();
        if (next->isWaiting()) { next->fulfill(kj::heap<Turn>(state)); return; }
      }
      state.busy = false;
    }
  };
  ObjectStore& store;
  Settings settings;
  Published published;
  Clock clock;
  std::map<std::string, std::unique_ptr<State>> states;

  Impl(ObjectStore& store, Settings settings, Published published, Clock clock)
      : store(store), settings(settings), published(std::move(published)), clock(std::move(clock)) {
    KJ_REQUIRE(bool(this->published) && bool(this->clock), "blob publication and clock callbacks are required");
  }
  State& state(const Target& target) {
    auto& entry = states[prefix(target)];
    if (!entry) { entry = std::make_unique<State>(); entry->tenant = target.tenant; }
    KJ_REQUIRE(entry->tenant == target.tenant, "a blob target cannot change tenants");
    return *entry;
  }
  kj::Promise<kj::Own<Turn>> turn(State& state) {
    if (!state.busy) { state.busy = true; return kj::heap<Turn>(state); }
    auto pair = kj::newPromiseAndFulfiller<kj::Own<Turn>>();
    state.waiting.push_back(kj::mv(pair.fulfiller));
    return kj::mv(pair.promise);
  }
  kj::Promise<ObjectListing> list(std::string wanted) {
    KJ_REQUIRE(!wanted.empty(), "blob listing requires an explicit prefix");
    return store.list(std::move(wanted));
  }
  kj::Promise<void> load(const Target& target, State& state) {
    if (state.loaded) co_return;
    const auto root = prefix(target), bodies = root + "b/", markers = root + "trash/";
    std::map<std::string, uint64_t> blobs;
    std::map<std::string, Marker> trash;
    auto listedBlobs = co_await list(bodies);
    for (const auto& item : listedBlobs.objects) {
      requireStorage(item.key.starts_with(bodies), "blob listing escaped its prefix");
      auto name = item.key.substr(bodies.size());
      requireStorage(validName(name), "blob listing contains an invalid relative name");
      blobs.emplace(std::move(name), item.size);
    }
    auto listedMarkers = co_await list(markers);
    for (const auto& item : listedMarkers.objects) {
      requireStorage(item.key.starts_with(markers), "trash listing escaped its prefix");
      auto name = item.key.substr(markers.size());
      requireStorage(validName(name), "trash listing contains an invalid relative name");
      auto answer = co_await store.get(item.key); checkRead(answer);
      if (answer.status == 200) trash.emplace(name, parseMarker(target, name, answer.bytes));
    }
    state.blobs = std::move(blobs); state.trash = std::move(trash); state.loaded = true;
  }
};

NodeBlobStore::NodeBlobStore(ObjectStore& store, Settings settings, Published published, Clock clock)
    : impl(std::make_unique<Impl>(store, settings, std::move(published), std::move(clock))) {}
NodeBlobStore::~NodeBlobStore() noexcept = default;

bool NodeBlobStore::validName(std::string_view name) {
  // Full store keys are never relative names, including another object's otherwise valid key.
  if (name.empty() || name.starts_with("do/")) return false;
  size_t start = 0;
  while (true) {
    const auto slash = name.find('/', start);
    if (!word(name.substr(start, slash == std::string_view::npos ? slash : slash - start))) return false;
    if (slash == std::string_view::npos) return true;
    start = slash + 1;
  }
}
bool NodeBlobStore::validTarget(const Target& target) {
  return word(target.tenant) && id(target.namespaceId) && id(target.objectId) &&
      (target.kind == Kind::KV || target.kind == Kind::R2);
}

kj::Promise<NodeBlobStore::Reply> NodeBlobStore::put(Target target, std::string name, uint64_t length, std::string body) {
  if (!validTarget(target) || !validName(name)) co_return status(400, "INVALID_BLOB_NAME_OR_TARGET");
  if (length > MAX_BLOB_BYTES || body.size() > MAX_BLOB_BYTES) co_return status(413, "BLOB_TOO_LARGE");
  if (body.size() != length) co_return status(400, "BLOB_LENGTH_MISMATCH");
  auto& state = impl->state(target);
  auto held = co_await impl->turn(state);
  // Invalidate after cancellation or uncertain I/O too: a create may have taken effect without its answer.
  bool complete = false;
  auto invalidate = kj::defer([&]() { if (!complete) state.loaded = false; });
  const auto root = prefix(target), key = root + "b/" + name;
  auto trash = co_await impl->store.get(root + "trash/" + name); checkRead(trash);
  if (trash.status == 200) { complete = true; co_return status(409, "BLOB_DELETED"); }
  auto old = co_await impl->store.get(key); checkRead(old);
  if (old.status == 200) {
    complete = true;
    co_return old.bytes == body ? metadata(200, body) : status(409, "BLOB_CONFLICT");
  }
  co_await impl->load(target, state);
  const auto used = state.totals();
  const auto live = target.limits.liveBytes.value_or(impl->settings.defaultLiveBytes);
  const auto garbage = target.limits.garbageBytes.value_or(impl->settings.defaultGarbageBytes.value_or(live));
  if (used.liveBytes > live || length > live - used.liveBytes || used.garbageBytes >= garbage) {
    complete = true; co_return status(422, "BLOB_QUOTA_EXCEEDED");
  }
  auto made = co_await impl->store.create(key, body);
  if (!made.created) {
    auto back = co_await impl->store.get(key); checkRead(back);
    requireStorage(back.status == 200, "a conflicting blob could not be read back");
    state.blobs[name] = back.bytes.size();
    complete = true;
    co_return back.bytes == body ? metadata(200, body) : status(409, "BLOB_CONFLICT");
  }
  state.blobs[name] = length; complete = true;
  co_return metadata(201, body);
}

kj::Promise<NodeBlobStore::Reply> NodeBlobStore::get(Target target, std::string name, std::optional<Range> range) {
  if (!validTarget(target) || !validName(name)) co_return status(400, "INVALID_BLOB_NAME_OR_TARGET");
  if (range && range->length == 0) co_return status(400, "INVALID_BLOB_RANGE");
  auto& state = impl->state(target);
  auto held = co_await impl->turn(state);
  const auto root = prefix(target);
  auto trash = co_await impl->store.get(root + "trash/" + name); checkRead(trash);
  if (trash.status == 200) co_return status(404, "BLOB_NOT_FOUND");
  auto answer = co_await impl->store.get(root + "b/" + name); checkRead(answer);
  if (answer.status == 404) co_return status(404, "BLOB_NOT_FOUND");
  // The existing client downloads at most 64 MiB; this component currently downloads a whole blob for a range.
  requireStorage(answer.bytes.size() <= MAX_BLOB_BYTES, "stored blob exceeds the component limit");
  if (range && range->offset >= answer.bytes.size()) co_return status(416, "BLOB_RANGE_NOT_SATISFIABLE");
  auto reply = metadata(range ? 206 : 200, answer.bytes);
  if (range) {
    reply.length = std::min<uint64_t>(range->length, answer.bytes.size() - range->offset);
    reply.body = answer.bytes.substr(size_t(range->offset), size_t(reply.length));
  } else reply.body = std::move(answer.bytes);
  co_return reply;
}

kj::Promise<NodeBlobStore::Reply> NodeBlobStore::erase(Target target, std::string name, Position position) {
  if (!validTarget(target) || !validName(name)) co_return status(400, "INVALID_BLOB_NAME_OR_TARGET");
  auto& state = impl->state(target);
  auto held = co_await impl->turn(state);
  bool complete = false;
  auto invalidate = kj::defer([&]() { if (!complete) state.loaded = false; });
  const auto key = prefix(target) + "trash/" + name;
  co_await impl->load(target, state);
  auto old = co_await impl->store.get(key); checkRead(old);
  if (old.status == 200) {
    state.trash[name] = parseMarker(target, name, old.bytes); complete = true; co_return status(204);
  }
  Marker marker{name, position, add(impl->clock(), impl->settings.retentionMs)};
  auto made = co_await impl->store.create(key, markerJson(target, marker));
  if (!made.created) {
    auto back = co_await impl->store.get(key); checkRead(back);
    requireStorage(back.status == 200, "an existing blob trash marker could not be read back");
    marker = parseMarker(target, name, back.bytes);
  }
  state.trash[name] = std::move(marker); complete = true;
  co_return status(204);
}

kj::Promise<NodeBlobStore::Usage> NodeBlobStore::usage(Target target) {
  auto& state = impl->state(target); auto held = co_await impl->turn(state);
  co_await impl->load(target, state);
  co_return state.totals();
}
kj::Promise<NodeBlobStore::Usage> NodeBlobStore::rebuild(Target target) {
  auto& state = impl->state(target); auto held = co_await impl->turn(state);
  state.loaded = false; co_await impl->load(target, state);
  co_return state.totals();
}
kj::Promise<NodeBlobStore::Sweep> NodeBlobStore::sweep(Target target) {
  auto& state = impl->state(target); auto held = co_await impl->turn(state);
  state.loaded = false; co_await impl->load(target, state);
  auto published = co_await impl->published(target);
  const auto now = impl->clock();
  const auto root = prefix(target);
  Sweep result;
  for (auto entry = state.trash.begin(); entry != state.trash.end();) {
    const auto& marker = entry->second;
    if (!published || *published < marker.position || now < marker.notBeforeUnixMs) {
      ++result.deferred; ++entry; continue;
    }
    auto removed = co_await impl->store.remove(root + "b/" + marker.name);
    requireStorage(removed.status == 200 || removed.status == 204 || removed.status == 404,
        "a garbage blob could not be removed");
    state.blobs.erase(marker.name); ++result.blobsRemoved;
    auto unmarked = co_await impl->store.remove(root + "trash/" + marker.name);
    requireStorage(unmarked.status == 200 || unmarked.status == 204 || unmarked.status == 404,
        "a completed blob trash marker could not be removed");
    entry = state.trash.erase(entry); ++result.markersRemoved;
  }
  co_return result;
}

kj::Promise<NodeBlobStore::Reply> NodeBlobStore::handle(Target target, Request request) {
  try {
    switch (request.method) {
      case Request::Method::GET: co_return co_await get(std::move(target), std::move(request.name), request.range);
      case Request::Method::PUT:
        if (!request.knownLength) co_return status(411, "BLOB_LENGTH_REQUIRED");
        co_return co_await put(std::move(target), std::move(request.name), *request.knownLength, std::move(request.body));
      case Request::Method::DELETE:
        if (!request.dereferencedAt) co_return status(400, "BLOB_POSITION_REQUIRED");
        co_return co_await erase(std::move(target), std::move(request.name), *request.dereferencedAt);
    }
    co_return status(405, "BLOB_METHOD_NOT_ALLOWED");
  } catch (const kj::Exception&) {
    co_return status(503, "STORAGE_UNAVAILABLE");
  }
}
}  // namespace coworkerd

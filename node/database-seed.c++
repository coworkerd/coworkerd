#include "database-seed.h"
#include "deployment.h"
#include "json.h"
#include "support.h"
#include "tenant-manifest.h"
#include <cmath>
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace coworkerd {
namespace {
using json::quote;
bool identifier(const std::string& text) {
  return text.size() == 64 && std::all_of(text.begin(), text.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}
uint64_t indexInteger(capnp::JsonValue::Reader object, const char* field) {
  auto value = json::field(object, field);
  if (!value.isNumber()) return json::integer(object, field);
  const double n = value.getNumber(); KJ_REQUIRE(std::isfinite(n) && n >= 0 && n < double(UINT64_MAX) && std::floor(n) == n, "invalid index integer"); return uint64_t(n);
}
uint64_t sourcePartBytes(const std::string& bytes, const ObjectManifest& manifest) {
  KJ_REQUIRE(bytes.size() <= (4u << 20), "snapshot index exceeds metadata budget");
  capnp::MallocMessageBuilder message; capnp::JsonCodec codec;
  codec.decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), message.initRoot<capnp::JsonValue>()); auto root = message.getRoot<capnp::JsonValue>();
  KJ_REQUIRE(indexInteger(root, "sequence") == manifest.snapshotSequence && indexInteger(root, "bytes") == manifest.snapshotBytes,
      "snapshot index identity or size differs from frozen manifest");
  const auto partBytes = indexInteger(root, "partBytes");
  KJ_REQUIRE(partBytes > 0 && partBytes <= (64u << 20), "invalid snapshot partition size");
  auto facets = json::field(root, "facets").getArray(); KJ_REQUIRE(facets.size() == manifest.facets.size(), "snapshot facet set differs");
  for (size_t i = 0; i < facets.size(); ++i) {
    auto value = facets[i]; const auto& facet = manifest.facets[i];
    KJ_REQUIRE(indexInteger(value, "facet") == facet.facetId && indexInteger(value, "parent") == facet.parentFacetId &&
        json::text(value, "name") == facet.name && indexInteger(value, "bytes") == facet.bytes &&
        indexInteger(value, "parts") == facet.parts && indexInteger(value, "upToSeq") == facet.upToSeq &&
        json::text(value, "sha256") == facet.sha256 && facet.parts == (facet.bytes + partBytes - 1) / partBytes,
        "snapshot index and frozen manifest disagree");
  }
  return partBytes;
}
}

std::string DatabaseSeedInput::json() const {
  return "{\"source\":{\"namespaceId\":" + quote(sourceNamespace) + ",\"objectId\":" + quote(sourceObject) +
      ",\"revision\":\"" + std::to_string(sourceRevision) + "\",\"sha256\":" + quote(sourceSha256) +
      "},\"destination\":{\"namespaceId\":" + quote(namespaceId) + ",\"objectId\":" + quote(objectId) + ",\"tenant\":" + quote(tenant) + "}}";
}
DatabaseSeedInput DatabaseSeedInput::parse(const std::string& bytes) {
  capnp::MallocMessageBuilder message; capnp::JsonCodec codec;
  codec.decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), message.initRoot<capnp::JsonValue>()); auto root = message.getRoot<capnp::JsonValue>();
  auto source = json::field(root, "source"), destination = json::field(root, "destination"); DatabaseSeedInput input;
  input.sourceNamespace = json::text(source, "namespaceId"); input.sourceObject = json::text(source, "objectId");
  input.sourceRevision = json::integer(source, "revision"); input.sourceSha256 = json::text(source, "sha256");
  input.namespaceId = json::text(destination, "namespaceId"); input.objectId = json::text(destination, "objectId"); input.tenant = json::text(destination, "tenant");
  KJ_REQUIRE(identifier(input.sourceNamespace) && identifier(input.sourceObject) && identifier(input.sourceSha256) &&
      identifier(input.namespaceId) && identifier(input.objectId) && input.sourceRevision && tenantName(input.tenant) &&
      input.sourceNamespace != input.namespaceId, "invalid frozen database source or new destination");
  KJ_REQUIRE(input.sourceObject == durableObjectIdFromName(input.sourceNamespace, DATABASE_OBJECT_NAME) &&
      input.objectId == durableObjectIdFromName(input.namespaceId, DATABASE_OBJECT_NAME), "database root identity differs");
  return input;
}
std::string DatabaseSeed::Job::json() const {
  std::string fsync = "[";
  for (size_t i = 0; i < downloadFsyncMs.size(); ++i) fsync += (i ? "," : "") + std::to_string(downloadFsyncMs[i]);
  fsync += "]";
  std::string replaySync = "[";
  for (size_t i = 0; i < replayFsyncMs.size(); ++i) replaySync += (i ? "," : "") + std::to_string(replayFsyncMs[i]);
  replaySync += "]";
  return "{\"known\":true,\"input\":" + input.json() + ",\"phase\":" + quote(phase) + ",\"finished\":" +
      (finished ? "true" : "false") + ",\"bytes\":\"" + std::to_string(bytes) + "\",\"records\":\"" + std::to_string(budget.records) +
      "\",\"payloadBytes\":\"" + std::to_string(budget.payloadBytes) + "\",\"memoryBoundBytes\":\"" + std::to_string(budget.memoryHighWater) +
      "\",\"maxLoopDelayMs\":" + std::to_string(maxLoopDelayMs) + ",\"downloadFsyncMs\":" + fsync + ",\"replayFsyncMs\":" + replaySync + ",\"errorCode\":" + quote(errorCode) + ",\"error\":" + quote(error) + ",\"resource\":" + quote(resource) +
      ",\"required\":\"" + std::to_string(required) + "\",\"limit\":\"" + std::to_string(limit) + "\",\"result\":" + (result.empty() ? "null" : result) + "}";
}
DatabaseSeed::DatabaseSeed(DurableLog& log, DurableShards& shards, std::function<bool(const std::string&)> registered)
    : log(log), shards(shards), registered(std::move(registered)) {}
kj::Promise<ShardRoute> DatabaseSeed::route(const std::string& namespaceId) {
  KJ_REQUIRE(identifier(namespaceId), "invalid destination namespace");
  auto owner = co_await shards.route(namespaceId, 0);
  if (owner.kind == ShardRoute::Kind::FORWARD && owner.address.empty()) {
    auto member = log.membership.members.find(owner.nodeId);
    if (member == log.membership.members.end()) co_return ShardRoute::refuse(503, "OWNER_UNAVAILABLE", "the target owner left membership");
    owner.address = member->second.address;
  }
  co_return owner;
}
std::string DatabaseSeed::start(DatabaseSeedInput input, const ShardRoute& owner) {
  KJ_REQUIRE(owner.kind == ShardRoute::Kind::LOCAL && log.enabled(), "database seed requires the current target owner and object store");
  if (auto found = jobs.find(input.namespaceId); found != jobs.end()) {
    if (!found->second->finished) {
      KJ_REQUIRE(found->second->input.json() == input.json(), "destination already has a different seed request");
      return found->second->json();
    }
    // Even a cached success must check immutable history again: the database
    // may have been used since it was seeded. A failed job may use a new B.
    // Retain this result if a different target prevents accepting the retry.
  }
  KJ_REQUIRE(!running, "another database seed is in progress on this node");
  jobs.erase(input.namespaceId);
  if (jobs.size() >= 128) {
    auto old = std::find_if(jobs.begin(), jobs.end(), [](const auto& entry) { return entry.second->finished; });
    if (old != jobs.end()) jobs.erase(old);
  }
  auto job = std::make_shared<Job>(); job->input = std::move(input); job->owner = owner; job->nodeEpoch = log.nodeEpoch;
  job->budget.refuse = [job = job.get()](uint64_t required, uint64_t limit, const std::string& resource) {
    job->required = required; job->limit = limit; job->resource = resource; job->errorCode = "SEED_RESOURCE_LIMIT";
  };
  jobs[job->input.namespaceId] = job; running = job;
  tasks.add(measureLoop(job));
  tasks.add(run(job).then([this, job]() { job->finished = true; running.reset(); }, [this, job](kj::Exception&& error) {
    job->phase = "failed"; job->finished = true; if (job->errorCode.empty()) job->errorCode = isStorageFailure(error) ? "STORAGE_UNAVAILABLE" : "SEED_FAILED";
    job->error = error.getDescription().cStr(); running.reset();
  }));
  return job->json();
}
kj::Promise<std::string> DatabaseSeed::progress(std::string namespaceId) {
  KJ_REQUIRE(identifier(namespaceId), "invalid destination namespace");
  auto found = jobs.find(namespaceId);
  if (found == jobs.end()) co_return "{\"known\":false}";
  auto job = found->second;
  if (job->phase == "completed") {
    auto next = co_await log.store->get(objectManifestKey(namespaceId, job->input.objectId, 2));
    requireStorage(next.status == 200 || next.status == 404, "cannot check whether the target is already in use");
    if (next.status == 200) {
      job->phase = "failed"; job->errorCode = "SEED_TARGET_IN_USE"; job->error = "destination already exists and is in use";
    }
  }
  co_return job->json();
}
kj::Promise<void> DatabaseSeed::measureLoop(std::shared_ptr<Job> job) {
  while (!job->finished) {
    const auto began = monotonicTimeMs();
    co_await log.timer.afterDelay(5 * kj::MILLISECONDS);
    job->maxLoopDelayMs = std::max(job->maxLoopDelayMs, monotonicTimeMs() - began - 5);
  }
}
void DatabaseSeed::taskFailed(kj::Exception&&) {}
void DatabaseSeed::check(const Job& job) const {
  KJ_REQUIRE(shards.unavailable().empty() && shards.holds(job.input.namespaceId, 0) &&
      shards.knownShardEpoch(job.input.namespaceId, 0) == job.owner.epoch && log.nodeEpoch == job.nodeEpoch &&
      !log.ownerFenced(job.input.namespaceId, 0, job.owner.epoch) && !log.recoveryWriteFailed,
      "destination ownership changed during database seed");
}
void DatabaseSeed::disk(Job& job, uint64_t imageBytes) const {
  const uint64_t reservation = imageBytes > UINT64_MAX / 2 ? UINT64_MAX : imageBytes * 2;
  if (reservation > log.diskLimitBytes || job.diskBase > log.diskLimitBytes - reservation || reservation > job.availableBytes) {
    job.errorCode = "SEED_RESOURCE_LIMIT"; job.resource = "diskBytes";
    job.required = job.diskBase > UINT64_MAX - reservation ? UINT64_MAX : job.diskBase + reservation;
    job.limit = std::min(log.diskLimitBytes, job.diskBase + job.availableBytes);
    KJ_FAIL_REQUIRE("database seed disk budget exceeded", job.required, job.limit);
  }

}

kj::Promise<void> DatabaseSeed::run(std::shared_ptr<Job> job) {
  const auto input = job->input; auto checkOwner = [this, job]() { check(*job); }; checkOwner();
  HistoryGet get = [this, job, checkOwner](std::string key) -> kj::Promise<ObjectResponse> {
    checkOwner(); auto response = co_await log.store->get(std::move(key)); checkOwner(); job->bytes += response.bytes.size(); co_return response;
  };
  job->phase = "reading-source"; auto raw = co_await get(objectManifestKey(input.sourceNamespace, input.sourceObject, input.sourceRevision));
  KJ_REQUIRE(raw.status == 200 && raw.bytes.size() <= (4u << 20) && objectSha256(raw.bytes) == input.sourceSha256, "frozen source manifest is absent or its digest differs");
  auto parsed = parseObjectManifest(raw.bytes); KJ_REQUIRE(parsed.has_value() && parsed->namespaceId == input.sourceNamespace && parsed->objectId == input.sourceObject &&
      parsed->revision == input.sourceRevision && parsed->shard == 0, "frozen source manifest identity differs"); const auto source = std::move(*parsed);
  KJ_REQUIRE(source.facets.size() <= 1 && (source.facets.empty() || (source.facets[0].facetId == 0 && source.facets[0].parentFacetId == 0)), "only root-object databases can be seeded");
  const auto destination = makeRootStorageRef(input.namespaceId, input.objectId, job->owner.dataEpoch);
  const auto staging = ".seed-" + std::to_string(job->nodeEpoch) + "-" + std::to_string(job->owner.epoch);
  const auto verifying = staging + "-verify"; const auto objectName = durableHostName(input.namespaceId, input.objectId, job->owner.dataEpoch);
  std::filesystem::create_directories(destination.directory(log.settings.stackDirectory));
  auto cleanup = kj::defer([&]() {
    std::error_code ignored;
    std::filesystem::remove(destination.databasePath(log.settings.stackDirectory, 0, "", staging), ignored);
    std::filesystem::remove(destination.databasePath(log.settings.stackDirectory, 0, "", verifying), ignored);
  });
  auto used = co_await get(objectManifestKey(input.namespaceId, input.objectId, 2));
  requireStorage(used.status == 200 || used.status == 404, "cannot check target use");
  if (used.status == 200) { job->errorCode = "SEED_TARGET_IN_USE"; KJ_FAIL_REQUIRE("destination already exists and is in use"); }
  job->phase = "reading-segments";
  auto records = co_await readRecoverySegments(get, source, input.sourceNamespace + "/" + input.sourceObject,
      1, checkOwner, {}, &job->budget, [this]() { return log.timer.afterDelay(0 * kj::MILLISECONDS); }); checkOwner();
  uint64_t imageBytes = source.snapshotBytes;
  for (const auto& record : records) KJ_REQUIRE(record.facetId == 0, "source history is not a root database");
  // Reserve before writing. The base is the cleaner's accounting (logs plus
  // tracked database/WAL files), sampled once after evicting eligible cold files.
  for (const auto& record : records) {
    const auto page = record.pageSize;
    if (record.kind != LogRecordKind::FRAMES || !page) continue;
    for (size_t at = 0; at + 8 <= record.frameBytes.size(); at += uint64_t(page) + 24) {
      const auto* bytes = reinterpret_cast<const unsigned char*>(record.frameBytes.data() + at);
      auto big = [](const unsigned char* p) { return uint32_t(p[0]) << 24 | uint32_t(p[1]) << 16 | uint32_t(p[2]) << 8 | p[3]; };
      imageBytes = std::max(imageBytes, uint64_t(std::max(big(bytes), big(bytes + 4))) * page);
    }
  }
  // A previous interrupted attempt may have left these two exact staging paths.
  for (const auto& suffix : {staging, verifying}) {
    std::error_code error; std::filesystem::remove(destination.databasePath(log.settings.stackDirectory, 0, "", suffix), error);
    KJ_REQUIRE(!error, "cannot remove prior seed staging");
  }
  const auto reservation = imageBytes > UINT64_MAX / 2 ? UINT64_MAX : imageBytes * 2;
  if (reservation > log.diskLimitBytes) {
    job->errorCode = "SEED_RESOURCE_LIMIT"; job->resource = "diskBytes"; job->required = reservation; job->limit = log.diskLimitBytes;
    KJ_FAIL_REQUIRE("database seed disk budget exceeded", job->required, job->limit);
  }
  co_await log.dropColdFiles(reservation); checkOwner();
  job->diskBase = log.diskUsedBytes; job->availableBytes = std::filesystem::space(log.settings.stackDirectory).available;
  job->imageBytes = imageBytes; disk(*job, imageBytes);
  log.seedReservedBytes = reservation; auto releaseDisk = kj::defer([&]() { log.seedReservedBytes = 0; });
  if (source.hasSnapshot) {
    auto index = co_await get(objectSnapshotIndexKey(source.namespaceId, source.objectId, source.snapshotSequence));
    KJ_REQUIRE(index.status == 200, "frozen source snapshot index is missing"); const auto partBytes = sourcePartBytes(index.bytes, source);
    job->phase = "downloading"; co_await downloadRecoverySnapshot(get, source, destination, log.settings.stackDirectory, staging, partBytes, 1, checkOwner, [this]() { return log.timer.afterDelay(0 * kj::MILLISECONDS); }, [job](double ms) { job->downloadFsyncMs.push_back(ms); }); checkOwner();
  }
  job->phase = "replaying";
  auto replayed = co_await log.replayToAsync(destination, input.sourceNamespace + "/" + input.sourceObject, source.shardEpoch,
      staging, std::move(records), source.facets, source.snapshotShardEpoch, {source.isolated.begin(), source.isolated.end()}, {}, checkOwner,
      [job](double ms) { job->replayFsyncMs.push_back(ms); });
  const auto path = destination.databasePath(log.settings.stackDirectory, 0, "", staging);
  if (!std::filesystem::exists(path)) { const int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600); KJ_REQUIRE(fd >= 0, "cannot create empty seed image"); ::close(fd); }
  const auto expected = co_await recoveryFileDigestAsync(path, [this]() { return log.timer.afterDelay(0 * kj::MILLISECONDS); }, checkOwner); job->imageBytes = expected.bytes; disk(*job, expected.bytes); checkOwner();
  auto existing = co_await get(objectManifestKey(input.namespaceId, input.objectId, 1));
  if (existing.status == 404) {
    KJ_REQUIRE(!registered(input.namespaceId), "destination namespace is already in the catalog");
    auto old = log.objects.find(objectName); KJ_REQUIRE(old == log.objects.end(), "destination already has local object state");
    DurableLog::Object object; static_cast<RootStorageRef&>(object) = destination; object.objectName = objectName; object.tenant = input.tenant;
    object.shardEpoch = job->owner.epoch; object.shard = 0; object.state = DurableLog::ObjectState::REBUILDING; object.snapshotVersion = 1; object.recoveryManifestRevision = 0;
    DurableLog::Facet facet; facet.facetId = 0; facet.databaseBytes = expected.bytes; object.facets.emplace(0, facet); object.bytes = expected.bytes;
    KJ_REQUIRE(!log.settings.config.objectBytes || expected.bytes <= log.settings.config.objectBytes, "seed image exceeds object byte limit");
    const auto quota = log.hooks.tenantQuotaBytes ? log.hooks.tenantQuotaBytes(input.tenant) : 0;
    KJ_REQUIRE(!quota || expected.bytes + log.tenantBytes(input.tenant) <= quota, "seed image exceeds tenant byte quota");
    log.objects.emplace(objectName, std::move(object));
    auto release = kj::defer([&]() {
      auto found = log.objects.find(objectName);
      if (found != log.objects.end() && !log.hasLiveHolders(found->second) && !log.hasLiveOpenFacets(found->second) &&
          found->second.state != DurableLog::ObjectState::LOCAL) log.objects.erase(found);
    });
    job->phase = "publishing";
    co_await log.forceRecoverySnapshot(objectName, false, false, staging, [&, checkOwner]() {
      checkOwner(); KJ_REQUIRE(!registered(input.namespaceId), "destination registered before seed completion");
    }); checkOwner();
    existing = co_await get(objectManifestKey(input.namespaceId, input.objectId, 1));
  } else KJ_REQUIRE(existing.status == 200, "destination manifest could not be checked");
  job->phase = "verifying"; auto manifest = parseObjectManifest(existing.bytes);
  KJ_REQUIRE(manifest.has_value() && manifest->namespaceId == input.namespaceId && manifest->objectId == input.objectId && manifest->revision == 1 && manifest->hasSnapshot &&
      manifest->segments.empty() && manifest->isolated.empty() && manifest->facets.size() == 1 && manifest->facets[0].facetId == 0 &&
      manifest->facets[0].upToSeq == 0 && manifest->facets[0].bytes == expected.bytes && manifest->facets[0].sha256 == expected.sha256,
      "destination manifest 1 conflicts with the frozen source image");
  auto index = co_await get(objectSnapshotIndexKey(input.namespaceId, input.objectId, manifest->snapshotSequence));
  KJ_REQUIRE(index.status == 200, "published destination snapshot index is absent"); const auto partBytes = sourcePartBytes(index.bytes, *manifest);
  co_await downloadRecoverySnapshot(get, *manifest, destination, log.settings.stackDirectory, verifying, partBytes, 1, checkOwner, [this]() { return log.timer.afterDelay(0 * kj::MILLISECONDS); }, [job](double ms) { job->downloadFsyncMs.push_back(ms); }); checkOwner();
  const auto verified = co_await recoveryFileDigestAsync(destination.databasePath(log.settings.stackDirectory, 0, "", verifying), [this]() { return log.timer.afterDelay(0 * kj::MILLISECONDS); }, checkOwner);
  KJ_REQUIRE(verified.sha256 == expected.sha256 && verified.bytes == expected.bytes, "published destination image differs");
  job->result = "{\"namespaceId\":" + quote(input.namespaceId) + ",\"objectId\":" + quote(input.objectId) + ",\"revision\":\"1\",\"manifestSha256\":" +
      quote(objectSha256(existing.bytes)) + ",\"bytes\":\"" + std::to_string(expected.bytes) + "\",\"sha256\":" + quote(expected.sha256) + "}"; job->phase = "completed";
}

DatabaseSeedService::DatabaseSeedService(kj::HttpService& next, DatabaseSeed& seed, kj::Network& network, kj::Timer& timer,
    const kj::HttpHeaderTable& headers, std::function<bool(kj::StringPtr, const kj::HttpHeaders&)> authorize)
    : next(next), seed(seed), network(network), timer(timer), headers(headers), authorize(std::move(authorize)) {}
kj::Promise<void> DatabaseSeedService::answer(Response& response, uint status, std::string text) {
  kj::HttpHeaders out(headers); out.set(kj::HttpHeaderId::CONTENT_TYPE, "application/json"); auto stream = response.send(status, "database seed", out, text.size());
  co_await stream->write(kj::arrayPtr(reinterpret_cast<const kj::byte*>(text.data()), text.size()));
}
kj::Promise<void> DatabaseSeedService::request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& incoming,
    kj::AsyncInputStream& body, Response& response) {
  const std::string path(url.cStr()); constexpr const char* PREFIX = "/__coworkerd/database-seed/";
  if (!path.starts_with(PREFIX)) { co_await next.request(method, url, incoming, body, response); co_return; }
  if (!authorize(url, incoming)) { co_await answer(response, 401, "{\"code\":\"UNAUTHORIZED\"}"); co_return; }
  std::string error;
  try {
    DatabaseSeedInput input; std::string namespaceId, payload;
    if (method == kj::HttpMethod::POST && path == std::string(PREFIX) + "start") {
      auto bytes = co_await body.readAllText(16385); KJ_REQUIRE(bytes.size() <= 16384, "seed request exceeds 16 KiB");
      payload.assign(bytes.cStr(), bytes.size()); input = DatabaseSeedInput::parse(payload); namespaceId = input.namespaceId;
    } else if (method == kj::HttpMethod::GET && path.starts_with(std::string(PREFIX) + "progress/")) namespaceId = path.substr(std::string(PREFIX).size() + 9);
    else { co_await answer(response, 404, "{\"code\":\"NOT_FOUND\"}"); co_return; }
    if (method == kj::HttpMethod::GET) { co_await answer(response, 200, co_await seed.progress(namespaceId)); co_return; }
    auto owner = co_await seed.route(namespaceId);
    if (owner.kind == ShardRoute::Kind::REFUSE) { co_await answer(response, owner.status, "{\"code\":" + quote(owner.code) + ",\"reason\":" + quote(owner.reason) + "}"); co_return; }
    if (owner.kind == ShardRoute::Kind::FORWARD) {
      bool forwarded = false; incoming.forEach([&](kj::StringPtr name, kj::StringPtr) { if (name == "X-Coworkerd-Seed-Forwarded" || name == "x-coworkerd-seed-forwarded") forwarded = true; });
      if (forwarded) { co_await answer(response, 409, "{\"code\":\"OWNER_CHANGED\"}"); co_return; }
      auto client = kj::newHttpClient(timer, headers, network, kj::none); auto out = incoming.clone(); out.add("X-Coworkerd-Seed-Forwarded", "1");
      const auto address = owner.address.starts_with("http://") ? owner.address : "http://" + owner.address;
      auto call = client->request(method, (address + path).c_str(), out, payload.size());
      if (!payload.empty()) co_await call.body->write(kj::arrayPtr(reinterpret_cast<const kj::byte*>(payload.data()), payload.size()));
      call.body = nullptr; auto back = co_await call.response; auto text = co_await back.body->readAllText(65537); co_await answer(response, back.statusCode, std::string(text.cStr(), text.size())); co_return;
    }
    co_await answer(response, method == kj::HttpMethod::POST ? 202 : 200, seed.start(std::move(input), owner)); co_return;
  } catch (const kj::Exception& failure) { error = failure.getDescription().cStr(); }
  co_await answer(response, 409, "{\"code\":\"SEED_REFUSED\",\"reason\":" + quote(error) + "}");
}
kj::Promise<void> DatabaseSeedService::connect(kj::StringPtr host, const kj::HttpHeaders& incoming, kj::AsyncIoStream& connection,
    ConnectResponse& response, kj::HttpConnectSettings settings) { return next.connect(host, incoming, connection, response, settings); }
}  // namespace coworkerd

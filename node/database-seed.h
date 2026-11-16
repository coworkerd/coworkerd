#pragma once

#include "durable-log.h"
#include "durable-shards.h"
#include "recovery-history.h"
#include <kj/compat/http.h>

namespace coworkerd {

struct DatabaseSeedInput {
  std::string sourceNamespace, sourceObject, sourceSha256;
  uint64_t sourceRevision = 0;
  std::string namespaceId, objectId, tenant;
  std::string json() const;
  static DatabaseSeedInput parse(const std::string& json);
};

// Memory-only jobs. Completion is the destination's immutable manifest 1, not
// this cache, a local position file, or a persistent job/tenant status.
class DatabaseSeed final: private kj::TaskSet::ErrorHandler {
public:
  DatabaseSeed(DurableLog& log, DurableShards& shards, std::function<bool(const std::string&)> registered);
  kj::Promise<ShardRoute> route(const std::string& namespaceId);
  std::string start(DatabaseSeedInput input, const ShardRoute& route);
  kj::Promise<std::string> progress(std::string namespaceId);
private:
  struct Job {
    DatabaseSeedInput input;
    ShardRoute owner;
    std::string phase = "starting", errorCode, error, result;
    uint64_t nodeEpoch = 0, bytes = 0, required = 0, limit = 0, imageBytes = 0;
    uint64_t diskBase = 0, availableBytes = 0;
    double maxLoopDelayMs = 0;
    std::string resource;
    HistoryBudget budget;
    bool finished = false;
    std::string json() const;
  };
  kj::Promise<void> run(std::shared_ptr<Job> job);
  kj::Promise<void> measureLoop(std::shared_ptr<Job> job);
  void check(const Job& job) const;
  void disk(Job& job, uint64_t imageBytes) const;
  void taskFailed(kj::Exception&& error) override;
  DurableLog& log;
  DurableShards& shards;
  std::function<bool(const std::string&)> registered;
  std::map<std::string, std::shared_ptr<Job>> jobs;
  std::shared_ptr<Job> running;
  kj::TaskSet tasks{*this};
};

// Registered only on the management/peer listener. All other HTTP and CONNECT
// traffic is delegated unchanged; no tenant binding exposes these operations.
class DatabaseSeedService final: public kj::HttpService {
public:
  DatabaseSeedService(kj::HttpService& next, DatabaseSeed& seed, kj::Network& network, kj::Timer& timer,
      const kj::HttpHeaderTable& headers,
      std::function<bool(kj::StringPtr, const kj::HttpHeaders&)> authorize);
  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body, Response& response) override;
  kj::Promise<void> connect(kj::StringPtr host, const kj::HttpHeaders& headers, kj::AsyncIoStream& connection,
      ConnectResponse& response, kj::HttpConnectSettings settings) override;
private:
  kj::Promise<void> answer(Response& response, uint status, std::string json);
  kj::HttpService& next;
  DatabaseSeed& seed;
  kj::Network& network;
  kj::Timer& timer;
  const kj::HttpHeaderTable& headers;
  std::function<bool(kj::StringPtr, const kj::HttpHeaders&)> authorize;
};
}  // namespace coworkerd

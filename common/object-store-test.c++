// The object store client's kept-alive connections against a store of the test's own: a loopback HTTP/1.1 server that
// numbers its connections and answers, drops, holds, delays or closes each request as the case says.
#include "object-store.h"

#include <kj/debug.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace coworkerd {
namespace {

struct Seen {
  uint32_t connection = 0;
  std::string method, target, body;
  // The key: the target's path after the bucket, without the query.
  std::string key() const {
    auto path = target.substr(0, target.find('?'));
    const std::string bucket = "/coworkerd/";
    return path.starts_with(bucket) ? path.substr(bucket.size()) : path;
  }
};

class FakeStore final: private kj::TaskSet::ErrorHandler {
public:
  enum class Act {
    ANSWER,        // 200 (a GET: the key as its body)
    NOT_FOUND,     // 404
    ANSWER_CLOSE,  // 200 with Connection: close, then the store closes the connection
    CLOSE_AFTER,   // 200, then the store closes the connection without saying so
    DROP,          // the whole request read, the connection closed without an answer
    HOLD,          // answered once release() is called
    LATE,          // answered after lateMs
    EXTRA,         // 200, then at once a whole second answer nobody asked for; the connection stays open
    EXTRA_LATE,    // the same, the second answer 20 ms later
  };

  FakeStore(kj::AsyncIoContext& io): io(io), timer(io.provider->getTimer()) {
    listener = io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    accepting = acceptLoop().eagerlyEvaluate(nullptr);
  }

  ObjectStoreConfig config() {
    ObjectStoreConfig config;
    config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
    config.accessKey = "test-only";
    config.secretKey = "test-only";
    return config;
  }

  // Sends the store's FIN on a connection between two requests (a store's idle timeout), at once.
  void closeIdle(uint32_t connection) {
    auto found = open.find(connection);
    KJ_REQUIRE(found != open.end(), "no such open connection", connection);
    found->second->shutdownWrite();
  }
  void release() {
    auto waiting = kj::mv(held);
    held.clear();
    for (auto& fulfiller : waiting) fulfiller->fulfill();
  }

  std::function<Act(const Seen&)> decide;
  std::vector<Seen> seen;
  uint32_t connections = 0, closedByClient = 0;
  size_t holding() const { return held.size(); }
  uint32_t lateMs = 300;

  size_t count(std::string_view method) const {
    return std::count_if(seen.begin(), seen.end(), [&](const Seen& s) { return s.method == method; });
  }
  std::set<uint32_t> connectionsOf(std::function<bool(const Seen&)> which) const {
    std::set<uint32_t> result;
    for (const auto& s : seen) if (which(s)) result.insert(s.connection);
    return result;
  }

private:
  kj::AsyncIoContext& io;
  kj::Timer& timer;
  kj::Own<kj::ConnectionReceiver> listener;
  // Before `tasks`: the connections' coroutines use them until they are destroyed.
  std::map<uint32_t, kj::AsyncIoStream*> open;
  std::vector<kj::Own<kj::PromiseFulfiller<void>>> held;
  kj::TaskSet tasks{*this};
  kj::Promise<void> accepting = nullptr;

  void taskFailed(kj::Exception&&) override {}

  kj::Promise<void> acceptLoop() {
    while (true) {
      auto connection = co_await listener->accept();
      tasks.add(serve(kj::mv(connection), ++connections));
    }
  }

  static kj::Promise<void> write(kj::AsyncIoStream& stream, std::string text) {
    co_await stream.write(kj::arrayPtr(text.data(), text.size()).asBytes());
  }

  kj::Promise<void> answer(kj::AsyncIoStream& stream, const Seen& request, uint status, bool close) {
    const std::string body = status == 200 && request.method == "GET" ? request.key() : std::string();
    std::string text = "HTTP/1.1 " + std::to_string(status) + (status == 200 ? " OK" : " Not Found") +
        "\r\nContent-Length: " + std::to_string(body.size()) + (close ? "\r\nConnection: close" : "") + "\r\n\r\n";
    if (request.method != "HEAD") text += body;
    co_await write(stream, std::move(text));
  }

  kj::Promise<void> serve(kj::Own<kj::AsyncIoStream> stream, uint32_t id) {
    open[id] = stream.get();
    KJ_DEFER(open.erase(id));
    std::string buffer;
    char chunk[4096];
    while (true) {
      size_t end;
      while ((end = buffer.find("\r\n\r\n")) == std::string::npos) {
        size_t n = 0;
        try {
          n = co_await stream->tryRead(chunk, 1, sizeof chunk);
        } catch (...) {
          n = 0;
        }
        if (n == 0) {
          ++closedByClient;
          co_return;
        }
        buffer.append(chunk, n);
      }
      std::string head = buffer.substr(0, end);
      buffer.erase(0, end + 4);
      Seen request;
      request.connection = id;
      const auto line = head.substr(0, head.find("\r\n"));
      request.method = line.substr(0, line.find(' '));
      request.target = line.substr(request.method.size() + 1, line.rfind(' ') - request.method.size() - 1);
      std::string lower = head;
      std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
      size_t length = 0;
      if (auto at = lower.find("\r\ncontent-length:"); at != std::string::npos) {
        length = std::stoul(lower.substr(at + 17));
      }
      while (buffer.size() < length) {
        auto n = co_await stream->tryRead(chunk, 1, sizeof chunk);
        if (n == 0) co_return;
        buffer.append(chunk, n);
      }
      request.body = buffer.substr(0, length);
      buffer.erase(0, length);
      seen.push_back(request);
      const auto act = decide ? decide(request) : Act::ANSWER;
      switch (act) {
        case Act::ANSWER: co_await answer(*stream, request, 200, false); break;
        case Act::NOT_FOUND: co_await answer(*stream, request, 404, false); break;
        case Act::ANSWER_CLOSE: co_await answer(*stream, request, 200, true); co_return;
        case Act::CLOSE_AFTER: co_await answer(*stream, request, 200, false); co_return;
        case Act::DROP: co_return;
        case Act::HOLD: {
          auto pair = kj::newPromiseAndFulfiller<void>();
          held.push_back(kj::mv(pair.fulfiller));
          co_await pair.promise;
          co_await answer(*stream, request, 200, false);
          break;
        }
        case Act::LATE:
          co_await timer.afterDelay(lateMs * kj::MILLISECONDS);
          co_await answer(*stream, request, 200, false);
          break;
        case Act::EXTRA:
        case Act::EXTRA_LATE:
          co_await answer(*stream, request, 200, false);
          if (act == Act::EXTRA_LATE) co_await timer.afterDelay(20 * kj::MILLISECONDS);
          co_await write(*stream, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nextra");
          break;
      }
    }
  }
};

template <typename T>
T within(kj::AsyncIoContext& io, kj::Promise<T> promise, uint32_t ms = 5000) {
  return io.provider->getTimer().timeoutAfter(ms * kj::MILLISECONDS, kj::mv(promise)).wait(io.waitScope);
}
void pause(kj::AsyncIoContext& io, uint32_t ms) {
  io.provider->getTimer().afterDelay(ms * kj::MILLISECONDS).wait(io.waitScope);
}
template <typename T>
bool fails(kj::AsyncIoContext& io, kj::Promise<T> promise) {
  try {
    within(io, kj::mv(promise));
  } catch (const kj::Exception& error) {
    KJ_REQUIRE(isStorageFailure(error), "a store failure carries its detail", error.getDescription());
    return true;
  }
  return false;
}

// Requests in a row go on one connection; the stats and the process's totals say so.
void reuse() {
  auto io = kj::setupAsyncIo();
  FakeStore fake(io);
  ObjectStore store(fake.config(), io.provider->getNetwork(), io.provider->getTimer());
  const auto before = objectStoreProcessConnections();
  for (int i = 0; i < 20; ++i) {
    KJ_REQUIRE(within(io, store.get("do/a/" + std::to_string(i))).bytes == "do/a/" + std::to_string(i));
  }
  within(io, store.put("do/a/written", "bytes"));
  KJ_REQUIRE(within(io, store.exists("do/a/written")));
  within(io, store.remove("do/a/written"));
  KJ_REQUIRE(fake.connections == 1 && fake.seen.size() == 23, fake.connections, fake.seen.size());
  const auto& c = store.stats().connections;
  KJ_REQUIRE(c.created == 1 && c.reused == 22 && c.idle == 1 && c.inUse == 0 && c.discardedOnError == 0, c.json().c_str());
  // Several at once: one connection each, all kept (fewer than idleConnections).
  std::vector<kj::Promise<ObjectResponse>> many;
  for (int i = 0; i < 8; ++i) many.push_back(store.get("do/b/" + std::to_string(i)));
  for (auto& response : many) within(io, kj::mv(response));
  KJ_REQUIRE(c.created == 8 && c.reused == 23 && c.idle == 8 && c.inUse == 0, c.json().c_str());
  KJ_REQUIRE(fake.connections == 8);
  const auto json = store.stats().json();
  KJ_REQUIRE(json.find("\"connections\":{\"inUse\":0,\"idle\":8,\"created\":8,\"reused\":23,") != std::string::npos, json.c_str());
  // Another thread's instance: its own connections, counted in the process's totals. This loop serves the store
  // meanwhile.
  std::atomic<bool> done{false}, ok{false};
  std::thread worker([config = fake.config(), &done, &ok]() {
    try {
      auto io = kj::setupAsyncIo();
      ObjectStore threadStore(config, io.provider->getNetwork(), io.provider->getTimer());
      for (int i = 0; i < 3; ++i) threadStore.get("do/c/" + std::to_string(i)).wait(io.waitScope);
      ok = threadStore.stats().connections.created == 1 && threadStore.stats().connections.reused == 2;
    } catch (...) {}
    done = true;
  });
  for (int i = 0; i < 5000 && !done; ++i) pause(io, 1);
  worker.join();
  KJ_REQUIRE(ok, "the worker thread's instance kept its one connection");
  const auto after = objectStoreProcessConnections();
  KJ_REQUIRE(after.created - before.created == 9 && after.reused - before.reused == 25, after.json().c_str());
}

// An answer that says Connection: close, the store closing a kept connection (seen by the peek before anything is sent,
// or by KJ once the event loop got to it), unasked bytes: each ends the connection, and the next request -- an
// overwrite here, which is never sent twice -- goes on a new one and succeeds.
void broken() {
  auto io = kj::setupAsyncIo();
  FakeStore fake(io);
  ObjectStore store(fake.config(), io.provider->getNetwork(), io.provider->getTimer());
  const auto& c = store.stats().connections;
  fake.decide = [](const Seen& s) { return s.key() == "do/close" ? FakeStore::Act::ANSWER_CLOSE : FakeStore::Act::ANSWER; };
  within(io, store.get("do/close"));
  KJ_REQUIRE(c.closedByAnswer == 1 && c.idle == 0, c.json().c_str());
  within(io, store.put("do/after-close", "x"));
  KJ_REQUIRE(fake.connections == 2 && c.created == 2 && c.reused == 0, c.json().c_str());

  // The store closes the kept connection; the overwrite goes out in the same turn of the event loop, before KJ could
  // see the close: the peek finds it.
  within(io, store.get("do/first"));
  fake.closeIdle(fake.seen.back().connection);
  ::usleep(20 * 1000);  // the FIN arrives; the event loop does not run
  within(io, store.put("do/peeked", "y"));
  KJ_REQUIRE(c.closedByPeer == 1 && c.reusedWriteFailures == 0 && fake.count("PUT") == 2, c.json().c_str());
  KJ_REQUIRE(fake.seen.back().connection == fake.connections, "the overwrite went on a new connection");

  // The same, after the event loop ran (KJ saw the close as well).
  within(io, store.get("do/second"));
  fake.closeIdle(fake.seen.back().connection);
  pause(io, 50);
  within(io, store.put("do/after-loop", "z"));
  KJ_REQUIRE(c.closedByPeer == 2 && c.reusedWriteFailures == 0 && fake.count("PUT") == 3, c.json().c_str());
  KJ_REQUIRE(fake.seen.back().connection == fake.connections);

  // A store that answers, then closes without saying so.
  fake.decide = [](const Seen& s) { return s.key() == "do/silent" ? FakeStore::Act::CLOSE_AFTER : FakeStore::Act::ANSWER; };
  within(io, store.get("do/silent"));
  pause(io, 20);
  within(io, store.put("do/after-silent", "w"));
  KJ_REQUIRE(c.closedByPeer == 3 && c.reusedWriteFailures == 0, c.json().c_str());
  KJ_REQUIRE(store.stats().failures == 0 && store.stats().retries == 0, store.stats().json().c_str());

  // An overwrite the store refused (404): the store may not have read its whole body, so its connection is not kept.
  fake.decide = [](const Seen& s) { return s.key() == "do/refused" ? FakeStore::Act::NOT_FOUND : FakeStore::Act::ANSWER; };
  KJ_REQUIRE(fails(io, store.put("do/refused", "body")));
  const auto refused = fake.seen.back().connection;
  within(io, store.get("do/after-refused"));
  KJ_REQUIRE(fake.seen.back().connection != refused && c.closedByAnswer == 2, "a refused body's connection is not kept",
      c.json().c_str());
}

// The store sends a whole answer nobody asked for and keeps the connection open, right after an answer or a moment
// later: the next overwrite and the read after it each read their own answer (no client reads an idle connection; the
// peek finds bytes still in the socket, and bytes a client read ahead go with it).
void extraAnswer() {
  auto io = kj::setupAsyncIo();
  FakeStore fake(io);
  ObjectStore store(fake.config(), io.provider->getNetwork(), io.provider->getTimer());
  const auto& c = store.stats().connections;
  for (const auto act : {FakeStore::Act::EXTRA, FakeStore::Act::EXTRA_LATE}) {
    bool once = true;
    fake.decide = [&](const Seen& s) {
      if (s.key() == "do/extra" && once) { once = false; return act; }
      return FakeStore::Act::ANSWER;
    };
    KJ_REQUIRE(within(io, store.get("do/extra")).bytes == "do/extra");
    pause(io, 50);
    const auto put = within(io, store.put("do/after-extra", "x"));
    KJ_REQUIRE(put.bytes.empty(), "the overwrite read its own answer", put.bytes.c_str());
    const auto next = within(io, store.get("do/next")).bytes;
    KJ_REQUIRE(next == "do/next", "the read after it read its own answer", next.c_str());
  }
  KJ_REQUIRE(c.reusedWriteFailures == 0 && c.reusedReadFailures == 0 && store.stats().failures == 0, c.json().c_str());
}

// The retry rules on kept connections, unchanged: a read that fails is sent once more, on a new connection (not on
// another kept one); an overwrite that fails is never sent again; a create whose answer was lost reads the key back
// and creates once more, on a new connection, only when it is absent.
void retryRules() {
  auto io = kj::setupAsyncIo();
  FakeStore fake(io);
  ObjectStore store(fake.config(), io.provider->getNetwork(), io.provider->getTimer());
  const auto& c = store.stats().connections;
  // Two kept connections (1 and 2).
  {
    fake.decide = [](const Seen&) { return FakeStore::Act::HOLD; };
    auto one = store.get("do/one");
    auto two = store.get("do/two");
    while (fake.holding() < 2) pause(io, 1);
    fake.decide = nullptr;
    fake.release();
    within(io, kj::mv(one));
    within(io, kj::mv(two));
    KJ_REQUIRE(c.idle == 2 && fake.connections == 2, c.json().c_str());
  }
  // The store drops every request on those two: the read is sent once more on a new connection.
  fake.decide = [](const Seen& s) { return s.connection <= 2 ? FakeStore::Act::DROP : FakeStore::Act::ANSWER; };
  KJ_REQUIRE(within(io, store.get("do/read")).bytes == "do/read");
  KJ_REQUIRE(store.stats().retries == 1 && c.reusedReadFailures == 1 && fake.count("GET") == 4, store.stats().json().c_str());
  KJ_REQUIRE(fake.seen.back().connection == 3, "the second attempt went on a new connection", fake.seen.back().connection);

  // An overwrite on the kept connection 3, dropped: it fails, and the store saw it once.
  KJ_REQUIRE(c.idle == 2, c.json().c_str());  // 1 or 2, and 3
  fake.decide = [](const Seen& s) { return s.method == "PUT" ? FakeStore::Act::DROP : FakeStore::Act::ANSWER; };
  KJ_REQUIRE(fails(io, store.put("do/overwrite", "once")));
  KJ_REQUIRE(fake.count("PUT") == 1 && c.reusedWriteFailures == 1 && store.stats().retries == 1, store.stats().json().c_str());
  // Past writeIdleConnectionMs the overwrite goes on a new connection; dropped, the same: once.
  pause(io, 1100);
  const auto connectionsBefore = fake.connections;
  KJ_REQUIRE(fails(io, store.put("do/overwrite-fresh", "once")));
  KJ_REQUIRE(fake.count("PUT") == 2 && store.stats().retries == 1 && c.reusedWriteFailures == 1, store.stats().json().c_str());
  KJ_REQUIRE(fake.seen.back().connection == connectionsBefore + 1, "the overwrite went on a new connection");

  // A read dropped on every connection: two attempts, no third.
  fake.decide = [](const Seen& s) { return s.key() == "do/never" ? FakeStore::Act::DROP : FakeStore::Act::ANSWER; };
  const auto gets = fake.count("GET");
  KJ_REQUIRE(fails(io, store.get("do/never")));
  KJ_REQUIRE(fake.count("GET") == gets + 2, fake.count("GET") - gets);

  // A create on a kept connection whose answer is lost: read back (absent), created once more on a new connection.
  within(io, store.get("do/warm"));
  const auto warm = fake.seen.back().connection;
  bool firstCreate = true;
  fake.decide = [&](const Seen& s) {
    if (s.method == "PUT" && firstCreate) { firstCreate = false; return FakeStore::Act::DROP; }
    if (s.method == "GET" && s.key() == "do/created") return FakeStore::Act::NOT_FOUND;
    return FakeStore::Act::ANSWER;
  };
  const auto puts = fake.count("PUT");
  const auto connectionsBeforeCreate = fake.connections;
  auto created = within(io, store.create("do/created", "mine"));
  KJ_REQUIRE(created.created && created.readBack, created.created, created.readBack);
  KJ_REQUIRE(fake.count("PUT") == puts + 2 && store.stats().createsReadBack == 1, store.stats().json().c_str());
  const auto& last = fake.seen.back();
  KJ_REQUIRE(last.method == "PUT" && last.connection > connectionsBeforeCreate && last.connection != warm,
      "the second create went on a new connection", last.connection, connectionsBeforeCreate);
  KJ_REQUIRE(fake.seen[fake.seen.size() - 3].connection == warm, "the first create went on the kept connection");
}

// A request that timed out leaves its connection with an answer still to come: the connection is closed, never kept,
// and the next request reads its own answer.
void timeoutDiscard() {
  auto io = kj::setupAsyncIo();
  FakeStore fake(io);
  auto config = fake.config();
  config.readTimeoutMs = 100;
  ObjectStore store(config, io.provider->getNetwork(), io.provider->getTimer());
  const auto& c = store.stats().connections;
  within(io, store.get("do/warm"));
  bool late = true;
  fake.decide = [&](const Seen& s) {
    if (s.key() == "do/late" && late) { late = false; return FakeStore::Act::LATE; }
    return FakeStore::Act::ANSWER;
  };
  // The read times out on the kept connection and is sent once more on a new one.
  KJ_REQUIRE(within(io, store.get("do/late")).bytes == "do/late");
  // Two overwrites at once: were the timed-out connection kept, one would go on it behind the late answer and fail
  // (an overwrite is not sent twice).
  {
    auto one = store.put("do/concurrent/1", "a");
    auto two = store.put("do/concurrent/2", "b");
    const bool failed = fails(io, kj::mv(one)) | fails(io, kj::mv(two));
    KJ_REQUIRE(!failed && fake.count("PUT") == 2, "every overwrite reads its own answer", fake.count("PUT"));
  }
  KJ_REQUIRE(store.stats().readTimeouts == 1 && store.stats().retries == 1 && c.discardedOnError == 1, store.stats().json().c_str());
  // An overwrite that times out: not sent again, its connection closed.
  fake.decide = [](const Seen& s) { return s.method == "PUT" ? FakeStore::Act::LATE : FakeStore::Act::ANSWER; };
  KJ_REQUIRE(fails(io, store.put("do/slow-write", "x")));
  KJ_REQUIRE(fake.count("PUT") == 3 && c.discardedOnError == 2, c.json().c_str());
  fake.decide = nullptr;
  // The late answers arrive meanwhile on the closed connections; every next request reads its own.
  for (int i = 0; i < 10; ++i) {
    const auto key = "do/next/" + std::to_string(i);
    KJ_REQUIRE(within(io, store.get(key)).bytes == key);
    pause(io, 40);
  }
  KJ_REQUIRE(c.discardedOnError == 2 && c.reusedReadFailures == 0, c.json().c_str());
}

// At most idleConnections kept; a read takes one kept for at most idleConnectionMs, an overwrite only one used within
// writeIdleConnectionMs; kept ones are closed as they pass idleConnectionMs.
void limits() {
  auto io = kj::setupAsyncIo();
  FakeStore fake(io);
  auto config = fake.config();
  config.idleConnections = 4;
  config.idleConnectionMs = 300;
  config.writeIdleConnectionMs = 100;
  ObjectStore store(config, io.provider->getNetwork(), io.provider->getTimer());
  const auto& c = store.stats().connections;
  {
    fake.decide = [](const Seen&) { return FakeStore::Act::HOLD; };
    std::vector<kj::Promise<ObjectResponse>> many;
    for (int i = 0; i < 8; ++i) many.push_back(store.get("do/many/" + std::to_string(i)));
    while (fake.holding() < 8) pause(io, 1);
    KJ_REQUIRE(c.inUse == 8, c.json().c_str());
    fake.decide = nullptr;
    fake.release();
    for (auto& response : many) within(io, kj::mv(response));
  }
  KJ_REQUIRE(c.created == 8 && c.idle == 4 && c.closedOverCap == 4 && c.inUse == 0, c.json().c_str());
  pause(io, 400);
  KJ_REQUIRE(c.idle == 0 && c.closedIdle == 4, c.json().c_str());
  pause(io, 50);
  KJ_REQUIRE(fake.closedByClient == 8, fake.closedByClient);

  within(io, store.get("do/kept"));
  const auto kept = fake.seen.back().connection;
  pause(io, 150);
  // Past writeIdleConnectionMs: the overwrite opens a new connection; the read takes the newest kept one.
  within(io, store.put("do/written", "x"));
  KJ_REQUIRE(fake.seen.back().connection != kept && c.created == 10, c.json().c_str());
  const auto written = fake.seen.back().connection;
  within(io, store.get("do/read"));
  KJ_REQUIRE(fake.seen.back().connection == written, "a read takes the newest kept connection");
  within(io, store.put("do/written-again", "y"));
  KJ_REQUIRE(fake.seen.back().connection == written, "an overwrite takes one used just now");
}

// The heartbeat's own connections: however many requests the shared ones carry, a heartbeat goes out at once, never on
// a shared connection, and a held heartbeat does not hold up the next.
void heartbeat() {
  auto io = kj::setupAsyncIo();
  FakeStore fake(io);
  ObjectStore store(fake.config(), io.provider->getNetwork(), io.provider->getTimer());
  const auto& shared = store.stats().connections;
  const auto& own = store.stats().heartbeatConnections;
  auto isHeartbeat = [](const Seen& s) { return s.key().starts_with("members/"); };
  fake.decide = [&](const Seen& s) { return isHeartbeat(s) ? FakeStore::Act::ANSWER : FakeStore::Act::HOLD; };
  std::vector<kj::Promise<ObjectResponse>> recovery;
  for (int i = 0; i < 200; ++i) recovery.push_back(store.get("do/recovery/" + std::to_string(i)));
  while (fake.holding() < 200) pause(io, 1);
  for (int i = 0; i < 3; ++i) within(io, store.put("members/node-a/1", "beat", ObjectStoreLane::HEARTBEAT), 2000);
  KJ_REQUIRE(fake.holding() == 200, "the heartbeats went out while every other request waited");
  fake.decide = nullptr;
  fake.release();
  for (auto& response : recovery) within(io, kj::mv(response));
  // Every later heartbeat, and every later request, stays on its own lane's connections.
  for (int i = 0; i < 20; ++i) {
    within(io, store.put("members/node-a/1", "beat", ObjectStoreLane::HEARTBEAT));
    within(io, store.get("do/after/" + std::to_string(i)));
  }
  const auto heartbeatConnections = fake.connectionsOf(isHeartbeat);
  const auto sharedConnections = fake.connectionsOf([&](const Seen& s) { return !isHeartbeat(s); });
  for (auto id : heartbeatConnections) KJ_REQUIRE(!sharedConnections.count(id), "a heartbeat went on a shared connection", id);
  KJ_REQUIRE(heartbeatConnections.size() == 1 && own.created == 1 && own.reused == 22, own.json().c_str());
  KJ_REQUIRE(shared.idle == 64 && shared.created == 200 && shared.reused == 20, shared.json().c_str());

  // A heartbeat the store holds: the next goes out on another connection of the lane.
  fake.decide = [&](const Seen& s) {
    return isHeartbeat(s) && s.body == "held" ? FakeStore::Act::HOLD : FakeStore::Act::ANSWER;
  };
  auto first = store.put("members/node-a/1", "held", ObjectStoreLane::HEARTBEAT);
  while (fake.holding() < 1) pause(io, 1);
  within(io, store.put("members/node-a/1", "next", ObjectStoreLane::HEARTBEAT), 2000);
  KJ_REQUIRE(fake.holding() == 1 && own.created == 2, own.json().c_str());
  fake.release();
  within(io, kj::mv(first));
  KJ_REQUIRE(own.idle == 2 && own.inUse == 0, own.json().c_str());
  const auto json = store.stats().json();
  KJ_REQUIRE(json.find("\"heartbeatConnections\":{\"inUse\":0,\"idle\":2,\"created\":2,") != std::string::npos, json.c_str());
}

}  // namespace
}  // namespace coworkerd

int main(int argc, char** argv) {
  using namespace coworkerd;
  try {
    KJ_REQUIRE(argc == 2, "select an object store test case");
    const std::string_view label = argv[1];
    if (label == "reuse") reuse();
    else if (label == "broken") broken();
    else if (label == "retry-rules") retryRules();
    else if (label == "timeout") timeoutDiscard();
    else if (label == "limits") limits();
    else if (label == "heartbeat") heartbeat();
    else if (label == "extra-answer") extraAnswer();
    else KJ_FAIL_REQUIRE("unknown object store test case", argv[1]);
  } catch (const kj::Exception& error) {
    std::cerr << error.getDescription().cStr() << '\n';
    return 1;
  }
  std::cout << "object-store " << argv[1] << " passed\n";
  return 0;
}

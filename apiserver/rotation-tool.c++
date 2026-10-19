// `coworkerd-apiserver tool rotate`: asks a
// node's management endpoint to rotate its processes and, with --wait, follows the rotations.
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/async-io.h>
#include <kj/compat/http.h>
#include <kj/debug.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace coworkerd {
namespace {
double mono() { return std::chrono::duration<double, std::milli>(
    std::chrono::steady_clock::now().time_since_epoch()).count(); }
}

namespace {
uint64_t jsonNumber(capnp::JsonValue::Reader object, kj::StringPtr name) {
  if (!object.isObject()) return 0;
  for (auto field : object.getObject()) if (field.getName() == name && field.getValue().isNumber())
    return uint64_t(field.getValue().getNumber());
  return 0;
}
}

int runRotationTool(int argc, char** argv) {
  constexpr auto USAGE = "usage: coworkerd-apiserver tool rotate --server URL (--process ID... | --all) [--wait] [--timeout-ms N] [--token TOKEN]";
  std::string server;
  std::vector<std::string> processes;
  bool all = false, wait = false;
  uint64_t timeoutMs = 600000;
  // The management token: --token, else COWORKERD_MANAGEMENT_TOKEN.
  std::string token;
  if (const char* value = std::getenv("COWORKERD_MANAGEMENT_TOKEN")) token = value;
  for (int i = 0; i < argc; ++i) {
    std::string flag = argv[i];
    if (flag == "--all") all = true;
    else if (flag == "--token" && i + 1 < argc) token = argv[++i];
    else if (flag == "--wait") wait = true;
    else if (flag == "--server" && i + 1 < argc) server = argv[++i];
    else if (flag == "--process" && i + 1 < argc) processes.push_back(argv[++i]);
    else if (flag == "--timeout-ms" && i + 1 < argc) timeoutMs = std::stoull(argv[++i]);
    else KJ_FAIL_REQUIRE(USAGE, flag.c_str());
  }
  KJ_REQUIRE(!server.empty() && all != !processes.empty(), USAGE);
  while (server.ends_with('/')) server.pop_back();
  std::string query = all ? "all=1" : "";
  for (const auto& id : processes) {
    KJ_REQUIRE(!id.empty() && std::all_of(id.begin(), id.end(), [](char c) { return std::isalnum(static_cast<unsigned char>(c)); }),
        "invalid process id", id.c_str());
    query += (query.empty() ? "" : "&") + std::string("process=") + id;
  }
  auto io = kj::setupAsyncIo();
  auto table = kj::HttpHeaderTable::Builder().build();
  auto client = kj::newHttpClient(io.provider->getTimer(), *table, io.provider->getNetwork(), kj::none);
  auto send = [&](kj::HttpMethod method, const std::string& path) {
    kj::HttpHeaders headers(*table);
    if (!token.empty()) headers.addPtr("Authorization", kj::str("Bearer ", token.c_str()));
    auto url = server + path;
    auto request = client->request(method, url.c_str(), headers, uint64_t(0));
    request.body = nullptr;
    auto response = io.provider->getTimer().timeoutAfter(30 * kj::SECONDS, kj::mv(request.response)).wait(io.waitScope);
    auto text = response.body->readAllText(64 << 20).wait(io.waitScope);
    return std::pair<unsigned, std::string>(response.statusCode, text.cStr());
  };
  auto [status, body] = send(kj::HttpMethod::POST, "/__coworkerd/rotate?" + query);
  std::cout << body << std::endl;
  if (status != 200) return 1;
  if (!wait) return 0;
  std::vector<uint64_t> ids;
  {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(body.data(), body.size()), value);
    for (auto field : value.getObject()) if (field.getName() == "requests")
      for (auto item : field.getValue().getArray()) ids.push_back(jsonNumber(item, "requestId"));
  }
  std::string path = "/__coworkerd/rotation?requests=";
  for (size_t i = 0; i < ids.size(); ++i) path += (i ? "," : "") + std::to_string(ids[i]);
  const auto until = mono() + double(timeoutMs);
  for (;;) {
    auto [pollStatus, state] = send(kj::HttpMethod::GET, path);
    if (pollStatus != 200) { std::cerr << state << std::endl; return 1; }
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(state.data(), state.size()), value);
    if (jsonNumber(value, "pending") == 0) {
      std::cout << state << std::endl;
      return jsonNumber(value, "failed") == 0 && jsonNumber(value, "unknown") == 0 ? 0 : 1;
    }
    if (mono() > until) { std::cerr << "timed out waiting for the rotations: " << state << std::endl; return 2; }
    io.provider->getTimer().afterDelay(200 * kj::MILLISECONDS).wait(io.waitScope);
  }
}
}  // namespace coworkerd

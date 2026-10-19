// etcd's users and roles (docs/control-plane-api.md section 4): `coworkerd-apiserver tool
// etcd-auth`, run by the compose file's init at every start of the cluster (and by an administrator on bare metal,
// docs/guide/install.md). It brings etcd to this plan, whatever state it finds -- a new etcd without authentication,
// or one where an earlier run enabled it -- and enables authentication last:
//
//   user root                    role root                         the administrator; init's file only
//   user coworkerd-apiserver     role coworkerd-apiserver          read and write /coworkerd/v2/tenants/, credentials/
//                                                                  and deploy/; read /coworkerd/v2/nodes/
//   user coworkerd-node-<id>     roles coworkerd-node and          read /coworkerd/v2/deploy/, tenants/ and nodes/ (not
//                                coworkerd-node-<id>               credentials/); read and write its own member key
//                                                                  /coworkerd/v2/nodes/<id> and the prefix below it
//   user coworkerd-health        role coworkerd-health             nothing: `etcdctl --user coworkerd-health:coworkerd-
//                                                                  health endpoint health` is the compose file's
//                                                                  healthcheck (etcd counts a refused read as healthy).
//                                                                  Its password is not a secret.
//
// etcd's grants are unions of key ranges with no "except": "the v2 layout but nodes/ read-only" is three prefixes read
// and write plus nodes/ read, and a node's own range is two grants (its member key exactly, and the prefix below it),
// so that node-1 cannot write node-10's keys. Each user's password is the one its file holds (set again on every run:
// a password is changed by changing its file); permissions a role holds beyond the plan are revoked.
// Prints one JSON line with what it found and did; never a password.
#include "control.h"
#include "etcd.h"
#include "support.h"

#include <kj/async-io.h>
#include <kj/debug.h>
#include <kj/main.h>

#include <capnp/message.h>

#include <iostream>
#include <set>
#include <sstream>
#include <tuple>
#include <vector>

namespace coworkerd {
namespace {
using namespace etcd;

struct Permission {
  std::string type;  // READ, WRITE or READWRITE
  std::string key, rangeEnd;  // rangeEnd empty: one key
  bool operator<(const Permission& other) const {
    return std::tie(type, key, rangeEnd) < std::tie(other.type, other.key, other.rangeEnd);
  }
};
struct RolePlan {
  std::string role;
  std::vector<Permission> permissions;
};
struct UserPlan {
  EtcdCredentials credentials;
  std::vector<std::string> roles;
};

std::string prefixEnd(std::string prefix) {
  ++prefix.back();
  return prefix;
}
Permission prefix(const char* type, const std::string& value) { return {type, value, prefixEnd(value)}; }
Permission key(const char* type, const std::string& value) { return {type, value, ""}; }

// The roles of the v2 layout (the header comment).
std::vector<RolePlan> rolePlan(const std::vector<std::string>& nodes) {
  std::vector<RolePlan> roles = {
      {"coworkerd-apiserver", {prefix("READWRITE", TENANTS_PREFIX), prefix("READWRITE", CREDENTIALS_PREFIX),
          prefix("READWRITE", DEPLOYMENT_PREFIX), prefix("READ", NODES_PREFIX)}},
      {"coworkerd-node", {prefix("READ", DEPLOYMENT_PREFIX), prefix("READ", TENANTS_PREFIX), prefix("READ", NODES_PREFIX)}},
      {"coworkerd-health", {}}};
  for (const auto& node : nodes)
    roles.push_back({"coworkerd-node-" + node, {key("READWRITE", nodeMemberKey(node)), prefix("READWRITE", nodeMemberKey(node) + "/")}});
  return roles;
}

Json::Reader parse(capnp::MallocMessageBuilder& message, const std::string& text) {
  auto json = message.initRoot<Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), json);
  return json.asReader();
}

// One call to etcd's JSON gateway with the given token (or none): the status and the body, without throwing for an
// HTTP error (the plan tells "already exists" from a failure).
kj::Promise<std::pair<uint, std::string>> call(kj::HttpClient& client, const kj::HttpHeaderTable& table,
    const std::string& endpoint, const std::string& path, const std::string& payload, const std::string& token) {
  kj::HttpHeaders headers(table);
  headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
  if (!token.empty()) headers.add(kj::str("Authorization"), kj::heapString(token.data(), token.size()));
  auto request = client.request(kj::HttpMethod::POST, (endpoint + path).c_str(), headers, payload.size());
  co_await request.body->write(kj::arrayPtr(payload.data(), payload.size()).asBytes());
  request.body = nullptr;
  auto response = co_await request.response;
  std::string body = (co_await response.body->readAllText(1 << 20)).cStr();
  co_return std::pair<uint, std::string>{response.statusCode, std::move(body)};
}

int usage() {
  std::cerr << "usage: coworkerd-apiserver tool etcd-auth --endpoint URL --root FILE --apiserver FILE --node ID=FILE "
               "[--node ID=FILE ...]\n"
               "  Each FILE is {\"user\":\"...\",\"password\":\"...\"}, readable by its owner only. Creates the users and roles of "
               "the plan (apiserver/etcd-auth.c++), sets the users' passwords from the files, revokes permissions beyond the plan "
               "and enables authentication.\n";
  return 2;
}
}  // namespace

int runEtcdAuthTool(int argc, char** argv) {
  std::string endpoint, rootFile, apiserverFile;
  std::vector<std::pair<std::string, std::string>> nodeFiles;
  for (int i = 0; i < argc; ++i) {
    const std::string arg = argv[i];
    auto value = [&]() -> std::string {
      KJ_REQUIRE(i + 1 < argc, "missing value", arg.c_str());
      return argv[++i];
    };
    if (arg == "--endpoint") endpoint = value();
    else if (arg == "--root") rootFile = value();
    else if (arg == "--apiserver") apiserverFile = value();
    else if (arg == "--node") {
      const auto text = value();
      const auto equals = text.find('=');
      KJ_REQUIRE(equals != std::string::npos && equals > 0, "--node takes ID=FILE", text.c_str());
      nodeFiles.emplace_back(text.substr(0, equals), text.substr(equals + 1));
      KJ_REQUIRE(safeName(nodeFiles.back().first), "invalid node id", nodeFiles.back().first.c_str());
    } else {
      std::cerr << "unknown argument: " << arg << "\n";
      return usage();
    }
  }
  if (endpoint.empty() || rootFile.empty() || apiserverFile.empty() || nodeFiles.empty()) return usage();
  KJ_REQUIRE(endpoint.starts_with("http://") && !endpoint.ends_with('/'), "the endpoint is an HTTP URL without a trailing slash");
  const auto root = readEtcdCredentials(rootFile);
  KJ_REQUIRE(root.user == "root", "the root file's user must be root", rootFile.c_str());
  std::vector<std::string> nodes;
  std::vector<UserPlan> users = {{readEtcdCredentials(apiserverFile), {"coworkerd-apiserver"}},
      {{"coworkerd-health", "coworkerd-health", "", "built in"}, {"coworkerd-health"}}};
  for (const auto& [node, file] : nodeFiles) {
    nodes.push_back(node);
    users.push_back({readEtcdCredentials(file), {"coworkerd-node", "coworkerd-node-" + node}});
  }
  {
    std::set<std::string> names = {"root"};
    for (const auto& user : users) KJ_REQUIRE(names.insert(user.credentials.user).second, "two files name the same user",
        user.credentials.user.c_str());
  }
  const auto roles = rolePlan(nodes);

  auto io = kj::setupAsyncIo();
  kj::HttpHeaderTable table;
  auto& timer = io.provider->getTimer();
  auto client = kj::newHttpClient(timer, table, io.provider->getNetwork(), kj::none);
  std::ostringstream report;
  auto run = [&]() -> kj::Promise<void> {
    // Whether authentication is on: etcd answers the status anonymously only while it is off.
    auto [statusCode, statusBody] = co_await call(*client, table, endpoint, "/v3/auth/status", "{}", "");
    bool wasEnabled;
    if (statusCode == 200) {
      capnp::MallocMessageBuilder message;
      wasEnabled = boolean(parse(message, statusBody), "enabled");
    } else {
      KJ_REQUIRE(statusBody.find("user name is empty") != std::string::npos, "etcd auth status failed", statusCode, statusBody.c_str());
      wasEnabled = true;
    }
    std::string token;
    if (wasEnabled) {
      const auto payload = "{\"name\":\"root\",\"password\":" + quote(root.password) + "}";
      auto [code, body] = co_await call(*client, table, endpoint, "/v3/auth/authenticate", payload, "");
      KJ_REQUIRE(code == 200, "authentication as root failed (is the root file the one this etcd was set up with?)", code);
      capnp::MallocMessageBuilder message;
      token = text(parse(message, body), "token");
    }
    std::vector<std::string> done;
    auto expect = [&](const std::string& what, uint code, const std::string& body, bool existsIsFine) {
      if (code == 200) { done.push_back(what); return; }
      KJ_REQUIRE(existsIsFine && body.find("already exists") != std::string::npos, "etcd refused", what.c_str(), code, body.c_str());
    };
    // root first (its role is built in): without it etcd cannot enable authentication.
    if (!wasEnabled) {
      auto [code, body] = co_await call(*client, table, endpoint, "/v3/auth/user/add",
          "{\"name\":\"root\",\"password\":" + quote(root.password) + "}", token);
      expect("user add root", code, body, true);
      if (code != 200) {
        auto [changed, changedBody] = co_await call(*client, table, endpoint, "/v3/auth/user/changepw",
            "{\"name\":\"root\",\"password\":" + quote(root.password) + "}", token);
        expect("user changepw root", changed, changedBody, false);
      }
      auto [granted, grantedBody] = co_await call(*client, table, endpoint, "/v3/auth/user/grant", "{\"user\":\"root\",\"role\":\"root\"}", token);
      expect("user grant root root", granted, grantedBody, false);
    }
    for (const auto& role : roles) {
      auto [code, body] = co_await call(*client, table, endpoint, "/v3/auth/role/add", "{\"name\":" + quote(role.role) + "}", token);
      expect("role add " + role.role, code, body, true);
      // The role's permissions as etcd holds them; those beyond the plan are revoked, the plan's granted.
      auto [got, gotBody] = co_await call(*client, table, endpoint, "/v3/auth/role/get", "{\"role\":" + quote(role.role) + "}", token);
      KJ_REQUIRE(got == 200, "etcd role get failed", role.role.c_str(), got, gotBody.c_str());
      std::set<Permission> held;
      {
        capnp::MallocMessageBuilder message;
        auto perms = field(parse(message, gotBody), "perm");
        if (perms.isArray()) for (auto perm : perms.getArray()) {
          auto type = field(perm, "permType");
          auto rangeEnd = field(perm, "range_end");
          held.insert({type.isString() ? type.getString().cStr() : "READ", unbase64(text(perm, "key")),
              rangeEnd.isString() ? unbase64(rangeEnd.getString().cStr()) : std::string()});
        }
      }
      const std::set<Permission> wanted(role.permissions.begin(), role.permissions.end());
      for (const auto& permission : held) {
        if (wanted.contains(permission)) continue;
        auto payload = "{\"role\":" + quote(role.role) + ",\"key\":" + quote(base64(permission.key)) +
            (permission.rangeEnd.empty() ? std::string() : ",\"range_end\":" + quote(base64(permission.rangeEnd))) + "}";
        auto [revoked, revokedBody] = co_await call(*client, table, endpoint, "/v3/auth/role/revoke", payload, token);
        expect("role revoke " + role.role + " " + permission.type + " " + permission.key, revoked, revokedBody, false);
      }
      for (const auto& permission : role.permissions) {
        if (held.contains(permission)) continue;
        auto payload = "{\"name\":" + quote(role.role) + ",\"perm\":{\"permType\":" + quote(permission.type) + ",\"key\":" +
            quote(base64(permission.key)) + (permission.rangeEnd.empty() ? std::string() : ",\"range_end\":" + quote(base64(permission.rangeEnd))) + "}}";
        auto [granted, grantedBody] = co_await call(*client, table, endpoint, "/v3/auth/role/grant", payload, token);
        expect("role grant " + role.role + " " + permission.type + " " + permission.key, granted, grantedBody, false);
      }
    }
    for (const auto& user : users) {
      const auto& name = user.credentials.user;
      auto [code, body] = co_await call(*client, table, endpoint, "/v3/auth/user/add",
          "{\"name\":" + quote(name) + ",\"password\":" + quote(user.credentials.password) + "}", token);
      expect("user add " + name, code, body, true);
      if (code != 200) {
        auto [changed, changedBody] = co_await call(*client, table, endpoint, "/v3/auth/user/changepw",
            "{\"name\":" + quote(name) + ",\"password\":" + quote(user.credentials.password) + "}", token);
        expect("user changepw " + name, changed, changedBody, false);
      }
      for (const auto& role : user.roles) {
        auto [granted, grantedBody] = co_await call(*client, table, endpoint, "/v3/auth/user/grant",
            "{\"user\":" + quote(name) + ",\"role\":" + quote(role) + "}", token);
        expect("user grant " + name + " " + role, granted, grantedBody, false);
      }
    }
    if (!wasEnabled) {
      auto [code, body] = co_await call(*client, table, endpoint, "/v3/auth/enable", "{}", token);
      expect("auth enable", code, body, false);
    }
    // What anonymous requests get now: refused.
    auto [anonymous, anonymousBody] = co_await call(*client, table, endpoint, "/v3/kv/range",
        "{\"key\":" + quote(base64(V2_PREFIX)) + ",\"range_end\":" + quote(base64(prefixEnd(V2_PREFIX))) + ",\"count_only\":true}", "");
    KJ_REQUIRE(anonymous != 200, "etcd still answers anonymous reads after enabling authentication");
    report << "{\"event\":\"etcd-auth\",\"endpoint\":" << quote(endpoint) << ",\"wasEnabled\":" << (wasEnabled ? "true" : "false")
           << ",\"enabled\":true,\"roles\":[";
    for (size_t i = 0; i < roles.size(); ++i) report << (i ? "," : "") << quote(roles[i].role);
    report << "],\"users\":[" << quote("root");
    for (const auto& user : users) report << "," << quote(user.credentials.user);
    report << "],\"changes\":" << done.size() << ",\"anonymousRead\":" << anonymous << "}";
  };
  run().wait(io.waitScope);
  std::cout << "[etcd-auth] " << report.str() << std::endl;
  return 0;
}

}  // namespace coworkerd

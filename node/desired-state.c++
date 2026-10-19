#include "desired-state.h"
#include "code-package.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>

#include <iostream>
#include <sstream>

namespace coworkerd {
kj::Own<DesiredState> newEtcdDesiredState(const DesiredStateConfig&, kj::Network&, kj::Timer&, const kj::HttpHeaderTable&);
namespace {

std::string quote(const std::string& value) {
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<capnp::JsonValue>();
  json.setString(value.c_str());
  return capnp::JsonCodec().encodeRaw(json).cStr();
}

class FileDesiredState final: public DesiredState {
public:
  FileDesiredState(DesiredStateConfig config, std::filesystem::path manifest)
      : settings(std::move(config)), manifest(std::move(manifest)) {
    if (settings.codeDirectory.empty()) settings.codeDirectory = this->manifest.parent_path();
  }
  kj::Promise<void> start() override { reload(); return kj::READY_NOW; }
  kj::Promise<void> run() override { return kj::NEVER_DONE; }
  void reload() override {
    auto input = readDeploymentManifest(manifest);
    // A single-machine reload stays all-or-nothing, including label immutability
    // across reloads of this process.
    for (const auto& [label, version] : input.versions) {
      auto seen = fingerprints.find(label);
      if (seen == fingerprints.end() || seen->second == version->definitionHash) continue;
      auto old = index.exact(label, seen->second);
      KJ_REQUIRE(old->contentHash == version->contentHash,
          "Worker Version is immutable; deploy changed content under a new versionId", label.c_str());
      KJ_FAIL_REQUIRE("Worker Version definition is immutable; deploy changed configuration under a new versionId",
          label.c_str());
    }
    auto next = std::make_unique<DeploymentState>(DeploymentState::fromInput(input,
        current ? current->revision() + 1 : 1, current ? current->generation() + 1 : 1));
    auto problem = next->firstProblem();
    KJ_REQUIRE(problem.empty(), problem.c_str());
    KJ_REQUIRE(!next->routes().empty(), "at least one route is required");
    auto code = readDeploymentCode(input, settings.codeDirectory);
    for (const auto& [label, version] : input.versions) {
      index.remember(version);
      fingerprints.emplace(label, version->definitionHash);
    }
    for (auto& [hash, source] : code.sources) sources.emplace(hash, std::move(source));
    for (auto& [key, bytes] : code.objects) {
      if (key.starts_with("packages/") || key.starts_with("modules/")) objects.emplace(key, std::move(bytes));
    }
    current = std::move(next);
    std::cout << "[deployment] file generation=" << current->generation()
              << " definitions=" << current->definitions().size() << std::endl;
  }
  const DeploymentState& state() const override { return *current; }
  std::shared_ptr<const VersionDefinition> find(const VersionPin& value, std::string& reason) const override {
    return index.find(value, reason);
  }
  std::shared_ptr<const VersionDefinition> exact(const std::string& label, const std::string& hash) const override {
    return index.exact(label, hash);
  }
  std::shared_ptr<const VersionDefinition> byHash(const std::string& hash) const override { return index.byHash(hash); }
  kj::Promise<std::shared_ptr<const VersionDefinition>> resolve(
      VersionPin pin, double, std::string& reason) override {
    return index.find(pin, reason);
  }
  kj::Promise<std::string> ensureCode(const VersionDefinition& version, double) override {
    return sources.contains(version.definitionHash) ? std::string() : std::string("代码不可用：缺失");
  }
  kj::Promise<std::string> source(const VersionDefinition& version) override {
    return sources.at(version.definitionHash);
  }
  std::string bundlesJson() const override { return "{\"backend\":\"file\"}"; }
  kj::Promise<SharedAssetIndex> assets(const VersionDefinition& version, double, std::string& reason) override {
    auto manifest = objects.find("packages/" + version.contentHash);
    if (manifest == objects.end()) { reason = "资源不可用：包清单缺失"; return SharedAssetIndex(); }
    auto& cached = assetIndexes[version.contentHash];
    if (!cached) {
      auto index = std::make_shared<AssetIndex>();
      auto contents = readPackage(manifest->second);
      for (auto& asset : contents.assets) index->byPath.emplace(asset.path, std::move(asset));
      index->bytes = contents.assetBytes;
      cached = std::move(index);
    }
    return cached;
  }
  kj::Promise<AssetRead> assetBytes(const CodeAsset& asset, double) override {
    auto found = objects.find("modules/" + asset.sha256);
    if (found == objects.end() || deploymentSha256(found->second) != asset.sha256) {
      return AssetRead{{}, "资源不可用：缺失或哈希不符"};
    }
    return AssetRead{found->second, {}};
  }
  std::string json(bool full) const override;
  const DesiredStateConfig& config() const override { return settings; }

private:
  DesiredStateConfig settings;
  std::filesystem::path manifest;
  DefinitionIndex index;
  std::map<std::string, std::string> fingerprints;
  std::unique_ptr<DeploymentState> current;
  std::map<std::string, std::string> sources;
  // Package manifests and module/asset bytes by object key (append-only, like sources).
  std::map<std::string, std::string> objects;
  std::map<std::string, SharedAssetIndex> assetIndexes;
};

std::string activeVersionsJson(const DeploymentState& state, bool details) {
  std::string out = "{";
  size_t index = 0;
  for (const auto& [id, worker] : state.workers()) {
    if (!worker.valid) continue;
    if (index++) out += ',';
    out += quote(id) + ':' + (details ? versionStatusJson(*worker.version, state.secretsStatusField(*worker.version))
        : "{\"versionId\":" + quote(worker.version->versionId) + ",\"definitionHash\":" + quote(worker.version->definitionHash) + '}');
  }
  return out + '}';
}

std::string FileDesiredState::json(bool full) const {
  std::ostringstream out;
  out << "{\"backend\":\"file\",\"revision\":\"" << current->revision()
      << "\",\"generation\":" << current->generation();
  if (full) {
    out << ",\"activeVersions\":" << activeVersionsJson(*current, false)
        << ",\"activeVersionDetails\":" << activeVersionsJson(*current, true);
  }
  out << ',' << current->statusFields(20)
      << ",\"rejectedRevisions\":0,\"lastRejectedRevision\":\"0\",\"lastRejectedReason\":\"\""
      << ",\"fullRereads\":0,\"compactions\":0,\"watchConnected\":false"
      << ",\"versionResolutionForTest\":" << quote(settings.versionResolutionForTest)
      << ",\"deploymentApplyDelayMsForTest\":" << settings.deploymentApplyDelayMsForTest << '}';
  return out.str();
}
}  // namespace

kj::Own<DesiredState> newDesiredState(const DesiredStateConfig& config, const std::filesystem::path& manifest,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headers) {
  KJ_REQUIRE(config.backend == "file" || config.backend == "etcd", "unknown desired state backend");
  KJ_REQUIRE(config.versionResolutionForTest.empty() || config.versionResolutionForTest == "per-call",
      "versionResolutionForTest must be empty or per-call");
  if (config.backend == "etcd") return newEtcdDesiredState(config, network, timer, headers);
  return kj::heap<FileDesiredState>(config, manifest);
}
}  // namespace coworkerd

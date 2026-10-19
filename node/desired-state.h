#pragma once

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <filesystem>
#include <memory>
#include <string>

#include "config.h"
#include "deployment.h"

// The node's view of the desired state: a manifest file, or etcd.
namespace coworkerd {

class DesiredState {
public:
  virtual ~DesiredState() noexcept(false) = default;
  virtual kj::Promise<void> start() = 0;
  virtual kj::Promise<void> run() = 0;
  virtual void reload() = 0;
  // The current revision's view. Callers use it synchronously; apply replaces
  // Worker entries in place between event-loop turns.
  virtual const DeploymentState& state() const = 0;
  virtual std::shared_ptr<const VersionDefinition> find(
      const VersionPin& pin, std::string& reason) const = 0;
  virtual std::shared_ptr<const VersionDefinition> exact(
      const std::string& label, const std::string& hash = {}) const = 0;
  virtual std::shared_ptr<const VersionDefinition> byHash(const std::string& hash) const = 0;
  virtual kj::Promise<std::shared_ptr<const VersionDefinition>> resolve(
      VersionPin pin, double remainingMs, std::string& reason) = 0;
  virtual kj::Promise<std::string> ensureCode(const VersionDefinition& version, double remainingMs) = 0;
  virtual kj::Promise<std::string> source(const VersionDefinition& version) = 0;
  virtual std::string bundlesJson() const = 0;
  // The asset index of a version's package, fetched on first use. Null with a
  // reason when the package manifest cannot be obtained or verified.
  virtual kj::Promise<SharedAssetIndex> assets(const VersionDefinition& version, double remainingMs, std::string& reason) = 0;
  // The verified bytes of one asset, fetched by hash on first use.
  virtual kj::Promise<AssetRead> assetBytes(const CodeAsset& asset, double remainingMs) = 0;
  // Full status (`full`) or the deployment summary without per-Worker maps.
  virtual std::string json(bool full = true) const = 0;
  virtual const DesiredStateConfig& config() const = 0;
};

kj::Own<DesiredState> newDesiredState(
    const DesiredStateConfig& config, const std::filesystem::path& manifest,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable);

}  // namespace coworkerd

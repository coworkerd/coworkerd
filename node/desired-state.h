#pragma once

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "config.h"
#include "deployment.h"

// The node's view of the desired state: a manifest file (the positional form), or the tenant index and the tenants'
// manifests on the object store (desired-state-store.c++).
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
  // A read of the desired state now (the object store's: the index's hint, and what changed), shared with a poll
  // already running -- one that started after this call, so that a write made before it is seen. A trial load that names
  // a newer revision than this node applied asks for it instead of waiting for the next poll.
  virtual kj::Promise<void> refresh() { return kj::READY_NOW; }
  // The tenants the desired state names, each with its record (none: a tenant only the operations tool writes), at
  // every applied revision; `records` false when this backend has no records (the file backend never calls).
  virtual void onTenants(std::function<void(const std::map<std::string, std::optional<TenantRecord>>&, uint64_t revision,
      bool records)>) {}
};

kj::Own<DesiredState> newDesiredState(
    const DesiredStateConfig& config, const std::filesystem::path& manifest,
    kj::Network& network, kj::Timer& timer, const kj::HttpHeaderTable& headerTable);

}  // namespace coworkerd

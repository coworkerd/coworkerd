#pragma once

// The control plane's view of the desired state on the object store (common/tenant-manifest.h) and its one way of
// writing it: an in-memory mirror of the tenant index, every tenant's manifest the index names and the credentials
// index, read in full at startup and kept current by reading the two indexes' hints every pollIntervalMs (and every
// aggregate again after another writer created its next revision first). Every change is one aggregate's next revision
// computed from the mirror, created only if absent: created is committed; a revision another writer created first
// sends the change back to read and compute again. The operations tool (deployment-tool.c++) uses the same class once.
#include "deployment.h"
#include "object-state.h"
#include "object-store.h"
#include "tenancy.h"
#include "tenant-manifest.h"

#include <kj/async-io.h>

#include <functional>
#include <map>
#include <optional>
#include <iostream>
#include <set>
#include <string>
#include <vector>

namespace coworkerd {

class ManifestStore {
public:
  // `log` takes the [state] lines (the control plane's log; the operations tool's standard error, its output being the
  // operation's record).
  ManifestStore(ObjectStore& store, kj::Timer& timer, std::optional<SecretsKey> key = std::nullopt, std::ostream& log = std::cout);

  // The cluster's marker first: a store without it is not a cluster's (`create`: the operations tool creates it
  // where it is missing -- its writes make the bucket one; the control plane refuses to start without it). Then the
  // index, every manifest it names and (with `credentials`) the credentials index.
  kj::Promise<void> load(bool create, bool credentials);
  // The indexes' hints read again and what changed read: the control plane's poll.
  kj::Promise<void> refresh();
  // The current revision of one tenant's manifest read from the store into the mirror (hint, then later revisions).
  kj::Promise<void> reloadTenant(std::string tenant);

  const TenantIndex& index() const { return tenantIndex; }
  const CredentialIndex& credentials() const { return credentialIndex; }
  const std::map<std::string, TenantManifest>& tenants() const { return manifests; }
  const TenantManifest* tenant(const std::string& name) const;
  const std::optional<SecretsKey>& secretsKey() const { return key; }
  ObjectStore& objects() { return store; }
  // This process's instance: every revision object it writes names it (WriterIdentity), with a random value per write.
  const std::string& writerInstance() const { return instance; }
  void setWriterInstance(std::string value) { instance = std::move(value); }
  // The cluster's marker, created when absent: its bytes as this process writes them.
  std::string markerBytes() const { return clusterFormatBytes(newWriter(instance)); }

  // One tenant's next manifest. `change` edits a copy of the current one (revision 0, no record and no Workers when the
  // tenant has none) or throws its refusal; when nothing changed nothing is written. The result is created as the next
  // revision only if absent; when another writer created that revision first, the tenant is read again and `change`
  // runs again -- `maxAttempts` times in all, jittered pauses of 5 to 160 ms between -- then the tenant's hint and the
  // index. `deployment`: what the nodes serve changes (the generation counts it).
  struct Written {
    TenantManifest manifest;
    bool changed = false;
    uint32_t attempts = 0;
    std::vector<uint64_t> conflictWaitsMs;  // per attempt that met another writer's revision: how long its create took
    uint64_t indexRevision = 0;
    bool indexed = true;  // false: the index could not be written (retried by the next poll)
    uint64_t submittedAtUnixMs = 0, committedAtUnixMs = 0;
  };
  kj::Promise<Written> writeTenant(std::string tenant, std::function<kj::Promise<void>(TenantManifest&)> change, bool deployment,
      uint32_t maxAttempts = 8);
  // The end of a tenant's deletion: the tenant out of the index, then its objects (tenants/<t>/) removed. Returns how
  // many objects were removed; a removal that fails is counted in `failed`.
  struct Forgotten { uint64_t indexRevision = 0, removed = 0, failed = 0; };
  kj::Promise<Forgotten> forgetTenant(std::string tenant);
  // The credentials index's next revision (the same protocol).
  struct WrittenCredentials { CredentialIndex index; bool changed = false; uint32_t attempts = 0; };
  kj::Promise<WrittenCredentials> writeCredentials(std::function<void(CredentialIndex&)> change, uint32_t maxAttempts = 8);
  // The index written for what this process wrote and has not indexed yet (the poll retries a failed one).
  kj::Promise<uint64_t> syncIndex();

  // Version definitions by hash (definitions/<hash>, immutable): fetched once, kept only when they verify against their
  // hash; `failures` names the ones that did not.
  kj::Promise<void> fetchDefinitions(std::set<std::string> hashes);
  const DefinitionBytes& definitions() const { return definitionBytes; }
  // A definition this process computed itself (a package's), known without a read.
  void rememberDefinition(const VersionDefinition& version);
  std::shared_ptr<const VersionDefinition> definition(const std::string& hash) const;
  const std::map<std::string, std::string>& definitionFailures() const { return failedDefinitions; }

  // The deployment state of one tenant -- its keys, every tenant's routes, the generation -- as the nodes evaluate it,
  // with `replacement` in place of its manifest when given. Secrets count as set when they open with this process's key
  // (or, without a key, when they read as sealed records); their values never enter it.
  DeploymentState stateOf(const std::string& tenant, const TenantManifest* replacement = nullptr) const;

  // For /v1/status and the logs.
  struct Counters {
    uint64_t writes = 0, conflicts = 0, retries = 0, indexWrites = 0, indexConflicts = 0, credentialWrites = 0,
        credentialConflicts = 0, polls = 0, pollFailures = 0, definitionFetches = 0;
    uint64_t lastPollAtUnixMs = 0;
    std::string lastPollError;
  };
  const Counters& counters() const { return stats; }
  std::string json() const;
  void notePollFailure(const std::string& reason);
  // Every tenant's definitions (reference counting the store's objects).
  kj::Promise<std::vector<std::shared_ptr<const VersionDefinition>>> allDefinitions(std::function<bool(const std::string&)> tenantFilter);

private:
  ObjectStore& store;
  kj::Timer& timer;
  std::optional<SecretsKey> key;
  std::ostream& log;
  std::string instance = newWriterInstance();
  TenantIndex tenantIndex;
  CredentialIndex credentialIndex;
  std::map<std::string, TenantManifest> manifests;
  // Tenants whose manifest this process wrote and the index does not name yet, and tenants it is removing.
  std::set<std::string> unindexed, removing;
  DefinitionBytes definitionBytes;
  std::map<std::string, std::shared_ptr<const VersionDefinition>> parsedDefinitions;
  std::map<std::string, std::string> failedDefinitions;
  Counters stats;
  // One writer at a time per aggregate in this process ("index", "credentials", "tenant:<t>"): writers of one process
  // never take each other's revision; other processes' writers meet as created-first revisions.
  std::map<std::string, kj::ForkedPromise<void>> turns;
  struct Turn;
  kj::Promise<kj::Own<Turn>> turn(const std::string& aggregate);
  kj::Promise<void> pause(uint32_t attempt);
  void adoptManifest(TenantManifest manifest);
  kj::Promise<void> readManifest(std::string tenant, uint64_t revision);
  kj::Promise<void> adoptIndex(TenantIndex index);
};

}  // namespace coworkerd

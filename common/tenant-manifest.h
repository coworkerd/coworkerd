#pragma once

#include "deployment.h"
#include "object-store.h"
#include "tenancy.h"

#include <array>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

// The desired state on the object store (docs/s3-truth-and-durable-objects.md, sections 1 and 3): one immutable
// manifest per tenant -- its record, and each Worker's active pointer, version table, routes and sealed secrets --, the
// tenant index that names each tenant's current manifest, and the credentials index. Each is a revision sequence
// (object-state.h) with a hint, written by the control plane alone; the nodes read the index and the manifests it names
// and turn them into the deployment state's keys (DeploymentState), whose evaluation is the one 0.3's keys had.
namespace coworkerd {

// ---- Keys ----
//   cluster/format/1                       the cluster's marker (created only if absent): nodes and the control plane
//                                          refuse to start on a bucket without it
//   cluster/tenants/<rev>, .../current     the tenant index and its hint
//   cluster/credentials/<rev>, .../current the credentials index and its hint
//   tenants/<t>/manifest/<rev>             a tenant's manifests
//   tenants/<t>/current                    their hint
// A hint holds {"rev":n} and only says which revision is current: a reader always reads that revision's object, and
// lists the prefix for the largest revision when the hint is missing or behind what it knows.
inline constexpr const char* CLUSTER_FORMAT_KEY = "cluster/format/1";
inline constexpr const char* TENANT_INDEX_PREFIX = "cluster/tenants";
inline constexpr const char* CREDENTIAL_INDEX_PREFIX = "cluster/credentials";
inline constexpr const char* TENANT_OBJECTS_PREFIX = "tenants/";
// Who wrote a revision object -- a tenant manifest, a tenant index, a credentials index, the cluster's marker: the
// writing process (an instance drawn at random when it starts, never reused), when, and a random value of this one write.
// A create whose answer is lost is decided by reading the key back (ObjectStore::create): these bytes there are this
// write's, other bytes another writer's.
struct WriterIdentity {
  std::string instance, nonce;
  uint64_t writtenAtUnixMs = 0;
  bool operator==(const WriterIdentity&) const = default;
};
// A process's instance: 16 random bytes, hexadecimal.
std::string newWriterInstance();
// One write's identity: the instance, now, and 8 random bytes (hexadecimal) of its own.
WriterIdentity newWriter(const std::string& instance);
std::string writerJson(const WriterIdentity& writer);

// What cluster/format/1 holds: {"format":1,"kind":"coworkerd-cluster","writer":{...}}.
std::string clusterFormatBytes(const WriterIdentity& writer);
// The hint of a revision sequence: <prefix>/current for the indexes.
std::string indexHintKey(const std::string& prefix);
std::string tenantManifestPrefix(const std::string& tenant);  // tenants/<t>/manifest
std::string tenantHintKey(const std::string& tenant);         // tenants/<t>/current
std::string tenantObjectsPrefix(const std::string& tenant);   // tenants/<t>/
std::string hintJson(uint64_t revision);
std::optional<uint64_t> parseHint(const std::string& bytes);

// ---- The tenant index: every tenant's status, hostnames and current manifest ----
struct TenantIndexEntry {
  std::string status = "active";
  std::vector<std::string> hostnames;
  uint64_t manifestRev = 0;
  bool operator==(const TenantIndexEntry&) const = default;
};
struct TenantIndex {
  uint64_t revision = 0, writtenAtUnixMs = 0;
  WriterIdentity writer;  // empty in an index written before writers named themselves
  std::map<std::string, TenantIndexEntry> tenants;
};
std::string tenantIndexJson(const TenantIndex& index);
TenantIndex decodeTenantIndex(const std::string& bytes);

// ---- A tenant's manifest ----
struct ManifestPointer {
  std::string versionId, definitionHash;
  WorkerLimits limits;  // the Worker's own; empty: the tenant's defaults, then the node's
  bool operator==(const ManifestPointer&) const = default;
};
struct ManifestVersion {
  std::string definitionHash;
  uint64_t uploadedRevision = 0;  // the manifest revision that added the version (its upload order)
  bool operator==(const ManifestVersion&) const = default;
};
struct ManifestRoute {
  std::string host, pathPrefix;
  auto operator<=>(const ManifestRoute&) const = default;
};
// A secret's value sealed with the cluster's secrets key (AES-256-GCM, SecretsKey): a 96-bit nonce and the ciphertext
// with its 128-bit tag, both base64; the additional data is <tenant>/<worker>/<name>.
struct ManifestSecret {
  std::string nonce, ciphertext;
  uint64_t setAtUnixMs = 0;
  bool operator==(const ManifestSecret&) const = default;
};
// A Durable Object namespace of a Worker, by its class's name: the id the control plane made when a version first
// declared the class (32 random bytes, hexadecimal; a rename moves it to the new name), the manifest revision that made
// it, its status ("active", or "deleting" once a version deleted the class: no Binding to it, no new request), and the
// shards it was made with (fixed for its life). There is one kind of object, so no tier is recorded.
struct ManifestNamespace {
  std::string id;
  uint64_t createdRevision = 0;
  std::string status = "active";
  uint32_t shards = 1024;
  bool operator==(const ManifestNamespace&) const = default;
};
struct ManifestWorker {
  std::optional<ManifestPointer> active;
  std::map<std::string, ManifestVersion> versions;   // versionId -> definition
  std::vector<ManifestRoute> routes;                 // sorted, each once
  std::map<std::string, ManifestSecret> secrets;     // name -> sealed value
  std::map<std::string, ManifestNamespace> namespaces;  // class -> namespace
  bool operator==(const ManifestWorker&) const = default;
  bool empty() const { return !active && versions.empty() && routes.empty() && secrets.empty() && namespaces.empty(); }
};
std::string manifestNamespaceJson(const ManifestNamespace& entry);
// A new namespace's id: 32 random bytes, hexadecimal.
std::string newNamespaceId();
struct TenantManifest {
  std::string tenant;
  uint64_t revision = 0, generation = 0, writtenAtUnixMs = 0;
  WriterIdentity writer;
  // The tenant's record (quotas, hostnames, default limits, deletion progress); none for a tenant only the operations
  // tool writes (its default tenant).
  std::optional<TenantRecord> record;
  std::map<std::string, ManifestWorker> workers;
  std::string status() const { return record ? record->status : "active"; }
  std::vector<std::string> hostnames() const { return record ? record->hostnames : std::vector<std::string>{}; }
};
std::string tenantManifestJson(const TenantManifest& manifest);
TenantManifest decodeTenantManifest(const std::string& bytes);
// The same record and Workers (revision, generation and the time written aside).
bool sameContent(const TenantManifest& a, const TenantManifest& b);
// The index's entry for a manifest.
TenantIndexEntry indexEntryOf(const TenantManifest& manifest);

// ---- The credentials index: keyId -> record (the SHA-256 of the secret part, never the secret) ----
struct CredentialIndex {
  uint64_t revision = 0, writtenAtUnixMs = 0;
  WriterIdentity writer;
  std::map<std::string, CredentialRecord> credentials;
};
std::string credentialIndexJson(const CredentialIndex& index);
CredentialIndex decodeCredentialIndex(const std::string& bytes);

// ---- Secrets at rest ----
// The cluster's secrets key: a JSON file {"key":"<64 hexadecimal digits>"} (32 bytes) that the control plane and every
// node hold (secrets.keyFile, COWORKERD_SECRETS_KEY_FILE). Errors name the file and the rule, never its content.
class SecretsKey {
public:
  static SecretsKey load(const std::filesystem::path& file);
  // A new key file's content (32 random bytes).
  static std::string generateJson();
  ManifestSecret seal(const std::string& aad, const std::string& plaintext, uint64_t setAtUnixMs) const;
  // The plaintext, or none when the record does not open with this key (another key, damaged, or not one).
  std::optional<std::string> open(const std::string& aad, const ManifestSecret& sealed) const;
  // The first 12 hexadecimal digits of SHA-256("coworkerd-secrets-key" || key): which key, never the key.
  const std::string& fingerprint() const { return print; }
private:
  std::array<unsigned char, 32> key{};
  std::string print;
};
std::string secretAad(const std::string& tenant, const std::string& worker, const std::string& name);
// The length of a sealed value (the ciphertext without its tag); none when the record is not one.
std::optional<size_t> sealedLength(const ManifestSecret& sealed);

// ---- Into the deployment state ----
// Definitions as the store holds them, by hash (the bytes of definitions/<hash>).
using DefinitionBytes = std::map<std::string, std::string>;
// A secret's value for the deployment state: its plaintext, or none when it cannot be opened (the Worker that declares
// it is then not valid: "missing secret: <name>").
using SecretOpener = std::function<std::optional<std::string>(const std::string& tenant, const std::string& worker,
    const std::string& name, const ManifestSecret& sealed)>;
// The keys one tenant contributes, as DeploymentState reads them: <t>/active/<w>,
// <t>/definitions/<w>/<v> (a version whose definition is not in `definitions` has no key: unknown, as a definition key
// that is missing), <t>/secrets/<w>/<name>, <t>/namespaces/<w> (its Durable Object namespaces' ids and statuses). Routes are every tenant's at once (routeStateKeys).
std::map<std::string, std::string> tenantStateKeys(const TenantManifest& manifest, const DefinitionBytes& definitions,
    const SecretOpener& open);
// routes/<host><pathPrefix> -> its value, over every tenant. A slot two tenants name goes to the first of them by name
// (the control plane refuses such a write; this only keeps every node's choice the same); `conflicts` counts them.
// The tenants by name (pointers into the caller's manifests).
using ManifestView = std::map<std::string, const TenantManifest*>;
std::map<std::string, std::string> routeStateKeys(const ManifestView& tenants, size_t* conflicts = nullptr);
// "generation": the sum of the tenants' generations, a JSON decimal string as DeploymentState reads it.
std::string generationValue(const ManifestView& tenants);
// Every definition hash the tenants' pointers and version tables name.
std::set<std::string> namedDefinitions(const TenantManifest& manifest);
// A definition object read by hash: valid only when it is a well-formed definition with exactly that hash.
bool definitionMatches(const std::string& hash, const std::string& bytes);

}  // namespace coworkerd

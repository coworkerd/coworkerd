#pragma once
#include "json.h"
#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace coworkerd {
struct CodeModule { std::string name, type, sha256; uint64_t bytes; };
using CodePackage = std::vector<CodeModule>;
// A static asset the Server serves itself: request path, size, type, and the
// SHA-256 under which its bytes are stored (modules/<sha256>, shared with modules).
struct CodeAsset { std::string path, sha256, contentType; uint64_t bytes; };
using CodeAssets = std::vector<CodeAsset>;
// Limits on a package's assets; code modules keep their own 10 MiB limit.
// COWORKERD_ASSET_MAX_COUNT, COWORKERD_ASSET_MAX_FILE_BYTES and
// COWORKERD_ASSET_MAX_TOTAL_BYTES override the defaults (tool and Server alike).
struct AssetLimits {
  uint64_t maxCount = 20000;
  uint64_t maxFileBytes = 25ull * 1024 * 1024;
  uint64_t maxTotalBytes = 100ull * 1024 * 1024;
  static AssetLimits fromEnvironment();
};
struct PackageContents {
  CodePackage modules;
  CodeAssets assets;
  uint64_t assetBytes = 0;
};
bool moduleSafeName(const std::string& name);
// Starts with '/', normalized (no empty, "." or ".." segments, no trailing
// '/'), no control characters or backslashes.
bool assetSafePath(const std::string& path);
std::string canonicalJson(json::Json::Reader value);
std::string canonicalJson(const std::string& bytes);
// A package manifest, validated: its code modules and its optional assets.
PackageContents readPackage(const std::string& manifest, const AssetLimits& limits = AssetLimits::fromEnvironment());
// The code modules of a package (what the runtime loads).
CodePackage readCodePackage(const std::string& manifest);
// Canonical manifest bytes. Without assets the bytes are those of a code-only
// package (`{"modules":[...]}`), so existing package hashes do not change.
std::string packageJson(CodePackage modules, CodeAssets assets = {});
std::string packageJson(const PackageContents& contents);
std::string packageSource(const std::string& manifest, const std::map<std::string, std::string>& modules);
// Every object of a package directory, verified against the manifest:
// packages/<hash> and modules/<sha256> for each module and each asset (asset
// files live under <package directory>/__coworkerd_assets/<path>: module names
// never start with the reserved __coworkerd_ prefix). `assets` is whether the
// version declares an assets configuration; it must agree with the package.
std::map<std::string, std::string> packageObjects(const std::filesystem::path& manifest,
    const std::string& expectedHash, const std::string& mainModule, bool assets = false,
    const AssetLimits& limits = AssetLimits::fromEnvironment());
}  // namespace coworkerd

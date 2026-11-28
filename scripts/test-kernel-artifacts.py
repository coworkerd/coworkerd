#!/usr/bin/env python3
"""Exercise the packaging and fetch commands with small archive fixtures."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest

from kernel_artifacts import ARCHITECTURES, BINARIES, PROFILED, ROOT, archive_name, sha256


class KernelArtifactsTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        for name in ("kernel_artifacts.py", "package-kernel.py", "fetch-kernel.py", "kernel-release.py",
                     "workerd-patches.py", "syscall-allowlist-check.py"):
            shutil.copyfile(ROOT / "scripts" / name, scripts / name)
        self.checkout = self.root / "workerd"
        self.checkout.mkdir()
        self.patches = self.root / "patches" / "workerd"
        self.patches.mkdir(parents=True)
        for directory in (self.checkout, self.root):
            self.git(directory, "init", "-q")
            self.git(directory, "commit", "--allow-empty", "-qm", "Fixture base")
        with (self.checkout / ".git/info/exclude").open("a") as stream:
            stream.write("/bazel-bin/\n")
        self.tag = "v1-fixture"
        self.commit = self.git(self.checkout, "rev-parse", "HEAD").decode().strip()
        self.git(self.checkout, "tag", self.tag)
        source = self.checkout / "src/workerd/server/fixture.c++"
        source.parent.mkdir(parents=True)
        source.write_text("// Kernel artifact test fixture\n")
        self.git(self.checkout, "add", ".")
        patch = self.git(self.checkout, "diff", "--cached", "--binary")
        self.git(self.checkout, "commit", "-qm", "Fixture kernel change")
        (self.patches / "UPSTREAM").write_text(f"tag={self.tag}\ncommit={self.commit}\n")
        (self.patches / "series").write_text("# Fixture series\n\n0001-fixture.patch\n")
        (self.patches / "0001-fixture.patch").write_bytes(patch)
        self.series_sha = hashlib.sha256(patch).hexdigest()
        self.out = self.root / "local release"
        self.allowlist = self.root / "allowlist"
        self.allowlist.mkdir()

    def git(self, directory, *args):
        return subprocess.check_output(["git", "-C", str(directory), "-c", "user.name=Test",
                                        "-c", "user.email=test@example.invalid", *args])

    def run_script(self, name, *args, ok=True, env=None):
        result = subprocess.run([sys.executable, str(self.root / "scripts" / name), *args],
                                capture_output=True, text=True, env=env)
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result

    def write_build(self, arch):
        machine, elf_machine = ARCHITECTURES[arch]
        for name, relative in BINARIES.items():
            path = self.checkout / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            # Only the ELF headers are needed. The fixture is never executed.
            path.write_bytes(b"\x7fELF\x02\x01\x01" + b"\0" * 9
                             + struct.pack("<HH", 3, elf_machine) + b"\0" * 44 + name.encode())
            path.chmod(0o755)
        workerd = sha256(self.checkout / BINARIES[PROFILED[0]])
        record = {"architecture": machine, "workerd": {"sha256": workerd},
                  "profiles": [{"run": "fixture"}], "generatedAt": "fixture"}
        for kind in ("startup", "steady"):
            name = f"coworkerd-syscalls-{kind}.txt"
            path = self.allowlist / name
            path.write_text(f"# workerd-sha256 {workerd}\n0\n")
            record[kind] = {"file": name, "sha256": sha256(path), "count": 1}
        (self.allowlist / "allowlist.json").write_text(json.dumps(record))

    def write_pins(self, archives, **fields):
        (self.patches / "artifacts.json").write_text(json.dumps({
            "upstream": self.tag, "upstreamCommit": self.commit, "patchSeriesSha256": self.series_sha,
            "archives": archives, **fields,
        }))

    def package(self, arch, *, ok=True):
        return self.run_script("package-kernel.py", "--arch", arch, "--checkout", str(self.checkout),
                               "--allowlist", str(self.allowlist), "--out", str(self.out), ok=ok)

    def fetch(self, arch, *args, ok=True):
        return self.run_script("fetch-kernel.py", "--arch", arch, *args, ok=ok)

    def test_package_fetch_and_refuse_wrong_archive(self):
        pins = {}
        for arch in ARCHITECTURES:
            self.write_build(arch)
            asset = self.out / archive_name(arch)
            self.package(arch)
            pins[arch] = sha256(asset)
            self.package(arch)
            self.assertEqual(sha256(asset), pins[arch], "packaging stable inputs must be deterministic")
        self.assertEqual((self.out / "SHA256SUMS").read_text().splitlines(),
                         [f"{pins[arch]}  {archive_name(arch)}" for arch in sorted(pins)])
        self.write_pins(pins)
        for arch, digest in pins.items():
            with self.subTest(arch=arch):
                asset = self.out / archive_name(arch)
                original = asset.read_bytes()
                # Keep a real archive and publish its modified checksum beside it.
                asset.write_bytes(original + b"\0")
                (self.out / "SHA256SUMS").write_text(f"{sha256(asset)}  {asset.name}\n")
                result = self.fetch(arch, "--local-dir", str(self.out), ok=False)
                self.assertIn("SHA-256 mismatch", result.stderr)
                self.assertFalse((self.root / ".cache/kernel" / digest).exists())
                self.assertFalse(list((self.root / "build/kernel").glob(f"kernel-*/{arch}/{digest}")))

                asset.write_bytes(original)
                self.assertEqual(self.fetch(arch, "--sha256").stdout.strip(), digest)
                installed = Path(self.fetch(arch, "--local-dir", str(self.out)).stdout.strip())
                self.assertEqual(installed.name, digest)
                self.assertEqual(self.fetch(arch, "--installed").stdout.strip(), str(installed))
                self.assertEqual(self.fetch(arch).stdout.strip(), str(installed))
                for name in BINARIES:
                    self.assertEqual((installed / name).stat().st_mode & 0o777, 0o755)
                self.assertTrue((installed / "allowlist/allowlist.json").is_file())
                (self.root / ".cache/kernel" / digest / asset.name).write_bytes(b"damaged cache")
                self.assertIn("SHA-256 mismatch", self.fetch(arch, ok=False).stderr)

    def test_refuse_wrong_architecture_and_mismatched_allowlist(self):
        self.write_build("linux-x86_64")
        wrong_arch = self.package("linux-arm64", ok=False)
        self.assertIn("not an executable ELF64 for linux-arm64", wrong_arch.stderr)
        with (self.allowlist / "coworkerd-syscalls-startup.txt").open("a") as stream:
            stream.write("1\n")
        mismatch = self.package("linux-x86_64", ok=False)
        self.assertIn("allowlist.json says", mismatch.stderr)
        self.assertFalse(self.out.exists() and list(self.out.glob("*.tar.gz")))

    def test_missing_pin_and_stale_series(self):
        arch = "linux-arm64"
        self.write_pins(dict.fromkeys(ARCHITECTURES))
        self.assertEqual(self.fetch(arch, "--installed").stdout, "")
        self.assertIn("Building the kernel from source", self.fetch(arch, ok=False).stderr)
        with (self.patches / "0001-fixture.patch").open("a") as stream:
            stream.write("\n")
        self.assertIn("do not match this upstream and patch series", self.fetch(arch, ok=False).stderr)

    def test_bundle_requires_one_run_and_a_profile_of_the_threaded_binary(self):
        commit = self.git(self.root, "rev-parse", "HEAD").decode().strip()
        env = {**os.environ, "GITHUB_REPOSITORY": "example/kernel", "GITHUB_RUN_ID": "42",
               "GITHUB_RUN_ATTEMPT": "1", "GITHUB_WORKFLOW_SHA": commit}
        pins = {}
        for arch in ARCHITECTURES:
            self.write_build(arch)
            digests = [sha256(self.checkout / relative) for relative in BINARIES.values()]
            allowlist = self.allowlist / "allowlist.json"
            record = json.loads(allowlist.read_text())
            record["workerd"]["sha256s"] = digests
            record["profiles"] = [{"run": name, "workerdSha256": digest} for name, digest in zip(BINARIES, digests)]
            for kind in ("startup", "steady"):
                path = self.allowlist / record[kind]["file"]
                path.write_text(f"# workerd-sha256 {digests[0]}\n# workerd-sha256 {digests[1]}\n0\n")
                record[kind]["sha256"] = sha256(path)
            allowlist.write_text(json.dumps(record))
            self.run_script("package-kernel.py", "--github-run", "--arch", arch, "--allowlist", str(self.allowlist),
                            "--out", str(self.out), env=env)
            pins[arch] = sha256(self.out / archive_name(arch))
        self.run_script("kernel-release.py", "assemble", str(self.out), "--commit", commit, env=env)
        self.run_script("kernel-release.py", "verify", str(self.out), "--commit", commit, env=env)
        wrong_attempt = self.run_script("kernel-release.py", "verify", str(self.out), "--commit", commit,
                                        "--attempt", "2", env=env, ok=False)
        self.assertIn("kernel build record does not match", wrong_attempt.stderr)

        tag = f"kernel-{self.tag}-{self.series_sha[:12]}-run-42-1"
        self.write_pins(pins, releaseTag=tag)
        arch = "linux-arm64"
        installed = Path(self.fetch(arch, "--local-dir", str(self.out)).stdout.strip())
        self.assertEqual(installed.parent.parent.name, tag)
        expected = {"path": str(installed), "archive": {
            "releaseTag": tag, "archiveAsset": archive_name(arch), "archiveSha256": pins[arch],
        }}
        mirrored = self.fetch(arch, "--local-dir", str(self.out), "--json")
        self.assertEqual(json.loads(mirrored.stdout), expected)
        self.assertEqual(json.loads(self.fetch(arch, "--json").stdout), expected)

        record["profiles"].pop()
        allowlist.write_text(json.dumps(record))
        self.run_script("package-kernel.py", "--github-run", "--arch", arch, "--allowlist", str(self.allowlist),
                        "--out", str(self.out), env=env)
        wrong_profile = self.run_script("kernel-release.py", "assemble", str(self.out), "--commit", commit,
                                        env=env, ok=False)
        self.assertIn("syscall profiles must cover the threaded executable", wrong_profile.stderr)

        # A table made after the node stopped starting the upstream binary names the threaded one alone.
        for each in ARCHITECTURES:
            self.write_build(each)
            threaded = sha256(self.checkout / BINARIES[PROFILED[0]])
            record = json.loads(allowlist.read_text())
            record["workerd"]["sha256s"] = [threaded]
            record["profiles"] = [{"run": PROFILED[0], "workerdSha256": threaded}]
            allowlist.write_text(json.dumps(record))
            self.run_script("package-kernel.py", "--github-run", "--arch", each, "--allowlist", str(self.allowlist),
                            "--out", str(self.out), env=env)
        self.run_script("kernel-release.py", "assemble", str(self.out), "--commit", commit, env=env)


if __name__ == "__main__":
    unittest.main()

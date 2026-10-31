"""Deterministic bridge between the workerd patch sequence and local commits."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
FORMAT = ["--zero-commit", "--no-signature", "--no-stat", "--full-index", "--no-numbered", "--no-renames", "--no-notes", "--no-cover-letter",
    "--src-prefix=a/", "--dst-prefix=b/", "--unified=3", "--diff-algorithm=myers", "--no-color", "--no-ext-diff"]
IDENTITY_PATCH = "0015-rpc-invocation-identity.patch"
IDENTITY_PATHS = frozenset({
    "src/workerd/api/worker-loader.h", "src/workerd/api/worker-loader.c++",
    "src/workerd/io/io-channels.h", "src/workerd/io/io-channels.c++",
})
RPC_CODE_PATCH = "0016-rpc-code-parent-death.patch"
RPC_CODE_PATHS = frozenset({
    "src/workerd/jsg/util.c++", "src/workerd/jsg/BUILD.bazel",
    "src/workerd/jsg/coworkerd-rpc-code.h", "src/workerd/jsg/coworkerd-rpc-code.c++",
})

def git(checkout, *args, env=None, input=None):
    return subprocess.check_output(["git", "-c", "core.autocrlf=false", "-c", "diff.noprefix=false", "-c", "core.quotePath=true",
        "-c", "diff.suppressBlankEmpty=false", "-c", "format.subjectPrefix=PATCH", "-c", "format.useAutoBase=false",
        "-C", str(checkout), *args], env=env, input=input)

def allowed_path(name, path):
    # Do not normalize a suspicious spelling into an authorized path.
    canonical = path and not path.startswith("/") and "\\" not in path and all(
        part not in ("", ".", "..") for part in path.split("/"))
    return canonical and (path.startswith("src/workerd/server/") or
                          name == IDENTITY_PATCH and path in IDENTITY_PATHS or
                          name == RPC_CODE_PATCH and path in RPC_CODE_PATHS)

def validate_patch(checkout, name, data):
    """Validate Git's parsed paths and literal headers before any checkout mutation."""
    headers, current, in_hunk = set(), None, False
    rpc_conversion_hunks = 0
    for line in data.splitlines():
        if line.startswith(b"diff --git "):
            match = re.fullmatch(rb"diff --git a/([^\s]+) b/([^\s]+)", line)
            assert match, "patch scope: noncanonical diff header"
            old, new = (part.decode("utf-8") for part in match.groups())
            assert old == new and allowed_path(name, new), "patch scope: " + name + ": " + old + " -> " + new
            headers.add(new)
            current, in_hunk = new, False
        elif line.startswith(b"@@"):
            in_hunk = True
            if name == RPC_CODE_PATCH and current == "src/workerd/jsg/util.c++":
                rpc_conversion_hunks += 1
        elif not in_hunk:
            if line.startswith((b"rename from ", b"rename to ", b"copy from ", b"copy to ")):
                raise AssertionError("patch scope: rename/copy headers are not allowed")
            if line.startswith((b"--- ", b"+++ ")):
                expected = (b"a/" if line.startswith(b"--- ") else b"b/") + (current or "").encode()
                assert current and line[4:] in (expected, b"/dev/null"), "patch scope: inconsistent file header"
            if line.startswith((b"new file mode ", b"old mode ", b"new mode ", b"deleted file mode ")):
                assert line.split()[-1] in (b"100644", b"100755"), "patch scope: non-regular file mode"
    parsed = set()
    for row in git(checkout, "apply", "--numstat", "-z", "-", input=data).split(b"\0"):
        if not row:
            continue
        fields = row.split(b"\t", 2)
        assert len(fields) == 3 and fields[2], "patch scope: ambiguous Git path"
        path = fields[2].decode("utf-8")
        assert allowed_path(name, path), "patch scope: " + name + ": " + path
        parsed.add(path)
    assert headers and headers == parsed, "patch scope: literal and Git paths differ"
    if name == RPC_CODE_PATCH:
        assert rpc_conversion_hunks == 1, "RPC conversion requires exactly one upstream hook"
    return sorted(parsed)

def settings(directory):
    upstream = dict(line.split("=", 1) for line in (directory / "UPSTREAM").read_text().splitlines() if line)
    series = [line for line in (directory / "series").read_text().splitlines() if line and not line.startswith("#")]
    assert len(series) == len(set(series)) and series
    assert all(Path(p).name == p and p.endswith(".patch") for p in series)
    assert len(upstream["commit"]) == 40
    return upstream, series

def clean(checkout):
    assert not git(checkout, "status", "--porcelain"), "checkout must be clean"

def format_commit(checkout, commit):
    return git(checkout, "format-patch", *FORMAT, "-1", "--stdout", commit)

def operate(action, checkout, directory, output=None):
    checkout, directory = checkout.resolve(), directory.resolve()
    upstream, series = settings(directory)
    base = upstream["commit"]
    assert git(checkout, "rev-parse", upstream["tag"]).decode().strip() == base
    patches = [directory / p for p in series]
    # All actions preflight the entire series. A bad final patch cannot leave an apply/import
    # half-finished or overwrite part of an export.
    scopes = {patch.name: validate_patch(checkout, patch.name, patch.read_bytes()) for patch in patches}
    if action in ("apply", "import"):
        clean(checkout)
        assert git(checkout, "rev-parse", "HEAD").decode().strip() == base
        if action == "apply":
            for patch in patches:
                git(checkout, "apply", "--index", str(patch))
        else:
            git(checkout, "-c", "user.name=Codex", "-c", "user.email=codex@openai.com", "am", "--committer-date-is-author-date", *map(str, patches))
    elif action == "export":
        clean(checkout)
        commits = git(checkout, "rev-list", "--reverse", "--first-parent", base + "..HEAD").decode().splitlines()
        assert len(commits) == len(series), "one commit per patch is required"
        target = output.resolve() if output else directory
        target.mkdir(parents=True, exist_ok=True)
        exported = []
        for name, commit in zip(series, commits):
            data = format_commit(checkout, commit)
            validate_patch(checkout, name, data)
            exported.append((name, data))
        for name in ("UPSTREAM", "series"):
            if target != directory:
                (target / name).write_bytes((directory / name).read_bytes())
        for name, data in exported:
            (target / name).write_bytes(data)
    else:
        with tempfile.TemporaryDirectory(prefix="coworkerd-patches-") as temporary:
            environment = {**os.environ, "GIT_INDEX_FILE": str(Path(temporary) / "expected")}
            git(checkout, "read-tree", base, env=environment)
            for patch in patches:
                git(checkout, "apply", "--cached", str(patch), env=environment)
            expected_tree = git(checkout, "write-tree", env=environment).decode().strip()
            expected = git(checkout, "diff", "--cached", "--binary", "--full-index", "--no-ext-diff", "--no-renames", base, env=environment)
            environment["GIT_INDEX_FILE"] = str(Path(temporary) / "actual")
            git(checkout, "read-tree", "HEAD", env=environment)
            git(checkout, "add", "-A", env=environment)
            actual_tree = git(checkout, "write-tree", env=environment).decode().strip()
            actual = git(checkout, "diff", "--cached", "--binary", "--full-index", "--no-ext-diff", "--no-renames", base, env=environment)
            assert actual == expected, "complete checkout difference does not match patch sequence byte for byte"
            paths = git(checkout, "diff", "--cached", "--name-only", base, env=environment).decode().splitlines()
            assert paths and all(any(allowed_path(name, p) for name in series) for p in paths), paths
            assert actual_tree == expected_tree, "complete Git trees differ"
        return {"passed": True, "upstream": upstream, "diffSha256": hashlib.sha256(actual).hexdigest(),
                "expectedTree": expected_tree, "actualTree": actual_tree, "modifiedFiles": paths, "patchScopes": scopes}
    return {"passed": True, "operation": action, "upstream": upstream, "patches": series, "patchScopes": scopes}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("apply", "import", "export", "check"))
    parser.add_argument("--checkout", type=Path, default=ROOT / "workerd")
    parser.add_argument("--patch-dir", type=Path, default=ROOT / "patches/workerd")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    print(json.dumps(operate(args.operation, args.checkout, args.patch_dir, args.output_dir), indent=2))

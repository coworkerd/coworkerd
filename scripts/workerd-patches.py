"""Deterministic bridge between the workerd patch sequence and local commits."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
FORMAT = ["--zero-commit", "--no-signature", "--no-stat", "--full-index", "--no-numbered", "--no-renames", "--no-notes", "--no-cover-letter",
    "--src-prefix=a/", "--dst-prefix=b/", "--unified=3", "--diff-algorithm=myers", "--no-color", "--no-ext-diff"]

def git(checkout, *args, env=None):
    return subprocess.check_output(["git", "-c", "core.autocrlf=false", "-c", "diff.noprefix=false", "-c", "core.quotePath=true",
        "-c", "diff.suppressBlankEmpty=false", "-c", "format.subjectPrefix=PATCH", "-c", "format.useAutoBase=false",
        "-C", str(checkout), *args], env=env)

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
        for name in ("UPSTREAM", "series"):
            if target != directory:
                (target / name).write_bytes((directory / name).read_bytes())
        for name, commit in zip(series, commits):
            paths = git(checkout, "diff-tree", "--no-commit-id", "--name-only", "-r", commit).decode().splitlines()
            assert paths and all(p.startswith("src/workerd/server/") for p in paths), paths
            (target / name).write_bytes(format_commit(checkout, commit))
    else:
        with tempfile.TemporaryDirectory(prefix="coworkerd-patches-") as temporary:
            environment = {**os.environ, "GIT_INDEX_FILE": str(Path(temporary) / "expected")}
            git(checkout, "read-tree", base, env=environment)
            for patch in patches:
                git(checkout, "apply", "--cached", str(patch), env=environment)
            expected = git(checkout, "diff", "--cached", "--binary", "--full-index", "--no-ext-diff", "--no-renames", base, env=environment)
            environment["GIT_INDEX_FILE"] = str(Path(temporary) / "actual")
            git(checkout, "read-tree", "HEAD", env=environment)
            git(checkout, "add", "-A", env=environment)
            actual = git(checkout, "diff", "--cached", "--binary", "--full-index", "--no-ext-diff", "--no-renames", base, env=environment)
            assert actual == expected, "complete checkout difference does not match patch sequence byte for byte"
            paths = git(checkout, "diff", "--cached", "--name-only", base, env=environment).decode().splitlines()
            assert paths and all(p.startswith("src/workerd/server/") for p in paths), paths
        return {"passed": True, "upstream": upstream, "diffSha256": hashlib.sha256(actual).hexdigest(), "modifiedFiles": paths}
    return {"passed": True, "operation": action, "upstream": upstream, "patches": series}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("apply", "import", "export", "check"))
    parser.add_argument("--checkout", type=Path, default=ROOT / "workerd")
    parser.add_argument("--patch-dir", type=Path, default=ROOT / "patches/workerd")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    print(json.dumps(operate(args.operation, args.checkout, args.patch_dir, args.output_dir), indent=2))

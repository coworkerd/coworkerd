#!/usr/bin/env python3
"""Publish an already verified run artifact, resuming a draft without replacing assets."""

import json
import os
from pathlib import Path
import subprocess
import sys

from kernel_artifacts import ARCHITECTURES, archive_name, run_identity, sha256


def gh(*args):
    return subprocess.check_output(["gh", *args], text=True)


def publish(directory):
    repository = os.environ["GITHUB_REPOSITORY"]
    run = run_identity(repository, os.environ["RUN_ID"], os.environ["RUN_ATTEMPT"], os.environ["WORKFLOW_COMMIT"])
    tag = os.environ["RELEASE_TAG"]
    commit = os.environ["SOURCE_COMMIT"]
    notes = f"Coworkerd source `{commit}`\n\n[Verified build attempt]({run['url']})\n"

    files = [directory / archive_name(arch) for arch in sorted(ARCHITECTURES)]
    hashes = {file.name: sha256(file) for file in files}
    sums = directory / "SHA256SUMS"
    if sums.read_text() != "".join(f"{digest}  {name}\n" for name, digest in hashes.items()):
        raise ValueError("SHA256SUMS does not match both kernel archives")
    files.append(sums)
    hashes[sums.name] = sha256(sums)

    # List drafts as well as published releases. A failed upload leaves a draft to resume.
    matches = gh("api", "--paginate", f"repos/{repository}/releases?per_page=100",
                 "--jq", f".[] | select(.tag_name == {json.dumps(tag)}) | .id")
    if not matches.strip():
        gh("release", "create", tag, "--repo", repository, "--draft", "--prerelease",
           "--target", run["workflowCommit"], "--title", tag, "--notes", notes)

    def release():
        state = json.loads(gh("release", "view", tag, "--repo", repository,
                              "--json", "isDraft,isPrerelease,assets,url"))
        if not state["isPrerelease"]:
            raise ValueError(f"{tag} must be a pre-release")
        present = {asset["name"]: asset["digest"] for asset in state["assets"]}
        for name, digest in present.items():
            if name not in hashes or digest != f"sha256:{hashes[name]}":
                raise ValueError(f"{tag} already contains an unexpected asset: {name}")
        return state, present

    state, present = release()
    if state["isDraft"]:
        missing = [str(file) for file in files if file.name not in present]
        if missing:
            gh("release", "upload", tag, *missing, "--repo", repository)
        state, present = release()
        if present.keys() != hashes.keys():
            raise ValueError(f"{tag} is missing verified assets")
        gh("release", "edit", tag, "--repo", repository, "--draft=false", "--prerelease", "--latest=false")
        state, present = release()
    if state["isDraft"] or present.keys() != hashes.keys():
        raise ValueError(f"{tag} is not a published pre-release with all verified assets")
    print(state["url"])
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
        print(f"[Published `{tag}`]({state['url']}) from [build run]({run['url']}).", file=output)


if __name__ == "__main__":
    try:
        publish(Path(sys.argv[1]))
    except (KeyError, OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(f"publish-kernel: {error}")

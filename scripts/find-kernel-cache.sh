#!/usr/bin/env bash
# Discover immutable, same-repository cache artifacts made by a default-branch push.
set -euo pipefail

repo="repos/$GITHUB_REPOSITORY"
if [[ -n ${BENCHMARK_RUN:-} ]]; then
  [[ $GITHUB_REF_TYPE == branch && $GITHUB_REF_NAME != "$DEFAULT_BRANCH" && $BENCHMARK_RUN =~ ^[1-9][0-9]*$ ]]
  run_ids=$(gh api "$repo/actions/runs/$BENCHMARK_RUN" |
    jq -r --arg repo "$GITHUB_REPOSITORY" --arg branch "$GITHUB_REF_NAME" '
      select(.head_repository.full_name == $repo and .event == "workflow_dispatch" and
        .path == ".github/workflows/kernel-build.yml" and .head_branch == $branch and .conclusion == "success") | .id')
else
  run_ids=$(gh api --method GET "$repo/actions/workflows/kernel-build.yml/runs" \
    -f event=push -f branch="$DEFAULT_BRANCH" -f status=success -F per_page=10 |
    jq -r --arg repo "$GITHUB_REPOSITORY" '.workflow_runs[] | select(.head_repository.full_name == $repo) | .id')
fi

for run in $run_ids; do
  artifact_id=$(gh api --method GET "$repo/actions/runs/$run/artifacts" -f name="$ARTIFACT" |
    jq -r --arg name "$ARTIFACT" '.artifacts[] | select(.name == $name and .expired == false) | .id')
  if [[ $artifact_id =~ ^[1-9][0-9]*$ ]]; then
    printf 'run_id=%s\nartifact_id=%s\n' "$run" "$artifact_id" >> "$GITHUB_OUTPUT"
    exit 0
  fi
done
echo 'No compatible Bazel action cache found. Building without a seed.'

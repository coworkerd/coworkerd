#!/usr/bin/env bash
# Discover immutable cache artifacts made by the default branch's compile jobs.
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
    -f branch="$DEFAULT_BRANCH" -f status=completed -F per_page=50 |
    jq -r --arg repo "$GITHUB_REPOSITORY" --arg branch "$DEFAULT_BRANCH" '
      .workflow_runs[] | select(.head_repository.full_name == $repo and .head_branch == $branch and
        (.event == "push" or .event == "schedule") and (.conclusion == "success" or .conclusion == "failure")) | .id')
fi

for run in $run_ids; do
  # A successful upload is per architecture. The other architecture may have failed.
  asset=$(gh api "$repo/actions/runs/$run/artifacts" |
    jq -r --arg prefix "$CACHE_PREFIX" --arg current "$CURRENT_CACHE" '
      first(.artifacts[] | select((.name | startswith($prefix)) and .expired == false) |
        [.id, (.name == $current and ((.expires_at | fromdateiso8601) > now + 86400))] | @tsv) // empty')
  if [[ -n $asset ]]; then
    read -r artifact_id fresh <<< "$asset"
    printf 'run_id=%s\nartifact_id=%s\nfresh=%s\n' "$run" "$artifact_id" "$fresh" >> "$GITHUB_OUTPUT"
    exit 0
  fi
done
echo 'No compatible Bazel action cache found. Building without a seed.'

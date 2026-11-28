#!/usr/bin/env bash
# Discover immutable cache artifacts made by the default branch's compile jobs.
set -euo pipefail

repo="repos/$GITHUB_REPOSITORY"
# Allow two extra days beyond artifact retention for time spent queuing and building.
since=$(jq -nr 'now - 9 * 86400 | todateiso8601')
completed_runs() {
  gh api --method GET --paginate --slurp "$repo/actions/workflows/kernel-build.yml/runs" \
    -f branch="$DEFAULT_BRANCH" -f event="$1" -f status=completed -f "created=>=$since" -F per_page=100
}
push_runs=$(completed_runs push)
scheduled_runs=$(completed_runs schedule)
run_ids=$(printf '%s\n' "$push_runs" "$scheduled_runs" |
  jq -rs --arg repo "$GITHUB_REPOSITORY" --arg branch "$DEFAULT_BRANCH" '
    [.[][] | .workflow_runs[] | select(.head_repository.full_name == $repo and .head_branch == $branch and
      (.event == "push" or .event == "schedule") and (.conclusion == "success" or .conclusion == "failure"))]
    | sort_by(.created_at, .id) | reverse | .[].id')

checked=0
for run in $run_ids; do
  checked=$((checked + 1))
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
printf 'No compatible Bazel action cache after checking artifacts in %d completed push and schedule runs on `%s` created since `%s`. Building without a seed.\n' \
  "$checked" "$DEFAULT_BRANCH" "$since" | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"

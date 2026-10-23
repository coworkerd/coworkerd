# Patches

Coworkerd runs upstream [workerd](https://github.com/cloudflare/workerd) with a thin series of patches.

| Path | What it is |
| --- | --- |
| `workerd/UPSTREAM` | The upstream tag and commit the series applies to (`v1.20261012.1`, `5358e3d`) |
| `workerd/series` | The order of the patches |
| `workerd/0001-native-binding.patch` ... | One patch per file; the header of each states its purpose |
| `kj-http-abort.diff` | A change to Cap'n Proto's KJ that `CMakeLists.txt` applies when it builds the daemons: KJ's HTTP entity writers honor `abortWrite()`, so a response that fails midway ends with an error instead of looking complete |

## How the series is maintained

- **The fork.** The patched workerd is the fork [coworkerd/workerd](https://github.com/coworkerd/workerd): one branch
  per upstream tag, `coworkerd-<tag>` (now `coworkerd-v1.20261012.1`), which is the tag plus one commit per patch.
  `patches/workerd/` is exported from that branch so that every change to workerd can be reviewed here; a change to
  the branch and its exported copy go into the same change of this repository.
- **Thin patches.** Changes stay in `src/workerd/server/` (`scripts/workerd-patches.py` checks it); the logic lives in
  new files of our own, upstream files only get hooks, and upstream's command line entry is not touched. The fewer the
  hooks, the cheaper following upstream is.
- **Following upstream.** About every four weeks, and at once when V8 has a security fix: a multi-tenant platform
  runs mutually untrusting code, and lagging behind upstream is itself an exposure. V8 patches, if any are needed, go
  into the fork's `patches/v8/`, where upstream keeps its own.
- **A contract with the daemons.** The patches and the node change together (the CPU limit's parameters, the culprit
  reports and the descriptors workerd inherits are a contract between them), and a release's `VERSION` names the
  upstream tag and the series it was built from.

## Working on the series

`scripts/workerd-patches.py` goes back and forth between the files here and commits in a workerd checkout (by default
`workerd/`, which this repository ignores):

```sh
git clone https://github.com/cloudflare/workerd.git workerd
git -C workerd switch --detach "$(sed -n 's/^tag=//p' patches/workerd/UPSTREAM)"
python3 scripts/workerd-patches.py import     # one commit per patch (apply: the changes staged, no commits)
python3 scripts/workerd-patches.py check      # the checkout is exactly UPSTREAM plus the series
```

To follow a new tag, rebase the commits onto it (in the fork, as the branch `coworkerd-<new tag>`), resolve conflicts
keeping only hooks in upstream files, write the new tag and commit into `UPSTREAM`, and export:

```sh
git -C workerd fetch --depth=1 https://github.com/cloudflare/workerd.git "refs/tags/$TAG:refs/tags/$TAG"
git -C workerd rebase --onto "$TAG" "$(sed -n 's/^commit=//p' patches/workerd/UPSTREAM)"
python3 scripts/workerd-patches.py export
python3 scripts/workerd-patches.py check
```

The export is deterministic (no commit hashes, signatures or statistics): exporting the same commits twice gives the
same files. A new series also needs the compatibility flags the node accepts regenerated
(`python3 scripts/compatibility-flags.py`, into `common/generated/`), new workerd builds for both architectures,
their system call allowlists (`scripts/syscall-profile.mjs`, then `scripts/syscall-allowlist.py`) and the pinned
hashes in `scripts/release.sh`. `check` is the source side of checking a sync: the checkout differs from the tag in
`UPSTREAM` by exactly the series, and only under `src/workerd/server/`.

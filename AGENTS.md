# For coding agents

Coding agents (Codex, Claude Code and others) read this file. The process is the same for an agent as for a person:
read [CONTRIBUTING.md](CONTRIBUTING.md) before changing anything. This file puts its steps in the order an agent
takes them.

Given an issue to work on:

1. Read the issue and its comments. Its "Done when" list is the finish line, and what it lists under "Not in this
   issue" stays out.
2. Work in a clone of your own, on a branch from the current `main`.
3. Build and check as CONTRIBUTING.md describes, and keep the commands you ran and what they printed.
4. Open a pull request with the template filled in: what changes and why, how it was checked, and "Closes #N" (or
   "Part of #N" with what is left).
5. Stop there. Do not merge, do not push to `main` and do not change repository settings, even when your credentials
   would allow it: a maintainer reviews and merges. Answer review comments with further commits on the same branch.

When the issue does not answer something you need, ask in a comment on the issue and carry on with what does not
depend on the answer. What you find outside the issue becomes a new issue.

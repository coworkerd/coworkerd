# coworkerd-launcher: interface with the Server

The launcher starts one workerd process in its least-privilege environment and then becomes it (`execv`). It is
built with the Server (CMake target `coworkerd-launcher`, Linux only) and installed beside it as
`bin/coworkerd-launcher`. This file fixes how the Server and the launcher talk.

## Command line

```
coworkerd-launcher [OPTION...] -- PROGRAM ARG...
coworkerd-launcher --version
```

Everything after `--` is workerd's command line exactly as the Server would start it without the launcher
(`<workerd> serve --experimental --socket-fd rpc=4 <stack>/processes/<slot>.capnp`). The options are read from
the command line only; the launcher reads no environment variable and changes none except `PWD` (set to `/`,
the new working directory).

| Option | Meaning |
| --- | --- |
| `--status-fd N` | the Server's end of the status socket (below); made close-on-exec at once |
| `--program-fd N` | the workerd file the Server opened (made close-on-exec at once). `PROGRAM`'s path is bound read-only into the new root and must be that same file -- device and inode compared after the bind -- so the bytes executed are the ones the Server identified; a file replaced in between fails the launch (step `binds`). The descriptor itself cannot be the bind's source: its mount belongs to the Server's mount namespace, and the kernel binds only mounts of the launcher's own (`EINVAL`) |
| `--parent-pid PID` | the Server's pid: the parent-death signal is checked against it |
| `--root DIR` | a new mount namespace with a tmpfs root mounted on `DIR` (an existing directory of the Server; the tmpfs exists only in the new namespace) |
| `--bind PATH` / `--bind-rw PATH` / `--bind-optional PATH` | `PATH` of the Server's root appears at the same path inside, read-only / read-write / read-only and skipped when it does not exist |
| `--user` | a user namespace; this process is `--uid`/`--gid` inside (default 65534), mapped to its own ids outside (one id each way, `setgroups` denied) |
| `--net`, `--ipc`, `--uts` | network (only loopback, down), IPC and UTS namespaces; `--hostname NAME` (default `workerd`) |
| `--drop-caps` | every capability dropped: bounding, permitted, effective, inheritable and ambient sets |
| `--no-new-privs` | `PR_SET_NO_NEW_PRIVS` |
| `--non-dumpable` | `PR_SET_DUMPABLE 0` (see "Layers" for what survives the `execv`) |
| `--seccomp MODE` | the system call allowlist's mode: `off` (no filter), `observe` (a call outside the list is reported to the Server and then proceeds) or `enforce` (it ends the process). The filter is the last step before the `execv`, so it is in force for workerd itself |
| `--allow-file FILE` | the allowlist the build generated (`scripts/syscall-allowlist.py`): this architecture's call numbers, one per line, everything else a comment -- among them a `# workerd-sha256 <hex>` line for **every** workerd the list belongs to (at most 8) and `# name <number> <name>`, the table the Server reports an observed call by. Read before the mount namespace is built, since the file is a path of the Server's root, and read again at every launch, so a list put in place while the Server runs takes effect for the launches that follow |
| `--program-sha256 HEX` | the digest the Server has for the workerd file of this launch. It must be **among** the digests the allowlist names: a list that does not cover these bytes would end processes in the enforce mode and report calls that are not this binary's in the observe one. The Server passes the digest of the very file it opened as `--program-fd`, and that file is what gets bound, so the comparison is between the allowlist and what actually runs. No match fails the launch at step `allowlist`; the `exec` record says which digest matched and how many the list names, which is what makes an upgrade possible: a list covering the binary in place and the one about to replace it is installed first, then the binary is replaced (docs/guide/install.md) |
| `--check` | everything above, then exit 0 instead of executing `PROGRAM` (the Server's startup check) |

The Server passes, for every process: `--status-fd 7 --program-fd 8 --parent-pid <its pid> --root <stack>/sandbox
--user --net --ipc --uts --drop-caps --no-new-privs --non-dumpable`, and when the allowlist is in effect
`--seccomp <mode> --allow-file <path> --program-sha256 <hex>`, the ELF interpreter and the libraries of the
workerd file as its dynamic loader resolves them (`<interpreter> --list <workerd>`, once per workerd file),
`/dev/null` (read-write), `/dev/urandom`, `<stack>/processes` (this generation's configuration and runtime
module), `<stack>/run` (read-write: its listening socket and the two callback sockets) and, optional,
`/sys/devices/system/cpu` and `/etc/localtime`; the stack's paths and the libraries come from the Server rather
than being fixed. Every path keeps its absolute name inside, so workerd's command
line, its configuration (`embed`, the `unix:` callback addresses) and its descriptors are unchanged.

## Descriptors

- **0–6 are workerd's and are never touched**: 1, 2 output pipes; 3 the limit event pipe; 4 the listening Unix
  socket; 5 the lifecycle shared memory; 6 the memory band shared memory (the numbers of `process-pool.h`).
- **7 and 8 are the launcher's own** (the numbers are the Server's choice, given with `--status-fd` and
  `--program-fd`): the launcher marks both close-on-exec before anything else, so the `execv` closes them and
  workerd's descriptor table is the one it has without the launcher. The observe mode's notification listener
  is closed the same way, right after it has been sent to the Server, so it never reaches workerd either.

## Status records and failures

Descriptor 7 is a **Unix socket** (`socketpair`), not a pipe: each record is one line of JSON written with one
`sendmsg`, and the record of an observe-mode launch carries the filter's notification listener with it
(`SCM_RIGHTS`). The Server reads the socket with `recvmsg` as the records arrive -- not only once the
generation is ready -- because until it holds that listener a call outside the allowlist would wait for a
supervisor that is not there yet. The Server is that supervisor: it answers every notification with
"let the call proceed" (`SECCOMP_USER_NOTIF_FLAG_CONTINUE`), on a thread of its own per generation, since the
kernel's receive is a blocking wait whatever the descriptor's flags say. Because the Server is the supervisor,
the observations are its own records -- the `[syscall-observed]` lines of its output and
`processes.sandbox.syscalls` -- and not something the launcher passes on; nothing of this goes through the limit
event pipe (descriptor 3), whose end is workerd's own evidence.

Each record is one line of JSON:

| `event` | When | Fields |
| --- | --- | --- |
| `exec` | right before the `execv` of workerd | `protocol`, `version`, `pid`, `program`, `layers` (what the launcher applied: `user`, `mount`, `network`, `ipc`, `uts`, `capabilities`, `noNewPrivs`, `nonDumpable`, `parentDeathSignal`, `systemCalls`), `uid`, `gid` (inside), `binds`, `optionalSkipped`, `systemCalls` (`mode`, `allowFile`, `workerdSha256` -- the digest of the allowlist's that this launch matched --, `workerdFiles`, `allowed`, `instructions`, `listener`), `elapsedMicros` |
| `check` | `--check` passed | the same fields |
| `failed` | a step failed before workerd was executed | `protocol`, `version`, `pid`, `step` (`options`, `allowlist`, `unshare`, `id-maps`, `hostname`, `root`, `binds`, `pivot`, `capabilities`, `privileges`, `parent`, `seccomp`, `exec`), `error`, `errno`, `errnoText` |

A launch that fails **exits with status 125** after writing its `failed` record to the status descriptor and,
prefixed with `[coworkerd-launcher] `, to standard error (which is workerd's output pipe, so the line also reaches
`logs/workerd.log`). A failed `execv` writes `failed` after `exec`: the last record is the outcome. On a successful
`execv` the status pipe reaches its end (the launcher's write end closes with the `execv`, the Server closed its own
copy after the spawn).

The Server reads the pipe without blocking when the generation answers its readiness probe (the `exec` record is
then complete) and when the process exits. A process that exits before it is ready with a `failed` record, or with
no `exec` record at all, is a **launch failure** of that slot (state endpoint: the slot's `lastExitCategory`
`launch-failed`, `launchFailures`, `lastLaunchFailure`, and `processes.recentLaunchFailures`), not a workerd crash;
the slot is restarted with the restart backoff it always had (`processPool.restartBackoffMs` doubling up to
`restartMaxMs`), and no generation, rotation or limit state is attributed to it.

## Parent death

As the last step before the `execv` (after every credential change, which would clear it) the launcher sets
`PR_SET_PDEATHSIG` to `SIGKILL`, then compares `getppid()` with `--parent-pid`: when the Server died in between,
the launcher is already reparented and exits (`failed`, step `parent`). The setting survives the `execv` of workerd
(a plain, non-set-id file), so when the Server's process ends -- the thread that spawned it, which is the Server's
event loop thread -- the kernel kills the workerd processes of that Server, and the next Server in the same stack
directory finds their sockets refused and removes them (its start-up probe) instead of refusing to start.

## Layers

What the launcher applies are the layers the `exec` record lists under `layers`, the system call allowlist among them.
The filter is built this way: the architecture checked, the x32 range rejected, one equality test
per allowed call, then the action for everything else -- `SECCOMP_RET_USER_NOTIF` in the observe mode,
`SECCOMP_RET_KILL_PROCESS` in the enforce one. It goes on as the last step before the `execv`, so the
allowlist has to contain the calls the launcher itself still makes (`execve`, `sendmsg`, `write`,
`clock_gettime`, `getpid`, `exit_group`); the generator adds them with their reason, and everything the
record needs is read before the filter is installed. A startup allowlist necessarily allows more than the
steady state needs -- the generator produces the steady list as well, for the in-process tightening. The Server does not take the launcher's word for what is in effect: once a generation is ready it reads the
process's `/proc/<pid>` (`ns/*` against its own, `uid_map`, `status`, `mountinfo`, `net/dev`) and reports those
observations per process (state endpoint `sandbox.layers`).

`nonDumpable`: `PR_SET_DUMPABLE 0` is applied, but the kernel sets a process back to dumpable when it executes a
file it may read (`begin_new_exec`: `SUID_DUMP_USER` unless the program or its interpreter is unreadable), so workerd
runs dumpable. The record says what the launcher applied; the Server's observation (`/proc/<pid>/environ` owned by
the process's own uid rather than root) says what is in effect.

## Exit statuses

| Status | Meaning |
| --- | --- |
| 125 | the launch failed before workerd was executed (`failed` record) |
| none: signal 31 | the enforce mode's filter ended the process on a call outside the allowlist, so it has no exit status at all: it was signalled (`SIGSYS`). The Server's exit category for such an exit is `syscall-denied` |
| 0 | `--check` or `--version` passed |
| anything else | workerd's own: after the `execv` the process is workerd |

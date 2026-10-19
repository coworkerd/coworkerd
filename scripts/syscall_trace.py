"""Parse and summarise the strace output of a workerd profile, for scripts/syscall-summarise.py (in the container,
while profiling) and for anything that recomputes a profile from its stored traces.

A trace is one workerd process: `strace -f -ttt -qq -yy -s 200 [-k]` attached to the wrapper shell, which
then executes the workerd file the profile names (`program`, default the environment's COWORKERD_TRACE_PROGRAM). Only the threads of workerd count: from the main pid's execve of the
workerd binary on, plus every thread a workerd thread creates (clone/clone3 results); the wrapper's own
helpers (grep, sleep) are left out. Each call is put in a phase by its start time: startup (execve to the
time the Server saw the process ready), steady (after that), and the steady phase is further split by
the profile run's workload windows. With -k every call is followed by its stack (" > " frame lines).
"""
import collections
import re
import subprocess

LINE = re.compile(r'^(\d+)\s+(\d+\.\d+)\s+(.*)$')
CALL = re.compile(r'^([a-z0-9_]+)\(')
RESUMED = re.compile(r'^<\.\.\. ([a-z0-9_]+) resumed>')
RESULT = re.compile(r'\)\s+=\s+(-?\d+|\?|0x[0-9a-f]+)(?:\s+([A-Z][A-Z0-9_]+))?')
FRAME = re.compile(r'^\s*> (.*)$')
import os
WORKERD = os.environ.get('COWORKERD_TRACE_PROGRAM', '/v45/bin/workerd')
# Calls whose first string argument is a path (the path that the call names).
PATH_CALLS = {'open', 'openat', 'openat2', 'stat', 'lstat', 'newfstatat', 'statx', 'access', 'faccessat', 'faccessat2',
              'readlink', 'readlinkat', 'execve', 'execveat', 'mkdir', 'mkdirat', 'unlink', 'unlinkat', 'rename', 'renameat',
              'renameat2', 'chdir', 'chroot', 'truncate', 'statfs', 'getxattr', 'lgetxattr', 'listxattr', 'inotify_add_watch',
              'symlink', 'symlinkat', 'link', 'linkat', 'mknod', 'mknodat', 'chmod', 'fchmodat', 'chown', 'fchownat', 'utimensat',
              'name_to_handle_at', 'mount', 'umount2', 'pivot_root', 'swapon', 'uselib'}
FIRST_STRING = re.compile(r'"((?:[^"\\]|\\.)*)"')
DECODED_FD = re.compile(r'^[a-z0-9_]+\((-?\d+)<([^>]*)>')
CONNECT_PATH = re.compile(r'sun_path="((?:[^"\\]|\\.)*)"')


def calls(lines, attached=False, program=None):
    """Every traced call with its stack: a record is complete once the line after its last frame is read."""
    held = None
    for record in _calls(lines, attached, program):
        if held is not None and held is not record:
            yield held
        held = record
    if held is not None:
        yield held


def _calls(lines, attached=False, program=None):
    """Yields one record per traced call of workerd's threads: tid, time, name, body, result, errno, stack.
    `attached`: the trace comes from `strace -f -p <workerd>` (every thread counts from the first line).
    `program`: the file whose execve starts the trace (default WORKERD)."""
    program = program or WORKERD
    main = None          # the pid of the wrapper, which becomes workerd at its execve
    started = attached
    threads = set()
    pending = {}         # tid -> (time, name, body) of an <unfinished ...> call
    last = None
    for raw in lines:
        line = raw.rstrip('\n')
        frame = FRAME.match(line)
        if frame:
            if last is not None:
                last['stack'].append(frame.group(1))
            continue
        match = LINE.match(line)
        if not match:
            continue
        tid, stamp, body = int(match.group(1)), float(match.group(2)), match.group(3)
        if main is None:
            main = tid
        if body.startswith('---') or body.startswith('+++'):
            last = None
            continue
        resumed = RESUMED.match(body)
        if resumed:
            if tid not in pending:
                last = None
                continue
            when, name, head = pending.pop(tid)
            body = head + body[resumed.end():]
            stamp = when
        else:
            call = CALL.match(body)
            if not call:
                last = None
                continue
            name = call.group(1)
            if body.endswith('<unfinished ...>'):
                pending[tid] = (stamp, name, body[:-len('<unfinished ...>')].rstrip())
                last = None
                # An execve of the workerd binary by the main pid starts the trace even before it returns.
                if not started and tid == main and name == 'execve' and f'"{program}"' in body:
                    started = True
                    threads.add(main)
                continue
        if attached:
            threads.add(tid)
        if not started:
            if tid == main and name == 'execve' and f'"{program}"' in body and not body.rstrip().endswith('= -1'):
                started = True
                threads.add(main)
            else:
                last = None
                continue
        if tid not in threads:
            last = None
            continue
        result = RESULT.search(body)
        value = result.group(1) if result else None
        errno = result.group(2) if result else None
        if name in ('clone', 'clone3', 'fork', 'vfork') and value and value.isdigit() and int(value) > 0:
            threads.add(int(value))
        last = {'tid': tid, 't': stamp, 'name': name, 'body': body, 'result': value, 'errno': errno, 'stack': []}
        yield last


def path_of(record):
    name, body = record['name'], record['body']
    if name in PATH_CALLS:
        found = FIRST_STRING.search(body)
        return found.group(1) if found else None
    if name in ('connect', 'bind'):
        found = CONNECT_PATH.search(body)
        return found.group(1) if found else None
    return None


def fd_target(record):
    found = DECODED_FD.match(record['body'])
    return (int(found.group(1)), found.group(2)) if found else None


def phase_of(t, ready, windows):
    """startup before `ready` (seconds); otherwise the name of the workload window holding t, else 'steady-other'."""
    if ready is None or t < ready:
        return 'startup'
    for window in windows:
        if window['startUnixMs'] / 1000 <= t <= window['endUnixMs'] / 1000:
            return window['name']
    return 'steady-other'


# Attribution of a call to the code that made it, from its stack (-k). Frames are "<object>(<symbol>+<offset>) [<address>]".
FRAME_PARTS = re.compile(r'^(\S+?)\((.*?)(?:\+0x[0-9a-f]+)?\)\s*\[0x[0-9a-f]+\]')
GLIBC = ('libc.so', 'libm.so', 'ld-linux', 'libpthread')
CATEGORIES = [
    ('our patches', ('coworkerd', 'Coworkerd', 'CpuLimit', 'cpuLimit', 'IdleReaper', 'idleEviction', 'LifecycleChannel', 'MemoryLimit',
                     'memoryLimit', 'limitWatchdog', 'LimitWatchdog', 'MemoryBand', 'inheritedBand', 'mapInherited', 'eventFd', 'EventChannel')),
    ('V8', ('v8::', 'V8::', 'cppgc::', 'heap::base::')),
    ('tcmalloc', ('tcmalloc', 'TCMalloc', 'MallocExtension', 'absl::')),
    ('Rust I/O (kj-rs, tokio)', ('tokio', 'mio::', 'std::sys', 'std::thread', 'std::rt', 'kj_rs', 'cxx::', 'rust_', 'core::ops')),
    ('BoringSSL', ('bssl', 'RAND_', 'CRYPTO_', 'boringssl', 'OPENSSL_')),
    ('ICU', ('icu_', 'icu::')),
    ('workerd', ('workerd::',)),
    ('KJ / Cap\'n Proto', ('kj::', 'capnp::')),
    ('glibc startup / loader', ('_dl_', '__libc_start', '_start', 'dl_main', '__libc_early_init', '_init', 'init_cacheinfo')),
]


def classify(symbol):
    for category, needles in CATEGORIES:
        if any(n in symbol for n in needles):
            return category
    return 'other'


def attribution(stack, demangle=lambda s: s):
    """(category, symbol): the first frame outside glibc and the loader that has a symbol; falls back to glibc."""
    first_glibc = None
    for frame in stack:
        parts = FRAME_PARTS.match(frame.strip())
        if not parts:
            continue
        obj, symbol = parts.group(1), parts.group(2)
        if any(g in obj for g in GLIBC):
            if first_glibc is None and symbol:
                first_glibc = symbol
            if symbol and ('_dl_' in symbol or symbol.startswith('_dl') or 'dl_main' in symbol or '__libc_start' in symbol):
                return 'glibc startup / loader', symbol
            continue
        if not symbol:
            continue
        name = demangle(symbol)
        return classify(name), name
    return ('glibc', first_glibc or '?')


def summarise(lines, ready, windows, demangle=None, attached=False, program=None):
    """Counts per phase and call, the paths named, the directory descriptors used, thread names, errors."""
    counts = collections.defaultdict(collections.Counter)
    errors = collections.defaultdict(collections.Counter)
    paths = collections.Counter()
    descriptors = collections.defaultdict(collections.Counter)
    thread_names = {}
    tids = collections.defaultdict(collections.Counter)
    stacks = collections.defaultdict(collections.Counter)
    first = last = None
    for record in calls(lines, attached, program):
        t = record['t']
        first = t if first is None else first
        last = t
        phase = phase_of(t, ready, windows)
        counts[phase][record['name']] += 1
        tids[phase][record['tid']] += 1
        if record['errno']:
            errors[phase][f"{record['name']} {record['errno']}"] += 1
        if record['name'] == 'prctl' and 'PR_SET_NAME' in record['body']:
            found = FIRST_STRING.search(record['body'])
            if found:
                thread_names[record['tid']] = found.group(1)
        path = path_of(record)
        if path is not None:
            paths[(phase if phase == 'startup' else 'steady', record['name'], path, record['errno'] or 'ok')] += 1
        target = fd_target(record)
        if target and target[1].startswith('/') and not target[1].startswith('/dev/') and not target[1].startswith('/memfd:'):
            descriptors[phase if phase == 'startup' else 'steady'][f"{record['name']} {target[0]}<{target[1]}>"] += 1
        if record['stack'] and demangle is not None:
            category, symbol = attribution(record['stack'], demangle)
            stacks[(phase if phase == 'startup' else 'steady', record['name'])][(category, symbol[:160])] += 1
    return {'first': first, 'last': last, 'counts': {k: dict(v) for k, v in counts.items()},
            'errors': {k: dict(v) for k, v in errors.items()},
            'paths': [{'phase': p, 'call': c, 'path': path, 'result': r, 'count': n} for (p, c, path, r), n in sorted(paths.items())],
            'fileDescriptorUse': {k: dict(v) for k, v in descriptors.items()},
            'threadNames': {str(k): v for k, v in thread_names.items()},
            'threadCalls': {k: {str(t): n for t, n in v.items()} for k, v in tids.items()},
            'stacks': [{'phase': p, 'call': c, 'category': cat, 'symbol': sym, 'count': n}
                       for (p, c), counter in sorted(stacks.items()) for (cat, sym), n in counter.most_common(6)]}


def demangler():
    """A caching demangler over c++filt (Itanium C++ and legacy Rust names)."""
    cache = {}

    def demangle(symbol):
        if symbol not in cache:
            cache[symbol] = subprocess.run(['c++filt'], input=symbol, capture_output=True, text=True).stdout.strip() or symbol
        return cache[symbol]
    return demangle

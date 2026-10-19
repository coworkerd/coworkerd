"""Summarises one profile run of a workerd build: per process
and in total, the calls of the startup and of the steady state (by workload window), the paths named, the
directory descriptors used and the thread names. The summary is what scripts/syscall-allowlist.py reads.

usage: syscall-summarise.py <profile-dir> <profile-record.json> <output.json>
"""
import collections
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import syscall_trace  # noqa: E402

directory, record_path, output = sys.argv[1:4]
record = json.load(open(record_path))
windows = [w for w in record['windows']]
events = [e for e in record.get('processLines', []) if e.get('tag') == 'process']
ready = {e['pid']: e['observedAtUnixMs'] / 1000 for e in events if e.get('event') == 'ready'}
identity = {}
for e in events:
    if e.get('event') == 'spawn':
        identity[e['pid']] = {'processId': e['processId'], 'generation': e['generation'], 'processClass': e['processClass']}
exits = {e['pid']: e.get('reason') for e in events if e.get('event') == 'exit'}

cache = {}


def demangle_all(symbols):
    todo = [s for s in symbols if s not in cache]
    if todo:
        out = subprocess.run(['c++filt'], input='\n'.join(todo), capture_output=True, text=True).stdout.split('\n')
        for s, d in zip(todo, out):
            cache[s] = d


def demangle(symbol):
    if symbol not in cache:
        demangle_all([symbol])
    return cache[symbol]


processes = []
for name in sorted(os.listdir(os.path.join(directory, 'traces'))):
    pid = int(name.split('-')[1].split('.')[0])
    with open(os.path.join(directory, 'traces', name), errors='replace') as stream:
        attached = bool(record.get('stack'))  # stack runs attach after the processes are ready (all steady)
        # The trace of a process begins at the execve of the workerd file the profile names, which is
        # the program the record carries (the wrapper execs it); the default is of no use here.
        summary = syscall_trace.summarise(stream, 0 if attached else ready.get(pid), windows,
                                     demangle if record.get('stack') else None, attached,
                                     record.get('traceProgram'))
    processes.append({'trace': name, 'pid': pid, **identity.get(pid, {}), 'readyUnixS': ready.get(pid), 'exit': exits.get(pid), **summary})

totals = collections.defaultdict(collections.Counter)
users = collections.defaultdict(set)
for p in processes:
    for phase, counter in p['counts'].items():
        for call, n in counter.items():
            totals[phase][call] += n
            users[(phase, call)].add(p['pid'])
startup = set(totals['startup'])
steady = set().union(*[set(c) for phase, c in totals.items() if phase != 'startup'])
print(json.dumps({'processes': len(processes), 'startupCalls': len(startup), 'steadyCalls': len(steady),
                  'startupOnly': sorted(startup - steady), 'steadyOnly': sorted(steady - startup)}, indent=1))
json.dump({'record': os.path.basename(record_path), 'run': record.get('run'), 'stack': record.get('stack'),
           'workerdSha256': record.get('binaries', {}).get('workerd'), 'program': record.get('traceProgram'),
           'windows': [{'name': w['name'], 'startUnixMs': w['startUnixMs'], 'endUnixMs': w['endUnixMs']} for w in windows],
           'processes': processes,
           'totals': {k: dict(v) for k, v in totals.items()},
           'processesUsing': {f'{phase} {call}': len(pids) for (phase, call), pids in users.items()}},
          open(output, 'w'), indent=1, sort_keys=True)

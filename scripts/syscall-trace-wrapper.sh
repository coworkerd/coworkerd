#!/bin/sh
# The Server's "workerd executable" while profiling a build. strace attaches to this shell and follows it into
# the workerd binary it then executes, so the pid the Server knows is workerd itself (its SIGTERM, SIGKILL
# and exit status are workerd's) and every thread is traced from the execve on. strace gets none of the
# descriptors the Server hands over (3-6) and writes one trace per process, named by that pid.
# COWORKERD_TRACE_PROGRAM is the workerd file to become; COWORKERD_TRACE_DIR holds one trace per process.
# COWORKERD_TRACE_STACK=1 adds a stack trace to every call except the high-frequency event loop, I/O and memory
# calls (their unwinding alone would exceed the Server's 15 s startup budget).
strace -f -ttt -qq -yy -s 200 ${COWORKERD_TRACE_STACK:+-k --stack-trace-frame-limit=40 -e trace=!epoll_wait,recvfrom,sendto,write,writev,read,futex,clock_gettime,gettid,mprotect,madvise} \
  -o "${COWORKERD_TRACE_DIR:?}/workerd-$$.trace" -p $$ </dev/null >/dev/null 2>&1 3>&- 4>&- 5>&- 6>&- &
until grep -q '^TracerPid:[[:space:]]*[1-9]' /proc/$$/status; do sleep 0.01; done
exec "${COWORKERD_TRACE_PROGRAM:?}" "$@"

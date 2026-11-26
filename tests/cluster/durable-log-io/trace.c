// Benchmark probe shared by both daemon builds. All recorded calls execute.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

enum Kind { DAEMON = 1, POLL_GAP, WRITE, WRITEV, PWRITE, FDATASYNC, FSYNC, SYNCFS };

struct Event {
  uint64_t kind, start, end, tid, cpu_ns, failed;
  char path[192];
};

static int output = -1;
static pid_t main_tid;
static uint64_t last_poll, last_cpu;
static int (*next_wait)(int, struct epoll_event *, int, int);
static int (*next_pwait)(int, struct epoll_event *, int, int, const sigset_t *);
static int (*next_fdatasync)(int);
static int (*next_fsync)(int);
static int (*next_syncfs)(int);
static ssize_t (*next_write)(int, const void *, size_t);
static ssize_t (*next_writev)(int, const struct iovec *, int);
static ssize_t (*next_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*next_pwrite64)(int, const void *, size_t, off64_t);

static uint64_t now_ns(clockid_t clock) {
  struct timespec now;
  clock_gettime(clock, &now);
  return (uint64_t)now.tv_sec * 1000000000 + now.tv_nsec;
}

static void record(struct Event *event) {
  event->tid = gettid();
  if (syscall(SYS_write, output, event, sizeof(*event)) != sizeof(*event)) _exit(1);
}

__attribute__((constructor)) static void initialize(void) {
  next_wait = dlsym(RTLD_NEXT, "epoll_wait");
  next_pwait = dlsym(RTLD_NEXT, "epoll_pwait");
  next_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
  next_fsync = dlsym(RTLD_NEXT, "fsync");
  next_syncfs = dlsym(RTLD_NEXT, "syncfs");
  next_write = dlsym(RTLD_NEXT, "write");
  next_writev = dlsym(RTLD_NEXT, "writev");
  next_pwrite = dlsym(RTLD_NEXT, "pwrite");
  next_pwrite64 = dlsym(RTLD_NEXT, "pwrite64");
  const char *path = getenv("COWORKERD_LOG_IO_TRACE");
  if (!path || strcmp(program_invocation_short_name, "coworkerd") != 0) return;
  // Only the first daemon in the container records events.
  output = open(path, O_WRONLY | O_APPEND | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  if (output < 0) {
    if (errno == EEXIST) return;
    perror(path);
    _exit(1);
  }
  main_tid = getpid();
  struct Event event = { .kind = DAEMON };
  record(&event);
}

static int begin_poll(void) {
  if (output < 0 || gettid() != main_tid) return 0;
  const uint64_t before = now_ns(CLOCK_MONOTONIC);
  // Short gaps have a known upper bound and need no trace write.
  if (last_poll && before - last_poll >= 100000) {
    struct Event event = { .kind = POLL_GAP, .start = last_poll, .end = before,
        .cpu_ns = now_ns(CLOCK_THREAD_CPUTIME_ID) - last_cpu };
    record(&event);
  }
  return 1;
}

int epoll_wait(int fd, struct epoll_event *events, int count, int timeout) {
  const int tracing = begin_poll();
  const int status = next_wait(fd, events, count, timeout);
  if (tracing) {
    const int error = errno;
    last_poll = now_ns(CLOCK_MONOTONIC);
    last_cpu = now_ns(CLOCK_THREAD_CPUTIME_ID);
    errno = error;
  }
  return status;
}

int epoll_pwait(int fd, struct epoll_event *events, int count, int timeout, const sigset_t *mask) {
  const int tracing = begin_poll();
  const int status = next_pwait(fd, events, count, timeout, mask);
  if (tracing) {
    const int error = errno;
    last_poll = now_ns(CLOCK_MONOTONIC);
    last_cpu = now_ns(CLOCK_THREAD_CPUTIME_ID);
    errno = error;
  }
  return status;
}

static void begin_file(struct Event *event, int fd, enum Kind kind) {
  if (output < 0) return;
  const int error = errno;
  char link[64];
  snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  const ssize_t length = readlink(link, event->path, sizeof(event->path) - 1);
  errno = error;
  const char directory[] = "/var/lib/coworkerd/do/log";
  const size_t prefix = sizeof(directory) - 1;
  if (length < (ssize_t)prefix || memcmp(event->path, directory, prefix) != 0 ||
      (event->path[prefix] != '/' && event->path[prefix] != '\0')) return;
  event->kind = kind;
  event->start = now_ns(CLOCK_MONOTONIC);
}

static void end_file(struct Event *event, int failed) {
  if (!event->kind) return;
  const int error = errno;
  event->end = now_ns(CLOCK_MONOTONIC);
  event->failed = failed;
  record(event);
  errno = error;
}

// Interpose the data writes and synchronization calls used by the daemon's
// log directory. The output file sits on a separate tmpfs.
#define FILE_CALL(type, function, kind, params, args) \
  type function params { \
    struct Event event = {0}; \
    begin_file(&event, fd, kind); \
    const type result = next_##function args; \
    end_file(&event, result < 0); \
    return result; \
  }

FILE_CALL(ssize_t, write, WRITE, (int fd, const void *bytes, size_t length), (fd, bytes, length))
FILE_CALL(ssize_t, writev, WRITEV, (int fd, const struct iovec *parts, int size), (fd, parts, size))
FILE_CALL(ssize_t, pwrite, PWRITE, (int fd, const void *bytes, size_t length, off_t offset), (fd, bytes, length, offset))
FILE_CALL(ssize_t, pwrite64, PWRITE, (int fd, const void *bytes, size_t length, off64_t offset), (fd, bytes, length, offset))
FILE_CALL(int, fdatasync, FDATASYNC, (int fd), (fd))
FILE_CALL(int, fsync, FSYNC, (int fd), (fd))
FILE_CALL(int, syncfs, SYNCFS, (int fd), (fd))

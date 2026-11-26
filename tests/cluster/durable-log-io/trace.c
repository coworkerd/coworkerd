// Benchmark probe shared by both daemon builds. The real fdatasync always runs.
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
#include <time.h>
#include <unistd.h>

static int output = -1;
static pid_t main_tid;
static uint64_t last_poll;
static int (*next_wait)(int, struct epoll_event *, int, int);
static int (*next_pwait)(int, struct epoll_event *, int, int, const sigset_t *);
static int (*next_fdatasync)(int);

static uint64_t now_ns(void) {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return (uint64_t)now.tv_sec * 1000000000 + now.tv_nsec;
}

__attribute__((constructor)) static void initialize(void) {
  next_wait = dlsym(RTLD_NEXT, "epoll_wait");
  next_pwait = dlsym(RTLD_NEXT, "epoll_pwait");
  next_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
  const char *path = getenv("COWORKERD_LOG_IO_TRACE");
  if (!path || strcmp(program_invocation_short_name, "coworkerd") != 0) return;
  main_tid = getpid();
  output = open(path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  if (output < 0) {
    perror(path);
    _exit(1);
  }
}

static void record(uint64_t kind, uint64_t start, uint64_t end) {
  uint64_t row[] = {kind, start, end, gettid() == main_tid};
  if (syscall(SYS_write, output, row, sizeof(row)) != sizeof(row)) _exit(1);
}

static int begin_poll(void) {
  if (output < 0 || gettid() != main_tid) return 0;
  const uint64_t before = now_ns();
  // Report all intervals relevant to the 5 ms budget without a trace write
  // for each short poll. A 100 us upper bound covers the unrecorded gaps.
  if (last_poll && before - last_poll >= 100000) record(1, last_poll, before);
  return 1;
}

int epoll_wait(int fd, struct epoll_event *events, int count, int timeout) {
  const int tracing = begin_poll();
  const int status = next_wait(fd, events, count, timeout);
  if (tracing) last_poll = now_ns();
  return status;
}

int epoll_pwait(int fd, struct epoll_event *events, int count, int timeout, const sigset_t *mask) {
  const int tracing = begin_poll();
  const int status = next_pwait(fd, events, count, timeout, mask);
  if (tracing) last_poll = now_ns();
  return status;
}

int fdatasync(int fd) {
  if (output < 0) return next_fdatasync(fd);
  char link[64], file[4096];
  snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  const ssize_t length = readlink(link, file, sizeof(file));
  const int trace = length > 4 && memcmp(file + length - 4, ".log", 4) == 0;
  if (!trace) return next_fdatasync(fd);
  const uint64_t start = now_ns();
  const int status = next_fdatasync(fd);
  const int error = errno;
  record(status == 0 ? 2 : 3, start, now_ns());
  errno = error;
  return status;
}

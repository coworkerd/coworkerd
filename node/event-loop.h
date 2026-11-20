#pragma once

#include <kj/async.h>
#include <kj/async-unix.h>
#include <kj/debug.h>

#include <csignal>
#include <unistd.h>
#ifdef __linux__
#include <sys/signalfd.h>
#endif

namespace coworkerd {

// A connection burst can keep the continuation queue runnable for a long time.
// Poll ready I/O between batches so established connections make progress.
// Every poll is an epoll_wait and a clock read (on hosts whose clock is not in
// the vDSO, another system call), so it runs every NODE_BUSY_POLL_INTERVAL
// turns, not every other one: at 1 the node's main thread made about 500 of
// its 619 system calls per request on cwkd-p40 (v138 §1).
constexpr uint NODE_BUSY_POLL_INTERVAL = 64;
inline void configureNodeEventLoop(kj::WaitScope& scope) {
  scope.setBusyPollInterval(NODE_BUSY_POLL_INTERVAL);
}

// True for the first call in a turn of the event loop and false for the later ones of the same turn: work that
// several callers of one turn ask for (the process pool's refresh(), several times a request) is done once. The
// turn ends with the events queued so far (kj::evalLater).
class OncePerTurn {
public:
  explicit OncePerTurn(kj::TaskSet& tasks): tasks(tasks) {}
  bool first() {
    if (done) return false;
    done = true;
    tasks.add(kj::evalLater([this]() { done = false; }));
    return true;
  }

private:
  kj::TaskSet& tasks;
  bool done = false;
};

// SIGTERM and SIGINT for the node's exit, read from a signalfd in the event
// loop. With UnixEventPort::onSignal() waiting, every poll of the loop also
// called sigtimedwait(); a signalfd is one more descriptor in the same epoll.
// Elsewhere than Linux it is UnixEventPort's own capture.
class ShutdownSignals {
public:
  // Blocks both signals in the calling thread, before the node starts any thread
  // (threads inherit the mask; child processes reset it at spawn), and opens the
  // signalfd. A signal that arrives before anyone waits stays pending. A handler
  // is installed as UnixEventPort::captureSignal() does, so that a signal the
  // parent ignored is still queued.
  ShutdownSignals() {
#ifdef __linux__
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGINT);
    KJ_SYSCALL(pthread_sigmask(SIG_BLOCK, &mask, nullptr));
    struct sigaction action = {};
    action.sa_handler = [](int) {};
    sigemptyset(&action.sa_mask);
    KJ_SYSCALL(sigaction(SIGTERM, &action, nullptr));
    KJ_SYSCALL(sigaction(SIGINT, &action, nullptr));
    KJ_SYSCALL(descriptor = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC));
#else
    kj::UnixEventPort::captureSignal(SIGTERM);
    kj::UnixEventPort::captureSignal(SIGINT);
#endif
  }
  ~ShutdownSignals() noexcept(false) {
    observer = nullptr;
    if (descriptor >= 0) close(descriptor);
  }
  KJ_DISALLOW_COPY_AND_MOVE(ShutdownSignals);
  // The next signal's number. One wait at a time.
  kj::Promise<int> next(kj::UnixEventPort& port) {
#ifdef __linux__
    if (!observer) observer = kj::heap<kj::UnixEventPort::FdObserver>(port, descriptor, kj::UnixEventPort::FdObserver::OBSERVE_READ);
    for (;;) {
      signalfd_siginfo info;
      const auto n = read(descriptor, &info, sizeof(info));
      if (n == sizeof(info)) co_return int(info.ssi_signo);
      KJ_REQUIRE(n < 0 && (errno == EAGAIN || errno == EINTR), "cannot read the shutdown signals", n, errno);
      if (errno == EAGAIN) co_await observer->whenBecomesReadable();
    }
#else
    auto info = co_await port.onSignal(SIGTERM).exclusiveJoin(port.onSignal(SIGINT));
    co_return info.si_signo;
#endif
  }

private:
  int descriptor = -1;
  kj::Own<kj::UnixEventPort::FdObserver> observer;
};

}  // namespace coworkerd

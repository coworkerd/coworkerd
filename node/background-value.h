#pragma once

#include <kj/async.h>
#include <kj/debug.h>
#include <atomic>
#include <list>
#include <memory>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>

namespace coworkerd {

// Own the joinable threads on the node's event-loop thread. They only use captured values,
// so cancellation of the awaiting coroutine does not invalidate a job or block the event loop.
class BackgroundValues {
public:
  ~BackgroundValues() { join(); }

  void join() {
    for (auto& job : jobs) job.thread.join();
    jobs.clear();
  }

  template <typename Function, typename Value = std::invoke_result_t<Function>>
  kj::Promise<Value> run(Function function) {
    for (auto i = jobs.begin(); i != jobs.end();) {
      if (i->done.load(std::memory_order_acquire)) {
        i->thread.join();
        i = jobs.erase(i);
      } else {
        ++i;
      }
    }
    auto pair = kj::newPromiseAndCrossThreadFulfiller<Value>();
    auto fulfiller = std::make_shared<kj::Own<kj::CrossThreadPromiseFulfiller<Value>>>(kj::mv(pair.fulfiller));
    // Keep the callable available to the caller if starting its thread fails, even for move-only inputs.
    auto work = std::make_shared<Function>(std::move(function));
    auto& job = jobs.emplace_back();
    try {
      job.thread = std::thread([work, fulfiller, &job]() mutable {
        try {
          auto value = (*work)();
          work.reset();
          (*fulfiller)->fulfill(kj::mv(value));
        } catch (kj::Exception& error) {
          (*fulfiller)->reject(kj::mv(error));
        } catch (...) {
          (*fulfiller)->reject(KJ_EXCEPTION(FAILED, "node background job failed"));
        }
        // Publish completion after releasing the job's input and promise endpoints.
        work.reset();
        fulfiller.reset();
        job.done.store(true, std::memory_order_release);
      });
    } catch (const std::system_error&) {
      jobs.pop_back();
      return (*work)();
    }
    return kj::mv(pair.promise);
  }

private:
  struct Job {
    std::thread thread;
    std::atomic<bool> done{false};
  };
  std::list<Job> jobs;
};

inline thread_local BackgroundValues backgroundValues;

template <typename Function, typename Value = std::invoke_result_t<Function>>
kj::Promise<Value> backgroundValue(Function function) {
  return backgroundValues.run(std::move(function));
}

}  // namespace coworkerd

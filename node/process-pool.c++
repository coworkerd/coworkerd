#include "process-pool.h"
#include "tenancy.h"
#include "support.h"
#include "async-log.h"
#include "memory-watermark.h"
#include <capnp/rpc-twoparty.h>
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <spawn.h>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#ifdef __linux__
#include <elf.h>
#include <linux/seccomp.h>
#include <poll.h>
#include <sys/ioctl.h>
// Kernel 5.5's "notify a user-space supervisor, then let the call proceed", which the observe mode of the
// system call allowlist is built on; a build whose headers predate it still compiles.
#ifndef SECCOMP_USER_NOTIF_FLAG_CONTINUE
#define SECCOMP_USER_NOTIF_FLAG_CONTINUE (1UL << 0)
#endif
#ifndef SECCOMP_IOCTL_NOTIF_RECV
#define SECCOMP_IOCTL_NOTIF_RECV _IOWR('!', 0, struct seccomp_notif)
#endif
#ifndef SECCOMP_IOCTL_NOTIF_SEND
#define SECCOMP_IOCTL_NOTIF_SEND _IOWR('!', 1, struct seccomp_notif_resp)
#endif
#endif

extern char** environ;
namespace coworkerd {
namespace {
// Supervisor cadence for reading limit events (status, exits and failures read on demand).
constexpr double LIMIT_EVENT_SYNC_MS = 1000;
// A finished invocation without a `completed` row is forgotten after this long, or sooner
// beyond the cap; only a rotation's drain waits for open contexts, at most its drain period.
constexpr double CONTEXT_RETENTION_MS = 600000;
constexpr size_t CONTEXT_CAP = 65536;
constexpr size_t ROTATION_HISTORY = 256, ROTATION_REQUESTS = 4096;
constexpr uint64_t ROTATION_BACKOFF_MAX_MS = 300000;
double mono() { return std::chrono::duration<double, std::milli>(
    std::chrono::steady_clock::now().time_since_epoch()).count(); }
uint64_t wall() { return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count(); }
// The memory.events of this process's own cgroup (cgroup v2: the "0::<path>" line of /proc/self/cgroup -- a systemd
// service's, or "/" in a container with a cgroup namespace of its own), where the kernel counts the OOM kills of every
// process in it.
std::string memoryEventsPath() {
  std::ifstream input("/proc/self/cgroup");
  for (std::string line; std::getline(input, line);) {
    if (!line.starts_with("0::")) continue;
    const auto path = line.substr(3);
    if (path.empty() || path == "/") break;
    const auto candidate = "/sys/fs/cgroup" + path + "/memory.events";
    if (std::filesystem::exists(candidate)) return candidate;
  }
  return "/sys/fs/cgroup/memory.events";
}
uint64_t kernelOomKills(const std::string& path) {
  std::ifstream input(path);
  std::string key; uint64_t value;
  while (input >> key >> value) if (key == "oom_kill") return value;
  return 0;
}
// An OOM kill the cgroup counted is this pool's when one of its processes exits by SIGKILL within this long after it was
// seen (the kernel counts the kill before the process is gone and is reaped); one that no such exit takes was another
// process's of the cgroup, and never makes a later SIGKILL a kernel OOM.
constexpr double OOM_ATTRIBUTION_MS = 2000;
std::string escape(const std::string& value) {
  std::string result;
  for (auto ch : value) {
    if (ch == '"' || ch == '\\') result += '\\';
    if (ch == '\n') result += "\\n";
    else if (ch == '\r') result += "\\r";
    else if (static_cast<unsigned char>(ch) < 0x20) { char code[8]; snprintf(code, sizeof code, "\\u%04x", ch); result += code; }
    else result += ch;
  }
  return result;
}
// The variables of the node's environment a workerd process inherits as they are: the locale, the time zone, the
// search path and temporary directory, output colouring, the switches workerd itself reads for tests, and the
// build's system call profile (scripts/syscall-trace-wrapper.sh stands in for workerd and reads its three). Every
// other one stays with the node; the COWORKERD_* settings workerd reads are set for it by spawn().
bool workerdInherits(std::string_view name) {
  static constexpr std::string_view NAMES[] = {"PATH", "TZ", "LANG", "LANGUAGE", "TMPDIR", "NO_COLOR", "FORCE_COLOR",
      "KJ_CLEAN_SHUTDOWN", "WORKERD_ALL_AUTOGATES", "COWORKERD_SECCOMP_STEADY_PROBE_FOR_TEST",
      "COWORKERD_TRACE_DIR", "COWORKERD_TRACE_PROGRAM", "COWORKERD_TRACE_STACK"};
  return name.starts_with("LC_") || std::find(std::begin(NAMES), std::end(NAMES), name) != std::end(NAMES);
}
// Descriptor numbers each workerd inherits (see process-pool.h); the launcher's own two, which it
// closes before workerd runs.
constexpr int EVENT_FD = 3, LISTEN_FD = 4, LIFECYCLE_FD = 5, BAND_FD = 6;
constexpr int LAUNCHER_STATUS_FD = 7, LAUNCHER_PROGRAM_FD = 8;
// A durable process's end of the Durable Object log channel (the launcher leaves it alone: it touches only 7 and 8).
constexpr int DURABLE_LOG_FD = 9;
// The node alone holds the write end; EOF on this read end ends workerd after node death.
constexpr int PARENT_FD = 10;
// The unique key of the durable processes' Host namespace, the same on every node: an object's Host id is
// HOST.idFromName(<namespace id>/<object id>) under it, and its databases are in <stack>/do/<key>/.
constexpr const char* DURABLE_HOST_KEY = "coworkerd-do-host-v1";
// The launcher's interface version (launcher/PROTOCOL.md) and its exit status for a failed launch.
constexpr int LAUNCHER_PROTOCOL = 2, LAUNCH_FAILED_STATUS = 125;
// A process the system call filter ended leaves this signal (the enforce mode).
constexpr int SYSCALL_DENIED_SIGNAL = SIGSYS;
// One line per call number at most this often, so a process that calls something outside the allowlist in
// a loop cannot fill the log; every occurrence is still counted and answered.
constexpr double SYSCALL_REPORT_INTERVAL_MS = 10000;
// The distinct call numbers one generation reports; beyond it the counters go on but no new entry is kept.
constexpr size_t SYSCALL_OBSERVED_CAP = 64;
// A descriptor above PARENT_FD, close-on-exec: the numbers a child inherits (3-10) never collide
// with one a spawn action still has to read.
int raised(int fd) {
  if (fd < 0 || fd > PARENT_FD) return fd;
  int moved = fcntl(fd, F_DUPFD_CLOEXEC, PARENT_FD + 1);
  ::close(fd);
  KJ_REQUIRE(moved >= 0, "cannot move a descriptor");
  return moved;
}
void closeOnExec(int fd) { fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC); }
// A close-on-exec pipe for limit events.
std::pair<int, int> eventPipe() {
  int fds[2];
#ifdef __linux__
  KJ_REQUIRE(pipe2(fds, O_CLOEXEC) == 0, "cannot create an inherited channel pipe");
  // Room for bursts while the Server's event loop is busy (best effort; the default is 64 KiB).
  fcntl(fds[1], F_SETPIPE_SZ, 1 << 20);
#else
  KJ_REQUIRE(pipe(fds) == 0, "cannot create an inherited channel pipe");
  closeOnExec(fds[0]); closeOnExec(fds[1]);
#endif
  fcntl(fds[0], F_SETFL, fcntl(fds[0], F_GETFL) | O_NONBLOCK);
  return {raised(fds[0]), raised(fds[1])};
}
// The workerd I/O provider wraps sockets, so its one-way parent pipe uses a UNIX socket pair.
std::pair<int, int> parentPipe() {
  int fds[2];
  KJ_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0, "cannot create the parent pipe");
  closeOnExec(fds[0]); closeOnExec(fds[1]);
  KJ_REQUIRE(shutdown(fds[1], SHUT_RD) == 0 && shutdown(fds[0], SHUT_WR) == 0,
      "cannot make the parent pipe one-way");
  return {raised(fds[0]), raised(fds[1])};
}
// Anonymous shared memory: a memfd on Linux, an unlinked POSIX shared memory object elsewhere.
int anonymousMemory(const char* name, size_t size) {
#ifdef __linux__
  int fd = memfd_create(name, MFD_CLOEXEC);
#else
  static uint64_t counter = 0;
  auto path = "/cwkd-" + std::to_string(getpid()) + "-" + std::to_string(counter++);
  int fd = shm_open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
  if (fd >= 0) { shm_unlink(path.c_str()); closeOnExec(fd); }
#endif
  KJ_REQUIRE(fd >= 0, "cannot create shared memory", name);
  KJ_REQUIRE(ftruncate(fd, off_t(size)) == 0, "cannot size shared memory", name);
  return raised(fd);
}
// A listening Unix socket at `path` (an older file there is replaced).
int listenUnix(const std::string& path) {
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  KJ_REQUIRE(path.size() < sizeof(address.sun_path), "Unix socket path too long", path.c_str());
  memcpy(address.sun_path, path.c_str(), path.size() + 1);
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  KJ_REQUIRE(fd >= 0, "cannot create a Unix socket");
  closeOnExec(fd);
  fd = raised(fd);
  ::unlink(path.c_str());
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || listen(fd, 128) != 0) {
    auto error = errno;
    ::close(fd);
    KJ_FAIL_REQUIRE("cannot listen on a Unix socket", path.c_str(), strerror(error));
  }
  return fd;
}
// The lifecycle snapshot channel; workerd (coworkerd-idle-eviction.c++) is the one writer. A
// 64-byte header (publications completed, magic, capacity per buffer), then two buffers, each a
// sequence (odd while being written), a length and the data; the latest snapshot is in buffer
// (publications % 2), which the writer never touches while it is the latest. Pages are allocated
// as written, so the capacity costs nothing until used.
class LifecycleView final {
public:
  static constexpr uint32_t MAGIC = 0x434c5743, CAPACITY = 8 << 20;
  static constexpr size_t SIZE = 64 + 2 * (16 + size_t(CAPACITY));
  LifecycleView() = default;
  LifecycleView(const LifecycleView&) = delete;
  ~LifecycleView() { if (base) munmap(base, SIZE); }
  // Creates and maps the shared memory; the returned descriptor is for the child.
  int create() {
    int fd = anonymousMemory("coworkerd-lifecycle", SIZE);
    auto mapping = mmap(nullptr, SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) { ::close(fd); KJ_FAIL_REQUIRE("cannot map the lifecycle channel"); }
    base = static_cast<char*>(mapping);
    __atomic_store_n(reinterpret_cast<uint32_t*>(base + 8), MAGIC, __ATOMIC_RELAXED);
    __atomic_store_n(reinterpret_cast<uint32_t*>(base + 12), CAPACITY, __ATOMIC_RELEASE);
    return fd;
  }
  // The latest snapshot; empty before the first one, or while publications overtake every copy.
  std::string read() const {
    if (!base) return {};
    for (int attempt = 0; attempt < 8; ++attempt) {
      auto published = __atomic_load_n(reinterpret_cast<const uint64_t*>(base), __ATOMIC_ACQUIRE);
      if (published == 0) return {};
      const char* buffer = base + 64 + (published % 2) * (16 + size_t(CAPACITY));
      auto before = __atomic_load_n(reinterpret_cast<const uint64_t*>(buffer), __ATOMIC_ACQUIRE);
      if (before & 1) continue;
      auto length = __atomic_load_n(reinterpret_cast<const uint32_t*>(buffer + 8), __ATOMIC_RELAXED);
      if (length > CAPACITY) continue;
      std::string copy(buffer + 16, length);
      __atomic_thread_fence(__ATOMIC_ACQUIRE);
      if (__atomic_load_n(reinterpret_cast<const uint64_t*>(buffer), __ATOMIC_RELAXED) == before) return copy;
    }
    return {};
  }
private:
  char* base = nullptr;
};
std::string hex(const unsigned char* bytes, unsigned int length) {
  std::ostringstream out;
  for (unsigned int i = 0; i < length; ++i) out << std::hex << std::setw(2) << std::setfill('0') << int(bytes[i]);
  return out.str();
}
class RuntimeConnection final {
public:
  kj::Own<kj::NetworkAddress> address;
  kj::Own<kj::HttpClient> connector;
  kj::Own<kj::AsyncIoStream> stream;
  kj::Own<capnp::TwoPartyClient> rpc;
  v1::rpc::WorkerdBootstrap::Client bootstrap = nullptr;
  bool disconnected = false;
  OpenEvent open(kj::StringPtr descriptor) {
    auto request = bootstrap.startEventRequest();
    request.setCfBlobJson(descriptor); request.setFromPersistentStub(false);
    auto response = request.send();
    return {response.getDispatcher(), response.ignoreResult()};
  }
};

// The identity of an executable file as stat() sees it; content hashes are keyed by it.
struct BinarySignature {
  uint64_t device = 0, inode = 0, size = 0;
  int64_t modifiedNs = 0, changedNs = 0;
  bool valid = false;
  bool operator==(const BinarySignature&) const = default;
  std::string key() const {
    if (!valid) return {};
    std::ostringstream out;
    out << device << ':' << inode << ':' << size << ':' << modifiedNs << ':' << changedNs;
    return out.str();
  }
};
BinarySignature signatureOf(const struct stat& info) {
  BinarySignature value;
  value.device = info.st_dev; value.inode = info.st_ino; value.size = uint64_t(info.st_size);
#ifdef __APPLE__
  value.modifiedNs = int64_t(info.st_mtimespec.tv_sec) * 1000000000 + info.st_mtimespec.tv_nsec;
  value.changedNs = int64_t(info.st_ctimespec.tv_sec) * 1000000000 + info.st_ctimespec.tv_nsec;
#else
  value.modifiedNs = int64_t(info.st_mtim.tv_sec) * 1000000000 + info.st_mtim.tv_nsec;
  value.changedNs = int64_t(info.st_ctim.tv_sec) * 1000000000 + info.st_ctim.tv_nsec;
#endif
  value.valid = true;
  return value;
}
BinarySignature statSignature(const std::string& path) {
  struct stat info;
  if (::stat(path.c_str(), &info) != 0) return {};
  return signatureOf(info);
}
// SHA-256 of executables on one background thread: a workerd binary is ~140 MB, too much
// for the event loop. Each file is hashed through the descriptor it was opened with, and the
// digest is stored under that descriptor's signature, so a replaced file is never mislabelled.
class BinaryHasher {
public:
  ~BinaryHasher() {
    { std::lock_guard lock(mutex); stopping = true; }
    wake.notify_all();
    if (worker.joinable()) worker.join();
    for (const auto& job : jobs) if (job.fd >= 0) ::close(job.fd);
  }
  void request(const std::string& path, const BinarySignature& expected) {
    if (!expected.valid) return;
    const auto key = expected.key();
    std::lock_guard lock(mutex);
    if (results.count(key) || queued.count(key) || stopping) return;
    queued.insert(key);
    jobs.push_back({path, key});
    if (!worker.joinable()) worker = std::thread([this] { loop(); });
    wake.notify_one();
  }
  // The same through a descriptor the Server opened (the workerd file a generation runs), so the
  // digest is of exactly that file even if the path is replaced meanwhile. The descriptor is duplicated.
  void request(int fd, const BinarySignature& expected) {
    if (!expected.valid || fd < 0) return;
    const auto key = expected.key();
    std::lock_guard lock(mutex);
    if (results.count(key) || queued.count(key) || stopping) return;
    const int copy = fcntl(fd, F_DUPFD_CLOEXEC, 10);
    if (copy < 0) return;
    queued.insert(key);
    jobs.push_back({{}, key, copy});
    if (!worker.joinable()) worker = std::thread([this] { loop(); });
    wake.notify_one();
  }
  std::string lookup(const BinarySignature& value) const {
    if (!value.valid) return {};
    std::lock_guard lock(mutex);
    auto found = results.find(value.key());
    return found == results.end() ? std::string() : found->second;
  }
  std::pair<uint64_t, uint64_t> counts() const { std::lock_guard lock(mutex); return {hashed, failed}; }
  // The digest of a file, computed now on the calling thread (a small file: the launcher at startup).
  static std::string digestOf(int fd) {
    std::string key, digest;
    BinaryHasher none;
    return none.hashDescriptor(fd, key, digest) ? digest : std::string();
  }
private:
  struct Job { std::string path, key; int fd = -1; };
  mutable std::mutex mutex;
  std::condition_variable wake;
  std::deque<Job> jobs;
  std::set<std::string> queued;
  std::map<std::string, std::string> results;
  uint64_t hashed = 0, failed = 0;
  bool stopping = false;
  std::atomic<bool> stop{false};
  std::thread worker;
  void loop() {
    for (;;) {
      Job job;
      {
        std::unique_lock lock(mutex);
        wake.wait(lock, [&] { return stopping || !jobs.empty(); });
        if (stopping) { stop = true; return; }
        job = std::move(jobs.front());
        jobs.pop_front();
      }
      std::string key, digest;
      bool ok;
      if (job.fd >= 0) {
        ok = hashDescriptor(job.fd, key, digest);
        ::close(job.fd);
      } else {
        ok = hash(job.path, key, digest);
      }
      std::lock_guard lock(mutex);
      queued.erase(job.key);
      if (ok) { results[key] = digest; ++hashed; } else ++failed;
    }
  }
  bool hash(const std::string& path, std::string& key, std::string& digest) {
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return false;
    auto closeFd = kj::defer([&] { ::close(fd); });
    return hashDescriptor(fd, key, digest);
  }
  // Keyed by the descriptor's own signature; read with pread, so a shared file offset is left alone.
  bool hashDescriptor(int fd, std::string& key, std::string& digest) {
    struct stat info;
    if (fstat(fd, &info) != 0) return false;
    key = signatureOf(info).key();
    auto* context = EVP_MD_CTX_new();
    if (!context) return false;
    auto freeContext = kj::defer([&] { EVP_MD_CTX_free(context); });
    if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1) return false;
    std::vector<unsigned char> buffer(1 << 20);
    for (off_t offset = 0;;) {
      { std::lock_guard lock(mutex); if (stopping) return false; }
      auto n = ::pread(fd, buffer.data(), buffer.size(), offset);
      if (n < 0 && errno == EINTR) continue;
      if (n < 0) return false;
      if (n == 0) break;
      offset += n;
      if (EVP_DigestUpdate(context, buffer.data(), size_t(n)) != 1) return false;
    }
    unsigned char out[EVP_MAX_MD_SIZE];
    unsigned int length = 0;
    if (EVP_DigestFinal_ex(context, out, &length) != 1) return false;
    std::ostringstream hex;
    for (unsigned int i = 0; i < length; ++i) hex << std::hex << std::setw(2) << std::setfill('0') << int(out[i]);
    digest = hex.str();
    return true;
  }
};

// ---- The sandbox ----
// A field of a one-line JSON record (the launcher's): its text, or the JSON of a non-string value.
std::string recordField(const std::string& record, const char* name) {
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(record.data(), record.size()), value);
    if (!value.isObject()) return {};
    for (auto field : value.getObject()) {
      if (field.getName() != name) continue;
      auto v = field.getValue();
      if (v.isString()) return v.getString().cStr();
      return capnp::JsonCodec().encodeRaw(v.asReader()).cStr();
    }
  } catch (const kj::Exception&) {}
  return {};
}
// A launcher record as JSON for the state endpoint: the line itself when it is one JSON object, else null.
std::string jsonOrNull(const std::string& record) {
  return !record.empty() && !recordField(record, "event").empty() ? record : "null";
}
std::string readText(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) return {};
  std::ostringstream text;
  text << input.rdbuf();
  return text.str();
}
#ifdef __linux__
// ---- The system call allowlist ----
// The file the build generated (scripts/syscall-allowlist.py), in the form the launcher reads: this
// architecture's call numbers, one per line, and comments -- among them the digest of the workerd the list
// belongs to and this architecture's number-to-name table, which the Server reports an observed call by.
// The Server reads the same file the launcher does and hashes it, so what the state endpoint reports and
// what is installed are the same bytes.
struct Allowlist {
  // Every workerd this list belongs to: more than one is how an upgrade is prepared (a list covering the
  // binary in place and the one about to replace it is installed first, then the binary is replaced).
  std::vector<std::string> workerdSha256;
  std::vector<int> calls;
  std::map<int, std::string> names;
};
Allowlist parseAllowlist(const std::string& text) {
  Allowlist result;
  std::istringstream lines(text);
  for (std::string line; std::getline(lines, line);) {
    const auto at = line.find_first_not_of(" \t");
    if (at == std::string::npos) continue;
    if (line[at] == '#') {
      std::istringstream fields(line.substr(at + 1));
      std::string word;
      fields >> word;
      if (word == "workerd-sha256") {
        std::string digest;
        if (fields >> digest) result.workerdSha256.push_back(digest);
      } else if (word == "name") {
        int number = -1;
        std::string name;
        if ((fields >> number >> name) && number >= 0) result.names[number] = name;
      }
      continue;
    }
    if (!std::isdigit(static_cast<unsigned char>(line[at]))) continue;
    result.calls.push_back(std::atoi(line.c_str() + at));
  }
  return result;
}
// The name of a thread of this Server's own child, for an observation's record.
std::string threadName(unsigned tid) {
  auto name = readText("/proc/" + std::to_string(tid) + "/comm");
  while (!name.empty() && (name.back() == '\n' || name.back() == '\r')) name.pop_back();
  return name;
}
// The observe mode's supervisor of one generation. It holds the filter's notification listener, which
// the launcher sent over the status socket, and answers every notification with "let the call proceed", so
// the process behaves as it does without the filter. This runs on a thread of its own because the kernel's
// receive is a blocking wait whatever the descriptor's flags say (`down_interruptible`; the documented way
// to use it is to poll first) -- on the Server's event loop it would stop everything else.
// The thread only receives, answers and records; the event loop takes the records, counts them
// and writes the report, so nothing but the kernel's answer is on the hot path. The listener outlives the
// generation's exit by as long as the thread needs to notice it (the poll below), so it is kept here rather
// than in the generation, and a notification whose thread died meanwhile is simply not answerable (ENOENT).
class SyscallSupervisor final {
public:
  struct Observation { int number = 0; unsigned tid = 0; std::string thread; };
  explicit SyscallSupervisor(int listener): listener(listener) {}
  SyscallSupervisor(const SyscallSupervisor&) = delete;
  ~SyscallSupervisor() { finish(); }
  void start() { worker = std::thread([this] { loop(); }); }
  void finish() {
    stopping = true;
    if (worker.joinable()) worker.join();
    if (listener >= 0) ::close(listener);
    listener = -1;
  }
  // What the thread saw since the last call, and the counters.
  // `counts` are exact (per call number); `observations` (thread and process of each) at most 4096 per drain, the rest
  // counted in `dropped`.
  struct Drained {
    std::vector<Observation> observations;
    std::map<int, uint64_t> counts;
    uint64_t notifications = 0, interrupted = 0, errors = 0, dropped = 0;
  };
  Drained drain() {
    std::lock_guard lock(mutex);
    Drained result{std::move(pending), std::move(counts), notifications, interrupted, errors, dropped};
    pending.clear();
    counts.clear();
    notifications = interrupted = errors = dropped = 0;
    return result;
  }
private:
  int listener;
  std::atomic<bool> stopping{false};
  std::thread worker;
  std::mutex mutex;
  std::vector<Observation> pending;
  std::map<int, uint64_t> counts;
  uint64_t notifications = 0, interrupted = 0, errors = 0, dropped = 0;
  void record(const Observation& observation, bool answered) {
    std::lock_guard lock(mutex);
    ++notifications;
    ++counts[observation.number];
    if (!answered) ++errors;
    if (pending.size() < 4096) pending.push_back(observation);
    else ++dropped;
  }
  void note(uint64_t SyscallSupervisor::*field) {
    std::lock_guard lock(mutex);
    ++(this->*field);
  }
  void loop() {
    while (!stopping) {
      struct pollfd waiting = {listener, POLLIN, 0};
      const int ready = ::poll(&waiting, 1, 50);
      if (ready < 0) {
        if (errno == EINTR) continue;
        note(&SyscallSupervisor::errors);
        return;
      }
      if (ready == 0) continue;                       // the generation is still running; nothing to answer
      if (!(waiting.revents & POLLIN)) return;        // POLLHUP: the filter is gone with its process
      struct seccomp_notif request;
      memset(&request, 0, sizeof request);
      if (ioctl(listener, SECCOMP_IOCTL_NOTIF_RECV, &request) != 0) {
        if (errno == EINTR) continue;
        // ENOENT: that thread was signalled while the notification was being made -- nothing to answer.
        if (errno == ENOENT) { note(&SyscallSupervisor::interrupted); continue; }
        note(&SyscallSupervisor::errors);
        return;
      }
      struct seccomp_notif_resp response;
      memset(&response, 0, sizeof response);
      response.id = request.id;
      response.flags = SECCOMP_USER_NOTIF_FLAG_CONTINUE;
      const bool answered = ioctl(listener, SECCOMP_IOCTL_NOTIF_SEND, &response) == 0 || errno == ENOENT;
      record({int(request.data.nr), unsigned(request.pid), threadName(unsigned(request.pid))}, answered);
    }
  }
};
// The ELF interpreter (PT_INTERP) of an executable; empty for a static or non-ELF file.
std::string elfInterpreter(int fd) {
  Elf64_Ehdr header;
  if (pread(fd, &header, sizeof header, 0) != ssize_t(sizeof header) || memcmp(header.e_ident, ELFMAG, SELFMAG) != 0 ||
      header.e_ident[EI_CLASS] != ELFCLASS64 || header.e_phentsize != sizeof(Elf64_Phdr)) return {};
  for (unsigned i = 0; i < header.e_phnum; ++i) {
    Elf64_Phdr program;
    if (pread(fd, &program, sizeof program, off_t(header.e_phoff + i * sizeof program)) != ssize_t(sizeof program)) return {};
    if (program.p_type != PT_INTERP || program.p_filesz == 0 || program.p_filesz > 4096) continue;
    std::string path(program.p_filesz, '\0');
    if (pread(fd, path.data(), path.size(), off_t(program.p_offset)) != ssize_t(path.size())) return {};
    path.resize(strnlen(path.c_str(), path.size()));
    return path;
  }
  return {};
}
// Runs a command to its end and returns its exit status and standard output (at most 1 MiB).
std::pair<int, std::string> capture(const std::vector<std::string>& args) {
  int fds[2];
  KJ_REQUIRE(pipe2(fds, O_CLOEXEC) == 0, "cannot create a pipe");
  posix_spawn_file_actions_t actions;
  posix_spawn_file_actions_init(&actions);
  posix_spawn_file_actions_adddup2(&actions, fds[1], STDOUT_FILENO);
  posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, "/dev/null", O_WRONLY, 0);
  std::vector<char*> argv;
  for (const auto& arg : args) argv.push_back(const_cast<char*>(arg.c_str()));
  argv.push_back(nullptr);
  pid_t pid = 0;
  const int error = posix_spawn(&pid, argv[0], &actions, nullptr, argv.data(), environ);
  posix_spawn_file_actions_destroy(&actions);
  ::close(fds[1]);
  std::string output;
  if (error == 0) {
    char buffer[65536];
    for (;;) {
      auto n = ::read(fds[0], buffer, sizeof buffer);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) break;
      if (output.size() < (1 << 20)) output.append(buffer, size_t(n));
    }
  }
  ::close(fds[0]);
  KJ_REQUIRE(error == 0, "cannot run", args[0].c_str(), strerror(error));
  int status = 0;
  while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
  return {WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status), output};
}
// The files the dynamic loader maps for `file`, as `<interpreter> --list <file>` resolves them: each
// library (`name => path (address)`) and the interpreter itself; empty for a static executable.
std::vector<std::string> sharedObjects(int fd, const std::string& file) {
  const auto interpreter = elfInterpreter(fd);
  if (interpreter.empty()) return {};
  auto [status, output] = capture({interpreter, "--list", file});
  KJ_REQUIRE(status == 0, "the dynamic loader cannot resolve the workerd executable's libraries", interpreter.c_str(), output.c_str());
  std::vector<std::string> paths;
  std::istringstream lines(output);
  for (std::string line; std::getline(lines, line);) {
    const auto arrow = line.find("=> ");
    std::string path;
    if (arrow != std::string::npos) path = line.substr(arrow + 3);
    else {
      const auto first = line.find_first_not_of(" \t");
      if (first == std::string::npos || line[first] != '/') continue;  // linux-vdso.so.1 and the like
      path = line.substr(first);
    }
    path = path.substr(0, path.find(" ("));
    KJ_REQUIRE(!path.empty() && path.front() == '/', "a library of the workerd executable is not found", line.c_str());
    if (std::find(paths.begin(), paths.end(), path) == paths.end()) paths.push_back(path);
  }
  if (std::find(paths.begin(), paths.end(), interpreter) == paths.end()) paths.push_back(interpreter);
  return paths;
}
// What /proc shows of a process against this Server's own: its namespaces, ids, capability sets, the
// no_new_privs bit, whether it is dumpable, its mounts and network interfaces.
// Read once a generation is ready; as JSON for the state endpoint.
std::string observeSandbox(pid_t pid) {
  const auto base = "/proc/" + std::to_string(pid);
  auto namespaceDiffers = [&](const char* name) -> std::string {
    char mine[128] = {}, theirs[128] = {};
    const auto a = readlink((std::string("/proc/self/ns/") + name).c_str(), mine, sizeof mine - 1);
    const auto b = readlink((base + "/ns/" + name).c_str(), theirs, sizeof theirs - 1);
    if (a <= 0 || b <= 0) return "null";
    return strcmp(mine, theirs) != 0 ? "true" : "false";
  };
  const auto status = readText(base + "/status");
  auto statusField = [&](const char* name) -> std::string {
    const auto at = status.find(std::string("\n") + name + ":");
    if (at == std::string::npos) return {};
    auto value = status.substr(at + strlen(name) + 2, status.find('\n', at + 1) - at - strlen(name) - 2);
    value.erase(0, value.find_first_not_of(" \t"));
    return value;
  };
  bool capabilities = !status.empty();
  for (const char* set : {"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"})
    capabilities = capabilities && statusField(set).find_first_not_of('0') == std::string::npos && !statusField(set).empty();
  // Its effective uid as this Server sees it; /proc/<pid>/environ belongs to that uid while the process is
  // dumpable and to root otherwise -- which says nothing when the process runs as root outside.
  std::string euid;
  { std::istringstream ids(statusField("Uid")); std::string real; ids >> real >> euid; }
  std::string nonDumpable = "null";
  struct stat owner;
  if (!euid.empty() && euid != "0" && ::stat((base + "/environ").c_str(), &owner) == 0)
    nonDumpable = owner.st_uid == 0 ? "true" : "false";
  // The root mount (mount point "/") and its options; every mount.
  const auto mountinfo = readText(base + "/mountinfo");
  size_t mounts = 0;
  std::string rootReadOnly = "null";
  { std::istringstream lines(mountinfo);
    for (std::string line; std::getline(lines, line);) {
      ++mounts;
      std::istringstream fields(line);
      std::string id, parent, device, root, point, options;
      fields >> id >> parent >> device >> root >> point >> options;
      if (point == "/") rootReadOnly = (options == "ro" || options.starts_with("ro,")) ? "true" : "false";
    } }
  std::vector<std::string> interfaces;
  { std::istringstream lines(readText(base + "/net/dev"));
    for (std::string line; std::getline(lines, line);) {
      const auto colon = line.find(':');
      if (colon == std::string::npos) continue;
      auto name = line.substr(0, colon);
      name.erase(0, name.find_first_not_of(' '));
      interfaces.push_back(name);
    } }
  // Routes: none means nothing is reachable, loopback included (it is down). A kernel with the tunnel
  // drivers loaded adds their inert fallback devices to every new network namespace (down, no address), and
  // every IPv6 table holds the null entries (RTF_REJECT), which are not routes.
  auto rows = [&](const char* file, size_t header, int flagsField) {
    size_t count = 0, line = 0;
    std::istringstream lines(readText(base + "/net/" + file));
    for (std::string text; std::getline(lines, text);) {
      if (text.empty() || line++ < header) continue;
      std::istringstream fields(text);
      std::string field;
      for (int i = 0; i <= flagsField && fields >> field; ++i) {}
      if (!(std::strtoul(field.c_str(), nullptr, 16) & 0x0200)) ++count;
    }
    return count;
  };
  const auto ipv4Routes = rows("route", 1, 3), ipv6Routes = rows("ipv6_route", 0, 8);
  auto oneLine = [](std::string text) {
    std::string out;
    std::istringstream words(text);
    for (std::string word; words >> word;) out += (out.empty() ? "" : " ") + word;
    return out;
  };
  const auto uidMap = oneLine(readText(base + "/uid_map")), gidMap = oneLine(readText(base + "/gid_map")), uids = oneLine(statusField("Uid"));
  const auto seccomp = statusField("Seccomp");
  // How many filters the process carries: the launcher's startup table is one, the steady-state table
  // the process installs itself is the second. The kernel has reported this since 5.9; an older one leaves
  // the field out, and then the count is null and `steady` says nothing either way.
  auto filterCount = statusField("Seccomp_filters");
  if (filterCount.empty() || filterCount.find_first_not_of("0123456789") != std::string::npos) filterCount.clear();
  // A layer is in effect when its namespace is not this Server's and it does what it is for: one mapping, to
  // 65534 inside (user); a read-only root (mount); no route (network). "null": /proc did not say.
  auto both = [](const std::string& differs, bool holds) -> std::string {
    return differs == "null" ? differs : differs == "true" && holds ? "true" : "false";
  };
  const auto user = both(namespaceDiffers("user"), uidMap.starts_with("65534 ") && uidMap.find(' ', 6) != std::string::npos &&
      std::count(uidMap.begin(), uidMap.end(), ' ') == 2 && gidMap.starts_with("65534 ") && std::count(gidMap.begin(), gidMap.end(), ' ') == 2);
  const auto mount = both(namespaceDiffers("mnt"), rootReadOnly == "true");
  const auto network = both(namespaceDiffers("net"), ipv4Routes == 0 && ipv6Routes == 0);
  std::ostringstream out;
  out << "{\"observedAtUnixMs\":" << wall() << ",\"layers\":{\"user\":" << user << ",\"mount\":" << mount
      << ",\"network\":" << network << ",\"ipc\":" << namespaceDiffers("ipc") << ",\"uts\":" << namespaceDiffers("uts")
      << ",\"capabilities\":" << (capabilities ? "true" : "false") << ",\"noNewPrivs\":" << (statusField("NoNewPrivs") == "1" ? "true" : "false")
      << ",\"nonDumpable\":" << nonDumpable << ",\"seccomp\":" << (seccomp == "2" ? "true" : "false")
      << ",\"steadySyscalls\":" << (filterCount.empty() ? "null" : std::stoul(filterCount) >= 2 ? "true" : "false")
      << "},\"seccompFilters\":" << (filterCount.empty() ? "null" : filterCount)
      << ",\"uidMap\":\"" << escape(uidMap) << "\",\"gidMap\":\"" << escape(gidMap) << "\",\"uids\":\"" << escape(uids)
      << "\",\"mounts\":" << mounts << ",\"rootReadOnly\":" << rootReadOnly << ",\"interfaces\":[";
  for (size_t i = 0; i < interfaces.size(); ++i) out << (i ? "," : "") << '"' << escape(interfaces[i]) << '"';
  out << "],\"ipv4Routes\":" << ipv4Routes << ",\"ipv6Routes\":" << ipv6Routes << ",\"seccompMode\":\"" << escape(seccomp) << "\"}";
  return out.str();
}
#endif
}

// What every generation's `completed` rows say a tenant's executions used (for TenantUsage), CPU nanoseconds by
// tenant; shared by the pool and its generations, read and reset by ProcessPool::takeCpuUsage.
using TenantCpu = std::map<std::string, uint64_t>;

struct ProcessGeneration {
  std::string id;
  bool quarantine = false, durable = false;
  std::shared_ptr<TenantCpu> cpuUsage;
  uint64_t generation;
  // Its egress token: in its configuration only; the egress service attributes a request to it by this.
  std::string egressToken;
  // Its listening socket in the run directory: <slot>-<generation>.sock.
  std::string socketName, socketPath;
  pid_t pid = 0;
  // Only this node holds this generation's parent-liveness write end.
  int parentWrite = -1;
  std::string state = "starting", lastReason;
  // Rotation: spawned as a slot's next generation; switched away from (takes no new calls);
  // closing (connections closed, SIGTERM sent); and whether the exit was the rotation's own doing.
  bool rotation = false, retiring = false, closing = false, plannedExit = false;
  std::string exitStatus;
  double spawnedAt = mono(), readyAt = 0, nextProbeAt = 0, probeStartedAt = 0, exitedAt = 0;
  uint64_t spawnedAtUnixMs = wall(), readyAtUnixMs = 0, exitedAtUnixMs = 0;
  uint32_t inFlight = 0;
  uint64_t dispatched = 0;
  std::map<std::string, ProcessCall*> calls;
  struct CompletedIdentity { std::string workerId, versionId, invocationId; };
  std::deque<CompletedIdentity> recentDispatched;
  std::map<std::string, double> heat;
  std::deque<double> probeDelays;
  bool probePending = false;
  double probeAttemptAt = 0;
  uint64_t probeAttempts = 0, probeFailures = 0;
  // Limit event channel: the read end of the pipe whose write end the child
  // inherited. An observer on the event loop moves whatever arrives into `eventTail` (so the pipe
  // never fills and workerd's writes never wait); rows are parsed and applied only when needed --
  // supervisor cadence, exit, failure classification, status -- never per dispatch. At the exit the
  // rest is read up to the end of the pipe.
  struct LimitEvent {
    std::string event, isolate, workerId; uint32_t memoryLimitMiB; uint64_t peakUsedBytes;
    uint64_t pauseNs = 0; std::string row;  // gc-pause rows: the pause, and the row itself
  };
  bool eventChannel = false, eventsFinal = false, eventsEnded = false;
  int eventFd = -1;
  kj::Own<kj::UnixEventPort::FdObserver> eventObserver;
  EVP_MD_CTX* eventDigest = nullptr;  // SHA-256 of every byte read, in order
  std::string eventSha256;            // its value once the pipe ended
  pid_t spawnPid = 0;
  std::string eventTail;
  std::vector<LimitEvent> unappliedEvents;
  std::array<std::set<std::string>, 2> terminatedCalls;  // CPU, memory: pid, invocation, Worker, version
  // A durable process's limit rows name the request that started the object's context (workerd keeps one per object),
  // not the request that hit the limit: by pid, Worker and version, when each was last read.
  std::array<std::map<std::string, double>, 2> terminatedWorkers;
  uint64_t eventReadCalls = 0, eventBytes = 0, eventRows = 0, rejectedRows = 0, incompleteTailBytes = 0;
  double nextEventSync = 0;
  // The evidence rows: the latest CPU culprit (withdrawn by a later terminated row of the same
  // invocation: the termination took effect after all), the OOM row, the OOM identity row.
  std::string culpritRow, culpritInvocation, oomCulpritRow;
  bool oomSignalled = false;
  // Lifecycle snapshot shared memory.
  LifecycleView lifecycle;
  // Tenant request contexts. workerd writes a `completed` row when a request's context
  // ends -- after its waitUntil work, whether or not it ran JavaScript, limits or not -- so
  // a dispatched invocation stays open until that row is read.
  struct OpenContext { std::string workerId, versionId; double finishedAt = 0; };
  bool contextsVisible = false;
  std::unordered_map<std::string, OpenContext> openContexts;
  // The CPU a request that lasts -- a WebSocket connection, whose context ends with it -- has already been metered with,
  // from its cpu-progress rows, until its completed row comes.
  std::unordered_map<std::string, uint64_t> meteredCpu;
  uint64_t contextsCompleted = 0, contextsCleared = 0, contextsDropped = 0;
  double drainLifecycleCheck = 0;  // the Server drain's lifecycle reads
  // The workerd file the Server opened for this generation: its identity, which becomes the
  // generation's `binary` once the generation is ready (an identity is attached after the probe).
  BinarySignature spawnBinary, binary;
  // The sandbox: started through the launcher; this Server's end of its status socket until the
  // launcher's end closed; what it wrote (its `exec` record, or the `failed` one); what /proc showed once
  // ready. The notification listener of the observe mode, which the launcher sent over that socket,
  // and what this generation's filter has reported through it (per call number, and the thread names).
  bool launched = false, launchFailed = false;
  // The process-wide WebAssembly memory cap this generation was started with.
  uint32_t wasmMemoryCapMiB = 0;
  int statusFd = -1;
  // The launcher's end of the status socket closed (its execve, or its exit): everything it wrote has been read.
  bool statusEnded = false;
  std::string launchRecords, launchRecord, launchFailure, observedSandbox = "null";
  std::string syscallMode = "off";
#ifdef __linux__
  std::shared_ptr<SyscallSupervisor> syscalls;
#endif
  std::map<int, uint64_t> observedCalls;
  std::map<int, std::set<std::string>> observedThreads;
  uint64_t syscallNotifications = 0, syscallInterrupted = 0;
  std::shared_ptr<RuntimeConnection> runtime;
  kj::Own<kj::NetworkAddress> probeAddress;
  kj::Own<kj::HttpClient> probeClient;
  kj::Own<kj::PromiseFulfiller<void>> exited;
  kj::Maybe<kj::ForkedPromise<void>> exitSignal;
  ~ProcessGeneration() {
    if (parentWrite >= 0) ::close(parentWrite);
    closeEvents();
    if (eventDigest) EVP_MD_CTX_free(eventDigest);
    closeStatus();
    closeNotify();
  }
  // The status socket's observer goes before its descriptor: its destructor removes the descriptor from the event port.
  void closeStatus() {
    statusObserver = nullptr;
    if (statusFd >= 0) ::close(statusFd);
    statusFd = -1;
  }
  // The supervisor's thread ends; its listener closes with it.
  void closeNotify() {
#ifdef __linux__
    if (syscalls) syscalls->finish();
#endif
  }
  // Moves what the launcher wrote into `launchRecords` without blocking; the last `exec` or `failed`
  // record is the outcome (PROTOCOL.md). At most a few KB: the socket never fills. The record that
  // reports an observe-mode filter carries the notification listener with it, so this reads the
  // status socket with recvmsg and keeps the descriptor that arrives.
  void readLaunch() {
    while (statusFd >= 0 && !statusEnded) {
      char buffer[4096];
      alignas(struct cmsghdr) char control[CMSG_SPACE(sizeof(int))];
      struct iovec part = {buffer, sizeof buffer};
      struct msghdr message = {};
      message.msg_iov = &part;
      message.msg_iovlen = 1;
      message.msg_control = control;
      message.msg_controllen = sizeof control;
      auto n = ::recvmsg(statusFd, &message, 0);
      if (n < 0 && errno == EINTR) continue;
      if (n > 0) {
        for (auto* header = CMSG_FIRSTHDR(&message); header != nullptr; header = CMSG_NXTHDR(&message, header)) {
          if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS ||
              header->cmsg_len != CMSG_LEN(sizeof(int))) continue;
          int received = -1;
          memcpy(&received, CMSG_DATA(header), sizeof received);
          if (received < 0) continue;
#ifdef __linux__
          if (syscalls) { ::close(received); continue; }  // only the first listener is kept
          syscalls = std::make_shared<SyscallSupervisor>(raised(received));
          syscalls->start();
#else
          ::close(received);
#endif
        }
        if (launchRecords.size() < 65536) launchRecords.append(buffer, size_t(n));
        continue;
      }
      // The end: the descriptor stays open until its observer is gone (closeStatus).
      if (n == 0) statusEnded = true;
      break;  // EAGAIN: nothing more yet
    }
    std::istringstream lines(launchRecords);
    for (std::string line; std::getline(lines, line);) {
      const auto event = recordField(line, "event");
      if (event == "exec") { launchRecord = line; launchFailed = false; }
      else if (event == "failed") { launchFailure = line; launchFailed = true; }
    }
  }
  kj::Own<kj::UnixEventPort::FdObserver> statusObserver;
  void closeEvents() {
    eventObserver = nullptr;  // before its descriptor closes
    if (eventFd >= 0) ::close(eventFd);
    eventFd = -1;
  }
  // Moves everything in the pipe into `eventTail`; notes the end once the child's write end closed.
  void drainEvents() {
    if (eventFd < 0 || eventsEnded) return;
    char buffer[65536];
    for (;;) {
      ++eventReadCalls;
      auto n = ::read(eventFd, buffer, sizeof buffer);
      if (n < 0 && errno == EINTR) continue;
      if (n == 0) { eventsEnded = true; break; }
      if (n < 0) break;  // EAGAIN: every byte written so far has been read
      eventBytes += n;
      if (eventDigest) EVP_DigestUpdate(eventDigest, buffer, size_t(n));
      eventTail.append(buffer, n);
    }
  }
  std::string currentEventSha256() const {
    if (!eventSha256.empty() || !eventDigest) return eventSha256;
    unsigned char out[EVP_MAX_MD_SIZE];
    unsigned int length = 0;
    auto* copy = EVP_MD_CTX_new();
    if (!copy) return {};
    std::string value;
    if (EVP_MD_CTX_copy_ex(copy, eventDigest) == 1 && EVP_DigestFinal_ex(copy, out, &length) == 1) value = hex(out, length);
    EVP_MD_CTX_free(copy);
    return value;
  }
  static std::string callKey(uint64_t pid, std::string_view invocation, std::string_view worker, std::string_view version) {
    return std::to_string(pid).append(1, '\n').append(invocation).append(1, '\n').append(worker).append(1, '\n').append(version);
  }
  bool terminated(size_t cause, const std::string& invocation, const std::string& worker, const std::string& version) {
    pullEvents();
    return pid > 0 && terminatedCalls[cause].count(callKey(pid, invocation, worker, version)) > 0;
  }
  // A durable process: a limit row of the Worker's version read since `since` (a request to one of its objects).
  bool terminatedSince(size_t cause, const std::string& worker, const std::string& version, double since) {
    pullEvents();
    if (pid <= 0) return false;
    auto found = terminatedWorkers[cause].find(callKey(pid, "", worker, version));
    return found != terminatedWorkers[cause].end() && found->second >= since;
  }
  std::string channelJson() const {
    std::ostringstream out;
    out << "{\"open\":" << (eventFd >= 0 ? "true" : "false") << ",\"final\":" << (eventsFinal ? "true" : "false")
        << ",\"readCalls\":" << eventReadCalls << ",\"bytes\":" << eventBytes << ",\"rows\":" << eventRows
        << ",\"rejectedRows\":" << rejectedRows << ",\"pendingBytes\":" << eventTail.size() << ",\"incompleteTailBytes\":" << incompleteTailBytes
        << ",\"sha256\":\"" << currentEventSha256() << "\"}";
    return out.str();
  }
  size_t backgroundContexts() const {
    return std::count_if(openContexts.begin(), openContexts.end(), [](const auto& entry) { return entry.second.finishedAt > 0; });
  }
  // Parses the rows read since the last call; an incomplete final row waits for the next call.
  void pullEvents() {
    if (!eventChannel || eventsFinal) return;
    drainEvents();
    size_t start = 0;
    for (size_t newline; (newline = eventTail.find('\n', start)) != std::string::npos; start = newline + 1) {
      const auto row = eventTail.substr(start, newline - start);
      ++eventRows;
      try {
        capnp::MallocMessageBuilder message;
        auto report = message.initRoot<v1::rpc::CpuReport>();
        capnp::JsonCodec().decode(kj::StringPtr(row.c_str()), report);
        if (report.getPid() != static_cast<uint32_t>(spawnPid)) continue;
        const std::string event = report.getEvent().cStr();
        if (event == "terminated" || event == "memory-terminated") {
          terminatedCalls[event == "terminated" ? 0 : 1].insert(callKey(report.getPid(), report.getInvocationId().cStr(),
              report.getWorkerId().cStr(), report.getVersionId().cStr()));
          if (durable) terminatedWorkers[event == "terminated" ? 0 : 1][callKey(report.getPid(), "", report.getWorkerId().cStr(),
              report.getVersionId().cStr())] = mono();
        }
        if (event == "completed" && !openContexts.empty() &&
            openContexts.erase(std::string(report.getInvocationId().cStr()))) ++contextsCompleted;
        // A tenant's CPU: the rows of its executions; a trial load's or a load check's are the platform's. The collection
        // CPU the limit did not charge (gc.budgetMs) is the tenant's CPU all the same. A request that lasts reports its CPU
        // so far in cpu-progress rows (at most once a second, at its events), each counted for what it adds, and its
        // completed row the rest.
        if ((event == "completed" || event == "cpu-progress") && cpuUsage &&
            std::string_view(report.getInvocationId().cStr()).find("-invocation-") != std::string_view::npos) {
          const auto tenant = tenantOf(report.getWorkerId().cStr());
          const uint64_t total = report.getCpuUsedNs() + report.getGcExcludedNs();
          auto metered = meteredCpu.find(report.getInvocationId().cStr());
          const uint64_t before = metered == meteredCpu.end() ? 0 : metered->second;
          if (!tenant.empty() && total > before) (*cpuUsage)[tenant] += total - before;
          if (event == "completed") { if (metered != meteredCpu.end()) meteredCpu.erase(metered); }
          else if (total > before) meteredCpu[report.getInvocationId().cStr()] = total;
        }
        if (event == "cpu-progress") continue;
        if (event == "culprit") {
          culpritRow = row; culpritInvocation = report.getInvocationId().cStr();
        } else if ((event == "terminated" || event == "memory-terminated") && !culpritRow.empty() &&
            culpritInvocation == report.getInvocationId().cStr()) {
          culpritRow.clear(); culpritInvocation.clear();
        }
        if (event == "memory-oom-undetermined") oomSignalled = true;
        if (event == "memory-culprit") oomCulpritRow = row;
        unappliedEvents.push_back({event, report.getIsolate().cStr(), report.getWorkerId().cStr(),
            report.getMemoryLimitMiB(), report.getPeakUsedBytes(), report.getPauseNs(),
            event == "gc-pause" ? row : std::string()});
      } catch (const kj::Exception& error) {
        ++rejectedRows;
        std::cerr << "[process-pool] CPU report rejected: " << error.getDescription().cStr() << std::endl;
      }
    }
    eventTail.erase(0, start);
  }
  // Bounds the open contexts of a long-lived generation; see CONTEXT_RETENTION_MS.
  void pruneContexts(double now) {
    const auto before = openContexts.size();
    std::erase_if(openContexts, [&](const auto& entry) {
      return entry.second.finishedAt > 0 && now - entry.second.finishedAt > CONTEXT_RETENTION_MS;
    });
    if (openContexts.size() > CONTEXT_CAP) std::erase_if(openContexts, [&](const auto& entry) {
      return entry.second.finishedAt > 0 && now - entry.second.finishedAt > 60000;
    });
    contextsDropped += before - openContexts.size();
  }
};
ProcessCall::ProcessCall(std::shared_ptr<ProcessGeneration> process)
    : processId(process->id), generation(process->generation), process(std::move(process)) {
  ++this->process->inFlight;
}
ProcessCall::~ProcessCall() { finish(); }
void ProcessCall::finish() {
  if (finished) return;
  finished = true;
  KJ_ASSERT(process->inFlight > 0);
  --process->inFlight;
  if (dispatched) {
    if (process->recentDispatched.size() == 4096) process->recentDispatched.pop_front();
    process->recentDispatched.push_back({workerId, versionId, invocationId});
    auto open = process->openContexts.find(invocationId);
    if (open != process->openContexts.end()) open->second.finishedAt = mono();
  }
  process->calls.erase(invocationId);
}
kj::Promise<void> ProcessCall::onExit() {
  return KJ_ASSERT_NONNULL(process->exitSignal).addBranch().then([]() -> kj::Promise<void> {
    return KJ_EXCEPTION(DISCONNECTED, "dispatched workerd generation exited; result uncertain");
  });
}
// A failed call is classified against this generation's events: a matching
// row whose pid is the generation's current pid (none once the exit was observed).
bool ProcessCall::cpuLimitExceeded() const {
  if (process->durable) return process->terminatedSince(0, workerId, versionId, startedAtMonotonicMs);
  return process->terminated(0, invocationId, workerId, versionId);
}
bool ProcessCall::memoryLimitExceeded() const {
  if (process->durable) return process->terminatedSince(1, workerId, versionId, startedAtMonotonicMs);
  return process->terminated(1, invocationId, workerId, versionId);
}

// A slot's or a generation's class, as the state endpoint and the [process] lines name it, and its code for the output
// drainer (WorkerdOutput::className).
template <typename T> const char* processClass(const T& p) { return p.durable ? "durable" : p.quarantine ? "quarantine" : "ordinary"; }
template <typename T> uint32_t processClassCode(const T& p) { return p.durable ? 2 : p.quarantine ? 1 : 0; }

class ProcessPool::Impl final: public kj::TaskSet::ErrorHandler {
public:
  struct Slot {
    std::string id;
    bool quarantine = false, durable = false;
    uint64_t generation = 0, restarts = 0, exits = 0, rotations = 0;
    uint32_t failures = 0;
    double restartAt = 0;
    uint64_t lastExitAtUnixMs = 0, lastExitedGeneration = 0;
    std::string lastReason;
    uint64_t cpuTerminatedRequests = 0, cpuInvalidatedIsolates = 0, cpuReloadedIsolates = 0;
    uint64_t memoryTerminatedRequests = 0, memoryInvalidatedIsolates = 0, memoryReloadedIsolates = 0;
    // The long collections of every generation (gc-pause rows, gc.longPauseMs): how many, and the longest one's row.
    uint64_t gcLongPauses = 0, gcLongestPauseNs = 0;
    std::string gcLongestPause;
    // Launches that failed before workerd ran (the launcher's `failed` record, or no `exec` one).
    uint64_t launchFailures = 0;
    std::string lastLaunchFailure = "null";
    // With the allowlist on, a spawn waits briefly for the digest of the workerd file it would run, so a
    // file this Server has not hashed yet is not launched against a list it cannot be compared with.
    double digestWaitSince = 0;
    std::string lastExitCategory = "none";
    std::set<std::string> invalidatedIsolates;
    std::set<std::string> memoryInvalidated;
    // Dispatch target; the next generation while a rotation starts it; previous generations
    // draining or closing. Only `current` is ever selected.
    std::shared_ptr<ProcessGeneration> current, incoming;
    std::vector<std::shared_ptr<ProcessGeneration>> retiring;
  };
  struct RotationRequest {
    size_t slot;
    std::string reason;
    double requestedAt;
    uint64_t requestedAtUnixMs;
    std::vector<uint64_t> ids;
  };
  // At most one per machine: start the next generation, switch dispatch to it, drain the
  // previous one, close it (connections, SIGTERM, SIGKILL after a grace).
  struct Rotation {
    uint64_t id = 0;
    size_t slot = 0;
    std::string reason, phase = "starting", outcome, failure, drainOutcome = "none";
    std::vector<uint64_t> requestIds;
    std::shared_ptr<ProcessGeneration> old, next;
    double requestedAt = 0, startedAt = 0, switchedAt = 0, drainedAt = 0, terminatedAt = 0, finishedAt = 0, lastLifecycleCheck = 0;
    uint64_t requestedAtUnixMs = 0, startedAtUnixMs = 0, switchedAtUnixMs = 0, drainedAtUnixMs = 0, terminatedAtUnixMs = 0,
        finishedAtUnixMs = 0;
    uint32_t inFlightAtSwitch = 0, killedCalls = 0;
    size_t contextsAtSwitch = 0, backgroundAtSwitch = 0, killedContexts = 0;
    uint64_t clearedContexts = 0;
    bool backgroundVisible = false, sigkill = false;
    std::string oldBinarySha256, oldLifecycle = "null";
    // The WebSocket connections of the previous generation told at the switch to go away (1001).
    uint32_t webSocketsClosed = 0;
  };
  // A generation stops taking calls (a rotation's switch): its WebSocket connections are told to go away, as many as the
  // callback returns (set by the node, setRetiring).
  std::function<uint32_t(const std::string&, uint64_t)> retiring;
  ProcessPoolConfig config;
  std::string executable, runtimeFile, directory, runDirectory, bindingAddress, sourceAddress;
  std::string egressAddress;  // empty while tenant Workers have no network
  kj::AsyncIoProvider& provider;
  kj::UnixEventPort& eventPort;
  const kj::HttpHeaderTable& headers;
  Suspects& suspects;
  int bandFd = -1;  // the Server's own copy of the band's shared memory, at 10 or above
  uint64_t quarantineDispatches = 0, trialDispatches = 0, durableDispatches = 0;
  // The Durable Object log channel of each durable generation (node/durable-log.h).
  std::function<int(const std::string&)> durableChannelOpen;
  std::function<void(const std::string&)> durableChannelClose;
  DurableLogConfig durableLog;
  std::shared_ptr<TenantCpu> cpuUsage = std::make_shared<TenantCpu>();
  std::vector<Slot> slots;
  std::deque<std::string> kills;
  std::deque<std::string> memoryCrashes;
  // The latest gc-pause rows of any generation (gc.longPauseMs).
  std::deque<std::string> recentGcPauses;
  // The limit event channel of the last exited generations: what the Server read, to the end.
  std::deque<std::string> channelExits;
  // The cgroup's OOM kills (memoryEventsPath): the counter as last read, and the increments no exit has taken yet, each
  // with when it was seen (observeOomKills, at the supervisor cadence and at every exit).
  const std::string memoryEvents = memoryEventsPath();
  uint64_t lastOomKills = kernelOomKills(memoryEvents), foreignOomKills = 0, poolOomKills = 0;
  std::deque<double> unattributedOomKills;
  void observeOomKills() {
    const auto now = mono();
    for (const auto count = kernelOomKills(memoryEvents); lastOomKills < count; ++lastOomKills) unattributedOomKills.push_back(now);
    while (!unattributedOomKills.empty() && now - unattributedOomKills.front() > OOM_ATTRIBUTION_MS) {
      unattributedOomKills.pop_front();
      ++foreignOomKills;
    }
  }
  struct WorkerCpuStats { uint64_t terminated = 0, culprit = 0; };
  struct WorkerMemoryStats { uint64_t terminated = 0, culprit = 0, peakUsedBytes = 0; };
  std::map<std::string, WorkerCpuStats> workerCpuStats;
  std::map<std::string, WorkerMemoryStats> workerMemoryStats;
  // Rotation state.
  std::deque<RotationRequest> rotationQueue;
  std::unique_ptr<Rotation> rotation;
  std::deque<std::pair<uint64_t, std::string>> rotationHistory;
  struct RequestState { std::string status; uint64_t rotationId = 0; };
  std::map<uint64_t, RequestState> rotationRequests;
  std::map<std::string, uint64_t> rotationCounts, rotationReasons;
  uint64_t nextRotationId = 1, nextRequestId = 1;
  double automaticRetryAt = 0, nextMemoryCheck = 0, nextBinaryCheck = 0, nextLauncherCheck = 0;
  uint64_t automaticBackoffMs = 0;
  BinaryHasher hasher;
  // The limits each Worker names, as the node resolves them (the Worker's own, else its tenant's
  // default); empty before the node sets the view, and 0 for a Worker that names none, where this node's own
  // values apply. The processes apply them per isolate, so the pool uses them to read the limit rows a
  // process writes -- each row carries the limit it was measured against -- and the largest memory limit to
  // size a new generation's process-wide WebAssembly memory cap.
  ProcessPool::WorkerLimitsView workerLimits;
  BinarySignature configuredBinary;
  uint64_t binaryChecks = 0, binaryChanges = 0, binaryChangedAtUnixMs = 0;
  // The sandbox. The launcher is pinned at startup: this Server starts every process with the file it
  // checked then (through its descriptor), so a launcher replaced on disk -- one of another release -- is
  // never mixed in; that change only raises a restart hint, and no rotation.
  struct Sandbox {
    std::string mode, reason = "not set up";
    bool active = false;
    std::string launcherPath, launcherSha256, launcherVersion, root, checkRecord = "null";
    int launcherFd = -1;
    BinarySignature launcherSignature, diskSignature;
    bool changedOnDisk = false;
    uint64_t changedAtUnixMs = 0, launches = 0, failures = 0;
    double checkMs = 0;
    // The workerd file's interpreter and libraries (its dynamic loader's answer), by the file's signature.
    std::vector<std::string> sharedObjects;
    BinarySignature sharedObjectsFor;
    // Test only: a required bind that does not exist, added to every process's launch but
    // not to the startup check. COWORKERD_SANDBOX_BIND_FOR_TEST.
    std::string bindForTest;
    // The system call allowlist. The list is loaded once, here: the launcher reads the same file,
    // and a list that does not name the workerd this Server runs never reaches a launch.
    struct Syscalls {
      std::string mode = "off";
      bool active = false;
      std::string reason = "not set up";
      std::string path, sha256;
      // The digests the list names, and the one this Server's workerd matches.
      std::vector<std::string> workerdSha256;
      std::string workerdMatched;
      size_t calls = 0;
      std::map<int, std::string> names;
      // The workerd file this Server checked the list against, and its digest (computed once, at startup).
      BinarySignature programSignature;
      std::string programSha256;
      // What the observe mode has reported, over this Server's life: per call number, with the thread
      // names it came from, and when. Bounded by SYSCALL_OBSERVED_CAP distinct numbers.
      struct Observed {
        uint64_t count = 0, firstAtUnixMs = 0, lastAtUnixMs = 0;
        double lastReportedAt = 0;
        std::set<std::string> threads;
        std::set<std::string> processes;
      };
      std::map<int, Observed> observed;
      uint64_t notifications = 0, interrupted = 0, errors = 0, reported = 0, beyondCap = 0, detailsDropped = 0;
      // Processes the enforce mode ended, and the latest records of those exits.
      uint64_t denied = 0;
      std::deque<std::string> recentDenials;
      // The steady-state table: the same build's narrower list, which workerd installs itself at its
      // first request (coworkerd-steady.h) together with the dumpable bit the execve reset. This Server
      // reads and hashes the same file it hands over, so what it reports and what is installed are the same
      // bytes, and what /proc shows of each process (`Seccomp_filters` 2, non-dumpable) is reported beside
      // it. A call outside this table is handled by the process itself: in `observe` it reports the call and
      // proceeds, in `enforce` the kernel ends it, which is the exit category syscall-denied, as for the
      // startup table.
      struct Steady {
        std::string mode = "off";
        bool active = false;
        std::string reason = "not set up";
        std::string path, sha256;
        std::vector<std::string> workerdSha256;
        std::string workerdMatched;
        size_t calls = 0;
      } steady;
      std::string name(int number) const {
        auto found = names.find(number);
        return found == names.end() ? std::string() : found->second;
      }
    } syscalls;
  } sandbox;
  std::deque<std::string> recentLaunchFailures;
  kj::TaskSet tasks{*this};
  bool stopping = false;
  // Server exit: no rotation starts once shutting down; nothing is started,
  // restarted or rotated once closing.
  bool shuttingDown = false, closingAll = false;
  // The durable processes were closed for the node's exit (closeDurable()): none starts again.
  bool durableClosed = false;
  uint64_t shutdownClearedContexts = 0;

  Impl(ProcessPoolConfig config, std::string executable, std::string runtimeFile,
      std::string directory, std::string bindingAddress, std::string sourceAddress,
      kj::AsyncIoProvider& provider, kj::UnixEventPort& eventPort, const kj::HttpHeaderTable& headers,
      Suspects& suspects)
      : config(config), executable(std::move(executable)), runtimeFile(std::move(runtimeFile)),
        directory(std::move(directory)), runDirectory(this->directory + "/run"), bindingAddress(std::move(bindingAddress)),
        sourceAddress(std::move(sourceAddress)), provider(provider), eventPort(eventPort), headers(headers), suspects(suspects) {
    if (auto* ms = std::getenv("COWORKERD_CPU_LIMIT_MS")) {
      char* end = nullptr; auto parsed = std::strtoull(ms, &end, 10);
      KJ_REQUIRE(*ms && end && !*end && parsed <= UINT32_MAX, "invalid CPU limit override");
      this->config.cpuLimitMs = static_cast<uint32_t>(parsed);
    }
    if (auto* text = std::getenv("COWORKERD_MEMORY_LIMIT_MIB")) {
      char* end = nullptr; auto value = std::strtoull(text, &end, 10);
      KJ_REQUIRE(*text && *text != '-' && end && !*end && value <= UINT32_MAX, "invalid memory limit override");
      this->config.memoryLimitMiB = static_cast<uint32_t>(value);
    }
    if (auto* count = std::getenv("COWORKERD_QUARANTINE_PROCESSES")) {
      char* end = nullptr; auto parsed = std::strtoull(count, &end, 10);
      KJ_REQUIRE(*count && end && !*end && parsed <= 256, "invalid quarantine process override");
      this->config.quarantineProcesses = static_cast<uint32_t>(parsed);
    }
    if (auto* count = std::getenv("COWORKERD_DURABLE_PROCESSES")) {
      char* end = nullptr; auto parsed = std::strtoull(count, &end, 10);
      KJ_REQUIRE(*count && end && !*end && parsed <= 64, "invalid durable process override");
      this->config.durableProcesses = static_cast<uint32_t>(parsed);
    }
    for (const auto& setting : {std::pair{"COWORKERD_IDLE_EVICT_MS", &this->config.idleEvictMs},
                               std::pair{"COWORKERD_IDLE_DESTROY_INTERVAL_MS", &this->config.idleDestroyIntervalMs},
                               std::pair{"COWORKERD_ROTATION_DRAIN_TIMEOUT_MS", &this->config.rotationDrainTimeoutMs},
                               std::pair{"COWORKERD_ROTATION_TERM_GRACE_MS", &this->config.rotationTermGraceMs},
                               std::pair{"COWORKERD_ROTATION_BINARY_CHECK_MS", &this->config.rotationBinaryCheckMs},
                               std::pair{"COWORKERD_ROTATION_RETRY_MS", &this->config.rotationRetryMs}}) {
      if (auto* text = std::getenv(setting.first)) {
        char* end = nullptr; auto value = std::strtoull(text, &end, 10);
        KJ_REQUIRE(*text && *text != '-' && end && !*end && value <= UINT32_MAX, "invalid process pool override", setting.first);
        *setting.second = static_cast<uint32_t>(value);
      }
    }
    for (const auto& setting : {std::pair{"COWORKERD_GC_BUDGET_MS", &this->config.gcBudgetMs},
                               std::pair{"COWORKERD_GC_REFILL_MS_PER_S", &this->config.gcRefillMsPerSecond},
                               std::pair{"COWORKERD_GC_LONG_PAUSE_MS", &this->config.gcLongPauseMs},
                               std::pair{"COWORKERD_IDLE_GC_MS", &this->config.gcIdleCollectAfterMs},
                               std::pair{"COWORKERD_IDLE_GC_MIN_MIB", &this->config.gcIdleCollectMinGrowthMiB},
                               std::pair{"COWORKERD_GC_INITIAL_OLD_SPACE_MIB", &this->config.gcInitialOldSpaceMiB},
                               std::pair{"COWORKERD_STATIC_HEAP_MIB", &this->config.gcRuntimeHeapMiB}}) {
      if (auto* text = std::getenv(setting.first)) {
        char* end = nullptr; auto value = std::strtoull(text, &end, 10);
        KJ_REQUIRE(*text && *text != '-' && end && !*end && value <= UINT32_MAX, "invalid gc override", setting.first);
        *setting.second = static_cast<uint32_t>(value);
      }
    }
    for (const auto& setting : {std::pair{"COWORKERD_ROTATION_MAX_AGE_MS", &this->config.rotationMaxAgeMs},
                               std::pair{"COWORKERD_ROTATION_MAX_REQUESTS", &this->config.rotationMaxRequests},
                               std::pair{"COWORKERD_ROTATION_RESIDENT_BYTES", &this->config.rotationResidentBytes}}) {
      if (auto* text = std::getenv(setting.first)) {
        char* end = nullptr; auto value = std::strtoull(text, &end, 10);
        KJ_REQUIRE(*text && *text != '-' && end && !*end, "invalid rotation override", setting.first);
        *setting.second = value;
      }
    }
    if (auto* mode = std::getenv("COWORKERD_SANDBOX_MODE")) this->config.sandboxMode = mode;
    if (auto* mode = std::getenv("COWORKERD_SANDBOX_SYSCALLS")) this->config.syscallsMode = mode;
    if (auto* path = std::getenv("COWORKERD_SANDBOX_ALLOWLIST")) this->config.allowlistPath = path;
    if (auto* bind = std::getenv("COWORKERD_SANDBOX_BIND_FOR_TEST")) sandbox.bindForTest = bind;
    this->config.validate();
    KJ_REQUIRE(access(this->executable.c_str(), X_OK) == 0, "workerd executable is required");
    KJ_REQUIRE(std::filesystem::is_regular_file(this->runtimeFile), "runtime module is required");
    std::filesystem::create_directories(this->directory + "/logs");
    std::filesystem::create_directories(this->directory + "/processes");
    std::filesystem::create_directories(runDirectory);
    std::filesystem::permissions(runDirectory, std::filesystem::perms::owner_all, std::filesystem::perm_options::replace);
    // The durable processes' objects: one SQLite database per object (and its facets), below the Host namespace's
    // unique key -- in the stack, so that a stack's objects go with it.
    if (this->config.durableProcesses) {
      std::filesystem::create_directories(durableDirectory());
      std::filesystem::permissions(durableDirectory(), std::filesystem::perms::owner_all, std::filesystem::perm_options::replace);
    }
    for (uint32_t i = 0; i < config.count; ++i) slots.push_back({"p" + std::to_string(i)});
    for (uint32_t i = 0; i < this->config.quarantineProcesses; ++i) slots.push_back({"q" + std::to_string(i), true});
    for (uint32_t i = 0; i < this->config.durableProcesses; ++i) {
      Slot slot;
      slot.id = "d" + std::to_string(i);
      slot.durable = true;
      slots.push_back(std::move(slot));
    }
    // Sockets a previous Server in this stack left behind (generation numbers start again at 1): removed only
    // when nothing listens on them. A socket a process still accepts connections on belongs to a Server that
    // runs in this stack, and this one does not start.
    for (const auto& entry : std::filesystem::directory_iterator(runDirectory)) {
      const auto name = entry.path().filename().string();
      if (!name.ends_with(".sock") || !(name.starts_with("p") || name.starts_with("q") || name.starts_with("d")) ||
          name.find('-') == std::string::npos) continue;
      const auto path = entry.path().string();
      sockaddr_un address{};
      address.sun_family = AF_UNIX;
      KJ_REQUIRE(path.size() < sizeof(address.sun_path), "Unix socket path too long", path.c_str());
      memcpy(address.sun_path, path.c_str(), path.size() + 1);
      const int probe = socket(AF_UNIX, SOCK_STREAM, 0);
      KJ_REQUIRE(probe >= 0, "cannot create a Unix socket");
      fcntl(probe, F_SETFL, fcntl(probe, F_GETFL) | O_NONBLOCK);
      const int error = connect(probe, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0 ? 0 : errno;
      ::close(probe);
      if (error == ENOENT) continue;
      KJ_REQUIRE(error == ECONNREFUSED, "a process listens on a socket of this stack's run directory: another Server runs here",
                 path.c_str(), error ? strerror(error) : "the socket accepts connections");
      std::filesystem::remove(entry.path());
    }
    if (this->config.memoryBandFd >= 0) {
      bandFd = fcntl(this->config.memoryBandFd, F_DUPFD_CLOEXEC, 10);
      KJ_REQUIRE(bandFd >= 0, "cannot keep the memory band descriptor");
    }
    setUpSandbox();
    configuredBinary = statSignature(this->executable);
    std::cout << "[process-pool] {\"event\":\"channels\",\"runDirectory\":\"" << escape(runDirectory)
        << "\",\"binding\":\"" << escape(this->bindingAddress) << "\",\"source\":\"" << escape(this->sourceAddress)
        << "\",\"memoryBand\":" << (bandFd >= 0 ? "true" : "false")
        << ",\"ignored\":{\"processPool.basePort\":" << this->config.basePort << "}}" << std::endl;
  }
  ~Impl() noexcept(false) {
    stopping = true;
    // SIGKILL is deterministic even when a child is stuck in native or tenant code.
    for (auto& s : slots) eachGeneration(s, [](const auto& p) { if (p->pid > 0) kill(p->pid, SIGKILL); });
    for (auto& s : slots) eachGeneration(s, [](const auto& p) {
      if (p->pid == 0) return;
      int status;
      while (waitpid(p->pid, &status, 0) < 0 && errno == EINTR) {}
      p->pid = 0;
      ::unlink(p->socketPath.c_str());
    });
    if (bandFd >= 0) ::close(bandFd);
    if (sandbox.launcherFd >= 0) ::close(sandbox.launcherFd);
  }
  void taskFailed(kj::Exception&& error) override {
    std::cerr << "[process-pool] task failed: " << error.getDescription().cStr() << std::endl;
  }
  // What a Worker's executions run under: the limits it names, else this node's own.
  WorkerLimits limitsOf(const std::string& workerId) const {
    WorkerLimits effective{config.cpuLimitMs, config.memoryLimitMiB};
    if (workerLimits.of && !workerId.empty()) {
      const auto named = workerLimits.of(workerId);
      if (named.cpuMs) effective.cpuMs = named.cpuMs;
      if (named.memoryMiB) effective.memoryMiB = named.memoryMiB;
    }
    return effective;
  }
  // A new generation's WebAssembly memory cap: a process-wide V8 flag, so it is the largest limit any Worker
  // here may run under rather than one Worker's. An isolate's own heap constraint and its counting allocator
  // are exact (patch 0005), and this only keeps a single WebAssembly memory from growing past the largest
  // limit of the moment; a generation started before a Worker with a higher limit arrives keeps the cap it
  // was started with until it rotates, which the state endpoint reports per generation.
  uint32_t wasmMemoryCapMiB() const {
    uint32_t cap = config.memoryLimitMiB;
    if (workerLimits.maxMemoryMiB) cap = std::max(cap, workerLimits.maxMemoryMiB());
    return cap;
  }
  // Where the durable processes keep their objects (workerd's localDisk storage): <stack>/do.
  std::string durableDirectory() const { return directory + "/do"; }
  // The durable slot of an object: the first of d0, d1, ... in its key's rendezvous order -- the same while the slots
  // are, whether or not that process is ready now.
  std::string durableSlotFor(const std::string& objectKey) const {
    std::vector<std::string> ids;
    for (const auto& s : slots) if (s.durable) ids.push_back(s.id);
    auto order = rendezvousOrder(objectKey, ids);
    return order.empty() ? std::string() : order.front();
  }
  // Every generation of a slot not yet released: current, incoming, retiring.
  template <typename Function> static void eachGeneration(const Slot& s, Function&& function) {
    if (s.current) function(s.current);
    if (s.incoming) function(s.incoming);
    for (const auto& p : s.retiring) function(p);
  }
  void event(const ProcessGeneration& p, const char* event, const std::string& reason = "") {
    std::cout << "[process] {\"event\":\"" << event << "\",\"processId\":\"" << p.id
        << "\",\"processClass\":\"" << processClass(p)
        << "\",\"generation\":" << p.generation << ",\"pid\":" << p.pid
        << ",\"observedAtUnixMs\":" << wall() << ",\"observedAtMonotonicMs\":" << std::setprecision(17)
        << mono() << ",\"reason\":\"" << escape(reason) << "\"}" << std::endl;
  }
  // ---- The sandbox ----
  // The launcher file's digest as last seen on disk (the pinned one while the file is unchanged).
  std::string diskLauncherSha256() const {
    if (sandbox.diskSignature.valid && sandbox.diskSignature == sandbox.launcherSignature) return sandbox.launcherSha256;
    return hasher.lookup(sandbox.diskSignature);
  }
  std::string sandboxStateJson() const {
    const auto& b = sandbox;
    std::ostringstream out;
    out << std::setprecision(17) << "{\"mode\":\"" << b.mode << "\",\"active\":" << (b.active ? "true" : "false")
        << ",\"reason\":\"" << escape(b.reason) << "\",\"launcher\":{\"path\":\"" << escape(b.launcherPath) << "\",\"sha256\":\"" << b.launcherSha256
        << "\",\"version\":\"" << escape(b.launcherVersion) << "\",\"protocol\":" << LAUNCHER_PROTOCOL << ",\"diskSha256\":\""
        << diskLauncherSha256() << "\",\"changedOnDisk\":" << (b.changedOnDisk ? "true" : "false") << "},\"pendingRestart\":";
    if (b.changedOnDisk) {
      out << "{\"reason\":\"the launcher file changed on disk; this Server keeps starting workerd with the launcher it checked at "
             "startup (no rotation); restart the Server to use the new one\",\"startedWithSha256\":\"" << b.launcherSha256
          << "\",\"diskSha256\":\"" << diskLauncherSha256() << "\",\"changedAtUnixMs\":" << b.changedAtUnixMs << "}";
    } else {
      out << "null";
    }
    out << ",\"root\":\"" << escape(b.root) << "\",\"sharedObjects\":[";
    for (size_t i = 0; i < b.sharedObjects.size(); ++i) out << (i ? "," : "") << '"' << escape(b.sharedObjects[i]) << '"';
    out << "],\"check\":" << jsonOrNull(b.checkRecord) << ",\"checkMs\":" << b.checkMs << ",\"launches\":" << b.launches
        << ",\"launchFailures\":" << b.failures << ",\"syscalls\":" << syscallsStateJson() << "}";
    return out.str();
  }
  // The allowlist's state: the mode, whether a filter is installed in every launch and why not, the file
  // and its digest, the workerd it belongs to, and what the observe mode has reported.
  std::string syscallsStateJson() const {
    const auto& s = sandbox.syscalls;
    std::ostringstream out;
    out << "{\"mode\":\"" << escape(s.mode) << "\",\"active\":" << (s.active ? "true" : "false")
        << ",\"reason\":\"" << escape(s.reason) << "\",\"allowlist\":{\"path\":\"" << escape(s.path)
        << "\",\"sha256\":\"" << s.sha256 << "\",\"workerdSha256\":\"" << (s.workerdSha256.empty() ? std::string() : s.workerdSha256.front())
        << "\",\"calls\":" << s.calls << ",\"names\":" << s.names.size()
        << ",\"workerdSha256Running\":\"" << s.programSha256 << "\",\"workerdSha256s\":[";
    for (size_t i = 0; i < s.workerdSha256.size(); ++i) out << (i ? "," : "") << '"' << s.workerdSha256[i] << '"';
    out << "],\"workerdSha256Matched\":\"" << s.workerdMatched << "\"},\"steady\":{\"mode\":\"" << escape(s.steady.mode)
        << "\",\"active\":" << (s.steady.active ? "true" : "false") << ",\"reason\":\"" << escape(s.steady.reason)
        << "\",\"allowlist\":{\"path\":\"" << escape(s.steady.path) << "\",\"sha256\":\"" << s.steady.sha256
        << "\",\"calls\":" << s.steady.calls << ",\"workerdSha256s\":[";
    for (size_t j = 0; j < s.steady.workerdSha256.size(); ++j) out << (j ? "," : "") << '"' << s.steady.workerdSha256[j] << '"';
    out << "],\"workerdSha256Matched\":\"" << s.steady.workerdMatched << "\"},\"installedBy\":\"the process itself at its"
           " first request; each process reports it under processes[].sandbox.observed (seccompFilters, layers.steadySyscalls)\"}"
        << ",\"notifications\":" << s.notifications
        << ",\"interrupted\":" << s.interrupted << ",\"errors\":" << s.errors << ",\"reported\":" << s.reported
        << ",\"detailsDropped\":" << s.detailsDropped
        << ",\"beyondCap\":" << s.beyondCap << ",\"observedCalls\":" << s.observed.size() << ",\"observed\":[";
    size_t i = 0;
    for (const auto& [number, entry] : s.observed) {
      if (i++) out << ',';
      out << "{\"call\":" << number << ",\"name\":\"" << escape(s.name(number)) << "\",\"count\":" << entry.count
          << ",\"firstAtUnixMs\":" << entry.firstAtUnixMs << ",\"lastAtUnixMs\":" << entry.lastAtUnixMs << ",\"threads\":[";
      size_t j = 0;
      for (const auto& thread : entry.threads) out << (j++ ? "," : "") << '"' << escape(thread) << '"';
      out << "],\"processes\":[";
      j = 0;
      for (const auto& process : entry.processes) out << (j++ ? "," : "") << '"' << escape(process) << '"';
      out << "]}";
    }
    out << "],\"denied\":" << s.denied << ",\"recentDenials\":[";
    for (size_t j = 0; j < s.recentDenials.size(); ++j) out << (j ? "," : "") << s.recentDenials[j];
    out << "]}";
    return out.str();
  }
  // A generation's own: whether it went through the launcher, what the launcher reported, and what /proc
  // showed once it was ready.
  std::string generationSandboxJson(const ProcessGeneration& p) const {
    std::ostringstream out;
    out << "{\"mode\":\"" << sandbox.mode << "\",\"launched\":" << (p.launched ? "true" : "false");
    if (p.launched) out << ",\"launcherSha256\":\"" << sandbox.launcherSha256 << "\",\"launcherVersion\":\"" << escape(recordField(p.launchRecord, "version"))
        << "\",\"launch\":" << jsonOrNull(p.launchRecord);
    out << ",\"syscalls\":{\"mode\":\"" << escape(p.syscallMode) << "\",\"supervised\":"
#ifdef __linux__
        << (p.syscalls ? "true" : "false")
#else
        << "false"
#endif
        << ",\"steadyMode\":\"" << escape(p.launched && sandbox.syscalls.steady.active ? sandbox.syscalls.steady.mode : std::string("off"))
        << "\",\"notifications\":" << p.syscallNotifications << ",\"interrupted\":" << p.syscallInterrupted << ",\"observed\":[";
    size_t i = 0;
    for (const auto& [number, count] : p.observedCalls) {
      if (i++) out << ',';
      out << "{\"call\":" << number << ",\"name\":\"" << escape(sandbox.syscalls.name(number)) << "\",\"count\":" << count << ",\"threads\":[";
      size_t j = 0;
      const auto threads = p.observedThreads.find(number);
      if (threads != p.observedThreads.end()) for (const auto& thread : threads->second) out << (j++ ? "," : "") << '"' << escape(thread) << '"';
      out << "]}";
    }
    out << "]},\"wasmMemoryCapMiB\":" << p.wasmMemoryCapMiB << ",\"observed\":" << p.observedSandbox << "}";
    return out.str();
  }
  // The digest of a workerd file this Server may launch: the one it hashed when it loaded the allowlist,
  // or, for a file replaced since, the background hasher's. Empty while a replaced file is not hashed yet.
  std::string programDigest(const BinarySignature& signature) const {
    if (!signature.valid) return {};
    const auto& s = sandbox.syscalls;
    if (s.programSignature.valid && signature == s.programSignature) return s.programSha256;
    return hasher.lookup(signature);
  }
  // A workerd file this Server cannot name yet is not launched against the allowlist: the pair would
  // be refused and the slot would record a launch failure for a file that may well be the right one. The
  // digest lands from the background hasher within a moment, and the slot tries again; after five seconds
  // the spawn goes ahead and the launcher reports the pair it was given.
  bool launchReady(Slot& s) {
    if (!sandbox.active || !sandbox.syscalls.active) return true;
    const auto signature = statSignature(executable);
    if (!signature.valid || !programDigest(signature).empty()) { s.digestWaitSince = 0; return true; }
    hasher.request(executable, signature);
    const auto now = mono();
    if (s.digestWaitSince == 0) s.digestWaitSince = now;
    return now - s.digestWaitSince > 5000;
  }
#ifdef __linux__
  // The interpreter and libraries of the workerd file open as `fd`, once per file.
  void resolveSharedObjects(int fd, const BinarySignature& signature) {
    if (sandbox.sharedObjectsFor.valid && signature == sandbox.sharedObjectsFor) return;
    sandbox.sharedObjects = sharedObjects(fd, executable);
    sandbox.sharedObjectsFor = signature;
  }
  // The launcher's command line for a process (PROTOCOL.md): the sandbox's layers and binds, the system call
  // allowlist and the digest of the workerd this launch runs, then workerd's own command line.
  std::vector<std::string> launcherCommand(bool check, const std::vector<std::string>& program,
      const std::string& programSha256 = {}, bool durable = false) const {
    std::vector<std::string> args = {sandbox.launcherPath, "--status-fd", std::to_string(LAUNCHER_STATUS_FD), "--program-fd",
        std::to_string(LAUNCHER_PROGRAM_FD), "--parent-pid", std::to_string(getpid()), "--root", sandbox.root,
        "--user", "--net", "--ipc", "--uts", "--drop-caps", "--no-new-privs", "--non-dumpable"};
    if (sandbox.syscalls.active) {
      args.push_back("--seccomp");
      args.push_back(sandbox.syscalls.mode);
      args.push_back("--allow-file");
      args.push_back(sandbox.syscalls.path);
      args.push_back("--program-sha256");
      // A file this Server cannot name is never launched against a list that names another one: the
      // launcher refuses the pair and reports it as a launch failure (PROTOCOL.md, step `allowlist`).
      args.push_back(programSha256.empty() ? "unknown" : programSha256);
    }
    auto bind = [&](const char* option, const std::string& path) { args.push_back(option); args.push_back(path); };
    for (const auto& path : sandbox.sharedObjects) bind("--bind", path);
    bind("--bind-rw", "/dev/null");
    bind("--bind", "/dev/urandom");
    bind("--bind-optional", "/sys/devices/system/cpu");
    bind("--bind-optional", "/etc/localtime");
    bind("--bind", directory + "/processes");
    // The steady-state table is read by workerd itself, after the mount namespace exists, so unlike the
    // startup table -- which the launcher reads before it builds that namespace -- its file has to be in it.
    if (sandbox.syscalls.steady.active) bind("--bind", sandbox.syscalls.steady.path);
    bind("--bind-rw", runDirectory);
    // A durable process writes its objects' databases.
    if (durable) bind("--bind-rw", durableDirectory());
    if (!check && !sandbox.bindForTest.empty()) bind("--bind", sandbox.bindForTest);
    if (check) args.push_back("--check");
    args.push_back("--");
    args.insert(args.end(), program.begin(), program.end());
    return args;
  }
  // The pinned launcher, executed through its descriptor.
  std::string launcherExecutable() const { return "/proc/self/fd/" + std::to_string(sandbox.launcherFd); }
  // The startup check: the launcher sets up a process's whole environment with this Server's paths, then
  // exits instead of executing workerd. Returns its `check` record; throws its `failed` one.
  std::string runCheck(int program) {
    int fds[2];
    // A socket, not a pipe: the launcher writes its records with sendmsg, which in the observe mode also
    // carries the filter's notification listener (PROTOCOL.md).
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) != 0)
      throw std::runtime_error(std::string("cannot create the launcher's status socket: ") + strerror(errno));
    const int statusRead = raised(fds[0]), statusWrite = raised(fds[1]);
    auto closeRead = kj::defer([statusRead] { ::close(statusRead); });
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    auto cleanupActions = kj::defer([&] { posix_spawn_file_actions_destroy(&actions); });
    posix_spawn_file_actions_adddup2(&actions, statusWrite, LAUNCHER_STATUS_FD);
    posix_spawn_file_actions_adddup2(&actions, program, LAUNCHER_PROGRAM_FD);
    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);
    auto cleanupAttr = kj::defer([&] { posix_spawnattr_destroy(&attr); });
    sigset_t mask, defaults; sigemptyset(&mask); sigemptyset(&defaults);
    sigaddset(&defaults, SIGTERM); sigaddset(&defaults, SIGINT);
    posix_spawnattr_setsigmask(&attr, &mask); posix_spawnattr_setsigdefault(&attr, &defaults);
    posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF);
    auto args = launcherCommand(true, {executable}, sandbox.syscalls.programSha256);
    std::vector<char*> argv;
    for (auto& arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    pid_t pid = 0;
    const auto path = launcherExecutable();
    const int error = posix_spawn(&pid, path.c_str(), &actions, &attr, argv.data(), environ);
    ::close(statusWrite);
    if (error != 0) throw std::runtime_error(std::string("cannot start the launcher: ") + strerror(error));
    int status = 0;
    const auto deadline = mono() + 10000;
    while (waitpid(pid, &status, WNOHANG) == 0) {
      if (mono() > deadline) { kill(pid, SIGKILL); while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {} break; }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    std::string records;
    char buffer[4096];
    for (;;) {
      alignas(struct cmsghdr) char control[CMSG_SPACE(sizeof(int))];
      struct iovec part = {buffer, sizeof buffer};
      struct msghdr message = {};
      message.msg_iov = &part;
      message.msg_iovlen = 1;
      message.msg_control = control;
      message.msg_controllen = sizeof control;
      const auto n = ::recvmsg(statusRead, &message, 0);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) break;
      // The check installs the same filter as a launch; its listener has nothing to supervise.
      for (auto* header = CMSG_FIRSTHDR(&message); header != nullptr; header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS || header->cmsg_len != CMSG_LEN(sizeof(int))) continue;
        int received = -1;
        memcpy(&received, CMSG_DATA(header), sizeof received);
        if (received >= 0) ::close(received);
      }
      records.append(buffer, size_t(n));
    }
    std::string check, failed;
    std::istringstream lines(records);
    for (std::string line; std::getline(lines, line);) {
      const auto event = recordField(line, "event");
      if (event == "check") check = line; else if (event == "failed") failed = line;
    }
    if (!failed.empty()) {
      sandbox.checkRecord = failed;
      throw std::runtime_error(recordField(failed, "step") + ": " + recordField(failed, "error") + ": " + recordField(failed, "errnoText"));
    }
    const bool exited = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    if (!exited || check.empty() || recordField(check, "protocol") != std::to_string(LAUNCHER_PROTOCOL))
      throw std::runtime_error("the launcher's check did not pass (" + std::string(WIFEXITED(status) ? "exit " + std::to_string(WEXITSTATUS(status))
          : "signal " + std::to_string(WTERMSIG(status))) + ", record " + (check.empty() ? "none" : check) + ")");
    return check;
  }
#endif
#ifdef __linux__
  // Loads the allowlist the launcher is to install and checks that it belongs to the workerd this
  // Server runs: the file says which workerd it was generated for, and `programFd` is the file itself.
  // Throws for every reason the filter cannot go on; the caller decides what that means for the mode.
  void loadAllowlist(int programFd, const BinarySignature& programSignature) {
    auto& s = sandbox.syscalls;
    s.mode = config.syscallsMode;
    if (s.mode == "off") {
      s.reason = "sandbox.syscalls.mode is off";
      return;
    }
    if (s.path.empty()) throw std::runtime_error("no allowlist path");
    const int fd = ::open(s.path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) throw std::runtime_error("cannot open the system call allowlist " + s.path + ": " + strerror(errno));
    const int allowFd = raised(fd);
    auto closeAllowFd = kj::defer([allowFd] { ::close(allowFd); });
    s.sha256 = BinaryHasher::digestOf(allowFd);
    const auto text = readText(s.path);
    if (text.empty()) throw std::runtime_error("the system call allowlist " + s.path + " is empty");
    const auto parsed = parseAllowlist(text);
    if (parsed.calls.empty()) throw std::runtime_error("the system call allowlist " + s.path + " holds no call");
    if (parsed.workerdSha256.empty() || std::any_of(parsed.workerdSha256.begin(), parsed.workerdSha256.end(),
        [](const std::string& digest) { return digest.size() != 64; }))
      throw std::runtime_error("the system call allowlist " + s.path + " does not say which workerd it was generated for");
    s.calls = parsed.calls.size();
    s.names = parsed.names;
    s.workerdSha256 = parsed.workerdSha256;
    // The digest of the workerd this Server launches, computed once here (the background hasher's answer
    // comes too late for the first spawn, and every launch is compared against this one).
    s.programSignature = programSignature;
    s.programSha256 = BinaryHasher::digestOf(programFd);
    if (s.programSha256.empty()) throw std::runtime_error("cannot hash the workerd executable " + executable);
    // The list has to cover the bytes this Server runs; which of its digests that is, is recorded.
    if (std::find(s.workerdSha256.begin(), s.workerdSha256.end(), s.programSha256) == s.workerdSha256.end()) {
      std::string named;
      for (const auto& digest : s.workerdSha256) named += (named.empty() ? "" : ",") + digest.substr(0, 12);
      throw std::runtime_error("the system call allowlist " + s.path + " was generated for workerd " + named +
          ", and this Server runs " + s.programSha256.substr(0, 12) + ": an allowlist belongs to the bytes it was measured on");
    }
    s.workerdMatched = s.programSha256;
    s.active = true;
    s.reason = {};
  }
  // The steady-state table, read here for the same reasons as the startup one: the file workerd is
  // handed must name the bytes this Server runs, and the digest this Server reports must be of the bytes it
  // hands over. Its own failure takes nothing else with it -- the narrowing stays off with its reason -- and
  // in `launcher` mode, which guarantees the sandbox, a steady mode that cannot be honoured refuses the
  // start, like the startup table (setUpSandbox below).
  void loadSteadyAllowlist() {
    auto& s = sandbox.syscalls.steady;
    s.mode = config.steadySyscallsMode;
    if (s.mode == "off") {
      s.reason = "sandbox.syscalls.steady.mode is off";
      return;
    }
    if (!sandbox.syscalls.active) throw std::runtime_error(
        "the steady-state table needs the startup table: " + sandbox.syscalls.reason);
    if (s.path.empty()) throw std::runtime_error("no steady-state allowlist path");
    const int fd = ::open(s.path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) throw std::runtime_error("cannot open the steady-state system call allowlist " + s.path + ": " + strerror(errno));
    const int allowFd = raised(fd);
    auto closeAllowFd = kj::defer([allowFd] { ::close(allowFd); });
    s.sha256 = BinaryHasher::digestOf(allowFd);
    const auto parsed = parseAllowlist(readText(s.path));
    if (parsed.calls.empty()) throw std::runtime_error("the steady-state system call allowlist " + s.path + " holds no call");
    if (parsed.workerdSha256.empty() || std::any_of(parsed.workerdSha256.begin(), parsed.workerdSha256.end(),
        [](const std::string& digest) { return digest.size() != 64; }))
      throw std::runtime_error("the steady-state system call allowlist " + s.path + " does not say which workerd it was generated for");
    if (std::find(parsed.workerdSha256.begin(), parsed.workerdSha256.end(), sandbox.syscalls.programSha256) == parsed.workerdSha256.end()) {
      std::string named;
      for (const auto& digest : parsed.workerdSha256) named += (named.empty() ? "" : ",") + digest.substr(0, 12);
      throw std::runtime_error("the steady-state system call allowlist " + s.path + " was generated for workerd " + named +
          ", and this Server runs " + sandbox.syscalls.programSha256.substr(0, 12) +
          ": an allowlist belongs to the bytes it was measured on");
    }
    s.calls = parsed.calls.size();
    s.workerdSha256 = parsed.workerdSha256;
    s.workerdMatched = sandbox.syscalls.programSha256;
    s.active = true;
    s.reason = {};
  }
#endif
  // Whether the processes start through the launcher (ServerConfig.sandbox). "launcher" refuses to
  // start when the check fails; "auto" then starts workerd directly and says so (the [sandbox] line and the
  // state endpoint). Not Linux: the launcher does not exist there.
  void setUpSandbox() {
    auto& b = sandbox;
    b.mode = config.sandboxMode;
    b.launcherPath = config.launcherPath;
    b.root = directory + "/sandbox";
    b.syscalls.path = config.allowlistPath;
    b.syscalls.mode = config.syscallsMode;
    b.syscalls.reason = "the sandbox is not in effect";
    b.syscalls.steady.path = config.steadyAllowlistPath;
    b.syscalls.steady.mode = config.steadySyscallsMode;
    b.syscalls.steady.reason = "the sandbox is not in effect";
    if (b.mode == "off") {
      b.reason = "sandbox.mode is off";
      b.syscalls.reason = "sandbox.mode is off";
      b.syscalls.steady.reason = "sandbox.mode is off";
      std::cout << "[sandbox] " << sandboxStateJson() << std::endl;
      return;
    }
#ifndef __linux__
    b.reason = "the launcher exists on Linux only";
    b.syscalls.reason = "the launcher exists on Linux only";
    b.syscalls.steady.reason = "the launcher exists on Linux only";
    std::cout << "[sandbox] " << sandboxStateJson() << std::endl;
    KJ_REQUIRE(b.mode != "launcher", "sandbox.mode is launcher, but the launcher exists on Linux only");
#else
    const auto started = mono();
    try {
      if (b.launcherPath.empty()) throw std::runtime_error("no launcher path");
      const int fd = ::open(b.launcherPath.c_str(), O_RDONLY | O_CLOEXEC);
      if (fd < 0) throw std::runtime_error("cannot open the launcher " + b.launcherPath + ": " + strerror(errno));
      b.launcherFd = raised(fd);
      struct stat info;
      if (fstat(b.launcherFd, &info) != 0 || !S_ISREG(info.st_mode) || !(info.st_mode & 0111))
        throw std::runtime_error("the launcher " + b.launcherPath + " is not an executable file");
      b.launcherSignature = b.diskSignature = signatureOf(info);
      b.launcherSha256 = BinaryHasher::digestOf(b.launcherFd);
      std::filesystem::create_directories(b.root);
      std::filesystem::permissions(b.root, std::filesystem::perms::owner_all, std::filesystem::perm_options::replace);
      const int program = ::open(executable.c_str(), O_RDONLY | O_CLOEXEC);
      if (program < 0) throw std::runtime_error("cannot open the workerd executable " + executable + ": " + strerror(errno));
      const int raisedProgram = raised(program);
      auto closeProgram = kj::defer([raisedProgram] { ::close(raisedProgram); });
      struct stat programInfo;
      if (fstat(raisedProgram, &programInfo) != 0) throw std::runtime_error("cannot stat the workerd executable");
      try {
        resolveSharedObjects(raisedProgram, signatureOf(programInfo));
      } catch (const kj::Exception& error) {
        throw std::runtime_error(error.getDescription().cStr());
      }
      // The allowlist before the check, so the check installs the filter a launch would. Its own
      // failure does not take the layers that do work with it: the filter stays off with its reason, and
      // `launcher` -- the mode that guarantees the sandbox -- refuses to start below.
      try {
        loadAllowlist(raisedProgram, signatureOf(programInfo));
      } catch (const std::exception& error) {
        b.syscalls.active = false;
        b.syscalls.reason = error.what();
      }
      // The steady-state table is handed to workerd itself, so it is loaded here for the same reasons:
      // the digest this Server reports is of the bytes it hands over, and a table that names other workerd
      // bytes never reaches a process.
      try {
        loadSteadyAllowlist();
      } catch (const std::exception& error) {
        b.syscalls.steady.active = false;
        b.syscalls.steady.reason = error.what();
      }
      b.checkRecord = runCheck(raisedProgram);
      b.launcherVersion = recordField(b.checkRecord, "version");
      b.active = true;
      b.reason = {};
    } catch (const std::exception& error) {
      b.reason = error.what();
      if (b.launcherFd >= 0) { ::close(b.launcherFd); b.launcherFd = -1; }
    }
    b.checkMs = mono() - started;
    std::cout << "[sandbox] " << sandboxStateJson() << std::endl;
    if (!b.active && b.mode == "auto")
      std::cout << "[sandbox] {\"event\":\"warning\",\"warning\":\"sandbox.mode is auto and the launcher cannot set up the workerd "
                   "processes' environment here: workerd starts without it\",\"reason\":\"" << escape(b.reason) << "\"}" << std::endl;
    KJ_REQUIRE(b.active || b.mode != "launcher", "sandbox.mode is launcher, and the launcher cannot set up the workerd processes' environment here",
        b.reason.c_str());
    // The allowlist: `launcher` guarantees the whole sandbox, so a filter that cannot go on refuses
    // the start; `auto` says so and runs the layers that do work.
    if (b.active && config.syscallsMode != "off" && !b.syscalls.active) {
      std::cout << "[sandbox] {\"event\":\"warning\",\"warning\":\"sandbox.syscalls.mode is " << escape(config.syscallsMode)
                << " and the system call allowlist cannot be installed: workerd runs without the filter, with every other layer in"
                   " effect\",\"reason\":\"" << escape(b.syscalls.reason) << "\"}" << std::endl;
      KJ_REQUIRE(b.mode != "launcher", "sandbox.mode is launcher, and the system call allowlist cannot be installed",
          b.syscalls.reason.c_str());
    }
    // The same for the steady-state table: `launcher` guarantees what is configured, so a narrowing that
    // cannot be set up refuses the start; `auto` says so and runs without it.
    if (b.active && config.steadySyscallsMode != "off" && !b.syscalls.steady.active) {
      std::cout << "[sandbox] {\"event\":\"warning\",\"warning\":\"sandbox.syscalls.steady.mode is " << escape(config.steadySyscallsMode)
                << " and the steady-state system call table cannot be handed to the workerd processes: they run under the startup"
                   " table alone, with every other layer in effect\",\"reason\":\"" << escape(b.syscalls.steady.reason) << "\"}" << std::endl;
      KJ_REQUIRE(b.mode != "launcher", "sandbox.mode is launcher, and the steady-state system call table cannot be installed",
          b.syscalls.steady.reason.c_str());
    }
#endif
  }
  // A launch that failed before workerd ran: the launcher's `failed` record, or no `exec` record at all.
  void recordLaunchFailure(Slot& s, ProcessGeneration& p) {
    ++s.launchFailures; ++sandbox.failures;
    const auto& record = p.launchFailure;
    p.lastReason = record.empty() ? "launch failed: the launcher wrote no record (" + p.exitStatus + ")"
        : "launch failed: " + recordField(record, "step") + ": " + recordField(record, "error") + ": " + recordField(record, "errnoText");
    s.lastExitCategory = "launch-failed";
    std::ostringstream out;
    out << "{\"processId\":\"" << p.id << "\",\"processClass\":\"" << processClass(p) << "\",\"generation\":"
        << p.generation << ",\"pid\":" << p.spawnPid << ",\"observedAtUnixMs\":" << wall() << ",\"exit\":\"" << escape(p.exitStatus)
        << "\",\"reason\":\"" << escape(p.lastReason) << "\",\"failures\":" << s.launchFailures << ",\"record\":" << jsonOrNull(record) << "}";
    s.lastLaunchFailure = out.str();
    if (recentLaunchFailures.size() == 32) recentLaunchFailures.pop_front();
    recentLaunchFailures.push_back(out.str());
    std::cout << "[process-launch] " << out.str() << std::endl;
  }
  // A process the system call filter ended (the enforce mode): the exit category of this slot, a
  // record of the evidence -- the exit signal, the allowlist the filter was built from, and the last call
  // this generation reported while it was in the observe mode -- and the slot's own restart backoff. The
  // kernel also writes an audit record (`audit: type=1326 ... sig=31 syscall=<number>`) naming the call,
  // which a host with a readable kernel log keeps; the Server reports what it can see itself.
  void recordSyscallDenial(Slot& s, ProcessGeneration& p) {
    ++sandbox.syscalls.denied;
    p.lastReason = "ended by the system call filter (signal " + std::to_string(SYSCALL_DENIED_SIGNAL) + ")";
    s.lastExitCategory = "syscall-denied";
    s.lastReason = p.lastReason;
    std::ostringstream out;
    out << "{\"processId\":\"" << p.id << "\",\"processClass\":\"" << processClass(p) << "\",\"generation\":"
        << p.generation << ",\"pid\":" << p.spawnPid << ",\"observedAtUnixMs\":" << wall() << ",\"exit\":\"" << escape(p.exitStatus)
        << "\",\"reason\":\"" << escape(p.lastReason) << "\",\"mode\":\"" << escape(p.syscallMode)
        << "\",\"steadyMode\":\"" << escape(sandbox.syscalls.steady.active ? sandbox.syscalls.steady.mode : std::string("off"))
        << "\",\"steadyAllowlistSha256\":\"" << sandbox.syscalls.steady.sha256
        << "\",\"steadyAllowlistCalls\":" << sandbox.syscalls.steady.calls << ",\"allowlistSha256\":\""
        << sandbox.syscalls.sha256 << "\",\"allowlistCalls\":" << sandbox.syscalls.calls << ",\"workerdSha256\":\""
        << sandbox.syscalls.workerdMatched << "\",\"reportedWhileObserving\":[";
    size_t i = 0;
    for (const auto& [number, count] : p.observedCalls) {
      if (i++) out << ',';
      out << "{\"call\":" << number << ",\"name\":\"" << escape(sandbox.syscalls.name(number)) << "\",\"count\":" << count << "}";
    }
    out << "],\"inFlight\":" << p.inFlight << ",\"dispatchedCount\":" << p.dispatched
        << ",\"auditRecordHint\":\"the kernel's seccomp audit record names the call number (sig=31)\"}";
    if (sandbox.syscalls.recentDenials.size() == 32) sandbox.syscalls.recentDenials.pop_front();
    sandbox.syscalls.recentDenials.push_back(out.str());
    std::cout << "[process-syscall-denied] " << out.str() << std::endl;
  }
  // The launcher file on disk against the one this Server pinned: a change raises the restart hint only.
  void checkLauncher() {
    if (!sandbox.active) return;
    const auto disk = statSignature(sandbox.launcherPath);
    if (disk.valid && !(disk == sandbox.diskSignature)) {
      sandbox.diskSignature = disk;
      hasher.request(sandbox.launcherPath, disk);
    }
    const auto digest = disk.valid && disk == sandbox.launcherSignature ? sandbox.launcherSha256 : hasher.lookup(sandbox.diskSignature);
    const bool changed = !disk.valid || (!digest.empty() && digest != sandbox.launcherSha256);
    if (changed && !sandbox.changedOnDisk) {
      sandbox.changedOnDisk = true; sandbox.changedAtUnixMs = wall();
      std::cout << "[sandbox] {\"event\":\"launcher-changed\",\"path\":\"" << escape(sandbox.launcherPath) << "\",\"startedWithSha256\":\""
          << sandbox.launcherSha256 << "\",\"diskSha256\":\"" << digest << "\",\"observedAtUnixMs\":" << sandbox.changedAtUnixMs
          << ",\"note\":\"no rotation; restart the Server to use it\"}" << std::endl;
    } else if (!changed && sandbox.changedOnDisk && !digest.empty()) {
      sandbox.changedOnDisk = false;
    }
  }
  // A rotation spawns the slot's next generation beside the current one; each generation has its
  // own socket name, so the two never collide.
  void spawn(Slot& s, size_t index, bool rotation = false) {
    auto p = std::make_shared<ProcessGeneration>();
    p->id = s.id; p->quarantine = s.quarantine; p->durable = s.durable; p->generation = ++s.generation; p->rotation = rotation;
    p->cpuUsage = cpuUsage;
    p->socketName = p->id + "-" + std::to_string(p->generation) + ".sock";
    p->socketPath = runDirectory + "/" + p->socketName;
    // The channels (see process-pool.h): created here, inherited by the child under fixed numbers,
    // and the Server's copies of the child's ends closed once it is spawned.
    auto [eventRead, eventWrite] = eventPipe();
    p->eventFd = eventRead;
    auto closeEventWrite = kj::defer([fd = eventWrite] { ::close(fd); });
    p->eventChannel = true;
    p->contextsVisible = true;
    p->eventDigest = EVP_MD_CTX_new();
    KJ_REQUIRE(p->eventDigest && EVP_DigestInit_ex(p->eventDigest, EVP_sha256(), nullptr) == 1, "cannot hash the event channel");
    const int listener = listenUnix(p->socketPath);
    auto closeListener = kj::defer([listener] { ::close(listener); });
    const int lifecycle = p->lifecycle.create();
    auto closeLifecycle = kj::defer([lifecycle] { ::close(lifecycle); });
    if (!rotation && p->generation > 1) ++s.restarts;
    auto signal = kj::newPromiseAndFulfiller<void>();
    p->exited = kj::mv(signal.fulfiller); p->exitSignal = kj::mv(signal.promise).fork();
    const auto path = directory + "/processes/" + p->id + ".capnp";
    if (!egressAddress.empty()) {
      unsigned char random[16];
      KJ_REQUIRE(RAND_bytes(random, sizeof(random)) == 1, "cannot draw an egress token");
      static const char* digits = "0123456789abcdef";
      for (auto byte : random) { p->egressToken.push_back(digits[byte >> 4]); p->egressToken.push_back(digits[byte & 15]); }
    }
    std::filesystem::copy_file(runtimeFile, directory + "/processes/" + p->id + ".runtime.js",
        std::filesystem::copy_options::overwrite_existing);
    std::ofstream configFile(path);
    configFile << "using Workerd = import \"/workerd/workerd.capnp\";\nconst config :Workerd.Config = (\n";
    p->wasmMemoryCapMiB = wasmMemoryCapMiB();
    std::vector<std::string> v8Flags;
    if (p->wasmMemoryCapMiB) v8Flags = {"--wasm-max-mem-pages=" + std::to_string(uint64_t(p->wasmMemoryCapMiB) * 16),
        "--enforce-global-heap-limit", "--maximum-global-heap-limit-factor=1"};
    // Every isolate's old generation starts this small, so its first full collection comes early and is small
    // (gc.initialOldSpaceMiB); V8 grows it from there.
    if (config.gcInitialOldSpaceMiB) v8Flags.push_back("--initial-old-space-size=" + std::to_string(config.gcInitialOldSpaceMiB));
    if (!v8Flags.empty()) {
      configFile << "v8Flags = [";
      for (size_t i = 0; i < v8Flags.size(); ++i) configFile << (i ? ", " : "") << "\"" << v8Flags[i] << "\"";
      configFile << "],\n";
    }
    // A durable process's runtime also has the Host class's namespace (runtime.js: each object is a Host running its
    // Worker's class as a facet), stored on the local disk below <stack>/do, and the Host Binding. The runtime runs with
    // `experimental`: the Worker Loader gives a tenant isolate a streaming tail (its log lines as they are written) only
    // when its loader has that flag (runtime.js, TenantLog); the tenant isolates' own flags are the definitions'.
    configFile << "services = [(name = \"runtime\", worker = (compatibilityDate = \"2026-08-27\",\n"
        "compatibilityFlags = [\"enable_request_signal\", \"experimental\"], modules = [(name = \"runtime.js\", esModule = embed \""
        << p->id << ".runtime.js\")], bindings = [\n"
        "(name = \"LOADER\", workerLoader = (id = \"coworkerd-v1\")),\n"
        "(name = \"SERVER\", service = \"server\"), (name = \"SOURCE\", service = \"source\"),\n"
        << (egressAddress.empty() ? std::string() : "(name = \"EGRESS\", service = \"egress\"), (name = \"EGRESS_TOKEN\", text = \"" +
            p->egressToken + "\"),\n")
        << (p->durable ? "(name = \"HOST\", durableObjectNamespace = \"Host\"),\n"
            "(name = \"SQL_CLASS\", durableObjectClass = (name = \"runtime\", entrypoint = \"SqlDatabase\")),\n" : "") <<
        "(name = \"PROCESS_ID\", text = \"" << p->id << "\"), (name = \"PROCESS_GENERATION\", text = \""
        << p->generation << "\"), (name = \"PATH_TIMING\", text = \""
        << (std::getenv("COWORKERD_PATH_TIMING") ? "1" : "") << "\")], globalOutbound = \"none\""
        << (p->durable ? std::string(",\ndurableObjectNamespaces = [(className = \"Host\", uniqueKey = \"") + DURABLE_HOST_KEY +
            "\", enableSql = true)],\ndurableObjectStorage = (localDisk = \"do-disk\")" : std::string()) << ")),\n"
        << (p->durable ? "(name = \"do-disk\", disk = (path = \"" + escape(durableDirectory()) + "\", writable = true)),\n" : "") <<
        "(name = \"server\", external = (address = \"" << escape(bindingAddress)
        << "\", capnp = (capnpConnectHost = \"coworkerd-server\"))),\n"
        "(name = \"source\", external = (address = \"" << escape(sourceAddress) << "\", http = ())),\n"
        // The tenant Workers' global outbound: the request line carries the whole URL (the proxy form).
        << (egressAddress.empty() ? std::string() : "(name = \"egress\", external = (address = \"" + escape(egressAddress) +
            "\", http = (style = proxy))),\n") <<
        "(name = \"none\", network = (allow = [], deny = []))],\n"
        // No address: the listening socket is the descriptor given with --socket-fd.
        "sockets = [(name = \"rpc\", service = \"runtime\", http = (capnpConnectHost = \"coworkerd-runtime\"))]);\n";
    configFile.close(); KJ_REQUIRE(configFile.good(), "cannot write process configuration");
    posix_spawn_file_actions_t actions;
    KJ_REQUIRE(posix_spawn_file_actions_init(&actions) == 0);
    auto cleanupActions = kj::defer([&] { posix_spawn_file_actions_destroy(&actions); });
    auto logPath = directory + "/logs/workerd.log";
    // In serve mode a drainer process outside the event loop owns the read ends
    // and appends whole lines to logs/workerd.log, so a slow sink never blocks workerd.
    WorkerdOutput::Pipes output;
    const bool piped = WorkerdOutput::open(output);
    if (piped) {
      KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, output.stdoutWrite, STDOUT_FILENO) == 0);
      KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, output.stderrWrite, STDERR_FILENO) == 0);
    } else {
      KJ_REQUIRE(posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, logPath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600) == 0);
      KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO) == 0);
    }
    auto parent = parentPipe();
    const int parentRead = parent.first;
    p->parentWrite = parent.second;
    auto closeParentRead = kj::defer([parentRead] { ::close(parentRead); });
    // Sources are above fd 10, so no earlier action overwrites a later one's source.
    KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, eventWrite, EVENT_FD) == 0);
    KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, listener, LISTEN_FD) == 0);
    KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, lifecycle, LIFECYCLE_FD) == 0);
    KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, parentRead, PARENT_FD) == 0);
    if (bandFd >= 0) {
      KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, bandFd, BAND_FD) == 0);
    }
    // The Durable Object log channel: the node keeps its own end and reads the frames the process ships.
    int durableLogFd = -1;
    auto closeDurableLog = kj::defer([&durableLogFd] { if (durableLogFd >= 0) ::close(durableLogFd); });
    if (p->durable && durableChannelOpen) {
      durableLogFd = raised(durableChannelOpen(p->id + "-" + std::to_string(p->generation)));
      if (durableLogFd >= 0) {
        KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, durableLogFd, DURABLE_LOG_FD) == 0);
      }
    }
    // The workerd file this generation runs, opened by the Server itself: the generation's identity is
    // this descriptor's (no /proc/<pid>/exe, whose path differs in the launcher's mount namespace), and the
    // launcher binds exactly this file. Hashed through the descriptor, off the event loop, once per file.
    const int program = raised(::open(executable.c_str(), O_RDONLY | O_CLOEXEC));
    KJ_REQUIRE(program >= 0, "cannot open the workerd executable", executable.c_str(), strerror(errno));
    auto closeProgram = kj::defer([program] { ::close(program); });
    struct stat programInfo;
    KJ_REQUIRE(fstat(program, &programInfo) == 0, "cannot stat the workerd executable", executable.c_str());
    p->spawnBinary = signatureOf(programInfo);
    hasher.request(program, p->spawnBinary);
    int statusWrite = -1;
    auto closeStatusWrite = kj::defer([&statusWrite] { if (statusWrite >= 0) ::close(statusWrite); });
#ifdef __linux__
    if (sandbox.active) {
      try {
        resolveSharedObjects(program, p->spawnBinary);
      } catch (const kj::Exception& error) {
        // A workerd file whose libraries cannot be resolved: the previous list is used, and the launch shows it.
        std::cout << "[sandbox] {\"event\":\"warning\",\"warning\":\"cannot resolve the workerd executable's libraries\",\"reason\":\""
            << escape(error.getDescription().cStr()) << "\"}" << std::endl;
      }
      int fds[2];
      // A socket, not a pipe: the launcher writes its records with sendmsg, and in the observe mode of the
      // allowlist that record carries the filter's notification listener (PROTOCOL.md).
      KJ_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0, "cannot create the launcher's status socket");
      p->statusFd = raised(fds[0]);
      statusWrite = raised(fds[1]);
      fcntl(p->statusFd, F_SETFL, fcntl(p->statusFd, F_GETFL) | O_NONBLOCK);
      p->launched = true;
      if (sandbox.syscalls.active) p->syscallMode = sandbox.syscalls.mode;
      KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, statusWrite, LAUNCHER_STATUS_FD) == 0);
      KJ_REQUIRE(posix_spawn_file_actions_adddup2(&actions, program, LAUNCHER_PROGRAM_FD) == 0);
    }
#endif
    posix_spawnattr_t attr; KJ_REQUIRE(posix_spawnattr_init(&attr) == 0);
    auto cleanupAttr = kj::defer([&] { posix_spawnattr_destroy(&attr); });
    sigset_t mask, defaults; sigemptyset(&mask); sigemptyset(&defaults);
    sigaddset(&defaults, SIGTERM); sigaddset(&defaults, SIGINT);
    posix_spawnattr_setsigmask(&attr, &mask); posix_spawnattr_setsigdefault(&attr, &defaults);
    posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF);
    // Per-child configuration must not mutate the Server's environment. workerd gets the variables below and the
    // ones this node sets for it, nothing else of the node's environment: no management token, storage key or other
    // credential the node was given reaches a workerd process (directly or through the launcher, which passes its
    // environment on with PWD=/ added).
    std::vector<std::string> childEnvironment;
    for (char** entry = environ; *entry; ++entry) {
      std::string value(*entry);
      if (workerdInherits(value.substr(0, value.find('=')))) childEnvironment.push_back(std::move(value));
    }
    childEnvironment.push_back("COWORKERD_CPU_LIMIT_MS=" + std::to_string(config.cpuLimitMs));
    childEnvironment.push_back("COWORKERD_MEMORY_LIMIT_MIB=" + std::to_string(config.memoryLimitMiB));
    childEnvironment.push_back("COWORKERD_EVENT_FD=" + std::to_string(EVENT_FD));
    childEnvironment.push_back("COWORKERD_LIFECYCLE_FD=" + std::to_string(LIFECYCLE_FD));
    childEnvironment.push_back("COWORKERD_PARENT_FD=" + std::to_string(PARENT_FD));
    childEnvironment.push_back("COWORKERD_IDLE_EVICT_MS=" + std::to_string(config.idleEvictMs));
    childEnvironment.push_back("COWORKERD_IDLE_DESTROY_INTERVAL_MS=" + std::to_string(config.idleDestroyIntervalMs));
    if (bandFd >= 0) childEnvironment.push_back("COWORKERD_MEMORY_BAND_FD=" + std::to_string(BAND_FD));
    // The Durable Object log (workerd's patch coworkerd-wal-shipper.h): where to ship the frames, how long a commit
    // waits for the node's answer, and when to checkpoint.
    if (durableLogFd >= 0) {
      childEnvironment.push_back("COWORKERD_DO_WAL_FD=" + std::to_string(DURABLE_LOG_FD));
      childEnvironment.push_back("COWORKERD_DO_CONFIRM_TIMEOUT_MS=" + std::to_string(durableLog.confirmTimeoutMs));
      childEnvironment.push_back("COWORKERD_DO_CHECKPOINT_BYTES=" + std::to_string(durableLog.checkpointBytes));
      childEnvironment.push_back("COWORKERD_DO_RESIDENT_OBJECTS=" + std::to_string(durableLog.residentObjects));
    }
    // V8's collections (gc.*; workerd's patch coworkerd-gc.h): the budget the CPU limit does not charge, the long
    // pauses it names, the idle collection, and the runtime's heap.
    childEnvironment.push_back("COWORKERD_GC_BUDGET_MS=" + std::to_string(config.gcBudgetMs));
    childEnvironment.push_back("COWORKERD_GC_REFILL_MS_PER_S=" + std::to_string(config.gcRefillMsPerSecond));
    childEnvironment.push_back("COWORKERD_GC_LONG_PAUSE_MS=" + std::to_string(config.gcLongPauseMs));
    childEnvironment.push_back("COWORKERD_IDLE_GC_MS=" + std::to_string(config.gcIdleCollectAfterMs));
    childEnvironment.push_back("COWORKERD_IDLE_GC_MIN_MIB=" + std::to_string(config.gcIdleCollectMinGrowthMiB));
    childEnvironment.push_back("COWORKERD_STATIC_HEAP_MIB=" + std::to_string(config.gcRuntimeHeapMiB));
#ifdef __linux__
    // What only the process itself can do, once it serves (coworkerd-steady.h): narrow its system calls
    // to the steady table, and make itself non-dumpable again -- the launcher sets that bit before the
    // execve and the kernel undoes it. Both are asked for here and only where the sandbox is
    // in effect: without the launcher's environment there is nothing to narrow from.
    if (p->launched && sandbox.syscalls.steady.active) {
      childEnvironment.push_back("COWORKERD_SECCOMP_STEADY_MODE=" + sandbox.syscalls.steady.mode);
      childEnvironment.push_back("COWORKERD_SECCOMP_STEADY_FILE=" + sandbox.syscalls.steady.path);
    }
    if (p->launched) childEnvironment.push_back("COWORKERD_NON_DUMPABLE=1");
#endif
    std::vector<char*> childEnv;
    for (auto& entry: childEnvironment) childEnv.push_back(entry.data());
    childEnv.push_back(nullptr);
    // workerd's command line; with the sandbox, the launcher's options before it (PROTOCOL.md), and the
    // launcher itself executed through the descriptor this Server pinned at startup.
    std::vector<std::string> args = {executable, "serve", "--experimental", "--socket-fd", "rpc=" + std::to_string(LISTEN_FD), path};
    std::string file = executable;
#ifdef __linux__
    if (p->launched) {
      args = launcherCommand(false, args, programDigest(p->spawnBinary), p->durable);
      file = launcherExecutable();
      ++sandbox.launches;
    }
#endif
    std::vector<char*> argv;
    for (auto& arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    auto error = posix_spawn(&p->pid, file.c_str(), &actions, &attr, argv.data(), childEnv.data());
    if (error != 0) ::unlink(p->socketPath.c_str());
    KJ_REQUIRE(error == 0, "cannot spawn workerd", error);
    p->spawnPid = p->pid;
    p->nextEventSync = mono() + LIMIT_EVENT_SYNC_MS;
    if (piped) WorkerdOutput::attach(output, index, p->id, processClassCode(*p), p->generation, p->pid);
    if (rotation) {
      s.incoming = p;
    } else {
      s.current = p;
      writePidFiles(*p, index);
    }
    event(*p, "spawn", rotation ? "rotation" : "");
    tasks.add(watchEvents(p));
#ifdef __linux__
    // The launcher's records are read as they arrive, not only at readiness: in the observe mode its first
    // record carries the notification listener, and until this Server holds it a call outside the
    // allowlist would wait for a supervisor that is not there yet.
    if (p->launched) tasks.add(watchLaunch(p));
#endif
    tasks.add(initialize(p));
  }
#ifdef __linux__
  // Drains the launcher's status socket as it becomes readable, until the launcher's end closes (which the
  // execv does). Picks up the observe mode's notification listener and starts supervising it.
  kj::Promise<void> watchLaunch(std::shared_ptr<ProcessGeneration> p) {
    p->statusObserver = kj::heap<kj::UnixEventPort::FdObserver>(eventPort, p->statusFd, kj::UnixEventPort::FdObserver::OBSERVE_READ);
    while (p->statusFd >= 0 && !p->statusEnded) {
      co_await p->statusObserver->whenBecomesReadable();
      // Reading the record also takes the observe mode's notification listener and starts supervising it.
      p->readLaunch();
    }
    p->closeStatus();
  }
  // What the generation's supervisor thread saw since the last tick: counted, and reported by the event
  // loop, so the lines and the state endpoint are written where every other record of this Server is.
  void drainSyscallObservations(ProcessGeneration& p) {
    if (!p.syscalls) return;
    auto drained = p.syscalls->drain();
    sandbox.syscalls.interrupted += drained.interrupted;
    sandbox.syscalls.errors += drained.errors;
    p.syscallInterrupted += drained.interrupted;
    sandbox.syscalls.detailsDropped += drained.dropped;
    countSyscalls(p, drained.counts, drained.notifications);
    for (const auto& observation : drained.observations) observeSyscall(p, observation.number, observation.tid, observation.thread);
  }
  // The supervisor's exact counts: per call number for the generation and for this Server, whatever the details kept.
  void countSyscalls(ProcessGeneration& p, const std::map<int, uint64_t>& counts, uint64_t notifications) {
    auto& s = sandbox.syscalls;
    s.notifications += notifications;
    p.syscallNotifications += notifications;
    const auto now = wall();
    for (const auto& [number, count] : counts) {
      const auto existing = s.observed.find(number);
      if (existing == s.observed.end() && s.observed.size() >= SYSCALL_OBSERVED_CAP) { s.beyondCap += count; continue; }
      auto& entry = s.observed[number];
      entry.count += count;
      entry.lastAtUnixMs = now;
      if (entry.firstAtUnixMs == 0) entry.firstAtUnixMs = now;
      if (entry.processes.size() < 16) entry.processes.insert(p.id);
      if (p.observedCalls.size() < SYSCALL_OBSERVED_CAP || p.observedCalls.contains(number)) p.observedCalls[number] += count;
    }
  }
  // One observation's details: the name of the thread it came from, and a report at most once per number per
  // SYSCALL_REPORT_INTERVAL_MS (counted already: countSyscalls).
  void observeSyscall(ProcessGeneration& p, int number, unsigned tid, const std::string& thread) {
    auto& s = sandbox.syscalls;
    const auto existing = s.observed.find(number);
    if (existing == s.observed.end()) return;
    auto& entry = existing->second;
    if (entry.threads.size() < 16 && !thread.empty()) entry.threads.insert(thread);
    if (p.observedCalls.contains(number)) {
      auto& threads = p.observedThreads[number];
      if (threads.size() < 16 && !thread.empty()) threads.insert(thread);
    }
    const auto now = mono();
    if (entry.lastReportedAt != 0 && now - entry.lastReportedAt < SYSCALL_REPORT_INTERVAL_MS) return;
    entry.lastReportedAt = now;
    ++s.reported;
    std::cout << "[syscall-observed] {\"processId\":\"" << p.id << "\",\"processClass\":\"" << processClass(p)
        << "\",\"generation\":" << p.generation << ",\"pid\":" << p.spawnPid << ",\"tid\":" << tid << ",\"call\":" << number
        << ",\"name\":\"" << escape(s.name(number)) << "\",\"thread\":\"" << escape(thread) << "\",\"count\":" << entry.count
        << ",\"countThisProcess\":" << p.syscallNotifications << ",\"mode\":\"" << escape(s.mode) << "\",\"allowlistSha256\":\""
        << s.sha256 << "\",\"observedAtUnixMs\":" << entry.lastAtUnixMs << "}" << std::endl;
  }
#endif
  void writePidFiles(const ProcessGeneration& p, size_t index) {
    std::ofstream(directory + "/logs/" + p.id + ".pid") << p.pid << '\n';
    if (index == 0) std::ofstream(directory + "/logs/workerd.pid") << p.pid << '\n';
  }
  // Keeps the generation's event pipe drained as rows arrive, so workerd never waits on a full pipe.
  // The pipe ends when the child's last descriptor closes; the exit reads what is left (reap()).
  kj::Promise<void> watchEvents(std::shared_ptr<ProcessGeneration> p) {
    p->eventObserver = kj::heap<kj::UnixEventPort::FdObserver>(eventPort, p->eventFd, kj::UnixEventPort::FdObserver::OBSERVE_READ);
    while (!p->eventsFinal && !p->eventsEnded && p->eventFd >= 0) {
      co_await p->eventObserver->whenBecomesReadable();
      p->drainEvents();
    }
  }
  kj::Promise<void> initialize(std::shared_ptr<ProcessGeneration> p) {
    const auto until = mono() + 15000;
    auto text = "unix:" + p->socketPath;
    p->probeAddress = co_await provider.getNetwork().parseAddress(text.c_str());
    p->probeClient = kj::newHttpClient(provider.getTimer(), headers, *p->probeAddress);
    while (p->pid > 0 && mono() < until) {
      try {
        // The socket queues a connection from the moment it exists, before workerd accepts: the
        // readiness probe waits for its answer (within the startup budget) rather than giving the
        // connection up and leaving workerd to accept one whose peer is gone.
        kj::HttpHeaders probeHeaders(headers); probeHeaders.setPtr(kj::HttpHeaderId::HOST, "runtime");
        auto response = co_await provider.getTimer().timeoutAfter(int64_t(std::max(1.0, until - mono())) * kj::MILLISECONDS,
            p->probeClient->request(kj::HttpMethod::GET, "/__coworkerd/probe", probeHeaders).response);
        auto identity = co_await response.body->readAllText(1024);
        KJ_REQUIRE(response.statusCode == 200 && identity == kj::str(p->id.c_str(), ":", p->generation), "wrong process readiness identity");
        if (!p->binary.valid) {
          // The generation's identity attaches once it answered its probe: it is the workerd
          // file the Server opened for it at the spawn -- what the launcher bound, or what posix_spawn
          // executed -- not /proc/<pid>/exe, whose path differs in the launcher's mount namespace.
          p->binary = p->spawnBinary;
#ifdef __linux__
          if (p->launched) p->readLaunch();
          p->observedSandbox = observeSandbox(p->pid);
#endif
        }
        auto r = std::make_shared<RuntimeConnection>();
        r->address = co_await provider.getNetwork().parseAddress(text.c_str());
        r->connector = kj::newHttpClient(provider.getTimer(), headers, *r->address);
        auto tunnel = r->connector->connect("coworkerd-runtime", kj::HttpHeaders(headers), {});
        auto status = co_await provider.getTimer().timeoutAfter(500 * kj::MILLISECONDS, kj::mv(tunnel.status));
        KJ_REQUIRE(status.statusCode == 200, "workerd rejected CONNECT");
        r->stream = kj::mv(tunnel.connection); r->rpc = kj::heap<capnp::TwoPartyClient>(*r->stream);
        r->bootstrap = r->rpc->bootstrap().castAs<v1::rpc::WorkerdBootstrap>();
        if (p->pid == 0) co_return;
        p->runtime = r;
        // Weak: closing a generation releases its connection; a destroyed RPC system
        // rejects this promise, which is expected.
        tasks.add(r->rpc->onDisconnect().then([weak = std::weak_ptr<RuntimeConnection>(r)] {
          if (auto connection = weak.lock()) connection->disconnected = true;
        }, [](kj::Exception&&) {}));
        p->state = "ready"; p->readyAt = mono(); p->readyAtUnixMs = wall(); p->nextProbeAt = mono();
        event(*p, "ready"); co_return;
      } catch (const kj::Exception&) {}
      co_await provider.getTimer().afterDelay(20 * kj::MILLISECONDS);
    }
    if (p->pid > 0) killProcess(*p, "startup timeout", mono() - p->spawnedAt);
  }
  kj::Promise<void> probe(std::shared_ptr<ProcessGeneration> p) {
    auto clearPending = kj::defer([p] { p->probePending = false; });
    try {
      ++p->probeAttempts;
      if (auto* every = std::getenv("COWORKERD_PROBE_ERROR_EVERY_FOR_TEST")) {
        auto interval = std::strtoul(every, nullptr, 10);
        if (interval && p->probeAttempts % interval == 0)
          kj::throwRecoverableException(KJ_EXCEPTION(DISCONNECTED, "injected immediate probe failure"));
      }
      kj::HttpHeaders probeHeaders(headers); probeHeaders.setPtr(kj::HttpHeaderId::HOST, "runtime");
      auto request = p->probeClient->request(kj::HttpMethod::GET,
          "/__coworkerd/probe", probeHeaders);
      auto response = co_await provider.getTimer().timeoutAfter(config.stuckTimeoutMs * kj::MILLISECONDS, kj::mv(request.response));
      auto text = co_await response.body->readAllText(1024);
      KJ_REQUIRE(response.statusCode == 200 && text == kj::str(p->id.c_str(), ":", p->generation), "wrong probe identity");
      if (p->pid == 0 || p->state == "killed" || p->closing) co_return;
      if (p->probeDelays.size() == 1024) p->probeDelays.pop_front();
      p->probeDelays.push_back(mono() - p->probeAttemptAt);
      if (p->state == "paused") event(*p, "resume");
      // A draining generation answers probes but never becomes selectable again.
      p->state = p->retiring ? "draining" : "ready"; p->probeStartedAt = 0; p->nextProbeAt = mono() + config.probeIntervalMs;
    } catch (const kj::Exception&) {
      // The supervisor makes the overdue/kill decision using the original start time.
      if (p->pid > 0 && p->probeStartedAt == 0) p->probeStartedAt = mono();
      ++p->probeFailures;
      p->nextProbeAt = mono() + config.probeIntervalMs;
    }
  }
  void maybeProbe(const std::shared_ptr<ProcessGeneration>& p) {
    if (!config.watchdog || !p || p->closing || p->probePending || mono() < p->nextProbeAt) return;
    if (p->state != "ready" && p->state != "paused" && p->state != "draining") return;
    p->probePending = true; p->probeAttemptAt = mono();
    if (p->probeStartedAt == 0) p->probeStartedAt = p->probeAttemptAt;
    tasks.add(probe(p));
  }
  // Applies the generation's newly read limit events to the counters.
  void applyEvents(Slot& s, ProcessGeneration& p) {
    p.pullEvents();
    for (const auto& e : p.unappliedEvents) {
      // The row's limit is the one its isolate ran under: its Worker's own where it names one, else
      // this node's. A row that names another limit is not this Worker's accounting.
      const auto limit = limitsOf(e.workerId).memoryMiB;
      if (limit && e.memoryLimitMiB == limit && !e.workerId.empty())
        workerMemoryStats[e.workerId].peakUsedBytes = std::max(workerMemoryStats[e.workerId].peakUsedBytes, e.peakUsedBytes);
      if (e.event == "created") {
        if (s.invalidatedIsolates.erase(e.isolate)) ++s.cpuReloadedIsolates;
      } else if (e.event == "terminated") {
        ++s.cpuTerminatedRequests;
        if (s.invalidatedIsolates.insert(e.isolate).second) ++s.cpuInvalidatedIsolates;
        if (!e.workerId.empty()) ++workerCpuStats[e.workerId].terminated;
      } else if (e.event == "memory-created") {
        if (s.memoryInvalidated.erase(e.isolate)) ++s.memoryReloadedIsolates;
      } else if (e.event == "memory-terminated" && e.memoryLimitMiB == limit) {
        ++s.memoryTerminatedRequests;
        if (s.memoryInvalidated.insert(e.isolate).second) ++s.memoryInvalidatedIsolates;
        if (!e.workerId.empty()) ++workerMemoryStats[e.workerId].terminated;
      } else if (e.event == "gc-pause") {
        ++s.gcLongPauses;
        if (e.pauseNs > s.gcLongestPauseNs) { s.gcLongestPauseNs = e.pauseNs; s.gcLongestPause = e.row; }
        if (recentGcPauses.size() == 32) recentGcPauses.pop_front();
        recentGcPauses.push_back(e.row);
      }
    }
    p.unappliedEvents.clear();
  }
  void syncEvents() { for (auto& s : slots) eachGeneration(s, [&](const auto& p) { applyEvents(s, *p); }); }
  // The supervisor cadence: each live generation's events at most once per LIMIT_EVENT_SYNC_MS.
  void syncDueEvents() {
    observeOomKills();
    const auto now = mono();
    for (auto& s : slots) eachGeneration(s, [&](const auto& p) {
      if (p->pid == 0 || now < p->nextEventSync) return;
      applyEvents(s, *p);
      p->pruneContexts(now);
      p->nextEventSync = now + LIMIT_EVENT_SYNC_MS;
    });
  }
  // The OOM rows workerd's OOM handler wrote to the event pipe before the process died: the bare
  // OOM row, then the identity of the execution that hit it (when one was executing).
  void memoryExit(Slot& slot, ProcessGeneration& p, int status) {
    const auto& row = p.oomCulpritRow;
    std::string offender = "null";
    bool v8Oom = p.oomSignalled;
    if (!row.empty() && row.size() <= 16384) try {
      capnp::MallocMessageBuilder message;
      auto report = message.initRoot<v1::rpc::CpuReport>();
      capnp::JsonCodec().decode(kj::StringPtr(row.c_str()), report);
      const auto stateOom = report.getPid() == static_cast<uint32_t>(p.pid) &&
          (report.getEvent() == "memory-culprit" || report.getEvent() == "memory-oom-undetermined");
      v8Oom = v8Oom || stateOom;
      const std::string rowWorker = report.getWorkerId().cStr();
      if (stateOom && report.getEvent() == "memory-culprit" && report.getMemoryLimitMiB() == limitsOf(rowWorker).memoryMiB) {
        const std::string worker = rowWorker, version = report.getVersionId().cStr();
        const std::string invocation = report.getInvocationId().cStr();
        auto current = p.calls.find(invocation);
        bool dispatched = current != p.calls.end() && current->second->dispatched &&
            current->second->workerId == worker && current->second->versionId == version;
        if (!dispatched) dispatched = std::any_of(p.recentDispatched.begin(), p.recentDispatched.end(), [&](const auto& call) {
          return call.invocationId == invocation && call.workerId == worker && call.versionId == version;
        });
        const auto prefix = "coworkerd-v1:" + worker + "@" + version + "#";
        if (dispatched && !worker.empty() && !version.empty() && !invocation.empty() &&
            std::string(report.getIsolate().cStr()).starts_with(prefix)) {
          offender = capnp::JsonCodec().encode(report.asReader()).cStr();
          ++workerMemoryStats[worker].culprit;
          suspects.confirm(worker, p.id, p.generation, invocation);
        }
      }
    } catch (const kj::Exception&) {}
    // A kernel OOM: a SIGKILL exit that takes an OOM kill the cgroup counted just before it (another process's kill,
    // seen earlier, never makes one).
    observeOomKills();
    const bool kernelOom = WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL && !unattributedOomKills.empty();
    if (kernelOom) {
      unattributedOomKills.pop_front();
      ++poolOomKills;
    }
    if (!v8Oom && !kernelOom) return;
    slot.lastExitCategory = "memory-oom";
    p.lastReason = kernelOom ? "kernel memory OOM" : "V8 memory OOM";
    std::ostringstream out;
    out << "{\"processId\":\"" << p.id << "\",\"processClass\":\"" << processClass(p)
        << "\",\"generation\":" << p.generation << ",\"pid\":" << p.pid << ",\"observedAtUnixMs\":" << wall()
        << ",\"reason\":\"" << p.lastReason << "\",\"culpritStatus\":\"" << (offender == "null" ? "undetermined" : "confirmed")
        << "\",\"culprit\":" << offender << "}";
    if (memoryCrashes.size() == 32) memoryCrashes.pop_front();
    memoryCrashes.push_back(out.str());
    std::cout << "[process-memory-crash] " << out.str() << std::endl;
  }
  // The latest culprit row the watchdog thread wrote and no termination has withdrawn since.
  std::string culprit(ProcessGeneration& p) {
    p.pullEvents();
    const auto row = p.culpritRow;
    if (row.empty() || row.size() > 16384) return "null";
    try {
      capnp::MallocMessageBuilder message;
      auto value = message.initRoot<v1::rpc::CpuReport>();
      capnp::JsonCodec().decode(kj::StringPtr(row.c_str()), value);
      auto call = p.calls.find(value.getInvocationId().cStr());
      // The limit this Worker's executions run under (its own where it names one): the row has to be
      // against that limit, and to have spent it.
      const auto limit = call == p.calls.end() ? 0u : limitsOf(call->second->workerId).cpuMs;
      if (!limit) return "null";
      if (value.getEvent() != "culprit" || value.getPid() != static_cast<uint32_t>(p.pid) ||
          value.getCpuLimitMs() != limit ||
          value.getCpuUsedNs() < uint64_t(limit) * 1000000 ||
          call == p.calls.end() || !call->second->dispatched ||
          call->second->workerId != value.getWorkerId().cStr() ||
          call->second->versionId != value.getVersionId().cStr()) return "null";
      const auto prefix = "coworkerd-v1:" + call->second->workerId + "@" + call->second->versionId + "#";
      if (!std::string(value.getIsolate().cStr()).starts_with(prefix)) return "null";
      ++workerCpuStats[call->second->workerId].culprit;
      return capnp::JsonCodec().encode(value.asReader()).cStr();
    } catch (const kj::Exception&) { return "null"; }
  }
  void killProcess(ProcessGeneration& p, const std::string& reason, double duration) {
    if (p.pid == 0 || p.state == "killed") return;
    std::ostringstream out; out << std::setprecision(17) << "{\"processId\":\"" << p.id
        << "\",\"processClass\":\"" << processClass(p)
        << "\",\"generation\":" << p.generation << ",\"pid\":" << p.pid << ",\"reason\":\"" << escape(reason)
        << "\",\"stuckDurationMs\":" << duration << ",\"observedAtUnixMs\":" << wall() << ",\"inFlight\":[";
    size_t i = 0;
    for (const auto& [id, call] : p.calls) {
      if (i++) out << ',';
      out << "{\"workerId\":\"" << call->workerId << "\",\"versionId\":\"" << call->versionId
          << "\",\"invocationId\":\"" << id << "\",\"treeId\":\"" << call->treeId
          << "\",\"startedAtMonotonicMs\":" << call->startedAtMonotonicMs << ",\"startedAtUnixMs\":" << call->startedAtUnixMs
          << ",\"dispatched\":" << (call->dispatched ? "true" : "false") << "}";
    }
    auto offender = culprit(p);
    if (reason == "watchdog timeout" && offender != "null") {
      capnp::MallocMessageBuilder message;
      auto report = message.initRoot<v1::rpc::CpuReport>();
      capnp::JsonCodec().decode(kj::StringPtr(offender.c_str()), report);
      suspects.confirm(report.getWorkerId().cStr(), p.id, p.generation, report.getInvocationId().cStr());
    }
    out << "],\"culprit\":" << offender << ",\"culpritStatus\":\""
        << (offender == "null" ? "undetermined" : "confirmed") << "\"}";
    if (kills.size() == 32) kills.pop_front();
    kills.push_back(out.str()); std::cout << "[process-kill] " << out.str() << std::endl;
    p.state = "killed"; p.lastReason = reason;
    kill(p.pid, SIGKILL); event(p, "kill", reason);
  }
  // After the exit: what the Server read from the event pipe is final. A record of it is kept: rows,
  // bytes, their SHA-256 in order, and bytes after the last complete row (none: each row is one
  // write() of at most PIPE_BUF bytes).
  void finishEvents(ProcessGeneration& p) {
    p.eventsFinal = true;
    p.incompleteTailBytes = p.eventTail.size();
    if (p.eventDigest) {
      unsigned char out[EVP_MAX_MD_SIZE];
      unsigned int length = 0;
      if (EVP_DigestFinal_ex(p.eventDigest, out, &length) == 1) p.eventSha256 = hex(out, length);
      EVP_MD_CTX_free(p.eventDigest);
      p.eventDigest = nullptr;
    }
    ::unlink(p.socketPath.c_str());
    std::ostringstream out;
    out << "{\"processId\":\"" << p.id << "\",\"generation\":" << p.generation << ",\"pid\":" << p.spawnPid
        << ",\"exitedAtUnixMs\":" << wall() << ",\"ended\":" << (p.eventsEnded ? "true" : "false")
        << ",\"limitEvents\":" << p.channelJson() << "}";
    if (channelExits.size() == 32) channelExits.pop_front();
    channelExits.push_back(out.str());
  }
  // Observes the exit of any generation of the slot. Only the current generation is restarted;
  // a generation the rotation closed itself leaves the slot's exit record untouched.
  void reap(Slot& s, const std::shared_ptr<ProcessGeneration>& p) {
    int status; auto exited = waitpid(p->pid, &status, WNOHANG);
    if (exited != p->pid) return;
    // The child can write nothing more: read its event pipe to the end before the pid is cleared.
    applyEvents(s, *p);
    finishEvents(*p);
    p->exitStatus = WIFSIGNALED(status) ? "signal " + std::to_string(WTERMSIG(status)) : "exit " + std::to_string(WEXITSTATUS(status));
    // Through the launcher: an exit before readiness without its `exec` record, or with its `failed`
    // one, is a failed launch -- workerd never ran -- unless this Server ended the process itself.
    bool launchFailed = false;
    if (p->launched) {
      p->readLaunch();
      launchFailed = p->readyAt == 0 && !p->plannedExit && p->state != "killed" && (p->launchFailed || p->launchRecord.empty());
    }
    // A system call filter in the enforce mode ends a process with SIGSYS: its own exit category, its own
    // record, and the slot's restart backoff as for any other exit. The evidence of the CPU culprit and of an
    // OOM is read from the event pipe above, as for every exit. Either filter may be the one that ended it:
    // the launcher's startup table, or the steady-state table the process installed itself -- the kernel
    // reports the call in its audit record either way, and the record below says which modes were in force.
    const bool enforcing = p->syscallMode == "enforce" ||
        (sandbox.syscalls.steady.active && sandbox.syscalls.steady.mode == "enforce");
    const bool syscallDenied = p->launched && enforcing && !p->plannedExit && p->state != "killed" &&
        WIFSIGNALED(status) && WTERMSIG(status) == SYSCALL_DENIED_SIGNAL;
    if (launchFailed) {
      recordLaunchFailure(s, *p);
    } else if (syscallDenied) {
      recordSyscallDenial(s, *p);
    } else if (!p->plannedExit) {
      s.lastExitCategory = p->lastReason == "watchdog timeout" ? "cpu-watchdog" : "other";
      if (config.memoryLimitMiB && p->lastReason.empty()) memoryExit(s, *p, status);
    }
    auto reason = p->lastReason.empty() ? p->exitStatus : p->lastReason;
    event(*p, "exit", reason);
    p->pid = 0; p->state = "exited"; p->lastReason = reason;
    if (p->durable && durableChannelClose) durableChannelClose(p->id + "-" + std::to_string(p->generation));
    p->exitedAt = mono(); p->exitedAtUnixMs = wall();
    p->exited->fulfill();
    if (rotation && rotation->old == p) {
      // The last resident set workerd published before the exit (workerd exits without tearing
      // isolates down, so the generation's lifecycle record ends here).
      auto lifecycle = p->lifecycle.read();
      if (!lifecycle.empty() && lifecycle.front() == '{' && lifecycle.back() == '}') rotation->oldLifecycle = lifecycle;
    }
    if (p->plannedExit) return;
    s.lastReason = reason; ++s.exits;
    s.lastExitedGeneration = p->generation; s.lastExitAtUnixMs = wall();
    if (p != s.current) return;
    if (p->readyAt > 0 && mono() - p->readyAt > config.stuckTimeoutMs) s.failures = 0;
    auto delay = std::min<uint64_t>(uint64_t(config.restartBackoffMs) << std::min(s.failures++, 16u), config.restartMaxMs);
    s.restartAt = mono() + delay;
  }
  // Process state for dispatch decisions; no file is opened or read here except at an exit.
  void refresh() {
    for (auto& s : slots) eachGeneration(s, [&](const auto& p) {
      std::erase_if(p->heat, [&](const auto& entry) { return mono() - entry.second > config.heatIdleMs; });
#ifdef __linux__
      drainSyscallObservations(*p);
#endif
      if (p->pid > 0) reap(s, p);
      if (config.watchdog && p->pid > 0 && p->probeStartedAt > 0 && !p->closing) {
        auto delay = mono() - p->probeStartedAt;
        if (delay >= config.stuckTimeoutMs) killProcess(*p, "watchdog timeout", delay);
        else if (delay >= config.probeOverdueMs && p->state == "ready") { p->state = "paused"; event(*p, "pause"); }
      }
    });
  }
  bool admitted(const ProcessGeneration& p) const {
    return p.pid > 0 && p.state == "ready" && p.runtime && !p.runtime->disconnected && p.inFlight < config.inFlightLimit;
  }

  // ---- Rotation ----
  void track(uint64_t id, const std::string& status, uint64_t rotationId = 0) {
    rotationRequests[id] = {status, rotationId};
    while (rotationRequests.size() > ROTATION_REQUESTS) rotationRequests.erase(rotationRequests.begin());
  }
  bool queuedOrActive(size_t slot) const {
    if (rotation && rotation->slot == slot) return true;
    return std::any_of(rotationQueue.begin(), rotationQueue.end(), [&](const auto& r) { return r.slot == slot; });
  }
  uint64_t enqueue(size_t slot, const std::string& reason) {
    const auto id = nextRequestId++;
    ++rotationCounts["requested"]; ++rotationReasons[reason];
    track(id, "queued");
    std::cout << "[process-rotation] {\"event\":\"request\",\"requestId\":" << id << ",\"processId\":\"" << slots[slot].id
        << "\",\"reason\":\"" << reason << "\",\"observedAtUnixMs\":" << wall() << "}" << std::endl;
    // One queued rotation per slot; a manual request makes a queued automatic one unconditional.
    for (auto& queued : rotationQueue) if (queued.slot == slot) {
      if (reason == "manual") queued.reason = reason;
      queued.ids.push_back(id);
      return id;
    }
    rotationQueue.push_back({slot, reason, mono(), wall(), {id}});
    return id;
  }
  void enqueueAutomatic(size_t slot, const std::string& reason) {
    if (!queuedOrActive(slot)) enqueue(slot, reason);
  }
  // The slot's current generation runs other bytes than the configured executable.
  bool binaryMismatch(size_t index, const std::string& configured) const {
    const auto& p = slots[index].current;
    if (configured.empty() || !p || p->pid == 0 || !p->binary.valid || p->binary == configuredBinary) return false;
    auto running = hasher.lookup(p->binary);
    return !running.empty() && running != configured;
  }
  void triggers(double now) {
    // The launcher is not a rotation trigger: its change on disk only raises the restart hint.
    if (sandbox.active && now >= nextLauncherCheck) {
      nextLauncherCheck = now + 1000;
      checkLauncher();
    }
    if (config.rotationBinaryCheckMs && now >= nextBinaryCheck) {
      nextBinaryCheck = now + config.rotationBinaryCheckMs;
      ++binaryChecks;
      auto signature = statSignature(executable);
      if (signature.valid && !(signature == configuredBinary)) {
        ++binaryChanges; binaryChangedAtUnixMs = wall();
        automaticRetryAt = 0; automaticBackoffMs = 0;
        std::cout << "[process-binary] {\"event\":\"changed\",\"path\":\"" << escape(executable)
            << "\",\"observedAtUnixMs\":" << binaryChangedAtUnixMs << "}" << std::endl;
        configuredBinary = signature;
      }
      hasher.request(executable, configuredBinary);
    }
    if (now < automaticRetryAt) return;
    if (config.rotationBinaryCheckMs) {
      const auto configured = hasher.lookup(configuredBinary);
      for (size_t i = 0; i < slots.size(); ++i) if (binaryMismatch(i, configured)) enqueueAutomatic(i, "binary");
    }
    for (size_t i = 0; i < slots.size(); ++i) {
      const auto& p = slots[i].current;
      // A durable process is never replaced for its age or its requests: its objects would all start again.
      if (!p || p->pid == 0 || p->readyAt == 0 || slots[i].durable) continue;
      if (config.rotationMaxAgeMs && now - p->readyAt >= double(config.rotationMaxAgeMs)) enqueueAutomatic(i, "age");
      else if (config.rotationMaxRequests && p->dispatched >= config.rotationMaxRequests) enqueueAutomatic(i, "requests");
    }
    if (config.rotationResidentBytes && now >= nextMemoryCheck) {
      nextMemoryCheck = now + 1000;
      for (size_t i = 0; i < slots.size(); ++i) {
        const auto& p = slots[i].current;
        if (p && p->pid > 0 && p->readyAt > 0 && MemoryWatermark::rotationDue(p->pid, config.rotationResidentBytes))
          enqueueAutomatic(i, "memory");
      }
    }
  }
  void rotationTick() {
    // Closing the pool at the Server's exit takes over every generation.
    if (closingAll) return;
    const auto now = mono();
    // Exited generations no rotation still refers to are released.
    for (auto& s : slots) std::erase_if(s.retiring, [&](const auto& p) { return p->pid == 0 && !(rotation && rotation->old == p); });
    if (!shuttingDown) triggers(now);
    if (!rotation && !shuttingDown) startNext(now);
    if (rotation) advance(now);
  }
  // A rotation still in progress when the pool closes: its generations stay
  // tracked (the next one joins `retiring`) and are closed with all the others.
  // The close of every generation of the slots `which` picks: the Server's connections first, then SIGTERM, then SIGKILL
  // after the termination grace (closeAll(), closeDurable()); resolves with the record once all exited.
  kj::Promise<std::string> closeGenerations(std::function<bool(const Slot&)> which);
  void abortRotation(double now, const std::string& why = "the Server is shutting down") {
    auto& r = *rotation;
    auto& s = slots[r.slot];
    r.outcome = "aborted"; r.failure = why;
    r.finishedAt = now; r.finishedAtUnixMs = wall();
    if (s.incoming) {
      s.incoming->retiring = true;
      s.retiring.push_back(s.incoming);
      s.incoming = nullptr;
    }
    r.phase = "done";
    auto record = recordJson(r);
    std::cout << "[process-rotation] {\"event\":\"aborted\",\"record\":" << record << "}" << std::endl;
    rotationHistory.emplace_back(r.id, record);
    while (rotationHistory.size() > ROTATION_HISTORY) rotationHistory.pop_front();
    ++rotationCounts["aborted"];
    for (auto id : r.requestIds) track(id, "aborted", r.id);
    rotation.reset();
  }
  // The first queued rotation whose slot has a ready current generation; one at a time.
  void startNext(double now) {
    if (rotationQueue.empty()) return;
    for (auto it = rotationQueue.begin(); it != rotationQueue.end(); ++it) {
      auto& s = slots[it->slot];
      if (s.incoming || !s.current || s.current->pid == 0 || s.current->state != "ready") continue;
      auto request = std::move(*it);
      rotationQueue.erase(it);
      if (request.reason == "binary" && !binaryMismatch(request.slot, hasher.lookup(configuredBinary))) {
        for (auto id : request.ids) track(id, "skipped");
        ++rotationCounts["skipped"];
        std::cout << "[process-rotation] {\"event\":\"skipped\",\"processId\":\"" << s.id
            << "\",\"reason\":\"binary\",\"observedAtUnixMs\":" << wall() << "}" << std::endl;
        return;
      }
      begin(std::move(request), now);
      return;
    }
  }
  void begin(RotationRequest request, double now) {
    auto& s = slots[request.slot];
    rotation = std::make_unique<Rotation>();
    auto& r = *rotation;
    r.id = nextRotationId++; r.slot = request.slot; r.reason = request.reason; r.requestIds = std::move(request.ids);
    r.requestedAt = request.requestedAt; r.requestedAtUnixMs = request.requestedAtUnixMs;
    r.startedAt = now; r.startedAtUnixMs = wall(); r.old = s.current;
    r.backgroundVisible = s.current->contextsVisible;
    r.oldBinarySha256 = hasher.lookup(s.current->binary);
    for (auto id : r.requestIds) track(id, "active", r.id);
    ++rotationCounts["started"];
    std::cout << "[process-rotation] {\"event\":\"start\",\"rotationId\":" << r.id << ",\"processId\":\"" << s.id
        << "\",\"reason\":\"" << r.reason << "\",\"oldGeneration\":" << s.current->generation
        << ",\"observedAtUnixMs\":" << r.startedAtUnixMs << "}" << std::endl;
    // A durable process's next generation starts only once the previous one has exited: an object runs in one process at
    // a time, so its requests in flight finish and its facets close before any other process opens its database. Requests
    // to its objects wait meanwhile (the node's Durable Object dispatch).
    if (s.durable) {
      r.phase = "draining";
      switchOver(now);
      return;
    }
    try {
      spawn(s, request.slot, true);
      r.next = s.incoming;
    } catch (const kj::Exception& error) {
      finish(now, "failed", std::string("cannot start the next generation: ") + error.getDescription().cStr());
    }
  }
  // New calls go to the next generation from here on; the previous one takes none.
  void switchOver(double now) {
    auto& r = *rotation;
    auto& s = slots[r.slot];
    auto old = s.current, next = s.incoming;
    r.switchedAt = now; r.switchedAtUnixMs = wall();
    s.current = next; s.incoming = nullptr;
    if (next) {
      writePidFiles(*next, r.slot);
      event(*next, "switch", r.reason);
    }
    r.phase = "draining";
    if (old && old->pid > 0) {
      old->pullEvents();
      r.inFlightAtSwitch = old->inFlight;
      r.contextsAtSwitch = old->openContexts.size();
      r.backgroundAtSwitch = old->backgroundContexts();
      old->retiring = true;
      if (old->state == "ready" || old->state == "paused") old->state = "draining";
      s.retiring.push_back(old);
      event(*old, "drain", r.reason);
      if (retiring) r.webSocketsClosed = retiring(old->id, old->generation);
    } else {
      // The previous generation exited while the next one started: nothing to drain.
      r.drainOutcome = "gone";
    }
  }
  // Drained: no call in flight and, where workerd reports it, no open request context.
  bool drained(Rotation& r, double now) {
    return generationDrained(*r.old, r.switchedAt, now, config.rotationDrainTimeoutMs, r.lastLifecycleCheck, r.clearedContexts);
  }
  // The same criterion for any generation; `since` stands for the moment it stopped taking
  // new calls. The Server's exit applies it to every live generation.
  bool generationDrained(ProcessGeneration& old, double since, double now, double periodMs,
      double& lastLifecycleCheck, uint64_t& cleared) {
    old.pullEvents();
    if (old.inFlight > 0) return false;
    // Without an event channel no `completed` row arrives: background work is invisible, so the
    // generation keeps the whole drain period. (Every generation has one.)
    if (!old.contextsVisible) return now - since >= periodMs;
    if (old.openContexts.empty()) return true;
    if (now - lastLifecycleCheck < 100) return false;
    lastLifecycleCheck = now;
    // A request context holds its isolate. An invocation whose version has no resident isolate
    // in this generation (its load failed, or it was evicted) has no context left to wait for.
    auto lifecycle = old.lifecycle.read();
    if (lifecycle.empty() || lifecycle.front() != '{' || lifecycle.back() != '}') return false;
    std::erase_if(old.openContexts, [&](const auto& entry) {
      if (entry.second.finishedAt == 0) return false;
      const auto marker = "\"isolate\":\"coworkerd-v1:" + entry.second.workerId + "@" + entry.second.versionId + "#";
      if (lifecycle.find(marker) != std::string::npos) return false;
      ++old.contextsCleared; ++cleared;
      return true;
    });
    return old.openContexts.empty();
  }
  void closeConnections(ProcessGeneration& p) {
    p.runtime.reset();
    p.probeClient = nullptr;
    p.probeAddress = nullptr;
  }
  void advance(double now) {
    auto& r = *rotation;
    if (r.phase == "starting") {
      if (!r.next || r.next->pid == 0) {
        finish(now, "failed", "the next generation exited before it was ready: " + (r.next ? r.next->lastReason : std::string()));
        return;
      }
      if (r.next->state != "ready") return;
      switchOver(now);
    }
    if (r.phase == "draining") {
      if (!r.old || r.old->pid == 0) {
        if (r.drainOutcome == "none") r.drainOutcome = "exited";
        r.drainedAt = now; r.drainedAtUnixMs = wall();
        if (slots[r.slot].durable) { r.phase = "closing"; advance(now); return; }
        finish(now, "completed");
        return;
      }
      if (drained(r, now)) {
        r.drainOutcome = r.old->contextsVisible ? "drained" : "waited";
        r.drainedAt = now; r.drainedAtUnixMs = wall();
        r.old->closing = true; r.old->plannedExit = true;
        if (r.old->state != "killed") r.old->state = "closing";
        r.phase = "closing";
      } else if (now - r.switchedAt >= config.rotationDrainTimeoutMs) {
        // What is still in flight is lost with the process and gets RESULT_UNCERTAIN.
        r.drainOutcome = "timeout";
        r.drainedAt = now; r.drainedAtUnixMs = wall();
        r.killedCalls = r.old->inFlight; r.killedContexts = r.old->backgroundContexts();
        r.old->closing = true; r.old->plannedExit = true;
        killProcess(*r.old, "rotation drain timeout", now - r.switchedAt);
        r.sigkill = true; r.terminatedAt = now; r.terminatedAtUnixMs = wall();
        r.phase = "closing";
        return;
      } else {
        return;
      }
    }
    if (r.phase == "starting-next") {
      // A durable slot's next generation, started after the previous one exited.
      if (!r.next || r.next->pid == 0) {
        finish(now, "failed", "the next generation exited before it was ready: " + (r.next ? r.next->lastReason : std::string()));
        return;
      }
      if (r.next->state != "ready") return;
      auto& s = slots[r.slot];
      s.current = r.next; s.incoming = nullptr;
      writePidFiles(*r.next, r.slot);
      event(*r.next, "switch", r.reason);
      r.phase = "switched";
      finish(now, "completed");
      return;
    }
    if (r.phase == "closing") {
      if (r.old->pid == 0) {
        if (slots[r.slot].durable) {
          try {
            spawn(slots[r.slot], r.slot, true);
            r.next = slots[r.slot].incoming;
            r.phase = "starting-next";
          } catch (const kj::Exception& error) {
            finish(now, "failed", std::string("cannot start the next generation: ") + error.getDescription().cStr());
          }
          return;
        }
        finish(now, "completed");
        return;
      }
      if (r.terminatedAt == 0) {
        // The Server holds connections to it; workerd's drain waits for them, so close them first.
        if (r.old->probePending) return;
        closeConnections(*r.old);
        ::kill(r.old->pid, SIGTERM);
        r.terminatedAt = now; r.terminatedAtUnixMs = wall();
        event(*r.old, "terminate", "rotation");
        return;
      }
      if (!r.sigkill && now - r.terminatedAt >= config.rotationTermGraceMs) {
        r.sigkill = true;
        ::kill(r.old->pid, SIGKILL);
        event(*r.old, "kill", "rotation termination grace expired");
      }
    }
  }
  std::string recordJson(const Rotation& r) const {
    const auto& s = slots[r.slot];
    std::ostringstream out;
    out << std::setprecision(17);
    auto since = [](double later, double earlier) { return later > 0 && earlier > 0 ? later - earlier : 0.0; };
    out << "{\"rotationId\":" << r.id << ",\"processId\":\"" << s.id << "\",\"processClass\":\""
        << processClass(s) << "\",\"reason\":\"" << r.reason << "\",\"requestIds\":[";
    for (size_t i = 0; i < r.requestIds.size(); ++i) out << (i ? "," : "") << r.requestIds[i];
    out << "],\"phase\":\"" << r.phase << "\",\"outcome\":\"" << r.outcome << "\",\"failure\":\"" << escape(r.failure)
        << "\",\"oldGeneration\":" << (r.old ? r.old->generation : 0) << ",\"oldPid\":" << (r.old ? r.old->spawnPid : 0)
        << ",\"oldSocket\":\"" << (r.old ? r.old->socketName : "") << "\",\"oldBinarySha256\":\"" << r.oldBinarySha256
        << "\",\"newGeneration\":" << (r.next ? r.next->generation : 0) << ",\"newPid\":" << (r.next ? r.next->spawnPid : 0)
        << ",\"newSocket\":\"" << (r.next ? r.next->socketName : "") << "\",\"newBinarySha256\":\"" << (r.next ? hasher.lookup(r.next->binary) : "")
        << "\",\"requestedAtUnixMs\":" << r.requestedAtUnixMs << ",\"startedAtUnixMs\":" << r.startedAtUnixMs
        << ",\"readyAtUnixMs\":" << (r.next ? r.next->readyAtUnixMs : 0) << ",\"switchedAtUnixMs\":" << r.switchedAtUnixMs
        << ",\"drainedAtUnixMs\":" << r.drainedAtUnixMs << ",\"terminatedAtUnixMs\":" << r.terminatedAtUnixMs
        << ",\"oldExitedAtUnixMs\":" << (r.old ? r.old->exitedAtUnixMs : 0) << ",\"finishedAtUnixMs\":" << r.finishedAtUnixMs
        << ",\"queuedMs\":" << since(r.startedAt, r.requestedAt)
        << ",\"startupMs\":" << (r.next ? since(r.next->readyAt, r.startedAt) : 0.0)
        << ",\"drainMs\":" << since(r.drainedAt, r.switchedAt)
        << ",\"closeMs\":" << (r.old ? since(r.old->exitedAt, r.drainedAt) : 0.0)
        << ",\"terminateToExitMs\":" << (r.old ? since(r.old->exitedAt, r.terminatedAt) : 0.0)
        << ",\"durationMs\":" << (r.finishedAt > 0 ? r.finishedAt : mono()) - r.startedAt
        << ",\"inFlightAtSwitch\":" << r.inFlightAtSwitch << ",\"contextsAtSwitch\":" << r.contextsAtSwitch
        << ",\"backgroundAtSwitch\":" << r.backgroundAtSwitch << ",\"backgroundVisible\":" << (r.backgroundVisible ? "true" : "false")
        << ",\"drainOutcome\":\"" << r.drainOutcome << "\",\"killedCalls\":" << r.killedCalls
        << ",\"killedContexts\":" << r.killedContexts << ",\"clearedContexts\":" << r.clearedContexts
        << ",\"sigkill\":" << (r.sigkill ? "true" : "false") << ",\"webSocketsClosed\":" << r.webSocketsClosed
        << ",\"oldExit\":\"" << (r.old ? escape(r.old->exitStatus) : "") << "\",\"oldExitReason\":\"" << (r.old ? escape(r.old->lastReason) : "")
        << "\",\"oldDispatched\":" << (r.old ? r.old->dispatched : 0)
        << ",\"newDispatched\":" << (r.next ? r.next->dispatched : 0)
        << ",\"newColdLoadsAtCompletion\":" << coldLoads(r.next) << ",\"oldLifecycleAtExit\":" << r.oldLifecycle << "}";
    return out.str();
  }
  // Isolates the generation has created so far (its lifecycle snapshot), i.e. its cold loads.
  int64_t coldLoads(const std::shared_ptr<ProcessGeneration>& p) const {
    if (!p) return -1;
    auto text = p->lifecycle.read();
    auto at = text.find("\"createdIsolates\":");
    if (at == std::string::npos) return -1;
    return std::strtoll(text.c_str() + at + 18, nullptr, 10);
  }
  void finish(double now, const std::string& outcome, const std::string& failure = "") {
    auto& r = *rotation;
    auto& s = slots[r.slot];
    r.outcome = outcome; r.failure = failure;
    r.finishedAt = now; r.finishedAtUnixMs = wall();
    if (r.phase == "starting" || r.phase == "starting-next") {
      // The next generation never took over (it exited, or never started).
      if (r.next && r.next->pid > 0) {
        r.next->plannedExit = true;
        killProcess(*r.next, "rotation failed", now - r.startedAt);
        s.retiring.push_back(r.next);
      }
      s.incoming = nullptr;
    } else if (r.old) {
      // An exited generation is released with its last reference; its port is free again.
      std::erase(s.retiring, r.old);
    }
    r.phase = "done";
    auto record = recordJson(r);
    std::cout << "[process-rotation] {\"event\":\"" << outcome << "\",\"record\":" << record << "}" << std::endl;
    rotationHistory.emplace_back(r.id, record);
    while (rotationHistory.size() > ROTATION_HISTORY) rotationHistory.pop_front();
    ++rotationCounts[outcome];
    if (r.drainOutcome == "timeout") ++rotationCounts["drainTimeouts"];
    rotationCounts["killedCalls"] += r.killedCalls;
    rotationCounts["killedContexts"] += r.killedContexts;
    for (auto id : r.requestIds) track(id, outcome, r.id);
    if (outcome == "completed") {
      ++s.rotations;
      if (r.reason != "manual") { automaticBackoffMs = 0; automaticRetryAt = 0; }
    } else if (r.reason != "manual") {
      automaticBackoffMs = automaticBackoffMs ? std::min<uint64_t>(automaticBackoffMs * 2, ROTATION_BACKOFF_MAX_MS) : config.rotationRetryMs;
      automaticRetryAt = now + double(automaticBackoffMs);
    }
    rotation.reset();
  }
  std::string generationJson(const std::shared_ptr<ProcessGeneration>& p) const {
    if (!p) return "null";
    std::ostringstream out;
    out << std::setprecision(17) << "{\"generation\":" << p->generation << ",\"pid\":" << p->pid << ",\"socket\":\"" << p->socketName << "\""
        << ",\"state\":\"" << p->state << "\",\"inFlight\":" << p->inFlight << ",\"dispatchedCount\":" << p->dispatched
        << ",\"openContexts\":" << p->openContexts.size() << ",\"backgroundContexts\":" << p->backgroundContexts()
        << ",\"binarySha256\":\"" << hasher.lookup(p->binary) << "\",\"residentBytes\":" << MemoryWatermark::residentBytes(p->pid)
        << ",\"spawnedAtUnixMs\":" << p->spawnedAtUnixMs << ",\"readyAtUnixMs\":" << p->readyAtUnixMs
        << ",\"probeFailures\":" << p->probeFailures << ",\"lastReason\":\"" << escape(p->lastReason)
        << "\",\"sandbox\":" << generationSandboxJson(*p) << "}";
    return out.str();
  }
  // A slot's collections: the long ones of every generation (their gc-pause rows) and the current generation's own
  // counts from its lifecycle snapshot -- full, minor and idle collections, the longest pause of any kind.
  std::string gcJson(const Slot& s, const std::shared_ptr<ProcessGeneration>& p) const {
    std::string generation = "null";
    if (p) {
      auto row = p->lifecycle.read();
      if (!row.empty() && row.front() == '{') try {
        capnp::MallocMessageBuilder message;
        auto value = message.initRoot<capnp::JsonValue>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(row.data(), row.size()), value);
        auto gc = json::field(value, "gc");
        if (gc.isObject()) generation = capnp::JsonCodec().encodeRaw(gc).cStr();
      } catch (const kj::Exception&) {}
    }
    std::ostringstream out;
    out << std::setprecision(17) << "{\"longPauseMs\":" << config.gcLongPauseMs << ",\"longPauses\":" << s.gcLongPauses
        << ",\"longestPauseMs\":" << double(s.gcLongestPauseNs) / 1e6
        << ",\"longestPause\":" << (s.gcLongestPause.empty() ? std::string("null") : s.gcLongestPause)
        << ",\"generation\":" << generation << "}";
    return out.str();
  }
  // The state endpoint carries the latest records; /__coworkerd/rotation carries the whole history.
  std::string rotationStateJson(size_t recentLimit = ROTATION_HISTORY) const {
    std::ostringstream out;
    out << std::setprecision(17) << "{\"configuration\":{\"drainTimeoutMs\":" << config.rotationDrainTimeoutMs
        << ",\"termGraceMs\":" << config.rotationTermGraceMs << ",\"binaryCheckMs\":" << config.rotationBinaryCheckMs
        << ",\"maxAgeMs\":" << config.rotationMaxAgeMs << ",\"maxRequests\":" << config.rotationMaxRequests
        << ",\"residentBytes\":" << config.rotationResidentBytes << ",\"residentBytesSource\":\""
        << escape(config.rotationResidentBytesSource) << "\",\"retryMs\":" << config.rotationRetryMs
        << ",\"concurrency\":1},\"binary\":{\"path\":\"" << escape(executable) << "\",\"sha256\":\"" << hasher.lookup(configuredBinary)
        << "\",\"checks\":" << binaryChecks << ",\"changes\":" << binaryChanges << ",\"changedAtUnixMs\":" << binaryChangedAtUnixMs
        << ",\"hashes\":" << hasher.counts().first << ",\"hashFailures\":" << hasher.counts().second
        << "},\"automaticBackoffMs\":" << automaticBackoffMs << ",\"inProgress\":";
    if (rotation) out << recordJson(*rotation); else out << "null";
    out << ",\"queue\":[";
    for (size_t i = 0; i < rotationQueue.size(); ++i) {
      const auto& q = rotationQueue[i];
      out << (i ? "," : "") << "{\"processId\":\"" << slots[q.slot].id << "\",\"reason\":\"" << q.reason
          << "\",\"requestedAtUnixMs\":" << q.requestedAtUnixMs << ",\"requestIds\":[";
      for (size_t j = 0; j < q.ids.size(); ++j) out << (j ? "," : "") << q.ids[j];
      out << "]}";
    }
    out << "],\"counts\":{";
    size_t i = 0;
    for (const auto& [name, value] : rotationCounts) out << (i++ ? "," : "") << '"' << name << "\":" << value;
    out << "},\"requestedByReason\":{";
    i = 0;
    for (const auto& [name, value] : rotationReasons) out << (i++ ? "," : "") << '"' << name << "\":" << value;
    out << "},\"recent\":[";
    i = 0;
    const size_t skip = rotationHistory.size() > recentLimit ? rotationHistory.size() - recentLimit : 0;
    for (const auto& [id, record] : rotationHistory) if (i++ >= skip) out << (i > skip + 1 ? "," : "") << record;
    out << "]}";
    return out.str();
  }
};

ProcessPool::ProcessPool(ProcessPoolConfig config, std::string executable, std::string runtimeFile,
    std::string directory, std::string bindingAddress, std::string sourceAddress,
    kj::AsyncIoProvider& provider, kj::UnixEventPort& eventPort, const kj::HttpHeaderTable& headers,
    Suspects& suspects)
    : impl(std::make_unique<Impl>(config, std::move(executable), std::move(runtimeFile), std::move(directory),
        std::move(bindingAddress), std::move(sourceAddress), provider, eventPort, headers, suspects)) {}
ProcessPool::~ProcessPool() noexcept(false) = default;
kj::Promise<void> ProcessPool::run() {
  while (true) {
    impl->refresh();
    impl->syncDueEvents();
    impl->rotationTick();
    for (size_t i = 0; i < impl->slots.size(); ++i) {
      auto& s = impl->slots[i];
      // A rotation's next generation takes over the slot instead of a restart; a closing
      // pool (the Server's exit) starts nothing; with the allowlist on, a workerd file this Server has
      // not hashed yet waits for its digest rather than being launched against a list.
      // A durable slot being rotated starts its next generation itself, once the previous one has exited.
      const bool rotating = impl->rotation && impl->rotation->slot == i && s.durable;
      if ((!s.current || s.current->pid == 0) && !s.incoming && !rotating && mono() >= s.restartAt && !impl->closingAll
          && !(s.durable && impl->durableClosed) && impl->launchReady(s)) impl->spawn(s, i);
      impl->maybeProbe(s.current);
      for (const auto& p : s.retiring) impl->maybeProbe(p);
    }
    co_await impl->provider.getTimer().afterDelay(10 * kj::MILLISECONDS);
  }
}
// A Worker's tenant is the suspect: every Worker of a suspected tenant runs in the quarantine processes.
bool ProcessPool::quarantined(const std::string& worker) const {
  return impl->config.quarantineProcesses > 0 && impl->suspects.contains(tenantOf(worker));
}
bool ProcessPool::available(const std::string& worker) {
  impl->refresh();
  const auto quarantine = quarantined(worker);
  for (const auto& s : impl->slots) if (!s.durable && s.quarantine == quarantine && s.current && impl->admitted(*s.current)) return true;
  return false;
}
uint32_t ProcessPool::durableProcesses() const { return impl->config.durableProcesses; }
bool ProcessPool::durableHot(const std::string& hash) const {
  for (const auto& s : impl->slots) if (s.durable && s.current && s.current->pid > 0) {
    auto found = s.current->heat.find(hash);
    if (found != s.current->heat.end() && mono() - found->second <= impl->config.heatIdleMs) return true;
  }
  return false;
}
std::string ProcessPool::durableProcessFor(const std::string& objectKey) const { return impl->durableSlotFor(objectKey); }
bool ProcessPool::durableAvailable(const std::string& objectKey) {
  impl->refresh();
  const auto id = impl->durableSlotFor(objectKey);
  for (const auto& s : impl->slots) if (s.durable && s.id == id && s.current && impl->admitted(*s.current)) return true;
  return false;
}
std::shared_ptr<ProcessCall> ProcessPool::selectDurable(const std::string& objectKey) {
  impl->refresh();
  const auto id = impl->durableSlotFor(objectKey);
  for (const auto& s : impl->slots) {
    if (s.durable && s.id == id && s.current && impl->admitted(*s.current)) {
      ++impl->durableDispatches;
      return std::shared_ptr<ProcessCall>(new ProcessCall(s.current));
    }
  }
  return {};
}
uint32_t ProcessPool::durableInFlight() {
  impl->refresh();
  uint32_t calls = 0;
  for (const auto& s : impl->slots) if (s.durable) Impl::eachGeneration(s, [&](const auto& p) { if (p->pid > 0) calls += p->inFlight; });
  return calls;
}
uint32_t ProcessPool::resetDurable(const std::string& reason) {
  impl->refresh();
  if (impl->closingAll || impl->durableClosed) return 0;
  if (impl->rotation && impl->slots[impl->rotation->slot].durable) impl->abortRotation(mono(), reason);
  uint32_t killed = 0;
  for (auto& s : impl->slots) {
    if (!s.durable) continue;
    Impl::eachGeneration(s, [&](const auto& p) {
      if (p->pid == 0 || p->state == "killed") return;
      // Planned: the slot is not charged a failure, and its next generation starts as soon as this one is reaped.
      p->closing = true; p->plannedExit = true;
      impl->killProcess(*p, reason, 0);
      ++killed;
    });
    s.restartAt = 0;
  }
  return killed;
}
void ProcessPool::setDurableChannel(std::function<int(const std::string&)> open,
    std::function<void(const std::string&)> close, const DurableLogConfig& settings) {
  impl->durableChannelOpen = std::move(open);
  impl->durableChannelClose = std::move(close);
  impl->durableLog = settings;
}
void ProcessPool::setRetiring(std::function<uint32_t(const std::string&, uint64_t)> callback) {
  impl->retiring = std::move(callback);
}
void ProcessPool::enableEgress(std::string address) {
  impl->egressAddress = std::move(address);
}

std::optional<std::pair<std::string, uint64_t>> ProcessPool::egressSource(const std::string& token) const {
  if (token.size() != 32) return std::nullopt;
  for (const auto& slot : impl->slots) {
    for (const auto* generation : {&slot.current, &slot.incoming}) {
      if (*generation && (*generation)->egressToken == token) return std::pair{(*generation)->id, (*generation)->generation};
    }
    for (const auto& generation : slot.retiring)
      if (generation && generation->egressToken == token) return std::pair{generation->id, generation->generation};
  }
  return std::nullopt;
}

bool ProcessPool::ready() const {
  return std::all_of(impl->slots.begin(), impl->slots.end(), [](const auto& s) {
    return s.quarantine || s.durable || (s.current && s.current->state == "ready" && s.current->runtime && !s.current->runtime->disconnected);
  });
}
std::string ProcessPool::quarantineReadinessJson() const {
  std::string out = "[";
  for (const auto& s : impl->slots) {
    if (!s.quarantine) continue;
    const bool ready = s.current && s.current->state == "ready" && s.current->runtime && !s.current->runtime->disconnected;
    if (ready) continue;
    out += std::string(out.size() > 1 ? "," : "") + "{\"processId\":\"" + escape(s.id) + "\",\"state\":\"" +
        escape(s.current ? s.current->state : std::string("none")) + "\"}";
  }
  return out + "]";
}
uint32_t ProcessPool::cpuLimitMs() const { return impl->config.cpuLimitMs; }
uint32_t ProcessPool::memoryLimitMiB() const { return impl->config.memoryLimitMiB; }
void ProcessPool::setWorkerLimits(WorkerLimitsView view) { impl->workerLimits = std::move(view); }
WorkerLimits ProcessPool::effectiveLimits(const std::string& workerId) const { return impl->limitsOf(workerId); }
bool ProcessPool::hot(const std::string& worker, const std::string& hash, uint32_t idleMs) const {
  const auto quarantine = quarantined(worker);
  for (const auto& s : impl->slots) if (!s.durable && s.quarantine == quarantine && s.current && s.current->pid > 0) {
    auto found = s.current->heat.find(hash);
    if (found != s.current->heat.end() && mono() - found->second <= idleMs) return true;
  }
  return false;
}
// The rendezvous order of a Worker's slots: ordinary ones by the Worker, quarantine ones by its tenant (the
// quarantine processes are sharded by tenant, so one suspect tenant's crash takes down only its shard).
static std::string placementKey(const std::string& worker, bool quarantine) {
  const auto tenant = tenantOf(worker);
  return quarantine && !tenant.empty() ? tenant : worker;
}
std::shared_ptr<ProcessCall> ProcessPool::select(const std::string& worker) {
  impl->refresh();
  const auto quarantine = quarantined(worker);
  std::vector<std::string> ids;
  for (const auto& s : impl->slots) if (!s.durable && s.quarantine == quarantine && s.current && s.current->pid > 0) ids.push_back(s.id);
  for (const auto& id : rendezvousOrder(placementKey(worker, quarantine), ids)) for (const auto& s : impl->slots) {
    if (s.id == id && s.current && impl->admitted(*s.current)) {
      if (quarantine) ++impl->quarantineDispatches;
      return std::shared_ptr<ProcessCall>(new ProcessCall(s.current));
    }
  }
  return {};
}
// A trial load's process: the tenant's quarantine shard, never an ordinary process -- a version
// that crashes its process while loading takes no ordinary request with it. Without quarantine processes (configured
// to 0) the ordinary selection.
std::shared_ptr<ProcessCall> ProcessPool::selectTrial(const std::string& worker) {
  if (impl->config.quarantineProcesses == 0) return select(worker);
  impl->refresh();
  std::vector<std::string> ids;
  for (const auto& s : impl->slots) if (s.quarantine && s.current && s.current->pid > 0) ids.push_back(s.id);
  for (const auto& id : rendezvousOrder(placementKey(worker, true), ids)) for (const auto& s : impl->slots) {
    if (s.id == id && s.current && impl->admitted(*s.current)) {
      ++impl->trialDispatches;
      return std::shared_ptr<ProcessCall>(new ProcessCall(s.current));
    }
  }
  return {};
}
std::string ProcessPool::quarantineShard(const std::string& tenant) const {
  std::vector<std::string> ids;
  for (const auto& s : impl->slots) if (s.quarantine) ids.push_back(s.id);
  auto order = rendezvousOrder(tenant, ids);
  return order.empty() ? std::string() : order.front();
}
std::map<std::string, uint64_t> ProcessPool::takeCpuUsage() {
  impl->syncEvents();
  std::map<std::string, uint64_t> taken;
  taken.swap(*impl->cpuUsage);
  return taken;
}
std::map<std::string, uint64_t> ProcessPool::residentIsolatesByTenant() const {
  // Each generation's lifecycle snapshot: its resident isolates, named "<namespace>:<loader key>"; a dynamic
  // Worker's loader key starts with its full name (runtime/runtime.js: <workerId>@<versionId>#...).
  std::map<std::string, uint64_t> counts;
  for (const auto& s : impl->slots) Impl::eachGeneration(s, [&](const auto& p) {
    if (p->pid <= 0) return;
    auto row = p->lifecycle.read();
    if (row.empty() || row.front() != '{') return;
    try {
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(row.data(), row.size()), json);
      auto list = json::field(json, "residentWorkers");
      if (!list.isArray()) return;
      for (auto entry : list.getArray()) {
        auto name = json::field(entry, "isolate");
        if (!name.isString()) continue;
        std::string isolate = name.getString().cStr();
        const auto at = isolate.find('@');
        if (at == std::string::npos) continue;
        const auto colon = isolate.rfind(':', at);
        const auto tenant = tenantOf(isolate.substr(colon == std::string::npos ? 0 : colon + 1, at - (colon == std::string::npos ? 0 : colon + 1)));
        if (!tenant.empty()) ++counts[tenant];
      }
    } catch (const kj::Exception&) {}
  });
  return counts;
}
OpenEvent ProcessPool::open(std::shared_ptr<ProcessCall> call, kj::StringPtr descriptor, const std::string& hash) {
  auto p = call->process;
  call->startedAtMonotonicMs = mono(); call->startedAtUnixMs = wall();
  p->calls.emplace(call->invocationId, call.get()); p->heat[hash] = mono();
  ++p->dispatched;
  // An object's requests share its context in workerd, whose completed row comes when the object leaves memory: a durable
  // process is drained once its requests in flight are done (a WebSocket connection is told to go away).
  if (p->contextsVisible && !p->durable) p->openContexts[call->invocationId] = {call->workerId, call->versionId};
  // From this send onward, a lost connection or a dead child is never retried.
  call->dispatched = true;
  auto result = p->runtime->open(descriptor);
  result.ready = kj::mv(result.ready).exclusiveJoin(call->onExit()).attach(std::shared_ptr<ProcessCall>(call));
  return result;
}
std::string ProcessPool::json(uint32_t idleMs) const {
  impl->refresh();
  impl->syncEvents();
  std::ostringstream out; out << std::setprecision(17) << "{\"configuration\":{\"count\":" << impl->config.count
      << ",\"cpuLimitMs\":" << impl->config.cpuLimitMs
      << ",\"memoryLimitMiB\":" << impl->config.memoryLimitMiB
      << ",\"wasmMemoryCapMiB\":" << impl->wasmMemoryCapMiB()
      << ",\"idleEvictMs\":" << impl->config.idleEvictMs
      << ",\"idleDestroyIntervalMs\":" << impl->config.idleDestroyIntervalMs
      << ",\"quarantineProcesses\":" << impl->config.quarantineProcesses
      << ",\"durableProcesses\":" << impl->config.durableProcesses
      << ",\"inFlightLimit\":" << impl->config.inFlightLimit << ",\"probeIntervalMs\":" << impl->config.probeIntervalMs
      << ",\"probeOverdueMs\":" << impl->config.probeOverdueMs << ",\"stuckTimeoutMs\":" << impl->config.stuckTimeoutMs
      << ",\"watchdog\":" << (impl->config.watchdog ? "true" : "false") << ",\"restartBackoffMs\":" << impl->config.restartBackoffMs
      << ",\"restartMaxMs\":" << impl->config.restartMaxMs
      << ",\"gc\":{\"budgetMs\":" << impl->config.gcBudgetMs << ",\"refillMsPerSecond\":" << impl->config.gcRefillMsPerSecond
      << ",\"longPauseMs\":" << impl->config.gcLongPauseMs << ",\"idleCollectAfterMs\":" << impl->config.gcIdleCollectAfterMs
      << ",\"idleCollectMinGrowthMiB\":" << impl->config.gcIdleCollectMinGrowthMiB
      << ",\"initialOldSpaceMiB\":" << impl->config.gcInitialOldSpaceMiB << ",\"runtimeHeapMiB\":" << impl->config.gcRuntimeHeapMiB
      << "}},\"entries\":[";
  size_t i = 0;
  for (const auto& s : impl->slots) {
    const auto slotIndex = i;
    if (i++) out << ',';
    auto p = s.current;
    out << "{\"processId\":\"" << s.id << "\",\"processClass\":\"" << processClass(s)
        << "\",\"generation\":" << (p ? p->generation : s.generation) << ",\"pid\":" << (p ? p->pid : 0)
        << ",\"residentBytes\":" << MemoryWatermark::residentBytes(p ? p->pid : 0)
        << ",\"state\":\"" << (p ? p->state : "starting") << "\",\"restartCount\":" << s.restarts << ",\"exitCount\":" << s.exits
        << ",\"cpuTerminatedRequests\":" << s.cpuTerminatedRequests << ",\"cpuInvalidatedIsolates\":" << s.cpuInvalidatedIsolates
        << ",\"cpuReloadedIsolates\":" << s.cpuReloadedIsolates << ",\"gc\":" << impl->gcJson(s, p)
        << ",\"probeAttempts\":" << (p ? p->probeAttempts : 0)
        << ",\"probeFailures\":" << (p ? p->probeFailures : 0)
        << ",\"lastExitedGeneration\":" << s.lastExitedGeneration << ",\"lastExitAtUnixMs\":" << s.lastExitAtUnixMs
        << ",\"lastReason\":\"" << escape(p && !p->lastReason.empty() ? p->lastReason : s.lastReason) << "\",\"inFlight\":" << (p ? p->inFlight : 0)
        << ",\"lastExitCategory\":\"" << s.lastExitCategory << "\",\"memoryTerminatedRequests\":" << s.memoryTerminatedRequests
        << ",\"memoryInvalidatedIsolates\":" << s.memoryInvalidatedIsolates << ",\"memoryReloadedIsolates\":" << s.memoryReloadedIsolates
        << ",\"spawnedAtUnixMs\":" << (p ? p->spawnedAtUnixMs : 0) << ",\"readyElapsedMs\":" << (p && p->readyAt ? p->readyAt - p->spawnedAt : 0)
        << ",\"heatTableSize\":" << (p ? p->heat.size() : 0)
        << ",\"socket\":\"" << (p ? p->socketName : "") << "\",\"binarySha256\":\"" << (p ? impl->hasher.lookup(p->binary) : "")
        << "\",\"dispatchedCount\":" << (p ? p->dispatched : 0) << ",\"openContexts\":" << (p ? p->openContexts.size() : 0)
        << ",\"contextsVisible\":" << (p && p->contextsVisible ? "true" : "false")
        << ",\"contextsCompleted\":" << (p ? p->contextsCompleted : 0) << ",\"contextsDropped\":" << (p ? p->contextsDropped : 0)
        << ",\"rotationCount\":" << s.rotations << ",\"sandbox\":" << (p ? impl->generationSandboxJson(*p) : "null")
        << ",\"launchFailures\":" << s.launchFailures << ",\"lastLaunchFailure\":" << s.lastLaunchFailure
        << ",\"incoming\":" << impl->generationJson(s.incoming) << ",\"retiring\":[";
    for (size_t j = 0; j < s.retiring.size(); ++j) out << (j ? "," : "") << impl->generationJson(s.retiring[j]);
    out << "],\"output\":" << WorkerdOutput::processJson(slotIndex)
        << ",\"limitEvents\":";
    if (p && p->eventChannel) out << p->channelJson();
    else out << "null";
    out
        << ",\"lifecycle\":";
    std::string lifecycle = "null";
    if (p) {
      auto row = p->lifecycle.read();
      if (!row.empty() && row.front() == '{' && row.back() == '}') lifecycle = row;
    }
    out << lifecycle << ",\"hotVersions\":[";
    size_t n = 0;
    if (p && p->pid > 0) for (const auto& [hash, when] : p->heat) if (mono() - when <= idleMs) {
      if (n++) out << ',';
      out << "{\"definitionHash\":\"" << hash << "\",\"lastExecutionMonotonicMs\":" << when << "}";
    }
    out << "],\"probeDelayMs\":[";
    if (p) for (size_t j = 0; j < p->probeDelays.size(); ++j) { if (j) out << ','; out << p->probeDelays[j]; }
    out << "]}";
  }
  out << "],\"recentKills\":[";
  for (size_t j = 0; j < impl->kills.size(); ++j) { if (j) out << ','; out << impl->kills[j]; }
  out << "],\"recentGcPauses\":[";
  for (size_t j = 0; j < impl->recentGcPauses.size(); ++j) { if (j) out << ','; out << impl->recentGcPauses[j]; }
  out << "],\"workerCpuStats\":[";
  size_t workerIndex = 0;
  for (const auto& [worker, stats] : impl->workerCpuStats) {
    if (workerIndex++) out << ',';
    out << "{\"workerId\":\"" << escape(worker) << "\",\"terminatedRequests\":" << stats.terminated
        << ",\"culpritCount\":" << stats.culprit << "}";
  }
  out << "],\"workerMemoryStats\":[";
  workerIndex = 0;
  for (const auto& [worker, stats] : impl->workerMemoryStats) {
    if (workerIndex++) out << ',';
    out << "{\"workerId\":\"" << escape(worker) << "\",\"terminatedRequests\":" << stats.terminated
        << ",\"culpritCount\":" << stats.culprit << ",\"peakUsedBytes\":" << stats.peakUsedBytes << "}";
  }
  // The cgroup's OOM kills as this pool attributed them: its own processes', and the others' (not this pool's exits).
  out << "],\"kernelOomKills\":{\"memoryEvents\":\"" << escape(impl->memoryEvents) << "\",\"counted\":" << impl->lastOomKills
      << ",\"pool\":" << impl->poolOomKills << ",\"other\":" << impl->foreignOomKills
      << ",\"unattributed\":" << impl->unattributedOomKills.size() << "}";
  out << ",\"recentMemoryCrashes\":[";
  for (size_t j = 0; j < impl->memoryCrashes.size(); ++j) { if (j) out << ','; out << impl->memoryCrashes[j]; }
  out << "],\"recentChannelExits\":[";
  for (size_t j = 0; j < impl->channelExits.size(); ++j) { if (j) out << ','; out << impl->channelExits[j]; }
  out << "],\"sandbox\":" << impl->sandboxStateJson() << ",\"recentLaunchFailures\":[";
  for (size_t j = 0; j < impl->recentLaunchFailures.size(); ++j) { if (j) out << ','; out << impl->recentLaunchFailures[j]; }
  out << "],\"quarantineDispatches\":" << impl->quarantineDispatches << ",\"trialDispatches\":" << impl->trialDispatches
      << ",\"durableDispatches\":" << impl->durableDispatches
      << ",\"suspects\":" << impl->suspects.json()
      << ",\"output\":" << WorkerdOutput::stateJson() << ",\"rotation\":" << impl->rotationStateJson(16)
      << ",\"shuttingDown\":" << (impl->shuttingDown ? "true" : "false") << ",\"closing\":" << (impl->closingAll ? "true" : "false")
      << "}"; return out.str();
}
std::string ProcessPool::placementJson(const std::vector<std::string>& workers) {
  impl->refresh();
  std::ostringstream out; out << '[';
  for (size_t i = 0; i < workers.size(); ++i) {
    if (i) out << ',';
    std::vector<std::string> ids;
    for (const auto& s : impl->slots) if (!s.durable && s.quarantine == quarantined(workers[i]) && s.current && s.current->pid > 0) ids.push_back(s.id);
    auto order = rendezvousOrder(placementKey(workers[i], quarantined(workers[i])), ids);
    out << "{\"workerId\":\"" << escape(workers[i]) << "\",\"home\":\"" << (order.empty() ? "" : order.front()) << "\",\"candidates\":[";
    for (size_t j = 0; j < order.size(); ++j) { if (j) out << ','; out << '"' << order[j] << '"'; }
    out << "]}";
  }
  out << ']'; return out.str();
}
std::string ProcessPool::requestRotation(const std::vector<std::string>& processIds, const std::string& reason) {
  KJ_REQUIRE(!impl->shuttingDown, "the Server is draining for its exit; no rotation starts");
  std::vector<size_t> targets;
  if (processIds.empty()) for (size_t i = 0; i < impl->slots.size(); ++i) targets.push_back(i);
  for (const auto& id : processIds) {
    auto found = std::find_if(impl->slots.begin(), impl->slots.end(), [&](const auto& s) { return s.id == id; });
    KJ_REQUIRE(found != impl->slots.end(), "unknown process slot", id.c_str());
    if (std::find(targets.begin(), targets.end(), size_t(found - impl->slots.begin())) == targets.end())
      targets.push_back(found - impl->slots.begin());
  }
  std::ostringstream out;
  out << "{\"requests\":[";
  for (size_t i = 0; i < targets.size(); ++i) {
    auto id = impl->enqueue(targets[i], reason);
    out << (i ? "," : "") << "{\"requestId\":" << id << ",\"processId\":\"" << impl->slots[targets[i]].id << "\"}";
  }
  out << "],\"rotation\":" << impl->rotationStateJson(16) << "}";
  return out.str();
}
std::string ProcessPool::rotationJson(const std::vector<uint64_t>& requestIds) const {
  if (requestIds.empty()) return impl->rotationStateJson();
  std::set<uint64_t> rotations;
  size_t pending = 0, failed = 0, unknown = 0;
  std::ostringstream out;
  out << "{\"requests\":[";
  for (size_t i = 0; i < requestIds.size(); ++i) {
    auto found = impl->rotationRequests.find(requestIds[i]);
    std::string status = found == impl->rotationRequests.end() ? "unknown" : found->second.status;
    if (status == "queued" || status == "active") ++pending;
    else if (status == "failed") ++failed;
    else if (status == "unknown") ++unknown;
    if (found != impl->rotationRequests.end() && found->second.rotationId) rotations.insert(found->second.rotationId);
    out << (i ? "," : "") << "{\"requestId\":" << requestIds[i] << ",\"status\":\"" << status << "\",\"rotationId\":"
        << (found == impl->rotationRequests.end() ? 0 : found->second.rotationId) << "}";
  }
  out << "],\"pending\":" << pending << ",\"failed\":" << failed << ",\"unknown\":" << unknown << ",\"records\":[";
  size_t n = 0;
  for (const auto& [id, record] : impl->rotationHistory) if (rotations.count(id)) out << (n++ ? "," : "") << record;
  if (impl->rotation && rotations.count(impl->rotation->id)) out << (n++ ? "," : "") << impl->recordJson(*impl->rotation);
  out << "]}";
  return out.str();
}

void ProcessPool::beginShutdown() { impl->shuttingDown = true; }

ProcessPool::DrainState ProcessPool::drainState(double since, uint32_t periodMs) {
  impl->refresh();
  DrainState result;
  const auto now = mono();
  bool idle = true;
  std::ostringstream generations;
  size_t n = 0;
  for (auto& s : impl->slots) Impl::eachGeneration(s, [&](const auto& p) {
    if (p->pid == 0) return;
    ++result.liveGenerations;
    const bool drained = impl->generationDrained(*p, since, now, periodMs, p->drainLifecycleCheck, impl->shutdownClearedContexts);
    idle = idle && drained;
    result.inFlight += p->inFlight;
    result.openContexts += p->openContexts.size();
    result.backgroundContexts += p->backgroundContexts();
    result.contextsVisible = result.contextsVisible && p->contextsVisible;
    generations << (n++ ? "," : "") << "{\"processId\":\"" << p->id << "\",\"generation\":" << p->generation
        << ",\"pid\":" << p->pid << ",\"state\":\"" << p->state << "\",\"inFlight\":" << p->inFlight
        << ",\"openContexts\":" << p->openContexts.size() << ",\"backgroundContexts\":" << p->backgroundContexts()
        << ",\"contextsVisible\":" << (p->contextsVisible ? "true" : "false") << ",\"drained\":" << (drained ? "true" : "false") << "}";
  });
  result.idle = idle;
  result.clearedContexts = impl->shutdownClearedContexts;
  std::ostringstream out;
  out << "{\"idle\":" << (idle ? "true" : "false") << ",\"inFlight\":" << result.inFlight
      << ",\"openContexts\":" << result.openContexts << ",\"backgroundContexts\":" << result.backgroundContexts
      << ",\"contextsVisible\":" << (result.contextsVisible ? "true" : "false") << ",\"liveGenerations\":" << result.liveGenerations
      << ",\"clearedContexts\":" << result.clearedContexts << ",\"generations\":[" << generations.str() << "]}";
  result.json = out.str();
  return result;
}

ProcessPool::Killed ProcessPool::killBusy(double since, uint32_t periodMs, const std::string& reason) {
  impl->refresh();
  // What is killed here is not restarted: the pool is closed next.
  impl->shuttingDown = impl->closingAll = true;
  if (impl->rotation) impl->abortRotation(mono());
  Killed killed;
  const auto now = mono();
  for (auto& s : impl->slots) Impl::eachGeneration(s, [&](const auto& p) {
    if (p->pid == 0 || p->state == "killed") return;
    if (impl->generationDrained(*p, since, now, periodMs, p->drainLifecycleCheck, impl->shutdownClearedContexts)) return;
    // The calls still in flight are lost with the process (RESULT_UNCERTAIN).
    killed.calls += p->inFlight; killed.contexts += p->backgroundContexts(); ++killed.processes;
    p->closing = true; p->plannedExit = true;
    impl->killProcess(*p, reason, now - since);
  });
  return killed;
}

kj::Promise<std::string> ProcessPool::closeAll() {
  impl->refresh();
  impl->shuttingDown = impl->closingAll = true;
  if (impl->rotation) impl->abortRotation(mono());
  return impl->closeGenerations([](const Impl::Slot&) { return true; });
}

kj::Promise<std::string> ProcessPool::closeDurable() {
  impl->refresh();
  impl->durableClosed = true;
  if (impl->rotation && impl->slots[impl->rotation->slot].durable) impl->abortRotation(mono());
  return impl->closeGenerations([](const Impl::Slot& s) { return s.durable; });
}

kj::Promise<std::string> ProcessPool::Impl::closeGenerations(std::function<bool(const Slot&)> which) {
  auto* impl = this;
  const auto started = mono();
  const auto startedAtUnixMs = wall();
  struct Closing {
    std::shared_ptr<ProcessGeneration> p;
    double terminatedAt = 0;
    uint64_t terminatedAtUnixMs = 0;
    bool sigkill = false, signalled = false;
  };
  std::vector<Closing> closing;
  for (auto& s : impl->slots) {
    if (!which(s)) continue;
    Impl::eachGeneration(s, [&](const auto& p) {
      if (p->pid == 0) return;
      p->closing = true; p->plannedExit = true;
      if (p->state != "killed") p->state = "closing";
      closing.push_back({p});
    });
  }
  // The rotation's close of each generation, all at once: the Server's connections first (workerd's
  // drain waits for them, and an in-flight probe finishes first), then SIGTERM, then SIGKILL
  // after the termination grace.
  while (true) {
    impl->refresh();
    const auto now = mono();
    bool exited = true;
    for (auto& c : closing) {
      if (c.p->pid == 0) continue;
      exited = false;
      if (c.terminatedAt == 0) {
        if (c.p->probePending) continue;
        // A generation that never became ready is still in its startup probe, which owns
        // the probe client: it only gets the signal (its startup then ends with the exit).
        if (c.p->readyAt > 0) impl->closeConnections(*c.p);
        if (c.p->state != "killed") {
          ::kill(c.p->pid, SIGTERM);
          c.signalled = true;
          impl->event(*c.p, "terminate", "server shutdown");
        }
        c.terminatedAt = now; c.terminatedAtUnixMs = wall();
      } else if (!c.sigkill && c.signalled && now - c.terminatedAt >= impl->config.rotationTermGraceMs) {
        c.sigkill = true;
        ::kill(c.p->pid, SIGKILL);
        impl->event(*c.p, "kill", "server shutdown termination grace expired");
      }
    }
    if (exited) break;
    co_await impl->provider.getTimer().afterDelay(5 * kj::MILLISECONDS);
  }
  std::ostringstream out;
  out << std::setprecision(17) << "{\"startedAtUnixMs\":" << startedAtUnixMs << ",\"finishedAtUnixMs\":" << wall()
      << ",\"durationMs\":" << mono() - started << ",\"generations\":[";
  for (size_t i = 0; i < closing.size(); ++i) {
    const auto& c = closing[i];
    out << (i ? "," : "") << "{\"processId\":\"" << c.p->id << "\",\"processClass\":\"" << processClass(*c.p)
        << "\",\"generation\":" << c.p->generation << ",\"pid\":" << c.p->spawnPid
        << ",\"terminatedAtUnixMs\":" << c.terminatedAtUnixMs << ",\"exitedAtUnixMs\":" << c.p->exitedAtUnixMs
        << ",\"terminateToExitMs\":" << (c.terminatedAt > 0 && c.p->exitedAt > 0 ? c.p->exitedAt - c.terminatedAt : 0.0)
        << ",\"signalled\":" << (c.signalled ? "true" : "false") << ",\"sigkill\":" << (c.sigkill ? "true" : "false")
        << ",\"exitStatus\":\"" << escape(c.p->exitStatus) << "\",\"exitReason\":\"" << escape(c.p->lastReason) << "\"}";
  }
  out << "]}";
  co_return out.str();
}

ProcessPool::Killed ProcessPool::killAll(const std::string& reason) {
  impl->refresh();
  impl->shuttingDown = impl->closingAll = true;
  if (impl->rotation) impl->abortRotation(mono());
  Killed killed;
  for (auto& s : impl->slots) Impl::eachGeneration(s, [&](const auto& p) {
    if (p->pid == 0 || p->state == "killed") return;
    killed.calls += p->inFlight; killed.contexts += p->backgroundContexts(); ++killed.processes;
    p->closing = true; p->plannedExit = true;
    impl->killProcess(*p, reason, 0);
  });
  return killed;
}

}  // namespace coworkerd

#include "async-log.h"
#include "json.h"
#include "log-cleaning.h"
#include "log-sink.h"
#include "support.h"

#include <capnp/message.h>
#include <kj/debug.h>
#include <openssl/rand.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <poll.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>

namespace coworkerd {
namespace {
struct TempDir {
  std::string path = (std::filesystem::temp_directory_path() / "coworkerd-log-test-XXXXXX").string();
  TempDir() { KJ_REQUIRE(mkdtemp(path.data()) != nullptr); }
  ~TempDir() { std::error_code ignored; std::filesystem::remove_all(path, ignored); }
  std::string file(const char* name) const { return path + '/' + name; }
};
void writeFile(const std::string& path, std::string_view text) {
  const auto fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  KJ_REQUIRE(fd >= 0);
  size_t at = 0;
  while (at < text.size()) {
    const auto n = write(fd, text.data() + at, text.size() - at);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) { close(fd); KJ_FAIL_REQUIRE("could not write the component fixture"); }
    at += n;
  }
  close(fd);
}
std::string syntheticValue() {
  std::array<unsigned char, 24> bytes;
  KJ_REQUIRE(RAND_bytes(bytes.data(), bytes.size()) == 1);
  std::string value;
  constexpr char hex[] = "0123456789abcdef";
  for (auto byte : bytes) { value += hex[byte >> 4]; value += hex[byte & 15]; }
  return value;
}
// Even a failing test reports only a digest, never a value or the log holding it.
bool containsDigest(std::string_view text, size_t size, const std::string& digest) {
  for (size_t at = 0; at + size <= text.size(); ++at)
    if (sha256Hex(text.substr(at, size)) == digest) return true;
  return false;
}
void requireClean(const std::string& text, const std::string& value) {
  const auto digest = sha256Hex(value);
  const bool leaked = containsDigest(text, value.size(), digest);
  KJ_REQUIRE(!leaked, "diagnostic redaction failed; synthetic value SHA-256", digest.c_str());
}
void streams() {
  const auto value = syntheticValue() + "\n\"\\" + syntheticValue();
  std::vector<std::string> needles;
  addRedaction(needles, value);
  sortRedactions(needles);
  retainPlatformLogRedactions(needles);
  const auto escaped = jsonEscape(value);
  for (const auto& pattern : {value, escaped}) {
    for (size_t split = 0; split <= pattern.size(); ++split) {
      PlatformLogCleaner cleaner;
      auto text = cleaner.append("before:" + pattern.substr(0, split));
      text += cleaner.append(pattern.substr(split) + ":after\n", true);
      requireClean(text, pattern);
      const bool exact = text == "before:[redacted]:after\n";
      KJ_REQUIRE(exact, "stream split must preserve surrounding diagnostics", split);
    }
  }
  // A value removed from the current deployment still cleans queued old output.
  secretRedactions().stamp = nullptr;
  secretRedactions().needles();
  PlatformLogCleaner oldGeneration;
  const auto old = oldGeneration.append(value, true);
  requireClean(old, value);
  const bool replaced = old == "[redacted]";
  KJ_REQUIRE(replaced);
  // Exhaustion never falls back to writing raw diagnostics.
  retainPlatformLogRedactions({std::string(16 * 1024 * 1024 + 1, 'x')});
  PlatformLogCleaner exhausted;
  const bool discarded = exhausted.append("bounded diagnostic\n", true).empty() && exhausted.suppressed();
  KJ_REQUIRE(discarded, "a full dictionary must suppress output");
}
struct Child {
  pid_t pid = -1;
  int control = -1;
  Child(const char* workerd, const std::string& config, uint32_t slot, uint32_t processClass) {
    WorkerdOutput::Pipes pipes;
    KJ_REQUIRE(WorkerdOutput::open(pipes));
    int channel[2]; KJ_REQUIRE(pipe(channel) == 0);
    // Keep the control descriptor away from the stdout/stderr dup2 targets.
    const auto childControl = fcntl(channel[1], F_DUPFD_CLOEXEC, 64);
    close(channel[1]); KJ_REQUIRE(childControl >= 0);
    pid = fork(); KJ_REQUIRE(pid >= 0);
    if (pid == 0) {
      if (dup2(pipes.stdoutWrite, STDOUT_FILENO) < 0 || dup2(pipes.stderrWrite, STDERR_FILENO) < 0 ||
          dup2(childControl, 3) < 0) _exit(126);
      for (int fd : {pipes.stdoutRead, pipes.stdoutWrite, pipes.stderrRead, pipes.stderrWrite, channel[0], childControl})
        if (fd > 3) close(fd);
      execl(workerd, workerd, "serve", config.c_str(), "--control-fd=3", static_cast<char*>(nullptr));
      _exit(127);
    }
    close(childControl); control = channel[0];
    fcntl(control, F_SETFD, FD_CLOEXEC);
    WorkerdOutput::attach(pipes, slot, processClass == 2 ? "d0" : "p0", processClass, slot + 1, pid);
  }
  ~Child() {
    if (control >= 0) close(control);
    if (pid > 0) { kill(pid, SIGKILL); while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {} }
  }
  int wait() {
    int status = 0;
    for (unsigned i = 0; i < 500; ++i) {
      const auto result = waitpid(pid, &status, WNOHANG);
      if (result == pid) { pid = -1; return status; }
      KJ_REQUIRE(result == 0 || errno == EINTR);
      usleep(10000);
    }
    KJ_FAIL_REQUIRE("component Worker did not exit");
  }
  uint16_t listenPort() {
    std::string partial;
    const auto deadline = monotonicTimeMs() + 5000;
    while (monotonicTimeMs() < deadline) {
      pollfd descriptor{control, POLLIN, 0};
      KJ_REQUIRE(poll(&descriptor, 1, 100) >= 0 || errno == EINTR);
      if (!(descriptor.revents & (POLLIN | POLLHUP))) continue;
      char buffer[4096]; const auto n = read(control, buffer, sizeof(buffer));
      KJ_REQUIRE(n > 0, "Worker exited before listening");
      partial.append(buffer, n);
      for (auto end = partial.find('\n'); end != std::string::npos; end = partial.find('\n')) {
        capnp::MallocMessageBuilder message;
        capnp::JsonCodec codec;
        codec.decode(kj::StringPtr(partial.data(), end), message.initRoot<capnp::JsonValue>());
        const auto record = message.getRoot<capnp::JsonValue>().asReader();
        if (json::text(record, "event") == "listen" && json::text(record, "socket") == "http")
          return static_cast<uint16_t>(json::field(record, "port").getNumber());
        partial.erase(0, end + 1);
      }
    }
    KJ_FAIL_REQUIRE("component Worker did not listen");
  }
};
void request(uint16_t port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0); KJ_REQUIRE(fd >= 0);
  timeval timeout{5, 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  sockaddr_in address{}; address.sin_family = AF_INET; address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    close(fd); KJ_FAIL_REQUIRE("could not call the component Worker");
  }
  constexpr std::string_view call = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
  const auto sent = send(fd, call.data(), call.size(), 0);
  std::string answer; char buffer[4096];
  if (sent == static_cast<ssize_t>(call.size())) for (;;) {
    const auto n = read(fd, buffer, sizeof(buffer));
    if (n > 0) { answer.append(buffer, n); continue; }
    if (n < 0 && errno == EINTR) continue;
    break;
  }
  close(fd);
  KJ_REQUIRE(answer.starts_with("HTTP/1.1 500"), "the real request must throw");
}
void workerDiagnostics(const char* workerd) {
  KJ_REQUIRE(workerd && access(workerd, X_OK) == 0, "set COWORKERD_TEST_WORKERD to the built workerd");
  TempDir directory;
  const auto value = syntheticValue();
  const auto nodePath = directory.file("node.log"), workerdPath = directory.file("workerd.log");
  const auto savedOut = dup(STDOUT_FILENO), savedErr = dup(STDERR_FILENO);
  KJ_REQUIRE(savedOut >= 0 && savedErr >= 0);
  // Restore before an assertion can print. Raw values never reach the test log,
  // even when qualification deliberately removes the production cleaner.
  struct Restore {
    int out, err;
    ~Restore() { dup2(out, STDOUT_FILENO); dup2(err, STDERR_FILENO); close(out); close(err); }
  } restore{savedOut, savedErr};
  const auto nodeFd = open(nodePath.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  KJ_REQUIRE(nodeFd >= 0); dup2(nodeFd, STDOUT_FILENO); dup2(nodeFd, STDERR_FILENO); close(nodeFd);
  {
    AsyncLog nodeLog;
    WorkerdOutput output(workerdPath);
    // Both consumers already forked; production publication must reach them.
    auto& redactions = secretRedactions();
    redactions.stamp = [] { return std::pair<uint64_t, uint64_t>{1, 1}; };
    redactions.values = [value] { return std::vector<std::string>{value}; };
    redactions.needles();
    std::cout << "node-out:" << value << '\n';
    std::cerr << "node-error:" << value << '\n';
    uint32_t slot = 0;
    for (const auto processClass : {0u, 2u}) for (const bool top : {true, false}) {
      const auto stem = std::to_string(slot);
      const auto script = directory.path + '/' + stem + ".js";
      const auto config = directory.path + '/' + stem + ".capnp";
      writeFile(script, top ? "throw new Error('top-diagnostic:' + SECRET);" :
          "addEventListener('fetch', () => { throw new Error('request-diagnostic:' + SECRET); });");
      writeFile(config, "using Workerd = import \"/workerd/workerd.capnp\";\nconst test :Workerd.Config = ("
          "services = [(name = \"main\", worker = (serviceWorkerScript = embed \"" + stem +
          ".js\", compatibilityDate = \"2024-01-01\", bindings = [(name = \"SECRET\", text = \"" + value +
          "\")]))], sockets = [(name = \"http\", address = \"127.0.0.1:0\", http = (), service = \"main\")]);\n");
      Child child(workerd, config, slot++, processClass);
      if (top) { const auto status = child.wait(); KJ_REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) != 0); }
      else { request(child.listenPort()); kill(child.pid, SIGTERM); child.wait(); }
    }
    redactions.stamp = nullptr; redactions.values = nullptr;
  }
  // Stop capturing before hash assertions, including deliberately failing ones.
  dup2(savedOut, STDOUT_FILENO); dup2(savedErr, STDERR_FILENO);
  const auto node = readFile(nodePath), diagnostics = readFile(workerdPath);
  requireClean(node, value); requireClean(diagnostics, value);
  KJ_REQUIRE(node.find("node-out:[redacted]") != std::string::npos &&
      node.find("node-error:[redacted]") != std::string::npos, "both node streams must be cleaned");
  for (const auto marker : {"top-diagnostic:[redacted]", "request-diagnostic:[redacted]"}) {
    const auto first = diagnostics.find(marker);
    KJ_REQUIRE(first != std::string::npos && diagnostics.find(marker, first + 1) != std::string::npos,
        "ordinary and durable diagnostic sources must both be exercised", marker);
  }
}
}  // namespace
}  // namespace coworkerd

int main(int argc, char** argv) {
  try {
    if (argc == 2 && std::string_view(argv[1]) == "streams") coworkerd::streams();
    else { KJ_REQUIRE(argc == 2); coworkerd::workerDiagnostics(argv[1]); }
    return 0;
  } catch (const kj::Exception& error) {
    // KJ_REQUIRE expressions above carry booleans/digests, never raw logs.
    std::cerr << error.getDescription().cStr() << '\n'; return 1;
  }
}

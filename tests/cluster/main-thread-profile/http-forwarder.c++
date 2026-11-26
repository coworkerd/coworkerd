#include <kj/async-io.h>
#include <kj/compat/http.h>

int main(int argc, char** argv) {
  KJ_REQUIRE(argc == 3, "usage: coworkerd-bench-http-forwarder <listen-address> <upstream-address>");
  auto io = kj::setupAsyncIo();
  auto& network = io.provider->getNetwork();
  auto listener = network.parseAddress(argv[1]).wait(io.waitScope)->listen();
  auto upstream = network.parseAddress(argv[2]).wait(io.waitScope);
  auto headers = kj::HttpHeaderTable::Builder().build();
  auto& timer = io.provider->getTimer();
  auto client = kj::newHttpClient(timer, *headers, *upstream);
  auto service = kj::newHttpService(*client);
  kj::HttpServer server(timer, *headers, *service);
  server.listenHttp(*listener).wait(io.waitScope);
}

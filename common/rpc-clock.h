#pragma once

#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/time.h>

// Cap'n Proto two-party connections whose network times its outgoing messages with the coarse monotonic clock.
// TwoPartyVatNetwork reads its clock once per message sent, only to say how long the oldest queued message has
// waited; capnp::TwoPartyClient and TwoPartyServer always give it the precise clock, which on a host whose clock
// source has no vDSO reading (hpet: 1.4 microseconds a read) is a system call per message -- about ten
// per request on the node's main thread. The coarse clock is read from the vDSO whatever the clock
// source (14 ns there), at a resolution of one kernel tick.
namespace coworkerd {

struct RpcMessageCounts {
  uint64_t sent = 0, received = 0;
};

class CountedRpcStream final: public capnp::MessageStream {
public:
  CountedRpcStream(kj::AsyncIoStream& connection, RpcMessageCounts& counts): stream(connection), counts(counts) {}
  kj::Promise<kj::Maybe<capnp::MessageReaderAndFds>> tryReadMessage(
      kj::ArrayPtr<kj::OwnFd> fdSpace, capnp::ReaderOptions options, kj::ArrayPtr<capnp::word> scratch) override {
    return stream.tryReadMessage(fdSpace, options, scratch).then([this](auto message) {
      if (message != kj::none) ++counts.received;
      return message;
    });
  }
  kj::Promise<void> writeMessage(kj::ArrayPtr<const int> fds,
      kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>> segments) override {
    ++counts.sent;
    return stream.writeMessage(fds, segments);
  }
  kj::Promise<void> writeMessages(kj::ArrayPtr<kj::ArrayPtr<const kj::ArrayPtr<const capnp::word>>> messages) override {
    counts.sent += messages.size();
    return stream.writeMessages(messages);
  }
  kj::Maybe<int> getSendBufferSize() override { return stream.getSendBufferSize(); }
  kj::Promise<void> end() override { return stream.end(); }
private:
  capnp::AsyncIoMessageStream stream;
  RpcMessageCounts& counts;
};

class CoarseTwoPartyClient {
public:
  explicit CoarseTwoPartyClient(kj::AsyncIoStream& connection)
      : network(connection, capnp::rpc::twoparty::Side::CLIENT, capnp::ReaderOptions(), kj::systemCoarseMonotonicClock()),
        rpcSystem(capnp::makeRpcClient(network)) {}
  CoarseTwoPartyClient(kj::AsyncIoStream& connection, RpcMessageCounts& counts)
      : counted(kj::heap<CountedRpcStream>(connection, counts)),
        network(*counted, capnp::rpc::twoparty::Side::CLIENT, capnp::ReaderOptions(), kj::systemCoarseMonotonicClock()),
        rpcSystem(capnp::makeRpcClient(network)) {}
  KJ_DISALLOW_COPY_AND_MOVE(CoarseTwoPartyClient);
  // As capnp::TwoPartyClient::bootstrap().
  capnp::Capability::Client bootstrap() {
    capnp::word scratch[4]{};
    capnp::MallocMessageBuilder message(scratch);
    auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
    vatId.setSide(capnp::rpc::twoparty::Side::SERVER);
    return rpcSystem.bootstrap(vatId);
  }
  kj::Promise<void> onDisconnect() { return network.onDisconnect(); }

private:
  kj::Own<CountedRpcStream> counted;
  capnp::TwoPartyVatNetwork network;
  capnp::RpcSystem<capnp::rpc::twoparty::VatId> rpcSystem;
};

// As capnp::TwoPartyServer::accept(kj::AsyncIoStream&): the returned promise resolves when the client disconnects;
// dropping it cancels the connection's RPC.
class CoarseTwoPartyServer {
public:
  explicit CoarseTwoPartyServer(capnp::Capability::Client bootstrapInterface)
      : bootstrapInterface(kj::mv(bootstrapInterface)) {}
  kj::Promise<void> accept(kj::AsyncIoStream& connection, RpcMessageCounts* counts = nullptr) {
    auto state = counts ? kj::heap<Accepted>(connection, bootstrapInterface, *counts)
        : kj::heap<Accepted>(connection, bootstrapInterface);
    auto done = state->network.onDisconnect();
    return done.attach(kj::mv(state));
  }

private:
  struct Accepted {
    Accepted(kj::AsyncIoStream& connection, capnp::Capability::Client& bootstrap)
        : network(connection, capnp::rpc::twoparty::Side::SERVER, capnp::ReaderOptions(), kj::systemCoarseMonotonicClock()),
          rpcSystem(capnp::makeRpcServer(network, kj::cp(bootstrap))) {}
    Accepted(kj::AsyncIoStream& connection, capnp::Capability::Client& bootstrap, RpcMessageCounts& counts)
        : counted(kj::heap<CountedRpcStream>(connection, counts)),
          network(*counted, capnp::rpc::twoparty::Side::SERVER, capnp::ReaderOptions(), kj::systemCoarseMonotonicClock()),
          rpcSystem(capnp::makeRpcServer(network, kj::cp(bootstrap))) {}
    kj::Own<CountedRpcStream> counted;
    capnp::TwoPartyVatNetwork network;
    capnp::RpcSystem<capnp::rpc::twoparty::VatId> rpcSystem;
  };
  capnp::Capability::Client bootstrapInterface;
};

}  // namespace coworkerd

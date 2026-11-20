#pragma once

#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/time.h>

// Cap'n Proto two-party connections whose network times its outgoing messages with the coarse monotonic clock.
// TwoPartyVatNetwork reads its clock once per message sent, only to say how long the oldest queued message has
// waited; capnp::TwoPartyClient and TwoPartyServer always give it the precise clock, which on a host whose clock
// source has no vDSO reading (cwkd-p40's hpet: 1.4 microseconds a read) is a system call per message -- about ten
// per request on the node's main thread (v138 section 1). The coarse clock is read from the vDSO whatever the clock
// source (14 ns there), at a resolution of one kernel tick.
namespace coworkerd {

class CoarseTwoPartyClient {
public:
  explicit CoarseTwoPartyClient(kj::AsyncIoStream& connection)
      : network(connection, capnp::rpc::twoparty::Side::CLIENT, capnp::ReaderOptions(), kj::systemCoarseMonotonicClock()),
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
  capnp::TwoPartyVatNetwork network;
  capnp::RpcSystem<capnp::rpc::twoparty::VatId> rpcSystem;
};

// As capnp::TwoPartyServer::accept(kj::AsyncIoStream&): the returned promise resolves when the client disconnects;
// dropping it cancels the connection's RPC.
class CoarseTwoPartyServer {
public:
  explicit CoarseTwoPartyServer(capnp::Capability::Client bootstrapInterface)
      : bootstrapInterface(kj::mv(bootstrapInterface)) {}
  kj::Promise<void> accept(kj::AsyncIoStream& connection) {
    auto state = kj::heap<Accepted>(connection, bootstrapInterface);
    auto done = state->network.onDisconnect();
    return done.attach(kj::mv(state));
  }

private:
  struct Accepted {
    Accepted(kj::AsyncIoStream& connection, capnp::Capability::Client& bootstrap)
        : network(connection, capnp::rpc::twoparty::Side::SERVER, capnp::ReaderOptions(), kj::systemCoarseMonotonicClock()),
          rpcSystem(capnp::makeRpcServer(network, kj::cp(bootstrap))) {}
    capnp::TwoPartyVatNetwork network;
    capnp::RpcSystem<capnp::rpc::twoparty::VatId> rpcSystem;
  };
  capnp::Capability::Client bootstrapInterface;
};

}  // namespace coworkerd

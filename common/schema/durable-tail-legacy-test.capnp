@0xe13ef7c43ffe2271;
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("coworkerd::tailtest::legacy");

# Frozen wire layout of the request before the optional object ID was added.
struct DurableTailRequest {
  namespaceId @0 :Text;
  shard @1 :UInt32;
  # The asking node's own epoch of the shard: a tail is given out only to the shard's current owner.
  shardEpoch @2 :UInt64;
  # Whose log, and from which sequence on (what the object store already covers); epoch 0: any epoch of that node.
  ownerNodeId @3 :Text;
  ownerNodeEpoch @4 :UInt64;
  afterSeq @5 :UInt64;
  destinationEpoch @6 :Text;
}

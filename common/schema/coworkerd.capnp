@0xd3f914c7fb36eb25;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("coworkerd::v1::rpc");

using import "/capnp/compat/http-over-capnp.capnp".HttpService;

interface EventDispatcher @0xf20697475ec1752d {
  getHttpService @0 () -> (http :HttpService);
  unused1 @1 ();
  unused2 @2 ();
  unused3 @3 ();
  unused4 @4 ();
  unused5 @5 ();
  unused6 @6 ();
  unused7 @7 ();
  unused8 @8 ();
  # A JS RPC session on this event: workerd's own mechanism for a method call on a Service Binding or a Durable
  # Object stub (src/workerd/io/worker-interface.capnp). `topLevel` takes exactly one method call, and dropping
  # `session` ends the session. The node implements it on the channel its runtime's SERVER Binding opens and on the
  # peer session, where it only proxies capabilities: it reads the method's name -- our envelope -- and never a
  # serialized JavaScript value (docs/architecture.md, section 3).
  jsRpcSession @9 () -> (topLevel :JsRpcTarget, session :JsRpcSession);
}

# Mirrors of workerd's JS RPC types (src/workerd/io/worker-interface.capnp), with the interface ids workerd's own
# clients and servers send and a layout equal to theirs field for field: a serialized JavaScript value (`JsValue`) and
# a span context are `AnyPointer` here, because the node copies them from one message to the other -- capabilities
# inside them included -- and never decodes them. Everything the node reads is text: the name of the method.
interface JsRpcTarget @0xa1a96525142a9d98 {
  struct CallParams @0xaff95bff9d8bfd33 {
    union {
      methodName @0 :Text;
      methodPath @2 :List(Text);
    }
    operation :union {
      # A `JsValue` holding the call's arguments, or no arguments at all.
      callWithArgs @1 :AnyPointer;
      getProperty @3 :Void;
    }
    resultsStreamHandler :union {
      obsolete4 @4 :Capability;
      # The caller's `JsValue.ExternalPusher`, through which the callee pushes the streams and stubs its answer
      # carries: the node passes the capability on, so they are pushed into the calling isolate itself.
      externalPusher @5 :Capability;
    }
    callerSpanContext @6 :AnyPointer;
  }

  struct CallResults @0xf8796f556942bdc7 {
    # The returned `JsValue`.
    result @0 :AnyPointer;
    callPipeline @1 :JsRpcTarget;
    hasDisposer @2 :Bool;
    obsolete3 @3 :Capability;
  }

  call @0 CallParams -> CallResults;
}

interface JsRpcSession @0xbbfec607777f1620 {
  # No methods: the session lasts as long as the capability is held.
}

interface WorkerdBootstrap @0x878048cda334ade3 {
  startEvent @0 (cfBlobJson :Text, fromPersistentStub :Bool)
      -> (dispatcher :EventDispatcher);
}

interface PeerServer @0xab4d6df924ec776f {
  openInvocation @0 (invocation :RemoteInvocation)
      -> (result :InvocationOpenResult);
  # A request to a Durable Object, sent to the node that owns the object's shard: accepted only when the receiver holds
  # the shard at the epoch the sender expects (or, with epoch 0, once it took the shard), refused before anything ran.
  openObjectInvocation @1 (invocation :ObjectInvocation)
      -> (result :InvocationOpenResult);
  # The Durable Object log of the node that calls: its replicator opens one stream per peer and sends it the records of
  # every shard this node is a peer of, and this node buffers them until a segment on the object store covers them.
  openDurableLog @2 (session :DurableLogSession) -> (sink :DurableLogSink);
  # What this node still holds of a previous owner's log for one shard: a new owner asks for it after it has replayed
  # the object store, so that the writes the previous owner answered but did not get into a segment are not lost.
  fetchDurableTail @3 (request :DurableTailRequest) -> (result :DurableTailResult);
}

struct DurableLogSession {
  # The owner that opens the stream: its node id, its membership epoch and the process that runs it.
  nodeId @0 :Text;
  nodeEpoch @1 :UInt64;
  instance @2 :Text;
  # The receiver's membership epoch the sender addresses (a stream to another epoch is refused).
  destinationEpoch @3 :Text;
  # The first node sequence this stream carries.
  firstSeq @4 :UInt64;
}

interface DurableLogSink {
  # One or more records of the owner's log, in sequence order, each prefixed with its length (UInt32, little endian).
  # The answer is the highest sequence this node now holds of the stream, which is what lets the owner open its output
  # gate; `refused` names the reason when the records were not taken (the buffer is full, or the shard's epoch is not
  # the one this node knows: the owner has been fenced). `partial` carries a piece of what the next calls complete --
  # one transaction's frames can be larger than a message may be -- and holds the bytes until the call that is not
  # partial; nothing of a record is held until it is whole.
  append @0 (records :Data, partial :Bool) -> (accepted :UInt64, refused :Text);
  # Every record up to and including `upTo` is in a segment on the object store: this node drops them.
  trim @1 (upTo :UInt64) -> ();
  # Records the owner isolated: they were never confirmed and will never be in a segment, so they must not reach a
  # rebuild of their object.
  drop @2 (sequences :List(UInt64)) -> ();
}

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

struct DurableTailResult {
  # The records, each prefixed with its length (UInt32, little endian), in sequence order and without a gap.
  records @0 :Data;
  firstSeq @1 :UInt64;
  lastSeq @2 :UInt64;
  # The sequence up to which this node has dropped what it held (trimmed or isolated), and how many records it holds
  # beyond `lastSeq` that it could not give out in one answer.
  droppedTo @3 :UInt64;
  count @4 :UInt32;
  more @5 :Bool;
  refused @6 :Text;
}

struct InvocationOpenResult {
  union {
    accepted @0 :EventDispatcher;
    rejected @1 :InvocationRejection;
  }
}

struct InvocationRejection {
  code @0 :Text;
  reason @1 :Text;
  loadBand @2 :Text;
  inFlight @3 :UInt32;
  limit @4 :UInt32;
  avoidMs @5 :UInt32;
  status @6 :UInt16;
  # LOAD_FAILED: package-unavailable, evaluation, limit or secret-missing.
  category @7 :Text;
}

struct RemoteInvocation {
  workerId @0 :Text;
  versionId @1 :Text;
  contentHash @2 :Text;
  treeId @3 :Text;
  parentInvocationId @4 :Text;
  subrequestGrant @5 :UInt16;
  remainingMs @6 :Float64;
  depth @7 :UInt16;
  placementHops @8 :UInt8;
  alreadyPlaced @9 :Bool;
  originServerId @10 :Text;
  callSequence @11 :UInt64;
  orderSequence @12 :UInt64;
  versionPins @13 :List(VersionPin);
  definitionHash @14 :Text;
  destinationEpoch @15 :Text;
  ingressRoot @16 :Bool = false;
  ingressServerId @17 :Text;
  entrypoint @18 :Text;
}

# A request to a Durable Object from the node where its caller runs to the node that owns the object's shard. The owner
# runs it as a request it received itself would run, on its own durable process, with the Worker's active version as it
# sees it; it never sends it on. The caller is a stateless Worker's call tree (treeId set: the request is a Binding call
# of that tree, with its share of the budget and the remaining time) or an object (callerObjectId set: the request
# starts a call tree of its own on the owner).
struct ObjectInvocation {
  workerId @0 :Text;
  namespaceId @1 :Text;
  className @2 :Text;
  objectId @3 :Text;
  # The object's shard, and the shard's epoch the sender expects the receiver to hold; 0: the sender found no live owner
  # and asks the receiver -- the shard's default owner as the sender sees the members -- to take the shard.
  shard @4 :UInt32;
  shardEpoch @5 :UInt64;
  # How many Durable Object calls led to this request (x-coworkerd-do-hops; beyond 8, DO_HOP_LIMIT).
  hops @6 :UInt16;
  # The receiver's membership epoch the sender addresses.
  destinationEpoch @7 :Text;
  originServerId @8 :Text;
  treeId @9 :Text;
  parentInvocationId @10 :Text;
  subrequestGrant @11 :UInt16;
  depth @12 :UInt16;
  remainingMs @13 :Float64;
  callSequence @14 :UInt64;
  orderSequence @15 :UInt64;
  ingressServerId @16 :Text;
  callerNamespaceId @17 :Text;
  callerObjectId @18 :Text;
  # A tenant's database (the D1 shape): its name, so the owner runs the platform's own SQL Worker class for it.
  database @19 :Text;
  # The firing of the object's alarm: the time it was scheduled for and how many times the handler has been tried.
  alarmAtUnixMs @20 :UInt64;
  alarmRetryCount @21 :UInt32;
}

struct ServerConfig {
  # The node's id: the segment of its objects members/<nodeId>/ and nodes/<nodeId>/, which its
  # object store identity alone writes, and of every record it writes.
  nodeId @0 :Text;
  peerListenAddress @1 :Text;
  peers @2 :List(Peer);
  placements @3 :List(Placement);
  initialPlacementHops @4 :UInt8 = 8;
  disableAlreadyPlacedForTest @5 :Bool = false;
  peerTimeoutMs @6 :UInt32 = 2000;
  clockOffsetMsForTest @7 :Int64 = 0;
  membership @8 :MembershipConfig;
  desiredState @9 :DesiredStateConfig;
  placement @10 :PlacementConfig;
  processPool @11 :ProcessPoolConfig;
  ingressPlacement @12 :Bool = true;
  cpuLimitMs @13 :UInt32 = 200;
  # Quarantine processes; suspects are tenants, sharded over these processes by the tenant's
  # rendezvous hash, and trial loads run here too.
  quarantineProcesses @14 :UInt32 = 2;
  idleEvictMs @17 :UInt32 = 300000;
  idleDestroyIntervalMs @18 :UInt32 = 10;
  memoryWatermarkEnabled @19 :Bool = true;
  memoryBudgetBytes @20 :UInt64 = 0;
  memoryLimitMiB @21 :UInt32 = 128;
  suspectBaseMs @15 :UInt32 = 600000;
  suspectMaxMs @16 :UInt32 = 3600000;
  # The Server's exit: how long it waits for its in-flight call trees after
  # announcing that it drains; 0 turns the drain off (exit at once).
  drainTimeoutMs @22 :UInt32 = 30000;
  # Process rotation; COWORKERD_ROTATION_* environment variables override each field.
  rotation @23 :RotationConfig;
  # The load check: before a version's first dispatch to a workerd process (and again once
  # the last check there is older than the placement idle period), the Server loads it there as a
  # trial does and dispatches only if it loads; a failure is an explicit refusal, LOAD_FAILED.
  # false turns the check off; COWORKERD_LOAD_CHECK=0|1 overrides it.
  loadCheck @24 :Bool = true;

  # The single configuration file of `coworkerd --config FILE`. A minimal file
  # names nodeId and storage.endpoint; every other field has a default. The
  # legacy positional form (manifest, workerd, addresses on the command line) takes the
  # shortcuts below too, but not the four paths and addresses its arguments give.
  # Default /var/lib/coworkerd: logs/, run/, cache/, processes/ of this Server. Serve only.
  stackDirectory @25 :Text;
  # Default: `workerd` beside this executable. Serve only.
  workerdPath @26 :Text;
  # Default: ../runtime/runtime.js from this executable's directory. Serve only.
  runtimePath @27 :Text;
  # The ingress. Default 0.0.0.0:8930. Serve only.
  listenAddress @28 :Text;
  # Ordinals 29 and 39 named a store 0.3 used beside the object store; 0.4 has no etcd, and a configuration that still
  # names either field is refused at once (common/config.c++).
  unused29 @29 :Text;
  # What other Servers dial for this one (membership.advertisedAddress); default
  # <hostname>:<port of peerListenAddress>, whose default is 0.0.0.0:8934 in serve.
  advertisedAddress @30 :Text;
  # The object store: the desired state (sets desiredState.objectStore and its cache, and the desired state's backend to
  # objectStore unless desiredState.backend says otherwise), the code packages, and the members (the membership's backend
  # to objectStore unless membership.backend says otherwise) with the suspects and the usage beside them.
  storage @31 :StorageConfig;
  # The management endpoints (/__coworkerd/ except ready and probe); COWORKERD_MANAGEMENT_TOKEN
  # overrides the token.
  management @32 :ManagementConfig;
  # The control plane: `coworkerd-apiserver --config FILE` reads the same file -- the object store with the deploy
  # identity (storage), the secrets key (secrets), the management token (management), the usage's retention (usage) --
  # and this.
  control @33 :ControlConfig;
  # The Workers' logs: each Server's in-memory log of every Worker, the tenant log quota, and
  # the size rotation of logs/workerd.log.
  logs @34 :LogsConfig;
  # The workerd processes' least-privilege environment: each workerd started through
  # coworkerd-launcher (launcher/PROTOCOL.md).
  sandbox @35 :SandboxConfig;
  # Per-tenant usage: counted on each node, one object per hour with every tenant's counts below its own prefix,
  # nodes/<nodeId>/usage/<hour>.
  usage @36 :UsageConfig;
  # Tenant Workers' outbound requests (docs/egress-and-auth.md): a tenant's fetch() reaches this node's egress
  # service through <stack>/run/egress.sock; the node checks the destination against the policy, resolves the name and
  # connects itself (TLS included), meters the bytes per tenant and answers. The tenant's workerd process still has
  # no network of its own.
  egress @37 :EgressConfig;
  # The peer listener's authentication: every node signs its peer connections with the first key of a cluster
  # key file and accepts either of its keys. Unset: peer connections are not authenticated (a startup warning).
  peerAuth @38 :PeerAuthConfig;
  # See ordinal 29.
  unused39 @39 :Text;
  # The cluster's secrets key: secrets' values are sealed with it in the tenants' manifests on the object store (AES-256-GCM).
  # The control plane and every node with the objectStore desired state need it.
  secrets @40 :SecretsConfig;
  # V8's garbage collection in the workerd processes (docs/process-isolation.md, section 4): the CPU limit leaves a full
  # collection out of the request it ran in, as far as its Worker's budget goes; long pauses are named; an idle
  # Worker's isolate is collected between its requests; the runtime's heap is sized for its working set.
  gc @41 :GcConfig;
  # The ingress's inbound WebSocket connections (docs/compatibility.md, section 7).
  ingress @42 :IngressConfig;
  # Durable Objects (docs/compatibility.md, section 7): the workerd processes that run them on this node.
  durableObjects @43 :DurableObjectsConfig;

  struct DurableObjectsConfig {
    # The durable processes d0, d1, ...: each object runs in one of them, chosen by the object's id. They are not replaced
    # with age; they rotate when the workerd file changes, by hand and at rotation.residentBytes, each draining first.
    processes @0 :UInt32 = 1;
    # An object's write is answered once K peers hold its frames: the first `peers` nodes after the object's shard on the
    # member ring, or one fewer than the live members when there are not that many. 0 keeps every write on this node
    # alone (its log is written and flushed to this machine's disk before the answer, which costs tens of milliseconds).
    peers @1 :UInt32 = 2;
    # How long a commit waits for its peers before the write fails with STORAGE_UNAVAILABLE. Below workerd's own 30 s
    # output gate timeout, so the caller gets a code it can act on rather than a reset object.
    confirmTimeoutMs @2 :UInt32 = 5000;
    # How long a request for an object being rebuilt waits before 503 DO_RECOVERING.
    recoverTimeoutMs @3 :UInt32 = 30000;
    # A segment of the node's log is sealed and uploaded every segmentIntervalMs or once it holds segmentBytes.
    segmentIntervalMs @4 :UInt32 = 30000;
    segmentBytes @5 :UInt64 = 16777216;
    # An object's whole database is uploaded as a new snapshot once its log since the last one passes snapshotBytes, or
    # once snapshotIntervalMs has gone by. A snapshot is uploaded in parts of snapshotPartBytes.
    snapshotBytes @6 :UInt64 = 67108864;
    snapshotIntervalMs @7 :UInt32 = 86400000;
    snapshotPartBytes @8 :UInt64 = 16777216;
    # This machine's Durable Object files are a cache: the files of cold objects whose changes are all in a segment are
    # removed once they take more than diskBytes. 0: half of the filesystem the stack is on, measured at startup.
    diskBytes @9 :UInt64 = 0;
    # What one node buffers for the owners it is a peer of. Past it the owners' writes are refused, so their callers get
    # STORAGE_UNAVAILABLE rather than an unbounded buffer here.
    peerBufferBytes @10 :UInt64 = 268435456;
    # The largest one object may become, by its database file at a checkpoint; past it the object answers 413
    # DO_OBJECT_TOO_LARGE (10 GB, as on Cloudflare).
    objectBytes @11 :UInt64 = 10737418240;
    # The WAL size at which a durable process checkpoints an object, and how long an object must be idle for the node to
    # ask for one.
    checkpointBytes @12 :UInt64 = 4194304;
    idleCheckpointMs @13 :UInt32 = 10000;
    # A failed upload of a segment or snapshot part is tried again after uploadRetryMs, doubling to uploadRetryMaxMs.
    uploadRetryMs @14 :UInt32 = 500;
    uploadRetryMaxMs @15 :UInt32 = 30000;
    # How many parts of a snapshot or segments a rebuild downloads at once (R5: more than four 16 MiB reads at once met
    # errors on a volume server of SeaweedFS 3.97).
    recoverParallelism @16 :UInt32 = 4;
    # How many objects one durable process keeps resident. Past it the least recently used objects are evicted: a
    # hibernatable WebSocket of an evicted object hibernates rather than closes, so the connection stays up and the
    # next message wakes the object on the same storage. 0: no limit.
    residentObjects @17 :UInt32 = 4096;
    # How often a node publishes what its objects hold, per tenant, for the durableObjectBytes quota.
    usagePublishMs @18 :UInt32 = 3600000;
  }

  struct IngressConfig {
    # The open WebSocket connections this node's ingress holds for one tenant: an upgrade beyond them is refused before
    # the Worker runs, 429 WEBSOCKET_LIMIT, while other requests go on as usual. 0: no limit.
    webSocketsPerTenant @0 :UInt32 = 1000;
  }

  struct SecretsConfig {
    # A JSON file {"key":"<64 hexadecimal digits>"} (32 bytes) readable by its owner only; compose's secrets step writes
    # secrets-key.json and gives the control plane and every node a copy. COWORKERD_SECRETS_KEY_FILE overrides it.
    keyFile @0 :Text;
  }

  struct UsageConfig {
    # How often the hours whose counts changed are written (a node that stops loses at most this much).
    flushIntervalMs @0 :UInt32 = 60000;
    # How long the hourly objects are kept; older ones are removed -- by their node once an hour, and by the control plane
    # once a day for the nodes that left. 0 keeps them.
    retentionHours @1 :UInt32 = 744;
    # How often the resident isolates are sampled for the isolate time.
    sampleIntervalMs @2 :UInt32 = 1000;
  }

  struct EgressConfig {
    # on: tenant Workers' fetch() goes through the egress service. off: tenant Workers have no network.
    mode @0 :Text = "on";
    # The destinations a tenant reaches without a rule of its own. public: public addresses on publicPorts. deny:
    # nothing; each tenant's allow list opens what it needs. Neither ever reaches the built-in denials: loopback,
    # link-local, cloud metadata, unspecified and reserved addresses, the cluster's own addresses (its nodes,
    # the object store, clusterAddresses) -- and internal (private) addresses only through a tenant's allowInternal.
    defaultPolicy @1 :Text = "public";
    publicPorts @2 :List(UInt16) = [80, 443];
    # Each request: until the destination's response headers arrive, and then between two reads of either body.
    timeoutMs @3 :UInt32 = 30000;
    # Per tenant and node: requests in flight, and requests per second (a token bucket of that size).
    concurrency @4 :UInt32 = 32;
    requestsPerSecond @5 :UInt32 = 100;
    # How long a resolved name is reused (the system resolver reports no TTL; this is the upper bound). 0: never.
    dnsCacheMs @6 :UInt32 = 10000;
    # PEM certificates trusted besides the system's (systemTrustStore) for HTTPS destinations.
    caFile @7 :Text;
    systemTrustStore @8 :Bool = true;
    # The cluster's addresses the node cannot see on its own (the control plane, a load balancer): HOST[:PORT],
    # an address or a CIDR. Denied like the object store and the members' addresses, whatever the port.
    clusterAddresses @9 :List(Text);
    # Each tenant's rules, by its name (until the tenant records carry them). Entries: a name (api.example.com), a wildcard
    # (*.example.com), an address or CIDR (203.0.113.7, 198.51.100.0/24, [2001:db8::/32]), each with an optional
    # :PORT. deny comes first; allow opens public destinations (any port unless one is named); allowInternal, the
    # administrator's exception, also opens internal (private-network) ones.
    tenants @10 :List(EgressTenant);

    struct EgressTenant {
      tenant @0 :Text;
      allow @1 :List(Text);
      deny @2 :List(Text);
      allowInternal @3 :List(Text);
      # 0: the cluster's value.
      concurrency @4 :UInt32;
      requestsPerSecond @5 :UInt32;
    }
  }

  struct PeerAuthConfig {
    # A JSON file {"keys":[{"id":"k2","secret":"<64-128 hex digits>"},{"id":"k1","secret":"..."}]}, readable by its
    # owner only: the first key signs, both are accepted (rotation). Re-read when it changes.
    keyFile @0 :Text;
    # How far a signature's time may be from this node's clock, and how long its nonce is remembered.
    windowMs @1 :UInt32 = 30000;
  }

  struct SandboxConfig {
    # off: workerd is started directly. launcher: through the launcher; the Server
    # refuses to start when the launcher cannot set up the environment here (its startup check).
    # auto: through the launcher when its startup check passes, otherwise directly, with a warning in
    # the startup log and the state endpoint. COWORKERD_SANDBOX_MODE overrides it.
    mode @0 :Text = "auto";
    # Default: coworkerd-launcher beside coworkerd.
    launcherPath @1 :Text;
    # The system call allowlist, installed by the launcher before it executes workerd.
    syscalls @2 :SyscallsConfig;

    struct SyscallsConfig {
      # The allowlist is the build's: scripts/syscall-allowlist.py generates it from a profile of the
      # workerd of the same package, and it names the workerd it belongs to -- a list that names other
      # bytes than the ones a launch runs is refused by the launcher (a launch failure of that slot).
      #   off      no filter.
      #   observe  a call outside the list is reported to the Server (the [syscall-observed] line and
      #            processes.sandbox.syscalls) and then proceeds, so the process behaves as it does
      #            without the filter. Run this before enforce; see docs/guide/install.md.
      #   enforce  a call outside the list ends the process (exit category syscall-denied).
      # COWORKERD_SANDBOX_SYSCALLS overrides it.
      mode @0 :Text = "observe";
      # Default: coworkerd-syscalls-startup.txt beside coworkerd, as the release package has it.
      # COWORKERD_SANDBOX_ALLOWLIST overrides it.
      allowlistPath @1 :Text;
      # The steady-state table: the narrower list of the same build, which the launcher cannot install
      # (it has to execve workerd, so its own filter must allow execve and the other calls only the startup
      # needs, and a seccomp filter can only ever be narrowed). workerd installs it itself at its first
      # request -- the readiness probe, before it has answered anything -- and becomes non-dumpable in the
      # same step. The modes are the startup table's; a call outside the steady table in `observe` is
      # reported by the process itself ([coworkerd-seccomp] in workerd.log, since by then it has no channel
      # to this node that can carry a notification listener), and in `enforce` it ends the process, which
      # this node reports as the exit category syscall-denied, as for the startup table. Default off: run
      # `observe` over real traffic before `enforce`, as for the startup table (docs/guide/install.md).
      steady @2 :SteadyConfig;

      struct SteadyConfig {
        # off, observe or enforce. COWORKERD_SANDBOX_SYSCALLS_STEADY overrides it.
        mode @0 :Text = "off";
        # Default: coworkerd-syscalls-steady.txt beside coworkerd, as the release package has it.
        # COWORKERD_SANDBOX_STEADY_ALLOWLIST overrides it.
        allowlistPath @1 :Text;
      }
    }
  }

  struct ControlConfig {
    # The control plane's API. Default 0.0.0.0:8940.
    listenAddress @0 :Text;
    # Where an uploaded package is unpacked while its request runs; default $TMPDIR (or /tmp).
    temporaryDirectory @1 :Text;
    # The largest upload accepted (a package: code, static assets and their framing).
    maxUploadBytes @2 :UInt64 = 167772160;
    # How long the control plane waits for each Server's state or logs.
    serverTimeoutMs @3 :UInt32 = 2000;
    # The administrator's credential of /v1; COWORKERD_ADMIN_CREDENTIAL overrides it. 16 to 4096
    # printable characters without spaces. Tenant credentials are created through the API.
    adminCredential @4 :Text;
    # Path routes under the cluster's default host: a tenant may route `*` with a prefix /<tenant> or
    # /<tenant>/...; false: only hostnames the tenant owns.
    pathRouting @5 :Bool = true;
    # How often the control plane reads the desired state's hints again (the writes of another control plane on the same
    # bucket); its own writes are in its mirror as it makes them.
    pollIntervalMs @6 :UInt32 = 5000;
    # The object cleanup of a version's or a tenant's deletion removes only objects last modified longer ago than this
    # (an upload may be about to use an object it found there); younger ones are removed by a later sweep.
    cleanupGraceMs @7 :UInt64 = 3600000;
  }

  struct LogsConfig {
    # The lines kept per tenant (the oldest go first), and over all tenants.
    # A tenant's quota (logBufferBytes) replaces the first.
    tenantBufferBytes @0 :UInt32 = 1048576;
    totalBufferBytes @1 :UInt64 = 67108864;
    # The quota: lines per tenant and second beyond this one are dropped (and counted); a longer
    # line is cut to maxLineBytes. Neither touches logs/workerd.log, which keeps every line. A tenant's
    # quota (logLinesPerSecond) replaces it.
    linesPerSecond @2 :UInt32 = 100;
    maxLineBytes @3 :UInt32 = 4096;
    # logs/workerd.log is renamed to workerd.log.1 (and .1 to .2, ...) once it reaches
    # workerdLogMaxBytes; workerdLogFiles files are kept in all. 0 turns the rotation off.
    workerdLogMaxBytes @4 :UInt64 = 268435456;
    workerdLogFiles @5 :UInt32 = 4;
  }

  struct StorageConfig {
    endpoint @0 :Text;
    bucket @1 :Text = "coworkerd";
    region @2 :Text = "us-east-1";
    # A JSON file {"accessKey":"...","secretKey":"..."} with the Servers' identity: it reads, and with the objectStore
    # membership also lists and writes members/<nodeId>/ and nodes/<nodeId>/ (its usage); without one,
    # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    credentialsFile @3 :Text;
    # The longest a prefetch waits for one object; a single request is bounded by the three timeouts below.
    timeoutMs @4 :UInt32 = 30000;
    # The local package cache; default <stackDirectory>/cache.
    cacheDirectory @5 :Text;
    cacheMaxBytes @6 :UInt64 = 67108864;
    # Every request: its connection (the name resolved and connected) within connectTimeoutMs; its answer, and every
    # read or write after the one before, within readTimeoutMs; a create-if-absent's answer within createTimeoutMs
    # (SeaweedFS 4.48 queues the later writers of one key, about 1 s each). GET, HEAD, listings, DELETE and creates
    # that fail are sent once more on a new connection; a create whose answer was lost is decided by reading it back.
    connectTimeoutMs @7 :UInt32 = 2000;
    readTimeoutMs @8 :UInt32 = 5000;
    createTimeoutMs @9 :UInt32 = 30000;
  }

  struct ManagementConfig {
    # Required as `Authorization: Bearer <token>` on every /__coworkerd/ endpoint but ready and probe;
    # COWORKERD_MANAGEMENT_TOKEN overrides it. A node without one refuses to start.
    token @0 :Text;
    # Development only: start without a token, the endpoints open to whoever reaches the node (warned at startup).
    allowWithoutToken @1 :Bool = false;
  }

  struct Peer {
    nodeId @0 :Text;
    address @1 :Text;
  }

  struct Placement {
    workerId @0 :Text;
    nodeId @1 :Text;
  }
}

struct ProcessPoolConfig {
  count @0 :UInt32 = 1;
  inFlightLimit @1 :UInt32 = 512;
  # Parsed and ignored: each workerd listens on a Unix socket it inherits from the
  # Server, in the stack's run directory, and has no TCP port.
  basePort @2 :UInt32 = 9000;
  probeIntervalMs @3 :UInt32 = 50;
  probeOverdueMs @4 :UInt32 = 400;
  stuckTimeoutMs @5 :UInt32 = 3000;
  restartBackoffMs @6 :UInt32 = 100;
  restartMaxMs @7 :UInt32 = 2000;
  watchdog @8 :Bool = true;
}

struct RotationConfig {
  # How long a previous generation's in-flight calls and request contexts get to finish.
  drainTimeoutMs @0 :UInt32 = 30000;
  # SIGKILL this long after SIGTERM.
  termGraceMs @1 :UInt32 = 5000;
  # How often the workerd executable is checked for a change; 0 turns that trigger off.
  binaryCheckMs @2 :UInt32 = 1000;
  # Age, dispatched-request and resident-memory limits of a generation; 0 turns each off. A generation rotates when
  # its workerd file changes (an upgrade), by hand, and when its resident memory reaches residentBytes -- not with age
  # by default: a process's memory stays bounded by the collections (gc), and a new generation would only rebuild every
  # isolate and its first, largest collection. residentBytes left out of the file: 70% of the node's memory (its
  # container's memory.max, else memoryBudgetBytes, else the machine's).
  maxAgeMs @3 :UInt64 = 0;
  maxRequests @4 :UInt64 = 0;
  residentBytes @5 :UInt64 = 0;
  # First retry after a failed automatic rotation (then doubling).
  retryMs @6 :UInt32 = 5000;
}

struct GcConfig {
  # A Worker's budget, in each process, of collection CPU the CPU limit does not charge to the request it ran in: a
  # token bucket of budgetMs refilled at refillMsPerSecond (a tenant that allocates on purpose gets at most that much,
  # one core's tenth by default); a collection beyond it is charged as before. 0 charges every collection.
  budgetMs @0 :UInt32 = 2000;
  refillMsPerSecond @1 :UInt32 = 100;
  # A collection that pauses its process at least this long writes one gc-pause row (workerd.log's [cpu-limit] lines):
  # which isolate, how long, in whose request, what kind; the state endpoint counts them per process. 0: no rows.
  longPauseMs @2 :UInt32 = 100;
  # A Worker's isolate idle at least idleCollectAfterMs, whose old generation grew at least idleCollectMinGrowthMiB since
  # its last full collection, is collected between its requests -- never while other work is queued. 0 turns it off.
  idleCollectAfterMs @3 :UInt32 = 50;
  idleCollectMinGrowthMiB @4 :UInt32 = 4;
  # V8's initial old generation, for every isolate of a process (--initial-old-space-size): a smaller first full
  # collection. 0 leaves V8's own.
  initialOldSpaceMiB @5 :UInt32 = 8;
  # The runtime's heap (the isolate every call passes through), which V8 otherwise sizes from the machine. 0 leaves
  # V8's own; otherwise at least 16.
  runtimeHeapMiB @6 :UInt32 = 128;
}

# Written by workerd resource enforcement, then matched against live invocations.
struct CpuReport {
  event @0 :Text;
  isolate @1 :Text;
  workerId @2 :Text;
  versionId @3 :Text;
  invocationId @4 :Text;
  cpuUsedNs @5 :UInt64;
  cpuLimitMs @6 :UInt32;
  pid @7 :UInt32;
  memoryLimitMiB @8 :UInt32;
  heapUsedBytes @9 :UInt64;
  externalOwnedBytes @10 :UInt64;
  arrayBufferBytes @11 :UInt64;
  peakUsedBytes @12 :UInt64;
  deniedAllocationBytes @13 :UInt64;
  headroomBytes @14 :UInt64;
  nearHeapCalls @15 :UInt64;
  # The collection CPU the limit did not charge (gc.budgetMs); a request's CPU is cpuUsedNs plus this.
  gcExcludedNs @16 :UInt64;
  # gc-pause rows: the collection's kind (mark-compact, scavenge, ...), its pause, what its Worker's budget held after
  # it, whether it ran inside a request (whose identity the row carries) or was an idle collection, and which full
  # collection of its isolate it was (1: the first).
  gcType @17 :Text;
  pauseNs @18 :UInt64;
  gcBudgetNs @19 :UInt64;
  inRequest @20 :Bool;
  idleCollection @21 :Bool;
  majorOrdinal @22 :UInt64;
  # When the collection ended (gc-pause rows).
  observedAtUnixMs @23 :UInt64;
}

struct PlacementConfig {
  rootLimit @0 :UInt32 = 128;
  nestedLimit @1 :UInt32 = 384;
  idleMs @2 :UInt32 = 30000;
  homeProbability @3 :Float64 = 0.05;
  avoidMaxMs @4 :UInt32 = 2000;
  rejectionAvoidMs @5 :UInt32 = 100;
  connectBackoffMs @6 :UInt32 = 100;
  loadBandForTest @7 :Text = "green";
  equalLimitsForTest @8 :Bool = false;
  # Where a stateless subcall executes -- a Service Binding call, by `fetch` or by a method: "home" (the default) sends
  # it to the Worker's home as any call of its own would go, "local" runs it on the calling node, as Cloudflare's
  # Service Bindings do. The choice is a policy point with the caller's node as its hint, so that a mixed rule (a
  # Worker's own preference, say) can replace it; which default is right waits on an R-line measurement
  # (docs/s3-truth-and-durable-objects.md, section 9).
  subcalls @9 :Text = "home";
}

struct MembershipConfig {
  # "file" (static peers) or "objectStore" (heartbeat objects and epoch registrations under members/ in the bucket of the
  # storage block -- or of desiredState.objectStore -- which it needs; written with the node's storage identity; the
  # default when the file has the storage block). The suspects ride it: objectStore keeps them beside the heartbeats.
  backend @0 :Text = "file";
  # Unused since 0.4; accepted.
  endpoint @1 :Text;
  advertisedAddress @2 :Text;
  # objectStore: how old a heartbeat may be before its identity is free again (and, with
  # livenessGraceMs, before the other members drop it); a node that wrote none for that long by its own monotonic
  # clock places calls on itself only when no other member takes them, keeps serving the stateless calls addressed to
  # any epoch it held, and registers again with a new epoch.
  ttlSeconds @3 :UInt32 = 15;
  requestTimeoutMs @4 :UInt32 = 750;
  startupTimeoutMs @5 :UInt32 = 15000;
  retryMs @6 :UInt32 = 250;
  # objectStore only. The heartbeat's interval (less than half of ttlSeconds), how often the members are listed, the
  # grace beyond the TTL before a member whose heartbeat stopped is dropped (at least the heartbeat interval), the
  # largest difference between this machine's clock and the store's Date header (more refuses the start; at runtime it
  # is checked every minute and only warned of), and how long a new epoch's registration settles before it is read back
  # (a registration that reads back another process's is lost).
  heartbeatIntervalMs @7 :UInt32 = 5000;
  listIntervalMs @8 :UInt32 = 10000;
  livenessGraceMs @9 :UInt32 = 10000;
  clockSkewToleranceMs @10 :UInt32 = 2000;
  createSettleMs @11 :UInt32 = 1000;
}

struct DesiredStateConfig {
  # "file" (a manifest file of the stack, the positional form) or "objectStore" (the tenant index and the tenants'
  # manifests in the bucket of the storage block, which `storage` sets): the node reads the index's hint every
  # pollIntervalMs, and the index and the manifests it names when they change.
  backend @0 :Text = "file";
  # Unused since 0.4; accepted.
  endpoint @1 :Text;
  codeDirectory @2 :Text;
  requestTimeoutMs @3 :UInt32 = 750;
  startupTimeoutMs @4 :UInt32 = 15000;
  retryMs @5 :UInt32 = 250;
  versionResolutionForTest @6 :Text;
  deploymentApplyDelayMsForTest @7 :UInt32 = 0;
  objectStore @8 :ObjectStoreConfig;
  cache @9 :BundleCacheConfig;
  # objectStore: how often the tenant index's hint is read (one GET; a changed one reads the index and the manifests it
  # names anew).
  pollIntervalMs @10 :UInt32 = 5000;
}

struct ObjectStoreConfig {
  backend @0 :Text = "s3";
  directory @1 :Text;
  endpoint @2 :Text;
  bucket @3 :Text = "coworkerd";
  region @4 :Text = "us-east-1";
  accessKey @5 :Text;
  secretKey @6 :Text;
  # As StorageConfig's.
  timeoutMs @7 :UInt32 = 30000;
  connectTimeoutMs @8 :UInt32 = 2000;
  readTimeoutMs @9 :UInt32 = 5000;
  createTimeoutMs @10 :UInt32 = 30000;
}

struct BundleCacheConfig {
  directory @0 :Text;
  maxBytes @1 :UInt64 = 67108864;
  prefetchConcurrency @2 :UInt32 = 2;
}

struct VersionPin {
  workerId @0 :Text;
  versionId @1 :Text;
  contentHash @2 :Text;
  definitionHash @3 :Text;
}

# A version's Durable Object namespace Binding (wrangler's durable_objects.bindings): its name in env and one of the
# version's own classes.
struct DurableObjectBinding {
  binding @0 :Text;
  className @1 :Text;
  # A d1_databases Binding: the tenant's database it names. Its class is then the platform's own SQL Worker class,
  # which no version declares, and its namespace comes from the tenant's databases.
  database @2 :Text;
}

# The Durable Object classes a version's migrations leave, applied in order: those declared (every class is a SQLite
# class), those deleted, and every rename from one class to another.
struct DurableObjectClasses {
  declared @0 :List(Text);
  deleted @1 :List(Text);
  renamed @2 :List(Rename);

  struct Rename {
    from @0 :Text;
    to @1 :Text;
  }
}

struct DeploymentDefinition {
  workerId @0 :Text;
  versionId @1 :Text;
  compatibilityDate @2 :Text;
  mainModule @3 :Text;
  contentHash @4 :Text;
  definitionHash @5 :Text;
  bindings @6 :List(Manifest.Binding);
  format @7 :Text = "v1";
  compatibilityFlags @8 :List(Text);
  variables @9 :List(EnvironmentVariable);
  assets @10 :DeploymentAssets;
  # Names of the secrets the version reads from env (v2 only); their values are
  # set separately under secrets/<workerId>/<name> and never part of a definition.
  secrets @11 :List(Text);
  # The version's Durable Object Bindings and classes (v2 only).
  durableObjects @12 :List(DurableObjectBinding);
  classes @13 :DurableObjectClasses;
}

# The value of a secret's deployment key secrets/<workerId>/<name>: the secret string and when it
# was set.
struct SecretRecord {
  value @0 :Text;
  setAtUnixMs @1 :Float64;
}

# Present when the version's package carries static assets (v2 only).
struct DeploymentAssets {
  notFoundHandling @0 :Text;
  # run_worker_first: true sends every request to the Worker first; a rule
  # list (Cloudflare's patterns, "!" excludes) sends the matching paths first.
  runWorkerFirst @1 :Bool;
  runWorkerFirstRules @2 :List(Text);
}

struct EnvironmentVariable {
  name @0 :Text;
  type @1 :Text = "text";
  value @2 :Text;
}

struct ActiveVersion {
  versionId @0 :Text;
  definitionHash @1 :Text;
  # The Worker's own limits, set through the API; absent or 0: the tenant's default, then the node's.
  limits @2 :WorkerLimits;
}

struct WorkerLimits {
  cpuMs @0 :UInt32;
  memoryMiB @1 :UInt32;
}

# The value of a route's deployment key routes/<host><pathPrefix>.
struct RouteRecord {
  host @0 :Text;
  pathPrefix @1 :Text;
  tenant @2 :Text;
  worker @3 :Text;
}

struct Manifest {
  compatibilityDate @0 :Text;
  routes @1 :List(Route);
  workers @2 :List(Worker);

  struct Route {
    host @0 :Text;
    pathPrefix @1 :Text;
    workerId @2 :Text;
  }

  struct Worker {
    workerId @0 :Text;
    activeVersion @1 :Text;
    versions @2 :List(Version);
  }

  struct Version {
    versionId @0 :Text;
    mainModule @1 :Text;
    bindings @2 :List(Binding);
    contentHash @3 :Text;
    definitionHash @4 :Text;
    format @5 :Text = "v1";
    compatibilityDate @6 :Text;
    compatibilityFlags @7 :List(Text);
    variables @8 :List(EnvironmentVariable);
    packagePath @9 :Text;
    assets @10 :DeploymentAssets;
    secrets @11 :List(Text);
    durableObjects @12 :List(DurableObjectBinding);
    classes @13 :DurableObjectClasses;
  }

  struct Binding {
    name @0 :Text;
    workerId @1 :Text;
    entrypoint @2 :Text;
    # The version's own asset layer (assets.binding); names no Worker.
    assets @3 :Bool;
  }
}

struct InvocationDescriptor {
  workerId @0 :Text;
  versionId @1 :Text;
  compatibilityDate @2 :Text;
  mainModule @3 :Text;
  contentHash @4 :Text;
  bindings @5 :List(Binding);
  invocationId @6 :Text;
  treeId @7 :Text;
  lease @8 :InvocationLease;
  orderSequence @9 :UInt64;
  depth @10 :UInt16;
  requiresChildSequence @11 :Bool;
  definitionHash @12 :Text;
  serverId @13 :Text;
  processId @14 :Text;
  processGeneration @15 :UInt64;
  ingressServerId @16 :Text;
  format @17 :Text = "v1";
  compatibilityFlags @18 :List(Text);
  variables @19 :List(EnvironmentVariable);
  entrypoint @20 :Text;
  # The version's secrets with the values of the executing Server's deployment
  # view; the runtime merges them into env when it loads the Worker.
  secrets @21 :List(Secret);
  # A trial load at deployment: the runtime loads the version as for a call
  # and reports its exports, calling no handler. No Binding and no lease.
  trial @22 :Bool;
  # With `trial`: the load check before a call's dispatch, for the entrypoint
  # the call names (`entrypoint`); the source is fetched for `sourceInvocationId`,
  # the call it precedes.
  loadCheck @23 :Bool;
  sourceInvocationId @24 :Text;
  # The load check before this dispatch loaded the version: the call's cold start.
  coldLoaded @25 :Bool;
  # The limits this Worker names: its own, else its tenant's default; 0 means this node's own value
  # applies. The runtime bridge puts them in the loader key, so the
  # version's isolate is created under them (and a change of them loads another isolate), and workerd's CPU
  # and memory limits take them from there. The ceiling against the tenant's maximum is the control plane's.
  cpuLimitMs @26 :UInt32;
  memoryLimitMiB @27 :UInt32;
  # The version's Durable Object namespace Bindings: each one's name, class and namespace id (empty while the namespace
  # cannot be used: none for the class, or being deleted).
  durableObjects @28 :List(DurableObjectNamespace);
  # A request to an object, which the runtime of a durable process hands to the object's Host: the object's namespace,
  # id and class, and how many Durable Object calls led to it (0 from a stateless Worker).
  durableObject @29 :DurableObjectTarget;
  # A trial load (and a load check): the Durable Object classes the version declares, checked among its exports.
  durableObjectClasses @30 :List(Text);

  struct Binding {
    name @0 :Text;
    workerId @1 :Text;
    versionId @2 :Text;
    contentHash @3 :Text;
    definitionHash @4 :Text;
    entrypoint @5 :Text;
    assets @6 :Bool;
  }

  struct Secret {
    name @0 :Text;
    value @1 :Text;
  }

  struct DurableObjectNamespace {
    name @0 :Text;
    className @1 :Text;
    namespaceId @2 :Text;
    # A d1_databases Binding: the tenant's database it names and the id of the one object that database is, so the
    # runtime needs no hash of its own for it.
    database @3 :Text;
    objectId @4 :Text;
  }

  struct DurableObjectTarget {
    namespaceId @0 :Text;
    objectId @1 :Text;
    className @2 :Text;
    hops @3 :UInt16;
    # The object's shard, the shard's epoch this node holds it at, and the epoch since which this node's copy of the
    # shard's objects is the one: the object's Host is named by it, so that a shard this node takes over from another
    # node starts its objects from empty while one it takes back from itself keeps them.
    shard @4 :UInt32;
    shardEpoch @5 :UInt64;
    dataEpoch @6 :UInt64;
    # A tenant's database (the D1 shape): its name. The class is then the platform's own SQL Worker class, which the
    # Host takes from its SQL_CLASS Binding and runs as the object's facet -- no tenant code is loaded.
    database @7 :Text;
    # This request is the object's alarm, scheduled for this time (Unix milliseconds): the object's class runs its
    # alarm() handler, and the answer says what its own database held.
    alarmAtUnixMs @8 :UInt64;
    # How many times its handler has already been tried (alarmInfo.retryCount).
    alarmRetryCount @9 :UInt32;
  }
}

struct InvocationLease {
  remainingSubrequests @0 :UInt16;
  grant @1 :UInt16;
  expiresAtMonotonicMs @2 :Float64;
}

struct BindingDescriptor {
  workerId @0 :Text;
  versionId @1 :Text;
  contentHash @2 :Text;
  treeId @3 :Text;
  parentInvocationId @4 :Text;
  callSequence @5 :UInt64;
  orderSequence @6 :UInt64;
  expiresAtMonotonicMs @7 :Float64;
  sequenceMapSize @8 :UInt64;
  definitionHash @9 :Text;
  entrypoint @10 :Text;
  # A call of the ASSETS Binding: answered by the asset layer of this version.
  assets @11 :Bool;
  # A call to a Durable Object of this Worker: its namespace, class and object id; from an object, that object's
  # namespace and id (a stateless Worker's call names its call tree instead).
  durableObject @12 :DurableObjectCall;

  struct DurableObjectCall {
    namespaceId @0 :Text;
    className @1 :Text;
    objectId @2 :Text;
    callerNamespaceId @3 :Text;
    callerObjectId @4 :Text;
    # A d1_databases Binding's call: the tenant's database it names. The class is then the platform's own SQL Worker
    # class, which no version declares.
    database @5 :Text;
  }
}

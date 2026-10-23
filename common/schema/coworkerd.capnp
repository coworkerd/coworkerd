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
  unused9 @9 ();
}

interface WorkerdBootstrap @0x878048cda334ade3 {
  startEvent @0 (cfBlobJson :Text, fromPersistentStub :Bool)
      -> (dispatcher :EventDispatcher);
}

interface PeerServer @0xab4d6df924ec776f {
  openInvocation @0 (invocation :RemoteInvocation)
      -> (result :InvocationOpenResult);
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

struct ServerConfig {
  # The node's id: its member key /coworkerd/v2/nodes/<nodeId>, its etcd
  # user's prefix and every record it writes.
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
  # names serverId, etcdEndpoint and storage.endpoint; every other field has a default. The
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
  # The deployment state in etcd, and the membership too unless membership.backend says objectStore (sets the
  # backends to etcd; membership.backend may name objectStore beside it).
  etcdEndpoint @29 :Text;
  # What other Servers dial for this one (membership.advertisedAddress); default
  # <hostname>:<port of peerListenAddress>, whose default is 0.0.0.0:8934 in serve.
  advertisedAddress @30 :Text;
  # The object store of the code packages (sets desiredState.objectStore and its cache), and of the members when
  # membership.backend is objectStore.
  storage @31 :StorageConfig;
  # The management endpoints (/__coworkerd/ except ready and probe); COWORKERD_MANAGEMENT_TOKEN
  # overrides the token.
  management @32 :ManagementConfig;
  # The control plane: `coworkerd-apiserver --config FILE` reads the same file -- etcd
  # (etcdEndpoint), the object store with the deploy identity (storage), the management token
  # (management) -- and this.
  control @33 :ControlConfig;
  # The Workers' logs: each Server's in-memory log of every Worker, the tenant log quota, and
  # the size rotation of logs/workerd.log.
  logs @34 :LogsConfig;
  # The workerd processes' least-privilege environment: each workerd started through
  # coworkerd-launcher (launcher/PROTOCOL.md).
  sandbox @35 :SandboxConfig;
  # Per-tenant usage: counted on each node and rolled up per hour under its own etcd prefix,
  # /coworkerd/v2/nodes/<nodeId>/usage/<hour>/<tenant>.
  usage @36 :UsageConfig;
  # Tenant Workers' outbound requests (docs/egress-and-auth.md): a tenant's fetch() reaches this node's egress
  # service through <stack>/run/egress.sock; the node checks the destination against the policy, resolves the name and
  # connects itself (TLS included), meters the bytes per tenant and answers. The tenant's workerd process still has
  # no network of its own.
  egress @37 :EgressConfig;
  # The peer listener's authentication: every node signs its peer connections with the first key of a cluster
  # key file and accepts either of its keys. Unset: peer connections are not authenticated (a startup warning).
  peerAuth @38 :PeerAuthConfig;
  # etcd's authentication: a JSON file {"user":"...","password":"..."} readable by its owner only; unset, the
  # requests carry no credentials. COWORKERD_ETCD_CREDENTIALS_FILE overrides it.
  etcdCredentialsFile @39 :Text;

  struct UsageConfig {
    # How often the current hour's counts are written (a node that stops loses at most this much).
    flushIntervalMs @0 :UInt32 = 60000;
    # How long a node keeps its own hourly rows; older ones are deleted. 0 keeps them.
    retentionHours @1 :UInt32 = 744;
    # How often the resident isolates are sampled for the isolate time.
    sampleIntervalMs @2 :UInt32 = 1000;
  }

  struct EgressConfig {
    # on: tenant Workers' fetch() goes through the egress service. off: tenant Workers have no network.
    mode @0 :Text = "on";
    # The destinations a tenant reaches without a rule of its own. public: public addresses on publicPorts. deny:
    # nothing; each tenant's allow list opens what it needs. Neither ever reaches the built-in denials: loopback,
    # link-local, cloud metadata, unspecified and reserved addresses, the cluster's own addresses (its nodes, etcd,
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
    # an address or a CIDR. Denied like etcd, the object store and the members' addresses, whatever the port.
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
    # membership also lists and writes members/<nodeId>/; without one, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    credentialsFile @3 :Text;
    timeoutMs @4 :UInt32 = 30000;
    # The local package cache; default <stackDirectory>/cache.
    cacheDirectory @5 :Text;
    cacheMaxBytes @6 :UInt64 = 67108864;
  }

  struct ManagementConfig {
    # Required as `Authorization: Bearer <token>`; unset keeps the endpoints open.
    token @0 :Text;
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
  # Age, dispatched-request and resident-memory limits of a generation; 0 turns each off. A
  # generation rotates after an hour by default (resident memory grows between rotations).
  maxAgeMs @3 :UInt64 = 3600000;
  maxRequests @4 :UInt64 = 0;
  residentBytes @5 :UInt64 = 0;
  # First retry after a failed automatic rotation (then doubling).
  retryMs @6 :UInt32 = 5000;
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
}

struct MembershipConfig {
  # "file" (static peers), "etcd" (a key per node on an etcd lease, what etcdEndpoint sets) or "objectStore" (heartbeat
  # objects and epoch registrations under members/ in the bucket of the storage block, which it needs; written with
  # the node's storage identity).
  backend @0 :Text = "file";
  endpoint @1 :Text;
  advertisedAddress @2 :Text;
  # etcd: the lease's TTL. objectStore: how old a heartbeat may be before its identity is free again (and, with
  # livenessGraceMs, before the other members drop it); a node that wrote none for that long by its own monotonic
  # clock takes no calls from other nodes and registers again with a new epoch.
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
  backend @0 :Text = "file";
  endpoint @1 :Text;
  codeDirectory @2 :Text;
  requestTimeoutMs @3 :UInt32 = 750;
  startupTimeoutMs @4 :UInt32 = 15000;
  retryMs @5 :UInt32 = 250;
  versionResolutionForTest @6 :Text;
  deploymentApplyDelayMsForTest @7 :UInt32 = 0;
  objectStore @8 :ObjectStoreConfig;
  cache @9 :BundleCacheConfig;
}

struct ObjectStoreConfig {
  backend @0 :Text = "s3";
  directory @1 :Text;
  endpoint @2 :Text;
  bucket @3 :Text = "coworkerd";
  region @4 :Text = "us-east-1";
  accessKey @5 :Text;
  secretKey @6 :Text;
  timeoutMs @7 :UInt32 = 30000;
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
}

# The etcd value of secrets/<workerId>/<name>: the secret string and when the
# deployment tool set it.
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

# The etcd value of a route: /coworkerd/v2/deploy/routes/<host><pathPrefix>.
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
}

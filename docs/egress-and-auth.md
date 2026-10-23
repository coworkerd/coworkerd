# Egress and authentication

This document is for operators and security reviewers. It describes how tenant code reaches the network -- through one policy point per node, with built-in refusals, pinned resolution, per-tenant rules, limits and metering -- and how the components authenticate each other: the signed peer connection, etcd's users and roles, two object storage identities, the management token and the `/v1` credentials. The last section lists what is not done yet. The configuration keys and their defaults are in the [installation guide](guide/install.md), section 5, and its sections 11 and 12 describe egress and the cluster key; the errors tenant code sees are in [error codes](guide/errors.md), section 4.

## 1. What has to hold

- Tenant code must never reach the cluster's own services -- etcd, object storage, the peer and management ports, the control plane -- whatever address or name it uses.
- Tenant code reaches the rest of the network only through a point that can apply the platform's policy and meter it per tenant.
- Every channel between components has a credential of its own, so being on the cluster's network is not enough to use it -- though the object store may serve more than its S3 interface (section 6). The cluster key and the management token are in force only when they are configured: without a key file or a token the channel is open, and the node warns at startup. etcd's users protect etcd once its authentication has been turned on; from then on a component without its credentials file is refused, while an etcd without authentication is open to anyone who reaches it. Compose sets all of this up. The admin credential and the object storage credentials are required: without them the component does not start.

Egress and authentication depend on each other: opening the network to tenants without authenticated back ends would let tenant code talk to them directly.

## 2. The policy point

A tenant isolate's global outbound -- where its `fetch()` goes -- is an entry point of the runtime bridge, bound when the isolate loads to the Worker's full name and version taken from the call, never from tenant code. It forwards the request to the node's egress service on the Unix socket `<stack>/run/egress.sock`, adding a header with its process's egress token; the node opens the connection itself, TLS included, and relays the answer.

- **Attribution.** Every workerd generation has its own random egress token. The node accepts a request only with a live process's token and a valid Worker name, and the tenant is the first part of that name; anything else is refused as `unknown-source`.
- **No identity leaves the node.** The node adds no header of its own and drops its egress headers and the hop-by-hop ones in both directions; a platform service that needs to recognise a tenant uses the tenant's own credentials.
- **No other way out.** With the sandbox active a workerd process has no usable network: its own network namespace has no route and only a loopback device, which is down (a kernel with tunnel drivers loaded adds their inert devices); without it, the only route out for tenant code is still this entry point, because that is how workerd is configured, not because the process could not reach the network.
- `egress.mode` `off` keeps the policy point but refuses every request (`EGRESS_DENIED: egress-off`): an isolate always has an outbound, so a throw after `passThroughOnException` fails that request alone instead of ending the shared workerd process.

## 3. Policy, limits and metering

### Destinations

**Refused whatever a rule says**: the cluster's own addresses on any port; the cloud metadata addresses on the node's list (`169.254.169.254`, `169.254.170.2`, `169.254.169.253`, `100.100.100.200`, `168.63.129.16`, `fd00:ec2::254`, `fd00:ec2::23`); unspecified, loopback and link-local addresses; the reserved ranges on its list -- documentation ranges, multicast and the rest of IPv6 outside the global unicast space among them. Another cloud's IPv6 metadata address in the unique-local range counts as a private network. IPv4 addresses inside IPv6 forms (IPv4-mapped, NAT64's well-known prefix `64:ff9b::/96`, 6to4) are judged as the IPv4 address.

**The cluster's own addresses** are every interface address of the node, its listen, peer and advertised addresses, every member's advertised address, the etcd and object storage endpoints, the addresses every control plane announces to the nodes (`POST /__coworkerd/control-plane`, every 10 s, forgotten after 10 minutes without an announcement), and `egress.clusterAddresses` -- addresses, CIDRs or names, for the control plane, load balancers and networks a node cannot see by itself. The set is rebuilt every 5 seconds and whenever the membership changes.

**Private networks** (RFC 1918, 100.64.0.0/10, 198.18.0.0/15, IPv6 ULA and site-local, and the local-use NAT64 prefix `64:ff9b:1::/48`) are refused unless the administrator lists the destination in the tenant's `allowInternal`: the platform's own database or services, a decision tenants cannot make. The local-use NAT64 prefix is judged as a whole, so an `allowInternal` entry that covers it opens it whatever IPv4 address it carries.

**The rules**, per address: the tenant's `deny` first; then a private address needs `allowInternal`; a public address passes when `allow` or `allowInternal` names it; otherwise the default policy decides -- `public` (the default) allows the ports in `egress.publicPorts` (80 and 443), `deny` nothing. An entry is a name, a wildcard name, an address or a CIDR, with an optional port; names apply only when the URL names a host, addresses and CIDRs to every resolved address, as it is: an IPv4 entry does not match the same address written in an IPv6 form, so a `deny` entry for an IPv4 address does not stop a request to that address in its IPv4-mapped form. Rules and limits are per tenant (`egress.tenants[]`, in the node's configuration), shared by all its Workers.

**Resolution and pinning.** The node resolves names itself and checks **every** address of the answer; it connects only to checked addresses, never resolving the name again for the connection. A resolution is reused for at most `egress.dnsCacheMs` (10 s) -- a fixed bound, since the system resolver reports no TTL -- so a name later pointed at a private address is refused at the next resolution. The node follows no redirect: tenant code's `fetch` follows each hop as a new request through the policy point, checked again.

**Protocols.** HTTP and HTTPS, each request on a connection of its own, and WebSocket, relayed message by message. The node verifies HTTPS destinations against the system's root certificates (unless `egress.systemTrustStore` is false) and the PEM file `egress.caFile`.

### Limits and metering

- Per tenant and node: `egress.concurrency` requests in flight (32) and `egress.requestsPerSecond` (100, a token bucket); beyond them `EGRESS_DENIED: concurrency-limit` or `rate-limit`, without affecting other tenants. A request counts as in flight until its body or WebSocket session ends.
- `egress.timeoutMs` (30 s) bounds resolution, connecting, TLS and the response headers together, and then each read of either body; WebSocket messages have no timeout.
- The bytes of request and response bodies, and of WebSocket messages, count into the tenant's usage as egress bytes.
- Every request has an `[egress]` line in the node's log -- tenant, Worker, host, port, address, result, rule, status, bytes, duration -- outside the tenant's log; refusals for concurrency and rate are logged at most once per second per tenant, while the counters miss none.
- A refusal is an exception thrown by `fetch`, whose message is the code and the rule or class only (`EGRESS_DENIED: private-network`, `EGRESS_FAILED: dns`), never an address or a name inside the cluster.

## 4. Peer connections: the cluster key

A node calls another over one Cap'n Proto session per peer, opened by an HTTP CONNECT to the peer listener; the node signs that CONNECT with the first key of the cluster key file (`peerAuth.keyFile`): HMAC-SHA256 over the method, the path, the target node's id, a timestamp, a nonce and the body's hash. The calls inside the session are not signed one by one. The listener accepts the session only with a known key, its own id as the target, a timestamp within `peerAuth.windowMs` (30 s) of its clock, a valid signature and a nonce it has not seen; anything else gets 401 and a `[peer-auth]` line with the reason (`unknown-key`, `wrong-target`, `expired`, `replayed` and others). A CONNECT that names another host than `coworkerd-peer` fails before the check.

- **Rotation without interruption.** The file holds one or two keys: the first signs, both are accepted. Adding a key second, making it the first and removing the other -- each step on every node before the next -- changes the key without failing a call, except a call still running, one window after the removal, on a session opened with the old key; compose's `secrets` service has a command for each step.
- **Reloading.** The node checks the file every second and rereads it when it changes; when the new file cannot be read, it keeps the keys it has. A new signing key makes the node open new connections; sessions established with a key that left the file are closed one window later.
- **The file must be private**: a key file with any group or other permission stops the node at its start.
- Clocks of all nodes have to agree within the window (NTP).
- Without a key file, peer connections are not authenticated and the node warns at startup.

The same listener also serves `POST /__coworkerd/trial-load`, `GET /__coworkerd/logs` and `GET /__coworkerd/state` over plain HTTP, because the control plane reaches every node at its advertised address for trial loads, logs and state. Those requests are not signed: the management token protects them (section 7).

Signing keeps the peer session closed to anything that can reach the port but does not hold the key, tenant code included. It is not mTLS: every node holds the same key, so a node that has it can act as any node.

## 5. etcd: users and roles

etcd runs with authentication on, and every component logs in as its own user: `coworkerd-apiserver` for the control plane and its tool, `coworkerd-node-<nodeId>` for each node, `coworkerd-health` with no permission at all for compose's health check, and `root` only for the tool below, never for a daemon. The roles behind them -- the control plane writes the desired state, a node reads it and writes only its own keys, no node reads credentials -- are in [cluster state](cluster-state.md), section 2.

- One command, `coworkerd-apiserver tool etcd-auth`, brings etcd's users and roles to this plan, and can run again: each time it adds what is missing, sets the planned users' passwords from their files -- root's only on a run that finds authentication off, and `coworkerd-health`'s fixed -- and revokes the permissions a role of the plan holds beyond it; it never takes a role away from a user, and never deletes users or roles outside the plan, such as those of a node no longer listed. Compose's `init` runs it at every start.
- With authentication on, an anonymous request reads and writes nothing; the tool checks that an anonymous read is refused after turning it on.
- Credentials are files (`etcdCredentialsFile`, `{"user","password"}`) readable only by their owner. When etcd refuses the token of a request, the component authenticates again, rereading the file if it changed. A watch refused for its token only drops it, and the next authentication uses the password in memory: when a watch is the first to meet a changed password, that authentication fails, and the component cannot authenticate again until it restarts.
- etcd is reached through its plain-HTTP gateway: passwords and tokens, and everything else, cross the network unencrypted, so etcd belongs on a network only the cluster can reach.

## 6. Object storage: two identities

Requests to object storage are signed with AWS Signature Version 4, over plain HTTP. The control plane holds the read-write identity that uploads packages; nodes only read, so a read-only identity is enough for them, and compose gives them one. What keeps code intact is its hash rather than the identities: every object a node reads is checked against its hash, so a changed object is refused, never run. Compose runs SeaweedFS's whole server, listening on every address of its container, with no security configuration of its own. Reads are signed as query strings valid for 60 seconds, because SeaweedFS asks for write permission when it checks a header signature on a read; such a URL names the access key, though never the secret. `coworkerd check storage` checks a store before use: it creates the bucket, writes a probe object, reads it back with the read identity and expects unsigned requests to be refused; it does not prove that the read identity cannot write.

## 7. The management token and `/v1` credentials

- **The management token** protects everything under `/__coworkerd/` on both listeners of a node except `ready` and `probe`: state, deployment, placement, drain, rotation, trial loads, logs. The control plane sends it to read state and logs and for trial loads, and the operations tool for trial loads, drains and rotations; nodes do not use it among themselves, their sessions are signed. A request without it gets 401 and a `[management]` line that never contains the token; without a configured token the endpoints are open and the node warns at startup.
- **`/v1` credentials**: the admin credential, configured for the control plane, can do everything; tenant credentials are issued through `/v1`, shown once, stored only as hashes, can expire and be revoked, and act only on their own tenant ([control plane API](control-plane-api.md), section 3). A refused request gets 401 or 403 and a `[control]` line, without the credential.
- **Ingress traffic is not authenticated**: tenants' users reach Workers through the platform's own gateway in front of the nodes.

## 8. Not done yet

- **mTLS between nodes and a procedure for a node to join**: today a node is a member when it holds the cluster key and its own etcd user.
- **Encryption of secret values at rest**: Worker secrets are stored in etcd as plain values, readable by the control plane and by every node, whatever Workers it runs, and they remain in etcd's history until it is compacted, which compose does not do by itself.
- **TLS to etcd and object storage**, between the control plane and the nodes, and for `/v1` itself, which is plain HTTP unless something in front of the control plane terminates TLS.
- **Per-tenant egress rules from the control plane**: the rules are part of each node's configuration.

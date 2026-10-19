# Architecture overview

- **Status**: 0.3.0 released -- a multi-node cluster, tenants and /v1, an egress policy and authentication, each workerd running in a least-privilege environment with a system call allowlist; the "Server" of this text is called a node (`coworkerd`) in 0.3.0. The body below is the project's original design narrative; the current state of each question is in the other design documents

This document covers only **the parts that change most slowly**: the project's boundaries, the core concepts, the execution model.
It does not pre-empt the trade-offs of specific mechanisms.

## 1. Project boundaries

Coworkerd is a **single-region** distributed Worker execution plane based on workerd. It presents several Servers and several
workerd processes to the layer above as one uniform Worker execution environment.

**Does:** resolve requests to a specific Worker version, decide within the cluster where to execute, load and execute dynamically,
keep call semantics across machines, present the cluster as one execution environment.

**Does not:** users and organizations, product-level release management, billing, consoles, CI/CD. These belong to systems built on top of
Coworkerd. Coworkerd only takes the runtime configuration that execution needs.

**Out of scope:** clusters across regions, routing and failover across regions. A cluster is confined to one region or an equivalent low-latency internal network.

**Also out of scope:** location-bound stateful resources (Durable Object, D1, KV).
Coworkerd schedules only stateless calls; stateful services are owned by the layer above and accessed as external services.
But "stateless" does not mean "never bound to a location".

## 1.0 Positioning and consumers

**Coworkerd is a fairly low-level distributed Worker runtime foundation, not an end product.**
It concentrates on two things, **Worker runtime & cluster**, aiming to stay as close as possible to Cloudflare's architecture --
**natively distributed scheduling**.

### Consumers

Not one particular product but a class: agentic platforms, app platforms, workflow runtimes,
low-code engines, vibe platforms, throwaway apps and the like. They build their own product capabilities on top of Coworkerd
and reuse the existing ecosystem of Cloudflare Workers.

**So the configuration contract is a first-class public interface**, not an internal detail: how routes, versions and Binding plans enter
Coworkerd has to be a stable convention that different upper-layer systems can integrate with.

### Expected scale

**Millions of users, millions of Workers.** This is a target to optimize for later,
but it already constrains the architecture's direction.

### The foundation must be able to run on its own

Pushing the source of configuration as a whole onto "upper-layer systems" would be too narrow. The split is:

| | Belongs to |
| --- | --- |
| Distribution and storage of Worker bundles | **Coworkerd** (the foundation itself needs them) |
| Cluster membership and leases | **Coworkerd** |
| Receiving versions, routes and Binding plans and putting them into effect | **Coworkerd** |
| Users and organizations, product-level release processes, billing, consoles, CI/CD | Upper-layer systems |
| Stateful resources such as Durable Objects | **Not done**; the layer above brings its own |

Coworkerd does not build a complete control plane, but it **must be able to run by itself**,
and may depend on external infrastructure (such as some coordination service and an artifact repository); the specific choices are open.

## 1.1 Trust model

**Untrusted multi-tenant code, with different tenants' Workers running in the same workerd process.**
That is the Cloudflare Workers model.

- **The V8 isolate is the boundary between tenants.** One workerd process hosts several tenants.
- **The outer sandbox wraps the workerd process** (protecting the host), not one per tenant.
  The upstream README's "run it inside an appropriate secure sandbox, such as a virtual
  machine" refers to the workerd process, not to each tenant.
- **One Server manages several workerd processes**, but not divided by tenant -- by fault domain and capacity,
  plus a separate quarantine pool of processes that takes suspicious Workers.
- **The execution location has two levels**: first choose the Server, then the workerd process within that Server.
- **Resource limits must be per isolate.** A process-level cgroup is not enough, because a process holds several tenants.

**This adds a project responsibility:** the defensive layers that support multiple tenants within a process -- frozen timers,
no multithreading or shared memory, dynamic process isolation, memory reshuffling/periodic restarts, a host sandbox --
**are not in open-source workerd**, and Coworkerd has to build them itself.
Cloudflare is honest about this: isolates do not reach process-level isolation; the aim is to make attacks too slow to be practical.
Adopting the same model means accepting the same security argument.

## 2. Core concepts

**Worker and `workerId`** -- the logical unit that Routes, releases and Bindings all point to.
`workerId` is the stable internal identity and also the **shard key**.

**Worker Version and `versionId`** -- an immutable snapshot: code, static assets, Bindings,
compatibility settings. `versionId` is the identity of **the thing actually executed**. State in external resources does not belong to the version.
There is no second concept of a version reference.

**Deployment** -- decides which versions of a logical Worker are active and how a call chooses among them.
Once the choice is made, what executes is one definite `versionId`. The product-side release process is outside Coworkerd,
but its runtime state is an input to execution.

**Route** -- maps the Host / Path and similar information of a top-level HTTP request to a logical Worker.

**Binding** -- a capability granted to a Worker Version. A Service Binding points to another logical
Worker, possibly also naming an entry point, carrying call properties, and exposing an HTTP or RPC style interface.
**A Binding is a combination of a permission and an API**, not just a target address.

**Calls and call trees** -- a top-level call comes from an external ingress; a nested call is made by a Worker through a Binding.
A nested call is an independent execution of the target version, but still belongs to the caller's call tree.

**Home Server** -- the preferred execution node computed deterministically from the `workerId` and the cluster's membership view.
**Home is a locality preference, not ownership**; it is not a node calls must pass through, nor an availability dependency.

**Warm execution** -- a possibly reusable execution of some `versionId`. It may disappear at any time and
**can only serve as a scheduling hint**: staleness may only affect latency, never correctness.

## 3. Design principles

**Follow Cloudflare's public material.** The official Workers / workerd documentation, source code and architecture articles are the primary references.
Deviations must state their reasons explicitly. **Details that are neither publicly described nor explicitly decided by us stay open
and must not turn into implicit constraints.**

**Bindings are the foundation, not a later add-on.** They decide how a Worker reaches other Workers and platform resources.
"First build a simplified HTTP execution plane and deal with Bindings later" is not allowed -- that would push the project's most central problem back.

**Keep each interface's native semantics.** A `fetch` call keeps HTTP's request, response, streaming, cancellation and backpressure semantics;
an RPC-style Binding keeps its RPC, capability and lifecycle semantics.
**The wire representation is an implementation choice and must not redefine the call model a Worker sees.**

**Placement is not ownership.** Placement decides where one call executes; it does not bind a Worker or a version permanently to a
machine. Execution state is dynamic and reclaimable.

**Authorization must be enforced outside what users control.** The target and permissions admission approves must be exactly the target and
permissions execution actually uses. Business parameters a user passes in cannot choose another execution identity.
(This principle was forced by concrete counterexamples.)

**Uncertain results are not replayed automatically.** Timeouts and disconnects cannot prove that execution did not happen. Only an explicit refusal before execution,
or a confirmed non-delivery, allows rerouting. In every other case the uncertain result is returned truthfully to the caller.

## 4. Execution model

Two ways determine a call's target:

```text
Top-level call:  ordinary HTTP request -> Route -> logical Worker
Nested call:     Binding capability -> target logical Worker + call context
```

A nested call **does not redo Host/Path route resolution** -- its Binding has already determined the target and the interface.

Both then enter the same model:

```text
logical Worker + release policy + call context
  -> a definite versionId
  -> distributed placement
  -> workerd execution
  -> call result
```

This sequence expresses only **semantic dependencies**. It does not decide which component chooses the version, where the placement logic runs,
or how the target and the context are transported.

## 4.1 Breakdown of responsibilities

What follows expresses **responsibilities**; it does not mean four separate network services are required. One Server can
take on several of them in one process, or they can be split.

```text
Runtime configuration and immutable artifacts
   ↓
Node control and process management
   ↓
Placement and admission
   ↓
Runtime integration and execution lifecycle  ←──────┐
   ↓                                                 │ authorized Binding capability
workerd user isolates ───────────────────────────────┘
```

**Runtime configuration** -- provides routes, release policies, immutable artifacts, Binding plans and execution policies.
Coworkerd needs explicit input and caching contracts, but does not take over the product-level release process.
**A resolved call keeps its original `versionId` while being forwarded and on fallback**; the receiving side may not choose code again from its own current release.
A configuration revision identifier is not a second executable Version identity.

**Node control and process management** -- membership view, node health, starting/stopping/restarting runtime processes, draining, allocating node resources.
The failure model is tentatively "a small supervisor + managed runtime processes".
The number of processes and the grouping of tenants are decided by the workload.

**Placement and admission** -- choose a legitimate node and runtime for an **authorized** target.
Note that node capacity, runtime health, code loading capacity and call resource admission are **four different conditions**.
**Admission must correspond to a real resource reservation, with a release path.**

**Runtime integration and execution lifecycle** -- owns the lifecycle of actual calls and capabilities:
dispatch, metering and limits, cancellation, streams, capability passing, residency, completion notification.
User code gets only the capabilities granted to it; **the local fast path and the remote path follow the same permission and budget rules.**

## 5. Placement model

```text
HomeServer = ConsistentHash(workerId, cluster membership view)
```

High-level behaviour:

1. if there is a suitable warm execution locally, use it directly;
2. otherwise prefer the Worker's Home Server;
3. only a confirmed refusal before execution, or a non-delivery, allows executing on another machine;
4. when the call may already have been delivered or started, report the uncertain result truthfully, unless the call contract explicitly allows a replay.

Hence: all versions of the same Worker share a Home (while `workerId` and the membership view do not change);
the Home does not need to preload or keep any version resident; the same version can execute on several machines at once under load.

**Having no warm execution is no reason to refuse** -- it only means a cold load may be needed, not that there is no capacity to execute.

## 6. Runtime

workerd is the execution engine; it provides ingress, Bindings, isolates, runtime semantics and dynamic loading.
Coworkerd provides the distributed execution and runtime hosting layer around it.

Upstream facts already confirmed (pinned version `workerd@1.20260827.1`):

- workerd **does not provide** instance-level admission, drain, eviction or resource limits. The Server uses
  `NullIsolateLimitEnforcer`, and the dynamic entry point does not use the resource limit values passed in.
- Named dynamic Workers are kept in a namespace map, and the only removal path we found is an isolate abort.
- The public Loader API only promises **best-effort reuse** of isolates; it neither guarantees residency nor offers a controllable eviction policy.

**So the residency and eviction of isolates are Coworkerd's own responsibility** (with multiple tenants in a process, a single tenant cannot be reclaimed by killing the process); we cannot assume workerd will do it.

**Coworkerd maintains a thin patch of workerd.**

The reason is not performance but two hard constraints:

1. **per-isolate resource limits** -- the trust model requires them, stock workerd uses
   `NullIsolateLimitEnforcer`, and the JS layer has no way out;
2. **native call semantics** -- without a fork we would have to hand-write a shadow implementation of workerd's private wire format,
   which is more brittle than a patch, and streaming / cancellation / capabilities would be missing for a long time.

Measured cost: 3 files, 101 net new lines, no core abstraction touched; a cold build takes 35 minutes, an incremental one 55 seconds.
**Upstreaming is not considered** -- upstream most likely keeps these extension points closed on purpose; plan for long-term self-maintenance.

## 7. Transport constraints

Distributed transport must keep the call model rather than flatten every execution into one generic function call.

For HTTP / `fetch`: the application-visible method, URL, headers, body, status, response headers, response body,
streaming, cancellation and backpressure stay complete. Other Binding interfaces keep their own parameters, results, capabilities and
lifecycle semantics.

**At any moment exactly one component owns a request stream.** Transfer of ownership, confirmed non-delivery and refusal before execution
must be expressed explicitly by the protocol, **never inferred from a timeout or a disconnect**.

Cap'n Proto is a well-founded integration candidate, because workerd itself uses its capability model.
But **the protocol's name alone does not guarantee that the call semantics are kept** -- the current implementation based on it still buffers completely.

## 8. Public references

- [workerd design principles](https://github.com/cloudflare/workerd/blob/main/README.md)
- [workerd is not a hardened sandbox](https://github.com/cloudflare/workerd/blob/v1.20260827.1/README.md#warning-workerd-is-not-a-hardened-sandbox)
- [Workers versions and deployments](https://developers.cloudflare.com/workers/versions-and-deployments/)
- [Workers Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/)
- [Service Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/)
- [Workers RPC lifecycle](https://developers.cloudflare.com/workers/runtime-apis/rpc/lifecycle/)
- [Dynamic Worker Loader](https://developers.cloudflare.com/dynamic-workers/api-reference/)
- [Sharding Workers: shard and conquer](https://blog.cloudflare.com/eliminating-cold-starts-2-shard-and-conquer/)

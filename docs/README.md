# Documentation

## Using Coworkerd

- [Installing and running Coworkerd](guide/install.md): the release package, a cluster with one compose command, bare metal, configuration, the sandbox and the system call allowlist, egress, the cluster key and etcd credentials.
- [Quickstart](guide/quickstart.md): from a wrangler project to the first request -- a tenant, a credential, `cowctl deploy`, logs, versions, secrets and usage.
- [Compatibility scope](guide/compat.md): which features of a wrangler project work, and what is refused at deployment.
- [Error codes](guide/errors.md): what calls, the control plane and the management endpoints answer with when they fail, what `fetch` throws for tenants' outgoing requests, and what each code means.

## Design

- [Architecture overview](architecture.md): the boundaries, positioning and trust model, the core concepts, and the execution and placement model.
- [Components and boundaries](components.md): the node daemon, the control plane, the launcher and the command line; the source directories and the release artifacts.
- [Control plane API](control-plane-api.md): tenants, `/v1`, credentials, quotas and the single write path into etcd.
- [Egress and authentication](egress-and-auth.md): tenants' outbound requests through the node's policy point, and the authentication between components.
- [Cluster state](cluster-state.md): membership, deployment state, code distribution and placement across nodes.
- [Process isolation and resource governance](process-isolation.md): the process pool, the watchdog, CPU and memory limits, quarantine, rotation and the sandbox.
- [Ecosystem compatibility](ecosystem-compat.md): how wrangler projects are converted and run -- the package format, the runtime bridge, static assets, secrets and trial loads.
- [Following upstream workerd](upstream-sync.md): the fork that holds the patched workerd, and how a sync with upstream is done and checked.

## Interfaces and patches

- [The launcher's interface](../launcher/PROTOCOL.md): how a node starts each workerd through `coworkerd-launcher` -- options, descriptors, status records and exit statuses.
- [Patch notes](../patches/README.md): the workerd patch series -- what it contains, how it is maintained, and how to work on it and follow a new upstream tag.

# Documentation

The guide is for running Coworkerd and deploying to it; the design documents explain how it works and why. Start with the guide, then read the architecture overview, which points to the rest.

## Using Coworkerd

- [Installing and running Coworkerd](guide/install.md): the release package, a cluster with one compose command and adding a node to it, bare metal with your own object store, configuration, upgrading, the sandbox and the system call allowlist, egress, the cluster key and the other credentials.
- [Quickstart](guide/quickstart.md): from a wrangler project to the first request -- a tenant, a credential, `cowctl deploy`, logs, versions, secrets and usage.
- [Compatibility scope](guide/compat.md): which features of a wrangler project work, and what is refused at deployment.
- [Databases (D1's shape)](guide/databases.md): making one, binding it to a Worker, `env.DB`, the HTTP SQL endpoint and what is not there.
- [Durable Objects](guide/durable-objects.md): classes, migrations, IDs, fetch and methods, hibernation, alarms and takeover.
- [Error codes](guide/errors.md): what calls, the control plane and the management endpoints answer when they fail, and what `fetch` throws for tenants' outgoing requests.

## Design

- [Architecture overview](architecture.md): one cluster for one platform, the components and their channels, the path of a call from the ingress to an isolate, and the system's boundaries.
- [Components and boundaries](components.md): what each component does, the processes of a node, compose and bare metal, the channels and their credentials, configuration sources, the repository and the release package.
- [Control plane API](control-plane-api.md): tenants and Workers, the `/v1` endpoints, credentials, quotas, versions and deployments, routes, secrets, usage and the version promise.
- [Cluster state](cluster-state.md): desired state and uploaded durable history in the object store and what it must guarantee, the object layout and who writes it, membership, deployment state and why a call tree sees one deployment, code distribution, placement, the control plane's write protocol, usage and suspects, and draining.
- [Process isolation and resource governance](process-isolation.md): the process pool and watchdog, CPU and memory limits, culprits and quarantine, eviction, rotation, the launcher, the system call allowlist, and where isolation ends.
- [Egress and authentication](egress-and-auth.md): tenants' outgoing requests through the node's policy point, the cluster key, an object storage identity for every node, the secrets key, tokens and credentials, and what is not done yet.
- [Compatibility with Cloudflare Workers](compatibility.md): how a wrangler project becomes a version, how its code runs, what is refused and why, and where behaviour differs from Cloudflare.

## Interfaces and patches

- [The launcher's interface](../launcher/PROTOCOL.md): how a node starts each workerd through `coworkerd-launcher` -- options, descriptors, status records and exit statuses.
- [Patch notes](../patches/README.md): the workerd patch series -- what it contains, how the fork is maintained and how it follows upstream.

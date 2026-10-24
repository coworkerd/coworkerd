# The Coworkerd image: the binaries of a release package, nothing compiled here. The build context is an
# unpacked package (bin/, runtime/, docker/), as scripts/release.sh builds it:
#   docker build --platform linux/<arch> --build-arg COWORKERD_VERSION=<version> -t coworkerd:<version> \
#     -f docker/Dockerfile <package directory>
# The base is Debian 13 slim (glibc 2.41, OpenSSL 3), what the binaries are built against; BASE may name the
# same image under a local tag where the registry cannot be reached.
ARG BASE=debian:trixie-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f
FROM ${BASE}
ARG COWORKERD_VERSION=0.0.0-dev
LABEL org.opencontainers.image.title="coworkerd" \
      org.opencontainers.image.description="Coworkerd: the node daemon, the control plane (with the deployment tool), workerd and the runtime" \
      org.opencontainers.image.version="${COWORKERD_VERSION}"
# The system's root certificates: the node's egress service makes tenants' HTTPS connections itself and verifies
# the destinations against them (egress.caFile adds more).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
# An unprivileged user owns the Server's state (stack directory) and the cluster's generated secrets.
# The compose file's secrets container also mounts the object store's volume at the last directory first,
# so that a new volume takes its owner and the object store runs as this user too. That directory is not
# empty: the store's image has content at /data that Docker would copy (as root) into a volume still empty
# when that container starts.
RUN useradd --system --uid 10001 --user-group --home-dir /var/lib/coworkerd --no-create-home --shell /usr/sbin/nologin coworkerd \
 && install -d -o coworkerd -g coworkerd -m 0750 /var/lib/coworkerd /var/lib/coworkerd-secrets \
 && install -d -o coworkerd -g coworkerd -m 0700 /var/lib/coworkerd-storage \
 && install -o coworkerd -g coworkerd -m 0600 /dev/null /var/lib/coworkerd-storage/.coworkerd
# coworkerd-launcher starts each workerd process in its least-privilege environment; in a container that
# needs the options of docker-compose.sandbox.yml (sandbox.mode auto runs workerd without it otherwise).
# The two coworkerd-syscalls-*.txt are the system call allowlist of this package's workerd, which the
# launcher installs before it executes it; the Server finds them beside its own executable.
COPY bin/coworkerd bin/coworkerd-apiserver bin/coworkerd-launcher bin/workerd \
     bin/coworkerd-syscalls-startup.txt bin/coworkerd-syscalls-steady.txt /opt/coworkerd/bin/
COPY runtime/ /opt/coworkerd/runtime/
COPY docker/entrypoint.sh /usr/local/bin/coworkerd-entrypoint
ENV PATH=/opt/coworkerd/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
USER coworkerd:coworkerd
WORKDIR /var/lib/coworkerd
# 8930: the ingress (and the management endpoints); 8934: the peer listener (other Servers, trial loads);
# 8940: the control plane (`coworkerd-entrypoint apiserver`).
EXPOSE 8930 8934 8940
ENTRYPOINT ["coworkerd-entrypoint"]
CMD ["node"]

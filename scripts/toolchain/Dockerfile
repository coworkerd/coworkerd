FROM node:22-trixie

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang-19 lld-19 libc++-19-dev libc++abi-19-dev libunwind-19-dev \
    tcl python3 git build-essential pkg-config cmake ca-certificates curl \
    xz-utils unzip strace procps && rm -rf /var/lib/apt/lists/*
RUN npm install -g @bazel/bazelisk@1.28.1
ENV PATH="/usr/lib/llvm-19/bin:${PATH}"
WORKDIR /work/workerd

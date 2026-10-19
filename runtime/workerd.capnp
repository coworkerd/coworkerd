using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "runtime", worker = (
      compatibilityDate = "2026-08-27",
      compatibilityFlags = ["enable_request_signal"],
      modules = [(name = "runtime.js", esModule = embed "runtime.js")],
      bindings = [
        (name = "LOADER", workerLoader = (id = "coworkerd-v1")),
        (name = "SERVER", service = "server"),
        (name = "SOURCE", service = "source"),
      ],
      globalOutbound = "none",
    )),
    (name = "server", external = (
      address = "127.0.0.1:8932",
      capnp = (capnpConnectHost = "coworkerd-server"),
    )),
    (name = "source", external = (address = "127.0.0.1:8933", http = ())),
    (name = "none", network = (allow = [], deny = [])),
  ],
  sockets = [
    (name = "rpc", address = "127.0.0.1:8931", service = "runtime",
      http = (capnpConnectHost = "coworkerd-runtime")),
  ],
);

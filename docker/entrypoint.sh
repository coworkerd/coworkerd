#!/usr/bin/env bash
# The entry point of the Coworkerd image (installed as coworkerd-entrypoint).
#
#   node         (default) Start one node (coworkerd). With COWORKERD_CONFIG, that file is the configuration;
#                otherwise one is written from the environment to $COWORKERD_STACK_DIR/config.json:
#                  COWORKERD_NODE_ID              required
#                  COWORKERD_STORAGE_ENDPOINT     required, e.g. http://storage:8333
#                  COWORKERD_STORAGE_BUCKET       default coworkerd
#                  COWORKERD_STORAGE_REGION       default us-east-1
#                  COWORKERD_STORAGE_CREDENTIALS_FILE  default $COWORKERD_SECRETS_DIR/server.json: the node's own identity of
#                                                 the object store (its members, suspects and usage are written below its
#                                                 own prefixes members/<id>/ and nodes/<id>/)
#                  COWORKERD_SECRETS_KEY_FILE     the secrets key (it opens the secrets' values of the desired state on
#                                                 the object store), default $COWORKERD_SECRETS_DIR/secrets-key.json
#                  COWORKERD_PEER_KEY_FILE        the cluster key, default $COWORKERD_SECRETS_DIR/cluster-key.json
#                                                 when it exists
#                  COWORKERD_CONTROL_ADDRESS      the control plane, which tenant code may not reach (egress), default
#                                                 apiserver:8940
#                  COWORKERD_PEER_ADDRESS         what other nodes dial, default <hostname>:8934
#                  COWORKERD_LISTEN_ADDRESS       the ingress, default 0.0.0.0:8930
#                  COWORKERD_PEER_LISTEN_ADDRESS  default 0.0.0.0:8934
#                  COWORKERD_PROCESS_COUNT        ordinary workerd processes (a node has two quarantine processes besides
#                                                 them), default 1
#                  COWORKERD_STACK_DIR            default /var/lib/coworkerd
#                The management token is COWORKERD_MANAGEMENT_TOKEN, else the one `secrets` generated
#                ($COWORKERD_SECRETS_DIR/management-token); the Server reads it from its environment
#                and it is never written into the configuration.
#   secrets      At every start of the cluster, before anything else: every credential the cluster uses,
#                unless it exists, and a copy of each in the directory of every service that reads it -- in the
#                compose file each service mounts only its own directory of the secrets volume:
#                  init/        cluster-key.json (the peer key), secrets-key.json (the key that seals secrets' values in
#                               the desired state), management-token, admin-credential (the API's administrator) -- the
#                               originals; only init and secrets mount the whole volume
#                  storage/     s3.json, the identity the object store reads at its start: deploy (the control plane
#                               and the tool) reads, writes and lists the whole bucket; and hotload/<id>.weed, each
#                               node's identity, which the store's container puts into the store's filer (weed shell
#                               s3.configure) at each of its starts and whenever the file appears or changes, so that
#                               any of them can be withdrawn without a restart: it reads what a node reads (the
#                               cluster's marker, the tenant index and the manifests, definitions and code, the members)
#                               and writes only its own prefixes members/<id>/ and nodes/<id>/ -- never another node's,
#                               never cluster/, the code or the credentials index (SeaweedFS's prefix actions, bucket
#                               COWORKERD_STORAGE_BUCKET, default coworkerd)
#                  apiserver/   deploy.json (the deploy identity), secrets-key.json, management-token, admin-credential;
#                               the tool service mounts it too
#                  nodes/<id>/  server.json (the node's own identity), cluster-key.json, secrets-key.json, management-token;
#                               one directory per id of COWORKERD_NODE_IDS (default node-1,node-2,node-3) and per node
#                               added with add-node
#                The management token and the administrator's credential are not generated when COWORKERD_MANAGEMENT_TOKEN
#                or COWORKERD_ADMIN_CREDENTIAL is set: the services take it from their environment then.
#                Copies are replaced (renamed into place) only when the original changed.
#                `secrets rotate-cluster-key add|promote|retire`: the three steps of a peer key rotation (a new key
#                accepted second; then signing; then the old one gone), each distributed at once (the nodes re-read
#                the key within a second). `secrets add-node ID`: a node joining a running cluster -- its directory
#                nodes/ID/ with an identity of its own and storage/hotload/ID.weed, which the object store's container
#                applies within seconds (no restart of the store); then start the node with that directory. The same
#                happens for an id COWORKERD_NODE_IDS gains while the store runs. `secrets remove-node ID`: a node
#                leaving for good -- its identity withdrawn: storage/hotload/ID.remove.weed, which the store's container
#                applies within seconds (s3.configure -delete: out of the filer, which holds every node's identity),
#                its hotload file and its directory nodes/ID/ removed. An id that COWORKERD_NODE_IDS still names gets
#                a new identity at the cluster's next start.
#   init         At every start of the cluster: the bucket and a self-check -- written by the deploy identity, read
#                back by a node's identity (the first node's), unsigned requests refused, the contract the membership
#                needs (create-if-absent, a listing's Date and LastModified, the clock) and that node identity's prefix
#                policy, reported (coworkerd check storage); the cluster's marker cluster/format/1 and empty indexes in
#                the bucket when there are none yet (coworkerd-apiserver tool init: nodes and the control plane refuse
#                to start on a bucket without the marker). Retries until the store answers.
#   apiserver    Start the control plane (coworkerd-apiserver), the API cowctl uses. With COWORKERD_CONFIG,
#                that file; otherwise one written to $COWORKERD_STACK_DIR/control.json from
#                  COWORKERD_STORAGE_ENDPOINT (required), COWORKERD_STORAGE_BUCKET,
#                  COWORKERD_STORAGE_REGION, COWORKERD_DEPLOY_CREDENTIALS_FILE (default the deploy identity
#                  secrets wrote, $COWORKERD_SECRETS_DIR/deploy.json), COWORKERD_SECRETS_KEY_FILE (default
#                  $COWORKERD_SECRETS_DIR/secrets-key.json), COWORKERD_CONTROL_LISTEN_ADDRESS (default 0.0.0.0:8940).
#                The administrator's credential (required): COWORKERD_ADMIN_CREDENTIAL, else the one `secrets` generated
#                ($COWORKERD_SECRETS_DIR/admin-credential). The management token (what it sends to the nodes) as for
#                node.
#   tool ARGS    coworkerd-apiserver tool ARGS with the cluster's object store, the deploy identity and the secrets key, e.g.
#                `tool upsert --manifest /work/manifest.json` (the default tenant; --tenant T for another);
#                `tool token` prints the management token, `tool admin-credential` the administrator's credential
#                of the API (what cowctl's administrator commands take), `tool version` coworkerd-apiserver --version.
#   healthcheck [node|apiserver]  node (default): exit 0 when this container's node is ready (GET
#                /__coworkerd/ready); apiserver: exit 0 when its control plane answers HTTP (a request without the
#                token gets its 401).
#   version      coworkerd --version.
set -euo pipefail

SECRETS=${COWORKERD_SECRETS_DIR:-/var/lib/coworkerd-secrets}
STACK=${COWORKERD_STACK_DIR:-/var/lib/coworkerd}

fail() { printf 'coworkerd-entrypoint: %s\n' "$*" >&2; exit 2; }
# Values go into JSON unescaped: each must match its pattern.
check() {
  local name=$1 value=$2 pattern=$3
  [[ $value =~ $pattern ]] || fail "$name is not valid: '$value'"
}
NAME='^[A-Za-z0-9._-]+$'
# A node id names its object keys (members/<id>/, nodes/<id>/): a NAME that is not . or ..
node_id() { [[ $1 =~ $NAME && $1 != . && $1 != .. ]]; }
ADDRESS='^[A-Za-z0-9._-]+:[0-9]+$'
ENDPOINT='^http://[A-Za-z0-9._-]+(:[0-9]+)?$'
PATHNAME='^/[A-Za-z0-9._/-]+$'

# A management token from the environment, else the one secrets wrote.
load_token() {
  if [[ -z ${COWORKERD_MANAGEMENT_TOKEN:-} && -s $SECRETS/management-token ]]; then
    COWORKERD_MANAGEMENT_TOKEN=$(<"$SECRETS/management-token")
    export COWORKERD_MANAGEMENT_TOKEN
  fi
}

# The administrator's credential of the API from the environment, else the one secrets wrote.
load_admin() {
  if [[ -z ${COWORKERD_ADMIN_CREDENTIAL:-} && -s $SECRETS/admin-credential ]]; then
    COWORKERD_ADMIN_CREDENTIAL=$(<"$SECRETS/admin-credential")
    export COWORKERD_ADMIN_CREDENTIAL
  fi
}

# "accessKey" or "secretKey" of a credentials file this script wrote ({"accessKey":"..","secretKey":".."}).
credential() {
  local text
  text=$(<"$1")
  [[ $text =~ \"$2\":\"([0-9a-f]+)\" ]] || fail "cannot read $2 from $1"
  printf '%s' "${BASH_REMATCH[1]}"
}

node() {
  load_token
  local config=${COWORKERD_CONFIG:-}
  mkdir -p "$STACK"
  if [[ -z $config ]]; then
    local id=${COWORKERD_NODE_ID:-} storage=${COWORKERD_STORAGE_ENDPOINT:-}
    [[ -n $id && -n $storage ]] || fail "set COWORKERD_NODE_ID and COWORKERD_STORAGE_ENDPOINT (or COWORKERD_CONFIG)"
    local credentials=${COWORKERD_STORAGE_CREDENTIALS_FILE:-$SECRETS/server.json}
    local listen=${COWORKERD_LISTEN_ADDRESS:-0.0.0.0:8930} peerListen=${COWORKERD_PEER_LISTEN_ADDRESS:-0.0.0.0:8934}
    local bucket=${COWORKERD_STORAGE_BUCKET:-coworkerd} region=${COWORKERD_STORAGE_REGION:-us-east-1}
    check COWORKERD_NODE_ID "$id" "$NAME"
    node_id "$id" || fail "COWORKERD_NODE_ID is not valid: '$id' (. and .. name no object key)"
    check COWORKERD_STORAGE_ENDPOINT "$storage" "$ENDPOINT"
    check COWORKERD_STORAGE_BUCKET "$bucket" "$NAME"
    check COWORKERD_STORAGE_REGION "$region" "$NAME"
    check COWORKERD_STORAGE_CREDENTIALS_FILE "$credentials" "$PATHNAME"
    check COWORKERD_STACK_DIR "$STACK" "$PATHNAME"
    check COWORKERD_LISTEN_ADDRESS "$listen" "$ADDRESS"
    check COWORKERD_PEER_LISTEN_ADDRESS "$peerListen" "$ADDRESS"
    [[ -s $credentials ]] || fail "no storage credentials at $credentials (run the secrets step first)"
    local extra=""
    # The cluster key, as the secrets step distributed it; the control plane's address, which the egress policy denies
    # as the cluster's own.
    local peerKey=${COWORKERD_PEER_KEY_FILE:-$SECRETS/cluster-key.json}
    if [[ -s $peerKey ]]; then
      check COWORKERD_PEER_KEY_FILE "$peerKey" "$PATHNAME"
      extra+=$',\n  "peerAuth": {"keyFile": "'"$peerKey"'"}'
    fi
    # The secrets key opens the secrets' values in the tenants' manifests.
    local secretsKey=${COWORKERD_SECRETS_KEY_FILE:-$SECRETS/secrets-key.json}
    check COWORKERD_SECRETS_KEY_FILE "$secretsKey" "$PATHNAME"
    [[ -s $secretsKey ]] || fail "no secrets key at $secretsKey (run the secrets step first)"
    local control=${COWORKERD_CONTROL_ADDRESS:-apiserver:8940}
    check COWORKERD_CONTROL_ADDRESS "$control" "$ADDRESS"
    extra+=$',\n  "egress": {"clusterAddresses": ["'"$control"'"]}'
    if [[ -n ${COWORKERD_PEER_ADDRESS:-} ]]; then
      check COWORKERD_PEER_ADDRESS "$COWORKERD_PEER_ADDRESS" "$ADDRESS"
      extra+=$',\n  "advertisedAddress": "'"$COWORKERD_PEER_ADDRESS"'"'
    fi
    if [[ -n ${COWORKERD_PROCESS_COUNT:-} ]]; then
      check COWORKERD_PROCESS_COUNT "$COWORKERD_PROCESS_COUNT" '^[1-9][0-9]?$'
      extra+=$',\n  "processPool": {"count": '"$COWORKERD_PROCESS_COUNT"'}'
    fi
    config=$STACK/config.json
    cat >"$config.next" <<JSON
{
  "nodeId": "$id",
  "stackDirectory": "$STACK",
  "listenAddress": "$listen",
  "peerListenAddress": "$peerListen",
  "secrets": {"keyFile": "$secretsKey"},
  "storage": {"endpoint": "$storage", "bucket": "$bucket", "region": "$region", "credentialsFile": "$credentials"}$extra
}
JSON
    mv "$config.next" "$config"
  fi
  exec coworkerd --config "$config"
}

apiserver() {
  load_token
  load_admin
  local config=${COWORKERD_CONFIG:-}
  mkdir -p "$STACK"
  if [[ -z $config ]]; then
    local storage=${COWORKERD_STORAGE_ENDPOINT:-}
    [[ -n $storage ]] || fail "set COWORKERD_STORAGE_ENDPOINT (or COWORKERD_CONFIG)"
    local credentials=${COWORKERD_DEPLOY_CREDENTIALS_FILE:-$SECRETS/deploy.json}
    local listen=${COWORKERD_CONTROL_LISTEN_ADDRESS:-0.0.0.0:8940}
    local bucket=${COWORKERD_STORAGE_BUCKET:-coworkerd} region=${COWORKERD_STORAGE_REGION:-us-east-1}
    check COWORKERD_STORAGE_ENDPOINT "$storage" "$ENDPOINT"
    check COWORKERD_STORAGE_BUCKET "$bucket" "$NAME"
    check COWORKERD_STORAGE_REGION "$region" "$NAME"
    check COWORKERD_DEPLOY_CREDENTIALS_FILE "$credentials" "$PATHNAME"
    check COWORKERD_CONTROL_LISTEN_ADDRESS "$listen" "$ADDRESS"
    check COWORKERD_STACK_DIR "$STACK" "$PATHNAME"
    [[ -s $credentials ]] || fail "no deploy credentials at $credentials (run the secrets step first)"
    local secretsKey=${COWORKERD_SECRETS_KEY_FILE:-$SECRETS/secrets-key.json}
    check COWORKERD_SECRETS_KEY_FILE "$secretsKey" "$PATHNAME"
    [[ -s $secretsKey ]] || fail "no secrets key at $secretsKey (run the secrets step first)"
    mkdir -p "$STACK/control-tmp"
    config=$STACK/control.json
    cat >"$config.next" <<JSON
{
  "secrets": {"keyFile": "$secretsKey"},
  "storage": {"endpoint": "$storage", "bucket": "$bucket", "region": "$region", "credentialsFile": "$credentials"},
  "control": {"listenAddress": "$listen", "temporaryDirectory": "$STACK/control-tmp"}
}
JSON
    mv "$config.next" "$config"
  fi
  exec coworkerd-apiserver --config "$config"
}

# The credentials of the cluster: see `secrets` in the header. Prints one line per step; never a value.
secrets() {
  umask 077
  local random
  random() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }
  # A copy of an original in a service's directory, renamed into place only when it differs (readers never see half a
  # file, and a node re-reads its key only when it changed).
  place() {
    local source=$1 target=$2
    if [[ ! -f $target ]] || ! cmp -s "$source" "$target"; then
      cp "$source" "$target.next" && mv "$target.next" "$target"
      printf '[secrets] {"event":"placed","file":"%s"}\n' "${target#"$SECRETS"/}"
    fi
  }
  local nodes=${COWORKERD_NODE_IDS:-node-1,node-2,node-3} bucket=${COWORKERD_STORAGE_BUCKET:-coworkerd}
  [[ $nodes =~ ^[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*$ ]] || fail "COWORKERD_NODE_IDS is not valid: '$nodes'"
  local one
  for one in ${nodes//,/ }; do node_id "$one" || fail "COWORKERD_NODE_IDS is not valid: '$one' (. and .. name no object key)"; done
  check COWORKERD_STORAGE_BUCKET "$bucket" "$NAME"
  mkdir -p "$SECRETS/init" "$SECRETS/storage" "$SECRETS/apiserver" "$SECRETS/nodes"
  case ${1:-} in
    "") ;;
    rotate-cluster-key)
      local file=$SECRETS/init/cluster-key.json text first second
      text=$(<"$file")
      [[ $text =~ ^\{\"keys\":\[(\{\"id\":\"[A-Za-z0-9._-]+\",\"secret\":\"[0-9a-f]+\"\})(,(\{\"id\":\"[A-Za-z0-9._-]+\",\"secret\":\"[0-9a-f]+\"\}))?\]\}$ ]] ||
        fail "$file is not a key file this script wrote; edit it by hand (docs/guide/install.md)"
      first=${BASH_REMATCH[1]} second=${BASH_REMATCH[3]}
      case ${2:-} in
        add) [[ -z $second ]] || fail "two keys already: promote or retire first"
             second=$(printf '{"id":"k%s","secret":"%s"}' "$(date -u +%Y%m%d%H%M%S)" "$(random 32)")
             printf '{"keys":[%s,%s]}\n' "$first" "$second" >"$file.next" ;;
        promote) [[ -n $second ]] || fail "one key only: add a new one first"
             printf '{"keys":[%s,%s]}\n' "$second" "$first" >"$file.next" ;;
        retire) [[ -n $second ]] || fail "one key only: nothing to retire"
             printf '{"keys":[%s]}\n' "$first" >"$file.next" ;;
        *) fail "secrets rotate-cluster-key takes add, promote or retire" ;;
      esac
      mv "$file.next" "$file"
      printf '[secrets] {"event":"cluster-key","step":"%s"}\n' "$2" ;;
    add-node)
      # A node joining the running cluster: its id with the others, then the same steps as at a start.
      node_id "${2:-}" || fail "secrets add-node takes a node id ([A-Za-z0-9._-]+, not . or ..)"
      [[ -s $SECRETS/storage/s3.json ]] || fail "no cluster in $SECRETS yet: start it first (the secrets step runs at every start)"
      rm -f "$SECRETS/storage/hotload/$2.remove.weed"
      nodes="$nodes,$2" ;;
    remove-node)
      # A node leaving for good: its directory and hotload file go, and the store's container takes its identity out of
      # the filer (hotload/ID.remove.weed: s3.configure -delete).
      node_id "${2:-}" || fail "secrets remove-node takes a node id ([A-Za-z0-9._-]+, not . or ..)"
      [[ -d $SECRETS/nodes/$2 ]] || fail "no node $2 in $SECRETS/nodes"
      rm -rf "${SECRETS:?}/nodes/$2"
      rm -f "$SECRETS/storage/hotload/$2.weed"
      mkdir -p "$SECRETS/storage/hotload"
      printf 's3.configure -user=node-%s -delete -apply\n' "$2" >"$SECRETS/storage/hotload/$2.remove.weed.next"
      mv "$SECRETS/storage/hotload/$2.remove.weed.next" "$SECRETS/storage/hotload/$2.remove.weed"
      local listed=false kept=""
      for one in ${nodes//,/ }; do
        if [[ $one == "$2" ]]; then listed=true; else kept+="${kept:+,}$one"; fi
      done
      nodes=$kept
      printf '[secrets] {"event":"node-removed","node":"%s","file":"storage/hotload/%s.remove.weed","inNodeIds":%s}\n' "$2" "$2" "$listed" ;;
    *) fail "secrets takes no argument, rotate-cluster-key add|promote|retire, add-node ID or remove-node ID" ;;
  esac
  # The object store's identities. deploy writes everything; each node's identity has SeaweedFS's prefix actions
  # (Action:bucket/prefix*, the prefix ending in '/' so that node-1's never covers node-10's): it reads what a node reads --
  # the cluster's marker, the tenant index and the manifests (and lists them when a hint is missing), the definitions
  # and the code, the members (and lists them: liveness, the suspects), the Durable Object shards (and lists their
  # epochs) -- and reads, writes and lists only its own members/<id>/ and nodes/<id>/ (heartbeats, epochs, the drain
  # marker, its suspects; its usage); it also writes the shards' epochs, release markers and owner hints
  # (do/<namespace>/shards/<shard>/...): any node may take a shard over. It never reads the credentials index or writes
  # another node's prefixes, cluster/, tenants/ or the code.
  node_actions() {
    local b=$bucket id=$1 action out="" prefix
    for prefix in cluster/format cluster/tenants tenants definitions packages modules bundles members do; do
      out+="\"Read:$b/$prefix/*\","
    done
    for prefix in members cluster/tenants tenants do; do out+="\"List:$b/$prefix/*\","; done
    for prefix in "members/$id" "nodes/$id"; do
      for action in Read Write List; do out+="\"$action:$b/$prefix/*\","; done
    done
    out+="\"Write:$b/do/*/shards/*\","
    printf '%s' "${out%,}"
  }
  local id
  if [[ ! -s $SECRETS/apiserver/deploy.json ]]; then
    printf '{"accessKey":"%s","secretKey":"%s"}\n' "$(random 12)" "$(random 24)" >"$SECRETS/apiserver/deploy.json.next"
    mv "$SECRETS/apiserver/deploy.json.next" "$SECRETS/apiserver/deploy.json"
    printf '[secrets] {"event":"storage-identity","identity":"deploy","generated":true}\n'
  fi
  for id in ${nodes//,/ }; do
    mkdir -p "$SECRETS/nodes/$id"
    if [[ ! -s $SECRETS/nodes/$id/server.json ]]; then
      printf '{"accessKey":"%s","secretKey":"%s"}\n' "$(random 12)" "$(random 24)" >"$SECRETS/nodes/$id/server.json.next"
      mv "$SECRETS/nodes/$id/server.json.next" "$SECRETS/nodes/$id/server.json"
      printf '[secrets] {"event":"storage-identity","identity":"node-%s","generated":true}\n' "$id"
    fi
  done
  # The file of identities SeaweedFS reads at its start: deploy alone, rewritten when it changes (one from before, which
  # also held the nodes', is replaced). An identity read from it stays in force until the store's next start, so the
  # nodes' are not there: each is hotload/<id>.weed, which the store's container applies with weed shell's s3.configure
  # at each of its starts and whenever the file appears or changes -- kept in the store's filer, in force at once, and
  # withdrawn the same way (remove-node).
  local directory candidate
  printf '{"identities":[{"name":"deploy","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write","List"]}]}\n' \
    "$(credential "$SECRETS/apiserver/deploy.json" accessKey)" "$(credential "$SECRETS/apiserver/deploy.json" secretKey)" \
    >"$SECRETS/storage/s3.json.candidate"
  if [[ -s $SECRETS/storage/s3.json ]] && cmp -s "$SECRETS/storage/s3.json.candidate" "$SECRETS/storage/s3.json"; then
    rm -f "$SECRETS/storage/s3.json.candidate"
  else
    mv "$SECRETS/storage/s3.json.candidate" "$SECRETS/storage/s3.json"
    printf '[secrets] {"event":"storage-identities","written":true}\n'
  fi
  mkdir -p "$SECRETS/storage/hotload"
  for directory in "$SECRETS"/nodes/*/; do
    id=${directory%/}; id=${id##*/}
    [[ -s $SECRETS/nodes/$id/server.json ]] || continue
    rm -f "$SECRETS/storage/hotload/$id.remove.weed"
    candidate=$(printf 's3.configure -user=node-%s -access_key=%s -secret_key=%s -actions=%s -apply' "$id" \
      "$(credential "$SECRETS/nodes/$id/server.json" accessKey)" "$(credential "$SECRETS/nodes/$id/server.json" secretKey)" \
      "$(node_actions "$id" | tr -d '"')")
    [[ -s $SECRETS/storage/hotload/$id.weed && $(cat "$SECRETS/storage/hotload/$id.weed") == "$candidate" ]] && continue
    printf '%s\n' "$candidate" >"$SECRETS/storage/hotload/$id.weed.next"
    mv "$SECRETS/storage/hotload/$id.weed.next" "$SECRETS/storage/hotload/$id.weed"
    printf '[secrets] {"event":"storage-identity-hotload","identity":"node-%s","file":"storage/hotload/%s.weed"}\n' "$id" "$id"
  done
  # SeaweedFS's S3 gateway also serves a gRPC configuration interface of its own beside S3, on the same addresses and
  # without credentials. security.toml (read by the storage container) makes it require a client certificate of a
  # certificate authority that signs nothing -- its key is dropped at once, no client certificate exists -- while the
  # gateway's own certificate comes from another authority whose key is dropped too: the gateway's key pair in this
  # directory is no client certificate the interface accepts, so that nobody can use it. The master, volume and filer
  # servers listen on the storage container's loopback only (docker-compose.yml). A security.toml from before (one
  # authority for both) is replaced.
  if [[ ! -s $SECRETS/storage/security.toml || ! -s $SECRETS/storage/grpc-client-ca.crt ]]; then
    local tls
    tls=$(mktemp -d)
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 36500 -subj /CN=coworkerd-storage-grpc-clients \
      -keyout "$tls/client-ca.key" -out "$SECRETS/storage/grpc-client-ca.crt" 2>/dev/null
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 36500 -subj /CN=coworkerd-storage-grpc \
      -keyout "$tls/ca.key" -out "$tls/ca.crt" 2>/dev/null
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=localhost \
      -keyout "$SECRETS/storage/grpc-s3.key" -out "$tls/s3.csr" 2>/dev/null
    openssl x509 -req -in "$tls/s3.csr" -CA "$tls/ca.crt" -CAkey "$tls/ca.key" -set_serial 1 -days 36500 \
      -out "$SECRETS/storage/grpc-s3.crt" 2>/dev/null
    rm -rf "$tls"
    rm -f "$SECRETS/storage/grpc-ca.crt"
    chmod 0600 "$SECRETS/storage/grpc-s3.key"
    printf '[grpc]\nca = "/config/grpc-client-ca.crt"\n\n[grpc.s3]\ncert = "/config/grpc-s3.crt"\nkey = "/config/grpc-s3.key"\n' \
      >"$SECRETS/storage/security.toml.next"
    mv "$SECRETS/storage/security.toml.next" "$SECRETS/storage/security.toml"
    printf '[secrets] {"event":"storage-grpc","written":true}\n'
  fi
  if [[ -z ${COWORKERD_MANAGEMENT_TOKEN:-} && ! -s $SECRETS/init/management-token ]]; then
    random 32 >"$SECRETS/init/management-token.next"
    mv "$SECRETS/init/management-token.next" "$SECRETS/init/management-token"
    printf '[secrets] {"event":"management-token","generated":true}\n'
  fi
  # The administrator's credential of the API.
  if [[ -z ${COWORKERD_ADMIN_CREDENTIAL:-} && ! -s $SECRETS/init/admin-credential ]]; then
    random 32 >"$SECRETS/init/admin-credential.next"
    mv "$SECRETS/init/admin-credential.next" "$SECRETS/init/admin-credential"
    printf '[secrets] {"event":"admin-credential","generated":true}\n'
  fi
  # The secrets key: secrets' values are sealed with it in the tenants' manifests (AES-256-GCM); the control plane seals,
  # every node opens.
  if [[ ! -s $SECRETS/init/secrets-key.json ]]; then
    printf '{"key":"%s"}\n' "$(random 32)" >"$SECRETS/init/secrets-key.json.next"
    mv "$SECRETS/init/secrets-key.json.next" "$SECRETS/init/secrets-key.json"
    printf '[secrets] {"event":"secrets-key","generated":true}\n'
  fi
  # The cluster key: the nodes sign their peer connections with it.
  if [[ ! -s $SECRETS/init/cluster-key.json ]]; then
    printf '{"keys":[{"id":"k%s","secret":"%s"}]}\n' "$(date -u +%Y%m%d%H%M%S)" "$(random 32)" >"$SECRETS/init/cluster-key.json.next"
    mv "$SECRETS/init/cluster-key.json.next" "$SECRETS/init/cluster-key.json"
    printf '[secrets] {"event":"cluster-key","generated":true}\n'
  fi
  [[ -s $SECRETS/init/management-token ]] && place "$SECRETS/init/management-token" "$SECRETS/apiserver/management-token"
  [[ -s $SECRETS/init/admin-credential ]] && place "$SECRETS/init/admin-credential" "$SECRETS/apiserver/admin-credential"
  place "$SECRETS/init/secrets-key.json" "$SECRETS/apiserver/secrets-key.json"
  for directory in "$SECRETS"/nodes/*/; do
    id=${directory%/}; id=${id##*/}
    place "$SECRETS/init/cluster-key.json" "$SECRETS/nodes/$id/cluster-key.json"
    place "$SECRETS/init/secrets-key.json" "$SECRETS/nodes/$id/secrets-key.json"
    [[ -s $SECRETS/init/management-token ]] && place "$SECRETS/init/management-token" "$SECRETS/nodes/$id/management-token"
  done
  if [[ ${1:-} == add-node ]]; then
    printf '[secrets] {"event":"node-added","node":"%s","directory":"nodes/%s"}\n' "$2" "$2"
  fi
  printf '[secrets] {"event":"ready","nodes":"%s"}\n' "$nodes"
}

init() {
  local storage=${COWORKERD_STORAGE_ENDPOINT:-http://storage:8333} bucket=${COWORKERD_STORAGE_BUCKET:-coworkerd}
  check COWORKERD_STORAGE_ENDPOINT "$storage" "$ENDPOINT"
  check COWORKERD_STORAGE_BUCKET "$bucket" "$NAME"
  umask 077
  [[ -s $SECRETS/storage/s3.json && -s $SECRETS/apiserver/deploy.json ]] ||
    fail "no credentials in $SECRETS (the secrets step writes them first)"
  # The node identity the self-check reads with and whose prefix policy it reports: the first node's.
  local reader="" directory
  for directory in "$SECRETS"/nodes/*/; do
    [[ -s ${directory}server.json ]] && { reader=${directory%/}; reader=${reader##*/}; break; }
  done
  [[ -n $reader ]] || fail "no node identity in $SECRETS/nodes (the secrets step writes them first)"
  # The bucket first (SeaweedFS answers 200 to an object PUT into a missing bucket and cannot read it
  # back), then the self-check. The store may still be starting: retry for up to two minutes.
  local started=$SECONDS attempt=0
  until AWS_ACCESS_KEY_ID=$(credential "$SECRETS/apiserver/deploy.json" accessKey) \
        AWS_SECRET_ACCESS_KEY=$(credential "$SECRETS/apiserver/deploy.json" secretKey) \
        COWORKERD_READ_ACCESS_KEY=$(credential "$SECRETS/nodes/$reader/server.json" accessKey) \
        COWORKERD_READ_SECRET_KEY=$(credential "$SECRETS/nodes/$reader/server.json" secretKey) \
        COWORKERD_READ_NODE_ID=$reader \
        coworkerd check storage "$storage" "$bucket" >"$SECRETS/init/.probe.log" 2>&1; do
    attempt=$((attempt + 1))
    if ((SECONDS - started > 120)); then cat "$SECRETS/init/.probe.log" >&2; fail "the object store did not pass its self-check"; fi
    sleep 1
  done
  printf '[init] {"event":"storage","bucket":"%s","attempts":%d,"seconds":%d}\n' "$bucket" "$((attempt + 1))" "$((SECONDS - started))"
  printf '[init] %s\n' "$(grep -o '{"contract".*' "$SECRETS/init/.probe.log")"
  # Nodes and the control plane start only on a bucket that holds the cluster's marker (cluster/format/1; a bucket
  # without it may be a wrong one): written once, with empty indexes beside it, by the deploy identity.
  local format=""
  started=$SECONDS
  until format=$(COWORKERD_STORAGE_ENDPOINT=$storage COWORKERD_STORAGE_BUCKET=$bucket \
        AWS_ACCESS_KEY_ID=$(credential "$SECRETS/apiserver/deploy.json" accessKey) \
        AWS_SECRET_ACCESS_KEY=$(credential "$SECRETS/apiserver/deploy.json" secretKey) \
        coworkerd-apiserver tool init 2>"$SECRETS/init/.format.log"); do
    if ((SECONDS - started > 120)); then cat "$SECRETS/init/.format.log" >&2; fail "the cluster's marker could not be written"; fi
    sleep 1
  done
  printf '[init] %s\n' "$format"
  printf '[init] {"event":"ready","seconds":%d}\n' "$SECONDS"
}

tool() {
  load_token
  load_admin
  if [[ ${1:-} == token ]]; then
    [[ -n ${COWORKERD_MANAGEMENT_TOKEN:-} ]] || fail "no management token"
    printf '%s\n' "$COWORKERD_MANAGEMENT_TOKEN"
    return
  fi
  if [[ ${1:-} == admin-credential ]]; then
    [[ -n ${COWORKERD_ADMIN_CREDENTIAL:-} ]] || fail "no administrator's credential"
    printf '%s\n' "$COWORKERD_ADMIN_CREDENTIAL"
    return
  fi
  if [[ ${1:-} == version || ${1:-} == --version ]]; then
    exec coworkerd-apiserver --version
  fi
  export COWORKERD_STORAGE_ENDPOINT=${COWORKERD_STORAGE_ENDPOINT:-http://storage:8333}
  if [[ -s $SECRETS/deploy.json ]]; then
    AWS_ACCESS_KEY_ID=$(credential "$SECRETS/deploy.json" accessKey)
    AWS_SECRET_ACCESS_KEY=$(credential "$SECRETS/deploy.json" secretKey)
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  fi
  # The secrets key: `tool secret put` seals the value with it.
  if [[ -z ${COWORKERD_SECRETS_KEY_FILE:-} && -s $SECRETS/secrets-key.json ]]; then
    export COWORKERD_SECRETS_KEY_FILE=$SECRETS/secrets-key.json
  fi
  # The desired state and the members (the trial loads go to the live ones) are the object store's.
  exec coworkerd-apiserver tool "$@"
}

healthcheck() {
  local role=${1:-node} listen status
  case $role in
    node) listen=${COWORKERD_LISTEN_ADDRESS:-0.0.0.0:8930} ;;
    apiserver) listen=${COWORKERD_CONTROL_LISTEN_ADDRESS:-0.0.0.0:8940} ;;
    *) fail "healthcheck takes node or apiserver, not $role" ;;
  esac
  exec 3<>"/dev/tcp/127.0.0.1/${listen##*:}" || exit 1
  if [[ $role == apiserver ]]; then
    printf 'GET /v1/status HTTP/1.0\r\nHost: localhost\r\n\r\n' >&3
    IFS= read -r -t 5 status <&3 || exit 1
    [[ $status == *" 401 "* ]]
  else
    printf 'GET /__coworkerd/ready HTTP/1.0\r\nHost: localhost\r\n\r\n' >&3
    IFS= read -r -t 5 status <&3 || exit 1
    [[ $status == *" 200 "* ]]
  fi
}

case ${1:-node} in
  node) node ;;
  apiserver) apiserver ;;
  secrets) shift; secrets "$@" ;;
  init) init ;;
  tool) shift; tool "$@" ;;
  healthcheck) shift; healthcheck "$@" ;;
  version) exec coworkerd --version ;;
  *) exec "$@" ;;
esac

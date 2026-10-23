#!/usr/bin/env bash
# The entry point of the Coworkerd image (installed as coworkerd-entrypoint).
#
#   node         (default) Start one node (coworkerd). With COWORKERD_CONFIG, that file is the configuration;
#                otherwise one is written from the environment to $COWORKERD_STACK_DIR/config.json:
#                  COWORKERD_NODE_ID              required
#                  COWORKERD_ETCD_ENDPOINT        required, e.g. http://etcd:2379
#                  COWORKERD_STORAGE_ENDPOINT     required, e.g. http://storage:8333
#                  COWORKERD_STORAGE_BUCKET       default coworkerd
#                  COWORKERD_STORAGE_REGION       default us-east-1
#                  COWORKERD_STORAGE_CREDENTIALS_FILE  default $COWORKERD_SECRETS_DIR/server.json
#                  COWORKERD_MEMBERSHIP_BACKEND   objectStore (default: the members' heartbeats under members/ in the
#                                                 bucket, written with the storage identity) or etcd
#                  COWORKERD_ETCD_CREDENTIALS_FILE     default $COWORKERD_SECRETS_DIR/etcd.json when it exists
#                  COWORKERD_PEER_KEY_FILE        the cluster key, default $COWORKERD_SECRETS_DIR/cluster-key.json
#                                                 when it exists
#                  COWORKERD_CONTROL_ADDRESS      the control plane, which tenant code may not reach (egress), default
#                                                 apiserver:8940
#                  COWORKERD_PEER_ADDRESS         what other nodes dial, default <hostname>:8934
#                  COWORKERD_LISTEN_ADDRESS       the ingress, default 0.0.0.0:8930
#                  COWORKERD_PEER_LISTEN_ADDRESS  default 0.0.0.0:8934
#                  COWORKERD_PROCESS_COUNT        workerd processes besides the quarantine one, default 1
#                  COWORKERD_STACK_DIR            default /var/lib/coworkerd
#                The management token is COWORKERD_MANAGEMENT_TOKEN, else the one `secrets` generated
#                ($COWORKERD_SECRETS_DIR/management-token); the Server reads it from its environment
#                and it is never written into the configuration.
#   secrets      At every start of the cluster, before anything else: every credential the cluster uses,
#                unless it exists, and a copy of each in the directory of every service that reads it -- in the
#                compose file each service mounts only its own directory of the secrets volume:
#                  init/        etcd-root.json (etcd's root), server.json (the object store's node identity),
#                               cluster-key.json (the peer key), management-token, admin-credential (the API's
#                               administrator) -- the originals; only init and secrets mount the whole volume
#                  storage/     s3.json, the object store's identities (deploy writes; the nodes read, list and write
#                               their membership under members/ -- the whole bucket for now: per-prefix rights later)
#                  apiserver/   deploy.json (the deploy identity), etcd.json (etcd user coworkerd-apiserver),
#                               management-token, admin-credential; the tool service mounts it too
#                  nodes/<id>/  server.json (the node identity), etcd.json (etcd user coworkerd-node-<id>), cluster-key.json,
#                               management-token; one directory per id of COWORKERD_NODE_IDS (default
#                               node-1,node-2,node-3)
#                The management token and the administrator's credential are not generated when COWORKERD_MANAGEMENT_TOKEN
#                or COWORKERD_ADMIN_CREDENTIAL is set: the services take it from their environment then.
#                Copies are replaced (renamed into place) only when the original changed.
#                `secrets rotate-cluster-key add|promote|retire`: the three steps of a peer key rotation (a new key
#                accepted second; then signing; then the old one gone), each distributed at once (the nodes re-read
#                the key within a second). `secrets rotate-etcd-password apiserver|nodes/<id>`: a new password in that
#                directory's etcd.json; then run init, which sets every user's password from its file (a daemon reads
#                its file again when etcd refuses its token).
#   init         At every start of the cluster: the bucket and a self-check -- written by the deploy identity, read
#                back by the nodes' identity, unsigned requests refused, and the contract the membership needs
#                (create-if-absent, a listing's Date and LastModified, the clock; coworkerd check storage); etcd's users, roles
#                and authentication (coworkerd-apiserver tool etcd-auth: the roles of the v2 layout -- the control
#                plane writes tenants/, credentials/ and deploy/, each node only its own nodes/<id>/ -- every user's
#                password from its file, then authentication on); an empty deployment state in etcd when there is
#                none yet. Retries until the store and etcd answer.
#   apiserver    Start the control plane (coworkerd-apiserver), the API cowctl uses. With COWORKERD_CONFIG,
#                that file; otherwise one written to $COWORKERD_STACK_DIR/control.json from
#                  COWORKERD_ETCD_ENDPOINT, COWORKERD_STORAGE_ENDPOINT (required), COWORKERD_STORAGE_BUCKET,
#                  COWORKERD_STORAGE_REGION, COWORKERD_MEMBERSHIP_BACKEND (as for node: where the control plane reads the
#                  members), COWORKERD_DEPLOY_CREDENTIALS_FILE (default the deploy identity
#                  secrets wrote, $COWORKERD_SECRETS_DIR/deploy.json), COWORKERD_CONTROL_LISTEN_ADDRESS
#                  (default 0.0.0.0:8940).
#                The administrator's credential (required): COWORKERD_ADMIN_CREDENTIAL, else the one `secrets` generated
#                ($COWORKERD_SECRETS_DIR/admin-credential). The management token (what it sends to the nodes) as for
#                node; etcd's credentials as for node (COWORKERD_ETCD_CREDENTIALS_FILE, default
#                $COWORKERD_SECRETS_DIR/etcd.json when it exists).
#   tool ARGS    coworkerd-apiserver tool ARGS with the cluster's addresses and the deploy identity, e.g.
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
MEMBERSHIP='^(objectStore|etcd)$'
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
    local id=${COWORKERD_NODE_ID:-} etcd=${COWORKERD_ETCD_ENDPOINT:-} storage=${COWORKERD_STORAGE_ENDPOINT:-}
    [[ -n $id && -n $etcd && -n $storage ]] ||
      fail "set COWORKERD_NODE_ID, COWORKERD_ETCD_ENDPOINT and COWORKERD_STORAGE_ENDPOINT (or COWORKERD_CONFIG)"
    local credentials=${COWORKERD_STORAGE_CREDENTIALS_FILE:-$SECRETS/server.json}
    local listen=${COWORKERD_LISTEN_ADDRESS:-0.0.0.0:8930} peerListen=${COWORKERD_PEER_LISTEN_ADDRESS:-0.0.0.0:8934}
    local bucket=${COWORKERD_STORAGE_BUCKET:-coworkerd} region=${COWORKERD_STORAGE_REGION:-us-east-1}
    check COWORKERD_NODE_ID "$id" "$NAME"
    check COWORKERD_ETCD_ENDPOINT "$etcd" "$ENDPOINT"
    check COWORKERD_STORAGE_ENDPOINT "$storage" "$ENDPOINT"
    check COWORKERD_STORAGE_BUCKET "$bucket" "$NAME"
    check COWORKERD_STORAGE_REGION "$region" "$NAME"
    check COWORKERD_STORAGE_CREDENTIALS_FILE "$credentials" "$PATHNAME"
    check COWORKERD_STACK_DIR "$STACK" "$PATHNAME"
    check COWORKERD_LISTEN_ADDRESS "$listen" "$ADDRESS"
    check COWORKERD_PEER_LISTEN_ADDRESS "$peerListen" "$ADDRESS"
    local membership=${COWORKERD_MEMBERSHIP_BACKEND:-objectStore}
    check COWORKERD_MEMBERSHIP_BACKEND "$membership" "$MEMBERSHIP"
    [[ -s $credentials ]] || fail "no storage credentials at $credentials (run init first)"
    local extra=""
    # etcd's user and the cluster key, as the secrets step distributed them; the control plane's address, which
    # the egress policy denies as the cluster's own.
    local etcdCredentials=${COWORKERD_ETCD_CREDENTIALS_FILE:-$SECRETS/etcd.json}
    if [[ -s $etcdCredentials ]]; then
      check COWORKERD_ETCD_CREDENTIALS_FILE "$etcdCredentials" "$PATHNAME"
      extra+=$',\n  "etcdCredentialsFile": "'"$etcdCredentials"'"'
    fi
    local peerKey=${COWORKERD_PEER_KEY_FILE:-$SECRETS/cluster-key.json}
    if [[ -s $peerKey ]]; then
      check COWORKERD_PEER_KEY_FILE "$peerKey" "$PATHNAME"
      extra+=$',\n  "peerAuth": {"keyFile": "'"$peerKey"'"}'
    fi
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
  "etcdEndpoint": "$etcd",
  "membership": {"backend": "$membership"},
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
    local etcd=${COWORKERD_ETCD_ENDPOINT:-} storage=${COWORKERD_STORAGE_ENDPOINT:-}
    [[ -n $etcd && -n $storage ]] || fail "set COWORKERD_ETCD_ENDPOINT and COWORKERD_STORAGE_ENDPOINT (or COWORKERD_CONFIG)"
    local credentials=${COWORKERD_DEPLOY_CREDENTIALS_FILE:-$SECRETS/deploy.json}
    local listen=${COWORKERD_CONTROL_LISTEN_ADDRESS:-0.0.0.0:8940}
    local bucket=${COWORKERD_STORAGE_BUCKET:-coworkerd} region=${COWORKERD_STORAGE_REGION:-us-east-1}
    check COWORKERD_ETCD_ENDPOINT "$etcd" "$ENDPOINT"
    check COWORKERD_STORAGE_ENDPOINT "$storage" "$ENDPOINT"
    check COWORKERD_STORAGE_BUCKET "$bucket" "$NAME"
    check COWORKERD_STORAGE_REGION "$region" "$NAME"
    check COWORKERD_DEPLOY_CREDENTIALS_FILE "$credentials" "$PATHNAME"
    check COWORKERD_CONTROL_LISTEN_ADDRESS "$listen" "$ADDRESS"
    check COWORKERD_STACK_DIR "$STACK" "$PATHNAME"
    local membership=${COWORKERD_MEMBERSHIP_BACKEND:-objectStore}
    check COWORKERD_MEMBERSHIP_BACKEND "$membership" "$MEMBERSHIP"
    [[ -s $credentials ]] || fail "no deploy credentials at $credentials (run init first)"
    mkdir -p "$STACK/control-tmp"
    # etcd's user coworkerd-apiserver, as the secrets step distributed it.
    local etcdCredentials=${COWORKERD_ETCD_CREDENTIALS_FILE:-$SECRETS/etcd.json} etcdLine=""
    if [[ -s $etcdCredentials ]]; then
      check COWORKERD_ETCD_CREDENTIALS_FILE "$etcdCredentials" "$PATHNAME"
      etcdLine=$'\n  "etcdCredentialsFile": "'"$etcdCredentials"'",'
    fi
    config=$STACK/control.json
    cat >"$config.next" <<JSON
{$etcdLine
  "etcdEndpoint": "$etcd",
  "membership": {"backend": "$membership"},
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
  local nodes=${COWORKERD_NODE_IDS:-node-1,node-2,node-3}
  [[ $nodes =~ ^[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*$ ]] || fail "COWORKERD_NODE_IDS is not valid: '$nodes'"
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
    rotate-etcd-password)
      # A new password for etcd's user of a service directory (root's is changed with etcdctl, docs/guide/install.md).
      local directory=${2:-} file user
      [[ $directory =~ ^(apiserver|nodes/[A-Za-z0-9._-]+)$ && -s $SECRETS/$directory/etcd.json ]] ||
        fail "secrets rotate-etcd-password takes apiserver or nodes/<id>"
      file=$SECRETS/$directory/etcd.json
      [[ $(<"$file") =~ \"user\":\"([A-Za-z0-9._-]+)\" ]] || fail "cannot read the user of $file"
      user=${BASH_REMATCH[1]}
      printf '{"user":"%s","password":"%s"}\n' "$user" "$(random 24)" >"$file.next" && mv "$file.next" "$file"
      printf '[secrets] {"event":"etcd-password","user":"%s","next":"run init: it sets every user'"'"'s password from its file"}\n' "$user"
      return ;;
    *) fail "secrets takes no argument, rotate-cluster-key add|promote|retire, or rotate-etcd-password DIR" ;;
  esac
  # The object store's two identities and SeaweedFS's file of them: deploy writes everything; the nodes read, list and
  # write -- their membership's objects under members/<id>/; SeaweedFS's rights are by bucket here, so the whole bucket.
  if [[ ! -s $SECRETS/storage/s3.json ]]; then
    local deployAccess deploySecret serverAccess serverSecret
    deployAccess=$(random 12) deploySecret=$(random 24) serverAccess=$(random 12) serverSecret=$(random 24)
    printf '{"accessKey":"%s","secretKey":"%s"}\n' "$deployAccess" "$deploySecret" >"$SECRETS/apiserver/deploy.json"
    printf '{"accessKey":"%s","secretKey":"%s"}\n' "$serverAccess" "$serverSecret" >"$SECRETS/init/server.json"
    printf '{"identities":[{"name":"deploy","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write","List"]},{"name":"server","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Read","Write","List"]}]}\n' \
      "$deployAccess" "$deploySecret" "$serverAccess" "$serverSecret" >"$SECRETS/storage/s3.json.next"
    mv "$SECRETS/storage/s3.json.next" "$SECRETS/storage/s3.json"
    printf '[secrets] {"event":"storage-credentials","written":true}\n'
  fi
  # SeaweedFS's S3 gateway also serves a gRPC configuration interface of its own beside S3, on the same addresses and
  # without credentials. security.toml (read by the storage container) makes it require a client certificate of a
  # certificate authority whose key is not kept, so that nobody can use it; the master, volume and filer servers listen
  # on the storage container's loopback only (docker-compose.yml).
  if [[ ! -s $SECRETS/storage/security.toml ]]; then
    local tls
    tls=$(mktemp -d)
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 36500 -subj /CN=coworkerd-storage-grpc \
      -keyout "$tls/ca.key" -out "$SECRETS/storage/grpc-ca.crt" 2>/dev/null
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=localhost \
      -keyout "$SECRETS/storage/grpc-s3.key" -out "$tls/s3.csr" 2>/dev/null
    openssl x509 -req -in "$tls/s3.csr" -CA "$SECRETS/storage/grpc-ca.crt" -CAkey "$tls/ca.key" -set_serial 1 -days 36500 \
      -out "$SECRETS/storage/grpc-s3.crt" 2>/dev/null
    rm -rf "$tls"
    chmod 0600 "$SECRETS/storage/grpc-s3.key"
    printf '[grpc]\nca = "/config/grpc-ca.crt"\n\n[grpc.s3]\ncert = "/config/grpc-s3.crt"\nkey = "/config/grpc-s3.key"\n' \
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
  # The cluster key: the nodes sign their peer connections with it.
  if [[ ! -s $SECRETS/init/cluster-key.json ]]; then
    printf '{"keys":[{"id":"k%s","secret":"%s"}]}\n' "$(date -u +%Y%m%d%H%M%S)" "$(random 32)" >"$SECRETS/init/cluster-key.json.next"
    mv "$SECRETS/init/cluster-key.json.next" "$SECRETS/init/cluster-key.json"
    printf '[secrets] {"event":"cluster-key","generated":true}\n'
  fi
  # etcd's users: root (init only), the control plane, one per node; init gives them their roles.
  user() {
    local file=$1 name=$2
    if [[ ! -s $file ]]; then
      printf '{"user":"%s","password":"%s"}\n' "$name" "$(random 24)" >"$file.next" && mv "$file.next" "$file"
      printf '[secrets] {"event":"etcd-user","user":"%s","generated":true}\n' "$name"
    fi
  }
  user "$SECRETS/init/etcd-root.json" root
  user "$SECRETS/apiserver/etcd.json" coworkerd-apiserver
  [[ -s $SECRETS/init/management-token ]] && place "$SECRETS/init/management-token" "$SECRETS/apiserver/management-token"
  [[ -s $SECRETS/init/admin-credential ]] && place "$SECRETS/init/admin-credential" "$SECRETS/apiserver/admin-credential"
  local id
  for id in ${nodes//,/ }; do
    mkdir -p "$SECRETS/nodes/$id"
    user "$SECRETS/nodes/$id/etcd.json" "coworkerd-node-$id"
    place "$SECRETS/init/server.json" "$SECRETS/nodes/$id/server.json"
    place "$SECRETS/init/cluster-key.json" "$SECRETS/nodes/$id/cluster-key.json"
    [[ -s $SECRETS/init/management-token ]] && place "$SECRETS/init/management-token" "$SECRETS/nodes/$id/management-token"
  done
  printf '[secrets] {"event":"ready","nodes":"%s"}\n' "$nodes"
}

init() {
  local storage=${COWORKERD_STORAGE_ENDPOINT:-http://storage:8333} bucket=${COWORKERD_STORAGE_BUCKET:-coworkerd}
  check COWORKERD_STORAGE_ENDPOINT "$storage" "$ENDPOINT"
  check COWORKERD_STORAGE_BUCKET "$bucket" "$NAME"
  umask 077
  [[ -s $SECRETS/storage/s3.json && -s $SECRETS/apiserver/deploy.json && -s $SECRETS/init/server.json ]] ||
    fail "no credentials in $SECRETS (the secrets step writes them first)"
  # The bucket first (SeaweedFS answers 200 to an object PUT into a missing bucket and cannot read it
  # back), then the self-check. The store may still be starting: retry for up to two minutes.
  local started=$SECONDS attempt=0
  until AWS_ACCESS_KEY_ID=$(credential "$SECRETS/apiserver/deploy.json" accessKey) \
        AWS_SECRET_ACCESS_KEY=$(credential "$SECRETS/apiserver/deploy.json" secretKey) \
        COWORKERD_READ_ACCESS_KEY=$(credential "$SECRETS/init/server.json" accessKey) \
        COWORKERD_READ_SECRET_KEY=$(credential "$SECRETS/init/server.json" secretKey) \
        coworkerd check storage "$storage" "$bucket" >"$SECRETS/init/.probe.log" 2>&1; do
    attempt=$((attempt + 1))
    if ((SECONDS - started > 120)); then cat "$SECRETS/init/.probe.log" >&2; fail "the object store did not pass its self-check"; fi
    sleep 1
  done
  printf '[init] {"event":"storage","bucket":"%s","attempts":%d,"seconds":%d}\n' "$bucket" "$((attempt + 1))" "$((SECONDS - started))"
  printf '[init] %s\n' "$(grep -o '{"contract".*' "$SECRETS/init/.probe.log")"
  # etcd's users, roles and authentication: the roles of the v2 layout -- the control plane writes tenants/,
  # credentials/ and deploy/, each node only its own nodes/<id>/ -- each user's password from its file, then
  # authentication on; anonymous requests are refused from here on.
  local etcd=${COWORKERD_ETCD_ENDPOINT:-http://etcd:2379} state="" args=() id
  check COWORKERD_ETCD_ENDPOINT "$etcd" "$ENDPOINT"
  args=(--endpoint "$etcd" --root "$SECRETS/init/etcd-root.json" --apiserver "$SECRETS/apiserver/etcd.json")
  for id in "$SECRETS"/nodes/*/; do
    id=${id%/}; id=${id##*/}
    [[ -s $SECRETS/nodes/$id/etcd.json ]] && args+=(--node "$id=$SECRETS/nodes/$id/etcd.json")
  done
  started=$SECONDS
  until coworkerd-apiserver tool etcd-auth "${args[@]}" >"$SECRETS/init/.etcd-auth.log" 2>&1; do
    if ((SECONDS - started > 120)); then cat "$SECRETS/init/.etcd-auth.log" >&2; fail "etcd did not take its users and roles"; fi
    sleep 1
  done
  printf '[init] %s\n' "$(grep -o '{"event":"etcd-auth".*' "$SECRETS/init/.etcd-auth.log")"
  # A Server starts only once the cluster's deployment state exists (an empty prefix may be a wrong
  # endpoint). A new cluster gets an empty one: generation 1, nothing deployed -- written by the
  # deployment tool as the control plane's etcd user, and only while nothing is there at all.
  export COWORKERD_ETCD_CREDENTIALS_FILE=$SECRETS/apiserver/etcd.json
  started=$SECONDS
  until state=$(coworkerd-apiserver tool show --endpoint "$etcd" 2>"$SECRETS/init/.show.log"); do
    if ((SECONDS - started > 120)); then cat "$SECRETS/init/.show.log" >&2; fail "etcd did not answer"; fi
    sleep 1
  done
  if [[ $state == '{"generation":0,'* && $state == *'"activeVersions":{},"routes":[],'* ]]; then
    local empty
    empty=$(mktemp)
    printf '{"compatibilityDate":"2026-10-01","routes":[],"workers":[]}\n' >"$empty"
    COWORKERD_STORAGE_ENDPOINT=$storage AWS_ACCESS_KEY_ID=$(credential "$SECRETS/apiserver/deploy.json" accessKey) \
      AWS_SECRET_ACCESS_KEY=$(credential "$SECRETS/apiserver/deploy.json" secretKey) \
      coworkerd-apiserver tool upsert --endpoint "$etcd" --manifest "$empty" --no-trial >/dev/null
    rm -f "$empty"
    printf '[init] {"event":"deployment-state","initialized":true}\n'
  else
    printf '[init] {"event":"deployment-state","initialized":false}\n'
  fi
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
  local etcd=${COWORKERD_ETCD_ENDPOINT:-http://etcd:2379}
  export COWORKERD_STORAGE_ENDPOINT=${COWORKERD_STORAGE_ENDPOINT:-http://storage:8333}
  # etcd's user coworkerd-apiserver (the tool writes the deployment state as the control plane does).
  if [[ -z ${COWORKERD_ETCD_CREDENTIALS_FILE:-} && -s $SECRETS/etcd.json ]]; then
    export COWORKERD_ETCD_CREDENTIALS_FILE=$SECRETS/etcd.json
  fi
  if [[ -s $SECRETS/deploy.json ]]; then
    AWS_ACCESS_KEY_ID=$(credential "$SECRETS/deploy.json" accessKey)
    AWS_SECRET_ACCESS_KEY=$(credential "$SECRETS/deploy.json" secretKey)
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  fi
  local args=("$@") given=false membershipGiven=false argument
  for argument in "${args[@]}"; do
    [[ $argument == --endpoint ]] && given=true
    [[ $argument == --membership ]] && membershipGiven=true
  done
  local membership=${COWORKERD_MEMBERSHIP_BACKEND:-objectStore}
  check COWORKERD_MEMBERSHIP_BACKEND "$membership" "$MEMBERSHIP"
  case ${1:-} in
    apply|upsert|remove|show|secret) $given || args+=(--endpoint "$etcd") ;;
  esac
  # The trial loads go to the members where they live.
  case ${1:-} in
    apply|upsert) $membershipGiven || args+=(--membership "$membership") ;;
  esac
  exec coworkerd-apiserver tool "${args[@]}"
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

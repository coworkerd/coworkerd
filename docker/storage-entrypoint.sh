#!/bin/sh
# Run under Compose's init: BusyBox timeout leaves orphaned watchdog children.
# Keep shell output in a private directory: s3.configure prints credentials.

reload() {
  state=$1
  cd /tmp || exit 1
  while sleep 2; do
    for file in /config/hotload/*.weed; do
      [ -f "$file" ] || continue
      id=${file##*/}; id=${id%.weed}
      case "$id" in *.remove) id=${id%.remove}; want=no;; *) want=yes;; esac
      case "$id" in ''|.|..|*[!A-Za-z0-9._-]*) continue;; esac
      # secrets replaces files atomically. Apply and hash the same private snapshot,
      # even when add/remove/rejoin replaces the source during a command.
      cp "$file" "$state/input" 2>/dev/null || continue
      sum=$(sha256sum < "$state/input") || continue
      token="$want $sum"
      [ "$(cat "$state/applied/$id" 2>/dev/null)" = "$token" ] && continue
      if timeout -k 2 60 /usr/bin/weed shell < "$state/input" > /dev/null 2>&1 &&
          timeout -k 2 60 /usr/bin/weed shell < "$state/query" > "$state/output" 2>/dev/null &&
          grep -Fq '"identities":' "$state/output"; then
        # Consume the whole command output before searching, so an early match
        # cannot close its pipe or hide a later command failure.
        tr -d '[:space:]' < "$state/output" > "$state/compact" || continue
        if grep -Fq "\"name\":\"node-$id\"" "$state/compact"; then have=yes; else have=no; fi
        if [ "$have" = "$want" ]; then
          printf '%s\n' "$token" > "$state/applied/$id"
          printf '[storage-hotload] applied %s %s\n' "$id" "$want"
          continue
        fi
      fi
      printf '[storage-hotload] retry %s\n' "$id" >&2
    done
  done
}

if [ "${1:-}" = reload ]; then
  reload "$2"
  exit
fi

server=
loader=
state=
stop() {
  trap '' TERM INT HUP
  trap - EXIT
  server_status=0
  # The reloader has its own process group, including commands and watchdogs.
  # It has no storage writes to drain; stop it before signalling the server.
  if [ -n "$loader" ]; then
    kill -KILL -"$loader" "$loader" 2>/dev/null || :
    wait "$loader" 2>/dev/null || :
  fi
  if [ -n "$server" ] && kill -0 "$server" 2>/dev/null; then
    kill -TERM "$server" 2>/dev/null || :
    # Allow the filer's gRPC drain and the volume's final heartbeat delay.
    # A forced stop must remain visible in the supervisor's exit status.
    setsid /bin/sh -c 'sleep 35; echo "[storage-hotload] storage shutdown timed out" >&2; kill -KILL "$1" 2>/dev/null' storage-stop "$server" &
    deadline=$!
    wait "$server" 2>/dev/null
    server_status=$?
    # The leader may not have reached setsid yet when the store exits quickly.
    kill -KILL -"$deadline" "$deadline" 2>/dev/null || :
    wait "$deadline" 2>/dev/null || :
  fi
  [ -z "$state" ] || rm -rf "$state"
  return "$server_status"
}
trap 'stop; exit $?' TERM INT HUP
trap stop EXIT

until [ -s /config/s3.json ] && [ -s /config/security.toml ]; do sleep 0.2; done
umask 077
state=$(mktemp -d /tmp/coworkerd-storage.XXXXXX) || exit 1
mkdir "$state/applied" || exit 1
printf '%s\n' s3.configure > "$state/query"
cd /config || exit 1
/usr/bin/weed server -ip=127.0.0.1 -ip.bind=127.0.0.1 -s3.ip.bind=0.0.0.0 -dir=/data -volume.max=20 \
  -master.volumeSizeLimitMB=64 -s3 -s3.config=/config/s3.json &
server=$!
setsid /bin/sh "$0" reload "$state" &
loader=$!
# BusyBox wait -n can miss a child that exited before the wait began. Its
# ordinary wait retains that status; poll liveness before collecting it.
while kill -0 "$server" 2>/dev/null && kill -0 "$loader" 2>/dev/null; do sleep 1; done
if kill -0 "$server" 2>/dev/null; then
  echo '[storage-hotload] reloader exited; stopping storage' >&2
  status=1
else
  wait "$server"
  status=$?
fi
exit "$status"

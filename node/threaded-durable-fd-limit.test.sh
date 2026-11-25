#!/bin/sh
set -eu
ulimit -n 512
exec "${0}.target" "$@"

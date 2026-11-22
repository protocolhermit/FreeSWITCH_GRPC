#!/bin/sh
# The module may export exactly one defined symbol: FreeSWITCH's entry point.
# gRPC, protobuf and abseil must not leak into the process.
set -eu
exported=$(nm -D --defined-only "$1" | awk '$2 ~ /^[A-Zu]$/ {print $3}' | sort)
if [ "$exported" != "mod_grpc_module_interface" ]; then
  echo "unexpected exported symbols:" >&2
  echo "$exported" | head -20 >&2
  exit 1
fi

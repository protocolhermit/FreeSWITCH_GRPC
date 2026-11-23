#!/bin/sh
# mod_grpc: FreeSWITCH control-plane module (gRPC)
# Copyright (C) 2026 Protocol Hermit
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# The module may export exactly one defined symbol: FreeSWITCH's entry point.
# gRPC, protobuf and abseil must not leak into the process.
set -eu
exported=$(nm -D --defined-only "$1" | awk '$2 ~ /^[A-Zu]$/ {print $3}' | sort)
if [ "$exported" != "mod_grpc_module_interface" ]; then
  echo "unexpected exported symbols:" >&2
  echo "$exported" | head -20 >&2
  exit 1
fi

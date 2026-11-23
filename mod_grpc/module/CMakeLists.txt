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

# Built only with -DFS_PREFIX=/usr/local/freeswitch (an installed FreeSWITCH).
find_package(PkgConfig REQUIRED)
set(ENV{PKG_CONFIG_PATH} "${FS_PREFIX}/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")
pkg_check_modules(FS REQUIRED IMPORTED_TARGET freeswitch)
find_package(CURL REQUIRED)

add_library(mod_grpc MODULE mod_grpc.c)
set_target_properties(mod_grpc PROPERTIES PREFIX "" LINKER_LANGUAGE CXX)
target_link_libraries(mod_grpc PRIVATE PkgConfig::FS grpc_bridge CURL::libcurl)
# One C++ runtime inside the module, and only the FreeSWITCH entry point exported.
# nodelete: gRPC keeps background threads that outlive gb_destroy, so `unload mod_grpc`
# must not dlclose (unmap) the code they run; without it FreeSWITCH segfaults on unload.
target_link_options(mod_grpc PRIVATE -static-libstdc++ -static-libgcc -Wl,-z,nodelete
  -Wl,--exclude-libs,ALL -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exports.map)

install(TARGETS mod_grpc LIBRARY DESTINATION ${FS_PREFIX}/lib/freeswitch/mod)

add_test(NAME exported_symbols
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/check_symbols.sh $<TARGET_FILE:mod_grpc>)

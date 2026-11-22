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

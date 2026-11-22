cmake_minimum_required(VERSION 3.16)
project(mod_grpc C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 99)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
add_compile_options(-Wall -Wextra -Wno-unused-parameter)

# -DSANITIZE=address or -DSANITIZE=thread
set(SANITIZE "" CACHE STRING "sanitizer for the test build")
if(SANITIZE)
  add_compile_options(-fsanitize=${SANITIZE} -fno-sanitize-recover=all -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${SANITIZE} -fno-sanitize-recover=all)
endif()

find_package(Threads REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Generate the C++ stubs from the one shared contract.
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../proto)
set(GEN ${CMAKE_CURRENT_BINARY_DIR}/gen)
set(GEN_SRCS ${GEN}/controlplane/v1/control.pb.cc ${GEN}/controlplane/v1/control.grpc.pb.cc)
file(MAKE_DIRECTORY ${GEN})
add_custom_command(
  OUTPUT ${GEN_SRCS}
  COMMAND $<TARGET_FILE:protobuf::protoc> -I ${PROTO_DIR} --cpp_out=${GEN} --grpc_out=${GEN}
          --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
          ${PROTO_DIR}/controlplane/v1/control.proto
  DEPENDS ${PROTO_DIR}/controlplane/v1/control.proto)
add_library(gbproto STATIC ${GEN_SRCS})
target_include_directories(gbproto PUBLIC ${GEN})
target_link_libraries(gbproto PUBLIC gRPC::grpc++ protobuf::libprotobuf)


file(GLOB BRIDGE_SRCS CONFIGURE_DEPENDS bridge/*.cc)
add_library(grpc_bridge STATIC ${BRIDGE_SRCS})
target_include_directories(grpc_bridge PUBLIC bridge)
target_link_libraries(grpc_bridge PUBLIC gbproto Threads::Threads)

enable_testing()
# ponytail: tests/ is not in the published repo; build them only where they exist
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/tests/CMakeLists.txt)
  add_subdirectory(tests)
endif()


if(FS_PREFIX)
  add_subdirectory(module)
endif()

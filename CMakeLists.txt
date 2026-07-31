cmake_minimum_required(VERSION 3.10)

project(miniKV VERSION 0.1 LANGUAGES CXX)

include(CTest)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Build type" FORCE)
endif()
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "RelWithDebInfo" "MinSizeRel")

option(MINIKV_ENABLE_FRAME_POINTERS
    "Keep frame pointers for perf and flame graph profiling" ON)
if (MINIKV_ENABLE_FRAME_POINTERS)
    add_compile_options(-fno-omit-frame-pointer)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

#统一输出路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

file(GLOB NET_SRC src/network/*.cpp)
file(GLOB STORAGE_SRC src/storage/*.cpp)
file(GLOB UTILS_SRC src/utils/*.cpp)
# file(GLOB GATEWAY_SRC src/gateway/*.cpp)
file(GLOB HTTP_SRC src/http/*.cpp)
# file(GLOB AGENT_SRC src/agent/*.cpp)

file(GLOB_RECURSE GATEWAY_SRC src/gateway/*.cpp)
list(REMOVE_ITEM GATEWAY_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/gateway/gateway_main.cpp") # 排除 gateway_main.cpp


file(GLOB_RECURSE DATANODE_SRC src/DataNode/*.cpp)
list(REMOVE_ITEM DATANODE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/DataNode/datanode_main.cpp") # 排除 gateway_main.cpp

file(GLOB_RECURSE AGENT_SRC src/agent/*.cpp)
list(REMOVE_ITEM AGENT_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/agent/node_agent_main.cpp") # 排除 gateway_main.cpp

add_library(minikv_net STATIC ${NET_SRC})
add_library(minikv_storage STATIC ${STORAGE_SRC})
add_library(minikv_utils STATIC ${UTILS_SRC})
add_library(minikv_gateway STATIC ${GATEWAY_SRC})
add_library(minikv_http STATIC ${HTTP_SRC})
add_library(minikv_agent STATIC ${AGENT_SRC})
add_library(minikv_datanode STATIC ${DATANODE_SRC})

target_include_directories(minikv_net PUBLIC include)
target_include_directories(minikv_storage PUBLIC include)
target_include_directories(minikv_utils PUBLIC include)
target_include_directories(minikv_gateway PUBLIC include)
target_include_directories(minikv_http PUBLIC include)
target_include_directories(minikv_agent PUBLIC include)
target_include_directories(minikv_datanode PUBLIC include)

find_library(LEVELDB_LIB leveldb REQUIRED)
find_path(LEVELDB_INC leveldb/db.h REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(yaml-cpp REQUIRED)

add_library(minikv_benchmark STATIC
    src/benchmark/BenchmarkCli.cpp
    src/benchmark/BenchmarkHttpClient.cpp
    src/benchmark/BenchmarkInput.cpp
    src/benchmark/BenchmarkRunner.cpp
    src/benchmark/BenchmarkTypes.cpp
)
target_include_directories(minikv_benchmark PUBLIC include)
target_link_libraries(minikv_benchmark PUBLIC minikv_utils OpenSSL::Crypto pthread)


add_executable(minikv_v2_gateway src/gateway/gateway_main.cpp)
target_include_directories(minikv_v2_gateway PRIVATE ${LEVELDB_INC})
target_link_libraries(minikv_v2_gateway PRIVATE
    minikv_gateway
    minikv_http
    minikv_net
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(minikv_v2_datanode src/DataNode/datanode_main.cpp)
target_include_directories(minikv_v2_datanode PRIVATE ${LEVELDB_INC})
target_link_libraries(minikv_v2_datanode PRIVATE
    minikv_datanode
    minikv_http
    minikv_net
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(minikv_v2_node_agent src/agent/node_agent_main.cpp)
target_link_libraries(minikv_v2_node_agent PRIVATE
    minikv_agent
    minikv_utils
    yaml-cpp
    pthread
)

add_executable(test_async_http_timeout test/test_async_http_timeout.cpp)
target_link_libraries(test_async_http_timeout PRIVATE
    minikv_http
    minikv_net
    pthread
)

add_executable(test_http_stream_context test/test_http_stream_context.cpp)
target_link_libraries(test_http_stream_context PRIVATE
    minikv_http
    minikv_net
    pthread
)

add_executable(test_fast_data_store_read test/test_fast_data_store_read.cpp)
target_include_directories(test_fast_data_store_read PRIVATE ${LEVELDB_INC})
target_link_libraries(test_fast_data_store_read PRIVATE
    minikv_datanode
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_fast_data_store_region test/test_fast_data_store_region.cpp)
target_include_directories(test_fast_data_store_region PRIVATE ${LEVELDB_INC})
target_link_libraries(test_fast_data_store_region PRIVATE
    minikv_datanode
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_fast_data_store_delete test/test_fast_data_store_delete.cpp)
target_include_directories(test_fast_data_store_delete PRIVATE ${LEVELDB_INC})
target_link_libraries(test_fast_data_store_delete PRIVATE
    minikv_datanode
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_tcp_sendfile_offset test/test_tcp_sendfile_offset.cpp)
target_link_libraries(test_tcp_sendfile_offset PRIVATE
    minikv_net
    pthread
)

add_executable(test_gateway_write_lease test/test_gateway_write_lease.cpp)
target_include_directories(test_gateway_write_lease PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_write_lease PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_manifest_snapshot test/test_gateway_manifest_snapshot.cpp)
target_include_directories(test_gateway_manifest_snapshot PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_manifest_snapshot PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_catalog test/test_gateway_catalog.cpp)
target_include_directories(test_gateway_catalog PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_catalog PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_delete test/test_gateway_delete.cpp)
target_include_directories(test_gateway_delete PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_delete PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_write_admission test/test_write_admission.cpp)
target_include_directories(test_write_admission PRIVATE include)
target_link_libraries(test_write_admission PRIVATE pthread)

add_executable(test_benchmark_types test/test_benchmark_types.cpp)
target_include_directories(test_benchmark_types PRIVATE include)
target_link_libraries(test_benchmark_types PRIVATE minikv_benchmark)

add_executable(test_benchmark_input test/test_benchmark_input.cpp)
target_include_directories(test_benchmark_input PRIVATE include)
target_link_libraries(test_benchmark_input PRIVATE minikv_benchmark)

add_executable(test_benchmark_http test/test_benchmark_http.cpp)
target_include_directories(test_benchmark_http PRIVATE include)
target_link_libraries(test_benchmark_http PRIVATE minikv_benchmark)

add_executable(test_benchmark_cli test/test_benchmark_cli.cpp)
target_include_directories(test_benchmark_cli PRIVATE include)
target_link_libraries(test_benchmark_cli PRIVATE minikv_benchmark)

add_executable(minikv_v2_bench src/benchmark/benchmark_main.cpp)
target_link_libraries(minikv_v2_bench PRIVATE minikv_benchmark)

if(BUILD_TESTING)
    add_test(NAME async_http_timeout COMMAND test_async_http_timeout)
    add_test(NAME http_stream_context COMMAND test_http_stream_context)
    add_test(NAME fast_data_store_read COMMAND test_fast_data_store_read)
    add_test(NAME fast_data_store_region COMMAND test_fast_data_store_region)
    add_test(NAME fast_data_store_delete COMMAND test_fast_data_store_delete)
    add_test(NAME tcp_sendfile_offset COMMAND test_tcp_sendfile_offset)
    add_test(NAME gateway_write_lease COMMAND test_gateway_write_lease)
    add_test(NAME gateway_manifest_snapshot COMMAND test_gateway_manifest_snapshot)
    add_test(NAME gateway_catalog COMMAND test_gateway_catalog)
    add_test(NAME gateway_delete COMMAND test_gateway_delete)
    add_test(NAME datanode_delete
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test/test_v2_datanode_delete.sh
                          ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/minikv_v2_datanode)
    add_test(NAME v2_blob_download_fallback
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test/test_v2_blob_download_fallback.sh)
    add_test(NAME write_admission COMMAND test_write_admission)
    add_test(NAME benchmark_types COMMAND test_benchmark_types)
    add_test(NAME benchmark_input COMMAND test_benchmark_input)
    add_test(NAME benchmark_http COMMAND test_benchmark_http)
    add_test(NAME benchmark_cli COMMAND test_benchmark_cli)
endif()

# 链接上面打包好的网络静态库
# target_link_libraries(minikv PRIVATE minikv_net)
# target_link_libraries(minikv PRIVATE minikv_storage)
# target_link_libraries(minikv PRIVATE minikv_utils)
# target_link_libraries(minikv PRIVATE minikv_gateway)
# target_link_libraries(minikv PRIVATE minikv_http)
# target_link_libraries(minikv PRIVATE minikv_agent)

# target_include_directories(minikv PRIVATE ${LEVELDB_INC})
# target_link_libraries(minikv PRIVATE ${LEVELDB_LIB} pthread)
# target_link_libraries(minikv PRIVATE OpenSSL::Crypto)
# target_link_libraries(minikv PRIVATE yaml-cpp)

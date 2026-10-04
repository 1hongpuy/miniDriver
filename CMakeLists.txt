cmake_minimum_required(VERSION 3.10)

project(miniKV VERSION 0.1 LANGUAGES CXX)

include(CTest)

if (NOT CMAKE_CONFIGURATION_TYPES)
    if (NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Build type" FORCE)
    endif()
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

option(MINIKV_ENABLE_FRAME_POINTERS
    "Keep frame pointers for perf and flame graph profiling" ON)
option(MINIKV_ENABLE_NURAFT
    "Build the MiniDriver 4.0 Metadata Raft adapter" OFF)
option(MINIKV_BUILD_QT_CLIENT
    "Build the MiniDriver Qt desktop client" OFF)
option(MINIKV_DESKTOP_CLIENT_ONLY
    "Build only the portable SDK and Qt desktop client (for native Windows delivery)" OFF)
set(MINIKV_NURAFT_SOURCE_DIR "" CACHE PATH
    "Path to a pinned NuRaft source tree (v2.1.0 is supported by CMake 3.22)")
if (MINIKV_ENABLE_FRAME_POINTERS)
    add_compile_options(-fno-omit-frame-pointer)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# The server side intentionally remains Linux/epoll based.  This small build
# graph is the supported native-Windows route: it contains only the public
# synchronous SDK, OpenSSL utilities, and the Qt client, not DataNode,
# LevelDB, K3s, or the Linux Reactor implementation.
if(MINIKV_DESKTOP_CLIENT_ONLY)
    if(NOT MINIKV_BUILD_QT_CLIENT)
        message(FATAL_ERROR "MINIKV_DESKTOP_CLIENT_ONLY requires MINIKV_BUILD_QT_CLIENT=ON")
    endif()
    find_package(OpenSSL REQUIRED)
    add_library(minikv_utils STATIC src/utils/Util.cpp)
    add_library(minikv_client_transport STATIC src/client/HttpTransport.cpp)
    add_library(minikv_client STATIC src/client/MiniDriverClient.cpp)
    target_include_directories(minikv_utils PUBLIC include)
    target_include_directories(minikv_client_transport PUBLIC include)
    target_include_directories(minikv_client PUBLIC include)
    target_link_libraries(minikv_utils PUBLIC OpenSSL::Crypto)
    target_link_libraries(minikv_client PUBLIC minikv_client_transport minikv_utils OpenSSL::Crypto)
    if(WIN32)
        target_link_libraries(minikv_client_transport PUBLIC ws2_32)
    else()
        target_link_libraries(minikv_client_transport PUBLIC pthread)
        target_link_libraries(minikv_client PUBLIC pthread)
    endif()
    add_subdirectory(QTClient)
    return()
endif()

#统一输出路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

file(GLOB NET_SRC src/network/*.cpp)
file(GLOB STORAGE_SRC src/storage/*.cpp)
file(GLOB UTILS_SRC src/utils/*.cpp)
file(GLOB_RECURSE MEDIA_SRC src/media/*.cpp)
list(REMOVE_ITEM MEDIA_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/media/thumbnail_worker_main.cpp")
# file(GLOB GATEWAY_SRC src/gateway/*.cpp)
file(GLOB HTTP_SRC src/http/*.cpp)
# file(GLOB AGENT_SRC src/agent/*.cpp)

file(GLOB_RECURSE GATEWAY_SRC src/gateway/*.cpp)
list(REMOVE_ITEM GATEWAY_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/gateway/gateway_main.cpp") # 排除 gateway_main.cpp


file(GLOB_RECURSE DATANODE_SRC src/DataNode/*.cpp)
list(REMOVE_ITEM DATANODE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/DataNode/datanode_main.cpp") # 排除 gateway_main.cpp

file(GLOB_RECURSE AGENT_SRC src/agent/*.cpp)
list(REMOVE_ITEM AGENT_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/agent/node_agent_main.cpp") # 排除 gateway_main.cpp
file(GLOB_RECURSE METADATA_SRC src/metadata/*.cpp)
list(REMOVE_ITEM METADATA_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/metadata/metadata_service_main.cpp")
list(REMOVE_ITEM METADATA_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/metadata/metadata_command_tool.cpp")
list(REMOVE_ITEM METADATA_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/metadata/raft/NuRaftAdapters.cpp")
file(GLOB_RECURSE SEARCH_SRC src/search/*.cpp)

add_library(minikv_net STATIC ${NET_SRC})
add_library(minikv_storage STATIC ${STORAGE_SRC})
add_library(minikv_utils STATIC ${UTILS_SRC})
add_library(minikv_media STATIC ${MEDIA_SRC})
add_library(minikv_gateway STATIC ${GATEWAY_SRC})
add_library(minikv_http STATIC ${HTTP_SRC})
add_library(minikv_agent STATIC ${AGENT_SRC})
add_library(minikv_datanode STATIC ${DATANODE_SRC})
add_library(minikv_metadata STATIC ${METADATA_SRC})
add_library(minikv_search STATIC ${SEARCH_SRC})
add_library(minikv_client_transport STATIC src/client/HttpTransport.cpp)
add_library(minikv_client STATIC src/client/MiniDriverClient.cpp)

add_executable(minidriver_edge_cache
    src/edge/edge_cache_main.cpp
    src/edge/EdgeCacheStore.cpp)

target_include_directories(minikv_net PUBLIC include)
target_include_directories(minikv_storage PUBLIC include)
target_include_directories(minikv_utils PUBLIC include)
target_include_directories(minikv_media PUBLIC include third_party)
target_include_directories(minikv_gateway PUBLIC include)
target_include_directories(minikv_http PUBLIC include)
target_include_directories(minikv_agent PUBLIC include)
target_include_directories(minikv_datanode PUBLIC include)
target_include_directories(minikv_metadata PUBLIC include)
target_include_directories(minikv_search PUBLIC include)
target_include_directories(minikv_client_transport PUBLIC include)
target_include_directories(minikv_client PUBLIC include)

find_library(LEVELDB_LIB leveldb REQUIRED)
find_path(LEVELDB_INC leveldb/db.h REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(spdlog REQUIRED)
find_path(HIREDIS_INC hiredis/hiredis.h REQUIRED)
find_library(HIREDIS_LIB hiredis REQUIRED)
find_path(LIBRAW_INC libraw/libraw.h REQUIRED)
find_library(LIBRAW_LIB raw REQUIRED)

target_link_libraries(minikv_utils PUBLIC spdlog::spdlog OpenSSL::Crypto)
target_include_directories(minikv_media PUBLIC ${HIREDIS_INC} ${LIBRAW_INC})
target_link_libraries(minikv_media PUBLIC minikv_utils ${HIREDIS_LIB} ${LIBRAW_LIB})
target_link_libraries(minikv_gateway PUBLIC minikv_media minikv_metadata)
target_link_libraries(minikv_http PUBLIC minikv_utils)
target_link_libraries(minikv_agent PUBLIC minikv_utils)
target_link_libraries(minikv_client_transport PUBLIC pthread)
target_link_libraries(minikv_client PUBLIC minikv_client_transport minikv_utils OpenSSL::Crypto pthread)
target_link_libraries(minidriver_edge_cache PRIVATE minikv_client minikv_http minikv_net minikv_utils pthread)
target_link_libraries(minikv_search PUBLIC pthread)
target_link_libraries(minikv_metadata PUBLIC OpenSSL::Crypto minikv_client_transport)

if(MINIKV_ENABLE_NURAFT)
    if(NOT MINIKV_NURAFT_SOURCE_DIR OR
       NOT EXISTS "${MINIKV_NURAFT_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "MINIKV_ENABLE_NURAFT requires -DMINIKV_NURAFT_SOURCE_DIR=<pinned NuRaft source>")
    endif()
    set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    add_subdirectory("${MINIKV_NURAFT_SOURCE_DIR}"
                     "${CMAKE_BINARY_DIR}/third_party/nuraft" EXCLUDE_FROM_ALL)
    if(TARGET NuRaft::static_lib)
        set(MINIKV_NURAFT_TARGET NuRaft::static_lib)
    elseif(TARGET static_lib)
        set(MINIKV_NURAFT_TARGET static_lib)
    else()
        message(FATAL_ERROR "NuRaft static library target was not created")
    endif()
    add_library(minikv_metadata_raft STATIC
        src/metadata/raft/NuRaftAdapters.cpp)
    target_include_directories(minikv_metadata_raft PUBLIC
        include ${LEVELDB_INC}
        "${MINIKV_NURAFT_SOURCE_DIR}/include")
    target_compile_definitions(minikv_metadata_raft PUBLIC
        MINIKV_ENABLE_NURAFT=1)
    target_link_libraries(minikv_metadata_raft PUBLIC
        minikv_metadata minikv_utils ${MINIKV_NURAFT_TARGET} ${LEVELDB_LIB}
        OpenSSL::SSL OpenSSL::Crypto pthread)
endif()

add_library(minikv_benchmark STATIC
    src/benchmark/BenchmarkCli.cpp
    src/benchmark/BenchmarkInput.cpp
    src/benchmark/BenchmarkRunner.cpp
    src/benchmark/BenchmarkTypes.cpp
)
target_include_directories(minikv_benchmark PUBLIC include)
target_link_libraries(minikv_benchmark PUBLIC minikv_client minikv_utils OpenSSL::Crypto pthread)


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
    minikv_metadata
    minikv_http
    minikv_net
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(minikv_v2_thumbnail_worker src/media/thumbnail_worker_main.cpp)
target_link_libraries(minikv_v2_thumbnail_worker PRIVATE
    minikv_media
    minikv_http
    minikv_net
    minikv_utils
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

add_executable(test_persistent_http_session test/test_persistent_http_session.cpp)
target_link_libraries(test_persistent_http_session PRIVATE
    minikv_http
    minikv_net
    pthread
)

add_executable(test_event_loop_thread test/test_event_loop_thread.cpp)
target_link_libraries(test_event_loop_thread PRIVATE minikv_net pthread)

add_executable(test_event_loop_thread_pool test/test_event_loop_thread_pool.cpp)
target_link_libraries(test_event_loop_thread_pool PRIVATE minikv_net pthread)

add_executable(test_tcp_connection_context test/test_tcp_connection_context.cpp)
target_link_libraries(test_tcp_connection_context PRIVATE minikv_net pthread)

add_executable(test_tcp_server_multi_reactor test/test_tcp_server_multi_reactor.cpp)
target_link_libraries(test_tcp_server_multi_reactor PRIVATE minikv_net pthread)

add_executable(test_http_server_multi_reactor test/test_http_server_multi_reactor.cpp)
target_link_libraries(test_http_server_multi_reactor PRIVATE
    minikv_http
    minikv_net
    minikv_utils
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

add_executable(test_fast_data_store_durability test/test_fast_data_store_durability.cpp)
target_include_directories(test_fast_data_store_durability PRIVATE ${LEVELDB_INC})
target_link_libraries(test_fast_data_store_durability PRIVATE
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

add_executable(test_http_file_response test/test_http_file_response.cpp)
target_include_directories(test_http_file_response PRIVATE include)

add_executable(test_gateway_write_lease test/test_gateway_write_lease.cpp)
target_include_directories(test_gateway_write_lease PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_write_lease PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_lock_manager test/test_gateway_lock_manager.cpp)
target_include_directories(test_gateway_lock_manager PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_lock_manager PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_reservation_concurrency test/test_gateway_reservation_concurrency.cpp)
target_include_directories(test_gateway_reservation_concurrency PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_reservation_concurrency PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_reservation_recovery test/test_gateway_reservation_recovery.cpp)
target_include_directories(test_gateway_reservation_recovery PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_reservation_recovery PRIVATE
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

add_executable(test_gateway_route_retry_identity test/test_gateway_route_retry_identity.cpp)
target_include_directories(test_gateway_route_retry_identity PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_route_retry_identity PRIVATE
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

add_executable(test_gateway_object_cache test/test_gateway_object_cache.cpp)
target_include_directories(test_gateway_object_cache PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_object_cache PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_catalog_cache test/test_gateway_catalog_cache.cpp)
target_include_directories(test_gateway_catalog_cache PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_catalog_cache PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_manifest_cache test/test_gateway_manifest_cache.cpp)
target_include_directories(test_gateway_manifest_cache PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_manifest_cache PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_metadata_lazy_load test/test_gateway_metadata_lazy_load.cpp)
target_include_directories(test_gateway_metadata_lazy_load PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_metadata_lazy_load PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_upload_preflight test/test_gateway_upload_preflight.cpp)
target_include_directories(test_gateway_upload_preflight PRIVATE ${LEVELDB_INC})
target_link_libraries(test_gateway_upload_preflight PRIVATE
    minikv_gateway
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_lru_cache test/test_lru_cache.cpp)
target_include_directories(test_lru_cache PRIVATE include)
target_link_libraries(test_lru_cache PRIVATE pthread)

add_executable(test_async_logger test/test_async_logger.cpp)
target_include_directories(test_async_logger PRIVATE include)
target_link_libraries(test_async_logger PRIVATE minikv_utils pthread)

add_executable(test_node_agent_logging_config test/test_node_agent_logging_config.cpp)
target_include_directories(test_node_agent_logging_config PRIVATE include)
target_link_libraries(test_node_agent_logging_config PRIVATE minikv_agent yaml-cpp pthread)

add_executable(test_node_agent_thumbnail_worker_config test/test_node_agent_thumbnail_worker_config.cpp)
target_include_directories(test_node_agent_thumbnail_worker_config PRIVATE include)
target_link_libraries(test_node_agent_thumbnail_worker_config PRIVATE minikv_agent yaml-cpp pthread)

add_executable(test_replica_upload_metrics test/test_replica_upload_metrics.cpp)
target_include_directories(test_replica_upload_metrics PRIVATE include)
target_link_libraries(test_replica_upload_metrics PRIVATE pthread)

add_executable(test_replica_connection_pool test/test_replica_connection_pool.cpp)
target_link_libraries(test_replica_connection_pool PRIVATE
    minikv_datanode
    minikv_http
    minikv_net
    pthread
)

add_executable(test_process_supervisor test/test_process_supervisor.cpp)
target_include_directories(test_process_supervisor PRIVATE include)
target_link_libraries(test_process_supervisor PRIVATE minikv_agent pthread)

add_executable(test_write_admission test/test_write_admission.cpp)
target_include_directories(test_write_admission PRIVATE include)
target_link_libraries(test_write_admission PRIVATE pthread)

add_executable(test_node_resource_governor test/test_node_resource_governor.cpp)
target_include_directories(test_node_resource_governor PRIVATE include)
target_link_libraries(test_node_resource_governor PRIVATE pthread)

add_executable(test_disk_write_executor test/test_disk_write_executor.cpp)
target_include_directories(test_disk_write_executor PRIVATE include)
target_link_libraries(test_disk_write_executor PRIVATE minikv_datanode pthread)

add_executable(test_chunk_disk_write_pipeline test/test_chunk_disk_write_pipeline.cpp)
target_include_directories(test_chunk_disk_write_pipeline PRIVATE include)

add_executable(test_chunk_write_protocol test/test_chunk_write_protocol.cpp)
target_include_directories(test_chunk_write_protocol PRIVATE include test)
target_link_libraries(test_chunk_write_protocol PRIVATE
    minikv_datanode
    minikv_http
    minikv_utils
    OpenSSL::Crypto
)

add_executable(test_read_capability test/test_read_capability.cpp)
target_include_directories(test_read_capability PRIVATE include test)
target_link_libraries(test_read_capability PRIVATE minikv_utils OpenSSL::Crypto)

add_executable(test_http_range test/test_http_range.cpp)
target_link_libraries(test_http_range PRIVATE minikv_http minikv_utils)

add_executable(test_edge_cache_store
    test/test_edge_cache_store.cpp
    src/edge/EdgeCacheStore.cpp)
target_link_libraries(test_edge_cache_store PRIVATE minikv_client minikv_utils OpenSSL::Crypto pthread)

add_executable(test_checksum_and_opaque_store test/test_checksum_and_opaque_store.cpp)
target_include_directories(test_checksum_and_opaque_store PRIVATE ${LEVELDB_INC} test)
target_link_libraries(test_checksum_and_opaque_store PRIVATE
    minikv_datanode
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_media_job test/test_media_job.cpp)
target_include_directories(test_media_job PRIVATE include test)
target_link_libraries(test_media_job PRIVATE minikv_media)

add_executable(test_gateway_media_jobs test/test_gateway_media_jobs.cpp)
target_include_directories(test_gateway_media_jobs PRIVATE ${LEVELDB_INC} include test)
target_link_libraries(test_gateway_media_jobs PRIVATE
    minikv_gateway
    minikv_media
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_ai_index_outbox test/test_gateway_ai_index_outbox.cpp)
target_include_directories(test_gateway_ai_index_outbox PRIVATE ${LEVELDB_INC} include test)
target_link_libraries(test_gateway_ai_index_outbox PRIVATE
    minikv_gateway
    minikv_media
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_gateway_derived_upload test/test_gateway_derived_upload.cpp)
target_include_directories(test_gateway_derived_upload PRIVATE ${LEVELDB_INC} include test)
target_link_libraries(test_gateway_derived_upload PRIVATE
    minikv_gateway
    minikv_media
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

add_executable(test_redis_task_publisher test/test_redis_task_publisher.cpp)
target_include_directories(test_redis_task_publisher PRIVATE include test)
target_link_libraries(test_redis_task_publisher PRIVATE minikv_media pthread)

add_executable(test_redis_ai_event_publisher test/test_redis_ai_event_publisher.cpp)
target_include_directories(test_redis_ai_event_publisher PRIVATE include test)
target_link_libraries(test_redis_ai_event_publisher PRIVATE minikv_media pthread)

add_executable(test_jpeg_thumbnail_generator test/test_jpeg_thumbnail_generator.cpp)
target_include_directories(test_jpeg_thumbnail_generator PRIVATE include third_party test)
target_link_libraries(test_jpeg_thumbnail_generator PRIVATE minikv_media pthread)

add_executable(test_raw_embedded_preview test/test_raw_embedded_preview.cpp)
target_include_directories(test_raw_embedded_preview PRIVATE include test)
target_link_libraries(test_raw_embedded_preview PRIVATE minikv_media pthread)

add_executable(test_redis_task_consumer test/test_redis_task_consumer.cpp)
target_include_directories(test_redis_task_consumer PRIVATE include test)
target_link_libraries(test_redis_task_consumer PRIVATE minikv_media pthread)
target_link_libraries(test_chunk_disk_write_pipeline PRIVATE
    minikv_datanode
    minikv_net
    minikv_utils
    ${LEVELDB_LIB}
    pthread
    OpenSSL::Crypto
)

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

add_executable(test_minidriver_client test/test_minidriver_client.cpp)
target_include_directories(test_minidriver_client PRIVATE include test)
target_link_libraries(test_minidriver_client PRIVATE minikv_client minikv_utils)

add_executable(test_metadata_state_machine test/test_metadata_state_machine.cpp)
target_include_directories(test_metadata_state_machine PRIVATE include test)
target_link_libraries(test_metadata_state_machine PRIVATE minikv_metadata)

add_executable(test_metadata_snapshot test/test_metadata_snapshot.cpp)
target_include_directories(test_metadata_snapshot PRIVATE include test)
target_link_libraries(test_metadata_snapshot PRIVATE minikv_metadata)

add_executable(test_metadata_snapshot_store test/test_metadata_snapshot_store.cpp)
target_include_directories(test_metadata_snapshot_store PRIVATE include test)
target_link_libraries(test_metadata_snapshot_store PRIVATE minikv_metadata)

add_executable(test_metadata_soft_state test/test_metadata_soft_state.cpp)
target_include_directories(test_metadata_soft_state PRIVATE include test)
target_link_libraries(test_metadata_soft_state PRIVATE minikv_metadata)

add_executable(test_metadata_service test/test_metadata_service.cpp)
target_include_directories(test_metadata_service PRIVATE include test)
target_link_libraries(test_metadata_service PRIVATE minikv_metadata)

if(MINIKV_ENABLE_NURAFT)
    add_executable(test_metadata_nuraft_adapter
        test/test_metadata_nuraft_adapter.cpp)
    target_include_directories(test_metadata_nuraft_adapter PRIVATE include test)
    target_link_libraries(test_metadata_nuraft_adapter PRIVATE
        minikv_metadata_raft)
endif()

add_executable(test_search_index test/test_search_index.cpp)
target_include_directories(test_search_index PRIVATE include)
target_link_libraries(test_search_index PRIVATE minikv_search pthread)

add_executable(minikv_search_node src/search_node_main.cpp)
target_include_directories(minikv_search_node PRIVATE include)
target_link_libraries(minikv_search_node PRIVATE minikv_search minikv_http minikv_net minikv_utils pthread)

add_executable(minikv_metadata_service src/metadata/metadata_service_main.cpp)
target_include_directories(minikv_metadata_service PRIVATE include)
target_link_libraries(minikv_metadata_service PRIVATE
    minikv_metadata minikv_http minikv_net minikv_utils OpenSSL::Crypto pthread)

add_executable(minikv_metadata_command_tool
    src/metadata/metadata_command_tool.cpp)
target_include_directories(minikv_metadata_command_tool PRIVATE include)
target_link_libraries(minikv_metadata_command_tool PRIVATE minikv_metadata)

if(MINIKV_ENABLE_NURAFT)
    add_executable(minikv_metadata_raft_service
        src/metadata/metadata_service_main.cpp)
    target_include_directories(minikv_metadata_raft_service PRIVATE include)
    target_compile_definitions(minikv_metadata_raft_service PRIVATE
        MINIKV_METADATA_RAFT_BINARY=1)
    target_link_libraries(minikv_metadata_raft_service PRIVATE
        minikv_metadata_raft minikv_http minikv_net minikv_utils
        ${MINIKV_NURAFT_TARGET} OpenSSL::SSL OpenSSL::Crypto pthread)
endif()

add_executable(minikv_v2_bench src/benchmark/benchmark_main.cpp)
target_link_libraries(minikv_v2_bench PRIVATE minikv_benchmark)

# A standalone diagnostic utility. It writes only a private scratch file under
# the directory supplied at runtime and is intentionally independent of the
# DataNode/LevelDB/Raft implementations.
add_executable(minikv_storage_sync_probe src/diagnostics/storage_sync_probe.cpp)

# Minimal native Worker entry point. It is intentionally a consumer of the
# public SDK rather than another DataNode/extent reader.
add_executable(minidriver_client_worker src/client/minidriver_client_worker_main.cpp)
target_link_libraries(minidriver_client_worker PRIVATE minikv_client)

if(MINIKV_BUILD_QT_CLIENT)
    add_subdirectory(QTClient)
endif()

if(BUILD_TESTING)
    add_test(NAME async_http_timeout COMMAND test_async_http_timeout)
    add_test(NAME persistent_http_session COMMAND test_persistent_http_session)
    add_test(NAME event_loop_thread COMMAND test_event_loop_thread)
    add_test(NAME event_loop_thread_pool COMMAND test_event_loop_thread_pool)
    add_test(NAME tcp_connection_context COMMAND test_tcp_connection_context)
    add_test(NAME tcp_server_multi_reactor COMMAND test_tcp_server_multi_reactor)
    add_test(NAME http_server_multi_reactor COMMAND test_http_server_multi_reactor)
    add_test(NAME http_stream_context COMMAND test_http_stream_context)
    add_test(NAME fast_data_store_read COMMAND test_fast_data_store_read)
    add_test(NAME fast_data_store_region COMMAND test_fast_data_store_region)
    add_test(NAME fast_data_store_delete COMMAND test_fast_data_store_delete)
    add_test(NAME fast_data_store_durability COMMAND test_fast_data_store_durability)
    add_test(NAME tcp_sendfile_offset COMMAND test_tcp_sendfile_offset)
    add_test(NAME http_file_response COMMAND test_http_file_response)
    add_test(NAME gateway_write_lease COMMAND test_gateway_write_lease)
    add_test(NAME gateway_lock_manager COMMAND test_gateway_lock_manager)
    add_test(NAME gateway_reservation_concurrency COMMAND test_gateway_reservation_concurrency)
    add_test(NAME gateway_reservation_recovery COMMAND test_gateway_reservation_recovery)
    add_test(NAME gateway_manifest_snapshot COMMAND test_gateway_manifest_snapshot)
    add_test(NAME gateway_route_retry_identity COMMAND test_gateway_route_retry_identity)
    add_test(NAME gateway_catalog COMMAND test_gateway_catalog)
    add_test(NAME gateway_delete COMMAND test_gateway_delete)
    add_test(NAME gateway_object_cache COMMAND test_gateway_object_cache)
    add_test(NAME gateway_catalog_cache COMMAND test_gateway_catalog_cache)
    add_test(NAME gateway_manifest_cache COMMAND test_gateway_manifest_cache)
    add_test(NAME gateway_metadata_lazy_load COMMAND test_gateway_metadata_lazy_load)
    add_test(NAME gateway_upload_preflight COMMAND test_gateway_upload_preflight)
    add_test(NAME lru_cache COMMAND test_lru_cache)
    add_test(NAME async_logger COMMAND test_async_logger)
    add_test(NAME node_agent_logging_config COMMAND test_node_agent_logging_config)
    add_test(NAME node_agent_thumbnail_worker_config COMMAND test_node_agent_thumbnail_worker_config)
    add_test(NAME replica_upload_metrics COMMAND test_replica_upload_metrics)
    add_test(NAME replica_connection_pool COMMAND test_replica_connection_pool)
    add_test(NAME process_supervisor COMMAND test_process_supervisor)
    add_test(NAME datanode_delete
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test/test_v2_datanode_delete.sh
                          ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/minikv_v2_datanode)
    add_test(NAME v2_blob_download_fallback
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test/test_v2_blob_download_fallback.sh)
    add_test(NAME v2_thumbnail_contact_sheet_ui
             COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/test/test_v2_thumbnail_contact_sheet_ui.py)
    add_test(NAME write_admission COMMAND test_write_admission)
    add_test(NAME node_resource_governor COMMAND test_node_resource_governor)
    add_test(NAME disk_write_executor COMMAND test_disk_write_executor)
    add_test(NAME chunk_disk_write_pipeline COMMAND test_chunk_disk_write_pipeline)
    add_test(NAME chunk_write_protocol COMMAND test_chunk_write_protocol)
    add_test(NAME read_capability COMMAND test_read_capability)
    add_test(NAME http_range COMMAND test_http_range)
    add_test(NAME edge_cache_store COMMAND test_edge_cache_store)
    add_test(NAME checksum_and_opaque_store COMMAND test_checksum_and_opaque_store)
    add_test(NAME media_job COMMAND test_media_job)
    add_test(NAME gateway_media_jobs COMMAND test_gateway_media_jobs)
    add_test(NAME gateway_ai_index_outbox COMMAND test_gateway_ai_index_outbox)
    add_test(NAME gateway_derived_upload COMMAND test_gateway_derived_upload)
    add_test(NAME redis_task_publisher COMMAND test_redis_task_publisher)
    add_test(NAME v2_ai_file_event_redis
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test/test_v2_ai_file_event_redis.sh
                          ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_redis_ai_event_publisher)
    add_test(NAME jpeg_thumbnail_generator COMMAND test_jpeg_thumbnail_generator)
    add_test(NAME raw_embedded_preview COMMAND test_raw_embedded_preview)
    add_test(NAME redis_task_consumer
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test/test_v2_redis_task_consumer.sh
                          ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_redis_task_consumer)
    add_test(NAME v2_media_task_redis
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test/test_v2_media_task_redis.sh
                          ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_redis_task_publisher)
    add_test(NAME benchmark_types COMMAND test_benchmark_types)
    add_test(NAME benchmark_input COMMAND test_benchmark_input)
    add_test(NAME benchmark_http COMMAND test_benchmark_http)
    add_test(NAME benchmark_cli COMMAND test_benchmark_cli)
    add_test(NAME minidriver_client COMMAND test_minidriver_client)
    add_test(NAME metadata_state_machine COMMAND test_metadata_state_machine)
    add_test(NAME metadata_snapshot COMMAND test_metadata_snapshot)
    add_test(NAME metadata_snapshot_store COMMAND test_metadata_snapshot_store)
    add_test(NAME metadata_soft_state COMMAND test_metadata_soft_state)
    add_test(NAME metadata_service COMMAND test_metadata_service)
    if(MINIKV_ENABLE_NURAFT)
        add_test(NAME metadata_nuraft_adapter COMMAND test_metadata_nuraft_adapter)
    endif()
    add_test(NAME search_index COMMAND test_search_index)
    add_test(NAME search_node_http
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/test/test_search_node_http.sh
                          ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/minikv_search_node)
    add_test(NAME search_document_replay
             COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/test/test_search_document_replay.py
                             ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/minikv_search_node)
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

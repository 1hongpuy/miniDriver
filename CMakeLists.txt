cmake_minimum_required(VERSION 3.10)

project(miniKV VERSION 0.1 LANGUAGES CXX)

set(CMAKE_BUILD_TYPE "Debug")
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





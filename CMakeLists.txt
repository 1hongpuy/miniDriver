cmake_minimum_required(VERSION 3.10)

project(miniKV VERSION 0.1 LANGUAGES CXX)

set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

file(GLOB NET_SRC src/network/*.cpp)
file(GLOB STORAGE_SRC src/storage/*.cpp)

add_library(minikv_net STATIC ${NET_SRC})
add_library(minikv_storage STATIC ${STORAGE_SRC})

target_include_directories(minikv_net PUBLIC include)
target_include_directories(minikv_storage PUBLIC include)

find_library(LEVELDB_LIB leveldb REQUIRED)
find_path(LEVELDB_INC leveldb/db.h REQUIRED)
find_package(OpenSSL REQUIRED)

add_executable(minikv src/Server/miniDriverServer_v3.cpp)

# 链接上面打包好的网络静态库
target_link_libraries(minikv PRIVATE minikv_net)
target_link_libraries(minikv PRIVATE minikv_storage)


target_include_directories(minikv PRIVATE ${LEVELDB_INC})
target_link_libraries(minikv PRIVATE ${LEVELDB_LIB} pthread)
target_link_libraries(minikv PRIVATE OpenSSL::Crypto)







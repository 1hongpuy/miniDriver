cmake_minimum_required(VERSION 3.16)

find_package(Qt6 REQUIRED COMPONENTS Core Widgets Multimedia MultimediaWidgets)

set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTOUIC ON)
set(CMAKE_AUTORCC ON)

add_executable(minidriver_qt_client
    app/main.cpp
    include/qtclient/MainWindow.hpp
    include/qtclient/TransferManager.hpp
    include/qtclient/TransferModel.hpp
    include/qtclient/LogModel.hpp
    include/qtclient/CatalogTypes.hpp
    include/qtclient/CatalogWorker.hpp
    include/qtclient/TransferWorker.hpp
    include/qtclient/VideoPlayerWidget.hpp
    src/MainWindow.cpp
    src/TransferManager.cpp
    src/TransferModel.cpp
    src/LogModel.cpp
    src/CatalogWorker.cpp
    src/TransferWorker.cpp
    src/VideoPlayerWidget.cpp
    src/MiniDriverStorageAdapter.cpp
)

target_compile_features(minidriver_qt_client PRIVATE cxx_std_17)
target_include_directories(minidriver_qt_client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(minidriver_qt_client PRIVATE
    Qt6::Core
    Qt6::Widgets
    Qt6::Multimedia
    Qt6::MultimediaWidgets
    minikv_client
)

add_executable(minidriver_qt_k3s_smoke
    tests/k3s_smoke.cpp
    src/MiniDriverStorageAdapter.cpp
)

target_compile_features(minidriver_qt_k3s_smoke PRIVATE cxx_std_17)
target_include_directories(minidriver_qt_k3s_smoke PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(minidriver_qt_k3s_smoke PRIVATE
    Qt6::Core
    minikv_client
)

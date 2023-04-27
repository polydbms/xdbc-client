cmake_minimum_required(VERSION 3.21)

project(xdbc VERSION 0.1 DESCRIPTION "xdbc load")

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#TODO: fix hardcoded paths
set(ZSTD_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libzstd.so")
set(SNAPPY_LIBRARY_PATH "/usr/lib/x86_64-linux-gnu/libsnappy.so")
include(GNUInstallDirs)

add_library(xdbc SHARED
        xdbc/xclient.cpp
        )

set_target_properties(xdbc PROPERTIES
        #VERSION ${PROJECT_VERSION}
        #SOVERSION 1
        PUBLIC_HEADER xdbc/xclient.h)

target_include_directories(xdbc PUBLIC .)
target_link_libraries(${PROJECT_NAME} ${ZSTD_LIBRARY_PATH} ${SNAPPY_LIBRARY_PATH} Threads::Threads lzo2 lz4)
install(TARGETS xdbc
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

add_subdirectory(./tests)

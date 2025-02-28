cmake_minimum_required(VERSION 3.21)

project(xdbc-sinks VERSION 0.1 DESCRIPTION "xdbc sinks")

set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g")
#[[SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")
SET(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pg")]]

# Add source files
#add_library(CsvSink STATIC CSVSink.cpp)

# Find dependencies
find_package(spdlog REQUIRED)
find_package(Boost 1.40.0 REQUIRED system program_options)

find_package(Arrow REQUIRED)
find_library(PARQUET_SHARED_LIB parquet REQUIRED)
# Ensure the library is found
if (NOT PARQUET_SHARED_LIB)
    message(FATAL_ERROR "Parquet shared library not found.")
endif()


# Include Boost headers and libraries
#if (Boost_FOUND)
#    include_directories(${Boost_INCLUDE_DIRS})
#    link_directories(${Boost_LIBRARY_DIRS})
#endif()

add_executable(xdbcsinks main.cpp CSVSink/CSVSink.cpp PQSink/PQSink.cpp ../xdbc/ControllerInterface/WebSocketClient.cpp ../xdbc/EnvironmentReconfigure/EnvironmentManager.cpp)

# Link dependencies
set(USED_LIBS ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_REGEX_LIBRARY})

# Link the CSV Sink to required libraries
target_link_libraries(xdbcsinks PRIVATE xdbc spdlog::spdlog ${USED_LIBS} -larrow ${PARQUET_SHARED_LIB})

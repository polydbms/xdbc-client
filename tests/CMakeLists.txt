add_executable(test_xclient test_xclient.cpp)

find_package(spdlog REQUIRED)

target_link_libraries(test_xclient PRIVATE xdbc spdlog::spdlog)
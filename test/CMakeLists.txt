cmake_minimum_required(VERSION 3.10)

project(kv_storage_tests)

add_executable(kv_storage_tests main.cpp)

target_link_libraries(kv_storage_tests kv_storage)
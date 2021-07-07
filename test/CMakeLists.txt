cmake_minimum_required(VERSION 3.10)

project(kv_storage_tests)

set (BOOST_ROOT "C:\\boost_1_76_0")

find_package(Boost)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(kv_storage_tests main.cpp)
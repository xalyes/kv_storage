cmake_minimum_required(VERSION 3.10)

project(kv_storage)

set (BOOST_ROOT "C:\\boost_1_76_0")

find_package(Boost)
include_directories(${Boost_INCLUDE_DIRS} include)

set (SOURCES
    src/volume_impl.h
    src/volume_impl.cpp
    src/bp_node.cpp
    src/bp_node.h
    src/node.cpp
    src/node.h
    src/leaf.cpp
    src/leaf.h
    src/utils.h
    include/kv_storage/volume.h
)

set (CMAKE_CXX_STANDARD 17)

add_library(kv_storage STATIC ${SOURCES})

add_subdirectory(test)
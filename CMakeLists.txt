cmake_minimum_required(VERSION 3.10)

project(kv_storage)

set (Boost_USE_STATIC_LIBS ON)
set (Boost_USE_MULTITHREADED ON)
set (Boost_USE_STATIC_RUNTIME OFF)

set (Boost_INCLUDE_DIR "C:\\boost_1_76_0")
set (Boost_LIBRARY_DIR "C:\\boost_1_76_0\\stage\\lib")

find_package(Boost COMPONENTS thread REQUIRED)
include_directories(${Boost_INCLUDE_DIRS} include)
link_directories(${Boost_LIBRARY_DIR})

set (SOURCES
    src/bp_node.h
    src/node.h
    src/node.cpp
    src/leaf.cpp
    src/leaf.h
    src/utils.h
    include/kv_storage/volume.h
    include/kv_storage/storage.h
)

set (CMAKE_CXX_STANDARD 17)

add_library(kv_storage STATIC ${SOURCES})
target_link_libraries( kv_storage ${Boost_LIBRARIES} )

add_subdirectory(test)
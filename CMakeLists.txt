cmake_minimum_required(VERSION 3.10)

project(kv_storage)

set (Boost_USE_STATIC_LIBS ON)
set (Boost_USE_MULTITHREADED ON)
set (Boost_USE_STATIC_RUNTIME OFF)

if (WIN32)
    set (Boost_INCLUDE_DIR "C:\\boost_1_76_0")
    set (Boost_LIBRARY_DIR "C:\\boost_1_76_0\\stage\\lib")
endif ()

find_package(Boost COMPONENTS thread REQUIRED)
include_directories(${Boost_INCLUDE_DIRS} include)
link_directories(${Boost_LIBRARY_DIR})

set (CMAKE_CXX_STANDARD 17)

add_executable(kv_storage_tests test/main.cpp)
target_link_libraries( kv_storage_tests ${Boost_LIBRARIES} )

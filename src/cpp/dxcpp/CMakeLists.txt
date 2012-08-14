cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxcpp)
set(CMAKE_BUILD_TYPE, Release)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Wno-overloaded-virtual -Werror=return-type -Wno-deprecated")

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  add_definitions("-std=c++0x")
endif()

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  if (APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
  endif()
  add_definitions("-std=c++11")
endif()

set (CMAKE_C_FLAGS_RELEASE "-O3")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

set (CMAKE_C_FLAGS_DEBUG "-g -DDEBUG=1")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

add_subdirectory(../dxjson dxjson)
add_subdirectory(../SimpleHttpLib SimpleHttpLib)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/../SimpleHttpLib)

add_library(dxcpp dxcpp.cc api.cc bindings.cc bindings/dxapplet.cc bindings/dxrecord.cc bindings/dxfile.cc bindings/dxjob.cc bindings/dxgtable.cc bindings/dxtable.cc bindings/dxapp.cc bindings/dxproject.cc bindings/search.cc)
set_target_properties(dxcpp PROPERTIES COMPILE_FLAGS -fPIC)
target_link_libraries(dxcpp dxjson curl dxhttp boost_thread-mt boost_regex)

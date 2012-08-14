cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxjson)
set(CMAKE_BUILD_TYPE, Release)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated")

set (CMAKE_C_FLAGS_RELEASE "-O3")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

set (CMAKE_C_FLAGS_DEBUG "-g -DDEBUG=1")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

add_library(dxjson dxjson.cpp)
set_target_properties(dxjson PROPERTIES COMPILE_FLAGS -fPIC)

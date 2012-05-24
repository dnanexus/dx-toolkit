cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(resolver)
set(CMAKE_BUILD_TYPE, Release)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated")

include_directories(..)

add_library(resolver Resolver.cpp Thread.cpp)

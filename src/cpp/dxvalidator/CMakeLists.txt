cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxvalidator)
set(CMAKE_BUILD_TYPE, Release)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated -fopenmp")
include_directories( ./ ../ ../SimpleHttpLib )

add_library(dxvalidator dxvalidate_tools.cc ../dxjson/dxjson.cpp)
target_link_libraries(dxvalidator boost_system boost_filesystem)
set_target_properties(dxvalidator PROPERTIES COMPILE_FLAGS -fPIC)

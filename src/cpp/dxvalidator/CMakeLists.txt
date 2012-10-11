cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxvalidator)
set(CMAKE_BUILD_TYPE, Release)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated -fopenmp")
add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp EXCLUDE_FROM_ALL)
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_library(dxvalidate dxvalidate_tools.cc dxvalidate_gtable.cc dxvalidate_gri.cc)
target_link_libraries(dxvalidate dxcpp boost_system boost_filesystem)
set_target_properties(dxvalidate PROPERTIES COMPILE_FLAGS -fPIC)

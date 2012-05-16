cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

project (dxjson)
set (CMAKE_BUILD_TYPE, Release)

set(GTEST_DIR /opt/chef-gtest-1.6.0)
include_directories( ./ ${GTEST_DIR} ${GTEST_DIR}/include )

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 --std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated")

add_library(dxjson dxjson.cpp)
set_target_properties(dxjson PROPERTIES COMPILE_FLAGS -fPIC)

# test

add_executable(test_json gtest_json.cpp ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test_json pthread dxjson)

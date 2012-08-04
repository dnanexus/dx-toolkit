cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxtestcpp)
set(CMAKE_BUILD_TYPE, Release)

#set(GTEST_DIR /opt/chef-gtest-1.6.0)
#include_directories(../ ./ ${GTEST_DIR} ${GTEST_DIR}/include ../SimpleHttpLib ../dxjson ../dxcpp ../dxlog)
include_directories(../ ./ ../cpp/SimpleHttpLib ../cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated -fopenmp")

add_subdirectory(../cpp/dxcpp dxcpp)
# test

add_executable(pairedreads_ins_est pairedreads_ins_est.cpp)
target_link_libraries(pairedreads_ins_est dxcpp dxjson)

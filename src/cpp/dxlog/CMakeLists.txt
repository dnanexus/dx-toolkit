cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated -fopenmp")
include_directories( ./ ../ ${GTEST_DIR} ${GTEST_DIR}/include ../SimpleHttpLib )

add_library(DXLog unixDGRAM.cc dxLog.cc dxLog_helper.cc ../dxjson/dxjson.cpp)

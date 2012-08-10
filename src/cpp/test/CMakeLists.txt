cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxtestcpp)
set(CMAKE_BUILD_TYPE, Release)

set(GTEST_DIR /opt/chef-gtest-1.6.0)
include_directories(../ ./ ${GTEST_DIR} ${GTEST_DIR}/include ../SimpleHttpLib ../dxjson ../dxcpp ../dxlog)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated -fopenmp")

add_subdirectory(../dxcpp dxcpp)
add_subdirectory(../dxlog dxlog)

# test

add_executable(test_dxcpp test_dxcpp.cc ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test_dxcpp pthread dxcpp)

add_executable(test_json gtest_json.cpp ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test_json pthread dxjson)

add_executable(test_simple_http gtest_http.cpp ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test_simple_http pthread curl dxhttp)

add_executable(test_dxlog test_dxlog.cc ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test_dxlog pthread dxlog dxjson)

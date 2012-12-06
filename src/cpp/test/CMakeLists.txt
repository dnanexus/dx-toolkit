cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxtestcpp)
set(CMAKE_BUILD_TYPE, Release)

set(GTEST_DIR /opt/chef-gtest-1.6.0)
include_directories(../ ./ ${GTEST_DIR} ${GTEST_DIR}/include ../SimpleHttpLib ../dxjson ../dxcpp ../dxlog)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-overloaded-virtual -Wno-sign-compare -Werror=return-type -Wno-unused-result -Wno-deprecated")

add_subdirectory(../dxcpp dxcpp)

add_executable(test_dxcpp test_dxcpp.cc ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test_dxcpp dxcpp)

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  add_definitions("-std=c++0x")
  
  # compile dxlog, dxjson, simple http tests iff compiler is g++  
  add_executable(test_simple_http gtest_http.cpp ${GTEST_DIR}/src/gtest-all.cc)
  target_link_libraries(test_simple_http curl dxhttp) 
  
  add_executable(test_json gtest_json.cpp ${GTEST_DIR}/src/gtest-all.cc)
  target_link_libraries(test_json dxjson)

  # dxlog require openmp to compile
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fopenmp")

  add_subdirectory(../dxlog dxlog)

  # dxlog tests requires openmp
  add_executable(test_dxlog test_dxlog.cc ${GTEST_DIR}/src/gtest-all.cc)
  target_link_libraries(test_dxlog pthread dxlog dxjson)

endif()

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  add_definitions("-std=c++11")
endif()

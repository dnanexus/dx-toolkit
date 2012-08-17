cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

option (BUILD_TESTS "Build test" FALSE)
project(reads_validator)

set (CMAKE_BUILD_TYPE, Release)

if (BUILD_TEST)
  set (GTEST_DIR /opt/chef-gtest-1.6.0)
  include_directories(${GTEST_DIR} ${GTEST_DIR}/include)
endif()

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated -fopenmp")

set (CMAKE_C_FLAGS_RELEASE "-O3")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

set (CMAKE_C_FLAGS_DEBUG "-g -DDEBUG=1")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp EXCLUDE_FROM_ALL)
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_executable(reads_validator reads_validator.cpp)
target_link_libraries(reads_validator dxjson dxcpp)

if (BUILD_TEST)
  add_executable(test_reads_validator test_reads_validator.cpp ${GTEST_DIR}/src/gtest-all.cc)
  target_link_libraries(test_reads_validator dxjson dxcpp pthread)
endif()

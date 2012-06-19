cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxcpp)
set(CMAKE_BUILD_TYPE, Release)

set(GTEST_DIR /opt/chef-gtest-1.6.0)
include_directories( ./ ../ ${GTEST_DIR} ${GTEST_DIR}/include ../SimpleHttpLib )

add_custom_target(setup)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated")

add_subdirectory(../dxjson dxjson)
add_subdirectory(../SimpleHttpLib SimpleHttpLib)

add_library(dxcpp dxcpp.cc api.cc bindings.cc bindings/dxprogram.cc bindings/dxrecord.cc bindings/dxfile.cc bindings/dxjob.cc bindings/dxgtable.cc bindings/dxtable.cc bindings/dxapp.cc bindings/dxproject.cc)
set_target_properties(dxcpp PROPERTIES COMPILE_FLAGS -fPIC)
target_link_libraries(dxcpp dxjson curl dxhttp)

# test

add_executable(test_dxcpp test_dxcpp.cc ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test_dxcpp pthread dxcpp)

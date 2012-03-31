cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

project (dxcpp)
set (CMAKE_BUILD_TYPE, Debug)

set(GTEST_DIR /opt/chef-gtest-1.6.0)
include_directories( ./ ${GTEST_DIR} ${GTEST_DIR}/include ../SimpleHttpLib )

add_custom_target(setup)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wno-sign-compare -Werror=return-type -Wno-deprecated")

add_library(simplehttp ../SimpleHttpLib/SimpleHttp.cpp ../SimpleHttpLib/Utility.cpp ../SimpleHttpLib/SimpleHttpHeaders.cpp)
set_target_properties(simplehttp PROPERTIES COMPILE_FLAGS -fPIC)

add_library(dxcpp dxcpp.cc api.cc bindings.cc bindings/dxapp.cc bindings/dxrecord.cc bindings/dxcollection.cc bindings/dxfile.cc bindings/dxgroup.cc bindings/dxjob.cc bindings/dxtable.cc bindings/dxuser.cc)
set_target_properties(dxcpp PROPERTIES COMPILE_FLAGS -fPIC)
target_link_libraries(dxcpp simplehttp curl)

add_dependencies(setup dxcpp simplehttp)

# test

add_executable(test test_dxcpp.cc ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test pthread curl simplehttp dxcpp dxjson)
add_dependencies(test setup)

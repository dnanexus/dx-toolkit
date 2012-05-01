cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

project (dxhttp)
set (CMAKE_BUILD_TYPE, Debug)

set(GTEST_DIR /opt/chef-gtest-1.6.0)
include_directories( ./ ${GTEST_DIR} ${GTEST_DIR}/include )

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wno-sign-compare -Werror=return-type -Wno-deprecated -lcurl")

add_library(dxhttp SimpleHttp.cpp Utility.cpp SimpleHttpHeaders.cpp)
set_target_properties(dxhttp PROPERTIES COMPILE_FLAGS -fPIC)

#test
add_executable(test_simple_http gtest_http.cpp SimpleHttp.cpp Utility.cpp SimpleHttpHeaders.cpp ${GTEST_DIR}/src/gtest-all.cc)
target_link_libraries(test_simple_http pthread curl dxhttp)

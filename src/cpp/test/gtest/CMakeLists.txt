cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(gtest)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive") # to compile Google test
# Build gtest library
add_library(gtest gtest-all.cc)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(gtest pthread) 

cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dx-cpp-tests)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../dxcpp dxcpp)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/gtest gtest)

find_package(Boost 1.48 COMPONENTS filesystem date_time REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${CMAKE_CURRENT_SOURCE_DIR}/../SimpleHttpLib ${CMAKE_CURRENT_SOURCE_DIR}/../dxjson
        ${CMAKE_CURRENT_SOURCE_DIR}/../dxcpp ${CURL_INCLUDE_DIRS} ${Boost_INCLUDE_DIR})

# dxcpp_tests
add_executable(test_dxcpp test_dxcpp.cc)
target_link_libraries(test_dxcpp dxcpp gtest ${Boost_LIBRARIES})

# simple http tests
add_executable(test_simplehttp test_simplehttp.cpp)
target_link_libraries(test_simplehttp dxhttp gtest) 

# dxjson tests
add_executable(test_dxjson test_dxjson.cpp)
target_link_libraries(test_dxjson dxjson gtest) 

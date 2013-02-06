cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxvalidator)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp EXCLUDE_FROM_ALL)
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_library(dxvalidate dxvalidate_tools.cc dxvalidate_gtable.cc dxvalidate_gri.cc dxvalidate_contigset.cc)
target_link_libraries(dxvalidate dxcpp)

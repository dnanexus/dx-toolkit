cmake_minimum_required(VERSION 3.15)
cmake_policy(VERSION 3.15)

project(dxjson)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

#########################
# Find Boost header files
#########################
# Note: dxjson.cpp just needs 2 Boost header files (no libraries are required)
#       Therefore, we just add boost include directory to include path
find_package(Boost 1.48 REQUIRED)
include_directories(BEFORE ${Boost_INCLUDE_DIR})
message (STATUS "dxjson CMakeLists.txt says: Boost 1.48+ header files found in location ${Boost_INCLUDE_DIR}")
###################################

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)

add_library(dxjson dxjson.cpp)

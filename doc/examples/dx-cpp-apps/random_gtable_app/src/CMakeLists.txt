cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(random_gtable)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)

## Additional compiler flags (app specific) can be set by uncommenting the line below
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -SOME_ADDITIONAL_FLAGS_YOU_WANT_TO_SET")

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp EXCLUDE_FROM_ALL)
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_executable(random_gtable_app rand_gtable.cpp)
target_link_libraries(random_gtable_app dxjson dxcpp)

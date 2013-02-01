cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dx-wig-to-wiggle)

# Default build type.
set (CMAKE_BUILD_TYPE, Release)

# ==================== FLAGS ==================== 

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_C_FLAGS} -Wno-deprecated")

set (CMAKE_C_FLAGS_RELEASE "-O3")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

set (CMAKE_C_FLAGS_DEBUG "-g -DDEBUG=1")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp EXCLUDE_FROM_ALL)
add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/resolver" resolver EXCLUDE_FROM_ALL)
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_executable(dx-wig-to-wiggle wig_importer.cpp WigFile.cpp Compress.cpp)
target_link_libraries(dx-wig-to-wiggle dxjson dxcpp resolver z bz2)

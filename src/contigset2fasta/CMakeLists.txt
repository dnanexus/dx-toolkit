cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(contigset2fasta)

# Default build type.
set (CMAKE_BUILD_TYPE, Release)

# ==================== FLAGS ==================== 

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_C_FLAGS} -Wno-deprecated")

set (CMAKE_C_FLAGS_RELEASE "-O3")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

set (CMAKE_C_FLAGS_DEBUG "-g -DDEBUG=1")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp)

include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_executable(contigset2fasta contigset2fasta.cpp)
target_link_libraries(contigset2fasta dxcpp)

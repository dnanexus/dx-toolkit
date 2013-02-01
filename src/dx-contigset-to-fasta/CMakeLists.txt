cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dx-contigset-to-fasta)

IF (APPLE)
  FIND_PACKAGE(Boost REQUIRED)
  IF (Boost_FOUND)
       INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
       LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
  ENDIF()
ENDIF()

# Default build type.
set (CMAKE_BUILD_TYPE, Release)

# ==================== FLAGS ====================
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Wno-overloaded-virtual -Werror=return-type")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_C_FLAGS} -Wno-deprecated")

set (CMAKE_C_FLAGS_RELEASE "-O3")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

set (CMAKE_C_FLAGS_DEBUG "-g -DDEBUG=1")
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp)

include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_executable(dx-contigset-to-fasta dx_contigset_to_fasta.cpp)
target_link_libraries(dx-contigset-to-fasta dxcpp)

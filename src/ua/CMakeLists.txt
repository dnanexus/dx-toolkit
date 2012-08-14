cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(ua)

set(CMAKE_BUILD_TYPE, Release)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_C_FLAGS} -std=c++0x -Wno-deprecated")

# set(CMAKE_C_LINK_FLAGS "${CMAKE_C_LINK_FLAGS} -static")
# set(CMAKE_CXX_LINK_FLAGS "${CMAKE_CXX_LINK_FLAGS} ${CMAKE_C_LINK_FLAGS}")

set(CMAKE_C_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

set(CMAKE_C_FLAGS_DEBUG "-g -DDEBUG=1")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp EXCLUDE_FROM_ALL)
add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/resolver" resolver EXCLUDE_FROM_ALL)
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_executable(ua main.cpp options.cpp chunk.cpp)
target_link_libraries(ua dxjson dxcpp boost_program_options boost_filesystem boost_system boost_thread-mt)

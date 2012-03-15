cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

project (simplehttplibproj)
set (CMAKE_BUILD_TYPE, Debug)

include_directories( ./ )

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wno-sign-compare -Werror=return-type -Wno-deprecated")

add_executable(main ../main.cpp SimpleHttp.cpp Utility.cpp SimpleHttpHeaders.cpp)

target_link_libraries(main curl)

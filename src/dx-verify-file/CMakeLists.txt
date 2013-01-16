cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dx-verify-file)

#dx-verify-file version
set (VERSION 0.0.1)

# Get the output of command `git describe`, and save it in GITVERSION variable
execute_process(COMMAND git describe OUTPUT_VARIABLE GITVERSION)
# Trim the newline at the end of "git describe" output
string(REGEX REPLACE "\n" "" GITVERSION ${GITVERSION})

# Default build type.
set (CMAKE_BUILD_TYPE, Release)

# Find Boost >= 1.48, and add it's include/lib directories
find_package(Boost 1.48 REQUIRED)
if (Boost_FOUND)
     include_directories(${Boost_INCLUDE_DIR})
     link_directories(${Boost_LIBRARY_DIRS})
endif()

# Flags
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wno-sign-compare -Wno-overloaded-virtual -Werror=return-type --std=c++0x")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_C_FLAGS} -Wno-deprecated")
set (CMAKE_C_FLAGS_RELEASE "-O3")
set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

# Add dxcpp as a subdirectory (this will be build by cmake automatically),
# this will genereate libdxcpp.a, which we link against
add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp)

# This directory will be searched for header files
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

# Add Macros
if (APPLE)
  add_definitions(-DMAC_BUILD)
elseif(MINGW)
  add_definitions(-DWINDOWS_BUILD)
elseif(UNIX)
  add_definitions(-DLINUX_BUILD)
else()
  message(FATAL_ERROR "Unknown platform. None of these CMAKE variables found: APPLE, MINGW, or UNIX")
endif()

add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DDX_VERIFY_FILE_VERSION=\"${VERSION}\")
add_definitions(-DGITVERSION=\"${GITVERSION}\")

add_executable(dx-verify-file main.cpp options.cpp log.cpp chunk.cpp main.cpp File.cpp api_helper.cpp)

# Add dxcpp
target_link_libraries(dx-verify-file dxcpp)
target_link_libraries(dx-verify-file curl boost_program_options-mt boost_system-mt boost_filesystem-mt boost_regex-mt)

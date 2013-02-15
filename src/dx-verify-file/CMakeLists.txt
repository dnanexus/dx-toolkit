cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dx-verify-file)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)

#dx-verify-file version
set (DX_VERIFY_FILE_VERSION 0.0.1)

# Get the output of command `git describe`, and save it in DXTOOLKIT_GITVERSION variable
execute_process(COMMAND git describe WORKING_DIRECTORY $ENV{DNANEXUS_HOME} RESULT_VARIABLE GIT_DESCRIBE_STATUS_CODE OUTPUT_VARIABLE DXTOOLKIT_GITVERSION ERROR_VARIABLE GIT_DESCRIBE_ERROR)
if (GIT_DESCRIBE_STATUS_CODE EQUAL 0)
  # Trim the newline at the end of "git describe" output
  string(REGEX REPLACE "\n" "" DXTOOLKIT_GITVERSION ${DXTOOLKIT_GITVERSION})
  message(STATUS "dx-verify-file CMakeLists.txt says: DXTOOLKIT_GITVERSION= ${DXTOOLKIT_GITVERSION}")
else()
  set(DXTOOLKIT_GITVERSION unknown)
  message(STATUS "dx-verify-file CMakeLists.txt says: The command 'git describe' failed.")
  message(STATUS "\terror message: '${GIT_DESCRIBE_ERROR}'. Will set DXTOOLKIT_GITVERSION='${DXTOOLKIT_GITVERSION}'")
endif()
add_definitions(-DDXTOOLKIT_GITVERSION=\"${DXTOOLKIT_GITVERSION}\")

#########################################################################
# Find Boost library >= 1.48 (using the cmake find_package functionality)
if (STATIC_BOOST) # can be set by using -DSTATIC_BOOST=1 while running cmake
  message (STATUS "dx-verify-file says: Variable STATIC_BOOST is set, will turn on static linking for boost")
  set(Boost_USE_STATIC_LIBS ON) # Use static libs for boost
endif ()
set(Boost_USE_MULTITHREADED ON)

find_package(Boost 1.48 COMPONENTS thread program_options filesystem system REQUIRED)
if (Boost_FOUND)
  message(STATUS "dx-verify-file CMakeLists.txt says: Boost libraries found")
  message(STATUS "\t** BOOST_INCLUDE_DIR=\"${Boost_INCLUDE_DIR}\"")
  message(STATUS "\t** BOOST_LIBRARY_DIRS=\"${Boost_LIBRARY_DIRS}\"")
  message(STATUS "\t** BOOST_LIBRARIES = \"${Boost_LIBRARIES}\"")
  include_directories(BEFORE ${Boost_INCLUDE_DIR})
endif()
#########################################################################

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
add_definitions(-DDX_VERIFY_FILE_VERSION=\"${DX_VERIFY_FILE_VERSION}\")

add_executable(dx-verify-file main.cpp options.cpp log.cpp chunk.cpp main.cpp File.cpp)

# Add dxcpp
target_link_libraries(dx-verify-file dxcpp)
target_link_libraries(dx-verify-file ${Boost_LIBRARIES})

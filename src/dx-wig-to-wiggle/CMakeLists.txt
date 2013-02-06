cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dx-wig-to-wiggle)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)
# Set an additional flag (to disable some dx-wig-to-wiggle specific warnings)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result")

########################
# Find Bzip2 dev package
find_package (BZip2 REQUIRED)
if (BZIP2_FOUND)
  message(STATUS "dx-wig-to-wiggle says: lib-bzip2 found!")
  message(STATUS "\t** BZIP2_INCLUDE_DIR=\"${BZIP2_INCLUDE_DIR}\"")
  message(STATUS "\t** BZIP2_LIBRARIES = \"${BZIP2_LIBRARIES}\"")
  include_directories(${BZIP_INCLUDE_DIRS})
endif (BZIP2_FOUND)
########################

########################
# Find libz
find_path(Z_INCLUDE_DIR zlib.h)
find_library(Z_LIBRARY_PATH NAMES z)

if (Z_INCLUDE_DIR AND Z_LIBRARY_PATH)
  message(STATUS "dx-wig-to-wiggle says: libz found!")
  message(STATUS "\t** Z_INCLUDE_DIR = \"${Z_INCLUDE_DIR}\"")
  message(STATUS "\t** Z_LIBRARY_PATH = \"${Z_LIBRARY_PATH}\"")
else()
  message(FATAL_ERROR "\n'libz' not found .. cannot continue. Z_INCLUDE_DIR = \"${Z_INCLUDE_DIR}\", Z_LIBRARY_PATH = \"${Z_LIBRARY_PATH}\"")
endif()
include_directories(BEFORE ${Z_INCLUDE_DIR})
#########################

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp)
add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/resolver" resolver)
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_executable(dx-wig-to-wiggle wig_importer.cpp WigFile.cpp Compress.cpp)
target_link_libraries(dx-wig-to-wiggle dxjson dxcpp resolver ${Z_LIBRARY_PATH} ${BZIP2_LIBRARIES})

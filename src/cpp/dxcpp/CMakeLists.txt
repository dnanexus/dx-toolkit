cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dxcpp)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)
# Set an additional flag (to disable some dxcpp specific warnings)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-overloaded-virtual -Wno-switch")

include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_build_version.txt" NO_POLICY_SCOPE)

####################################
# Find libcrypto (and openssl/md5.h)
####################################
find_path(OPENSSL_INCLUDE_DIR openssl/md5.h)
find_library(CRYPTO_LIBRARY_PATH NAMES crypto)

if (OPENSSL_INCLUDE_DIR AND CRYPTO_LIBRARY_PATH)
  message(STATUS "dxcpp CMakeLists.txt says: libcrypto, and openssl/md5.h found!")
  message(STATUS "\t** OPENSSL_INCLUDE_PATH = \"${OPENSSL_INCLUDE_DIR}\"")
  message(STATUS "\t** CRYPTO_LIBRARY_PATH = \"${CRYPTO_LIBRARY_PATH}\"")
else()
  message(FATAL_ERROR "\ndxcpp says: Unable to find 'libcrypto' and/or 'openssl/md5.h'. OPENSSL_INCLUDE_PATH = \"${OPENSSL_INCLUDE_DIR}\", CRYPTO_LIBRARY_PATH = \"${CRYPTO_LIBRARY_PATH}\"")
endif()
include_directories(BEFORE ${OPENSSL_INCLUDE_DIR})
###################################

#########################################################################
# Find Boost library >= 1.48 (using the cmake find_package functionality)
if (STATIC_BOOST) # can be set by using -DSTATIC_BOOST=1 while running cmake
  message (STATUS "dxcpp says: Variable STATIC_BOOST is set, will turn on static linking for boost")
  set(Boost_USE_STATIC_LIBS ON) # Use static libs for boost
endif ()
set(Boost_USE_MULTITHREADED ON)

find_package(Boost 1.48 COMPONENTS thread regex system REQUIRED)
if (Boost_FOUND)
  message(STATUS "dxcpp CMakeLists.txt says: Boost libraries found")
  message(STATUS "\t** BOOST_INCLUDE_DIR=\"${Boost_INCLUDE_DIR}\"")
  message(STATUS "\t** BOOST_LIBRARY_DIRS=\"${Boost_LIBRARY_DIRS}\"")
  message(STATUS "\t** BOOST_LIBRARIES = \"${Boost_LIBRARIES}\"")
  include_directories(BEFORE ${Boost_INCLUDE_DIR})
endif()
#########################################################################

add_subdirectory(../dxjson dxjson)
add_subdirectory(../SimpleHttpLib SimpleHttpLib)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/.. ${CMAKE_CURRENT_SOURCE_DIR}/../SimpleHttpLib ${CMAKE_CURRENT_SOURCE_DIR}/../dxjson)

add_library(dxcpp dxcpp.cc api.cc bindings.cc bindings/dxapplet.cc bindings/dxrecord.cc bindings/dxfile.cc bindings/dxjob.cc bindings/dxgtable.cc bindings/dxapp.cc bindings/dxproject.cc bindings/search.cc bindings/execution_common_helper.cc exec_utils.cc utils.cc dxlog.cc)
target_link_libraries(dxcpp dxhttp dxjson ${CRYPTO_LIBRARY_PATH} ${Boost_LIBRARIES})

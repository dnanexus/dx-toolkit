cmake_minimum_required(VERSION 3.15)
cmake_policy(VERSION 3.15)

project(SimpleHttpLib)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message (FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)

#########################
# Find Boost header files
#########################
# Note: SimpleHttpHeaders.cpp just needs a Boost header files (no libraries are required)
#       Therefore, we just add boost include directory to include path
find_package(Boost 1.48 REQUIRED)
include_directories(BEFORE ${Boost_INCLUDE_DIR})
message (STATUS "SimpleHttpLib CMakeLists.txt says: Boost 1.48+ header files found in location ${Boost_INCLUDE_DIR}")
###################################

include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_build_version.txt" NO_POLICY_SCOPE)

########################################
# Find libcurl include path/library path
########################################
if (MINGW)
  find_path(CURL_INCLUDE_DIRS curl/curl.h HINTS /c/curl)
  find_library(CURL_LIBRARIES NAMES libcurl HINTS /c/curl/lib)
  if (CURL_INCLUDE_DIRS AND CURL_LIBRARIES)
    message(STATUS "SimpleHttpLib CMakeLists.txt says: libcurl found!")
    message(STATUS "\t** CURL_INCLUDE_DIRS = \"${CURL_INCLUDE_DIRS}\"")
    message(STATUS "\t** CURL_LIBRARIES = \"${CURL_LIBRARIES}\"")
  else()
    message(FATAL_ERROR "\nSimpleHttpLib says: Unable to find libcurl. CURL_INCLUDE_DIRS = \"${CURL_INCLUDE_DIRS}\", CURL_LIBRARIES = \"${CURL_LIBRARIES}\"")
  endif()
  include_directories(BEFORE ${CURL_INCLUDE_DIRS})
else()
  find_path(CURL_INCLUDE_DIR curl/curl.h HINTS "/usr/include")
  find_library(CURL_LIBRARY_PATH NAMES curl libcurl HINTS "/usr/lib")

  if (CURL_INCLUDE_DIR AND CURL_LIBRARY_PATH)
    message(STATUS "SimpleHttpLib CMakeLists.txt says: libcurl found!")
    message(STATUS "\t** CURL_INCLUDE_PATH = \"${CURL_INCLUDE_DIR}\"")
    message(STATUS "\t** CURL_LIBRARY_PATH = \"${CURL_LIBRARY_PATH}\"")
  else()
    message(FATAL_ERROR "\nSimpleHttpLib says: Unable to find libcurl. CURL_INCLUDE_PATH = \"${CURL_INCLUDE_DIR}\", CURL_LIBRARY_PATH = \"${CURL_LIBRARY_PATH}\"")
  endif()
  include_directories(BEFORE ${CURL_INCLUDE_DIR})
endif()
########################################

#######################################
# Find libcrypto (and openssl/crypto.h)
#######################################
if (MINGW)
  find_package(OpenSSL REQUIRED)
  if (OpenSSL_FOUND)
    message(STATUS "SimpleHttpLib CMakeLists.txt says: OpenSSL found!")
    message(STATUS "\t** OPENSSL_LIBRARIES = \"${OPENSSL_LIBRARIES}\"")
    message(STATUS "\t** OPENSSL_ROOT_DIR = \"${OPENSSL_ROOT_DIR}\"")
    find_path(OPENSSL_INCLUDE_DIR openssl/crypto.h)
    find_library(CRYPTO_LIBRARY_PATH NAMES crypto PATHS ${OPENSSL_ROOT_DIR})
    if (OPENSSL_INCLUDE_DIR)
      message(STATUS "\t** OPENSSL_INCLUDE_DIR = \"${OPENSSL_INCLUDE_DIR}\"")
    else()
      message(FATAL_ERROR "\nSimpleHttpLib says: Unable to find 'openssl/crypto.h'. OPENSSL_INCLUDE_DIR = \"${OPENSSL_INCLUDE_DIR}\"")
    endif()
  else()
    message(FATAL_ERROR "\ndxcpp says: Unable to find OpenSSL. OPENSSL_INCLUDE_DIR = \"${OPENSSL_INCLUDE_DIR}\", OPENSSL_LIBRARIES = \"${OPENSSL_LIBRARIES}\",")
  endif()
else()
  find_path(OPENSSL_INCLUDE_DIR openssl/crypto.h)
  find_library(CRYPTO_LIBRARY_PATH NAMES crypto)

  if (OPENSSL_INCLUDE_DIR AND CRYPTO_LIBRARY_PATH)
    message(STATUS "SimpleHttpLib CMakeLists.txt says: libcrypto, and openssl/crypto.h found!")
    message(STATUS "\t** OPENSSL_INCLUDE_PATH = \"${OPENSSL_INCLUDE_DIR}\"")
    message(STATUS "\t** CRYPTO_LIBRARY_PATH = \"${CRYPTO_LIBRARY_PATH}\"")
  else()
    message(FATAL_ERROR "\ndxcpp says: Unable to find 'libcrypto', and/or 'openssl/crypto.h'. OPENSSL_INCLUDE_PATH = \"${OPENSSL_INCLUDE_DIR}\", CRYPTO_LIBRARY_PATH = \"${CRYPTO_LIBRARY_PATH}\"")
  endif()
endif()
include_directories(BEFORE ${OPENSSL_INCLUDE_DIR})
######################################

add_library(dxhttp SimpleHttp.cpp Utility.cpp SimpleHttpHeaders.cpp SSLThreads.cpp)
if (MINGW)
  target_link_libraries (dxhttp ${CURL_LIBRARIES} ${OPENSSL_LIBRARIES})
else()
  target_link_libraries (dxhttp ${CURL_LIBRARY_PATH} ${CRYPTO_LIBRARY_PATH})
endif()


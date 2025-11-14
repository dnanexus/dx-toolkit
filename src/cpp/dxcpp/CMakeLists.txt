cmake_minimum_required(VERSION 3.15)
cmake_policy(VERSION 3.15)

project(dxcpp)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)
# Set an additional flag (to disable some dxcpp specific warnings)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-overloaded-virtual -Wno-switch")

include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_build_version.txt" NO_POLICY_SCOPE)

########################################
# Find libcurl include path on Windows
########################################

# Build libcurl from source on Windows unless C:\curl already exists.
# This also places the headers where cmake can find them:
if (MINGW AND NOT IS_DIRECTORY "c:/curl")
  message("Downloading the curl source and building libcurl")

  set(CurlVersion 7.45.0)
  set(CurlSHA1 e44fc290844c1e3c9b7f6ef9926d4366d0600514)

  # Create build folder name derived from version
  string(REGEX REPLACE "beta\\.([0-9])$" "beta\\1" CurlFolderName ${CurlVersion})
  string(REPLACE "." "_" CurlFolderName ${CurlFolderName})
  set(CurlFolderName curl_${CurlFolderName})

  set(CurlCacheDir ${CMAKE_BINARY_DIR})

  # Set up the full path to the source directory
  set(CurlSourceDir "${CurlFolderName}_${CMAKE_CXX_COMPILER_ID}_${CMAKE_CXX_COMPILER_VERSION}")
  string(REPLACE "." "_" CurlSourceDir ${CurlSourceDir})
  set(CurlSourceDir "${CurlCacheDir}/${CurlSourceDir}")

  set(ZipFilePath "${CurlCacheDir}/${CurlFolderName}.tar.gz")
  if(NOT EXISTS ${ZipFilePath})
    message(STATUS "Downloading curl ${CurlVersion} to ${CurlCacheDir}")
  endif()
  file(DOWNLOAD https://curl.haxx.se/download/curl-${CurlVersion}.tar.gz 
     ${ZipFilePath}
     STATUS Status
     SHOW_PROGRESS
     EXPECTED_HASH SHA1=${CurlSHA1}
     )

  # Extract curl if required
  string(FIND "${Status}" "returning early" Found)
  if(Found LESS "0" OR NOT IS_DIRECTORY "${CurlSourceDir}")
    set(CurlExtractFolder "${CurlCacheDir}/curl_unzip")
    file(REMOVE_RECURSE ${CurlExtractFolder})
    file(MAKE_DIRECTORY ${CurlExtractFolder})
    file(COPY ${ZipFilePath} DESTINATION ${CurlExtractFolder})
    message(STATUS "Extracting curl ${CurlVersion} to ${CurlExtractFolder}")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar zxf ${CurlFolderName}.tar.gz
                    WORKING_DIRECTORY ${CurlExtractFolder}
                    RESULT_VARIABLE Result
                    )
    if(NOT Result EQUAL "0")
      message(FATAL_ERROR "Failed extracting curl ${CurlVersion} to ${CurlExtractFolder}")
    endif()
    file(REMOVE ${CurlExtractFolder}/${CurlFolderName}.tar.gz)

    # Get the path to the extracted folder
    file(GLOB ExtractedDir "${CurlExtractFolder}/*")
    list(LENGTH ExtractedDir n)
    if(NOT n EQUAL "1" OR NOT IS_DIRECTORY ${ExtractedDir})
      message(FATAL_ERROR "Failed extracting curl ${CurlVersion} to ${CurlExtractFolder}")
    endif()
    file(RENAME ${ExtractedDir} ${CurlSourceDir})
    file(REMOVE_RECURSE ${CurlExtractFolder})
  endif()

  # Build curl if required
  message(STATUS "Configuring curl")
  #set(curlConfigure "bootstrap.bat")
  #list(APPEND curlConfigure mingw)
  set(curlConfigure bash -c './configure
    --disable-ldap --disable-ldaps 
    --disable-rtsp --disable-dict --disable-telnet --disable-tftp --disable-pop3 
    --disable-imap --disable-smtp --disable-gopher --disable-sspi --disable-ntlm-wb 
    --disable-tls-srp --without-gnutls --without-polarssl --without-cyassl 
    --without-nss --without-libmetalink --without-libssh2 --without-librtmp 
    --without-winidn --without-libidn 
    --enable-static=yes --enable-shared=yes --with-winssl --prefix=/'
    )

  message("Executing command '${curlConfigure}' in working dir ${CurlSourceDir}...")
  set(ENV{CPPFLAGS} -DWINVER=0x0501)
  execute_process(COMMAND ${curlConfigure} WORKING_DIRECTORY ${CurlSourceDir}
                  RESULT_VARIABLE Result OUTPUT_VARIABLE Output ERROR_VARIABLE Error)
  if(NOT Result EQUAL "0")
    message(FATAL_ERROR "Failed running '${curlConfigure}': '${Result}' '${Output}'\n'${Error}'\n")
  endif()

  # Build curl and install in C:\curl
  set(curlInstall make install DESTDIR='c:/curl')
  message("Executing command '${curlInstall}' in working dir ${CurlSourceDir}...")
  execute_process(COMMAND ${curlInstall} WORKING_DIRECTORY ${CurlSourceDir}
                  RESULT_VARIABLE Result OUTPUT_VARIABLE Output ERROR_VARIABLE Error)
  if(NOT Result EQUAL "0")
    message(FATAL_ERROR "Failed running '${curlInstall}': '${Result}' '${Output}'\n'${Error}'\n")
  endif()
endif()

# Find the curl include dir:
if (MINGW)
  find_path(CURL_INCLUDE_DIRS curl/curl.h HINTS c:/curl)
  if (CURL_INCLUDE_DIRS)
    message(STATUS "dxcpp CMakeLists.txt says: curl/curl.h found!")
    message(STATUS "\t** CURL_INCLUDE_DIRS = \"${CURL_INCLUDE_DIRS}\"")
  else()
    message(FATAL_ERROR "\ndxcpp CMakeLists.txt says: Unable to find curl/curl.h. CURL_INCLUDE_DIRS = \"${CURL_INCLUDE_DIRS}\"")
  endif()
  include_directories(BEFORE ${CURL_INCLUDE_DIRS})
endif()

####################################
# Find libcrypto (and openssl/md5.h)
####################################

# Build OpenSSL from source on Windows unless C:\openssl already exists.
# This also places the headers where cmake can find them:
if (MINGW AND NOT IS_DIRECTORY "c:/openssl")
  message("Downloading the openssl source and building libcrypto")

  set(OpenSSLVersionBase 1.0.1)
  set(OpenSSLVersion ${OpenSSLVersionBase}f)
  set(OpenSSLSHA1 9ef09e97dfc9f14ac2c042f3b7e301098794fc0f)

  # Create build folder name derived from version
  string(REGEX REPLACE "beta\\.([0-9])$" "beta\\1" OpenSSLFolderName ${OpenSSLVersion})
  string(REPLACE "." "_" OpenSSLFolderName ${OpenSSLFolderName})
  set(OpenSSLFolderName openssl_${OpenSSLFolderName})

  set(OpenSSLCacheDir ${CMAKE_BINARY_DIR})

  # Set up the full path to the source directory
  set(OpenSSLSourceDir "${OpenSSLFolderName}_${CMAKE_CXX_COMPILER_ID}_${CMAKE_CXX_COMPILER_VERSION}")
  string(REPLACE "." "_" OpenSSLSourceDir ${OpenSSLSourceDir})
  set(OpenSSLSourceDir "${OpenSSLCacheDir}/${OpenSSLSourceDir}")

  set(ZipFilePath "${OpenSSLCacheDir}/${OpenSSLFolderName}.tar.gz")
  if(NOT EXISTS ${ZipFilePath})
    message(STATUS "Downloading OpenSSL ${OpenSSLVersion} to ${OpenSSLCacheDir}")
  endif()
  file(DOWNLOAD https://www.openssl.org/source/old/${OpenSSLVersionBase}/openssl-${OpenSSLVersion}.tar.gz
     ${ZipFilePath}
     STATUS Status
     SHOW_PROGRESS
     EXPECTED_HASH SHA1=${OpenSSLSHA1}
     )

  # Extract openssl if required
  string(FIND "${Status}" "returning early" Found)
  if(Found LESS "0" OR NOT IS_DIRECTORY "${OpenSSLSourceDir}")
    set(OpenSSLExtractFolder "${OpenSSLCacheDir}/openssl_unzip")
    file(REMOVE_RECURSE ${OpenSSLExtractFolder})
    file(MAKE_DIRECTORY ${OpenSSLExtractFolder})
    file(COPY ${ZipFilePath} DESTINATION ${OpenSSLExtractFolder})
    message(STATUS "Extracting openssl ${OpenSSLVersion} to ${OpenSSLExtractFolder}")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar zxf ${OpenSSLFolderName}.tar.gz
                    WORKING_DIRECTORY ${OpenSSLExtractFolder}
                    RESULT_VARIABLE Result
                    )
    if(NOT Result EQUAL "0")
      message(FATAL_ERROR "Failed extracting openssl ${OpenSSLVersion} to ${OpenSSLExtractFolder}")
    endif()
    file(REMOVE ${OpenSSLExtractFolder}/${OpenSSLFolderName}.tar.gz)

    # Get the path to the extracted folder
    file(GLOB ExtractedDir "${OpenSSLExtractFolder}/*")
    list(LENGTH ExtractedDir n)
    if(NOT n EQUAL "1" OR NOT IS_DIRECTORY ${ExtractedDir})
      message(FATAL_ERROR "Failed extracting openssl ${OpenSSLVersion} to ${OpenSSLExtractFolder}")
    endif()
    file(RENAME ${ExtractedDir} ${OpenSSLSourceDir})
    file(REMOVE_RECURSE ${OpenSSLExtractFolder})
  endif()

  # Build openssl if required
  message(STATUS "Configuring openssl")
  set(OpenSSLConfigure bash -c './config shared --openssldir=c:/openssl')

  message("Executing command '${OpenSSLConfigure}' in working dir ${OpenSSLSourceDir}...")
  #set(ENV{CPPFLAGS} -DWINVER=0x0501)
  execute_process(COMMAND ${OpenSSLConfigure} WORKING_DIRECTORY ${OpenSSLSourceDir}
                  RESULT_VARIABLE Result OUTPUT_VARIABLE Output ERROR_VARIABLE Error)
  if(NOT Result EQUAL "0")
    message(FATAL_ERROR "Failed running '${OpenSSLConfigure}': '${Result}' '${Output}'\n'${Error}'\n")
  endif()

  # Build openssl and install in C:\openssl
  set(OpenSSLInstall make -j1 install)
  message("Executing command '${OpenSSLInstall}' in working dir ${OpenSSLSourceDir}...")
  execute_process(COMMAND ${OpenSSLInstall} WORKING_DIRECTORY ${OpenSSLSourceDir}
                  RESULT_VARIABLE Result OUTPUT_VARIABLE Output ERROR_VARIABLE Error)
  if(NOT Result EQUAL "0")
    message(FATAL_ERROR "Failed running '${OpenSSLInstall}': '${Result}' '${Output}'\n'${Error}'\n")
  endif()
endif()

if (MINGW)
  find_package(OpenSSL REQUIRED)
  find_path(OPENSSL_INCLUDE_DIR openssl/md5.h)
  find_library(CRYPTO_LIBRARY_PATH NAMES crypto)
  if (OpenSSL_FOUND)
    message(STATUS "dxcpp CMakeLists.txt says: OpenSSL found!")
    message(STATUS "\t** OPENSSL_INCLUDE_DIR = \"${OPENSSL_INCLUDE_DIR}\"")
    message(STATUS "\t** OPENSSL_LIBRARIES = \"${OPENSSL_LIBRARIES}\"")
  else()
    message(FATAL_ERROR "\ndxcpp says: Unable to find OpenSSL. OPENSSL_INCLUDE_DIR = \"${OPENSSL_INCLUDE_DIR}\", OPENSSL_LIBRARIES = \"${OPENSSL_LIBRARIES}\",")
  endif()
else()
  find_path(OPENSSL_INCLUDE_DIR openssl/md5.h)
  find_library(CRYPTO_LIBRARY_PATH NAMES crypto)

  if (OPENSSL_INCLUDE_DIR AND CRYPTO_LIBRARY_PATH)
    message(STATUS "dxcpp CMakeLists.txt says: libcrypto, and openssl/md5.h found!")
    message(STATUS "\t** OPENSSL_INCLUDE_DIR = \"${OPENSSL_INCLUDE_DIR}\"")
    message(STATUS "\t** CRYPTO_LIBRARY_PATH = \"${CRYPTO_LIBRARY_PATH}\"")
  else()
    message(FATAL_ERROR "\ndxcpp says: Unable to find 'libcrypto' and/or 'openssl/md5.h'. OPENSSL_INCLUDE_DIR = \"${OPENSSL_INCLUDE_DIR}\", CRYPTO_LIBRARY_PATH = \"${CRYPTO_LIBRARY_PATH}\"")
  endif()
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

add_library(dxcpp dxcpp.cc api.cc bindings.cc bindings/dxapplet.cc bindings/dxrecord.cc bindings/dxfile.cc bindings/dxjob.cc bindings/dxapp.cc bindings/dxproject.cc bindings/search.cc bindings/execution_common_helper.cc exec_utils.cc utils.cc dxlog.cc)
if (MINGW)
  target_link_libraries(dxcpp dxhttp dxjson ${OPENSSL_LIBRARIES} ${Boost_LIBRARIES})
else()
  target_link_libraries(dxcpp dxhttp dxjson ${CRYPTO_LIBRARY_PATH} ${Boost_LIBRARIES})
endif()


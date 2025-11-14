cmake_minimum_required(VERSION 3.15)
cmake_policy(VERSION 3.15)

project(dx-verify-file)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)

#dx-verify-file version
set (DX_VERIFY_FILE_VERSION 0.0.1)

include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_build_version.txt" NO_POLICY_SCOPE)

#########################################################################
# Find Boost library >= 1.48 (using the cmake find_package functionality)
if (STATIC_BOOST) # can be set by using -DSTATIC_BOOST=1 while running cmake
  message (STATUS "dx-verify-file says: Variable STATIC_BOOST is set, will turn on static linking for boost")
  set(Boost_USE_STATIC_LIBS ON) # Use static libs for boost
endif ()
set(Boost_USE_MULTITHREADED ON)

# Build boost from source on Windows unless C:\boost already exists
if (MINGW AND NOT IS_DIRECTORY "c:/boost")
  message("Downloading and building Boost")

  set(BoostVersion 1.60.0)
  set(BoostSHA1 ac74db1324e6507a309c5b139aea41af624c8110)

  # Create build folder name derived from version
  string(REGEX REPLACE "beta\\.([0-9])$" "beta\\1" BoostFolderName ${BoostVersion})
  string(REPLACE "." "_" BoostFolderName ${BoostFolderName})
  set(BoostFolderName boost_${BoostFolderName})

  set(BoostCacheDir ${CMAKE_BINARY_DIR})

  # Set up the full path to the source directory
  set(BoostSourceDir "${BoostFolderName}_${CMAKE_CXX_COMPILER_ID}_${CMAKE_CXX_COMPILER_VERSION}")
  string(REPLACE "." "_" BoostSourceDir ${BoostSourceDir})
  set(BoostSourceDir "${BoostCacheDir}/${BoostSourceDir}")

  set(ZipFilePath "${BoostCacheDir}/${BoostFolderName}.tar.gz")
  if(NOT EXISTS ${ZipFilePath})
    message(STATUS "Downloading boost ${BoostVersion} to ${BoostCacheDir}")
  endif()
  file(DOWNLOAD http://sourceforge.net/projects/boost/files/boost/${BoostVersion}/${BoostFolderName}.tar.gz/download
     ${ZipFilePath}
     STATUS Status
     SHOW_PROGRESS
     EXPECTED_HASH SHA1=${BoostSHA1}
     )

  # Extract boost if required
  string(FIND "${Status}" "returning early" Found)
  if(Found LESS "0" OR NOT IS_DIRECTORY "${BoostSourceDir}")
    set(BoostExtractFolder "${BoostCacheDir}/boost_unzip")
    file(REMOVE_RECURSE ${BoostExtractFolder})
    file(MAKE_DIRECTORY ${BoostExtractFolder})
    file(COPY ${ZipFilePath} DESTINATION ${BoostExtractFolder})
    message(STATUS "Extracting boost ${BoostVersion} to ${BoostExtractFolder}")
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar zxf ${BoostFolderName}.tar.gz
                    WORKING_DIRECTORY ${BoostExtractFolder}
                    RESULT_VARIABLE Result
                    )
    if(NOT Result EQUAL "0")
      message(FATAL_ERROR "Failed extracting boost ${BoostVersion} to ${BoostExtractFolder}")
    endif()
    file(REMOVE ${BoostExtractFolder}/${BoostFolderName}.tar.gz)

    # Get the path to the extracted folder
    file(GLOB ExtractedDir "${BoostExtractFolder}/*")
    list(LENGTH ExtractedDir n)
    if(NOT n EQUAL "1" OR NOT IS_DIRECTORY ${ExtractedDir})
      message(FATAL_ERROR "Failed extracting boost ${BoostVersion} to ${BoostExtractFolder}")
    endif()
    file(RENAME ${ExtractedDir} ${BoostSourceDir})
    file(REMOVE_RECURSE ${BoostExtractFolder})
  endif()

  # Build b2 (bjam) if required
  unset(b2Path CACHE)
  message(STATUS "Building b2 (bjam)")
  set(b2Bootstrap "bootstrap.bat")
  list(APPEND b2Bootstrap mingw)
  # TODO: Could also try set(b2Bootstrap bootstrap.bat mingw)
  message("Executing command '${b2Bootstrap}' in working dir ${BoostSourceDir}...")
  execute_process(COMMAND ${b2Bootstrap} WORKING_DIRECTORY ${BoostSourceDir}
                  RESULT_VARIABLE Result OUTPUT_VARIABLE Output ERROR_VARIABLE Error)
  if(NOT Result EQUAL "0")
    message(FATAL_ERROR "Failed running '${b2Bootstrap}': '${Result}' '${Output}'\n'${Error}'\n")
  endif()

  # Build boost and install in C:\boost
  set(b2Install b2 install
             --prefix=c:/boost
             toolset=gcc
             --with-thread
             --with-program_options
             --with-filesystem
             --with-system
             --with-regex
	     )
  message("Executing command '${b2Install}' in working dir ${BoostSourceDir}...")
  execute_process(COMMAND ${b2Install} WORKING_DIRECTORY ${BoostSourceDir}
                  RESULT_VARIABLE Result OUTPUT_VARIABLE Output ERROR_VARIABLE Error)
  if(NOT Result EQUAL "0")
    message(FATAL_ERROR "Failed running '${b2Install}': '${Result}' '${Output}'\n'${Error}'\n")
  endif()
endif()

# Locate boost
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

########################################
# Find libcurl include path on Windows
########################################
if (MINGW)
  find_path(CURL_INCLUDE_DIRS curl/curl.h HINTS /c/curl)
  if (CURL_INCLUDE_DIRS)
    message(STATUS "dxcpp CMakeLists.txt says: curl/curl.h found!")
    message(STATUS "\t** CURL_INCLUDE_DIRS = \"${CURL_INCLUDE_DIRS}\"")
  else()
    message(FATAL_ERROR "\ndxcpp CMakeLists.txt says: Unable to find curl/curl.h. CURL_INCLUDE_DIRS = \"${CURL_INCLUDE_DIRS}\"")
  endif()
  include_directories(BEFORE ${CURL_INCLUDE_DIRS})
endif()

add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DDX_VERIFY_FILE_VERSION=\"${DX_VERIFY_FILE_VERSION}\")

add_executable(dx-verify-file main.cpp options.cpp log.cpp chunk.cpp main.cpp File.cpp)

# Add dxcpp
target_link_libraries(dx-verify-file dxcpp)
target_link_libraries(dx-verify-file ${Boost_LIBRARIES})

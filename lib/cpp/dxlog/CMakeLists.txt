cmake_minimum_required (VERSION 2.6)
cmake_policy(VERSION 2.6)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++0x -D_FILE_OFFSET_BITS=64 -Wall -Wextra -Wno-sign-compare -Werror=return-type -Wno-deprecated -fopenmp")
include_directories( ./ ../ ${GTEST_DIR} ${GTEST_DIR}/include ../SimpleHttpLib )

add_library(DXLog mongoLog.cc unixDGRAM.cc dxLog.cc dxLog_helper.cc ../dxjson/dxjson.cpp)

add_executable(verify_logschema verify_logschema.cc)
target_link_libraries(verify_logschema DXLog boost_thread boost_filesystem boost_program_options boost_system)

add_executable(dx_dbLog dxDbLog.cc)
target_link_libraries(dx_dbLog DXLog mongoclient boost_thread boost_filesystem boost_program_options boost_system)

add_executable(dx_appLogHandler appLogHandler.cc)
target_link_libraries(dx_appLogHandler DXLog mongoclient boost_thread boost_filesystem boost_program_options boost_system)

add_executable(test_log test_log.cc)
target_link_libraries(test_log DXLog boost_system boost_filesystem)

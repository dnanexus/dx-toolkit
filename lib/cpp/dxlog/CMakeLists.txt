set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -Wall -Wno-sign-compare -Werror=return-type -Wno-deprecated -fopenmp")

add_library(DXLog mongoLog.cc unixDGRAM.cc dxLog.cc dxLog_helper.cc)

add_executable(verify_logschema verify_logschema.cc)
target_link_libraries(verify_logschema DXLog dxjson boost_thread boost_filesystem boost_program_options boost_system)

add_executable(dxDbLog dxDbLog.cc)
target_link_libraries(dxDbLog DXLog dxjson mongoclient boost_thread boost_filesystem boost_program_options boost_system)

add_executable(appLogHandler appLogHandler.cc)
target_link_libraries(appLogHandler DXLog dxjson mongoclient boost_thread boost_filesystem boost_program_options boost_system)

add_executable(test test_log.cc)
target_link_libraries(test DXLog dxjson)

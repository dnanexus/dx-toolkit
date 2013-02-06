cmake_minimum_required(VERSION 2.6)
cmake_policy(VERSION 2.6)

project(dx-contigset-to-fasta)

if (NOT DEFINED ENV{DNANEXUS_HOME})
  message(FATAL_ERROR "Environment variable DNANEXUS_HOME (location of dx-toolkit) not defined")
endif (NOT DEFINED ENV{DNANEXUS_HOME})

# Set default build type, common compiler flags, etc
include("$ENV{DNANEXUS_HOME}/src/cpp/cmake_include/set_compiler_flags.txt" NO_POLICY_SCOPE)

add_subdirectory("$ENV{DNANEXUS_HOME}/src/cpp/dxcpp" dxcpp)
include_directories("$ENV{DNANEXUS_HOME}/src/cpp")

add_executable(dx-contigset-to-fasta dx_contigset_to_fasta.cpp)
target_link_libraries(dx-contigset-to-fasta dxcpp)

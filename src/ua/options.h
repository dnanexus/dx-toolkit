// Copyright (C) 2013-2016 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

#ifndef UA_OPTIONS_H
#define UA_OPTIONS_H

#include <vector>
#include <string>
#include <iostream>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include "SimpleHttp.h"
#include "dxjson/dxjson.h"

namespace po = boost::program_options;

#if MAC_BUILD
  // Returns path of executable on Mac (not portable)
  std::string getExecutablePathOnMac();
#endif

class Options {
public:

  Options();

  void parse(int argc, char * argv[]);
  void setApiserverDxConfig();
  bool help();
  bool version();
  bool env();
  void printHelp(char * programName);
  void validate();
  unsigned int getNumberOfFilesInDirectory(const boost::filesystem::path &dir);

  friend std::ostream &operator<<(std::ostream &out, const Options &opt);

  std::vector<std::string> projects;
  std::vector<std::string> folders;
  std::vector<std::string> names;
  std::vector<std::string> files;
  std::string visibility;
  std::vector<std::string> propertiesInput;
  std::vector<std::string> typeInput;
  std::vector<std::string> tagsInput;
    
  int readThreads;
  int compressThreads;
  int uploadThreads;
  int chunkSize;
  int tries;
  bool doNotCompress;
  bool doNotResume;
  bool progress;
  bool verbose;
  bool waitOnClose;
  bool recursive;

  // Import flags
  bool reads;
  bool pairedReads;
  bool mappings;
  bool variants;
  std::string refGenome;

  bool noRoundRobinDNS;

  int64_t throttle;
  
  std::string detailsInput;
  dx::JSON properties;
  dx::JSON type;
  dx::JSON tags;
  dx::JSON details;

private:

  std::string rawChunkSize;
  std::string rawThrottle;

  // These params (if provided) are used for overriding the relevant dx::config::* values
  std::string apiserverProtocol;
  std::string apiserverHost;
  int apiserverPort;
  std::string authToken;
  std::string certificateFile;

  po::options_description * visible_opts;
  po::options_description * hidden_opts;
  po::options_description * command_line_opts;
  po::options_description * env_opts;
  po::positional_options_description * pos_opts;

  po::variables_map vm;
};

#endif

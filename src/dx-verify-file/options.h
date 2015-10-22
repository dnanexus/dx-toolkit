// Copyright (C) 2013-2015 DNAnexus, Inc.
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
#include "SimpleHttpLib/SimpleHttp.h"

namespace po = boost::program_options;

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

  friend std::ostream &operator<<(std::ostream &out, const Options &opt);

  std::string certificateFile;

  std::vector<std::string> localFiles;
  std::vector<std::string> remoteFiles;

  int readThreads;
  int md5Threads;
  bool verbose;
  
private:
  std::string apiserverProtocol;
  std::string apiserverHost;
  int apiserverPort;
  std::string authToken;
 
  po::options_description * visible_opts;
  po::options_description * hidden_opts;
  po::options_description * command_line_opts;
  po::options_description * env_opts;
  
  po::variables_map vm;
};

#endif

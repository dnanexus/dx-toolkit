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

  bool help();
  bool version();
  bool env();
  void printHelp(char * programName);
  void validate();

  friend std::ostream &operator<<(std::ostream &out, const Options &opt);

  std::string apiserverProtocol;
  std::string apiserverHost;
  int apiserverPort;
  std::string authToken;
  std::string certificateFile;

  std::vector<std::string> localFiles;
  std::vector<std::string> remoteFiles;

  int readThreads;
  int md5Threads;
  bool verbose;
  
private:

  po::options_description * visible_opts;
  po::options_description * hidden_opts;
  po::options_description * command_line_opts;
  po::options_description * env_opts;

  po::variables_map vm;
};

#endif

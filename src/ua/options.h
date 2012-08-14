#ifndef UA_OPTIONS_H
#define UA_OPTIONS_H

#include <vector>
#include <string>
#include <iostream>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

class Options {
public:

  Options();

  void parse(int argc, char * argv[]);

  bool help();
  void printHelp();
  void validate();

  friend std::ostream &operator<<(std::ostream &out, const Options &opt);

  std::string authToken;
  std::string apiserverHost;
  int apiserverPort;

  std::string project;
  std::string folder;
  std::string name;
  std::string file;

  int compressThreads;
  int uploadThreads;
  int chunkSize;
  int tries;
  bool compress;
  bool progress;

private:

  po::options_description * visible_opts;
  po::options_description * hidden_opts;
  po::options_description * command_line_opts;
  po::options_description * env_opts;
  po::positional_options_description * pos_opts;

  po::variables_map vm;
};

#endif

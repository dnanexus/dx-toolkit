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
  bool version();
  void printHelp(char * programName);
  void validate();

  friend std::ostream &operator<<(std::ostream &out, const Options &opt);

  std::string apiserverProtocol;
  std::string apiserverHost;
  int apiserverPort;
  std::string authToken;

  std::vector<std::string> projects;
  std::vector<std::string> folders;
  std::vector<std::string> names;
  std::vector<std::string> files;

  int readThreads;
  int compressThreads;
  int uploadThreads;
  int chunkSize;
  int tries;
  bool do_not_compress;
  bool progress;
  bool verbose;
  bool waitOnClose;

private:

  po::options_description * visible_opts;
  po::options_description * hidden_opts;
  po::options_description * command_line_opts;
  po::options_description * env_opts;
  po::positional_options_description * pos_opts;

  po::variables_map vm;
};

#endif

#ifndef UA_OPTIONS_H
#define UA_OPTIONS_H

#include <vector>
#include <string>
#include <iostream>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

class Options {
public:

  Options(int argc, char * argv[]);

  bool help();
  void printHelp();
  void validate();

  std::string getFile();

  friend std::ostream &operator<<(std::ostream &out, const Options &opt);

private:

  po::options_description * desc;
  po::variables_map vm;

  std::string authToken;
  std::string apiserverHost;
  int apiserverPort;

  std::string project;
  std::string folder;
  std::string name;
  std::string file;

  int threads;
  /* int chunks; */
  int chunkSize;
  int tries;
  bool progress;

  void initFromEnvironment();
};

#endif

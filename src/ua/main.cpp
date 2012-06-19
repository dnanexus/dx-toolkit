#include <iostream>

#include "options.h"

using namespace std;

int main(int argc, char * argv[]) {
  Options opt;
  opt.parse(argc, argv);

  if (opt.help() || opt.getFile().empty()) {
    opt.printHelp();
    return 1;
  }

  cerr << opt;
  opt.validate();

  return 0;
}

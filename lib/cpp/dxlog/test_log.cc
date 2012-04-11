#include "dxLog.h"
#include <iostream>
#include <boost/lexical_cast.hpp>

int main(void) {
  string errMsg, msg;
  for(int i = 0; i < 10; i++)
    for(int j = 0; j < 8; j++) {
      cout << i << j << endl;
      int k = i*8 + j;
      msg = "OK " + boost::lexical_cast<string>(k);
      if (DXLog::AppLog::log(msg, errMsg, j)) {
        std::cout << msg + "\n";
      } else {
        std::cout << msg + ":" + errMsg << "\n";
      }
//    sleep(1);
    }

  if (! DXLog::AppLog::done(errMsg)) {
    std::cout << errMsg << "\n";
    exit(1);
  }
  exit(0);
}

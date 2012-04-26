#include "dxLog.h"
#include <iostream>
#include <boost/lexical_cast.hpp>

int main(void) {
  string errMsg;
//  dx::JSON conf = DXLog::readJSON("scheme.js");
//  DXLog::AppLog::initEnv(conf);
  DXLog::logger a("scheme.js");
  cout << "OK\n";

  dx::JSON data(dx::JSON_OBJECT);

//  for(int i = 0; i < 10; i++)
    for(int j = 0; j < 8; j++) {
  //    cout << i << j << endl;
    //  int k = i*8 + j;
      data["level"] = j;
      data["msg"] = "OK " + boost::lexical_cast<string>(j);
      data["source"] = "app";
      data["jobId"] = "testJob";
      data["dbStore"] = true;
      //if (DXLog::AppLog::log(data, errMsg, j)) {
      if (a.Log(data, errMsg)) {
        std::cout << data.toString() + "\n";
      } else {
        std::cout << data.toString() + ":" + errMsg << "\n";
      }
//    sleep(1);
    }

//  if (! DXLog::AppLog::done(errMsg)) {
//    std::cout << errMsg << "\n";
//    exit(1);
//  }
  exit(0);
}

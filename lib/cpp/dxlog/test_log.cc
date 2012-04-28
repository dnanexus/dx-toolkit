#include "dxLog.h"
#include <iostream>
#include <boost/lexical_cast.hpp>

void testDirectLog() {
  string errMsg;
  dx::JSON schema = DXLog::readJSON("schema.js");
  DXLog::logger a(schema);

  dx::JSON data(dx::JSON_OBJECT);
  
  for(int j = 0; j < 8; j++) {
    data["level"] = j;
    data["msg"] = "近期活動 €þıœəßð some utf-8 ĸʒ×ŋµåäö𝄞\nNew Line " + boost::lexical_cast<string>(j);
    data["source"] = "app";
    data["jobId"] = "testJob";
    data["dbStore"] = true;
    
    if (a.Log(data, errMsg)) {
      std::cout << data.toString() + "\n";
    } else {
      std::cout << data.toString() + ":" + errMsg << "\n";
    }
  }
}

void testAppLog() {
  string errMsg;
  dx::JSON conf = DXLog::readJSON("test_appLog_conf.js");
  dx::JSON schema = DXLog::readJSON("schema.js");
  if (! DXLog::AppLog::initEnv(conf, schema, errMsg)) {
    cout << errMsg << endl;
    return;
  }

  dx::JSON data(dx::JSON_OBJECT);
  for(int j = 0; j < 40; j++) {
    data["level"] = j%8;
    data["msg"] = "Test App Log " + boost::lexical_cast<string>(j);
    data["source"] = "app";
    data["jobId"] = "testJob";
    
    if (DXLog::AppLog::log(data, errMsg)) {
      std::cout << data.toString() + "\n";
    } else {
      std::cout << data.toString() + ":" + errMsg << "\n";
    }
  }

  if (! DXLog::AppLog::done(errMsg)) std::cout << errMsg << endl;
}

int main(int argc, char **argv) {
  if (argc == 1) testDirectLog();
  else testAppLog();
  return (0);
}

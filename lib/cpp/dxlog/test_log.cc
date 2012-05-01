#include "dxLog.h"
#include <iostream>
#include <boost/lexical_cast.hpp>

#include <string>
#include <iostream>
#include <stdio.h>

using namespace std;
string exec(const char* cmd) {
  FILE* pipe = popen(cmd, "r");
  if (!pipe) return "ERROR";

  char buffer[1024];	
  string result = "";
		     
  while(!feof(pipe)) {
    if(fgets(buffer, 1024, pipe) != NULL)
      result += buffer;
  }	  
  pclose(pipe);
  return result;
}

bool test(const char *cmd, const string &desired_output) {
  int n = desired_output.length();
  string output = exec(cmd).substr(0, n);
  if (desired_output.compare(output) != 0) {
    cerr << "cmd: " << cmd << endl;
    cerr << "desired output: " << desired_output << endl;
    cerr << "actual output: " << output << endl << endl;
    return false;
  }

  return true;
}
/*
void testDirectLog() {
>>>>>>> 6988c53bacd2e937ee863c54b4c6fc370abec558
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
<<<<<<< HEAD

//  if (! DXLog::AppLog::done(errMsg)) {
//    std::cout << errMsg << "\n";
//    exit(1);
//  }
  exit(0);
=======
  }

  if (! DXLog::AppLog::done(errMsg)) std::cout << errMsg << endl;
}
*/
int main(void) {
  int count[2];
  count[0] = count[1] = 0;
  count[test("./appLogHandler 2>&1", "Usage: appLogHandler configFile")]++; 
  count[test("./appLogHandler test/appLog/non_exist.js 2>&1", "Illegal JSON value. Cannot start with :")]++; 
  count[test("./appLogHandler test/appLog/no_socketPath.js 2>&1", "socketPath is not specified")]++; 
  count[test("./appLogHandler test/appLog/empty_socketPath.js 2>&1", "socketPath is empty")]++; 
  count[test("./appLogHandler test/appLog/no_projectId.js 2>&1", "projectId is not specified")]++; 
  count[test("./appLogHandler test/appLog/no_jobId.js 2>&1", "jobId is not specified")]++; 
  count[test("./appLogHandler test/appLog/no_userId.js 2>&1", "userId is not specified")]++; 
  count[test("./appLogHandler test/appLog/no_programId.js 2>&1", "programId is not specified")]++; 
  count[test("./appLogHandler test/appLog/no_logschema.js 2>&1", "Log schema is not specified")]++; 
  count[test("./appLogHandler test/appLog/no_logschema_file.js 2>&1", "Illegal JSON value. Cannot start with :")]++; 
  count[test("./appLogHandler test/appLog/invalid_logschema.js 2>&1", "api missing 'format' in 'text'")]++; 
  unlink("./test/testlog1");
  count[test("./appLogHandler test/appLog/invalid_socket.js 2>&1", "Socket error: No such file or directory")]++;
  unlink("./test/testlog1");
  count[test("./appLogHandler test/appLog/invalid_socket2.js 2>&1", "Socket error: Address already in use")]++; 
  
  count[test("./verify_logschema test/logschema/invalid_schema.js 2>&1", "Log schema is not a hash")]++; 
  count[test("./verify_logschema test/logschema/invalid_logfacility.js 2>&1", "app Log facility is not an integer")]++; 
  count[test("./verify_logschema test/logschema/invalid_logfacility2.js 2>&1", "api Invalid log facility")]++;
  count[test("./verify_logschema test/logschema/invalid_required.js 2>&1", "cloudManager 'required' is not an array of string")]++; 
  count[test("./verify_logschema test/logschema/invalid_required2.js 2>&1", "api 'required' is not an array of string")]++; 
  count[test("./verify_logschema test/logschema/invalid_maxMsgSize.js 2>&1", "app 'maxMsgSize' is not an integer")]++; 
  count[test("./verify_logschema test/logschema/invalid_maxMsgSize2.js 2>&1", "cloudManager Invalid max message size")]++;
  count[test("./verify_logschema test/logschema/invalid_text.js 2>&1", "api 'text' is not a hash")]++; 
  count[test("./verify_logschema test/logschema/missing_text.js 2>&1", "app missing schema of 'text'")]++;
  count[test("./verify_logschema test/logschema/text_missing_format.js 2>&1", "cloudManager missing 'format' in 'text'")]++;
  count[test("./verify_logschema test/logschema/invalid_text_format.js 2>&1", "api 'format' in 'text' is not a string")]++;
  count[test("./verify_logschema test/logschema/text_missing_tag.js 2>&1", "app missing 'tag' in 'text'")]++;
  count[test("./verify_logschema test/logschema/invalid_text_tag.js 2>&1", "cloudManager 'tag' in 'text' is not a string")]++;
  count[test("./verify_logschema test/logschema/missing_mongodb.js 2>&1", "api missing schema of 'mongodb'")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb.js 2>&1", "app 'mongodb' is not a hash")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb_columns.js 2>&1", "cloudManager 'columns' in 'mongodb' is not a hash")]++;
  count[test("./verify_logschema test/logschema/mongodb_missing_columns.js 2>&1", "api missing 'columns' in 'mongodb'")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb_column_type.js 2>&1", "app column type of mongodb is not a string")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb_column_type2.js 2>&1", "cloudManager invalid column type int32 of mongodb")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb_indexes.js 2>&1", "api 'indexes' in 'mongodb' is not an array of hash")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb_indexes2.js 2>&1", "app 'indexes' in 'mongodb' is not an array of hash")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb_indexes3.js 2>&1", "cloudManager column hostname2 in 'indexes' does not match those in 'columns'")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb_indexes4.js 2>&1", "api index value of timestamp is neither 1 nor -1")]++;
  count[test("./verify_logschema test/logschema/invalid_mongodb_indexes5.js 2>&1", "app index value of timestamp is neither 1 nor -1")]++;
  
  count[test("./dxDbLog test/dBLog/missing_schema.js 2>&1", "log schema is not specified")]++;
  count[test("./dxDbLog test/dBLog/invalid_schema.js 2>&1", "api missing 'format' in 'text'")]++;
  count[test("./dxDbLog test/dBLog/missing_socketPath.js 2>&1", "socketPath is not specified")]++;
  count[test("./dxDbLog test/dBLog/invalid_socketPath.js 2>&1", "listen to socket /dev2/dblog\nSocket error: No such file or directory")]++;
  
  cout << count[0] + count[1] << " tests, " << count[0] << " failed\n"; 
  return (0);
}

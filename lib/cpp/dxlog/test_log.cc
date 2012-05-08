#include "dxLog.h"
#include <iostream>
#include <boost/lexical_cast.hpp>

#include <string>
#include <iostream>
#include <stdio.h>

using namespace std;
string myPath() {
  char buff[10000];
  size_t len = readlink("/proc/self/exe", buff, 9999);
  buff[len] = '\0';
  string ret_val = string(buff);
  int k = ret_val.find_last_of('/');
  return ret_val.substr(0, k);
}

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

bool test(const string cmd, const string &desired_output) {
  int n = desired_output.length();
  string output = exec(cmd.c_str()).substr(0, n);
  cout << cmd << endl;
  if (desired_output.compare(output) != 0) {
    cerr << "cmd: " << cmd << endl;
    cerr << "desired output: " << desired_output << endl;
    cerr << "actual output: " << output << endl << endl;
    throw("OK");
    return false;
  }

  return true;
}

bool test2(const string &filename) {
  string errMsg;
  dx::JSON schema = DXLog::readJSON(myPath() + "/../../../../logserver/config/schema.js");
  DXLog::logger a(schema);

  dx::JSON data = DXLog::readJSON(myPath() + "/" + filename);
  for (int i = 0; i< data.size(); i++) { 
    if (! a.Log(data[i], errMsg)) {
      cerr << data[i].toString() + ":" + errMsg << "\n";
      return false;
    }
  }

  return true;
}

bool test3(const string &filename) {
  string errMsg;
  dx::JSON schema = DXLog::readJSON(myPath() + "/../../../../logserver/config/schema.js");
  DXLog::logger a(schema);

  dx::JSON data = DXLog::readJSON(myPath() + "/" + filename);
  for (dx::JSON::object_iterator it = data.object_begin(); it != data.object_end(); it++) {
    string desired_output = it->first; 
    bool ret_val = a.Log(it->second, errMsg);
    if (ret_val) {
      cerr << desired_output << endl;
      return false;
    }

    if (desired_output.compare(errMsg.substr(0, desired_output.length())) != 0) {
      cerr << "desired error msg: " << desired_output << endl;
      cerr << "actual error msg: " << errMsg << endl << endl;
      return false;
    }
  }

  return true;
}

bool testAppLog() {
  bool ret_val = true;
  string errMsg;
  dx::JSON startJSON = DXLog::readJSON(myPath() + "/test/start_joblog.js");
  string cmd = "dx_startJobLog '" + startJSON.toString() + "'";
  cout << cmd << endl;
  system(cmd.c_str());

  dx::JSON stopJSON = DXLog::readJSON(myPath() + "/test/stop_joblog.js");
  dx::JSON schema = DXLog::readJSON("/etc/dxlog/schema.js");
  if (! DXLog::AppLog::initEnv(stopJSON, schema, errMsg)) {
    cerr << errMsg << endl;
    return false;
  }

  dx::JSON data(dx::JSON_OBJECT);
  for(int j = 0; j < 10; j++) {
    data["level"] = j%8;
    data["msg"] = "Test App Log " + boost::lexical_cast<string>(j);
    data["jobId"] = "testJob";
    
    if (! DXLog::AppLog::log(data, errMsg)) {
      cerr << data.toString() + ":" + errMsg << "\n";
      ret_val = false;
      break;
    }
  }

  cmd = "dx_stopJobLog '" + stopJSON.toString() + "'";
  system(cmd.c_str());
  return ret_val;
}

int main(void) {
  int count[2];
  count[0] = count[1] = 0;
  count[test(myPath() + "/dx_appLogHandler 2>&1", "Usage: appLogHandler configFile")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/non_exist.js 2>&1", "Illegal JSON value. Cannot start with :")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/no_socketPath.js 2>&1", "socketPath is not specified")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/empty_socketPath.js 2>&1", "socketPath is empty")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/no_projectId.js 2>&1", "projectId is not specified")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/no_jobId.js 2>&1", "jobId is not specified")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/no_userId.js 2>&1", "userId is not specified")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/no_programId.js 2>&1", "programId is not specified")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/no_logschema.js 2>&1", "Log schema is not specified")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/no_logschema_file.js 2>&1", "Illegal JSON value. Cannot start with :")]++; 
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/invalid_logschema.js 2>&1", "api missing 'format' in 'text'")]++; 
  unlink("./test/testlog1");
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/invalid_socket.js 2>&1", "Socket error: No such file or directory")]++;
  unlink("./test/testlog1");
  count[test(myPath() + "/dx_appLogHandler " + myPath() + "/test/appLog/invalid_socket2.js 2>&1", "Socket error: Address already in use")]++; 
  
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_schema.js 2>&1", "Log schema is not a hash")]++; 
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_logfacility.js 2>&1", "app Log facility is not an integer")]++; 
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_logfacility2.js 2>&1", "api Invalid log facility")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_required.js 2>&1", "cloudManager 'required' is not an array of string")]++; 
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_required2.js 2>&1", "api 'required' is not an array of string")]++; 
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_maxMsgSize.js 2>&1", "app 'maxMsgSize' is not an integer")]++; 
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_maxMsgSize2.js 2>&1", "cloudManager Invalid max message size")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_text.js 2>&1", "api 'text' is not a hash")]++; 
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/missing_text.js 2>&1", "app missing schema of 'text'")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/text_missing_format.js 2>&1", "cloudManager missing 'format' in 'text'")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_text_format.js 2>&1", "api 'format' in 'text' is not a string")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/text_missing_tag.js 2>&1", "app missing 'tag' in 'text'")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_text_tag.js 2>&1", "cloudManager 'tag' in 'text' is not a string")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/missing_mongodb.js 2>&1", "api missing schema of 'mongodb'")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb.js 2>&1", "app 'mongodb' is not a hash")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb_columns.js 2>&1", "cloudManager 'columns' in 'mongodb' is not a hash")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/mongodb_missing_columns.js 2>&1", "api missing 'columns' in 'mongodb'")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb_column_type.js 2>&1", "app column type of mongodb is not a string")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb_column_type2.js 2>&1", "cloudManager invalid column type int32 of mongodb")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb_indexes.js 2>&1", "api 'indexes' in 'mongodb' is not an array of hash")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb_indexes2.js 2>&1", "app 'indexes' in 'mongodb' is not an array of hash")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb_indexes3.js 2>&1", "cloudManager column hostname2 in 'indexes' does not match those in 'columns'")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb_indexes4.js 2>&1", "api index value of timestamp is neither 1 nor -1")]++;
  count[test(myPath() + "/verify_logschema " + myPath() + "/test/logschema/invalid_mongodb_indexes5.js 2>&1", "app index value of timestamp is neither 1 nor -1")]++;
  
  count[test(myPath() + "/dx_dbLog " + myPath() + "/test/dBLog/missing_schema.js 2>&1", "log schema is not specified")]++;
  count[test(myPath() + "/dx_dbLog " + myPath() + "/test/dBLog/invalid_schema.js 2>&1", "api missing 'format' in 'text'")]++;
  count[test(myPath() + "/dx_dbLog " + myPath() + "/test/dBLog/missing_socketPath.js 2>&1", "socketPath is not specified")]++;
  count[test(myPath() + "/dx_dbLog " + myPath() + "/test/dBLog/invalid_socketPath.js 2>&1", "listen to socket /dev2/dblog\nSocket error: No such file or directory")]++;
 
  count[test2("test/messages/api.js")] ++;
  count[test2("test/messages/app.js")] ++;
  count[test2("test/messages/cloudmanager.js")] ++;
  count[test2("test/messages/jobserver.js")] ++;
  count[test2("test/messages/execserver.js")] ++;
  count[test2("test/messages/audit.js")] ++;
  count[test3("test/messages/malformatted.js")] ++;

  count[testAppLog()] ++;
  cout << count[0] + count[1] << " tests, " << count[0] << " failed\n"; 
  return (0);
}

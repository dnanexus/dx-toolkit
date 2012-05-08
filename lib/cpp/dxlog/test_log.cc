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

bool test2(const string &filename) {
  string errMsg;
  dx::JSON schema = DXLog::readJSON("../../../../logserver/config/schema.js");
  DXLog::logger a(schema);

  dx::JSON data = DXLog::readJSON("./" + filename);
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
  dx::JSON schema = DXLog::readJSON("../../../../logserver/config/schema.js");
  DXLog::logger a(schema);

  dx::JSON data = DXLog::readJSON("./" + filename);
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

/*void testAppLog() {
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
 
  count[test2("test/messages/api.js")] ++;
  count[test2("test/messages/app.js")] ++;
  count[test2("test/messages/cloudmanager.js")] ++;
  count[test2("test/messages/jobserver.js")] ++;
  count[test2("test/messages/execserver.js")] ++;
  count[test2("test/messages/audit.js")] ++;
  count[test3("test/messages/malformatted.js")] ++;

  cout << count[0] + count[1] << " tests, " << count[0] << " failed\n"; 
  return (0);
}

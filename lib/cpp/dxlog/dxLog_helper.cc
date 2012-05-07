#include "dxLog.h"
#include "dxLog_helper.h"
#include <boost/lexical_cast.hpp>
#include "unistd.h"
#include <fstream>

void DXLog::throwString(const string &msg) {
  throw(msg);
}

string DXLog::levelString(int level) {
  switch(level) {
    case 0: return "EMERG";
    case 1: return "ALERT";
    case 2: return "CRIT";
    case 3: return "ERR";
    case 4: return "WARNING";
    case 5: return "NOTICE";
    case 6: return "INFO";
    default : return "DEBUG";
  }
}

string DXLog::getHostname() {
  char buf[1001];
  gethostname(buf, 1000);
  return string(buf);
}

string DXLog::timeISOString(int64 utc) {
  time_t t = utc/1000;
  struct tm *ptm = gmtime( &t);
  char timeString[80];
  strftime(timeString, 80, "%Y-%m-%dT%H:%M:%SZ", ptm);
  return string(timeString);
}

void DXLog::ValidateLogLevel(const dx::JSON &data) {
  if (data.type() != dx::JSON_INTEGER) throwString("Log level is not an integer");

  int level = int(data);
  if ((level < 0) || (level > 7)) throwString("Invalid log level " + boost::lexical_cast<string>(level));
}

void DXLog::ValidateLogFacility(const dx::JSON &data) {
  if (data.type() != dx::JSON_INTEGER) throwString("Log facility is not an integer");
 
  int facility = int(data);
  int k = facility >> 3;
  if (((facility % 8) != 0) || (k < 0) || (k > 23)) throwString("Invalid log facility " + boost::lexical_cast<string>(facility));
}

void DXLog::ValidateLogRequired(const dx::JSON &required) {
  if (required.type() != dx::JSON_ARRAY) throwString("'required' is not an array of strings");

  for (int i = 0; i < required.size(); i++) {
    if (required[i].type() != dx::JSON_STRING) throwString("'required' is not an array of strings");
  }
}

void DXLog::ValidateLogMsgSize(const dx::JSON &msgSize) {
  if (msgSize.type() != dx::JSON_INTEGER) throwString("'maxMsgSize' is not an integer");

  int s = int(msgSize);
  if ((s<100) || (s> 100000)) throwString("Invalid max message size " + boost::lexical_cast<string>(s));
}

void DXLog::ValidateLogText(const dx::JSON &text) {
  if (text.type() != dx::JSON_OBJECT) throwString("'text' is not a hash ");

  if (! text.has("format")) throwString("missing 'format' in 'text'");
  if (text["format"].type() != dx::JSON_STRING) throwString("'format' in 'text' is not a string");

  if (! text.has("tag")) throwString("missing 'tag' in 'text'");
  if (text["tag"].type() != dx::JSON_STRING) throwString("'tag' in 'text' is not a string");

  if (text.has("maxMsgSize")) ValidateLogMsgSize(text["maxMsgSize"]);
}

void DXLog::ValidateLogMongoDBColumns(const dx::JSON &columns) {
  if (columns.type() != dx::JSON_OBJECT) throwString("'columns' in 'mongodb' is not a hash");

  for (dx::JSON::const_object_iterator it = columns.object_begin(); it != columns.object_end(); it++) {
    if (it->second.type() != dx::JSON_STRING) throwString("column type of mongodb is not a string");

    string str = it->second.get<string>();
    if (str.compare("string") == 0) continue;
    if (str.compare("int") == 0) continue;
    if (str.compare("int64") == 0) continue;
    if (str.compare("boolean") == 0) continue;
    if (str.compare("double") == 0) continue;

    throwString("invalid column type " + str + " of mongodb");
  }
}

void DXLog::ValidateLogMongoDBIndex(const dx::JSON &index, const dx::JSON &columns) {
  if (index.type() != dx::JSON_OBJECT) throwString("'indexes' in 'mongodb' is not an array of hash");
  
  for (dx::JSON::const_object_iterator it = index.object_begin(); it != index.object_end(); it++) {
    if (! columns.has(it->first)) throwString("column " + it-> first + " in 'indexes' does not match those in 'columns'");
    if (it->second.type() != dx::JSON_INTEGER) throwString("index value of " + it->first + " is neither 1 nor -1");
    int k = int(it->second);
    if ((k != 1) && (k!= -1)) throwString("index value of " + it->first + " is neither 1 nor -1");
  }
}

void DXLog::ValidateLogMongoDBIndexes(const dx::JSON &indexes, const dx::JSON &columns) {
  if (indexes.type() != dx::JSON_ARRAY) throwString("'indexes' in 'mongodb' is not an array of hash");

  for (int i = 0; i < indexes.size(); i++)
    ValidateLogMongoDBIndex(indexes[i], columns);
}


void DXLog::ValidateLogMongoDB(const dx::JSON &mongodb) {
  if (mongodb.type() != dx::JSON_OBJECT) throwString("'mongodb' is not a hash");

  if (mongodb.has("maxMsgSize")) ValidateLogMsgSize(mongodb["maxMsgSize"]);

  if (! mongodb.has("columns")) throwString("missing 'columns' in 'mongodb'");
  ValidateLogMongoDBColumns(mongodb["columns"]);
  
  if (mongodb.has("indexes")) ValidateLogMongoDBIndexes(mongodb["indexes"], mongodb["columns"]);
}

void DXLog::ValidateLogSchemaSingle(const dx::JSON &schema) {
  if (schema.type() != dx::JSON_OBJECT) throwString("Log schema is not a hash");

  if (schema.has("facility")) ValidateLogFacility(schema["facility"]);
  if (schema.has("required")) ValidateLogRequired(schema["required"]);
  
  if (! schema.has("text")) throwString("missing schema of 'text'");
  ValidateLogText(schema["text"]);
  
  if (! schema.has("mongodb")) throwString("missing schema of 'mongodb'");
  ValidateLogMongoDB(schema["mongodb"]);
}

void DXLog::ValidateLogSchema(const dx::JSON &schema) {
  if (schema.type() != dx::JSON_OBJECT) throwString("Log schema is not a hash");

  for (dx::JSON::const_object_iterator it = schema.object_begin(); it != schema.object_end(); it++) {
    try {
      ValidateLogSchemaSingle(it->second);
    } catch (const string &errMsg) {
      throwString(it->first + " " + errMsg);
    }
  }
}

bool DXLog::formMessageHead(int facility, int level, const string &tag, string &head, string &errMsg) {
  try {
    ValidateLogLevel(level);
    ValidateLogFacility(facility);
  } catch (char *e) {
    errMsg = e;
    return false;
  }

  char pri[5];
  sprintf(pri, "%d", facility | level);

  string s= "<";
  head = (tag.length() > 100) ? s + pri + ">" + tag.substr(0, 100) : s + pri + ">" + tag;

  return true;
}

string DXLog::randomString(int n) {
  char *s = new char[n+1];
  static const char list[] = "0123456789abcdefghijklmnopqrstuvwxyz";
  for(int i = 0; i < n; i ++)
    s[i] = list[rand() * 35];

  return string(s);
}

void DXLog::splitMessage(const string &msg, vector<string> &Msgs, int msgSize) {
  // generate a random string to index the msg
  string s = randomString(20);

  int offset = 0, index = 0;
  Msgs.clear();
  while (offset < msg.size()) {
    Msgs.push_back(msg.substr(offset, msgSize) + " " + s + " - " + boost::lexical_cast<string>(index++));
    offset += msgSize;
  }
}

bool DXLog::SendMessage2Rsyslog(int facility, int level, const string &tag, const string &msg, int msgSize, string &errMsg) {
  string head;
  if (! formMessageHead(facility, level, tag, head, errMsg)) return false;
  
  if (msg.length() < msgSize) return SendMessage2UnixDGRAMSocket("/dev/log", head + " " + msg, errMsg);

  vector<string> Msgs;
  splitMessage(msg, Msgs, msgSize);

  for (int i = 0; i < Msgs.size(); i++) {
    if (! SendMessage2UnixDGRAMSocket("/dev/log", head + " " + Msgs[i], errMsg)) return false;
  }

  return true;
}

void DXLog::StoreMsgLocal(const string &filename, const string &msg) {
  time_t rawtime;
  time(&rawtime);
  struct tm *ptm = localtime(&rawtime);
  char timeString[20];
  strftime(timeString, 20, "%Y%m%d%H", ptm);

  cout << filename + timeString + ".log" << endl;
  ofstream messageFile((filename + timeString + "_" + boost::lexical_cast<string>(getpid()) + ".log").c_str(), ios::app);
  messageFile << msg << endl;
  messageFile.close();
}

#include "dxLog.h"
#include "dxLog_helper.h"
#include <boost/lexical_cast.hpp>
#include "unistd.h"
#include <fstream>

using namespace std;
using boost::gregorian::date;
using boost::posix_time::ptime;
using boost::posix_time::microsec_clock;

string DXLog::myPath() {
  char buff[10000];
  size_t len = readlink("/proc/self/exe", buff, 9999);
  buff[len] = '\0';
  string ret_val = string(buff);
  int k = ret_val.find_last_of('/');
  return ret_val.substr(0, k);
}

void DXLog::throwString(const string &msg) {
  throw(msg);
}

string DXLog::getHostname() {
  char buf[1001];
  gethostname(buf, 1000);
  return string(buf);
}

long long int DXLog::utcMS(){
  static ptime const epoch(date(1970, 1, 1));
  return (microsec_clock::universal_time() - epoch).total_milliseconds();
}

dx::JSON DXLog::defaultConf() { 
  return readJSON(myPath() + "/../config/dxlog.conf");
}

dx::JSON DXLog::defaultSchema() {
  return readJSON(myPath() + "/../config/dbSchema.js");
}

void DXLog::ValidateLogLevel(const dx::JSON &data) {
  if (data.type() != dx::JSON_INTEGER) throwString("Log level, " + data.toString() + ", is not an integer");

  int level = int(data);
  if ((level < 0) || (level > 7)) throwString("Invalid log level: " + boost::lexical_cast<string>(level));
}

void DXLog::ValidateLogFacility(const dx::JSON &data) {
  if (data.type() != dx::JSON_INTEGER) throwString("Log facility, " + data.toString() + ", is not an integer");
 
  int facility = int(data);
  int k = facility >> 3;
  if (((facility % 8) != 0) || (k < 0) || (k > 23)) throwString("Invalid log facility: " + boost::lexical_cast<string>(facility));
}

void DXLog::ValidateDBSchema(const dx::JSON &schema) {
  if (schema.type() != dx::JSON_OBJECT) throwString("Mongodb schema, " + schema.toString() + ", is not a JSON object");
  
  for (dx::JSON::const_object_iterator it = schema.object_begin(); it != schema.object_end(); it++) {
    if (it->second.type() != dx::JSON_OBJECT) throwString(it->first + " mongodb schema, " + it->second.toString() + ", is not a JSON object");
    if (! it->second.has("collection")) throwString(it->first + ": missing collection");
    if (it->second["collection"].type() != dx::JSON_STRING) throwString(it->first + ": collection, " + it->second["collection"].toString() + ", is not a string");
  }
}

bool DXLog::ValidateLogData(dx::JSON &message, string &errMsg) {
  // validate message
  try {
    if (message.type() != dx::JSON_OBJECT) throwString("Log input, " + message.toString() + ", is not a JSON object");

    if (! message.has("timestamp")) message["timestamp"] = utcMS();
    if (message["timestamp"].type() != dx::JSON_INTEGER) throwString("Log timestamp, " + message["timestamp"].toString() + ", is not an integer");

    if (! message.has("source")) throwString("Missing log source");
    if (message["source"].type() != dx::JSON_STRING) throwString("Log source, " + message["source"].toString() + ", is not a string");
    string source = message["source"].get<string>();
    if (source.substr(0,3).compare("DX_") != 0) throwString("Invalid log source: " + source);

    if (! message.has("level")) message["level"] = 6;
    ValidateLogLevel(message["level"]);

    if (! message.has("hostname")) message["hostname"] = getHostname();
    if (message["hostname"].type() != dx::JSON_STRING) throwString("Log hostname, " + message["hostname"].toString() + ", is not a string");

    return true;
  } catch (const string &msg) {
    errMsg = msg;
    return false;
  } catch (std::exception &e) {
    errMsg = string("JSONException: ") + e.what();
    return false;
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
  for(int i = 0; i < n; i ++) {
    s[i] = list[int(double(rand())/double(RAND_MAX) * 35)];
  }
  s[n] = '\0';

  string ret_val = string(s);
  delete [] s;
  return ret_val;
}

bool DXLog::SendMessage2Rsyslog(int level, const string &source, const string &msg, string &errMsg, const string &socketPath) {
  string head;
  if (! formMessageHead(8, level, source, head, errMsg)) return false;
  return SendMessage2UnixDGRAMSocket(socketPath, head + " " + msg, errMsg);
}

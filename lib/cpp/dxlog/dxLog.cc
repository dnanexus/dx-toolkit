#include "unixDGRAM.h"
#include <boost/lexical_cast.hpp>
#include "dxLog.h"

bool DXLog::AppLog::initialized = false;
string DXLog::AppLog::socketPath[2];
int DXLog::AppLog::msgCount[2];
dx::JSON DXLog::AppLog::data(dx::JSON_OBJECT);

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

void DXLog::formMessageHead(int facility, int level, const string &tag, string &head) {
  char pri[5], tStr[30];

  if ((level < 0) || (level > 7)) throw string("Invalid log level");
  int k = facility >> 3;
  if (((facility % 8) != 0) || (k < 0) || (k > 23)) throw string("Invalid log facility");

  sprintf(pri, "%d", facility | level);

  string s= "<";
  head = (tag.length() > 100) ? s + pri + ">" + tag.substr(0, 100) : s + pri + ">" + tag;
}

bool DXLog::splitMessage(const string &msg, vector<string> &Msgs, int msgSize) {
  if (msg.size() <= msgSize) return false;

  // generate a random string to index the msg
  char s[21];
  static const char list[] = "0123456789abcdefghijklmnopqrstuvwxyz";
  for(int i = 0; i < 20; i ++)
    s[i] = list[rand() * 35];

  int offset = 0, index = 0;
  Msgs.clear();
  while (offset < msg.size()) {
    Msgs.push_back(msg.substr(offset, msgSize) + " " + s + " - " + boost::lexical_cast<string>(index++));
    offset += msgSize;
  }

  return true;
}

bool DXLog::SendMessage2Rsyslog(int facility, int level, const string &tag, const string &msg, string &errMsg, int msgSize) {
  string head;
  formMessageHead(facility, level, tag, head);
  
  if (msg.length() < msgSize) return SendMessage2UnixDGRAMSocket("/dev/log", head + " " + msg, errMsg);

  vector<string> Msgs;
  splitMessage(msg, Msgs, msgSize);
  bool ret_val = true;
  string errmsg;
  for (int i = 0; i < Msgs.size(); i++) {
    if (! SendMessage2UnixDGRAMSocket("/dev/log", head + " " + Msgs[i], errmsg)) {
      ret_val = false;
      errMsg = errmsg;
    }
  }

  return ret_val;
}

// need to be implemented once those values are available
bool DXLog::AppLog::initEnv() {
  data["projectId"] = "testProject";
  data["jobId"] = "testJob";
  data["userId"] = "testUser";
  data["appId"] = "testApp";
  socketPath[0] = "log3";
  socketPath[1] = "log3";
  msgCount[0] = 0;
  msgCount[1] = 0;
  return true;
}

int DXLog::AppLog::socketIndex(int level) {
  return (level < 3) ? 0 : 1;
}

bool DXLog::AppLog::log(const string &msg, string &errMsg, int level) {
  if (! initialized) {
    initEnv();
    initialized = true;
  }

  if (msg.size() > 2000) {
    errMsg = "Message size bigger than 2K";
    return false;
  }

  int index = socketIndex(level);

  if (msgCount[index] >= 1000) {
      errMsg = "Messages beyond rate limitation";
      return false;
  }

  dx::JSON data(dx::JSON_OBJECT);
  data["timestamp"] = int64(time(NULL));
  data["msg"] = msg;
  data["level"] = level;
  if (! SendMessage2UnixDGRAMSocket(socketPath[index], data.toString(), errMsg)) return false;

  msgCount[index]++;
  return true;
}


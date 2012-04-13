#include "unixDGRAM.h"
#include <boost/lexical_cast.hpp>
#include "dxLog.h"
#include <fstream>

string DXLog::AppLog::socketPath[2];
int DXLog::AppLog::msgCount[2] = {0, 0};
int DXLog::AppLog::msgSize = 2000;
int DXLog::AppLog::msgLimit = 1000;

dx::JSON DXLog::AppLog::data(dx::JSON_OBJECT);

dx::JSON DXLog::readJSON(const string &filename) {
  dx::JSON ret_val;
  ifstream in(filename.c_str());
  ret_val.read(in);
  in.close();
  return ret_val;
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

void DXLog::formMessageHead(int facility, int level, const string &tag, string &head) {
  char pri[5];

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
void DXLog::AppLog::initEnv(const dx::JSON &conf) {
  data["projectId"] = conf["projectId"].get<string>();
  data["jobId"] = conf["jobId"].get<string>();
  data["userId"] = conf["userId"].get<string>();
  data["appId"] = conf["appId"].get<string>();
  socketPath[0] = conf["socketPath"][0].get<string>();
  socketPath[1] = conf["socketPath"][1].get<string>();
  msgSize = int(conf["maxMsgSize"]);
  msgLimit = int(conf["maxMsgNumber"]);
}

int DXLog::AppLog::socketIndex(int level) {
  return (level < 3) ? 0 : 1;
}

bool DXLog::AppLog::log(const string &msg, string &errMsg, int level) {
  if (msg.size() > msgSize) {
    errMsg = "Message size bigger than " + boost::lexical_cast<string>(msg);
    return false;
  }

  int index = socketIndex(level);

  if (msgCount[index] >= msgLimit) {
      errMsg = "Number of messages exceeds " + boost::lexical_cast<string>(msgLimit);
      return false;
  }

  data["timestamp"] = int64(time(NULL)*1000);
  data["msg"] = msg;
  data["level"] = level;
  if (! SendMessage2UnixDGRAMSocket(socketPath[index], data.toString(), errMsg)) return false;

  msgCount[index]++;
  return true;
}

bool DXLog::AppLog::done(string &errMsg) {
  if (socketPath[0].compare(socketPath[1]) == 0) return SendMessage2UnixDGRAMSocket(socketPath[0], "Done", errMsg);
  return (SendMessage2UnixDGRAMSocket(socketPath[0], "Done", errMsg) && SendMessage2UnixDGRAMSocket(socketPath[1], "Done", errMsg));
}


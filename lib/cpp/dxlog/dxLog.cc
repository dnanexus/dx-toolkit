#include <boost/lexical_cast.hpp>
#include "dxLog.h"
#include "dxLog_helper.h"
#include <fstream>

dx::JSON DXLog::readJSON(const string &filename) {
  dx::JSON ret_val;
  ifstream in(filename.c_str());
  ret_val.read(in);
  in.close();
  return ret_val;
}

bool DXLog::ValidateLogData(const dx::JSON &config, dx::JSON &message, string &errMsg) {
  // validate message
  if (! message.has("timestamp")) message["timestamp"] = int64(time(NULL)*1000);

  if (! message.has("source")) {
    errMsg = "Missing source of the log";
    return false;
  }

  string source = message["source"].get<string>();
  if (! config.has(source)) {
    errMsg = "Invalid log source " + source;
    return false;
  }

  dx::JSON tConfig = config[source];
  for (int i = 0; i < tConfig["required"].size(); i++) {
    if (! message.has(tConfig["required"][i].get<string>())) {
      errMsg = "Missing " + tConfig["required"].get<string>();
      return false;
    }
  }

  if (tConfig.has("facility")) message["facility"] = int(tConfig["facility"]);
  if (! message.has("facility")) message["facility"] = 8;

  return true;
}

bool DXLog::logger::Log(dx::JSON &message, string &errMsg) {
  if (! ValidateLogData(config, message, errMsg)) return false;

  dx::JSON tConfig = config[message["source"].get<string>()]["text"];
  int maxMsgSize = (tConfig.has("maxMsgSize")) ? int(tConfig["maxMsgSize"]) : 2000;

  bool ret_val = SendMessage2Rsyslog(int(message["facility"]), int(message["level"]), tConfig["tag"].get<string>(), message["msg"].get<string>(), errMsg, maxMsgSize);

  bool dbStore = (message.has("dbStore")) ? bool(message["dbStore"]) : false;
  if (ret_val && dbStore) return SendMessage2UnixDGRAMSocket("/dev/dblog", message.toString(), errMsg);
  return ret_val;
}

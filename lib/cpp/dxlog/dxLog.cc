#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include "dxLog.h"
#include "dxLog_helper.h"
#include <fstream>

string DXLog::AppLog::socketPath[2];
int DXLog::AppLog::msgCount[2] = {0, 0};
int DXLog::AppLog::msgLimit = 1000;
bool DXLog::AppLog::initialized = false;

//dx::JSON DXLog::AppLog::schema(dx::JSON_OBJECT);

dx::JSON DXLog::readJSON(const string &filename) {
  dx::JSON ret_val;
  ifstream in(filename.c_str());
  ret_val.read(in);
  in.close();
  return ret_val;
}

bool DXLog::ValidateLogData(const dx::JSON &config, dx::JSON &message, string &errMsg) {
  // validate message
  try {
    if (! message.type() == dx::JSON_OBJECT) throwString("log input is not a hash");

    if (! message.has("timestamp")) message["timestamp"] = (long long int)(time(NULL)*1000);

    if (! message.has("source")) throwString("Missing source of the log");
    string source = message["source"].get<string>();
    if (! config.has(source)) throwString("Invalid log source " + source);

    dx::JSON tConfig = config[source];
    for (int i = 0; i < tConfig["required"].size(); i++) {
      if (! message.has(tConfig["required"][i].get<string>())) throwString("Missing " + tConfig["required"][i].get<string>());
    }

    if (tConfig.has("facility")) message["facility"] = int(tConfig["facility"]);
    if (! message.has("facility")) message["facility"] = 8;
    ValidateLogFacility(message["facility"]);
    if (! message.has("level")) message["level"] = 6;
    ValidateLogLevel(message["level"]);
   
    bool dbStore = (message.has("dbStore")) ? bool(message["dbStore"]) : false;
    if (int(message["level"]) < 3) dbStore = true;
    if (dbStore) {
      int maxMsgSize = (tConfig["mongodb"].has("maxMsgSize")) ? int(tConfig["mongodb"]["maxMsgSize"]) : 2000;
      if (! message["msg"].get<string>().size() > maxMsgSize) throwString("Log message too log");
    }

    return true;
  } catch (const string &msg) {
    errMsg = msg;
    return false;
  } catch (std::exception &e) {
    errMsg = string("JSONException: ") + e.what();
    return false;
  }
}

void DXLog::logger::formMessage(const dx::JSON &message, string &msg) {
  dx::JSON columns = schema[message["source"].get<string>()]["required"];
  msg = schema[message["source"].get<string>()]["text"]["format"].get<string>();

  size_t index;
  for (int i = 0; i < columns.size(); i++) {
    string key = columns[i].get<string>();
    if ((index = msg.find("{" + key + "}")) != string::npos)
      msg.replace(index, key.length()+2, message[key].get<string>());
  }
}

DXLog::logger::logger(){
  try {
    dx::JSON dConf = defaultConf();
    schema = defaultSchema();
    ValidateLogSchema(schema);
    txtMsgFile = dConf["logserver"]["logDir"].get<string>() + "/local/Cppsyslog";
    dbMsgFile = dConf["logserver"]["logDir"].get<string>() + "/local/CppDBSocket";
    hostname = getHostname();
    ready = true;
  } catch (const string &msg) {
    errmsg = string("Invalid log schema: ") + msg;
    ready = false;
  }
}

bool DXLog::logger::isReady(string &msg) {
  if (ready) return true;
  msg = errmsg;
  return false;
}

bool DXLog::logger::Log(dx::JSON &message, string &eMsg) {
  if (! isReady(eMsg)) return false;
  if (! ValidateLogData(schema, message, eMsg)) return false;
  if (! message.has("hostname")) message["hostname"] = hostname;

  dx::JSON tConfig = schema[message["source"].get<string>()]["text"];
  
  int maxMsgSize = (tConfig.has("maxMsgSize")) ? int(tConfig["maxMsgSize"]) : 2000;

  string msg = tConfig["format"].get<string>();
  formMessage(message, msg);

  bool ret_val = SendMessage2Rsyslog(int(message["facility"]), int(message["level"]), tConfig["tag"].get<string>(), msg, maxMsgSize, eMsg);
  if (! ret_val) StoreMsgLocal(txtMsgFile, message.toString());

  bool dbStore = (message.has("dbStore")) ? bool(message["dbStore"]) : false;
  if (int(message["level"]) < 3) dbStore = true;
  if (ret_val && dbStore) {
    ret_val = SendMessage2UnixDGRAMSocket("/dev/dblog", message.toString(), eMsg);
    if (! ret_val) StoreMsgLocal(dbMsgFile, message.toString());
  }
  return ret_val;
}

// need to be implemented once those values are available
bool DXLog::AppLog::initEnv(const dx::JSON &conf, string &errMsg) {
  try {
    socketPath[0] = conf["socketPath"][0].get<string>();
    socketPath[1] = conf["socketPath"][1].get<string>();

    initialized = true;
    return true;
  } catch (const string &msg) {
    errMsg = msg;
    return false;
  } catch (std::exception &e) {
    errMsg = e.what();
    return false;
  }
}

int DXLog::AppLog::socketIndex(int level) {
  return (level < 3) ? 0 : 1;
}

bool DXLog::AppLog::log(dx::JSON &message, string &errMsg) {
  try {
    if (! initialized) {
      dx::JSON input = dx::JSON(dx::JSON_OBJECT);
      input["socketPath"] = dx::JSON(dx::JSON_ARRAY);
      input["socketPath"].push_back(defaultPrioritySocket);
      input["socketPath"].push_back(defaultBulkSocket);
      if (! initEnv(input, errMsg)) return false;
    }
    message["source"] = "app";

    int index = socketIndex(int(message["level"]));
    if (msgCount[index] >= msgLimit) {
      errMsg = "Number of messages exceeds " + boost::lexical_cast<string>(msgLimit);
      return false;
    }

    if (! boost::filesystem::exists(socketPath[index])) {
      errMsg = "Socket " + socketPath[index] + " does not exist!";
      return false;
    }

    if (! SendMessage2UnixDGRAMSocket(socketPath[index], message.toString(), errMsg)) return false;

    msgCount[index]++;
    return true;
  } catch (const string &msg) {
    errMsg = msg;
    return false;
  } catch (std::exception &e) {
    errMsg = e.what();
    return false;
  }
}

bool DXLog::AppLog::done(string &errMsg) {
  if (socketPath[0].compare(socketPath[1]) == 0) return SendMessage2UnixDGRAMSocket(socketPath[0], "Done", errMsg);
  return (SendMessage2UnixDGRAMSocket(socketPath[0], "Done", errMsg) && SendMessage2UnixDGRAMSocket(socketPath[1], "Done", errMsg));
}

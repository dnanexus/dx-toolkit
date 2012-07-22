#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include "dxLog.h"
#include <fstream>

string DXLog::AppLog::socketPath[2];
bool DXLog::AppLog::initialized = false;

//dx::JSON DXLog::AppLog::schema(dx::JSON_OBJECT);

dx::JSON DXLog::readJSON(const string &filename) {
  dx::JSON ret_val;
  ifstream in(filename.c_str());
  ret_val.read(in);
  in.close();
  return ret_val;
}

bool DXLog::logger::Log(dx::JSON &data, string &eMsg, const string &socketPath) {
  if (data.type() != dx::JSON_OBJECT) {
    eMsg = "Log input, " + data.toString() + ", is not a JSON object";
    return false;
  }

  if (! data.has("hostname")) data["hostname"] = hostname;
  if (! ValidateLogData(data, eMsg)) return false;

  return SendMessage2Rsyslog(int(data["level"]), data["source"].get<string>(), data.toString(), eMsg, socketPath);
}

// need to be implemented once those values are available
bool DXLog::AppLog::initEnv(const dx::JSON &conf, string &errMsg) {
  try {
    if (conf.type() != dx::JSON_OBJECT) throwString("App log config, " + conf.toString() + ", is not a JSON object");
    if (! conf.has("socketPath")) throwString("Missing socketPath in App log config");
    if (conf["socketPath"].type() != dx::JSON_ARRAY) throwString("socketPath, " + conf["socketPath"].toString() + ", is not a JSON array of strings");
    if (conf["socketPath"].size() < 2) throwString("Size of socketPath is smaller than 2");
    if ((conf["socketPath"][1].type() != dx::JSON_STRING) || (conf["socketPath"][0].type() != dx::JSON_STRING)) throwString("socketPath, " + conf["socketPath"].toString() + ", is not a JSON array of strings");

    socketPath[0] = conf["socketPath"][0].get<string>();
    socketPath[1] = conf["socketPath"][1].get<string>();

    initialized = true;
    return true;
  } catch (const string &msg) {
    errMsg = msg;
    return false;
  }
}

int DXLog::AppLog::socketIndex(int level) {
  return (level < 3) ? 0 : 1;
}

dx::JSON DXLog::AppLog::validateMsg(const string &msg, int level) {
  dx::JSON message(dx::JSON_OBJECT);
  if ((level < 0) || (level > 7)) throwString("Invalid log level: " + boost::lexical_cast<string>(level));

  message["source"] = "DX_APP";
  message["msg"] = msg;
  message["level"] = level;
  message["timestamp"] = utcMS();

  return message;
}

bool DXLog::AppLog::log(const string &msg, int level) {
  string errMsg;
  try {
    if (! initialized) {
      dx::JSON input = dx::JSON(dx::JSON_OBJECT);
      input["socketPath"] = dx::JSON(dx::JSON_ARRAY);
      input["socketPath"].push_back(defaultPrioritySocket);
      input["socketPath"].push_back(defaultBulkSocket);
      if (! initEnv(input, errMsg)) throwString(errMsg);
    }

    dx::JSON message = validateMsg(msg, level);

    int index = socketIndex(level);
    if (! boost::filesystem::exists(socketPath[index])) throwString("Socket " + socketPath[index] + " does not exist");

    if (! SendMessage2UnixDGRAMSocket(socketPath[index], message.toString(), errMsg)) throwString(errMsg);

    return true;
  } catch (const string &eMsg) {
    cerr << "Log error, " + eMsg + ", level: " << level << ", msg: " + msg << endl;
    return false;
  } catch (std::exception &e) {
    cerr << "Log error, " << e.what() << ", level: " << level << ", msg: " + msg << endl;
    return false;
  }
}

bool DXLog::AppLog::done(string &errMsg) {
  bool ret_val = SendMessage2UnixDGRAMSocket(socketPath[0], "Done", errMsg);
  if (! ret_val) errMsg = socketPath[0] + ": " + errMsg;
  if (socketPath[0].compare(socketPath[1]) == 0) return ret_val;

  string msg2;
  if (! SendMessage2UnixDGRAMSocket(socketPath[1], "Done", msg2)) {
    if (! ret_val) {
      errMsg += ", " + socketPath[1] + ": " + msg2;
    } else {
      errMsg = socketPath[1] + ": " + msg2;
      ret_val = false;
    }
  }

  return ret_val;
}

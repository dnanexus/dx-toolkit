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

bool DXLog::AppLog::log(const string &msg, int level) {
  string errMsg;
  dx::JSON message(dx::JSON_OBJECT);
  try {
    if (! initialized) {
      dx::JSON input = dx::JSON(dx::JSON_OBJECT);
      input["socketPath"] = dx::JSON(dx::JSON_ARRAY);
      input["socketPath"].push_back(defaultPrioritySocket);
      input["socketPath"].push_back(defaultBulkSocket);
      if (! initEnv(input, errMsg)) {
        cerr << errMsg << endl;
        return false;
      }
    }
    message["source"] = "DX_APP";
    message["msg"] = msg;
    message["level"] = level;
    message["timestamp"] = utcMS();

    int index = socketIndex(level);

    if (! boost::filesystem::exists(socketPath[index])) {
      cerr<< "Socket " + socketPath[index] + " does not exist!" << endl;
      return false;
    }

    if (! SendMessage2UnixDGRAMSocket(socketPath[index], message.toString(), errMsg)) {
      cerr << errMsg << endl;
      return false;
    }

    return true;
  } catch (const string &eMsg) {
    cerr << eMsg << endl;
    return false;
  } catch (std::exception &e) {
    cerr << e.what() << endl;
    return false;
  }
}

bool DXLog::AppLog::done(string &errMsg) {
  if (socketPath[0].compare(socketPath[1]) == 0) return SendMessage2UnixDGRAMSocket(socketPath[0], "Done", errMsg);
  return (SendMessage2UnixDGRAMSocket(socketPath[0], "Done", errMsg) && SendMessage2UnixDGRAMSocket(socketPath[1], "Done", errMsg));
}

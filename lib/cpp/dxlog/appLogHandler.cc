#include "appLogHandler.h"
#include "dxLog.h"

void DXLog::AppLogHandler::SendMessage() {
  string msg = boost::lexical_cast<string>(int64(data["timestamp"])) + " " + data["projectId"].get<string>() + " -- " + data["appId"].get<string>() + " -- " + data["jobId"].get<string>() + " -- " + data["userId"].get<string>() + " [msg] " + data["msg"].get<string>();
  string errMsg;
  if (! SendMessage2Rsyslog(8, data["level"], "DNAnexusAPP", msg, errMsg, msgSize))
    cerr << errMsg << endl;
  else msgCount += 1;
}

DXLog::AppLogHandler::AppLogHandler(int msgSize_, int msgLimit_) {
  msgSize = msgSize_;
  msgLimit = msgLimit_;
  bufSize = msgSize + 400;
  buffer = new char[bufSize];
  msgCount = 0;
}
      
bool DXLog::AppLogHandler::processMsg() {
  if (strcmp(buffer, "Done") == 0) return true;
//  cout << buffer << endl;
  if (msgCount < msgLimit) SendMessage();
  return false;
};

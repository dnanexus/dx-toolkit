#ifndef DXLOG_HELPER_H
#define DXLOG_HELPER_H

#include <boost/lexical_cast.hpp>
#include "dxLog.h"
#include <fstream>

namespace DXLog {
  string levelString(int level);

  void formMessageHead(int facility, int level, const string &tag, string &head);

  bool splitMessage(const string &msg, vector<string> &Msgs, int msgSize);

  bool SendMessage2Rsyslog(int facility, int level, const string &tag, const string &msg, string &errMsg, int msgSize);
};
#endif

#ifndef DXLOG_H
#define DXLOG_H

#include <dxjson/dxjson.h>
#include "unixDGRAM.h"

using namespace std;

namespace DXLog {
  enum Level { EMERG, ALERT, CRIT, ERR, WARN, NOTICE, INFO, DEBUG };

  // Read a json object from a file
  dx::JSON readJSON(const string &filename);
  
  bool ValidateLogData(const dx::JSON &config, dx::JSON &message, string &errMsg); 

  class logger {
    private:
      dx::JSON config;

    public:
      logger(const string &configF) { config = readJSON(configF); }
      bool Log(dx::JSON &msg, string &errMsg);
  };
};

#endif

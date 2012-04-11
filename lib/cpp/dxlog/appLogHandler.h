#ifndef APPLOG_HANDLER_H
#define APPLOG_HANDLER_H

#include <dxjson/dxjson.h>

namespace DXLog {
  class AppLogHandler {
    private:
      int msgCount, msgSize, msgLimit, bufSize;
      dx::JSON data;
      
      void SendMessage();

    public:
      char *buffer;

      AppLogHandler(int msgSize_ = 2000, int msgLimit_ = 1000);
      ~AppLogHandler() { delete [] buffer; }

      int bufferSize() { return bufSize; }
      
      bool processMsg();
  };
};

#endif

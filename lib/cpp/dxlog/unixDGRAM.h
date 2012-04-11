#ifndef DXLOG_UNIX_DATAGRAM_H
#define DXLOG_UNIX_DATAGRAM_H

#include "appLogHandler.h"

using namespace std;

namespace DXLog {
  // Send msg to a unix datagram. Return either true if the opreation succeeded. errMsg contains some details of error when the opration failed.
  bool SendMessage2UnixDGRAMSocket(const string &socketPath, const string &msg, string &errMsg);

  /** Create a unix datagram socket, reads msg from it and let handler process the msg
   *  The MsgHandler shall contain the following public variables and functions
   *  char *buffer;  // buffer for storing a msg
   *  int bufferSize(); // return the size of the buffer
   *  bool processMsg(); // This will be called whenever the socket receives a new piece of message. 
   *  	When it returns trues, the process will continue listen to the socket
   *  	Otherwise the socket will be closed.
   */
  bool unixDGRAMReader(AppLogHandler *handler, const string &socketPath, string &errMsg);
};

#endif

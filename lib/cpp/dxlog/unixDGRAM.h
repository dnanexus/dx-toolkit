#ifndef DXLOG_UNIX_DATAGRAM_H
#define DXLOG_UNIX_DATAGRAM_H

#include <fcntl.h>
#include <stdio.h>
#include <cstdlib>
#include <errno.h>
#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace DXLog {
  // Send msg to a unix datagram. Return either true if the opreation succeeded. errMsg contains some details of error when the opration failed.
  bool SendMessage2UnixDGRAM(const string &socketPath, const string &msg, string &errMsg);

  /** Create a unix datagram socket, reads msg from it and let handler process the msg
   *  The MsgHandler shall contain the following public variables and functions
   *  char *buffer;  // buffer for storing a msg
   *  int bufferSize(); // return the size of the buffer
   *  bool processMsg(); // This will be called whenever the socket receives a new piece of message. 
   *  	When it returns trues, the process will continue listen to the socket
   *  	Otherwise the socket will be closed.
   */
  template<class MsgHandler>
  bool unixDGRAMReader(MsgHandler *handler, const string &socketPath);
};

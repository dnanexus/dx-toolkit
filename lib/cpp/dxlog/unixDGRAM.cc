#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "unixDGRAM.h"

bool DXLog::SendMessage2UnixDGRAMSocket(const string &sockPath, const string &msg, string &errMsg) {
  int sock;
  struct sockaddr_un addr;
  bool ret_val;

  if ((sock = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0) {
    errMsg = "Socket error: " + string(strerror(errno));
    return false;
  }
   
  bzero((char *) &addr, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strcpy(addr.sun_path, sockPath.c_str());
  
  if (! (ret_val = (sendto(sock, msg.c_str(), msg.length(), 0, (sockaddr *) &addr, sizeof(sockaddr_un)) >= 0)))
    errMsg = "Error when sending log message: " + string(strerror(errno));

  close(sock);
  return ret_val;
}

bool DXLog::unixDGRAMReader(AppLogHandler *handler, const string &socketPath, string &errMsg) {
  int sock;
  struct sockaddr_un addr;

  if ((sock = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0) {
    errMsg = "Socket error: " + string(strerror(errno));
    return false;
  }

  bzero((char *) &addr, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strcpy(addr.sun_path, socketPath.c_str());

  bool ret_val = true;

  if (bind(sock, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
    errMsg = "Socket error: " + string(strerror(errno));
    ret_val = false;
  } else {
    while (true) {
      bzero(handler->buffer, handler->bufferSize());
      if (recv(sock, handler->buffer, handler->bufferSize(), 0) >= 0) {
        if (handler->processMsg()) break;
      }
    }
  }

  close(sock);
  unlink(socketPath.c_str());
  return ret_val;
}

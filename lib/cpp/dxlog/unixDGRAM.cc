#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "unixDGRAM.h"
#include <sys/stat.h>

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

DXLog::UnixDGRAMReader::UnixDGRAMReader(int bufSize_) {
  bufSize = bufSize_;
  buffer = new char[bufSize];
}

void DXLog::UnixDGRAMReader::setBufSize(int bufSize_) {
  if (bufSize_ == bufSize) return;

  delete [] buffer;
  bufSize = bufSize_;
  buffer = new char[bufSize];
}

bool DXLog::UnixDGRAMReader::run(const string &socketPath, string &errMsg) {
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
    chmod(socketPath.c_str(), 0666);
    while (true) {
      bzero(buffer, bufSize);
      if (recv(sock, buffer, bufSize, 0) >= 0) {
        if (processMsg()) break;
      }
    }
  }

  close(sock);
  unlink(socketPath.c_str());
  return ret_val;
}

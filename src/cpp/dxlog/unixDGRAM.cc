// Copyright (C) 2013 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "unixDGRAM.h"
#include <sys/stat.h>
#include <iostream>
#include <cstring>
#include <unistd.h>

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
  active = false;
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
    close(sock);
  } else {
    chmod(socketPath.c_str(), 0666);
    active = true;
    while (true) {
      bzero(buffer, bufSize);
      if (recv(sock, buffer, bufSize, 0) >= 0) {
        if (processMsg()) break;
      }
    }
    active = false;
    close(sock);
    unlink(socketPath.c_str());
  }

  return ret_val;
}

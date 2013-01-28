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

#ifndef DXLOG_UNIX_DATAGRAM_H
#define DXLOG_UNIX_DATAGRAM_H

#include <string>

using namespace std;

namespace DXLog {
  // Send msg to a unix datagram. Return either true if the opreation succeeded. errMsg contains some details of error when the opration failed.
  bool SendMessage2UnixDGRAMSocket(const string &socketPath, const string &msg, string &errMsg);

  // Create a unix datagram socket, reads msg from it and process accordingly
  class UnixDGRAMReader {
    protected:
      int bufSize;
      char *buffer;
      bool active;

      void setBufSize(int bufSize_);
      virtual bool processMsg(){ return true; };

    public:
      UnixDGRAMReader(int bufSize);
      ~UnixDGRAMReader() { delete [] buffer; }

      /** Create a unix datagram socket, reads msg from it and call processMsg()
	*  It keeps on reading msgs until processMsg() returns true;
	*/
      bool run(const string &socketPath, string &errMsg);
      bool isActive() { return active; }
  };
};

#endif

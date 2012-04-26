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

      void setBufSize(int bufSize_);
      virtual bool processMsg(){ return true; };

    public:
      UnixDGRAMReader(int bufSize);
      ~UnixDGRAMReader() { delete [] buffer; }

      /** Create a unix datagram socket, reads msg from it and call processMsg()
	*  It keeps on reading msgs until processMsg() returns true;
	*/
      bool run(const string &socketPath, string &errMsg);
  };
};

#endif

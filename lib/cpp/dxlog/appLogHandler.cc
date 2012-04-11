#include <dxjson/dxjson.h>
#include "unixDGRAM.h"
#include "dxLog.h"
#include <boost/lexical_cast.hpp>
#include <deque>
#include <omp.h>

using namespace std;

namespace DXLog {
  class AppLogHandler : public UnixDGRAMReader {
    private:
      int msgCount, msgSize, msgLimit;
      bool active;
      dx::JSON data;
      deque<string> msgQueue;
      
      void SendMessage() {
	 string msg = boost::lexical_cast<string>(int64(data["timestamp"])) + " " + data["projectId"].get<string>() + " -- " + data["appId"].get<string>() + " -- " + data["jobId"].get<string>() + " -- " + data["userId"].get<string>() + " [msg] " + data["msg"].get<string>();
	 string errMsg;
	 if (! SendMessage2Rsyslog(8, data["level"], "DNAnexusAPP", msg, errMsg, msgSize))
	   cerr << errMsg << endl;
	 else msgCount += 1;
      };

      void processQueue() {
	 while (true) {
	   if (msgQueue.size() > 0) {
	     data = dx::JSON::parse(msgQueue.front());
	     SendMessage();

	     #pragma omp critical
	     msgQueue.pop_front();
	   } else {
	     if (! active) return;
	     sleep(1);
	   }
        }
      };

      bool processMsg() {
	 if (strcmp(buffer, "Done") == 0) return true;
	 if (msgCount < msgLimit) {
          #pragma omp critical
	   msgQueue.push_back(string(buffer));
	 }
	 return false;
      };

    public:
      AppLogHandler(int msgSize_ = 2000, int msgLimit_ = 1000) : UnixDGRAMReader(msgSize + 1000), msgSize(msgSize_), msgLimit(msgLimit_), msgCount(0) {};

      bool process(const string &socketPath, string &errMsg) {
	 bool ret_val;
	 active = true;
	 msgQueue.clear();
        #pragma omp parallel sections num_threads(2) 
	 {
	   processQueue();

	   #pragma omp section
	   {
	     ret_val = run(socketPath, errMsg);
	     active = false;
	   }
	 }

	 return ret_val;
      }
  };
};

int main(int argc, char **argv) {
  DXLog::AppLogHandler a;
  string path = argv[1], errMsg;
  if (! a.process(path, errMsg)) {
    cout << errMsg << endl;
    exit(1);
  }
  exit(0);
}

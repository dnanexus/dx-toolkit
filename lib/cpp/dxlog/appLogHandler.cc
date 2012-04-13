#include <dxjson/dxjson.h>
#include "unixDGRAM.h"
#include "dxLog.h"
#include "mongoLog.h"
#include <boost/lexical_cast.hpp>
#include <deque>
#include <omp.h>

using namespace std;

namespace DXLog {
  // Ensuer log db has the following indices
  static bool ensureIndex(string &errMsg) {
    if (! MongoDriver::ensureIndex(BSON("level" << 1), "app", errMsg)) return false;
    if (! MongoDriver::ensureIndex(BSON("timestamp" << 1), "app", errMsg)) return false;
    if (! MongoDriver::ensureIndex(BSON("projectId" << 1), "app", errMsg)) return false;
    if (! MongoDriver::ensureIndex(BSON("programId" << 1), "app", errMsg)) return false;
    if (! MongoDriver::ensureIndex(BSON("jobId" << 1), "app", errMsg)) return false;
    return MongoDriver::ensureIndex(BSON("userId" << 1), "app", errMsg);
  }

  struct appLogConfig {
    int msgSize, msgLimit;
    string socketPath, projectId, jobId, userId, programId;
  };

  class AppLogHandler : public UnixDGRAMReader {
    private:
      appLogConfig conf;
      int msgCount;
      bool active;
      deque<string> msgQueue[2];
      char timeString[80];
      
      string timeISOString(int64 utc) {
	 time_t t = utc;
	 struct tm *ptm = gmtime( &t);
	 strftime(timeString, 80, "%Y-%m-%dT%H:%M:%SZ", ptm);
        return string(timeString);
      }

      bool SendMessage(const dx::JSON &data, string &errMsg, int index) {
	 if (index == 0) {
 	   string msg = "[" + timeISOString(int64(data["timestamp"])/1000) + "] " + conf.projectId + " -- " + conf.programId + " -- " + conf.jobId + " -- " + conf.userId + " [msg] " + data["msg"].get<string>();
	   return SendMessage2Rsyslog(8, int(data["level"]), "DNAnexusAPP", msg, errMsg, msg.size() + 1);
	 } else {
  	   BSONObj entry = BSON("level" << levelString(int(data["level"])) << "timestamp" << int64(data["timestamp"]) << "projectId" << conf.projectId << "programId" << conf.programId << "jobId" << conf.jobId << "userId" << conf.userId << "msg" << data["msg"].get<string>());
          return MongoDriver::insert(entry, "app", errMsg);
	 }
      };

      void processQueue(int index) {
        string errMsg;
	 while (true) {
	   if (msgQueue[index].size() > 0) {
            #pragma omp critical
	     cout << conf.socketPath << " Queue " << index << ": " << msgQueue[index].front() << endl;

	     try {
		dx::JSON data = dx::JSON::parse(msgQueue[index].front());

	       if (data["msg"].get<string>().size() > conf.msgSize) { data["msg"] = data["msg"].get<string>().substr(0, conf.msgSize); }

		for (int i = 0; i < 10; i++) {
		  if (SendMessage(data, errMsg, index)) {
		    #pragma omp critical
		    cout << conf.socketPath << " Queue " << index << ": Message sent" << endl;
		    break;
		  } else {
		    #pragma omp critical
		    cout << conf.socketPath << " Queue " << index << ": " << errMsg << endl;
		    sleep(5);
		  } 
		}
	     } catch (char *e) {
		cout << conf.socketPath << " Queue " << index << " Error: " << e << endl;
	     }
	
            #pragma omp critical
	     msgQueue[index].pop_front();
	   } else {
	     if (! active) return;
	     sleep(1);
	   }
        }
      };

      bool processMsg() {
	 if (strcmp(buffer, "Done") == 0) return true;
	 if (msgCount < conf.msgLimit) {
          #pragma omp critical
	   {
  	     msgQueue[0].push_back(string(buffer));
  	     msgQueue[1].push_back(string(buffer));
	   }
	 }
	 return false;
      };

    public:
      AppLogHandler(appLogConfig &conf_) : UnixDGRAMReader(conf_.msgSize + 1000), conf(conf_), msgCount(0) {};

      void process() {
	 string errMsg = "";
	 bool state;
	 active = true;
	 msgQueue[0].clear();
	 msgQueue[1].clear();
        #pragma omp parallel sections
	 {
	   state = run(conf.socketPath, errMsg);
          active = false;

	   #pragma omp section
	   processQueue(0);

	   #pragma omp section
          processQueue(1);
	 }

	 if (! state) throw(errMsg);
      };
  };
};

int main(int argc, char **argv) {
  if (argc < 2) {
    cerr << "Usage: appLogHandler configFile\n";
    exit(1);
  }

  try {
    dx::JSON conf = DXLog::readJSON(argv[1]);
    DXLog::appLogConfig appConf;

    appConf.msgSize = (conf.has("maxMsgSize")) ? int(conf["maxMsgSize"]) : 2000;
    appConf.msgLimit = (conf.has("maxMsgNumber")) ? int(conf["maxMsgNumber"]) : 1000;
 
    if (! conf.has("projectId")) throw ("projectId is not specified");
    appConf.projectId = conf["projectId"].get<string>();

    if (! conf.has("jobId")) throw ("jobId is not specified");
    appConf.jobId = conf["jobId"].get<string>();

    if (! conf.has("userId")) throw ("userId is not specified");
    appConf.userId = conf["userId"].get<string>();

    if (! conf.has("programId")) throw ("programId is not specified");
    appConf.programId = conf["programId"].get<string>();

    if (! conf.has("socketPath")) throw ("socketPath is not specified");

    if (conf.has("mongoServer")) DXLog::MongoDriver::setServer(conf["mongoServer"].get<string>());
    if (conf.has("database")) DXLog::MongoDriver::setDB(conf["database"].get<string>());
    string errMsg;
    if (! DXLog::ensureIndex(errMsg)) throw("errMsg");

    #pragma omp parallel for
    for (int i = 0; i < conf["socketPath"].size(); i++) {
      DXLog::appLogConfig tConf = appConf;
      tConf.socketPath = conf["socketPath"][i].get<string>();
      DXLog::AppLogHandler a(tConf);
      a.process();
    }
   
  } catch (char *e) {
    cout << e << endl;
    exit(1);
  }
  exit(0);
}

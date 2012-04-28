#include "dxLog.h"
#include "dxLog_helper.h"
#include "mongoLog.h"

using namespace std;

namespace DXLog {
  class MongoDbLog : public UnixDGRAMReader {
    private:
      dx::JSON schema;
      deque<string> que;
      int maxQueueSize;
      string socketPath, messagePath, hostname;
      bool active;

      // ensure log mongodb indexes based on log schema
      bool ensureIndex(string &errMsg) {
        for (dx::JSON::object_iterator it = schema.object_begin(); it != schema.object_end(); ++it) {
	   string key = it->first;
	   dx::JSON index = it->second["mongodb"]["indexes"];

	   for (int i = 0; i < index.size(); i++) {
	     BSONObjBuilder b;
	     for(dx::JSON::object_iterator it2 = index[i].object_begin(); it2 != index[i].object_end(); ++it2)
		b.append(it2->first, int(it2->second));

            if (! MongoDriver::ensureIndex(b.obj(), key, errMsg)) return false;
	   }
	 }
	 return true;
      }

      // write own log message to rsyslog
      void rsysLog(int level, const string &msg) {
	 string eMsg;
        #pragma omp critical
	 SendMessage2Rsyslog(8, level, "DNAnexusLog", msg, msg.size() + 1, eMsg);
      }

      // send message to mongodb
      bool sendMessage(dx::JSON &data, string &errMsg) {
	 if (! ValidateLogData(schema, data, errMsg)) return false;
	 if (! data.has("hostname")) data["hostname"] = hostname;

	 dx::JSON columns = schema[data["source"].get<string>()]["mongodb"]["columns"];
	 BSONObjBuilder b;

	 for(dx::JSON::object_iterator it = columns.object_begin(); it != columns.object_end(); ++it) {
	   string key = it->first, typ = it->second.get<string>();
	   if (! data.has(key)) continue;

	   if (typ.compare("string") == 0) {
	     b.append(key, data[key].get<string>());
	   } else if (typ.compare("int") == 0) {
	     b.append(key, int(data[key]));
	   } else if (typ.compare("int64") == 0) {
	     b.append(key, int64(data[key]));
	   } else if (typ.compare("boolean") == 0) {
	     b.append(key, bool(data[key]));
	   } else if (typ.compare("double") == 0) {
	     b.append(key, double(data[key]));
	   }
	 }
        
	 return MongoDriver::insert(b.obj(), data["source"].get<string>(), errMsg);
      };

      void processQueue() {
        string errMsg;
	 while (true) {
	   if (que.size() > 0) {
	     bool succeed = false;
	     try {
		dx::JSON data = dx::JSON::parse(que.front());
		for (int i = 0; i < 10; i++) {
		  if ((succeed = sendMessage(data, errMsg))) break;
	
		  if (i == 0) rsysLog(3, errMsg + " Msg: " + que.front());
		  sleep(5);
		}
	     } catch (std::exception &e) {
		rsysLog(3, string(e.what()) + " Msg: " + que.front());
	     }
	
 	     if (! succeed){
              #pragma omp critical
		StoreMsgLocal(messagePath, que.front());
	     }
	       
            #pragma omp critical
	     que.pop_front();
	   } else {
	     if (! active) return;
	     sleep(1);
	   }
        }
      };

      bool processMsg() {
        {
	   if (que.size() < maxQueueSize) {
	     que.push_back(string(buffer));
	   } else {
            #pragma omp critical
	     StoreMsgLocal(messagePath, string(buffer));
	     rsysLog(3, "Msg Queue Full, drop message " + string(buffer));
	   }
	 }

	 return false;
      };

    public:
      MongoDbLog(const dx::JSON &conf) : UnixDGRAMReader(1000 + int(conf["maxMsgSize"])) {
	 schema = readJSON(conf["schema"].get<string>());
	 ValidateLogSchema(schema);

	 socketPath = conf["socketPath"].get<string>();
	 maxQueueSize = (conf.has("maxQueueSize")) ? int(conf["maxQueueSize"]): 10000;
	 messagePath = (conf.has("messagePath")) ? conf["messagePath"].get<string>() : "/var/log/dnanexusLocal/DB";

	 if (conf.has("mongoServer")) DXLog::MongoDriver::setServer(conf["mongoServer"].get<string>());
	 if (conf.has("database")) DXLog::MongoDriver::setDB(conf["database"].get<string>());

	 hostname = getHostname();
      };

      void process() {
	 que.clear();
	 
	 active = true;
	 string errMsg;

	 if (! ensureIndex(errMsg)) {
	   rsysLog(3, errMsg);
	   cerr << errMsg << endl;
	 //  return;
	 }

	 getHostname();

        #pragma omp parallel sections
	 {
	   string errMsg;
	   unlink(socketPath.c_str());
	   run(socketPath, errMsg);
          rsysLog(3, errMsg);
	   cerr << errMsg << endl;
	   active = false;

	   #pragma omp section
	   processQueue();
	 }
      };
  };
};

int main(int argc, char **argv) {
  if (argc < 2) {
    cerr << "Usage: dxDbLog configFile\n";
    exit(1);
  }

  try {
    dx::JSON conf = DXLog::readJSON(argv[1]);
    if (! conf.has("maxMsgSize")) conf["maxMsgSize"] = 2000;
    if (! conf.has("schema")) DXLog::throwString("log schema is not specified");
    if (! conf.has("socketPath")) DXLog::throwString("socketPath is not specified");

    DXLog::MongoDbLog a(conf);
    a.process();   
  } catch (const string &msg) {
    cout << msg << endl;
    exit(1);
  } catch (std::exception &e) {
    cout << string("JSONException: ") + e.what() << endl;
    exit(1);
  }
  exit(0);
}

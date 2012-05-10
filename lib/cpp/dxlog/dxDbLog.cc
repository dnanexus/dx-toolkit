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
            b.append(key, (long long int)data[key]);
          } else if (typ.compare("boolean") == 0) {
            b.append(key, bool(data[key]));
          } else if (typ.compare("double") == 0) {
            b.append(key, double(data[key]));
          }
        }
        
        b.append("_shardKey", randomString(24));
        return MongoDriver::insert(b.obj(), schema[data["source"].get<string>()]["mongodb"]["collection"].get<string>(), errMsg);
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
        if (que.size() < maxQueueSize) {
          que.push_back(string(buffer));
        } else {
          #pragma omp critical
          StoreMsgLocal(messagePath, string(buffer));
          rsysLog(3, "Msg Queue Full, drop message " + string(buffer));
        }
        return false;
      };

    public:
      MongoDbLog(const dx::JSON &conf) : UnixDGRAMReader(1000 + int(conf["maxMsgSize"])) {
        dx::JSON dConf = defaultConf();
        schema = defaultSchema();
        ValidateLogSchema(schema);
        
        socketPath = conf["socketPath"].get<string>();
        maxQueueSize = (conf.has("maxQueueSize")) ? int(conf["maxQueueSize"]): 10000;
        messagePath = dConf["logserver"]["logDir"].get<string>() + "/local/DB";

        DXLog::MongoDriver::setServer(dConf["logmongod"]["host"].get<string>() + ":" + dConf["logmongod"]["port"].get<string>());
        DXLog::MongoDriver::setDB(dConf["logmongod"]["name"].get<string>());
        
        hostname = getHostname();
      };

      bool process(string &errMsg) {
        bool ret_val;
        que.clear();
        
        active = true;
        getHostname();

        #pragma omp parallel sections
        {
          unlink(socketPath.c_str());
          ret_val = run(socketPath, errMsg);
          rsysLog(3, errMsg);
          active = false;
          
          #pragma omp section
          processQueue();
        }
        
        return ret_val;
      };
  };
};

int main(int argc, char **argv) {
  if (argc < 2) {
    cerr << "Usage: dxDbLog configFile\n";
    exit(1);
  }

  try {
    string errMsg;
    dx::JSON conf = DXLog::readJSON(argv[1]);
    if (! conf.has("maxMsgSize")) conf["maxMsgSize"] = 2000;
    if (! conf.has("socketPath")) DXLog::throwString("socketPath is not specified");

    DXLog::MongoDbLog a(conf);
    cout << "listen to socket " + conf["socketPath"].get<string>() << endl;
    if (! a.process(errMsg)) {
      cerr << errMsg << endl;
      exit(1);
    }
  } catch (const string &msg) {
    cerr << msg << endl;
    exit(1);
  } catch (std::exception &e) {
    cerr << string("JSONException: ") + e.what() << endl;
    exit(1);
  }
  exit(0);
}

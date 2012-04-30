#include "dxLog.h"
#include "dxLog_helper.h"

using namespace std;

namespace DXLog {
  class AppLogHandler : public UnixDGRAMReader {
    private:
      bool active;
      int msgCount, msgLimit;
      string socketPath, projectId, jobId, userId, programId;
      logger *a;

      void validateInput(const dx::JSON &input) {
        msgLimit = (input.has("maxMsgNumber")) ? int(input["maxMsgNumber"]) : 1000;
	 
	 if (! input.has("projectId")) throwString("projectId is not specified");
	 projectId = input["projectId"].get<string>();
	 
	 if (! input.has("jobId")) throwString("jobId is not specified");
	 jobId = input["jobId"].get<string>();
	 
	 if (! input.has("userId")) throwString("userId is not specified");
	 userId = input["userId"].get<string>();
	 
	 if (! input.has("programId")) throwString("programId is not specified");
	 programId = input["programId"].get<string>();
	 
	 if (! input.has("schema")) throwString("Log schema is not specified");

        dx::JSON schema = readJSON(input["schema"].get<string>());
	 ValidateLogSchema(schema);
	 a = new logger(schema);
	 active = true;
      }

      bool processMsg() {
	 string errMsg;
	 if (! active) return true;
	 if (strcmp(buffer, "Test") == 0) return false;
	 if (strcmp(buffer, "Done") == 0) return true;

	 if (msgCount < msgLimit) {
	   msgCount ++;
	   dx::JSON data = dx::JSON::parse(string(buffer));

	   data["projectId"] = projectId; data["jobId"] = jobId;
	   data["programId"] = programId; data["userId"] = userId;
	   data["dbStore"] = true;

	   if (! a->Log(data, errMsg)) cerr << errMsg << endl;
	   return false;
	 } else return true;
      };

    public:
      AppLogHandler(dx::JSON &input, const string &socketPath_, int msgSize) : UnixDGRAMReader(msgSize + 1000), msgCount(0), socketPath(socketPath_) {
        active = false;
	 validateInput(input);
      };

      ~AppLogHandler() { if (a != NULL) delete a; }

      bool process(string &errMsg) {
	 if (! active) return true;
	 //unlink(socketPath.c_str());
	 return run(socketPath, errMsg);
      }

      void stopProcess() {
	 string errMsg;
	 active = false;
	 SendMessage2UnixDGRAMSocket(socketPath, "Done", errMsg);
      }
  };
};

int main(int argc, char **argv) {
  if (argc < 2) {
    cerr << "Usage: appLogHandler configFile\n";
    exit(1);
  }

  int i, j, k = 0;

  try {
    dx::JSON conf = DXLog::readJSON(argv[1]);
    int msgSize = (conf.has("maxMsgSize")) ? int(conf["maxMsgSize"]) : 2000;

    if (! conf.has("socketPath")) DXLog::throwString("socketPath is not specified");
    if (conf["socketPath"].size() == 0) DXLog::throwString("socketPath is empty");

    DXLog::AppLogHandler **h = new DXLog::AppLogHandler*[conf["socketPath"].size()];
    for (i = 0; i < conf["socketPath"].size(); i++)
      h[i] = new DXLog::AppLogHandler(conf, conf["socketPath"][i].get<string>(), msgSize);

    #pragma omp parallel for
    for (i = 0; i < conf["socketPath"].size(); i++) {
      string errMsg; 
      if (! h[i]->process(errMsg)) {
        k = 1;
        #pragma omp critical
        cerr << errMsg << endl;
        for (j = 0; j < conf["socketPath"].size(); j++) {
          #pragma omp critical
	   h[j]->stopProcess();
        }
      }
    }
  } catch (const string &err) {
    cerr << err << endl;
    exit(1);
  } catch (std::exception &e) {
    cerr << e.what() << endl;
    exit(1);
  }

  exit(k);
}

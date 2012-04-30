#include <dxjson/dxjson.h>
#include "unixDGRAM.h"
#include "dxLog.h"
#include "dxLog_helper.h"
#include "mongoLog.h"
#include <boost/lexical_cast.hpp>
#include <deque>
#include <omp.h>

using namespace std;

namespace DXLog {
  class AppLogHandler : public UnixDGRAMReader {
    private:
      logger *a;
      int msgCount, msgLimit;
      string socketPath, projectId, jobId, userId, programId, errMsg;

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
      }

      bool processMsg() {
	 if (strcmp(buffer, "Done") == 0) return true;

	 if (msgCount < msgLimit) {
	   msgCount ++;
	   dx::JSON data = dx::JSON::parse(string(buffer));

	   data["projectId"] = projectId; data["jobId"] = jobId;
	   data["programId"] = programId; data["userId"] = userId;
	   data["dbStore"] = true;

	   a->Log(data, errMsg);
	   return false;
	 } else return true;
      };

    public:
      AppLogHandler(dx::JSON &input, const string &socketPath_, int msgSize) : UnixDGRAMReader(msgSize + 1000), socketPath(socketPath_), msgCount(0) {
        validateInput(input);
      };

      ~AppLogHandler() { if(a != NULL) delete a; }

      void process() {
	 if (! run(socketPath, errMsg)) throwString(errMsg);
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
    int msgSize = (conf.has("maxMsgSize")) ? int(conf["maxMsgSize"]) : 2000;

    if (! conf.has("socketPath")) DXLog::throwString("socketPath is not specified");

    #pragma omp parallel for
    for (int i = 0; i < conf["socketPath"].size(); i++) {
      DXLog::AppLogHandler a(conf, conf["socketPath"][i].get<string>(), msgSize);
      a.process();
    }
  } catch (const string e) {
    cerr << e << endl;
    exit(1);
  } catch (std::exception &e) {
    cerr << string("JSONException: ") + e.what() << endl;
  }
  exit(0);
}

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
      bool active;
      int msgCount, msgLimit;
      string socketPath, projectId, jobId, userId, programId, errMsg;
      logger *a;

      void validateInput(const dx::JSON &input) {
        msgLimit = (input.has("maxMsgNumber")) ? int(input["maxMsgNumber"]) : 1000;
	 
	 if (! input.has("projectId")) {
          #pragma omp critical
	   cerr << "projectId is not specified" << endl;
	 }
	 projectId = input["projectId"].get<string>();
	 
	 if (! input.has("jobId")) {
          #pragma omp critical
	   cerr << "jobId is not specified" << endl;
	 }
	 jobId = input["jobId"].get<string>();
	 
	 if (! input.has("userId")) {
          #pragma omp critical
	   cerr << "userId is not specified" << endl;
	 }
	 userId = input["userId"].get<string>();
	 
	 if (! input.has("programId")) {
          #pragma omp critical
	   cerr << "programId is not specified" << endl;
	 }
	 programId = input["programId"].get<string>();
	 
	 if (! input.has("schema")) {
          #pragma omp critical
	   cerr << "Log schema is not specified" << endl;
	 }

        dx::JSON schema = readJSON(input["schema"].get<string>());
	 ValidateLogSchema(schema);
	 a = new logger(schema);
      }

      bool processMsg() {
	 if (! active) return true;
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
      AppLogHandler(dx::JSON &input, const string &socketPath_, int msgSize) : UnixDGRAMReader(msgSize + 1000), msgCount(0), socketPath(socketPath_) {
        validateInput(input);
      };

      ~AppLogHandler() { if (a != NULL) delete a; }

      void process() {
	 active = true;
	 unlink(socketPath.c_str());
	 if (! run(socketPath, errMsg)) {
	   cerr << errMsg << endl;
	   active = false;
	 }
      }
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

    if (! conf.has("socketPath")) cerr << "socketPath is not specified" << endl;

    #pragma omp parallel for
    for (int i = 0; i < conf["socketPath"].size(); i++) {
      DXLog::AppLogHandler a(conf, conf["socketPath"][i].get<string>(), msgSize);
      a.process();
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

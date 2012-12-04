#include <fstream>

#include "exec_utils.h"
#include "utils.h"

using namespace std;
using namespace dx;

void loadInput(JSON &input) {
  ifstream ifs(joinPath(getUserHomeDirectory(), "job_input.json"));
  input.read(ifs);
}

void writeOutput(const JSON &output) {
  ofstream ofs(joinPath(getUserHomeDirectory(), "job_output.json"));
  ofs << output.toString() << endl;
  ofs.close();
}

void reportError(const string &message, const bool internal) {
  ofstream ofs(joinPath(getUserHomeDirectory(), "job_error.json"));
  JSON error_json = JSON(JSON_HASH);
  error_json["error"] = JSON(JSON_HASH);
  error_json["error"]["type"] = internal ? "AppInternalError" : "AppError";
  error_json["error"]["message"] = message;
  ofs << error_json.toString() << endl;
  ofs.close();
  exit(1);
}

#include "dxjob.h"

using namespace std;
using namespace dx;

JSON DXJob::describe() const {
  return jobDescribe(dxid_);
}

void DXJob::create(const JSON &fn_input, const string &fn_name) {
  JSON input_params(JSON_OBJECT);
  input_params["input"] = fn_input;
  input_params["function"] = fn_name;
  const JSON resp = jobNew(input_params);
  setID(resp["id"].get<string>());
}

void DXJob::terminate() const {
  jobTerminate(dxid_);
}

void DXJob::waitOnDone(const int timeout) const {
  int elapsed = 0;
  string cur_state;
  do {
    cur_state = describe()["state"].get<string>();
    if (cur_state == "done")
      return;
    sleep(2);
    elapsed += 2;
  } while (elapsed <= timeout);
}

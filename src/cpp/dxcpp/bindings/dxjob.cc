#include "dxjob.h"

using namespace std;
using namespace dx;

JSON DXJob::describe() const {
  if (dxid_ == "")
    throw DXError("Uninitialized job handler: No dxid_ set");

  return jobDescribe(dxid_);
}

void DXJob::create(const JSON &fn_input, const string &fn_name, const string &job_name, const JSON resources) {
  JSON input_params(JSON_OBJECT);
  input_params["input"] = fn_input;
  input_params["function"] = fn_name;
  if (job_name.length() > 0)
    input_params["name"] = job_name;
  if (resources.type() == JSON_NULL)
    input_params["resources"] = resources;
  const JSON resp = jobNew(input_params);
  setID(resp["id"].get<string>());
}

void DXJob::terminate() const {
  if (dxid_ == "")
    throw DXError("Uninitialized job handler: No dxid_ set");
  jobTerminate(dxid_);
}

void DXJob::waitOnDone(const int timeout) const {
  int elapsed = 0;
  do {
    if (getState() == "done")
      return;
    sleep(2);
    elapsed += 2;
  } while (elapsed <= timeout);
}

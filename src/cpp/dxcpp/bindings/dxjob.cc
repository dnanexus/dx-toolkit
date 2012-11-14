#include "dxjob.h"
#include "execution_common_helper.h"

using namespace std;
using namespace dx;

JSON DXJob::describe() const {
  if (dxid_ == "")
    throw DXError("Uninitialized job handler: No dxid_ set");

  return jobDescribe(dxid_);
}

void DXJob::create(const JSON &fn_input, const string &fn_name, const string &job_name, const vector<string> &depends_on, const JSON &instance_type) {
  JSON input_params(JSON_OBJECT);
  input_params["input"] = fn_input;
  input_params["function"] = fn_name;
  if (job_name.length() > 0)
    input_params["name"] = job_name;

  appendDependsOnAndInstanceType(input_params, depends_on, fn_name, instance_type);    
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

JSON DXJob::getOutputRef(const string &field) {
  JSON jbor = JSON(JSON_HASH);
  jbor["job"] = dxid_;
  jbor["field"] = field;
  return jbor;
}

DXJob DXJob::newDXJob(const JSON &fn_input,
                      const string &fn_name,
                      const string &job_name,
                      const vector<string> &depends_on,
                      const JSON &instance_type
                      ) {
  DXJob dxjob;
  dxjob.create(fn_input, fn_name, job_name, depends_on, instance_type);
  return dxjob;
}

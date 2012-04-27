#include "dxprogram.h"

using namespace std;
using namespace dx;

void DXProgram::createFromFile(const string &codefile) const {
}

void DXProgram::createFromString(const string &codestring) const {
}

DXJob DXProgram::run(const JSON &program_input,
                     const string &project_context,
                     const string &output_folder) const {
  JSON input_params(JSON_OBJECT);
  input_params["input"] = program_input;
  if (g_JOB_ID == "")
    input_params["project"] = project_context;
  input_params["folder"] = output_folder;
  const JSON resp = programRun(dxid_, input_params);
  return DXJob(resp["id"].get<string>());
}

DXProgram DXProgram::clone(const string &dest_proj_id,
                         const string &dest_folder) const {
  clone_(dest_proj_id, dest_folder);
  return DXProgram(dxid_, dest_proj_id);
}

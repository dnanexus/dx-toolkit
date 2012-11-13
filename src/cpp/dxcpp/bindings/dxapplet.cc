#include "dxapplet.h"
#include "execution_common_helper.h"

using namespace std;
using namespace dx;

void DXApplet::create(JSON inp) {
  if (!inp.has("project")) 
    inp["project"] = g_WORKSPACE_ID;
  setIDs(appletNew(inp)["id"].get<string>(), inp["project"].get<string>());
}

DXJob DXApplet::run(const JSON &applet_input,
                     const string &output_folder,
                     const vector<string> &depends_on,
                     const dx::JSON &instance_type,
                     const string &project_context
                     ) const {
  JSON input_params(JSON_OBJECT);
  input_params["input"] = applet_input;
  if (g_JOB_ID == "")
    input_params["project"] = project_context;
  input_params["folder"] = output_folder;
  appendDependsOnAndInstanceType(input_params, depends_on, "main", instance_type);   
  const JSON resp = appletRun(dxid_, input_params);
  return DXJob(resp["id"].get<string>());
}

DXApplet DXApplet::clone(const string &dest_proj_id,
                         const string &dest_folder) const {
  clone_(dest_proj_id, dest_folder);
  return DXApplet(dxid_, dest_proj_id);
}

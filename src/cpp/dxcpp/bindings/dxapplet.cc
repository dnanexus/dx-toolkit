#include "dxapplet.h"

using namespace std;
using namespace dx;

DXJob DXApplet::run(const JSON &applet_input,
                     const string &project_context,
                     const string &output_folder) const {
  JSON input_params(JSON_OBJECT);
  input_params["input"] = applet_input;
  if (g_JOB_ID == "")
    input_params["project"] = project_context;
  input_params["folder"] = output_folder;
  const JSON resp = appletRun(dxid_, input_params);
  return DXJob(resp["id"].get<string>());
}

DXApplet DXApplet::clone(const string &dest_proj_id,
                         const string &dest_folder) const {
  clone_(dest_proj_id, dest_folder);
  return DXApplet(dxid_, dest_proj_id);
}

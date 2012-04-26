#include "dxLog.h"
#include "dxLog_helper.h"

using namespace DXLog;
int main(int argc, char **argv) {
  if (argc < 2) {
    cerr << "Usage: verify_logschema schemaFile\n";
    exit(1);
  }

  try {
    dx::JSON schema = readJSON(argv[1]);
    ValidateLogSchema(schema);
    cout << "Verify succeeded" << endl;
  } catch (const string &msg) {
    cout << msg << endl;
  } catch (std::exception &e) {
    cout << string("JSON parse error: ") + e.what() << endl;
  }

  exit(0);
}

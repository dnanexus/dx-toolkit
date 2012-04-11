#include "appLogHandler.h"
//#include <dxjson/dxjson.h>
#include <iostream>
#include "unixDGRAM.h"
//#include "dxLog.h"

using namespace std;
using namespace DXLog;

int main(int argc, char **argv) {
  AppLogHandler *a = new AppLogHandler();
  string path = "log3", errMsg;
  if (! unixDGRAMReader(a, path, errMsg)) {
    cout << errMsg << endl;
    exit(1);
  }
  exit(0);
}

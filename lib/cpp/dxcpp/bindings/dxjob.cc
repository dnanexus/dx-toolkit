#include "dxjob.h"

using namespace std;
using namespace dx;

void DXJob::create(const JSON &fn_input, const string &fn_name) {
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

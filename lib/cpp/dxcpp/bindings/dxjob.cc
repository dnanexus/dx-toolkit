#include "dxjob.h"

using namespace std;
using namespace dx;

void DXJob::create(const JSON &fn_input, const string &fn_name) {
}

void DXJob::waitOnDone() const {
  waitOnState("done");
}

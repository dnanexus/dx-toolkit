#include "dxjob.h"

void DXJob::create(const JSON &fn_input, const string &fn_name) {
}

void DXJob::waitOnDone() const {
  waitOnState("done");
}

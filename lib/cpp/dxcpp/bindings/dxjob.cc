#include "dxjob.h"

void DXJob::create(const JSON &fn_input, const string &fn_name) {
}

void DXJob::wait_on_done() const {
  this->wait_on_state("done");
}

#include <unistd.h>
#include "bindings.h"

using namespace std;
using namespace dx;

JSON search() {
  return dx::JSON();
}

void DXClass::waitOnState(const string &state,
			  const int timeout) const {
  int elapsed = 0;
  string cur_state;
  do {
    cur_state = describe()["state"].get<string>();
    if (cur_state == state)
      return;
    sleep(2);
    elapsed += 2;
  } while (elapsed <= timeout);
}

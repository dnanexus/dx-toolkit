#ifndef DXCPP_JSON_H
#define DXCPP_JSON_H

/**
 * [Incomplete] placeholder header file
 **/

#include <string>
using namespace std;

class JSON {
 public:
  JSON() {}
  JSON(const string &to_json) {}
  string to_string() const { return "temporary placeholder"; }
  bool operator==(const JSON &rhs) const { return true; }
  string operator[](const string &key) const { return ""; }
};

#endif

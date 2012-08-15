#include "log.h"

#include <iostream>

Log::Log() {
}

Log::~Log() {
  if (enabled) {
    std::cerr << oss.str();
  }
}

std::ostringstream& Log::get() {
  return oss;
}

bool Log::enabled = false;

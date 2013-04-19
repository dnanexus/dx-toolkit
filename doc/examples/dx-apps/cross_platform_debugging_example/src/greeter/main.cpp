#include "greeter.hpp"

int main(int argc, char* argv[]) {
  if (argc >= 2) {
    greet(argv[1]);
  }
}

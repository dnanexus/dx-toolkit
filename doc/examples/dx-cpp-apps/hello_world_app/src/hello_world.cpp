#include <fstream>
#include <string>
#include "dxjson/dxjson.h"

int main() {
  using namespace std;
  using namespace dx;
  
  // Read app input from file: job_input.json
  JSON input, output;
  ifstream ifs("job_input.json");
  input.read(ifs);

  // Create a file named "job_output.json" and write the output
  ofstream ofs("job_output.json");
  ofs << "{\"greeting\": \"Hello, "<< ((input.has("name")) ? input["name"].get<string>() : "World")<<"!\"}";
  ofs.close();

  return 0;
}

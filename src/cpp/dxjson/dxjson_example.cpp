// This is an exemplary usage of dxjson library
// To compile the program:
//   1) Build the library and copy header files into standard path
//      by running the script file ./make_standard_lib.sh (required once only)
//
//   2) Compile like this: g++ -Wall json_example.cpp -ldxjson -o ./json_example.o

#include <iostream>
#include <dxjson/dxjson.h>
#include <cassert>

using namespace std;
using namespace dx;

void testCreation() {
  // In this function we will create the following JSON in two different ways
  /*
  {
    "arr": [
        1,
        2,
        3
    ],
    "str": "hello world",
    "int": 10,
    "real": 10.01,
    "bool": true,
    "null": null
  }
  */

  // Method 1: Read the JSON from it's stringified represntation
  JSON j1 = JSON::parse("{\"arr\": [1,2,3], \"str\": \"hello world\", \"int\": 10, \"real\": 10.01, \"bool\": true, \"null\": null}");

  // Method 2: Create a blank JSON_OBJECT and add required values manually
  JSON j2(JSON_OBJECT); // JSON_OBJECT, tells the compiler to make a JSON hash
  j2["arr"] = JSON(JSON_ARRAY); // JSON_ARRAY tells the compiler to make a JSON array
  j2["arr"].push_back(1); // 1 is automatically detected as a JSON_INTEGER
  j2["arr"].push_back(2);
  j2["arr"].push_back(3);

  j2["str"] = "hello world"; // j2["str"] is automatically detcted as a JSON_STRING
  j2["int"] = 10; // Integer
  j2["real"] = 10.01; // Automatically detected as JSON_REAL
  j2["bool"] = true; // Automatically detected as JSON_BOOLEAN
  j2["null"] = JSON(JSON_NULL); // Create a JSON_NULL explicitly

  // NOTE: Equality is checked "deeply" (recursively).
  // Also JSON_INTEGER is always != JSON_REAL
  // So JSON(10.0) != JSON(10);
  assert(j1 == j2);
}

void accessValuesAndStringify() {
  // Create following JSON: {"n1": 10, "n2": 20, "n1 + n2": 30}
  JSON j1 = JSON::parse("{\"n1\": 10, \"n2\": 20}");
  j1["n1 + n2"] = j1["n1"].get<int>() + j1["n2"].get<int>(); // One way to cast into integer
  
  // casting json into integer using explicit cast
  int x = int(j1["n1"]) + int(j1["n2"]);
  assert(x == int(j1["n1 + n2"]));

  // Print the serialized json to  stdout
  std::cout<<j1.toString()<<endl;
}

void iterateArraysAndObjects() {
  // Creates a json array of 10 values (each being 0)
  // Assigning a vector, automatically makes it recognize it as JSON_ARRAY
  JSON j1 = vector<int>(10, 0);

  // Length of JSON array can be found out using size() method
  assert(j1.length() == 10u);
  for (unsigned i = 0; i < j1.size(); i++) {
    assert(j1[i] == 0);
    j1[i] = i; // Update value in j[i]
  }

  // Erase 2nd element of array
  j1.erase(1);
  assert(j1.length() == 9);
  assert(j1[1] == 2);

  // To iterate over objects you can use object_iterator
  // which is actually map<sting, JSON>::iterator
  // No ordering of keys is guaranteed (though currently it will be sorted by key names
  // since we internally use map to store it, but it can change later to hash_map)

  JSON j2(JSON_OBJECT);
  j2["a"] = 97;
  j2["b"] = 98;
  j2["c"] = 99;
  for(JSON::object_iterator it = j2.object_begin(); it != j2.object_end(); ++it) {
    // Key = it->first, value = it->second
    assert(int(it->first[0]) == it->second.get<int>());
  }

  // To check if a particular key is present in a JSON object use has()
  assert(j2.has("a"));
  assert(!j2.has("d"));

  // Similarly iterators for arrays are also available. See gtest_json.cpp for more such examples
}

int main() {
  testCreation();
  accessValuesAndStringify();
  iterateArraysAndObjects();
  std::cout<<"All assertions passed! Yay!";

  return 0;
}

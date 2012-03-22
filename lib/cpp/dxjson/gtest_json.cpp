#include <gtest/gtest.h>
#include <iostream>
#include "dxjson.h"

using namespace std;
using namespace dx;

bool exceptionflag;

// A macro which asserts that a JSONException is thrown by provided statement
#define ASSERT_JSONEXCEPTION(a) \
  exceptionflag = false; \
  try { a; ASSERT_TRUE(false); } catch(JSONException &e) { exceptionflag =  true; } \
  ASSERT_TRUE(exceptionflag);

// This function is useful for debuggin purposes (utf-8 cases)
void printStringAsIntegers(string str) {
  std::cout<<"\nString = "<<str<<"\nInteger version = ";
  for(int i=0;i<str.length();i++) {
    cout<<int(str[i])<<" ";
  }
  cout<<"\n";
}

TEST(JSONTest, CreationIndexingAndConstness) {

  JSON j1 = JSON::parse("{\"x\": 1, \"hello\": \"world\"}");
  ASSERT_EQ("1", j1["x"].toString());
  ASSERT_EQ("\"world\"", j1["hello"].toString());

  JSON j2(JSON_OBJECT);
  j2["k1"] = "blah";
  j2["k2"] = "foo";
  j2["k3"] = "k1";
  j2[j2["k3"]] = "blah-changed";

  ASSERT_EQ("\"blah-changed\"", j2["k1"].toString());
  ASSERT_EQ(j2, JSON::parse("{\"k1\": \"blah-changed\", \"k2\": \"foo\", \"k3\": \"k1\"}"));
  
  const JSON j2_const(j2);
  ASSERT_EQ("\"blah-changed\"", j2_const["k1"].toString());
  ASSERT_EQ(j2_const, JSON::parse("{\"k1\": \"blah-changed\", \"k2\": \"foo\", \"k3\": \"k1\"}"));
  
  JSON j3(JSON_ARRAY);
  j3.push_back(j2);
  j3.push_back(true);
  j3.push_back(JSON_NULL);
  j3.push_back(12.34);
  j3.push_back(0);
  ASSERT_EQ(j3.size(), 5);
  ASSERT_EQ(j3[0], j2);
  ASSERT_EQ(j3[1], JSON::parse("true"));
  ASSERT_EQ(j3[2], JSON_NULL);
  ASSERT_EQ(j3[3], 12.34);
  ASSERT_NE(j3[3],12.3400001);
  ASSERT_EQ(j3[j3[4]], j2);

  const JSON j3_const(j3);
  ASSERT_EQ(j3_const.size(), 5);
  ASSERT_EQ(j3_const[0], j2);
  ASSERT_EQ(j3_const[1], JSON::parse("true"));
  ASSERT_EQ(j3_const[2], JSON_NULL);
  ASSERT_EQ(j3_const[3], 12.34);
  ASSERT_NE(j3_const[3],12.3400001);
  ASSERT_EQ(j3_const[j3_const[4]], j2);
}

TEST(JSONTest, JSONEquality) {
  JSON j1(JSON_NULL);
  ASSERT_EQ(j1, JSON_NULL);

  JSON j2,j3;
  ASSERT_NE(j2, j3); // JSON_UNDEFINED != JSON_UNDEFINED

  JSON j4 = JSON::parse("[]");
  ASSERT_EQ(j4, j4);
  
  JSON j5(JSON_ARRAY);
  ASSERT_EQ(j4, j5);

  j4.push_back(12);
  j5.push_back(12);
  ASSERT_EQ(j4, j5);

  j4.push_back(14);
  ASSERT_NE(j4,j5);
  j5.push_back(14);
  ASSERT_EQ(j4, j5);

  JSON obj = JSON::parse("{\"foo\": 1, \"blah\": null}");
  j4.push_back(obj);
  j5.push_back(obj);
  ASSERT_EQ(j4, j5);
  
  ASSERT_EQ(j4[2]["blah"], JSON_NULL);


  j4[2]["blah"] = "null";
  ASSERT_NE(j4[2]["blah"], JSON_NULL);
  ASSERT_NE(j4, j5);
  
  j4[2]["blah"] = JSON(JSON_NULL);
  ASSERT_EQ(j4, j5);


  j4[2]["new"] = 0;
  ASSERT_NE(j4, j5);
  
  j5[2]["new"] = 0l;
  j4[2]["new"] = 0.0;
  ASSERT_NE(j4, j5);
  
  j4[2]["new"] = 0;
  ASSERT_TRUE(j4 == j5);
  ASSERT_FALSE(j4 != j5);

  ASSERT_EQ(JSON::parse("{}"), JSON(JSON_OBJECT));
}

TEST(JSONTest, Miscellaneous) {
  JSON j1 = "";
  ASSERT_EQ(j1.toString(), "\"\"");
  ASSERT_EQ(j1.get<std::string>(), "");
  ASSERT_EQ(JSON::parse("[1e-1000]"), JSON::parse("[0.0]"));
  ASSERT_EQ(JSON::parse("[1e-1000]"), JSON::parse("[0.0]"));
  ASSERT_EQ(JSON::parse("[1.213e-2]"), JSON::parse("[.01213]"));
  
  ASSERT_EQ(JSON::parse("[1.213E-2]"), JSON::parse("[.1213e-1]"));
}

TEST(JSONTest, AssignmentAndCopyConstructor) {
  JSON j1 = vector<int>(5,0);
  ASSERT_EQ(j1.type(), JSON_ARRAY);
  ASSERT_EQ(j1.length(), 5);
  ASSERT_EQ(j1[0], 0);
  ASSERT_EQ(JSON(vector<int>(5,0)), j1);

  map<string, double> mp;
  mp["k1"] = 1.0;
  mp["k2"] = 2.0;
  JSON j2 = mp;
  ASSERT_EQ(JSON(mp), j2);
  ASSERT_EQ(j2.length(), 2);
  ASSERT_EQ(j2["k1"], 1.0);
  ASSERT_EQ(double(j2["k1"]), 1);
  ASSERT_NE(j2["k1"], 1); // In this case, 1 will be converted to JSON, and 1.0 != 1 (in our json case)
}

TEST(JSONTest, ResizeArray) {
  JSON j1(JSON_ARRAY);
  ASSERT_EQ(j1.length(), 0);
  j1.resize_array(10);
  ASSERT_EQ(j1.length(), 10);
  ASSERT_EQ(j1[0].type(), JSON_UNDEFINED);
  
  JSON j2(JSON_ARRAY);
  for (int i = 0;i < 10 ; i++)
    j2.push_back(i);
  
  JSON j3 = j2;
  ASSERT_EQ(int(j2[4]), 4);
  ASSERT_EQ(int(j2[9]), 9);
  
  j2.resize_array(5);
  ASSERT_EQ(j2.length(), 5);
  ASSERT_EQ(int(j2[4]), 4);
  j2.resize_array(0);
  ASSERT_EQ(j2.length(), 0);
  
  int lastval = int(j3[j3.length() -1 ]);
  int len = j3.length();
  j3.resize_array(j3.length()); // Should have no effect;
  ASSERT_EQ(len, j3.length());
  ASSERT_EQ(lastval, int(j3[len-1]));
}

TEST(JSONTest, UnicodeAndEscapeSequences) {
  
  // TODO: Add more unicode tests
  JSON j1 = "\u6e05\u534e\u5927\u5b66";
  ASSERT_EQ(j1, "清华大学");
  
  JSON j2 = '\n';
  ASSERT_EQ(j2.toString(), "\"\\n\"");

  ASSERT_EQ(JSON::parse("[\"\\\"\\\\\\/\\b\\f\\n\\r\\t\"]").toString(), "[\"\\\"\\\\/\\b\\f\\n\\r\\t\"]");
  
  ASSERT_EQ(JSON::parse("[\"\\u0012 escaped control character\"]").toString(), "[\"\\u0012 escaped control character\"]");
  
  ASSERT_EQ(JSON::parse("[\"\\u000a\"]").toString(), "[\"\\n\"]");
  ASSERT_EQ(JSON::parse("[\"\\u000d\"]").toString(), "[\"\\r\"]");
  ASSERT_EQ(JSON::parse("[\"\\u001f\"]").toString(), "[\"\\u001f\"]");
  ASSERT_EQ(JSON::parse("[\"\\u0020\"]").toString(), "[\" \"]");
  ASSERT_EQ(JSON::parse("[\"\\u0000\"]").toString(), "[\"\\u0000\"]");
  std::string temp = "[\"x\"]";
  temp[2] = char(0);
  ASSERT_EQ(JSON::parse(temp).toString(), "[\"\\u0000\"]");

  ASSERT_EQ(JSON::parse("[\"\\uff13\"]").toString(), "[\"３\"]");
  ASSERT_EQ(JSON::parse("[\"\\uD834\\uDD1E surrogate, four-byte UTF-8\"]").toString(), "[\"𝄞 surrogate, four-byte UTF-8\"]");
  ASSERT_EQ(JSON::parse("[\"€þıœəßð some utf-8 ĸʒ×ŋµåäö𝄞\"]").toString(), "[\"€þıœəßð some utf-8 ĸʒ×ŋµåäö𝄞\"]");
 
  JSON j3 = JSON::parse("\"\\u0821\"");
  std::string s1j3 = j3.get<std::string>();
  ASSERT_EQ(s1j3.length(), 3); 
  string s2j3 = "\"";
  s2j3.push_back(-32);
  s2j3.push_back(-96);
  s2j3.push_back(-95);
  s2j3.push_back('\"');
  ASSERT_EQ(s1j3, (JSON::parse(s2j3)).get<std::string>());
  
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\ud800\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\ud800\\udb00\""));
  JSON::parse("\"\\ud800\\udc00\""); // Should not throw exception
  
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\u12\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\u\""));
  ASSERT_EQ(JSON::parse("\"\\\\u\"").get<std::string>(), "\\u");
 
  // Construct an invalid utf8 (should be replaced with replacement character (U+FFFD))
  temp = "\"";
  temp.push_back(0xc0);
  temp.push_back(0x8a);
  temp += "\"";
  temp = (JSON::parse(temp)).get<std::string>();
  ASSERT_EQ(temp, "\ufffd");
  
  JSON j4(JSON_OBJECT);
  j4[temp] = "blah";
  ASSERT_EQ(j4["\ufffd"].get<std::string>(), "blah");
  
  j4["\u0021"] = "foo";
  ASSERT_EQ(j4["!"].get<std::string>(), "foo");
  ASSERT_TRUE(j4.toString().find("!") != string::npos);
  ASSERT_FALSE(j4.toString().find("\\u0000") != string::npos);
  j4[std::string(1, char(0))] = "foo2";
  ASSERT_TRUE(j4.toString().find("\\u0000") != string::npos);
  ASSERT_TRUE(j4.has("!"));
  ASSERT_TRUE(j4.has("\u0021"));
  ASSERT_TRUE(j4.has("\\u0000"));
  ASSERT_TRUE(j4["\u0021"] == j4["!"]);
  ASSERT_EQ(j4["\\u0000"], j4[std::string(1, 0)]);
  
  // Weird that string "\u0000" in C++ actually becomes "\u0001";
  ASSERT_EQ("\u0000", "\u0001");
  ASSERT_NE("\u0000", "\u0002");
  //////////////////////////////////////////////////////////////

  temp = JSON::parse("\"a\x80\xe0\xa0\xc0\xaf\xed\xa0\x80z\"").get<std::string>();
  ASSERT_EQ(temp, "a\ufffd\ufffd\ufffd\ufffdz");
}

TEST(JSONTest, getAndConversionOperator) {
  JSON j1 = JSON::parse("{}");
  ASSERT_EQ(j1.type(), JSON_OBJECT);
  j1["1"] = 1;
  j1["2"] = 1.1;
  j1["3"] = 0;
  j1["4"] = "string";
  j1["5"] = true;

  ASSERT_EQ(j1["1"].get<int>(), 1);
  ASSERT_EQ(j1["2"].get<int>(), 1);
  ASSERT_TRUE( fabs(j1["2"].get<double>() - 1.1) < 1e-12);
  ASSERT_EQ(j1["3"].get<bool>(), false);
  ASSERT_EQ(j1["5"].get<bool>(), true);
  ASSERT_EQ(j1["1"].get<bool>(), true);
  ASSERT_EQ(j1["5"].get<int>(), 1);
  ASSERT_EQ(j1["4"].get<string>(), "string");
  ASSERT_EQ(j1["5"].get<bool>(), bool(j1["5"]));
  ASSERT_EQ(j1["1"].get<short int>(), (short int)j1["1"]);
  ASSERT_EQ(j1["1"].get<float>(), (float)j1["1"]);

  ASSERT_JSONEXCEPTION(j1["4"].get<int>());
  ASSERT_JSONEXCEPTION(j1["1"].get<std::string>());
}

TEST(JSONTest, HasAndErase) {
  JSON j1 = JSON::parse("{\"k1\": \"k2\", \"k2\": [1,2,3,4], \"k3\": 14}");
  JSON j2 = j1;
  ASSERT_EQ(j1.length(), 3);
  
  ASSERT_EQ(j1, j2);
  ASSERT_TRUE(j1.has("k1"));
  
  j1.erase("k1");
  ASSERT_FALSE(j1.has("k1"));
  ASSERT_NE(j1, j2);
  ASSERT_TRUE(j2.has("k1"));
  ASSERT_TRUE(j1.has(j2["k1"]));

  ASSERT_EQ(j1["k2"].length(), 4);
  ASSERT_EQ(j1["k2"], j2["k2"]);

  ASSERT_EQ(j1["k2"][2], 3);
  j1["k2"].erase(2);
  ASSERT_EQ(j1["k2"].length(), 3);
  ASSERT_EQ(j1["k2"][2], 4);
  ASSERT_NE(j1["k2"], j2["k2"]);
  ASSERT_EQ(j2["k2"].size(), 4);

  ASSERT_TRUE(j1["k2"].has(1.2));
  ASSERT_FALSE(j1["k2"].has(3.00001));
  ASSERT_TRUE(j2["k2"].has(3.00001));

  ASSERT_TRUE(j1["k2"].has(true));

  j1["k2"].erase(1);
  j1["k2"].erase(1);
 
  ASSERT_EQ(j1["k2"].size(), 1);
  ASSERT_TRUE(j1["k2"].has(false));
  ASSERT_FALSE(j1["k2"].has(true));
  
  ASSERT_TRUE(j1.has("k2"));

  const JSON j1_const(j1);
  ASSERT_EQ(j1_const["k2"].size(), 1);
  ASSERT_TRUE(j1_const["k2"].has(false));
  ASSERT_FALSE(j1_const["k2"].has(true));
  
  ASSERT_TRUE(j1_const.has("k2"));
}

TEST(JSONTest, TestPerformance) {
  // TODO
}

TEST(JSONTest, Iterators) {

}
TEST(JSONTest, FloatingPointPrecision) {
  JSON j1 = 5.7;
  JSON j2 = 5.701;
  ASSERT_TRUE(j1 != j2);
  JSON::setEpsilon(.1);
  ASSERT_TRUE(j1 == j2);
  ASSERT_EQ(JSON::getEpsilon(), .1);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

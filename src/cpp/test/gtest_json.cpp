#include <gtest/gtest.h>
#include <iostream>
#include "dxjson.h"
#include <fstream>
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

TEST(JSONTest, ParseJSONTestSuiteExampleFile) {
  // The file being parsed has been downloaded from here
  // http://code.google.com/p/json-test-suite/downloads/detail?name=sample.zip
  // Structure of this JSON file is like this (console.dir() output from v8(node.js) for this file):
  /*
  { a: 
   { '6U閆崬밺뀫颒myj츥휘:$薈mY햚#rz飏+玭V㭢뾿愴YꖚX亥ᮉ푊\u0006垡㐭룝"厓ᔧḅ^Sqpv媫"⤽걒"˽Ἆ?ꇆ䬔未tv{DV鯀Tἆl凸g\\㈭ĭ즿UH㽤': null,
     'b茤z\\.N': [ [Object] ],
     obj: { key: 'wrong value' },
     '퓲꽪m{㶩/뇿#⼢&᭙硞㪔E嚉c樱㬇1a綑᝖DḾ䝩': null },
  key: '6.908319653520691E8',
  z: 
   { '6U閆崬밺뀫颒myj츥휘:$薈mY햚#rz飏+玭V㭢뾿愴YꖚX亥ᮉ푊\u0006垡㐭룝"厓ᔧḅ^Sqpv媫"⤽걒"˽Ἆ?ꇆ䬔未tv{DV鯀Tἆl凸g\\㈭ĭ즿UH㽤': null,
     'b茤z\\.N': [ [Object] ],
     obj: { key: 'wrong value' },
     '퓲꽪m{㶩/뇿#⼢&᭙硞㪔E嚉c樱㬇1a綑᝖DḾ䝩': null } }
  */
  std::fstream ifs;
  ifs.open("json-test-suite.json", std::fstream::in);
  ASSERT_FALSE(ifs.fail()); // should be able to open this file
  // Read the whole file in a string
  std::string str((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  ifs.close();

  JSON j1 = JSON::parse(str); // Should be able to parse the file
  
  // Just afew simple assertions
  ASSERT_TRUE(j1.type() == JSON_OBJECT);
  ASSERT_EQ(j1["a"]["obj"]["key"], "wrong value");
  ASSERT_EQ(j1["z"]["퓲꽪m{㶩/뇿#⼢&᭙硞㪔E嚉c樱㬇1a綑᝖DḾ䝩"].type(), JSON_NULL);
  ASSERT_EQ(j1["z"]["6U閆崬밺뀫颒myj츥휘:$薈mY햚#rz飏+玭V㭢뾿愴YꖚX亥ᮉ푊\u0006垡㐭룝\"厓ᔧḅ^Sqpv媫\"⤽걒\"˽Ἆ?ꇆ䬔未tv{DV鯀Tἆl凸g\\㈭ĭ즿UH㽤"].type(), JSON_NULL);
  
  std::string stringification = j1.toString();
  // Assert that we stringify one of the unicode string in this file correctly
  ASSERT_TRUE(stringification.find("6U閆崬밺뀫颒myj츥휘:$薈mY햚#rz飏+玭V㭢뾿愴YꖚX亥ᮉ푊\\u0006垡㐭룝\\\"厓ᔧḅ^Sqpv媫\\\"⤽걒\\\"˽Ἆ?ꇆ䬔未tv{DV鯀Tἆl凸g\\\\㈭ĭ즿UH㽤") != std::string::npos);
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

  ASSERT_EQ(JSON::parse("{\"f\t\"     \r \v \t   \n      : \t  12}")["f\t"], 12);

  JSON j4 = JSON::parse("[0, 1, 2, 3]");
  ASSERT_EQ(j4[0.01], 0);
  ASSERT_EQ(j4[false], 0);
  ASSERT_EQ(j4[true], 1);
  ASSERT_EQ(j4[1.0], 1);

  JSON j5 = vector<int>(5,-1);
  ASSERT_EQ(j5.length(), 5);
  ASSERT_EQ(j5[2], -1);

  map<string, int> m;
  m["0"] = 0;
  m["1"] = 1;
  JSON j6 = m;
  j6["1"] = j6["1"].get<int>() + 1;
  ASSERT_EQ(j6.size(), 2);
  ASSERT_EQ(j6["0"], 0);
  ASSERT_EQ(j6["1"], 2);

  // Invalid cases:
  ASSERT_JSONEXCEPTION(JSON::parse("[\"\\x15\"]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[\\n]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[\"\\017\"]"));
  ASSERT_JSONEXCEPTION(JSON::parse("{\"a\":\"a"));
  ASSERT_JSONEXCEPTION(JSON::parse("sa"));
  ASSERT_JSONEXCEPTION(JSON::parse("å"));
  ASSERT_JSONEXCEPTION(JSON::parse(""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\a\""));
  ASSERT_JSONEXCEPTION(JSON::parse("[1,2,3 foo]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1,]"));
  ASSERT_JSONEXCEPTION(JSON::parse("{\"f\" 12}"));
}

//TEST(JSONTest, "ArithmeticOnJSON")
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
  JSON j6, j7;
  j6 = j7 = j5;
  ASSERT_TRUE(j5 == j7 && j6 == j7);

  ASSERT_EQ(JSON::parse("{}"), JSON(JSON_OBJECT));
}

TEST(JSONTest, CreationFromFile) {
  std::fstream ifs;
  ifs.open("test_data/pass1.json", fstream::in);
  JSON j1;
  j1.read(ifs);
  ifs.close();

  ifs.open("test_data/pass1.json", fstream::in);
  JSON j2;
  j2.read(ifs);
  ifs.close();
}

TEST(JSONTest, Miscellaneous) {
  JSON j1 = "";
  ASSERT_EQ(j1.toString(), "\"\"");
  ASSERT_EQ(j1.get<std::string>(), "");

  JSON j2 = JSON::parse("[null, false, true, {\"0\": {\"1\": {\"2\": 21.23e-2}}}, [[[[2121]]]]]");
  ASSERT_TRUE(j2.toString().find("false") != string::npos);
  ASSERT_TRUE(j2.toString().find("\"false\"") == string::npos);
  ASSERT_TRUE(j2.toString().find("null") != string::npos);
  ASSERT_TRUE(j2.toString().find("\"null\"") == string::npos);
  ASSERT_TRUE(j2[4][0][0][0][0] == 2121);
  ASSERT_TRUE(j2[3]["0"]["1"]["2"] == JSON(.2123));
  ASSERT_TRUE(j2[0] == JSON(JSON_NULL));
  ASSERT_TRUE(j2[1] == JSON(false));
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
  JSON j1_1 = "\u0905\u0928\u0941\u0930\u093e\u0917 \u092c\u093f\u092f\u093e\u0928\u0940";
  ASSERT_EQ(j1_1, "अनुराग बियानी");
  
  JSON j1 = "\u6e05\u534e\u5927\u5b66";
  ASSERT_EQ(j1, "清华大学");

  ASSERT_EQ(j1, JSON::parse("\"\\u6e05\\u534e\\u5927\\u5b66\""));
  ASSERT_EQ(JSON::parse("\"15\\u00f8C\"").get<string>(), "15øC");

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
  ASSERT_EQ(j4["\u0021"], j4["!"]); // Should be trivially true since in c++, "\u0021" == "!"
  ASSERT_FALSE(j4.has("\\u0021")); 
  ASSERT_TRUE(j4.has("\u0021"));
  ASSERT_TRUE(j4.toString().find("!") != string::npos);

  ASSERT_TRUE(j4.toString().find("\\u0000") == string::npos);
  const JSON j4_const(j4);
  ASSERT_EQ(j4_const["!"].get<std::string>(), "foo");
  ASSERT_EQ(j4_const["\u0021"], j4_const["!"]); // Should be true since in c++, "\u0021" == "!"
  ASSERT_FALSE(j4_const.has("\\u0021"));
  ASSERT_TRUE(j4_const.has("\u0021"));
  ASSERT_TRUE(j4_const.toString().find("!") != string::npos);
  ASSERT_FALSE(j4_const.toString().find("\\u0000") != string::npos);

  j4[std::string(1, char(0))] = "foo2";
  ASSERT_TRUE(j4.toString().find("\\u0000") != string::npos);
  ASSERT_TRUE(j4.has("!"));
  ASSERT_TRUE(j4.has("\u0021"));
  ASSERT_FALSE(j4.has("\\u0000"));
  ASSERT_TRUE(j4["\u0021"] == j4["!"]);
  ASSERT_EQ(j4[std::string(1, 0)].get<std::string>()[0], 'f');

  // Weird that string "\u0000" in C++ actually becomes "\u0001";
  // These two lines below are not exactly JSON parser test.
  // Just so that I remember this fact.
  ASSERT_EQ("\u0000", "\u0001");
  ASSERT_NE("\u0000", "\u0002");
  //////////////////////////////////////////////////////////////

  JSON j5 = "\\u0000"; // Will be treated as a normal string ("\u0000") and not json serilization
  ASSERT_TRUE(j5.get<std::string>().find("\\u0000") != string::npos);
  ASSERT_TRUE(j5.get<std::string>().find(char(0)) == string::npos);
  ASSERT_TRUE(j5.toString().find(char(0)) == string::npos);
  ASSERT_TRUE(j5.toString().find("\\u0000") != string::npos);
  
  JSON j5_2 = JSON::parse("\"\\u0000\""); 
  ASSERT_TRUE(j5_2.get<std::string>().find("\\u0000") == string::npos);
  ASSERT_TRUE(j5_2.get<std::string>().find(char(0)) != string::npos);
  ASSERT_TRUE(j5_2.toString().find(char(0)) == string::npos);
  ASSERT_TRUE(j5_2.toString().find("\\u0000") != string::npos);


  JSON j6 = "\\u000a";
  ASSERT_TRUE(j6.get<std::string>().find("\\u000a") != string::npos);
  ASSERT_TRUE(j6.get<std::string>().find(char(10)) == string::npos);
  ASSERT_TRUE(j6.toString().find(char(10)) == string::npos);
  ASSERT_TRUE(j6.toString().find("\\u000a") != string::npos);
  ASSERT_TRUE(j6.toString().find("\\n") == string::npos);
  
  JSON j6_2 = JSON::parse("\"\\u000a\"");
  ASSERT_TRUE(j6_2.get<std::string>().find("\\u000a") == string::npos);
  ASSERT_TRUE(j6_2.get<std::string>().find(char(10)) != string::npos);
  ASSERT_TRUE(j6_2.toString().find(char(10)) == string::npos);
  ASSERT_TRUE(j6_2.toString().find("\\u000a") == string::npos);
  ASSERT_TRUE(j6_2.toString().find("\\n") != string::npos);
 
  JSON j7 = JSON::parse("{\"\\u000a\": 12}");
  JSON j7_1;
  JSON j7_2 = JSON::parse("{\"\\n\": 12}");
  j7_1 = j7;
  ASSERT_TRUE(j7 == j7_1 && j7_2 == j7);
  ASSERT_TRUE(j7.has("\u000a"));
  ASSERT_FALSE(j7.has("\\n"));
  ASSERT_TRUE(j7.has("\u000a"));
  ASSERT_FALSE(j7.has("\\u000a"));
  
  j7.erase("\n");
  ASSERT_FALSE(j7.has("\u000A"));

  ASSERT_JSONEXCEPTION(j7_1.erase("\\u000a"));
  ASSERT_JSONEXCEPTION(j7_1.erase("\\n"));
  
  ASSERT_TRUE(j7_1.has("\u000a"));
  j7_1.erase(std::string(1,10));
  ASSERT_FALSE(j7_1.has("\u000a"));
  
  ASSERT_TRUE(j7_2.has("\n"));
  j7_2.erase("\u000a");
  ASSERT_FALSE(j7_2.has("\n"));

  temp = JSON::parse("\"a\x80\xe0\xa0\xc0\xaf\xed\xa0\x80z\"").get<std::string>();
  ASSERT_EQ(temp, "a\ufffd\ufffd\ufffd\ufffdz");

  ASSERT_JSONEXCEPTION(JSON::parse("\"\\u000\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\u000 1\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\uD800\\u\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\ud800\\ux912\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\ud800\\ug123\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\ud800\\udc0\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\uå\""));
  ASSERT_JSONEXCEPTION(JSON::parse("\"\\\n"));

  JSON j8 = JSON::parse("{\"\\\\a\": 12}");
  ASSERT_EQ(j8["\\a"], 12);
  j8["\\b"] = 15;
  ASSERT_EQ(j8["\\b"], 15);
  ASSERT_FALSE(j8.has("\\\\a"));

  JSON j9 = JSON::parse("{}");
  j9["\\n"] = 12;
  ASSERT_FALSE(j9.has(std::string(1,'\n')));
  j9["\n"] = 13;
  ASSERT_TRUE(j9.has("\n"));
  ASSERT_EQ(j9["\n"], 13);
  j9.erase("\\n");
  ASSERT_EQ(j9["\n"], 13);
  ASSERT_FALSE(j9.has("\\n"));
  
  const JSON j10 = JSON::parse("{\"\\\\r\": 0, \"\n\": 10}");
  ASSERT_TRUE(j10.has("\\r"));
  ASSERT_FALSE(j10.has("\r"));
  ASSERT_TRUE(j10.has("\n"));
  ASSERT_FALSE(j10.has("\\n"));
  ASSERT_EQ(j10["\\r"], 0);
  ASSERT_EQ(j10["\n"], 10);

  const JSON j11 = "\n";
  ASSERT_TRUE(j11.toString().find("\\n") != string::npos);
  ASSERT_TRUE(j11.toString().find("\n") == string::npos);
  ASSERT_TRUE(j11.get<std::string>().find("\n") != string::npos);

  const JSON j12 = "\\n";
  ASSERT_TRUE(j12.toString().find("\\\\n") != string::npos);
  ASSERT_TRUE(j12.get<std::string>().find("\n") == string::npos);

  JSON j13 = JSON::parse("{\"\\\\r\": 0, \"\n\": 10}");
  JSON j13_keys = JSON::parse("[\"\\\\r\", \"\n\"]");
  JSON j13_Invalidkeys = JSON::parse("[\"\\r\", \"\\\\n\"]");
  ASSERT_TRUE(j13.has(j13_keys[0]));
  ASSERT_TRUE(j13.has(j13_keys[1]));
  ASSERT_FALSE(j13.has(j13_Invalidkeys[0]));
  ASSERT_FALSE(j13.has(j13_Invalidkeys[1]));
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

TEST(JSONTest, Numbers) {
  JSON j1 = -1;
  JSON j2 = JSON::parse("-1");
  ASSERT_EQ(j1, j2);

  j1 = 1.0;
  j2 = 1;
  ASSERT_EQ(JSON(-1l), JSON(short(-1)));
  ASSERT_EQ(JSON(1ll), JSON(uint8_t(1)));
  ASSERT_NE(j1, j2); // REAL and INTEGER value are never equal in out implementation

  j1 = JSON::parse("-1e-20");
  j2 = JSON::parse("-1e-23");

  ASSERT_EQ(j1, j2);

  ASSERT_EQ(JSON::parse("[1e-1000]"), JSON::parse("[0.0]"));
  ASSERT_EQ(JSON::parse("[1e-1000]"), JSON::parse("[0.0]"));
  ASSERT_EQ(JSON::parse("[1.213e-2]"), JSON::parse("[0.01213]"));
  ASSERT_EQ(JSON::parse("[1.213E-2]"), JSON::parse("[0.1213e-1]"));

  ASSERT_NE(JSON::parse("[0.0]"), JSON::parse("[0]"));
  ASSERT_EQ(JSON::parse("[0.00000000]"), JSON::parse("[0.0]"));
  ASSERT_EQ(JSON::parse("[0.00]"), JSON::parse("[0.00E-2]"));
  ASSERT_EQ(JSON::parse("[0.00]"), JSON::parse("[0e+0]"));
  ASSERT_EQ(JSON::parse("[100.0]"), JSON::parse("[   1E+2   ]"));
  ASSERT_EQ(JSON::parse("[1.0]"), JSON::parse("[1e-0]"));
  ASSERT_EQ(JSON::parse("[10.0]"), JSON::parse("[0.1E+2  ]"));
  ASSERT_EQ(JSON::parse("[-0]"), JSON::parse("[0]"));

  JSON::parse("[-123]"); // Should not throw exception

  // These JSON::parse below are invalud json numbers
  ASSERT_JSONEXCEPTION(JSON::parse("[01]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1+2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1 2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1-2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[.1]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[+1]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[+1e-23-2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[+1e+23.2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[0001]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[0..1]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1..23]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1e-2.3]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1e.3]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1e+0.0"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1e]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1f+2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1ee2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1eE2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1e++2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1e+-2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[e+2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[--1]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[-+1]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[+0]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1--2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1e12e2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[00]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[00001]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[.e-2]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[0.2e-+23]"));
  ASSERT_JSONEXCEPTION(JSON::parse("[1+d2]"));
}

TEST(JSONTest, TestPerformance) {
  // TODO
}

TEST(JSONTest, Iterators) {
  JSON j1(JSON_OBJECT);
  j1["0"] = 0;
  j1["1"] = 1;
  j1["2"] = 2;
  j1["3"] = 3;
  j1["4"] = 4;
  int i=0;
  for(JSON::object_iterator it = j1.object_begin(); it != j1.object_end(); ++i, ++it)
    ASSERT_EQ(it->second, i);

  i = 4;
  for(JSON::object_reverse_iterator it = j1.object_rbegin(); it != j1.object_rend(); --i, ++it)
    ASSERT_EQ(it->second, i);

  const JSON j1_const = j1;
  i=0;
  for(JSON::const_object_iterator it = j1_const.object_begin(); it != j1_const.object_end(); ++i, ++it)
    ASSERT_EQ(it->second, i);

  i = 4;
  for(JSON::const_object_reverse_iterator it = j1_const.object_rbegin(); it != j1_const.object_rend(); --i, ++it)
    ASSERT_EQ(it->second, i);

  JSON j2(JSON_ARRAY);
  j2.resize_array(5);
  j2[0] = 0;
  j2[1] = 1;
  j2[2] = 2;
  j2[3] = 3;
  j2[4] = 4;
  i=0;
  for(JSON::array_iterator it = j2.array_begin(); it != j2.array_end(); ++i, ++it)
    ASSERT_EQ(*it, i);

  i = 4;
  for(JSON::array_reverse_iterator it = j2.array_rbegin(); it != j2.array_rend(); --i, ++it)
    ASSERT_EQ(*it, i);

  const JSON j2_const = j2;
  i=0;
  for(JSON::const_array_iterator it = j2_const.array_begin(); it != j2_const.array_end(); ++i, ++it)
    ASSERT_EQ(*it, i);

  i = 4;
  for(JSON::const_array_reverse_iterator it = j2_const.array_rbegin(); it != j2_const.array_rend(); --i, ++it)
    ASSERT_EQ(*it, i);

  // TODO: Add more iterators test, specially those which use stl algorithms heavily

}
TEST(JSONTest, RealNumberApproxComparisonTest) {
  double eps = JSON::getEpsilon();
  ASSERT_EQ(eps, std::numeric_limits<double>::epsilon());
  
  JSON j1 = 5.7;
  JSON j2 = 5.701;
  ASSERT_TRUE(j1 != j2);
  
  j1 = eps, j2 = 2.0*eps;
  ASSERT_TRUE(j1 == j2);

  j1 = eps, j2 = 2.1*eps;
  ASSERT_TRUE(j1 != j2);
  
  j1 = 0.0, j2 = eps;
  ASSERT_TRUE(j1 == j2);
  
  j1 = 0.0, j2 = eps * eps;
  ASSERT_TRUE(j1 == j2);
  
  j1 = -1.0 * eps, j2 = 0.0;
  ASSERT_TRUE(j1 == j2); 
  
  j1 = 0.0, j2 = 1.00000000000001 * eps;
  ASSERT_TRUE(j1 != j2);
  
  j1 = 0.0, j2 = .0000000000000001 * eps;
  ASSERT_TRUE(j1 == j2);

  j1 = 1e30, j2 = 1e30 - eps;
  ASSERT_TRUE(j1 == j2);

  // Due to relative error check the real number below are approx equal
  // Even though absolute difference between them is quite high
  j1 = 1e30, j2 = 1e30 - (0.9e30 * eps);
  ASSERT_TRUE(j1 == j2);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

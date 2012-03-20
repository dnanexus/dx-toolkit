#include <gtest/gtest.h>
#include "NotSoSimpleJSON.h"

using namespace std;

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

TEST(JSONTest, UnicodeAndEscapeSequences) {
  // TODO: Add more unicode tests
  JSON j1 = "\u6e05\u534e\u5927\u5b66";
  ASSERT_EQ(j1, "清华大学");
  
  JSON j2 = '\n';
  ASSERT_EQ(j2.toString(), "\"\\n\"");
}


TEST(JSONTest, HasAndErase) {
  JSON j1 = JSON::parse("{\"k1\": \"k2\", \"k2\": [1,2,3,4], \"k3\": 14}");
  JSON j2 = j1;
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

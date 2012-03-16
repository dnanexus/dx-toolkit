#include <gtest/gtest.h>
#include "NotSoSimpleJSON.h"

using namespace std;

TEST(JSONTest, TestCreationAndEqualityTesting) {
  // TODO
  dx::JSON json = dx::parse("{\"x\": 1, \"hello\": \"world\"}");
  ASSERT_EQ("1", json["x"].toString());
  ASSERT_EQ("world", json["hello"].toString());
}

TEST(JSONTest, TestModification) {
  // TODO
}

TEST(JSONTest, TestStringify) {
  // TODO
}

TEST(JSONTest, TestIterator) {
  // TODO
}

TEST(JSONTest, TestUnicode) {
  // TODO
}

TEST(JSONTest, TestPerformance) {
  // TODO
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

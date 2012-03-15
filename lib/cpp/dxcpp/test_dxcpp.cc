#include <gtest/gtest.h>
#include "../dxcpp/dxcpp.h"

using namespace std;

class DXFileTest : public testing::Test {
public:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(DXFileTest, DummyTest) {
  ASSERT_EQ(0, 0);
}

// Tests go outside.  If using the fixture:
// TEST_F(DXFileTest, TestCreateDestroy)
//
// Can also choose not to use a fixture:
// TEST(DXFileTest, TestCreateDestroy)
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

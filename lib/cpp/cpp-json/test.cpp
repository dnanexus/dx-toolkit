#include <cstdio>
#include "NotSoSimpleJSON.h"

int main() {
  using namespace std;
  try 
  {
    JSON j1(JSON_OBJECT);

    std::cout<<"Eps = "<<JSON::getEpsilon();
    j1["key"] = 12;
    JSON j2(JSON_OBJECT);
    j2["blah"] = "sdsdsd";
    j2["blah"] = "key";
    j2["lala"] = j1[j2["blah"]];
    std::cout<<"\nj2 = \n"<<j2.ToString()<<"\n";
    j2.erase("lala");
    std::cout<<"\n j2 after erasing 'lala' = \n"<<j2.ToString()<<"\n";
    // JSON parse tests
    JSON j3 = JSON::parse("{\"blah\": [  21,232,\"foo\" , {\"key\": \"val1\"}, true, false, null]}");
    j3["blah"].push_back(1.23456789101112);
    j3["blah"].push_back("dsdsd");
    j3["blah"].push_back(Null());
    j3["blah"].push_back(12.212);
    j3["foo"] = vector<int>(5,5);
    std::map<std::string, int> mp;
    mp["lala"] = 12.11e-212;
    mp["dsdsd"] = 1212;
    j3["map"] = mp;
    std::cout<<"\nj3 = "<<j3.ToString();
    std::cout<<"\nj3[blah] = "<<j3["blah"].ToString();
    std::cout<<"\nj3[blah][2] = "<<j3["blah"][2].ToString()<<"\n";
   
    j3["blah"].erase(2);
    std::cout<<"\nBlah after erasing indx = 2\n"<<j3["blah"].ToString()<<"\n";

    JSON j4;
    std::string str = "{\"清华大学\": [\"this should look like second element\", \"\\u6e05\\u534e\\u5927\\u5b66\", \"\\n\\b\\t\\\"\"]    }";
    j4 = JSON::parse(str);
    
    std::cout<<"j4 = "<<j4.ToString()<<"\n";

    JSON j5(JSON_BOOLEAN);
    j5 = true;
    std::cout<<"\nj5 = "<<j5.ToString()<<"\n";
    
    // Equality tests
    std::cout<<"\nj4 == j5: "<<((j4==j5)?"true":"false");

    JSON j5_copy = j5;
    std::cout<<"\nj5_copy == j5: "<<((j5_copy==j5) ? "true" : "false")<<"\n";
    
    JSON j6 = 12.21;
    JSON j7 = 12.22;
    assert(j6 != j7);
    JSON::setEpsilon(.2);
    assert(j6 == j7);
    JSON::setEpsilon(1e-12);
    
    JSON j8(JSON_ARRAY);
    j8.push_back(12.21);
    j8.push_back("hello");
    j8.push_back(j8);
    std::cout<<"\nj8 = "<<j8.ToString()<<"\n";
    JSON j9 = j8;
    assert(j9 == j8);
    j9.erase(2);
    assert(j9 != j8);
    assert(JSON(JSON_NULL) == JSON(JSON_NULL));
   
    // Iterator test
    int i=0;
    JSON j10(JSON_OBJECT);
    j10["key1"] = 12;

    j10["key2"] = 13;
    j10["key3"] = j8;

    j10["key4"] = j8;

    std::cout<<"\nChecking forward iterators now ... j10 = "<<j10.ToString()<<"\n";
    for(JSON::array_iterator it = j8.array_begin();it != j8.array_end(); ++it, ++i) {
      assert(j8[i] == *(it));
    }
    
    for(JSON::object_iterator it = j10.object_begin();it != j10.object_end(); ++it) {
      assert(j10[it->first] == it->second);
      std::cout<<"Key = "<<it->first<<", Value = "<<it->second.ToString()<<endl;
    } 
    std::cout<<"\nChecking reverse now ...\n"; 
    i = j8.size() - 1;
    for(JSON::array_reverse_iterator it = j8.array_rbegin();it != j8.array_rend(); ++it, --i) {
      assert(j8[i] == *(it));
    }
    
    for(JSON::object_reverse_iterator it = j10.object_rbegin();it != j10.object_rend(); ++it) {
      assert(j10[it->first] == it->second);
      std::cout<<"Key = "<<it->first<<", Value = "<<it->second.ToString()<<endl;
    } 
 
    // 
    //typedef std::map<std::string, JSON> ObjectIterator
    //ObjectIterator it = j4.ObjectBegin();
    // JSON Iterators (different class);

    //for (; it != j4.end(); ++it) {
      
  //  }

     // Check implicit cast operators
     JSON j11(JSON_OBJECT);
     j11["1"] = 1;
     j11["2"] = 12.33;
     j11["3"] = true;
     j11["4"] = 212l;
     j11["4.1"] = "blahh";
     j11["5"] = vector<int>(5,0);
     j11["6"] = "1";

     assert(j11["5"][0.9] == 0);
     assert(j11["5"][j11["1"]] == 0);
    
     assert(j11.has("1"));
     assert(!j11.has("random"));
     assert(j11["5"].has(0));
     assert(j11["5"].has(1));
     assert(j11["5"].has(j11["5"][0]));
     assert(j11.has(j11["6"]) && j11[j11["6"]] == 1);
     assert(j11["5"][j11["1"]] == 0);
     assert(double(j11["1"]) == 1);
     assert(double(j11["2"]) == 12.33);
     assert(bool(j11["3"]) == true);
     assert(j11["4.1"].ToString() == "\"blahh\"");
     assert(long(j11["4"]) == 212l);
     assert(double(j11["1"]) < double(j11["2"]));
      
     const JSON j12(j11);
   
     assert(j12["5"][0.9] == 0);
     assert(j12["5"][j11["1"]] == 0);

 
     assert(j12["5"][j11["1"]] == 0);
     assert(double(j12["1"]) == 1);
     assert(double(j12["2"]) == 12.33);
     assert(bool(j12["3"]) == true);
     assert(j12["4.1"].ToString() == "\"blahh\"");
     assert(long(j12["4"]) == 212l);
     assert(double(j12["1"]) < double(j11["2"]));

     std::cout<<"\nAll assertions perfoemed\n";
  }
  catch (JSONException &e) 
  {
    std::cout<<"\nErrror occured: \n"<<e.err<<"\n";
  }

  return 0;
}



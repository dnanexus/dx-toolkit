#ifndef __NOT_SO_SIMPLE_JSON_H__
#define __NOT_SO_SIMPLE_JSON_H__

#include <cstdio>
#include <cstring>
#include <vector>
#include <map>
#include <cstdlib>
#include <string>
#include <iostream>
#include <cstdio>
#include <exception>
#include <cassert>
#include <sstream>
#include <istream>
#include <ostream>
#include <limits>
#include <typeinfo>

#include "utf8/source/utf8.h"

typedef long long int64;

class JSON_exception {
public:
  std::string err;
  JSON_exception(const std::string &e): err(e) {}
};

/*
// Base class for all JSON related runtime errors 
class JSON_exception: public std::exception {
public:
  std::string err;
  JSON_exception(): err("An Unknown error occured");
  JSON_exception(const std::string &e): err(e) {}
  virtual const char* what() const throw() {
    return (const char*)err.c_str();
  }
  virtual ~JSON_exception() throw() { }
};*/
/////////////////////////////////////////////////

// TODO: - Add = operator for map
//       - Iterator support
//       - Create same copy constructors (as = operators)
//       - Provide == operator
//       - Overload << & >> operator ??
//       - Support strict flag for utf-8 enforcement
//       - Write tests
//       - Run ests using STL algorithms for JSON


// One possible way of design:
enum json_values {
  JSON_UNDEFINED = 0,
  JSON_OBJECT = 1,
  JSON_HASH = 1, // JSON_HASH and JSON_OBJECT are aliases (both have value = 1)
  JSON_ARRAY = 2,
  JSON_INTEGER = 3,
  JSON_REAL = 4,
  JSON_STRING = 5,
  JSON_BOOLEAN = 6,
  JSON_NULL = 7
};

// Each class can have a function which returns a JSON*, which basically says, read my self and return a  JSON object ?? 

class Value {
public:
//  virtual const std::string toString() const = 0; // Everybody should implement this function
  virtual const json_values type() const = 0; // Return type of particular dervied class
//  virtual Value* readFromStream(std::ostream& ) = 0; // Read value of whatever type from stream and return *this
  virtual void write(std::ostream &out) const = 0;
  virtual Value* returnMyNewCopy() const = 0;
  virtual void read(std::istream &in) = 0;
  
  /*template <typename T>
  operator T*() {
    std::cout<<"Dynamic castiiiiiiiiiing "<<std::string(typeid(T).name())<<std::endl;
    T *p = dynamic_cast<T*>(this);
    if (p == NULL)
      throw JSON_exception("Illegal conversion from Value* to " + std::string(typeid(T).name()));
    return p;
  }*/

//  friend ostream& operator <<(std::ostream&, const Value&);
//  friend istream& operator >>(istream&, Value &);
};

// Forward declarations
class Integer;
class Real;
class Null;
class Boolean;
class String;
class Array;
class Object;

// Class for top level json
class JSON {
public:
  // Will be exactly one of these 2
  Value *val; // Default: NULL

  // This will be a simple function call now
  //enum json_type type; // Default value if JSON_UNDEFINED

  JSON():val(NULL) {}
  //JSON(std::string) { }
  //JSON(Array);
  //JSON(Object);
  //JSON(Value* v);
  JSON(const JSON &rhs);
  JSON(const json_values &rhs);

  /*template <typename T>
  JSON(const json_values &rhs, T x) {
    switch(rhs) {
      case JSON_ARRAY: val = new Array(x); break;
      case JSON_OBJECT: val = new Object(x); break;
      case JSON_INTEGER: val = new Integer(x); break;
      case JSON_REAL: val = new Real(x); break;
      case JSON_STRING: val = new String(x); break;
      case JSON_BOOLEAN: val = new Boolean(x); break;
      case JSON_NULL: val = new Null(x); break;
      default: throw JSON_exception("Illegal json_values value for JSON initialization");
    }
  }*/

  template<typename T>
  JSON(const T& x);

  void clear() { delete val; val=NULL; }

  void write(std::ostream &out) const;
  void read(std::istream &in);
  
  void parse(const std::string&); // Populate JSON from a string
  std::string stringify() const;
  std::string toString() const;
  // ... similarly for all possible types

  const JSON& operator [](const size_t &indx) const;
  const JSON& operator [](const std::string &s) const;
  const JSON& operator [](const JSON &j) const;
  const JSON& operator [](const char *str) const;

  JSON& operator [](const size_t &indx);
  JSON& operator [](const std::string &s);
  JSON& operator [](const JSON &j);
  JSON& operator [](const char *str);


  // A non-templatized specialization is always preferred over template version
  template<typename T> JSON& operator =(const T &rhs);
  JSON& operator =(const JSON &);
  JSON& operator =(const char &c);
  JSON& operator =(const std::string &s);
  JSON& operator =(const bool &x);
  JSON& operator =(const char s[]);
  JSON& operator =(const Null &x);

  template<typename T>
  JSON& operator =(const std::vector<T> &vec) {
    clear();
    val = new Array(vec);
    return *this;
  }
  
  template<typename T>
  JSON& operator =(const std::map<std::string, T> &m) {
    clear();
    val = new Object(m);
    return *this;
  }

//  template<typename T> 
//  operator T*();

  const json_values type() const { return (val == NULL) ? JSON_UNDEFINED : val->type(); }

  size_t size() const;
  size_t length() const { return size(); }
    // overloading of  [] for strings (object case), and size_t (array case)
  
  // The functions below will throw error if type != JSON_ARRAY
//  size_t length() const;
//  size_t size() const; // should be alias for length, 

  /*template<class T>
  void push_back(const T&) {
    assert(type == JSON_ARRAY);
    Value *v = new T; // If T is String, Number_integer;
  }
  push_back(Number_integer(2));
*/
  void push_back(const JSON &j);
  void erase(const size_t &indx);
  void erase(const std::string &key);
  // create a reference version for push_back

 /* void push_back(const String&);
  void push_back(const Number_integer&);
  void push_back(const Number_double&);
  void push_back(const Boolean&); // Should allow pushing of JSON_TRUE, and JSON_FALSE
  void push_back(const Null&); // Should allow pushing of JSON_NULL
  void push_back(const Array&);
  void push_back(const Object&);
  void removeIndex(size_t index);*/
  ////////////////////////////////////////////////////////////

  // The function below will throw error if type != JSON_OBJECT
//  void removeKey(String);
//  const std::vector<std::string> getAllKeys() const;
//  size_t countKeys() const;
  ////////////////////////////////////////////////////////////

  ~JSON() { clear(); } 
  // TODO: Handle case of streams
};

/*ostream& operator >>(std::ostream& o, const Value& v) {
  v.write(o);
  return o;
}*/

// Implicit copy constructor will suffice for all of the classes below

class Integer: public Value {
public:
  int64 val;

  Integer() {}
  Integer(const int64 &v):val(v) {}
  void write(std::ostream &out) const { out<<val; }
  const json_values type() const { return JSON_INTEGER; }
  size_t returnAsArrayIndex() const { return static_cast<size_t>(val);}
  Value* returnMyNewCopy() const { return new Integer(*this); }
  operator const Value* () { return this; }  
  // read() Should not be called for Integer and Real, 
  // use ReadNumberValue() instead for these two "special" classes
  void read(std::istream &in) { assert(false); }
};

class Real: public Value {
public:
  double val;

  Real() {}
  Real(const double &v):val(v) {}
  void write(std::ostream &out) const { out<<val; }
  const json_values type() const { return JSON_REAL; }
  size_t returnAsArrayIndex() const { return static_cast<size_t>(val);}
  Value* returnMyNewCopy() const { return new Real(*this); }
  
  // read() Should not be called for Integer and Real, 
  // use ReadNumberValue() instead for these two "special" classes
  void read(std::istream &in) { assert(false); }
};

class String: public Value {
public:
  std::string val;
  
  String() {}
  String(const std::string &v):val(v) {}
  // TODO: Make sure cout<<stl::string workes as expected;
  void write(std::ostream &out) const;
  const json_values type() const { return JSON_STRING; }
  std::string returnString() const { return val; }
  Value* returnMyNewCopy() const { return new String(*this); }
  void read(std::istream &in);
  // Should have a constructor which allows creation from std::string directly.
};

class Object: public Value {
public:
  std::map<std::string, JSON> val;
  
  Object() { }
  Object(const Object &rhs): val(rhs.val) {}
  
 /* template<typename T>
 
  // TODO: Fix the map issue (templatizing the iterator)
  Object(const std::map<std::string, T> &v) {
    for (std::map<std::string, T>::const_iterator it = v.begin(); it != v.end(); ++it)
      ;//val[it->first] = *(new JSON(it->second));
  }
*/
  JSON& jsonAtKey(const std::string &s);
  const JSON& jsonAtKey(const std::string &s) const;
  void write(std::ostream &out) const;
  const json_values type() const { return JSON_OBJECT; }
  Value* returnMyNewCopy() const { return new Object(*this); }
  void read(std::istream &in);
  void erase(const std::string &key);
};

class Array: public Value {
public:
  std::vector<JSON> val;
  
  Array() { }
  Array(const Array& arr): val(arr.val) {}

  template<typename T>
  Array(const std::vector<T> &vec) {
    for (unsigned i = 0;i < vec.size(); i++)
      val.push_back(*(new JSON(vec[i])));
  }

  JSON& jsonAtIndex(size_t i);
  const JSON& jsonAtIndex(size_t i) const;
  void write(std::ostream &out) const;
  const json_values type() const { return JSON_ARRAY; }
  Value* returnMyNewCopy() const { return new Array(*this); }
  void read(std::istream &in);
  void push_back(const JSON &j) {
    val.push_back(j);
  }
  void erase(const size_t &i);
};

class Boolean: public Value {
public:
  bool val;
  
  Boolean() {}
  Boolean(const bool &v):val(v) {}
  JSON& jsonAtKey(const std::string &s);
  const JSON& jsonAtKey(const std::string &s) const;
  const json_values type() const { return JSON_BOOLEAN; }
  void write(std::ostream &out) const { out<<((val) ? "true" : "false"); }
  Value* returnMyNewCopy() const { return new Boolean(*this); }
  void read(std::istream &in);
};

class Null: public Value {
public:
  void write(std::ostream &out) const { out<<"null"; }
  const json_values type() const { return JSON_NULL; }
  Value* returnMyNewCopy() const { return new Null(*this); }
  void read(std::istream &in);
};

template<typename T>
JSON::JSON(const T& x) {
  val = NULL; // So that clear() works fine on this, else we will be deallocating some arbitrary memory - dangerous!
  *this = operator=(x);
}

template<typename T>
JSON& JSON::operator =(const T &x) {
  if (!std::numeric_limits<T>::is_specialized)
    throw JSON_exception("Sorry! We do not allow creating a JSON object from " + std::string(typeid(x).name()) + " type.");
  
  clear();
  if(std::numeric_limits<T>::is_integer)
    val = new Integer(static_cast<int64>(x));
  else
    val = new Real(static_cast<double>(x));
  return *this;
}
/*
template<typename T>
JSON::operator T*() {
  T *p = dynamic_cast<T*>(this->val);
  if (p == NULL)
    throw JSON_exception("Illegal conversion from JSON to " + std::string(typeid(T).name()));
  return p;
}*/
#endif

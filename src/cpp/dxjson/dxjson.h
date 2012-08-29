#ifndef __DXJSON_H__
#define __DXJSON_H__

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
#include <cmath>
#include <algorithm>
#include <stdint.h>

#include "utf8/utf8.h"
// NOTE:
// 1) UTF-8 validity is checked while reading JSON from a string;
// 2) UTF-8

/** @file */

/** @namespace dx This is the namespace which contain the JSON and JSONException
                  class
  */
namespace dx {

  /** JSONException class: Inherits from std::exception
    * Error of type JSONException is thrown by dxjson library.
    */
  class JSONException:public std::exception {
  public:
    std::string err;
    JSONException(const std::string &e): err(e) {}
    const char* what() const throw() {
      return (const char*)err.c_str();
    }
    ~JSONException() throw() { }
  };
  
  /** \enum JSONValue Enum for storing type of a particular JSON object.
    * See http://json.org/ for possible JSON values.
    * - JSON_UNDEFINED: Default type of an unintialized JSON object
    * - JSON_HASH: A key/value pair hash.
    * - JSON_OBJECT: Synonym for JSON_HASH (has same numerical value)
    * - JSON_ARRAY: A JSON array
    * - JSON_INTEGER: An integral numeric value. (internally: int64_t)
    * - JSON_REAL: A floating point numeric value. (internally: double)
    * - JSON_STRING: A JSON string.
    * - JSON_BOOLEAN: Either true/false JSON value.
    * - JSON_NULL: The JSON value: "null"
    *
    * To know which type of value a JSON object holds, use the function
    * JSON::type()
    *
    * @note 
    * - JSON_OBJECT and JSON_HASH are synonyms, and we use them interchangebly
    * throughout the documentation and elsewhere.
    * - We treat Integer and Real values differently, which is not in strict
    * accordance with JSON rfc (which just have a single "number" type).
    */
  enum JSONValue {
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

  /** An abstract base class to allow making a heterogenous container
   *  Classes for all possible JSON values are derived from this base class.
   */
  class Value {
  public:
    virtual JSONValue type() const = 0; // Return type of particular derived class
    virtual void write(std::ostream &out) const = 0;
    virtual Value* returnMyNewCopy() const = 0;
    virtual void read(std::istream &in) = 0;
    virtual bool isEqual(const Value* other) const = 0;
    virtual ~Value() { }
  };

  // Forward declarations
  class Integer;
  class Real;
  class Null;
  class Boolean;
  class String;
  class Array;
  class Object;

  /** The JSON class. Object of this class are capable of storing/operating on
    * arbitrary JSON values.
    */
  class JSON {
  public:
    
    typedef std::map<std::string, JSON>::iterator object_iterator;
    typedef std::map<std::string, JSON>::const_iterator const_object_iterator;
    typedef std::vector<JSON>::iterator array_iterator;
    typedef std::vector<JSON>::const_iterator const_array_iterator;

    typedef std::map<std::string, JSON>::const_reverse_iterator object_reverse_iterator;
    typedef std::map<std::string, JSON>::reverse_iterator const_object_reverse_iterator;
    typedef std::vector<JSON>::reverse_iterator array_reverse_iterator;
    typedef std::vector<JSON>::const_reverse_iterator const_array_reverse_iterator;

    /** This will store pointer to actual JSON value
      */
    Value *val;

    /** Returns the current "epsilon" paramerer value.
      * This value determines the "slack" while checking equality
      * of two JSON_REAL values.
      * @return The currrent value of "epsilon" parameter
      * @see json_epsilon
      */
    static double getEpsilon();

    /** Creates a new JSON object from a serialized representation.
      * @param str The serialized json object.
      * @return
      */
    static JSON parse(const std::string &str) {
      JSON tmp;
      tmp.readFromString(str);
      return tmp;
    }

    /** Default constructor for JSON. Creates JSON of type JSON_UNDEFINED.
      */
    JSON():val(NULL) {}

    /** Copy constructor
      * @param rhs This is the JSON object which will be copied.
      */
    JSON(const JSON &rhs);

    /** Construct a blank JSON object of a particular JSONValue type, i.e.,
      * (this->type() == rhs) after construction.
      */
    JSON(const JSONValue &rhs);

    /** This constructor copy the parameter x's value using operator=().
      * @throw JSONException If no suitable operator=() implementation found for copying
      * from a particular type.
      * @param x The new JSON object will be constructed from this value.
      */
    template<typename T>
    JSON(const T& x);

    /** Clears the content of JSON object. 
      * this->type() == JSON_UNDEFINED after the call*/
    void clear() { delete val; val=NULL; }

    /** Writes the serialized JSON object to the output stream
      * @param out Output stream object, to which the serialized object will be written to
      * @throw JSONException
      * @see read()
      * @see toString()
      */
    void write(std::ostream &out) const;

    /** Reads and populates current JSON object from specified input stream
      * containing a valid serialized represntation of JSON value.
      * @note 
      * - Previous contents will be cleared.
      * - It's not necessary that the JSON value being read is serialization of
      * an array or hash only, i.e., serialization of string, number, boolean, 
      * and null can be read as well.
      * - JSON value started by first character of stream is read into object
      *   and once the value finishes, rest of the stream's content is 
      *   not looked into. For example:
      *  - If stream contains: '{"hello": 12}blah', the function will read a 
      *   JSON_OBJECT({"hello": 12}) and return (without looking ahead at 'blah')
      *  - If stream contains: '12Hello', then function will read a
      *    JSON_INTEGER (12) and return (without looking ahead at 'Hello')
      *  - If stream contains: 'null12', then function will read a 
      *    JSON_NULL and return (without looking ahead at '12')
      *  - If stream contains: 'truenull' then function will read a 
      *    JSON_BOOLEAN(true) (without looking ahead at 'null').
      *  - If stream contains: '"hello"World' then function will read
      *    a JSON_STRING("hello") (without looking at 'World')
      *  - If stream contains: 'truHtrue' then function will throw an
      *    error, since characters 'truHtrue' do not represent any legal
      *    JSON value starting from first location.
      * @param in Input stream object (for reading the serialized JSON)
      * @throw JSONException If string being read is illegal JSON, or error
      * occured while reading the stream.
      * @see write()
      * @see readFromString()
      */
    void read(std::istream &in);

    /** Populates current JSON object from the given stringified json value.
      * See all the notes in read() documentation (applies here as well).
      * @param jstr String reprenting a valid JSON object
      * @exception JSONException If parameter jstr contain illegaly formatted JSON
      * @see read()
      * @see toString()
      */
    void readFromString(const std::string &jstr); // Populate JSON from a string

    /** Returns the stringified representation of JSON object.
      * @param onlyTopLevel If set to true, then only JSON objects of type JSON_OBJECT
      *                     or JSON_ARRAY can call this function.
      * @note If called on a JSON_STRING object directly,
      *       the string would be enclosed in quotes.
      * @return A string reprentation of current JSON object.
      * @see write()
      * @see readFromString()
      */
    std::string toString(bool onlyTopLevel = false) const;

    /** Equality comparison operator. Returns true if two JSON objects are equal.
      * A deep matching of JSON object is performed.
      * @note JSON_UNDEFINED != JSON_UNDEFINED
      * @param other The JSON object to which current object will be compared.
      * @return true if both objects are same, else false.
      * @see operator!=()
      */
    bool operator ==(const JSON& other) const;

    /** Inequality comparison operator.
      * @param other The JSON object to which current object will be compared.
      * @return false if *this == other, else true.
      * @see operator==()
      */
    bool operator !=(const JSON& other) const { return !(*this == other); }

    /** Access value stored inside a JSON array by numeric index
      * @param indx Index location to be accessed inside current JSON array.
      * @return A constant reference to JSON value stored at given location
      * @throw JSONException if indx is out of bounds or this->type() != JSON_ARRAY
      */
    const JSON& operator [](const size_t &indx) const;

    /** Access value stored inside JSON object by it's key
      * @param s A pre-existing Key inside current object
      * @note The parameter "s" will be treated as serialized JSON string
      * @return A constant reference to JSON value stored under given Key
      * @throw JSONException if key does not exist or this->type() != JSON_HASH
      */
    const JSON& operator [](const std::string &s) const;

    /** Access a value inside a JSON object/array, and indexed by the provided JSON.
      * The given JSON object (parameter j) must be numeric (if this->type() == JSON_ARRAY)
      * or string (if this->type() == JSON_OBJECT).
      * @param j This JSON value will be used to index the current JSON object.
      * @return A constant reference to JSON value stored at given index.
      * @throw JSONException if conditions specified in descriptions are not met, or if
      *            the referenced property/index does not exist.
      */
    const JSON& operator [](const JSON &j) const;

    /** Same as const JSON& operator[](const std::string &s), just that c style string
      * "str" is converted to std::string, before calling it.
      * @note The parameter "str" will be treated as serialized JSON string
      * @param str C style string, representing a key inside the object.
      * @return A constant reference to JSON value stored under given key.
      */
    const JSON& operator [](const char *str) const;

    /** This function take care of all possible numeric types used for referencing JSON array.
      * @param x A numeric value (specialized under std::numeric_limits). It is typecasted to
      *          size_t before using it as array index.
      * @throw JSONException If parameter x cannot be converted to a numeric type
      * or this->type() != JSON_ARRAY.
      * @return A constant reference to JSON value stored under given index.
      */
    template<typename T>
    const JSON& operator [](const T&x) const;

    /** A non-constant version of const JSON& operator[](const size_t &indx)
      * Returns a non-constant reference, and the value can modifed.
      */
    JSON& operator [](const size_t &indx);

    /** A non-constant version of const JSON& operator[](const std::string &indx)
      * Returns a non-constant reference, and the value can modifed.
      * @note If the specified key (parameter s) is not present, then it will be created
      *       and it's initial value will be set to JSON_UNDEFINED
      * @note The parameter "s" will be treated as serialized JSON string
      */
    JSON& operator [](const std::string &s);

    /** A non-constant version of const JSON& operator[](const JSON &indx)
      * Returns a non-constant reference, and the value can modifed.
      */
    JSON& operator [](const JSON &j);

    /** Same as JSON& operator[](const std::string &s), just that c style string
      * "str" is converted to std::string, before calling it.
      * @note The parameter "str" will be treated as serialized JSON string
      * @param str C style string, representing a key inside the object.
      * @return A constant reference to JSON value stored under given key.
      */
    JSON& operator [](const char *str);

    /** A non-constant version of const JSON& operator[](const JSON &indx)
      * Returns a non-constant reference, and the value can modifed.
      */
    template<typename T>
    JSON& operator [](const T& x) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[x]); }

    /** Sets the current JSON object's value to the provided numeric value.
      * This is a templatized version, specialized only for numeric types.
      * @note Current value in object will be erased.
      * @note The type of JSON object (JSON_INTEGER or JSON_REAL) will be determined by
      *       the type of value provided.
      * @param rhs The value which will be copied to current object.
      * @return Reference to current object (to allow chaining of = operations).
      */
    template<typename T> JSON& operator =(const T &rhs);

    /** Copies the provided JSON object's value to current JSON object.
      * @note Current value of object will be erased.
      * @param rhs The value which will be copied to current object.
      * @return Reference to current object (to allow chaining of = operations).
      */
    JSON& operator =(const JSON &);

    /** Copies the provided character value to current JSON object (as a JSON_STRING)
      * @note Current value of object will be erased.
      * @param rhs The value which will be copied to current object.
      * @return Reference to current object (to allow chaining of = operations).
      */
    JSON& operator =(const char &c);

    /** Copies the provided std::string value to current JSON object (as a JSON_STRING)
      * @note Current value of object will be erased.
      * @param rhs The value which will be copied to current object.
      * @return Reference to current object (to allow chaining of = operations).
      */
    JSON& operator =(const std::string &s);

    /** Copies the provided boolean value to current JSON object (as a JSON_BOOLEAN)
      * @note Current value of object will be erased.
      * @param rhs The value which will be copied to current object.
      * @return Reference to current object (to allow chaining of = operations).
      */
    JSON& operator =(const bool &x);

    /** Copies the provided char* value to current JSON object (as a JSON_STRING)
      * @note Current value of object will be erased.
      * @param rhs The value which will be copied to current object (a C-style string)
      * @return Reference to current object (to allow chaining of = operations).
      */
    JSON& operator =(const char s[]);

    /** Copies the provided Null object's value to current JSON object (as a JSON_NULL).
      * @note Current value of object will be erased.
      * @param rhs The value which will be copied to current object.
      * @return Reference to current object (to allow chaining of = operations).
      */
    JSON& operator =(const Null &x);

    /** Copies the provided std::vector value to current JSON object (as a JSON_ARRAY)
      * @note Current value of object will be erased.
      * @param rhs The value which will be copied to current object.
      * @return Reference to current object (to allow chaining of = operations).
      */
    template<typename T>
    JSON& operator =(const std::vector<T> &vec);

    /** Copies the provided std::map value to current JSON object (as a JSON_OBJECT)
      * @note Current value of object will be erased.
      * @param rhs The value which will be copied to current object.
      * @return Reference to current object (to allow chaining of = operations).
      */
    template<typename T>
    JSON& operator =(const std::map<std::string, T> &m);

    /** Conversion operator. Currently only typecasting to a numeric type (real/integer/bool)
      * is supported.
      * @return The typecasted value of JSON object in requested type.
      */
    template<typename T>
    operator T() const;

    /** Provide conversion functionality from JSON object to some fundamental data types.
      * This generic version works only for numeric types (and has same effect as overloaded
      * conversion operator). A specialization for std::string exist too.
      * @return Value of JSON object in desired fundamental type
      * @throw JSONException If no conversion from JSON object to required type is possible.
      */
    template<typename T>
    T get() const {
      // Reuses the conversion operator: operator T()
      return static_cast<T>(*this);
    }

    /** Returns the type of current JSON object.
      * @return Type (a variable of type enum JSONValue) of current JSON object.
      */
    JSONValue type() const { return (val == NULL) ? JSON_UNDEFINED : val->type(); }

    /** Resizes an JSON_ARRAY.
      * If current size of array = curr_size, and desired size provided by user = desired_size, then
      *
      * Case curr_size < desired_size: (desired_size - curr_size) number of JSON_UNDEFINED elements
      * are pushed at the end of JSON_ARRAY object.
      *
      * Case curr_size > desired_size: Last (curr_size - desired_size) number of JSON elements
      * are deleted from array
      *
      * Case curr_size == desired_size: No effect
      *
      * After executing this function, size() == desired_size
      * @param desired_size The desired size of new array
      */
    void resize_array(size_t desired_size);

    /** Returns total number of element in a JSON_ARRAY or JSON_HASH.
      * @throw JSONException If called on a object which is not of type JSON_ARRAY
      * or JSON_HASH
      *
      * @return Total number of keys (if a JSON_OBJECT), and total number of values (if JSON_ARRAY).
      * @see length()
      */
    size_t size() const;

    /** Exactly same as size()
      * @see size()
      */
    size_t length() const { return size(); }

    /** Returns true if the given numeric value represent a valid index in current JSON_ARRAY.
      * @throw JSONException If called for non JSON_ARRAY object.
      * @param indx the value representing an index inside array.
      * @return true if "indx" is a valid location in array, else false.
      */
    template<typename T>
    bool has(const T &indx) const { return has(static_cast<size_t>(indx)); }

    /** Returns true if the given size_t value represent a valid index in current JSON_ARRAY.
      * @throw JSONException If called for non JSON_ARRAY object.
      * @param indx the value representing an index inside array.
      * @return true if "indx" is a valid location in array, else false.
      */
    bool has(const size_t &indx) const;

    /** Returns true if the given std::string key represent a valid key in current JSON_OBJECT.
      * @throw JSONException If called for non JSON_HASH object.
      * @note The parameter "key" will be treated as serialized JSON string
      * @param key the value to be looked for inside current JSON_OBJECT.
      * @return true if is a valid key, else false.
      */
    bool has(const std::string &key) const;

    /** Allow using a JSON object for has() - converted to a suitable type before executing.
      * @throw JSONException If suitable conversion of input parameter is not possible.
      */
    bool has(const JSON &j) const;

    /** Exactly same behavior as has(const std::string &key) function. The given C-string
      * is converted to std::string before executing the function.
      * @throw JSONException If called for non JSON_HASH object.
      * @note The parameter "key" will be treated as serialized JSON string
      */
    bool has(const char *key) const;

    /** Appends a JSON value at end of current JSON_ARRAY object.
      * @throw JSONException If called for non JSON_ARRAY object.
      * @param j The value to be appended to array.
      */
    void push_back(const JSON &j);

    /** Removes a particular index inside a JSON_ARRAY
      * @throw JSONException If called for non JSON_ARRAY object.
      * @param indx The index to be removed from array
      */
    void erase(const size_t &indx);

    /** Removes a particular key and it's associated value inside a JSON_HASH
      * @throw JSONException If called for non JSON_HASH object.
      * @note The parameter "key" will be treated as serialized JSON string
      * @param key The key value to be removed from object.
      */
    void erase(const std::string &key);

    /** Returns iterator to beginning.
      * @throw JSONException If called on an object which is not a JSON_HASH
      * @return A const iterator to the first element in the container.
      */
    const_object_iterator object_begin() const;

    /** Returns iterator to beginning.
      * @throw JSONException If called on an object which is not a JSON_HASH
      * @return An iterator to the first element in the container.
      */
    object_iterator object_begin();
    
    /** Returns iterator to beginning.
      * @throw JSONException If called on an object which is not a JSON_ARRAY
      * @return A const iterator to the first element in the container.
      */ 
    const_array_iterator array_begin() const;
    
    /** Returns iterator to beginning.
      * @throw JSONException If called on an object which is not a JSON_ARRAY
      * @return An iterator to the first element in the container.
      */ 
    array_iterator array_begin();
    
    /** Returns iterator to end.
      * @throw JSONException If called on an object which is not a JSON_HASH
      * @return A const iterator to the element past-the-end of the container.
      */
    const_object_iterator object_end() const;
    
    /** Returns iterator to end.
      * @throw JSONException If called on an object which is not a JSON_HASH
      * @return An iterator to the element past-the-end of the container.
      */
    object_iterator object_end();
    
    /** Returns iterator to end.
      * @throw JSONException If called on an object which is not a JSON_ARRAY
      * @return A const iterator to the element past-the-end of the container.
      */   
    const_array_iterator array_end() const;
    
    /** Returns iterator to end.
      * @throw JSONException If called on an object which is not a JSON_ARRAY
      * @return An iterator to the element past-the-end of the container.
      */   
    array_iterator array_end();
    
    /** Returns reverse iterator to reverse beginning.
      * @throw JSONException If called on an object which not a JSON_ARRAY
      * @retrun A reverse iterator referring to the last element in the container.
      */
    const_array_reverse_iterator array_rbegin() const;
    
    /** Returns reverse iterator to reverse beginning.
      * @throw JSONException If called on an object which not a JSON_ARRAY
      * @retrun A const reverse iterator referring to the last element in the container.
      */
    array_reverse_iterator array_rbegin();
    
    /** Returns reverse iterator to reverse end.
      * @throw JSONException If called on an object which not a JSON_ARRAY
      * @retrun A const reverse iterator referring to the element right before the
      * first element (which is considered to be the reverse end)
      */  
    const_array_reverse_iterator array_rend() const;
    
    /** Returns reverse iterator to reverse end.
      * @throw JSONException If called on an object which not a JSON_ARRAY
      * @retrun A reverse iterator referring to the element right before the
      * first element (which is considered to be the reverse end)
      */  
    array_reverse_iterator array_rend();

    /** Erases and deallocate any memory for the current JSON object */
    ~JSON() { clear(); }
  };

  class Integer: public Value {
  public:
    int64_t val;

    Integer() {}
    Integer(const int64_t &v):val(v) {}
    void write(std::ostream &out) const { out << val; }
    JSONValue type() const { return JSON_INTEGER; }
    size_t returnAsArrayIndex() const { return static_cast<size_t>(val);}
    Value* returnMyNewCopy() const { return new Integer(*this); }
    operator const Value* () { return this; }
    bool isEqual(const Value *other) const;
    bool operator ==(const Integer& other) const { return isEqual(&other); }
    bool operator !=(const Integer &other) const { return !(*this == other); }

    // read() Should not be called for Integer and Real,
    // use ReadNumberValue() instead for these two "special" classes
    void read(std::istream &in __attribute__ ((unused)) ) { assert(false); }
  };

  class Real: public Value {
  public:
    double val;

    Real() {}
    Real(const double &v):val(v) {}
    void write(std::ostream &out) const { out << val; }
    JSONValue type() const { return JSON_REAL; }
    size_t returnAsArrayIndex() const { return static_cast<size_t>(val);}
    Value* returnMyNewCopy() const { return new Real(*this); }
    bool isEqual(const Value *other) const;
    bool operator ==(const Real& other) const { return isEqual(&other); }
    bool operator !=(const Real& other) const { return !(*this == other); }

    // read() Should not be called for Integer and Real,
    // use ReadNumberValue() instead for these two "special" classes
    void read(std::istream &in __attribute__ ((unused)) ) { assert(false); }
  };

  class String: public Value {
  public:
    std::string val;

    String() {}
    String(const std::string &v):val(v) {}
    // TODO: Make sure cout << stl::string works as expected;
    void write(std::ostream &out) const;
    JSONValue type() const { return JSON_STRING; }
    std::string returnString() const { return val; }
    Value* returnMyNewCopy() const { return new String(*this); }
    void read(std::istream &in);
    bool isEqual(const Value *other) const;
    bool operator ==(const String& other) const { return isEqual(&other); }
    bool operator !=(const String& other) const { return !(*this == other); }

    // Should have a constructor which allows creation from std::string directly.
  };

  class Object: public Value {
  public:
    std::map<std::string, JSON> val;

    Object() { }
    Object(const Object &rhs): val(rhs.val) {}

    template<typename T>
    Object(const std::map<std::string, T> &v) {
      val.insert(v.begin(), v.end());
    }

    JSON& jsonAtKey(const std::string &s);
    const JSON& jsonAtKey(const std::string &s) const;
    void write(std::ostream &out) const;
    JSONValue type() const { return JSON_OBJECT; }
    Value* returnMyNewCopy() const { return new Object(*this); }
    void read(std::istream &in);
    void erase(const std::string &key);
    bool isEqual(const Value *other) const;
    bool operator ==(const Object& other) const { return isEqual(&other); }
    bool operator !=(const Object& other) const { return !(*this == other); }
  };

  class Array: public Value {
  public:
    std::vector<JSON> val;

    Array() { }
    Array(const Array& arr): val(arr.val) {}

    template<typename T>
    Array(const std::vector<T> &vec) {
      for (unsigned i = 0;i < vec.size(); i++) {
        JSON tmp(vec[i]);
        val.push_back(tmp);
      }
    }

    JSON& jsonAtIndex(size_t i);
    const JSON& jsonAtIndex(size_t i) const;
    void write(std::ostream &out) const;
    JSONValue type() const { return JSON_ARRAY; }
    Value* returnMyNewCopy() const { return new Array(*this); }
    void read(std::istream &in);
    void push_back(const JSON &j) {
      val.push_back(j);
    }
    void erase(const size_t &i);
    bool isEqual(const Value* other) const;
    bool operator ==(const Array& other) const { return isEqual(&other); }
    bool operator !=(const Array& other) const { return !(*this == other); }

  };

  class Boolean: public Value {
  public:
    bool val;

    Boolean() {}
    Boolean(const bool &v):val(v) {}
    JSON& jsonAtKey(const std::string &s);
    const JSON& jsonAtKey(const std::string &s) const;
    JSONValue type() const { return JSON_BOOLEAN; }
    void write(std::ostream &out) const { out << ((val) ? "true" : "false"); }
    Value* returnMyNewCopy() const { return new Boolean(*this); }
    void read(std::istream &in);
    bool isEqual(const Value* other) const;
    bool operator ==(const Boolean& other) const { return isEqual(&other); }
    bool operator !=(const Boolean& other) const { return !(*this == other); }
  };

  class Null: public Value {
  public:
    void write(std::ostream &out) const { out << "null"; }
    JSONValue type() const { return JSON_NULL; }
    Value* returnMyNewCopy() const { return new Null(*this); }
    void read(std::istream &in);
    bool isEqual(const Value* other) const;
    bool operator ==(const Null& other) const { return isEqual(&other); }
    bool operator !=(const Null& other) const { return !(*this == other); }

  };

  template<typename T>
  JSON::JSON(const T& x) {
    val = NULL; // So that clear() works fine on this, else we will be deallocating some arbitrary memory - dangerous!
    *this = operator=(x);
  }

  template<typename T>
  JSON& JSON::operator =(const T &x) {
    if (!std::numeric_limits<T>::is_specialized)
      throw JSONException("Sorry! We do not allow creating a JSON object from " + std::string(typeid(x).name()) + " type.");

    clear();
    if(std::numeric_limits<T>::is_integer)
      this->val = new Integer(static_cast<int64_t>(x));
    else
      this->val = new Real(static_cast<double>(x));
    return *this;
  }

  template<typename T>
  JSON& JSON::operator =(const std::vector<T> &vec) {
    clear();
    this->val = new Array(vec);
    return *this;
  }

  template<typename T>
  JSON& JSON::operator =(const std::map<std::string, T> &m) {
    clear();
    this->val = new Object(m);
    return *this;
  }

  template<typename T>
  JSON::operator T() const {
    JSONValue typ = this->type();
    if (typ != JSON_INTEGER && typ != JSON_REAL && typ != JSON_BOOLEAN)
      throw JSONException("No typecast available for this JSON object to a Numeric/Boolean type");

    if (!std::numeric_limits<T>::is_specialized)
      throw JSONException("You cannot convert this JSON object to Numeric/Boolean type.");

    switch(typ) {
      case JSON_INTEGER:
        return static_cast<T>( ((Integer*)this->val)->val);
      case JSON_REAL:
        return static_cast<T>( ((Real*)this->val)->val);
      case JSON_BOOLEAN:
        return static_cast<T>( ((Boolean*)this->val)->val);
      default: assert(false); // Should never happen (already checked at top)
    }
  }

  template<typename T>
  const JSON& JSON::operator [](const T&x) const {
    return (*(const_cast<const JSON*>(this)))[static_cast<size_t>(x)];
  }

  /** Return back the string stored inside JSON_STRING object
    * @note The returned string is in C++ style, and not escaped like it would be
    *       if printed using toString() or write() method. For example: a newline will be
    *       present at ASCII 10, and not "\n" inside string. 
    *       Also it will not contain enclosing quotes.
    * @return The C++ style string for contents of JSON_STRING object
    */
  template<>
  inline std::string JSON::get<std::string>() const {
    if (this->type() != JSON_STRING)
      throw JSONException("You cannot use get<std::string>/get<char*> for a non JSON_STRING value");
    return ((String*)this->val)->val;
  }

}

#endif

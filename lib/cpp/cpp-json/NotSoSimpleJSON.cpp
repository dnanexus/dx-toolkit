#include "NotSoSimpleJSON.h"

namespace JSON_Utility 
{ 
  void SkipWhiteSpace(std::istream &in)
  {
    int c;
    do
    {
      c = in.get();
    } while ((c >= 0) && isspace(c));
    if (c >= 0)
      in.unget();
  }
  
  // Does enclose strings in quotes while writing
  void WriteEscapedString(const std::string &s, std::ostream &out)
  {   
    out<<'"';
    for (unsigned i = 0; i < s.length(); ++i) 
    {  
      switch (s[i]) 
      { 
      case '"': 
        out<<"\\\""; 
        break; 
      case '\\': 
        out<<"\\\\"; 
        break; 
      case '\b': 
        out<<"\\b"; 
        break; 
      case '\f': 
        out<<"\\f"; 
        break; 
      case '\n': 
        out<<"\\n"; 
        break; 
      case '\r': 
        out<<"\\r"; 
        break; 
      case '\t': 
        out<<"\\t"; 
        break; 
      default: 
        out<<s[i]; 
      } 
    } 
    out<<'"';
  }

  // Returns true if "ch" represent start of a number token in JSON
  bool isNumberStart(int ch) {
    if (ch == '+' || ch == '-' || isdigit(ch) || ch == '.')
      return true;
    return false;
  }

  bool isNullStart(int ch) {
    return (ch == 'n');
  }

  bool isStringStart(int ch) {
    return (ch == '\"');
  }

  bool isBooleanStart(int ch) {
    return (ch == 't' || ch == 'f');
  }
  
  bool isArrayStart(int ch) {
    return (ch == '[');
  }

  bool isObjectStart(int ch) {
    return (ch == '{');
  }
  
  Value* ReadNumberValue(std::istream &in) {
    // TODO: Validate numbers more strictly.
    // Currently we parse whatever we get as number (using standard iostream approach)
    // so a 'number' "12--323" will be parsed as 12
    // Ideally. we should throw error on illegal input

    Value *toReturn = NULL;
    std::string toParse = "";
    int ch;
    bool isDouble = false; // By default the number is integer, unless set otherwise
    do {
      ch = in.get();
      if (in.eof())
        break;

      // Currently allow all ., -, +,digit,e,E, as valid characters
      if (isdigit(ch) || ch == '+' || ch == '-') // All integer characters
        toParse += ch;
      else {
        if(ch == '.' || ch == 'e' || ch == 'E') { // All floating point characters
          isDouble = true;
          toParse += ch;
        }
        else // If none of these valid characters, then input is over. Push last character back
        {
          in.unget();
          break;
        }
      }
    }while(true);
    
    if(toParse.length() == 0)
      throw JSON_exception("Invalid number. Unable to parse");

    std::stringstream stream(toParse);
    if (isDouble) {
      Real *r = new Real();
      stream>>r->val;
      toReturn = dynamic_cast<Value*>(r);
    }
    else {
      Integer *i = new Integer();
      stream>>i->val;
      toReturn = dynamic_cast<Value*>(i);
    }
    assert(toReturn != NULL); // To check that dynamic_cast was succesful
    return toReturn;
  }

  void ReadJSONValue(std::istream &in, JSON &j, bool topLevel = false) {
    j.clear();
    JSON_Utility::SkipWhiteSpace(in);

    if (in.eof())
      throw JSON_exception("Unexpected EOF");
    
    int ch = in.get();
    in.unget();
    if (isObjectStart(ch))
      j.val = new Object();
    
    if (isArrayStart(ch))
      j.val = new Array();
    
    // If it's not an object or array, throw error if it was supposed to be a top-level object
    if (topLevel && j.val == NULL)
      throw JSON_exception("JSON::read() - Expected top level JSON to be an Object OR Array");

    if (isStringStart(ch))
      j.val = new String();
    
    if (isBooleanStart(ch))
      j.val = new Boolean();
    
    if (isNullStart(ch))
      j.val = new Null();
    
    if (j.val != NULL)
      j.val->read(in);
    else {
      // Treat number case slightly differently - since there can be two different types of numbers
      if (isNumberStart(ch))
        j.val = JSON_Utility::ReadNumberValue(in);
      else
        throw JSON_exception("Illegal JSON value. Cannot start with : " + std::string(1, char(ch)));
    }
  }

  std::string appendUTF8(int x) {
    char tmp[5] = {0};
    utf8::append(x, tmp);
    return std::string(tmp);
  }
  // See this function in utf8 namespace to fix invalid utf8 characters
  // void fix_utf8_string(std::string& str);
  
  inline int hexdigit_to_num(char ch) {
    if (ch >= '0' && ch <= '9')
      return int(ch - '0');
    ch = toupper(ch);
    if (ch >= 'A' && ch <= 'F')
      return int(ch - 'A' + 10);
    throw JSON_exception("Invalid Hex digit: " + std::string(1,ch));
  }

  // This function assumes, int is at least 32 bit long
  inline int string4_to_hex(char str[]) {
    // We assume that str is always exactly 4 character long
    return ( (hexdigit_to_num(str[0]) << 12) +
             (hexdigit_to_num(str[1]) << 8)  +
             (hexdigit_to_num(str[2]) << 4) + 
             (hexdigit_to_num(str[3])) );
  }

  std::string ReadString(std::istream &in) {
    std::string out = "";
    int ch = in.get();
    assert(ch == '"'); // First character in a string should be quote
    do {
      ch = in.get();
      if (in.eof() || in.fail())
        throw JSON_exception("Unexpected EOF while reading string");
      
      if (ch ==  '"') // String is over
        break;

      if (ch == '\\') {
        ch = in.get();
        if (in.eof() || in.fail())
          throw JSON_exception("Unexpected EOF while reading string");
        
        char hex[4];
        switch(ch) {
          case '"': out += '"';   break;
          case '\\': out += '\\'; break;
          case '/': out += '/';   break;
          case 'b': out += '\b';  break;
          case 'f': out += '\f';  break;
          case 'n': out += '\n';  break;
          case 'r': out += '\r';  break;
          case 't': out += '\t';  break;
          
          case 'u': 
            in.read(hex, 4);
            if(in.eof() || in.gcount() != 4u)
              throw JSON_exception("Expected exactly 4 hex digits after \\u");
            out += appendUTF8(string4_to_hex(hex));
            break;
          default:
            throw JSON_exception("Illegal escape sequence: \\" + std::string(1, ch));
        }
      }
      else
        out += char(ch);
    }while(true);
    return out;
  }
}

void String::write(std::ostream &out) const {
  JSON_Utility::WriteEscapedString(this->val, out);
}

void Object::write(std::ostream &out) const {
  out<<"{";
  bool firstElem = true;

  for (std::map<std::string, JSON>::const_iterator it = val.begin(); it != val.end(); ++it, firstElem = false) {
    if (!firstElem)
      out<<", ";
    JSON_Utility::WriteEscapedString((*it).first, out);
    out<<": ";
    (*it).second.write(out);
  }
  out<<"}"; 
}

// TODO: Figure out how to make [] work on constant members too (i.e. constant functions,
//       but still returning a reference
void Array::write(std::ostream &out) const {
  out<<"[";
  bool firstElem = true;
  for(unsigned i = 0; i < val.size(); ++i, firstElem = false) {
    if (!firstElem)
      out<<", ";
    val[i].write(out);
  }
  out<<"]";
}

const JSON& Array::jsonAtIndex(size_t i) const {
  if (val.size() == 0u || i >= val.size())
    throw JSON_exception("Illegal: Out of bound JSON_ARRAY access");
  return val[i];
}

JSON& Array::jsonAtIndex(size_t i) {
  if (val.size() == 0u || i >= val.size())
    throw JSON_exception("Illegal: Out of bound JSON_ARRAY access");
  return val[i];
}

// STL map's [] operator cannot be used on constant objects
const JSON& Object::jsonAtKey(const std::string &s) const {
  std::map<std::string, JSON>::const_iterator it = val.find(s);
  if (it == val.end())
    throw JSON_exception("Cannot add new key to a constant JSON_OBJECT");
  return it->second;
}

JSON& Object::jsonAtKey(const std::string &s) {
  // Unlike array, create new json object for particualr key if previously didn't exist
  return val[s];
}
void JSON::write(std::ostream &out) const {
  if (this->type() == JSON_UNDEFINED) {
    throw JSON_exception("Cannot call write() method on uninitialized json object");
  }
  out.precision(std::numeric_limits<double>::digits10);
  val->write(out);
  out.flush();
}

void JSON::parse(const std::string &jstr) {
  std::stringstream inp(jstr);
  this->read(inp);
}

// TODO: Dynamic casts can be turned into static casts for efficiency reasons
// TODO: Create const version for each of them
const JSON& JSON::operator[](const std::string &s) const {
  if (this->type() != JSON_OBJECT)
    throw JSON_exception("Cannot use string to index value of a non-JSON_OBJECT using [] operator");
  Object *o = dynamic_cast<Object*>(val);
  assert(o != NULL);
  return o->jsonAtKey(s);
}

const JSON& JSON::operator[](const char *str) const {
  return operator[](std::string(str));
}

const JSON& JSON::operator[](const size_t &indx) const {
  if (this->type() != JSON_ARRAY)
    throw JSON_exception("Cannot use integer to index value of non-JSON_ARRAY using [] operator");
  Array *a = dynamic_cast<Array*>(val);
  assert(a != NULL); 
  return a->jsonAtIndex(indx);
}

const JSON& JSON::operator[](const JSON &j) const {
  if (this->type() == JSON_ARRAY) {
    size_t i;
    Integer *ptr1;
    Real *ptr2;
    switch(j.type()) {
      case JSON_INTEGER:
        ptr1 = dynamic_cast<Integer*>(j.val);
        assert(ptr1 != NULL);
        i = static_cast<size_t>(ptr1->val);
        break;

      case JSON_REAL:
        ptr2 = dynamic_cast<Real*>(j.val);
        assert(ptr2 != NULL);
        i = static_cast<size_t>(ptr2->val);
        break;

      default: throw JSON_exception("Cannot use an non-numeric value to index JSON_ARRAY using []");
    }
    return (*this)[i];
  }
  if (this->type() == JSON_OBJECT) {
    if(j.type() != JSON_STRING)
      throw JSON_exception("Cannot use a non-string value to index JSON_OBJECT using []");

    String *ptr = dynamic_cast<String*>(j.val);
    assert(ptr != NULL);
    return (*this)[ptr->val];
  }
  throw JSON_exception("Only JSON_OBJECT and JSON_ARRAY can be indexed using []");
}

// A dirty hack for creating non-const versions of [] overload using const versions above
JSON& JSON::operator [](const size_t &indx) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[indx]); }
JSON& JSON::operator [](const std::string &s) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[s]); }
JSON& JSON::operator [](const JSON &j) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[j]); }
JSON& JSON::operator [](const char *str) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[str]); }


/*
JSON::JSON(Value *v) {
  if (v != NULL) {
    val = v->returnMyNewCopy();
  }
  else
    val = NULL;
}
*/

JSON::JSON(const json_values &rhs) {
  switch(rhs) {
    case JSON_ARRAY: val = new Array(); break;
    case JSON_OBJECT: val = new Object(); break;
    case JSON_INTEGER: val = new Integer(); break;
    case JSON_REAL: val = new Real(); break;
    case JSON_STRING: val = new String(); break;
    case JSON_BOOLEAN: val = new Boolean(); break;
    case JSON_NULL: val = new Null(); break;
    default: throw JSON_exception("Illegal json_values value for JSON initialization");
  }
}
    
JSON::JSON(const JSON &rhs) {
  if(rhs.type() != JSON_UNDEFINED)
    val = rhs.val->returnMyNewCopy();
  else
    val = NULL;
}
/*
JSON::JSON(Integer rhs) {
  this->val = rhs.returnMyNewCopy();
}
*/
JSON& JSON::operator =(const JSON &rhs) {
  if (this == &rhs) // Self-assignment check
    return *this;
  
  clear();

  if (rhs.type() != JSON_UNDEFINED)
    val = rhs.val->returnMyNewCopy();
  else
    val = NULL;
  
  return *this;
}

JSON& JSON::operator =(const std::string &s) {
  clear();
  val = new String(s);
  return *this;
}

JSON& JSON::operator =(const char &c) {
  return operator=(std::string(1u, c));
}

JSON& JSON::operator =(const bool &x) {
  clear();
  val = new Boolean(x);
  return *this;
}

JSON& JSON::operator =(const Null &x) {
  clear();
  val = new Null();
  return *this;
}

JSON& JSON::operator =(const char s[]) {
  return operator =(std::string(s));
}


size_t JSON::size() const {
  json_values t = type();
  if (t != JSON_ARRAY && t != JSON_OBJECT && t != JSON_STRING)
    throw JSON_exception("size()/length() can only be called for JSON_ARRAY/JSON_OBJECT/JSON_STRING");
  if(t == JSON_ARRAY) {
    Array *tmp = dynamic_cast<Array*>(this->val);
    assert(tmp != NULL);
    return tmp->val.size();
  }
  else {
    if (t == JSON_OBJECT) {
      Object *tmp = dynamic_cast<Object*>(this->val);
      assert(tmp != NULL);
      return tmp->val.size();
    }
  }
  String *tmp = dynamic_cast<String*>(this->val);
  assert(tmp != NULL);
  return tmp->val.size();
}

void JSON::push_back(const JSON &j) {
  if (this->type() != JSON_ARRAY)
    throw JSON_exception("Cannot push_back to a non-array");
  Array *tmp = dynamic_cast<Array*>(this->val);
  assert(tmp != NULL);
  tmp->push_back(j);
}

// TODO: Decide on what to do with this. probably add a topLevel flag (default: false)
std::string JSON::stringify() const {
  if (this->type() != JSON_OBJECT && this->type() != JSON_ARRAY)
    throw JSON_exception("Only a JSON_OBJECT/JSON_ARRAY can be stringified");
  
  return this->toString();
}

std::string JSON::toString() const {
  std::stringstream in;
  write(in);
  return in.str();
}

void JSON::read(std::istream &in) {
  JSON_Utility::ReadJSONValue(in, *this, true);
}

void JSON::erase(const size_t &indx) {
  if (this->type() != JSON_ARRAY)
    throw JSON_exception("erase(size_t) can only be called for a JSON_ARRAY");
  (dynamic_cast<Array*>(this->val))->erase(indx);
}

void JSON::erase(const std::string &indx) {
  if (this->type() != JSON_OBJECT)
    throw JSON_exception("erase(string) can only be called for a JSON_OBJECT");
  (dynamic_cast<Object*>(this->val))->erase(indx);
}

void String::read(std::istream &in) {
  val = JSON_Utility::ReadString(in);
}

void Boolean::read(std::istream &in) {
  
  // To store output of read() - maximum = "false", artificially adding "\0" at end 
  // since read() does not.
  char str[6] = {0};

  str[0] = in.get();
  bool fail = false;
  unsigned size = 0;
  if (str[0] == 't')
    size = 3; // read "rue"
  else
    size = 4; // read "alse"

  in.read(&str[1], size);
  fail = (in.gcount() == size) ? false : true;
  if (!fail && size == 3u && (strcmp(str, "true") == 0))
    val = true;
  else
    if (!fail && size == 4u && (strcmp(str, "false") == 0))
      val = false;
    else
      fail = true;

  if (fail)
    throw JSON_exception("Invalid Boolean value, expected exactly one of : 'true' or 'false'");
}

void Null::read(std::istream &in) {
  char str[5] = {0};
  in.read(str, 4);
  if (in.gcount() !=4 || (strcmp(str, "null") != 0))
    throw JSON_exception("Invalid JSON null, expected exactly: null");
}

void Object::read(std::istream &in) {
  int ch;
  val.clear();
  JSON_Utility::SkipWhiteSpace(in);
  ch = in.get();
  assert(ch == '{'); // Must be a valid object for Object::read to be called
  
  bool firstKey = true;
  do {
    JSON_Utility::SkipWhiteSpace(in);
    ch = in.get();
    if(in.eof() || in.fail())
      throw JSON_exception("Unexpected EOF while parsing object. ch = " + std::string(1,ch));
    
    // End of parsing for this JSON object
    if (ch == '}')
      break;
    
    // Keys:value pairs must be separated by , inside JSON object 
    if (!firstKey && ch != ',')
      throw JSON_exception("Expected , while parsing object. Got : " + std::string(1,char(ch)));
    
    if (!firstKey) {
      JSON_Utility::SkipWhiteSpace(in);
      ch = in.get();
    }

    if (!JSON_Utility::isStringStart(ch))
      throw JSON_exception("Expected start of a valid object key (string) at this location");

    // Push back the quote (") in stream again, and parse the key value (string)
    in.unget();
    
    std::string key = JSON_Utility::ReadString(in);
    JSON_Utility::SkipWhiteSpace(in);
    ch = in.get();
    if (ch != ':')
      throw JSON_exception("Expected :, got : " + std::string(1,ch));
    JSON_Utility::SkipWhiteSpace(in);
    JSON_Utility::ReadJSONValue(in, val[key], false);
    firstKey = false;
  } while(true);
}

void Array::read(std::istream &in) {
  int ch;
  val.clear();
  JSON_Utility::SkipWhiteSpace(in);
  ch = in.get();
  assert(ch == '['); // Must be a valid array for Array::read to be called
  
  bool firstKey = true;
  do {
    JSON_Utility::SkipWhiteSpace(in);
    ch = in.get();
    if(in.eof() || in.fail())
      throw JSON_exception("Unexpected EOF while parsing array");
    
    // End of parsing this array
    if (ch == ']')
      break;

    if (!firstKey && ch != ',')
      throw JSON_exception("Expected ,(comma) GOT: " + std::string(1, ch));
    
    if (!firstKey) {
      JSON_Utility::SkipWhiteSpace(in);
    }
    else
      in.unget();

    JSON_Utility::SkipWhiteSpace(in);
    val.push_back(*(new JSON())); // Append a blank json object. We will fill it soon
    JSON_Utility::ReadJSONValue(in, val[val.size() - 1u], false);
    firstKey = false;
  }while(true);
}

void Object::erase(const std::string &key) {
  if(val.erase(key) == 0)
    throw JSON_exception("Cannot erase non-existent key from a JSON_OBJECT. Key supplied = " + key);
}

void Array::erase(const size_t &indx) {
  if(indx >= val.size())
    throw JSON_exception("Cannot erase out of bound element in a JSON_ARRAY. indx supplied = " + indx);
  val.erase(val.begin() + indx);
}

#include "dxjson.h"

using namespace dx;

extern double JSON::epsilon = std::numeric_limits<double>::epsilon();


namespace JSON_Utility 
{ 
  
  std::string itos(int i)  // convert int to string
  {
    std::stringstream s;
    s << i;
    return s.str();
  }

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
  
  // Does not enclose strings in quotes while writing
  void WriteEscapedString(const std::string &s, std::ostream &out, bool enclosingQuotes = true)
  { 
    if (enclosingQuotes)
      out<<'"';

    for (unsigned i = 0; i < s.length(); ++i) 
    { 
      if(s[i] >= 0x0000 && s[i] <= 0x001f) // control character case
      {
        char temphex[5] = {0};
        switch(s[i]) {
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
            out<<"\\u";
            sprintf(temphex, "%04x", s[i]);
            out<<std::string(temphex);
            break;
        }
      }
      else {
        switch(s[i]) {
          case '"': 
            out<<"\\\""; 
            break; 
          case '\\': 
            out<<"\\\\"; 
            break; 
          default: 
            out<<s[i]; 
        } 
      }
    }
    if (enclosingQuotes)
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
      throw JSONException("Invalid number. Unable to parse");

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
      throw JSONException("Unexpected EOF");
    
    int ch = in.get();
    in.unget();
    if (isObjectStart(ch))
      j.val = new Object();
    
    if (isArrayStart(ch))
      j.val = new Array();
    
    // If it's not an object or array, throw error if it was supposed to be a top-level object
    if (topLevel && j.val == NULL)
      throw JSONException("JSON::read() - Expected top level JSON to be an Object OR Array");

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
        throw JSONException("Illegal JSON value. Cannot start with : " + std::string(1, char(ch)));
    }
  }

/*  std::string appendUTF8(uint32_t x) {
    std::string temp_str = "";
    //unsigned char tmp[5] = {0};
    utf8::append(x, std::back_inserter(temp_str));
    //return std::string(tmp);
    return temp_str;
  }*/
  // See this function in utf8 namespace to fix invalid utf8 characters
  // void fix_utf8_string(std::string& str);
  
  inline int32_t hexdigit_to_num(char ch) {
    if (ch >= '0' && ch <= '9')
      return uint32_t(ch - '0');
    ch = toupper(ch);
    if (ch >= 'A' && ch <= 'F')
      return uint32_t(ch - 'A' + 10);
    throw JSONException("Invalid Hex digit in unicode escape \\uxxxx: " + std::string(1,ch));
  }

  inline int32_t string4_to_hex(char str[]) {
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
        throw JSONException("Unexpected EOF while reading string");
      
      if (ch ==  '"') // String is over
        break;

      if (ch == '\\') {
        ch = in.get();
        if (in.eof() || in.fail())
          throw JSONException("Unexpected EOF while reading string");
        
        char hex[4];
        char temp_buffer[6];
        int32_t first16bit, second16bit;
        int32_t codepoint;
        char tx; // temporary
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
              throw JSONException("Expected exactly 4 hex digits after \\u");
            first16bit = string4_to_hex(hex);
            /*if(first16bit >= 0x0000 && first16bit <= 0x001f) {
              // Control character case, should be escaped
              // http://stackoverflow.com/questions/4901133/json-and-escaping-characters
              tx = char(first16bit);
              if (tx == '\b' || tx == '\f' || tx == '\n' || tx == '\r' || tx == '\t')
                out += tx;
              else // If it is not one of special characters (above): escape it in hex form
                out += ((((std::string("\\u") + hex[0]) + hex[1]) + hex[2]) + hex[3]);
              break;
            }*/
          
            codepoint = first16bit;
            if(0xD800 <= first16bit && first16bit <= 0xDBFF) {
              // Surrogate pair case
              // Must have next 6 characters of the form: \uxxxx as well
              in.read(temp_buffer, 6);
              if(in.eof() || in.gcount() != 6u || temp_buffer[0] != '\\' || temp_buffer[1] != 'u')
                throw JSONException("Missing surrogate pair in unicode sequence");
              second16bit = string4_to_hex(&temp_buffer[2]);

              if(0xDC00 <= second16bit && second16bit <= 0xDFFF) {
                /* valid second surrogate */
                codepoint = ((first16bit - 0xD800) << 10) + (second16bit - 0xDC00) + 0x10000;
              }
              else {
                // Invalid second surrogate
                throw JSONException("Invalid second 16 bit value in surrogate pair: first 16 bit = " + JSON_Utility::itos(first16bit) + " and second 16 bit = " + JSON_Utility::itos(second16bit));
              }
            }
            try {
              // changing it to utf8::unchecked::append will stop throwing of error
              // in case of invalid utf8 point
              utf8::append(codepoint, back_inserter(out));
            } 
            catch(utf8::invalid_code_point &e) {
                std::string line;
                getline(in, line);
                throw JSONException("Invalid UTF-8 code point found in text. Value = " + itos(codepoint) + ". Location = " + line + "\nInternal message = " + e.what());
            } 
            break;
          default:
            throw JSONException("Illegal escape sequence: \\" + std::string(1, ch));
        }
      }
      else
        out += char(ch);
    }while(true);
    return out;
  }
}

void String::write(std::ostream &out) const {
  JSON_Utility::WriteEscapedString(this->val, out, true);
}

void Object::write(std::ostream &out) const {
  out<<"{";
  bool firstElem = true;

  for (std::map<std::string, JSON>::const_iterator it = val.begin(); it != val.end(); ++it, firstElem = false) {
    if (!firstElem)
      out<<", ";
    JSON_Utility::WriteEscapedString((*it).first, out, true);
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
    throw JSONException("Illegal: Out of bound JSON_ARRAY access");
  return val[i];
}

JSON& Array::jsonAtIndex(size_t i) {
  if (val.size() == 0u || i >= val.size())
    throw JSONException("Illegal: Out of bound JSON_ARRAY access");
  return val[i];
}

// STL map's [] operator cannot be used on constant objects
const JSON& Object::jsonAtKey(const std::string &s) const {
  std::map<std::string, JSON>::const_iterator it = val.find(s);
  if (it == val.end())
    throw JSONException("Cannot add new key to a constant JSON_OBJECT");
  return it->second;
}

JSON& Object::jsonAtKey(const std::string &s) {
  // Unlike array, create new json object for particualr key if previously didn't exist
  return val[s];
}
void JSON::write(std::ostream &out) const {
  if (this->type() == JSON_UNDEFINED) {
    throw JSONException("Cannot call write() method on uninitialized json object");
  }
  out.precision(std::numeric_limits<double>::digits10);
  val->write(out);
  out.flush();
}

void JSON::readFromString(const std::string &jstr) {
  std::stringstream inp(jstr);
  this->read(inp);
}

// TODO: Dynamic casts can be turned into static casts for efficiency reasons
// TODO: Create const version for each of them
const JSON& JSON::operator[](const std::string &s) const {
  if (this->type() != JSON_OBJECT)
    throw JSONException("Cannot use string to index value of a non-JSON_OBJECT using [] operator");
  Object *o = dynamic_cast<Object*>(val);
  assert(o != NULL);
  return o->jsonAtKey(s);
}

const JSON& JSON::operator[](const char *str) const {
  return operator[](std::string(str));
}

const JSON& JSON::operator[](const size_t &indx) const {
  if (this->type() != JSON_ARRAY)
    throw JSONException("Cannot use integer to index value of non-JSON_ARRAY using [] operator");
  Array *a = dynamic_cast<Array*>(val);
  assert(a != NULL); 
  return a->jsonAtIndex(indx);
}

const JSON& JSON::operator[](const JSON &j) const {
  if (this->type() == JSON_ARRAY) {
    return (*this)[size_t(j)];
  }
  if (this->type() == JSON_OBJECT) {
    if(j.type() != JSON_STRING)
      throw JSONException("Cannot use a non-string value to index JSON_OBJECT using []");

    String *ptr = dynamic_cast<String*>(j.val);
    assert(ptr != NULL);
    return (*this)[ptr->val];
    
  }
  throw JSONException("Only JSON_OBJECT and JSON_ARRAY can be indexed using []");
}

// A dirty hack for creating non-const versions of [] overload using const versions above
JSON& JSON::operator [](const size_t &indx) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[indx]); }
JSON& JSON::operator [](const std::string &s) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[s]); }
JSON& JSON::operator [](const JSON &j) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[j]); }
JSON& JSON::operator [](const char *str) { return const_cast<JSON&>( (*(const_cast<const JSON*>(this)))[str]); }

JSON::JSON(const JSONValue &rhs) {
  switch(rhs) {
    case JSON_ARRAY: val = new Array(); break;
    case JSON_OBJECT: val = new Object(); break;
    case JSON_INTEGER: val = new Integer(); break;
    case JSON_REAL: val = new Real(); break;
    case JSON_STRING: val = new String(); break;
    case JSON_BOOLEAN: val = new Boolean(); break;
    case JSON_NULL: val = new Null(); break;
    default: throw JSONException("Illegal JSONValue value for JSON initialization");
  }
}
    
JSON::JSON(const JSON &rhs) {
  if(rhs.type() != JSON_UNDEFINED)
    val = rhs.val->returnMyNewCopy();
  else
    val = NULL;
}

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
  JSONValue t = type();
  if (t != JSON_ARRAY && t != JSON_OBJECT && t != JSON_STRING)
    throw JSONException("size()/length() can only be called for JSON_ARRAY/JSON_OBJECT/JSON_STRING");
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
    throw JSONException("Cannot push_back to a non-array");
  Array *tmp = dynamic_cast<Array*>(this->val);
  assert(tmp != NULL);
  tmp->push_back(j);
}

std::string JSON::toString(bool onlyTopLevel) const {
  if (onlyTopLevel && this->type() != JSON_OBJECT && this->type() != JSON_ARRAY)
    throw JSONException("Only a JSON_OBJECT/JSON_ARRAY can call toString() with onlyTopLevel flag set to true");
  std::stringstream in;
  write(in);
  return in.str();
}

bool JSON::has(const size_t &indx) const {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Illegal call to has(size_t) for non JSON_ARRAY object");
  size_t size = ((Array*)(this->val))->val.size();
  return (indx >= 0u && indx < size);
}

bool JSON::has(const std::string &key) const {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Illegal call to has(size_t) for non JSON_OBJECT object");
  return (((Object*)(this->val))->val.count(key) > 0u);
}

bool JSON::has(const char *x) const {
  return has(std::string(x));
}

bool JSON::has(const JSON &j) const {
  
  switch(this->type()) {
    case JSON_ARRAY: return has((const size_t)j);
    case JSON_OBJECT: 
      if (j.type() != JSON_STRING)
        throw JSONException("For a JSON_OBJECT, has(JSON &j) requires j to be JSON_STRING");
      return has( ((String*)(j.val))->val);
  
    default: throw JSONException("Illegal json object as input to has(const JSON &j)");
  }  
}

void JSON::read(std::istream &in) {
  JSON_Utility::ReadJSONValue(in, *this, false);
}

void JSON::erase(const size_t &indx) {
  if (this->type() != JSON_ARRAY)
    throw JSONException("erase(size_t) can only be called for a JSON_ARRAY");
  (dynamic_cast<Array*>(this->val))->erase(indx);
}

void JSON::erase(const std::string &indx) {
  if (this->type() != JSON_OBJECT)
    throw JSONException("erase(string) can only be called for a JSON_OBJECT");
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
    throw JSONException("Invalid Boolean value, expected exactly one of : 'true' or 'false'");
}

void Null::read(std::istream &in) {
  char str[5] = {0};
  in.read(str, 4);
  if (in.gcount() !=4 || (strcmp(str, "null") != 0))
    throw JSONException("Invalid JSON null, expected exactly: null");
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
      throw JSONException("Unexpected EOF while parsing object. ch = " + std::string(1,ch));
    
    // End of parsing for this JSON object
    if (ch == '}')
      break;
    
    // Keys:value pairs must be separated by , inside JSON object 
    if (!firstKey && ch != ',')
      throw JSONException("Expected , while parsing object. Got : " + std::string(1,char(ch)));
    
    if (!firstKey) {
      JSON_Utility::SkipWhiteSpace(in);
      ch = in.get();
    }

    if (!JSON_Utility::isStringStart(ch))
      throw JSONException("Expected start of a valid object key (string) at this location");

    // Push back the quote (") in stream again, and parse the key value (string)
    in.unget();
    
    std::string key = JSON_Utility::ReadString(in);
    JSON_Utility::SkipWhiteSpace(in);
    ch = in.get();
    if (ch != ':')
      throw JSONException("Expected :, got : " + std::string(1,ch));
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
      throw JSONException("Unexpected EOF while parsing array");
    
    // End of parsing this array
    if (ch == ']')
      break;

    if (!firstKey && ch != ',')
      throw JSONException("Expected ,(comma) GOT: " + std::string(1, ch));
    
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
    throw JSONException("Cannot erase non-existent key from a JSON_OBJECT. Key supplied = " + key);
}

void Array::erase(const size_t &indx) {
  if(indx >= val.size())
    throw JSONException("Cannot erase out of bound element in a JSON_ARRAY. indx supplied = " + indx);
  val.erase(val.begin() + indx);
}

bool Integer::isEqual(const Value *other) const {
  const Integer *p = dynamic_cast<const Integer*>(other);
  return (p != NULL && this->val == p->val);
}

bool Real::isEqual(const Value* other) const {
  const Real *p = dynamic_cast<const Real*>(other);
  return (p != NULL && (fabs(this->val - p->val) <= JSON::getEpsilon()));
}

bool String::isEqual(const Value* other) const {
  const String *p = dynamic_cast<const String*>(other);
  return (p != NULL && this->val == p->val);
}

bool Boolean::isEqual(const Value* other) const {
  const Boolean *p = dynamic_cast<const Boolean*>(other);
  return (p != NULL && this->val == p->val);
}

bool Null::isEqual(const Value* other) const {
  const Null *p = dynamic_cast<const Null*>(other);
  return (p != NULL);
}

bool Array::isEqual(const Value* other) const {
  const Array *p = dynamic_cast<const Array*>(other);
  return (p != NULL && this->val.size() == p->val.size() && equal(this->val.begin(), this->val.end(), p->val.begin()));
}

bool Object::isEqual(const Value* other) const {
  const Object *p = dynamic_cast<const Object*>(other);
  if (p == NULL || this->val.size() != p->val.size())
    return false;
  std::map<std::string, JSON>::const_iterator it1,it2;
  for (it1 = this->val.begin(), it2 = p->val.begin(); it1 != this->val.end() && it2 != p->val.end(); ++it1, ++it2) {
    if (it1->first != it2->first || it1->second != it2->second)
      return false;
  }
  return (it1 == this->val.end() && it2 == p->val.end());
}


bool JSON::operator ==(const JSON& other) const {
  if (this->type() != other.type() || this->type() == JSON_UNDEFINED)
    return false;
  return (this->val->isEqual(other.val));
}

JSON::const_object_iterator JSON::object_begin() const {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Cannot get JSON::object_iterator for a non-JSON_OBJECT");
  return (dynamic_cast<Object*>(this->val))->val.begin();
}

JSON::const_array_iterator JSON::array_begin() const {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Cannot get JSON::array_iterator for a non-JSON_ARRAY");
  return (dynamic_cast<Array*>(this->val))->val.begin();
}

JSON::object_iterator JSON::object_begin() {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Cannot get JSON::object_iterator for a non-JSON_OBJECT");
  return (dynamic_cast<Object*>(this->val))->val.begin();
}

JSON::array_iterator JSON::array_begin() {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Cannot get JSON::array_iterator for a non-JSON_ARRAY");
  return (dynamic_cast<Array*>(this->val))->val.begin();
}

JSON::const_object_iterator JSON::object_end() const {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Cannot get JSON::object_iterator for a non-JSON_OBJECT");
  return (dynamic_cast<Object*>(this->val))->val.end();
}

JSON::const_array_iterator JSON::array_end() const {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Cannot get JSON::array_iterator for a non-JSON_ARRAY");
  return (dynamic_cast<Array*>(this->val))->val.end();
}

JSON::object_iterator JSON::object_end() {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Cannot get JSON::object_iterator for a non-JSON_OBJECT");
  return (dynamic_cast<Object*>(this->val))->val.end();
}

JSON::array_iterator JSON::array_end() {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Cannot get JSON::array_iterator for a non-JSON_ARRAY");
  return (dynamic_cast<Array*>(this->val))->val.end();
}

// Reverse iterators
JSON::const_object_reverse_iterator JSON::object_rbegin() const {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Cannot get JSON::object_reverse_iterator for a non-JSON_OBJECT");
  return (dynamic_cast<Object*>(this->val))->val.rbegin();
}

JSON::const_array_reverse_iterator JSON::array_rbegin() const {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Cannot get JSON::array_reverse_iterator for a non-JSON_ARRAY");
  return (dynamic_cast<Array*>(this->val))->val.rbegin();
}

JSON::object_reverse_iterator JSON::object_rbegin() {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Cannot get JSON::object_reverse_iterator for a non-JSON_OBJECT");
  return (dynamic_cast<Object*>(this->val))->val.rbegin();
}

JSON::array_reverse_iterator JSON::array_rbegin() {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Cannot get JSON::array_reverse_iterator for a non-JSON_ARRAY");
  return (dynamic_cast<Array*>(this->val))->val.rbegin();
}

JSON::const_object_reverse_iterator JSON::object_rend() const {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Cannot get JSON::object_reverse_iterator for a non-JSON_OBJECT");
  return (dynamic_cast<Object*>(this->val))->val.rend();
}

JSON::const_array_reverse_iterator JSON::array_rend() const {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Cannot get JSON::array_reverse_iterator for a non-JSON_ARRAY");
  return (dynamic_cast<Array*>(this->val))->val.rend();
}

JSON::object_reverse_iterator JSON::object_rend() {
  if(this->type() != JSON_OBJECT)
    throw JSONException("Cannot get JSON::object_reverse_iterator for a non-JSON_OBJECT");
  return (dynamic_cast<Object*>(this->val))->val.rend();
}

JSON::array_reverse_iterator JSON::array_rend() {
  if(this->type() != JSON_ARRAY)
    throw JSONException("Cannot get JSON::array_reverse_iterator for a non-JSON_ARRAY");
  return (dynamic_cast<Array*>(this->val))->val.rend();
}

void JSON::resize_array(size_t desired_size) {
  if (this->type() != JSON_ARRAY)
    throw JSONException("Cannot call resize_array() on a non JSON_ARRAY object");
  ((Array*)this->val)->val.resize(desired_size);
}

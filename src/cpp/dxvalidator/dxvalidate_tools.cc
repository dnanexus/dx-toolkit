#include "dxvalidate_tools.h"

using namespace dx;
using namespace std;

void TypesHandler::Add(const JSON &t) {
  types.clear();
  duplicate = false;

  for (int i = 0; i < t.size(); i++) {
    string type = t[i].get<string>();
    if (Has(type)) {
      duplicate = true;
    } else {
      types.insert(type);
    }
  }
}

bool ColumnsHandler::identifyColumn() {
  map<string, string>::iterator it;

  for (int i = 0; i < 3; i ++) {
    it = columnTypes[i].find(cName);
    if (it == columnTypes[i].end()) continue;
  
    queryColumns.push_back(cName);
    
    string t = it->second;
    if (cType == t) return true;
    if ((t == "integer") && integerType()) return true;
    if ((t == "float or double") && floatType()) return true;

    columnLists[2].push_back(cName + " [" + t + "]");
    return true;
  }

  return false;
}

void ColumnsHandler::findMissingColumns() {
  map<string, string>::iterator it;
  for (int i = 0; i < 2; i++) {
    for (it = columnTypes[i].begin(); it != columnTypes[i].end(); it++) {
      string name = it->first;
      if (allColumns.find(name) == allColumns.end()) columnLists[i].push_back(name);
    }
  }
}

void ColumnsHandler::clearColumns() {
  for (int i = 0; i < 3; i++)
    columnTypes[i].clear();

  for (int i = 0; i < 5; i++)
    columnLists[i].clear();
  
  queryColumns.resize_array(0);
  allColumns.clear();
}

ColumnsHandler::ColumnsHandler() {
  intTypes.clear();
  intTypes.insert("uint8"); intTypes.insert("int16"); intTypes.insert("uint16");
  intTypes.insert("int32"); intTypes.insert("uint32"); intTypes.insert("int64");
  queryColumns = JSON(JSON_ARRAY);
}

void ColumnsHandler::Add(const JSON &c) {
  for (int i = 0; i < c.size(); i++) {
    cName = c[i]["name"].get<string>();
    cType = c[i]["type"].get<string>();
    
    allColumns.insert(cName);
    
    if (identifyColumn()) continue;
    if (isForbidden()) {
      columnLists[4].push_back(cName);
      continue;
    }
    if (isRecognized()) continue;

    columnLists[3].push_back(cName);
  }

  findMissingColumns();
}

string ColumnsHandler::getColumnList(int index) {
  if ((index < 0) || (index > 4)) return "";
  if (columnLists[index].size() == 0) return "";

  string ret_val = columnLists[index][0];
  for (int i = 1; i < columnLists[index].size(); i++)
    ret_val += ", " + columnLists[index][i];
  return ret_val;
}

string ErrorMsg::replaceStr() {
  for (int i = 0; i < msgData.size(); i++) {
    string marker = "{" + boost::lexical_cast<string>(i+1) + "}";
    size_t found = -1;
    while ((found = msg.find(marker, found+1)) != string::npos) {
      msg.replace(found, marker.size(), msgData[i]);
    }
  }
  return msg;
}
 
ErrorMsg::ErrorMsg() {
  errorMsg = JSON(JSON_OBJECT);     
  warningMsg = JSON(JSON_OBJECT);
}

void ErrorMsg::SetData(const string &msgD, uint32_t pos) {
  if (msgData.size() <= pos) msgData.resize(pos+1);
  msgData[pos] = msgD;
}

string ErrorMsg::GetError(const string &err, bool replace) { 
  msg =  errorMsg[err].get<string>();
  return (replace) ? replaceStr() : msg;
}

string ErrorMsg::GetWarning(const string &w, bool replace) {
  msg = warningMsg[w].get<string>();
  return (replace) ? replaceStr() : msg;
}

string dx::dataIndex(int64_t index) {
  string str = boost::lexical_cast<string>(index+1);
  switch(index) {
    case 0: return str + "st";
    case 1: return str + "nd";
    case 2: return str + "rd";
    default: return str + "th";
  }
}

JSON dx::readJSON(const string &filename) {
  JSON input;
  ifstream in(filename.c_str());
  input.read(in);
  in.close();
  return input;
}

void dx::writeJSON(const JSON &input, const string &filename) {
  ofstream out(filename.c_str());
  out << input.toString();
  out.close();
}

bool dx::hasString(const JSON &json, const string &val) {
  if (json.type() != JSON_ARRAY) return false;
  for (int i = 0; i < json.size(); i++) {
    if (json[i].get<string>() == val) return true;
  }
  return false;
}

string dx::myPath() {
  char buff[10000];
  size_t len = readlink("/proc/self/exe", buff, 9999);
  buff[len] = '\0';
  string ret_val = string(buff);
  int k = ret_val.find_last_of('/');
  return ret_val.substr(0, k);
}

bool dx::exec(const string &cmd, string &out) {
  cerr << cmd << endl;
  FILE* pipe = popen((cmd + " 2>/dev/null").c_str(), "r");
  if (!pipe) return false;
  char buffer[1024];
  out = "";
  while(!feof(pipe)) {
    if(fgets(buffer, 1024, pipe) != NULL) out += buffer;
  }
  pclose(pipe);

  boost::algorithm::trim(out);
  return true;
}

ValidateInfo::ValidateInfo(ErrorMsg &m) {
  msg = &m;
  info = JSON(JSON_OBJECT);
  info["valid"] = true;
}
      
void ValidateInfo::addWarning(const string &w, bool additionalInfo) {
  string str = msg->GetWarning(w, additionalInfo);
  if (! info.has("warning")) info["warning"] = JSON(JSON_ARRAY);
  info["warning"].push_back(str);
}

void ValidateInfo::addRowWarning(const string &w, uint32_t p) {
  setDataIndex(rowIndex, p);
  addWarning(w, true);
}

bool ValidateInfo::setError(const string &err, bool additionalInfo) {
  info["error"] = msg->GetError(err, additionalInfo);
  info["valid"] = false;
  return false;
}

bool ValidateInfo::setRowError(const string &err, uint32_t p) {
  setDataIndex(rowIndex, p);
  return setError(err, true);
}

bool ValidateInfo::setDXError(const string &m, const string &err) {
  setData(m, 0);
  setError(err, true);
  if (info.has("valid")) info.erase("valid");
  return false;
}

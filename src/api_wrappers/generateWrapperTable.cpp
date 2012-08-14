#include <cstdio>
#include <iostream>
#include <string>
#include <cctype>
#include <vector>
#include <fstream>
using namespace std;

void printUsage() {
  cerr<<"\nUsage:\ngenerateTableForAllWrappers <file_name_with_list_of_all_routes>\n";
}

/* http://www.oopweb.com/CPP/Documents/CPPHOWTO/Volume/C++Programming-HOWTO-7.html */
void Tokenize(const string& str, vector<string>& tokens, const string& delimiters = " ")
{
    // Skip delimiters at beginning.
    string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // Find first "non-delimiter".
    string::size_type pos = str.find_first_of(delimiters, lastPos);

    while (string::npos != pos || string::npos != lastPos)
    {
        // Found a token, add it to the vector.
        tokens.push_back(str.substr(lastPos, pos - lastPos));
        // Skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delimiters, pos);
        // Find next "non-delimiter"
        pos = str.find_first_of(delimiters, lastPos);
    }
}

int main (int argc, char **argv) {
  if (argc != 2) {
    printUsage();
    return 1;
  }
  
  ifstream file(argv[1]);
  
  if (!file.is_open()) {
    cerr<<"\nUnable to open file "<<argv[1]<<endl;
    return 1;
  }
  string line;
  cout<<"[\n";
  bool firstLoop = true;
  while (file.good()) {
    getline(file, line);

    //Treat the case of blank lines at end differently
    if (line.length() == 0)
      continue;
    
    if (!firstLoop) {
      printf(",\n");
    }
    firstLoop = false;
    
    vector<string> urlTokens;
    Tokenize(line, urlTokens, "/");
    if (urlTokens.size() != 2) {
      cerr<<"Unexpected line (see below) in input file:\n"<<line<<"\n";
      return 1;
    }
    bool objectCase = false;
    string first = urlTokens[0], second = urlTokens[1];
    if (urlTokens[0].find('-') != string::npos) {
      objectCase = true;
      // We are in case of object instance method
      vector<string> objToken;
      Tokenize(urlTokens[0], objToken, "-");
      if (objToken.size() != 2) {
        cerr<<"Unexpected line (see below) in input file:\n"<<line<<"\n";
        return 1;
      }
      first = objToken[0];
    }
    second[0] = toupper(second[0]);
    cout<<" [\n  \""<<line<<"\", \""<<first + second<<"(req";
    if (objectCase) {
      cout<<", objectId";
    }
    cout<<")\",  {\"objectMethod\":";
    cout<<( (objectCase) ? "true" : "false");
    cout<<"} \n ]";
  }
  cout<<"\n]\n";
  return 0;
}


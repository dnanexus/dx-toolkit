#include <cstdio>
#include <iostream>
#include <string>
#include <cctype>
#include <vector>
#include <fstream>
using namespace std;

void printUsage() {
  cerr << "\nUsage:\ngenerateTableForAllWrappers <file_name_with_list_of_all_routes>\n";
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

// Any line containing only whitespace (or zero length), or having "#" as the first non-whitespace character is considered a "comment line" and ignored
bool isCommentLine(string line) {
  for (unsigned i = 0; i < line.size(); ++i) {
    if (isspace(line[i]))
      continue;
    else
      return (line[i] == '#');
  }
  return true; // only whitespace found in line (or zero length), assumed to be a "comment line"
}

int main (int argc, char **argv) {
  if (argc != 2) {
    printUsage();
    return 1;
  }

  ifstream file(argv[1]);

  if (!file.is_open()) {
    cerr << "\nUnable to open file " << argv[1] << endl;
    return 1;
  }
  string line;
  cout << "[\n";
  bool firstLoopIter = true;
  while (file.good()) {
    getline(file, line);
    if (isCommentLine(line))
      continue;
    if (!firstLoopIter) {
      cout << ",\n";
    }
    firstLoopIter = false;

    // First try to tokenize on " " (to find out RETRYABLE routes)
    vector<string> lineTokens;
    Tokenize(line, lineTokens, " ");
    bool retryable = false;
    if (lineTokens[lineTokens.size() - 1] == "RETRYABLE")
      retryable = "true";

    vector<string> urlTokens;
    Tokenize(lineTokens[0], urlTokens, "/");
    if (urlTokens.size() != 2) {
      cerr << "Unexpected line in input file:\n" << line << "\n";
      return 1;
    }
    bool objectCase = false;
    string first = urlTokens[0], second = urlTokens[1];
    if (urlTokens[0].find('-') != string::npos) {
      objectCase = true;
      // Object instance method
      vector<string> objToken;
      Tokenize(urlTokens[0], objToken, "-");
      if (objToken.size() != 2) {
        cerr << "Unexpected line in input file:\n" << line << "\n";
        return 1;
      }
      first = objToken[0];
    }
    second[0] = toupper(second[0]);
    cout << " [\n  \"" << lineTokens[0] << "\", \"" << first + second << "(req";
    if (objectCase) {
      cout << ", objectId";
    }
    cout << ")\",  {\"objectMethod\":";
    cout << (objectCase ? "true" : "false");
    cout << ", \"retryable\":" << (retryable ? "true" : "false") << "}";
    cout << "\n ]";

  }
  cout << "\n]\n";
  return 0;
}

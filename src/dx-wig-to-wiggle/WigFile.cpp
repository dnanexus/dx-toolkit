// Copyright (C) 2013 DNAnexus, Inc.
//
// This file is part of wig_importer.
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <cassert>
#include <sstream>
#include <cstdlib>

#include "WigFile.h"
#include "Exceptions.h"

using namespace std;

// Remove whitespace from the beginning and end of a string
string TrimSpace(const string &in)
{
  size_t begin = 0;
  while ((begin < in.size()) && isspace(in[begin]))
    ++begin;

  size_t end = in.size();
  while ((end > begin) && isspace(in[end - 1]))
    --end;

  string out = in.substr(begin, end - begin);
  return out;
}

// Break a string into tokens
void Tokenize(const string &in, vector<string> &out, const string &delim = " 	")
{
  out.clear();

  size_t pos = 0;
  while (pos < in.size())
  {
    size_t newpos = in.find_first_of(delim, pos);
    if (newpos == string::npos)
    {
      out.push_back(in.substr(pos));
      break;
    }
    else
    {
      out.push_back(in.substr(pos, newpos - pos));
      if (newpos != in.size() - 1)
        pos = newpos + 1;
      else
      {
        out.push_back("");
        break;
      }
    }
  }

  if (in.size() == 0)
    out.push_back("");
}

// Split a track header of the form first=second
void SplitTwo(const string &in, string &first, string &second, char delim = '=')
{
  size_t pos = in.find(delim);
  if (pos == string::npos)
    throw AppError("Character '" + string(1, delim) + "' not found in track header token '" + in + "'");

  first = in.substr(0, pos);
  if (pos + 1 < in.size())
    second = in.substr(pos + 1);
  else
    second.clear();
}

// Associate an input stream
void WigFile::Open(istream *i)
{
  is = i;
}

// Parse an unsigned integer
template<typename T>
bool GetUnsigned(const string &s, T *u)
{
  char *loc = NULL;
  const char *str = s.c_str();
  (*u) = strtoul(str, &loc, 10);
  return (loc != str && *loc == 0);
}

// Parse a double
bool GetDouble(const string &s, double *d)
{
  char *loc = NULL;
  const char *str = s.c_str();
  (*d) = strtod(str, &loc);
  return (loc != str && *loc == 0);
}

// Return line number as a string
string WigFile::Line()
{
  stringstream ss;
  ss << line_number;
  return ss.str();
}

// Read and tokenize a line, skipping empty or starting with '#'; also maintain a counter
bool WigFile::GetLine()
{
  if (buffered)
  {
    line = buffered_line;
    fields = buffered_fields;
    buffered = false;
    return true;
  }
  for(;;)
  {
    ++line_number;
    istream &result = getline(*is, line);
    if (!result)
      return false;
    line = TrimSpace(line);
    if (line.size() > 0 && line[0] != '#')
    {
      Tokenize(line, fields);
      return true;
    }
  }
}

// Read a line, throw an error if EOF
void WigFile::GetLineForSure()
{
  if (!GetLine())
    throw AppError("The input file ended prematurely.");
}

// A buffer of a single line is also implemented
void WigFile::UngetLine()
{
  assert(buffered == false);
  buffered = true;
  buffered_line = line;
  buffered_fields = fields;
}

// Advance to the next track
bool WigFile::GetTrack(Track &t)
{
  if (state == 0)
  {
    // Initial state (0); look for the first track, with some heuristics for missing 'track' headers
    state = 1;
    for(;;)
    {
      GetLineForSure();
      if (fields[0] == "browser")
        continue; // Skip over 'browser' lines
      else if (fields[0] == "track")
      {
        // "Track" header found
        ParseHeader(t.header);
        return true;
      }
      else if (fields[0] == "variableStep" || fields[0] == "fixedStep")
      {
        // "Track" header missing but "variableStep"/"fixedStep" found;
        // return an empty "wiggle_0" header
        UngetLine();
        t.header.clear();
        t.header["type"] = "wiggle_0";
        return true;
      }
      else if (fields.size() == 4)
      {
        // "Track" header missing but bedGraph-style data found;
        // return an empty "bedGraph" header
        UngetLine();
        t.header.clear();
        t.header["type"] = "bedGraph";
        return true;
      }
      throw AppError("Invalid content encountered in line " + Line() + "; expected one of 'browser', 'track', 'variableStep', 'fixedStep' or bedGraph data");
    }
  }
  else if (GetLine())
  {
    if (fields[0] == "track")
    {
      ParseHeader(t.header);
      return true;
    }
    else
      throw AppError("Invalid content encountered in line " + Line() + "; expected 'track'");
  }
  else
    return false;
}

// Advance to the next tuple
bool WigFile::GetTuple(Tuple &t)
{
  while (GetLine())
  {
    if (fields[0] == "variableStep")
    {
      ParseVariableStep();
      state = 2;
    }
    else if (fields[0] == "fixedStep")
    {
      ParseFixedStep();
      state = 3;
    }
    else if (fields[0] == "track")
    {
      UngetLine();
      state = 1;
      return false;
    }
    else if (state == 2)
    {
      if (fields.size() != 2)
        throw AppError("Invalid content encountered in line " + Line() + "; expected two columns (a coordinate and a value)");

      unsigned long offset;
      if (!GetUnsigned<unsigned long>(fields[0], &offset) || offset < 1)
        throw AppError("Invalid content encountered in line " + Line() + "; expected chromosomal coordinate in first column");

      double value;
      if (!GetDouble(fields[1], &value))
        throw AppError("Invalid content encountered in line " + Line() + "; expected a numeric value in second column");

      t.chr = chrom;
      t.lo = offset - 1;
      t.hi = t.lo + span;
      t.val = value;
      return true;
    }
    else if (state == 3)
    {
      if (fields.size() != 1)
        throw AppError("Invalid content encountered in line " + Line() + "; expected a single column with a value");

      double value;
      if (!GetDouble(fields[0], &value))
        throw AppError("Invalid content encountered in line " + Line() + "; expected a numeric value in the first column");

      t.chr = chrom;
      t.lo = start - 1;
      t.hi = t.lo + span;
      t.val = value;
      start += step;
      return true;
    }
    else if (fields.size() == 4)
    {
      t.chr = fields[0];

      bool ok = GetUnsigned<uint32>(fields[1], &(t.lo)) && GetUnsigned<uint32>(fields[2], &(t.hi)) && GetDouble(fields[3], &(t.val));
      if (!ok)
        throw AppError("Invalid content encountered in line " + Line() + "; expected <chr> <start> <end> <val>");
      return true;
    }
    else
      throw AppError("Invalid content encountered in line " + Line() + "; expected wiggle or bedGraph data");
  }

  return false;
}

// Parse the variableStep header
void WigFile::ParseVariableStep()
{
  map<string, string> h;
  ParseHeader(h);

  // Get "chrom"
  chrom = h["chrom"];
  if (chrom == "")
    throw AppError("Missing 'chr' in header in line " + Line());

  // Get "span"
  span = 1;
  if (h["span"] != "")
  {
    if (!GetUnsigned<unsigned>(h["span"], &span) || (span < 1))
      throw AppError("Invalid 'span' in header in line " + Line());
  }
}

// Parse the fixedStep header
void WigFile::ParseFixedStep()
{
  ParseVariableStep();

  map<string, string> h;
  ParseHeader(h);

  // Get "start"
  if (h["start"] == "")
    throw AppError("Missing 'start' in header in line " + Line());

  if (!GetUnsigned<unsigned>(h["start"], &start) || (start < 1))
    throw AppError("Invalid 'start' in header in line " + Line());

  // Get "step"
  step = 1;
  if (h["step"] != "")
  {
    if (!GetUnsigned<unsigned>(h["step"], &step) || (step < 1))
      throw AppError("Invalid 'step' in header in line " + Line());
  }
}


// Parse a (tokenized) track header, return map<string,string>
// This function is aware of quotes (i.e. foo="bar baz" should work)
void WigFile::ParseHeader(map<string,string> &hash)
{
  unsigned i = 1;
  hash.clear();
  while (i < fields.size())
  {
    if (fields[i] != "")
    {
      string key, value;
      SplitTwo(fields[i], key, value);
      if ((value.size() > 0) && value[0] == '"')
      {
        value = value.substr(1);
        if (value.size() > 0 && value[value.size()-1] == '"')
        {
          value = value.substr(0, value.size() - 1);
        }
        else
        {
          for(;;)
          {
            ++i;
            if (i == fields.size())
              throw AppError("Invalid header encountered in line " + Line() + "; missing closing double quotes");
            if (fields[i].size() > 0 && fields[i][fields[i].size() - 1] == '"')
            {
              value += " " + fields[i].substr(0, fields[i].size() - 1);
              break;
            }
            else
            {
              value += " " + fields[i];
            }
          }
        }
      }
      hash[key] = value;
    }
    ++i;
  }
}

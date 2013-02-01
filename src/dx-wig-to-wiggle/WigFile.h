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

#ifndef WIGFILE_H
#define WIGFILE_H

#include <iostream>
#include <map>
#include <string>

#include "Common.h"

using namespace std;

class WigFile
{
  // Usage:
  //
  // WigFile w;
  // w.Open(some_istream);
  // while (w.GetTrack(track))
  // {  while (w.GetTuple(tuple))
  //    {  /* Do something with tuple */

  public:

  WigFile() : state(0), line_number(0), buffered(false) { }

  void Open(istream *i);

  class Track
  {
    public:
    map<string, string> header;
  };

  bool GetTrack(Track &t);

  class Tuple
  {
    public:
    string chr;
    uint32 lo;
    uint32 hi;
    double val;
  };

  bool GetTuple(Tuple &t);

  private:

  int state;
  istream *is;

  // variableStep & fixedStep state
  string chrom;
  unsigned span;
  unsigned start;
  unsigned step;

  // Current line
  int line_number;
  string line;
  vector<string> fields;
  string Line();

  bool GetLine();
  void GetLineForSure();
  void UngetLine();
  void ParseHeader(map<string, string> &h);
  void ParseVariableStep();
  void ParseFixedStep();

  // A buffer of one line
  bool buffered;
  string buffered_line;
  vector<string> buffered_fields;
};

#endif

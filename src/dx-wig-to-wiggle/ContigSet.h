// Copyright (C) 2013-2015 DNAnexus, Inc.
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

#ifndef CONTIGSET_H
#define CONTIGSET_H

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "Common.h"

#include "dxjson/dxjson.h"

using namespace dx;

class ContigSet
{
  public:

  vector<string> names;
  vector<uint64> sizes;
  vector<uint64> offsets;

  map<string, uint64> size_map;
  map<string, uint64> offset_map;

  uint64 total_size;
  uint64 max_chrom_size;

  void Init(const vector<string> &names_,
      const vector<uint64> &sizes_,
      const vector<uint64> &offsets_)
  {
    names = names_;
    sizes = sizes_;
    offsets = offsets_;

    total_size = 0;
    max_chrom_size = 0;
    for (unsigned i = 0; i < offsets.size(); ++i)
    {
      size_map[names[i]] = sizes[i];
      offset_map[names[i]] = offsets[i];
      total_size += sizes[i];
      if (sizes[i] > max_chrom_size)
        max_chrom_size = sizes[i];
    }
  }

  void Init(const JSON &json_details)
  {
    vector<string> names_;
    vector<uint64> sizes_;
    vector<uint64> offsets_;
    for (unsigned i = 0; i < json_details["contigs"]["sizes"].size(); ++i)
    {
      names_.push_back(json_details["contigs"]["names"][i].get<string>());
      sizes_.push_back(json_details["contigs"]["sizes"][i].get<uint64>());
      offsets_.push_back(json_details["contigs"]["offsets"][i].get<uint64>());
    }
    Init(names_, sizes_, offsets_);
  }

  bool HasChrom(const string &chr)
  {
    return size_map.find(chr) != size_map.end();
  }
};

#endif

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
#include <fstream>
#include <sstream>

#include <unistd.h>
#include <getopt.h>

#include "WigFile.h"
#include "Exceptions.h"
#include "Common.h"
#include "ContigSet.h"
#include "Compress.h"

#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"
#include "resolver/Resolver.h"

using namespace std;
using namespace dx;

const unsigned resolution_step = 10;

void griColumns(vector<JSON> &columns)
{
  columns.push_back(DXGTable::columnDesc("chr", "string"));
  columns.push_back(DXGTable::columnDesc("lo", "int32"));
  columns.push_back(DXGTable::columnDesc("hi", "int32"));
}

void griIndex(vector<JSON> &indices)
{
  indices.push_back(DXGTable::genomicRangeIndex("chr", "lo", "hi", "gri"));
}

template<typename T> struct TypeName { static const char *name; };
template<> const char *TypeName<double>::name = "double";
template<> const char *TypeName<float>::name = "float";

template<typename ValType>
DXGTable CreateTable(const string &contigset_id, const string &folder, const string &project)
{
  vector<JSON> columns;
  griColumns(columns);
  columns.push_back(DXGTable::columnDesc("val", TypeName<ValType>::name));

  vector<JSON> indices;
  griIndex(indices);

  JSON fields(JSON_OBJECT);
  fields["details"] = JSON(JSON_OBJECT);
  fields["details"]["original_contigset"] = DXLink(contigset_id);
  fields["hidden"] = true;
  fields["folder"] = folder;
  fields["parents"] = true;
  fields["project"] = project;

  return DXGTable::newDXGTable(columns, indices, fields);
}

template<typename ValType>
void OutputValues(DXGTable &dest, uint64 lod, const vector<ValType> &values, const string &chr, uint64 len)
{
  if (values.size() == 0)
    return;

  int lo = 0;
  ValType v = values[0];
  JSON rows(JSON_ARRAY);
  rows.push_back(JSON_ARRAY);
  JSON *row = (JSON *)&(rows[0]);
  row->resize_array(4);
  (*row)[0] = chr;
  for (unsigned i = 1; i < values.size(); ++i)
  {
    if (v != values[i])
    {
      (*row)[1] = lo * lod;
      (*row)[2] = min(i * lod, len);
      (*row)[3] = v;
      lo = i;
      v = values[i];
      dest.addRows(rows);
    }
  }
  (*row)[1] = lo * lod;
  (*row)[2] = len;
  (*row)[3] = v;
  dest.addRows(rows);
}

template<typename ValType>
void FlushChrom(const string &chr, vector<ValType> &values, const vector<uint64> &lods, vector<DXGTable> &tables)
{
  uint64 chrom_len = values.size();
  OutputValues<ValType>(tables[0], 1, values, chr, chrom_len);
  for (unsigned i = 1; i < lods.size(); ++i) {
    vector<ValType> values2;
    values2.reserve(values.size() / resolution_step);
    ValType sum = 0;
    int count = 0;
    for (unsigned j = 0; j < values.size(); ++j) {
      sum += values[j];
      ++count;
      if (count % resolution_step == 0) {
        values2.push_back(sum / count);
        sum = 0;
        count = 0;
      }
    }
    if (count > 0) {
      values2.push_back(sum / count);
    }
    OutputValues<ValType>(tables[i], lods[i], values2, chr, chrom_len);
    values2.swap(values);
  }
}

template<typename ValType>
string Process(const string &filename, const string &contigset_id, const string &project, const string &folder, const string &name, const map<string,string> &properties = map<string,string>(), const vector<string> &tags = vector<string>(), const string &file_id = "")
{
  cerr << "* Starting WIG/bedGraph file importer..." << endl;
  ContigSet cs;

  cs.Init(DXRecord(contigset_id).getDetails());

  // Find out how many resolutions (lods -- levels of detail) are required
  vector<uint64> lods;
  uint64 lod = 1;
  while (lod < cs.max_chrom_size)
  {
    lods.push_back(lod);
    lod *= resolution_step;
  }
  if (lods.size() == 0)
    throw AppError("This genome is too short");
  lods[0] = 0;

  // Wiggle object data
  vector<JSON> fields;
  for (unsigned i = 0; i < lods.size(); ++i)
  {
    JSON f(JSON_OBJECT);
    f["details"] = JSON(JSON_OBJECT);
    f["details"]["original_contigset"] = DXLink(contigset_id);
    f["details"]["signals"] = JSON(JSON_ARRAY);
    f["project"] = project;
    f["folder"] = folder;
    f["parents"] = true;
    f["types"] = JSON(JSON_ARRAY);
    f["types"].push_back("Wiggle");
    if (i > 0)
    {
      f["name"] = (name + " at resolution " + ToStr(lods[i]));
      f["hidden"] = true;
    }
    else
    {
      f["name"] = name;
      f["types"].push_back("TrackSpec");
      f["details"]["representations"] = JSON(JSON_ARRAY);
      if (file_id != "")
      {
        f["details"]["original_file"] = DXLink(file_id);
      }
    }

    fields.push_back(f);
  }

  // Open the file
  Compress::FileSniffer fs;
  istream *ifs;
 
  try
  {
    ifs = fs.Open(filename);
    if (ifs == NULL)
      throw RuntimeError("Error opening " + filename); //Should never happen when run as applet
  }
  catch (Compress::FileOpenError &e)
  {
    throw AppError("Error opening the supplied file -- potentially corrupted compressed data");
  }

  WigFile w;
  w.Open(ifs);

  WigFile::Track track;
  WigFile::Tuple t;
  while (w.GetTrack(track))
  {
    if (track.header["type"] != "wiggle_0" && track.header["type"] != "bedGraph")
      throw AppError("Track type '" + track.header["type"] + "' is not supported; this program only supports WIG and bedGraph files (tracks of type 'wiggle_0' and 'bedGraph')");

    // Construct a signal
    map<string,string> signal = track.header;
    signal["column"] = "val";
    if (signal["name"] == "")
      signal["name"] = "Track #" + ToStr(fields[0]["details"]["signals"].size() + 1);

    cerr << "* Processing track (" << signal["name"] << ")" << endl;

    JSON jsignal(JSON_OBJECT);
    for (map<string,string>::const_iterator j = signal.begin(); j != signal.end(); ++j)
      jsignal[j->first] = j->second;

    // Add signal to wiggles
    vector<DXGTable> tables;
    for (unsigned i = 0; i < lods.size(); ++i)
    {
      DXGTable table = CreateTable<ValType>(contigset_id, folder, project);
      table.setMaxBufferSize(10000000);
      tables.push_back(table);

      jsignal["source"] = DXLink(table.getID());
      fields[i]["details"]["signals"].push_back(jsignal);
    }

    // Maintain a list of processed chromosomes
    map<string, bool> seen;
    for (unsigned i = 0; i < cs.names.size(); ++i)
      seen[cs.names[i]] = false;

    string chr = "";
    bool chr_exists = false;
    vector<ValType> values;

    // Iterate through tuples
    JSON rows(JSON_ARRAY);
    rows.push_back(JSON_ARRAY);
    JSON *row = (JSON *)&(rows[0]);
    row->resize_array(4);
    while (w.GetTuple(t))
    {
      if (chr != t.chr)
      {
        if (chr_exists)
          FlushChrom<ValType>(chr, values, lods, tables);

        chr = t.chr;
        chr_exists = cs.HasChrom(chr);
        if (chr_exists)
        {
          if (seen[chr])
            throw AppError("The input file contains non-contiguous parts for chromosome '" + chr + "'");
          seen[chr] = true;
          values.resize(0);
          values.resize(cs.size_map[chr], 0);
        }
      }
      if (chr_exists)
      {
        if (t.hi > values.size())
          cerr << "WARNING: An entry was found for chromosome '" << chr << "' whose coordinates (" << t.lo << "-" << t.hi << ") are out of range; entry will be trimmed." << endl;
        for (unsigned j = t.lo; (j < t.hi) && (j < values.size()); ++j)
          values[j] = t.val;
      }
      else
      {
        (*row)[0] = chr;
        (*row)[1] = t.lo;
        (*row)[2] = t.hi;
        (*row)[3] = t.val;

        tables[0].addRows(rows);
      }
    }
    if (chr_exists)
      FlushChrom<ValType>(chr, values, lods, tables);
    cerr << "* Finalizing track..." << endl;
    for (unsigned i = 0; i < tables.size(); ++i)
      tables[i].close();
  }

  cerr << "* Finalizing object..." << endl;

  // Create wiggles and update TrackSpec
  vector<DXRecord> wiggles;
  for (unsigned i = 0; i < lods.size(); ++i)
  {
    DXRecord wiggle = DXRecord::newDXRecord(fields[i]);
    wiggles.push_back(wiggle);

    JSON representation(JSON_ARRAY);
    representation.push_back(lods[i]);
    JSON rendering_spec(JSON_OBJECT);
    rendering_spec["type"] = "wiggle";
    rendering_spec["source"] = DXLink(wiggle.getID());
    representation.push_back(rendering_spec);
    fields[0]["details"]["representations"].push_back(representation);
  }
  wiggles[0].setDetails(fields[0]["details"]);
  if (tags.size() > 0)
    wiggles[0].addTags(JSON(tags));
  if (properties.size() > 0)
    wiggles[0].setProperties(JSON(properties));

  for (int i = lods.size() - 1; i >= 0; --i)
    wiggles[i].close();

  return wiggles[0].getID();
}

string TrimSuffix(const string &str, const string &suffix)
{
  if ((str.size() >= suffix.size()) && (str.substr(str.size() - suffix.size()) == suffix))
    return str.substr(0, str.size() - suffix.size());

  return str;
}

void Usage()
{
  cerr << "Converts a wig/bedGraph file to a Wiggle object. Returns (in standard" << endl
       << "out) the object id of the generated Wiggle object." << endl
       << endl
       << "Usage:" << endl
       << "  dx-wig-to-wiggle [options] <filename> <contigset_path> <output_path>" << endl
       << endl
       << "Options:" << endl
       << "  --file-id <file-id>" << endl
       << "  If the wig/bedGraph file exists as a File object on the platform," << endl
       << "  supplying its id with this option will associate it with the resulting" << endl
       << "  Wiggle object. Clicking on the 'Download' action of the Wiggle object" << endl
       << "  on the website will prompt to download the original wig/bedGraph file." << endl
       << endl
       << "  --tag <tag>" << endl
       << "  Add the specified string tag to the output object. You can use tags" << endl
       << "  to better organize your data. You can supply this option multiple times." << endl
       << endl
       << "  --property <key>:<value>" << endl
       << "  Add the specified property (key/value string pair, separated via ':')" << endl
       << "  to the output object. You can use properties to better organize your data." << endl
       << "  You can supply this option multiple times." << endl
       << endl
       << "Example:" << endl
       << "  dx-wig-to-wiggle myfile.wig 'Reference Genomes:/b37/b37' myproject:mywiggle" << endl;
}

int main(int argc, char *argv[])
{
  if ((argc == 2) && (argv[1] == string("--as-applet")))
  {
    try
    {
      JSON input;
      dxLoadInput(input);

      DXFile file(input["file"]);
      cerr << "* Downloading " << file.getID() << endl;
      DXFile::downloadDXFile(file.getID(), "wigfile");

      DXRecord contigset(input["reference_genome"]);

      string output_project = getenv("DX_WORKSPACE_ID");
      string output_folder = "/";
      string output_name = "";
      if (input.has("output_name") && input["output_name"].type() == JSON_STRING)
        output_name = input["output_name"].get<string>();
      if (output_name == "")
      {
        output_name = DXFile(file.getID(), output_project).describe()["name"].get<string>();
        output_name = TrimSuffix(output_name, ".wig");
      }

      map<string,string> properties;
      if (input.has("properties") && input["properties"].type() == JSON_HASH)
      {
        for (JSON::const_object_iterator i = input["properties"].object_begin(); i != input["properties"].object_end(); ++i)
        {
          if ((i->second).type() != JSON_STRING)
            throw AppError("Invalid property value in the input; properties should be strings");
          properties[i->first] = (i->second).get<string>();
        }
      }

      vector<string> tags;
      if (input.has("tags") && input["tags"].type() == JSON_ARRAY)
      {
        for (unsigned i = 0; i != input["tags"].size(); ++i)
          tags.push_back(input["tags"][i].get<string>());
      }

      string output_id = Process<float>("wigfile", contigset.getID(), output_project, output_folder, output_name, properties, tags, file.getID());

      JSON output = JSON(JSON_HASH);
      output["wiggle"] = DXLink(output_id);
      dxWriteOutput(output);
    }
    catch (Compress::FileTypeError &e)
    {
      cerr << "ERROR: Invalid compressed data";
      dxReportError("Error uncompressing the input file");
      return 1;
    }
    catch (AppError &e)
    {
      cerr << "ERROR: " << e.what();
      dxReportError(e.what());
      return 1;
    }
  }
  else
  {
    try
    {
      struct option opts[4];
      // --file-id
      opts[0].name = "file-id";
      opts[0].has_arg = 1;
      opts[0].flag = NULL;
      opts[0].val = 'f';
      // --tag
      opts[1].name = "tag";
      opts[1].has_arg = 1;
      opts[1].flag = NULL;
      opts[1].val = 't';
      // --property
      opts[2].name = "property";
      opts[2].has_arg = 1;
      opts[2].flag = NULL;
      opts[2].val = 'p';
      // NULL
      opts[3].name = 0;
      opts[3].has_arg = 0;
      opts[3].flag = 0;
      opts[3].val = 0;

      string file_id = "";
      vector<string> tags;
      map<string,string> properties;

      int option;

      while ((option = getopt_long(argc, argv, "", opts, NULL)) != -1)
      {
        string tag, key, value;
        size_t pos;
        switch (option)
        {
          case 'f':
            file_id = string(optarg);
            if (file_id.substr(0, 5) != "file-")
            {
              cerr << "Invalid file id. File ids should start with 'file-'." << endl;
              return 1;
            }
            break;

          case 't':
            tag = string(optarg);
            if (tag == "")
            {
              cerr << "Invalid tag. Tags should be non-empty strings." << endl;
              return 1;
            }
            tags.push_back(tag);
            break;

          case 'p':
            tag = string(optarg);
            pos = tag.find(':');
            if (pos == string::npos)
            {
              cerr << "Invalid property. Properties should be of the form key:value." << endl;
              return 1;
            }
            key = tag.substr(0, pos);
            if (pos + 1 < tag.size())
              value = tag.substr(pos + 1);
            else
              value.clear();
            properties[key] = value;
            break;

          default:
            Usage();
            return 1;
        }
      }

      if (optind + 3 != argc)
      {
        Usage();
        return 1;
      }

      argv += optind;

      //Resolver r(getenv("DX_PROJECT_CONTEXT_ID"));
      Resolver r("");

      ObjectInfo contigset = r.FindPath(argv[1]);
      if (contigset.object.id == "")
      {
        cerr << "ContigSet object not found (" << argv[1] << ")" << endl;
        return 1;
      }
      ObjectInfo output = r.DestinationPath(argv[2]);
      if (output.project.id == "")
      {
        cerr << "No such project (" << output.project.name << ")" << endl;
        return 1;
      }

      string output_id = Process<float>(argv[0], contigset.object.id, output.project.id, output.object.folder, output.object.name, properties, tags, file_id);

      cout << output_id << endl;
    }
    catch (Compress::FileTypeError &e)
    {
      cerr << "ERROR: Invalid compressed data";
      return 1;
    }
    catch (RuntimeError &e)
    {
      cerr << "ERROR: " << e.what();
      return 1;
    }
  }

  return 0;
}


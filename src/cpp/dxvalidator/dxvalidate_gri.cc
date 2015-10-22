// Copyright (C) 2013-2015 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
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

#include "dxvalidate_gri.h"

using namespace dx;

void GriColumnsHandler::Init() {
  clearColumns();

  addColumn("chr", "string", 0);
  addColumn("lo" , "integer", 0);
  addColumn("hi" , "integer", 0);
}

GriRowValidator::GriRowValidator(const string &contigset_id, ValidateInfo *m) : GTableRowValidator(m) {
  cReader = new ContigSetReader(contigset_id, m);
  ready = cReader->isReady();

  chrCols.clear(); loCols.clear(); hiCols.clear(); chr_valid.clear();
  addGri("chr", "lo", "hi");
}

void GriRowValidator::addGri(const string &chr, const string &lo, const string &hi) {
  chrCols.push_back(chr); loCols.push_back(lo); hiCols.push_back(hi); chr_valid.push_back(true);
}

bool GriRowValidator::validateGri(const string &chr, int64_t lo, int64_t hi, int k) {
  if (lo < 0) {
    msg->setData(loCols[k], 1);
    return msg->setRowError("LO_TOO_SMALL");
  }

  if (lo > hi) {
    msg->setData(loCols[k], 1);
    msg->setData(hiCols[k], 2);
    return msg->setRowError("LO_TOO_LARGE");
  }

  chrIndex = cReader->chrIndex(chr);
  if (chrIndex >= 0) {
    if (hi > cReader->chrSize(chrIndex)) {
      msg->setData(hiCols[k], 1);
      return msg->setRowError("HI_TOO_LARGE");
    }
  } else {
    if (chr_valid[k]) {
      msg->setData(chrCols[k], 1);
      msg->addRowWarning("CHR_INVALID");
      chr_valid[k] = false;
    }
  }

  return true;
}

bool GriValidator::validateTypes() {
  GTableValidator::validateTypes();
  if (! types.Has("gri")) return msg->setError("TYPE_NOT_GRI");
  if (! hasGenomicIndex()) return msg->setError("GRI_INDEX_MISSING");
  return true;
}

bool GriValidator::validateColumns() {
  columns = new GriColumnsHandler();
  bool ret_val = processColumns();
  delete columns;
  return ret_val;
}

bool GriValidator::validateDetails() {
  if (! details.has("original_contigset")) return msg->setError("CONTIGSET_MISSING");
  if (details["original_contigset"].type() != JSON_OBJECT) return msg->setError("CONTIGSET_INVALID");
  if (! details["original_contigset"].has("$dnanexus_link")) return msg->setError("CONTIGSET_INVALID");
  if (details["original_contigset"]["$dnanexus_link"].type() != JSON_STRING) return msg->setError("CONTIGSET_INVALID");
  return true;
}

bool GriValidator::hasGenomicIndex() {
  if (! desc.has("indices")) return false; 
  for (int i = 0; i < desc["indices"].size(); i++) {
    if (desc["indices"][i]["name"] != "gri") continue;
    if (desc["indices"][i]["type"] != "genomic") return false;
    if (! desc["indices"][i].has("chr")) return false;
    if (desc["indices"][i]["chr"].get<string>() != "chr") return false;
    if (! desc["indices"][i].has("lo")) return false;
    if (desc["indices"][i]["lo"].get<string>() != "lo") return false;
    if (! desc["indices"][i].has("hi")) return false;
    if (desc["indices"][i]["hi"].get<string>() != "hi") return false;
    return true;
  }

  return false;
}

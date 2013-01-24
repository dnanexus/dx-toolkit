// Copyright (C) 2013 DNAnexus, Inc.
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

#include "api_helper.h"
#include "dxjson/dxjson.h"
#include "dxcpp/dxcpp.h"

#include "import_apps.h"

using namespace std;

string findRefGenomeProjID() {
  try {
    dx::JSON params(dx::JSON_OBJECT);
    params["name"] = "Reference Genomes";
    params["level"] = "VIEW";
    params["public"] = true;
    params["describe"] = false;
    params["billTo"] = "org-dnanexus";
    dx::JSON findResult = systemFindProjects(params);
    if (findResult["results"].size() != 1)
      throw runtime_error("Expected name = 'Reference Genomes', and, billTo = 'org-dnanexus', to return exactly one public project, but instead recieved " + boost::lexical_cast<string>(findResult["results"].size()) + " projects instead. Unable to resolve --ref-genome parameter.");
    return findResult["results"][0]["id"].get<string>();
  } catch (DXAPIError &e) {
    LOG << "Call to findProjects failed." << endl;
    throw;  
  }
}

string getRefGenomeID(const string &refGenome) {
  if (refGenome.find("record-") == 0)
    return refGenome; // User provided a record id

  //If not an ID, then find in 'Reference Genomes' project
  string refGenomeProj = findRefGenomeProjID();
  dx::JSON params(dx::JSON_OBJECT);
  params["name"] = refGenome;
  params["state"] = "closed";
  params["class"] = "record";
  params["type"] = "ContigSet";
  params["scope"] = dx::JSON(dx::JSON_OBJECT);
  params["scope"]["project"] = refGenomeProj;
  dx::JSON findResult = systemFindDataObjects(params);
  if (findResult["results"].size() == 0)
    throw runtime_error("Unable to find any reference genome with name: '" + refGenome + "'");
  if (findResult["results"].size() > 1) {
    // This case should not happen with a 'Reference Genomes' project with billTo: 'user-dnanexus'
    // But just adding this "if" clause for sanity check
    throw runtime_error("Too many matches for reference genome with name: '" + refGenome + "' (Number of matches : " \
                        + boost::lexical_cast<string>(findResult["results"].size()) + ")");
  }
  return findResult["results"][0]["id"].get<string>();
}

// A helper for runImportApps(): Logs all the activity while running an app (and any error)
string runApp_helper(const string &appName, const string &jobName, const dx::JSON &input, const string &project, const string &folder) {
  dx::JSON params(dx::JSON_OBJECT);
  params["name"] = jobName;
  params["input"] = input;
  params["project"] = project;
  params["folder"] = folder;
  dx::JSON output;

  try {
    LOG << "Running app: '" << appName << "'" << endl;
    LOG << "\tJob Name: " << jobName << endl;
    LOG << "\tProject context: " << project << endl;
    LOG << "\tOutput Folder: " << folder << endl;
    LOG << "\tInput JSON Hash: '" << input.toString() << "'" << endl;
    output = appRun(appName, params);
  } catch (exception &e) {
    LOG << "Error running the app. Message: " << e.what() << endl;
    return "failed";
  }
  LOG << "App started succesfuly, Job ID: " << output["id"].get<string>() << endl;
  return output["id"].get<string>();
}

inline dx::JSON getDnanexusLinkFormat(const string &objID) {
  return dx::JSON::parse("{\"$dnanexus_link\": \"" + objID + "\"}");
}

void runImportApps(const Options &opt, vector<File> &files) {
  const char *const readsImporter = "app-reads_importer";
  const char *const mappingsImporter = "app-sam_bam_importer";
  const char *const variantsImporter = "app-vcf_to_variants";
  string refGenomeID;
  if (opt.mappings || opt.variants) {
    LOG << "Obtaining record ID of reference genome from flag --ref-genome";
    refGenomeID = getRefGenomeID(opt.refGenome);
    LOG << "... Done (ref genome id = " << refGenomeID << ")" << endl;
  }
  const unsigned int incrementFactor = (opt.pairedReads) ? 2 : 1;
  for (unsigned i = 0; i < files.size(); i += incrementFactor) {
    if (files[i].failed || (opt.pairedReads && files[i + 1].failed)) {
      files[i].jobID = "failed";
      if (opt.pairedReads) {
        files[i + 1].jobID = "failed";
        LOG << "Atleast one of the file in " << i + 1 << "th pair, failed to upload properly. Won't run reads_importer app for it" << endl;
      } else {
        LOG << "File '" << files[i].localFile << "' failed to upload. Won't run importer app for it." << endl;
      }
      continue;
    }
    dx::JSON input(dx::JSON_OBJECT);
    
    if (opt.reads) {
      input["file"] = getDnanexusLinkFormat(files[i].fileID); 
      files[i].jobID =  runApp_helper(readsImporter, "import_reads", input, files[i].projectID, files[i].folder);
    }
    if (opt.pairedReads) {
      input["file"] = getDnanexusLinkFormat(files[i].fileID);
      input["file2"] = getDnanexusLinkFormat(files[i + 1].fileID);
      files[i].jobID = files[i + 1].jobID = runApp_helper(readsImporter, "import_paired_reads", input, files[i].projectID, files[i].folder);
    }
    if (opt.mappings) {
      input["file"] = getDnanexusLinkFormat(files[i].fileID);
      input["reference_genome"] = getDnanexusLinkFormat(refGenomeID);
      files[i].jobID = runApp_helper(mappingsImporter, "import_mappings", input, files[i].projectID, files[i].folder);
    }
    if (opt.variants) {
      input["vcf"] = getDnanexusLinkFormat(files[i].fileID);
      input["reference"] = getDnanexusLinkFormat(refGenomeID);
      files[i].jobID = runApp_helper(variantsImporter, "import_vcf", input, files[i].projectID, files[i].folder);
    }
  }
}

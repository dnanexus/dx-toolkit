// Copyright (C) 2013-2016 DNAnexus, Inc.
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

#ifndef UA_API_HELPER_H
#define UA_API_HELPER_H

#include <string>
#include <vector>

#include "dxjson/dxjson.h"

std::string resolveProject(const std::string &projectSpec);

void createFolder(const std::string &projectID, const std::string &folder);

void createFolders(const std::vector<std::string> &projects, const std::vector<std::string> &folders);

std::string createFileObject(const std::string &project, const std::string &folder, const std::string &name, const std::string &mimeType, const dx::JSON &properties, const dx::JSON &type, const dx::JSON & tags, const std::string &visibility, const dx::JSON &details);

void closeFileObject(const std::string &fileID);

std::string getFileState(const std::string &fileID);

dx::JSON findResumableFileObject(std::string project, std::string signature);

void removeFromProject(const std::string &projID, const std::string &objID);

void checkForUpdates();

std::string getProjectName(const std::string &projectID);

dx::JSON getPlatformInputHash();

#define FILE_SIGNATURE_PROPERTY ".system-fileSignature"

#endif

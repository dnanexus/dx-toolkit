#!/usr/bin/env v8cgi

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

// Stub. TODO: refill tests from oldJSLib

require.paths.push(".");

var dx = require('DNAnexus');

for (method in dx) {
  system.stdout('dx.' + method + "\n");
}
for (method in dx.api) {
  system.stdout('dx.api.' + method + "\n");
}

dx.api.systemSearch({});
dx.system('ls');

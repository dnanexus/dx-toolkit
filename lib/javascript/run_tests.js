#!/usr/bin/env v8cgi

// Stub. TODO: refill tests from oldJSLib

require.paths.push(".");

var dx = require('DNAnexus');

for (method in dx) {
  system.stdout('dx.'+method+"\n");
}
for (method in dx.api) {
  system.stdout('dx.api.'+method+"\n");
}

dx.api.systemSearch({});

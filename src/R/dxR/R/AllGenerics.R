# Copyright (C) 2013-2015 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

setGeneric("id", function(handler) {
  standardGeneric("id")
})

setGeneric("describe", function(handler) {
  standardGeneric("describe")
})

setGeneric("desc", function(handler) {
  standardGeneric("desc")
})

setGeneric("desc<-", function(handler, value) {
  standardGeneric("desc<-")
})

setGeneric("addRow", function(handler, row) {
  standardGeneric("addRow")
})

setGeneric("addRows", function(handler, ...) {
  standardGeneric("addRows")
})

setGeneric("getRows", function(handler, ...) {
  standardGeneric("getRows")
})

setGeneric("flushRows", function(handler, ...) {
  standardGeneric("flushRows")
})

setGeneric("closeObj", function(handler, block=FALSE) {
  standardGeneric("closeObj")
})

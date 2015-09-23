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

##' @import methods

library(methods)

##' GenomicTable (GTable) Handler
##'
##' This class is an interface to a GenomicTable(GTable) stored on the
##' DNAnexus platform.
##'
##' @section Slots: \describe{
##'     \item{\code{id}}{String ID of the GTable}
##'     \item{\code{project}}{String ID of the GTable's project}
##'     \item{\code{.desc}}{Cached copy from calling describe on the GTable}
##' }
##'
##' @name DXGTable-class
##' @rdname DXGTable-class
##' @export
setClass("DXGTable",
         representation(
                        id="character",
                        project="character",
                        desc="list"
                        ),
         prototype(id='',
                   project='',
                   desc=RJSONIO::emptyNamedList),
         validity=function(object) {
           if (!any(grep("^gtable-[a-zA-Z0-9]{24}$", object@id))) {
             return ("invalid GTable ID")
           }
           # Project ID must either be empty or valid
           if (object@project != '' &&
               !any(grep("^(project|container)-[a-zA-Z0-9]{24}$",
                         object@project))) {
             return ("invalid project or container ID")
           }
           return (TRUE)
         }
         )

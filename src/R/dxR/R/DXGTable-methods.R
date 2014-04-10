# Copyright (C) 2013-2014 DNAnexus, Inc.
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

##' GenomicTable handler constructor
##'
##' Construct a GenomicTable(GTable) handler using an object ID of the
##' form "gtable-xxxx".  If a project ID is not provided, the default
##' project (project or a job's temporary workspace) is used if available.
##'
##' @param id String object ID
##' @param project String project or container ID
##' @param describe Whether to cache a description of the gtable
##' @return An R object of class DXGTable
##' @rdname DXGTable
##' @examples
##' DXGTable("gtable-123456789012345678901234", describe=FALSE)
##' DXGTable("gtable-123456789012345678901234", project="project-12345678901234567890abcd", describe=FALSE)
##' @export
##' @seealso \code{\link{newDXGTable}} for creating a new GenomicTable
DXGTable <- function(id, project=dxEnv$DEFAULT_PROJECT, describe=TRUE) {
  handler <- new("DXGTable", id=id, project=project)
  if (describe) {
    desc(handler) <- describe(handler)
  }
  return (handler)
}

# TODO: constructors for opening a gtable for reading or writing

##' Create a GTable column descriptor
##'
##' Creates a column descriptor for creating a new GenomicTable on the
##' platform.  See the API documentation for a full list of available
##' data types and restrictions on column names.
##'
##' @param name Name of the column
##' @param type Type of the column, e.g. "boolean", "int", "double", "string"
##' @return a list that can be used as a column descriptor in
##' @seealso \code{\link{newDXGTable}}
##' @export
##' @examples
##' colDesc("chr", "string")
##' colDesc("lo", "int64")
##' colDesc("a_boolean", "boolean")
colDesc <- function(name, type) {
  return (list(name=name, type=type))
}

##' Create a genomic range index descriptor
##'
##' Creates a genomic range index descriptor describing which columns
##' are to be used to create the index.
##'
##' @param chr Name of the string column of chromosome names to be used
##' @param lo Name of the integer column of low coordinates to be used
##' @param hi Name of the integer column of high coordinates to be used
##' @param name Name to give the created genomic range index; used
##' when querying the GTable after it has been closed.
##' @return a list that can be used as an index descriptor when
##' calling \code{\link{newDXGTable}}
##' @seealso \code{\link{newDXGTable}}, \code{\link{genomicRangeQuery}}
##' @export
##' @examples
##' genomicRangeIndex("chr", "lo", "hi")
##' genomicRangeIndex("chr", "lo", "hi", name="othergri")
genomicRangeIndex <- function(chr, lo, hi, name="gri") {
  return (list(name=name, type="genomic", chr=chr, lo=lo, hi=hi))
}

##' Create a lexicographic index column descriptor
##'
##' Creates a lexicographic index column descriptor to be used for
##' creating a lexicographic index descriptor using
##' \code{\link{lexicographicIndex}}.
##'
##' @param name Name of the column to be indexed
##' @param ascending Whether to order entries of this column in ascending order
##' @param caseSensitive If \code{FALSE}, compare strings
##' case-insensitively (default is TRUE for string columns; note that
##' this option should only be used for string columns and will be an
##' error if used otherwise)
##' @return a list that can be used to create a lexicographic index
##' descriptor via \code{\link{lexicographicIndex}}
##' @seealso \code{\link{lexicographicIndex}}
##' @export
##' @examples
##' lexicographicIndexColumn("gene", caseSensitive=FALSE)
lexicographicIndexColumn <- function(name, ascending=TRUE, caseSensitive=NA) {
  column <- list(name=name)
  if (ascending) {
    column$order <- "asc"
  } else {
    column$order <- "desc"
  }
  if (!is.na(caseSensitive)) {
    column$caseSensitive <- caseSensitive
  }
  return (column)
}

##' Create a lexicographic index descriptor
##'
##' Creates a lexicographic index descriptor describing which
##' column(s) are to be used to create the index.
##'
##' @param columns List of lexicographic column descriptors as created
##' via \code{\link{lexicographicIndexColumn}}
##' @param name Name to give the created lexicographic index; used
##' when querying the GTable after it has been closed.
##' @return a list that can be used as an index descriptor when
##' calling \code{\link{newDXGTable}}
##' @seealso \code{\link{newDXGTable}},
##' \code{\link{lexicographicQuery}},
##' \code{\link{lexicographicIndexColumn}}
##' @export
##' @examples
##' lexicographicIndex(list("quality"), "qualityIndex")
##' lexicographicIndex(list("quality", "othercol"), "multiColIndex")
lexicographicIndex <- function(columns, name) {
  return (list(name=name, type="lexicographic", columns=columns))
}

##' Create a new GenomicTable and return a handler
##'
##' Construct a GenomicTable(GTable) handler after creating a new
##' GTable on the platform.  If a project ID is not provided, the new
##' GTable will be created in the default project (project or a job's
##' temporary workspace) is used if available.
##'
##' @param columns list of column descriptors
##' @param indices optional list of index descriptors; if not
##' provided, no indices are created when the GTable is closed
##' @param project String project or container ID
##' @return An R object of class DXGTable
##' @rdname newDXGTable
##' @export
##' @seealso \code{\link{DXGTable}} for making a handler for an
##' existing GTable.  \code{\link{colDesc}} for creating column
##' descriptors.  \code{\link{genomicRangeIndex}} and
##' \code{\link{lexicographicIndex}} for creating index descriptors.
newDXGTable <- function(columns, indices=NA, project=dxEnv$DEFAULT_PROJECT) {
  inputHash <- list(columns=columns, project=project)
  if (!is.na(indices)) {
    inputHash$indices <- indices
  }
  resp <- gtableNew(inputHash)
  handler <- new("DXGTable", id=resp[["id"]], project=project)
  desc(handler) <- describe(handler)
  return (handler)
}

##' Get the ID from a DNAnexus handler
##'
##' Returns the string ID of the referenced object..
##' 
##' @param handler A DNAnexus handler
##' @return string ID of the referenced object
##' @docType methods
##' @rdname id-methods
##' @examples
##' dxgtable <- DXGTable("gtable-123456789012345678901234", describe=FALSE)
##' id(dxgtable)
##' @export
##' @aliases id,DXGTable-method
setMethod("id", "DXGTable", function(handler) {
  handler@id
})

##' Get the cached describe from a DNAnexus handler
##'
##' Returns the description of the referenced data object, if cached.
##' 
##' @param handler A DNAnexus handler
##' @return description of the referenced object, if cached; otherwise NA
##' @docType methods
##' @rdname desc-methods
##' @export
##' @aliases desc,DXGTable-method
setMethod("desc", "DXGTable", function(handler) {
  if (length(handler@desc) == 0) {
    return (NA)
  } else {
    return (handler@desc)
  }
})

##' Describe the Data Object
##'
##' Returns the data frame containing columns describing the data
##' object.
##' 
##' @param handler A data object handler
##' @return named list of the data object's describe hash
##' @docType methods
##' @rdname describe-methods
##' @export
##' @aliases describe,DXGTable-method
setMethod("describe", "DXGTable", function(handler) {
  validObject(handler)
  inputHash <- RJSONIO::emptyNamedList
  if (handler@project != '') {
    inputHash$project <- handler@project
  }
  dxHTTPRequest(paste("/", handler@id, "/describe", sep=''),
                data=inputHash)
})

##' Set the cached description for the data object
##'
##' Sets the cached description for the data object with the given
##' value.  Usually called with \code{\link{describe}}.
##' 
##' @param handler A data object handler
##' @param value The value to save
##' @return the modified data object handler
##' @docType methods
##' @examples
##' # The following command refreshes the cached description a
##' # DXGTable object called dxgtable
##' \dontrun{desc(dxgtable) <- describe(dxgtable)}
##' @aliases desc<-,DXGTable-method
##' @name desc<-
##' @rdname desc-replace-methods
##' @export "desc<-"
setReplaceMethod("desc", "DXGTable", function(handler, value) {
  handler@desc <- value
  return (handler)
})

# gtableAddRowsBuffers is an R environment for storing buffers of data
# to be flushed
gtableAddRowsBuffers <- new.env()

# TODO: how to deal with null value?  NA <-> -21...? at
# addRows/getRows time?

##' Add a Row to a GTable
##'
##' Takes in a list of values and adds it to the referenced GTable.
##' 
##' @param handler A data object handler
##' @param row A list of row values, either in exact column order, or
##' named with column names.
##' @docType methods
##' @rdname addRow-methods
##' @export
##' @aliases addRow,DXGTable-method
##' @seealso \code{\link{addRows}} for adding multiple rows at once
##' from a data frame
setMethod("addRow", "DXGTable", function(handler, row) {
  if (!exists(handler@id, envir=gtableAddRowsBuffers, inherits=FALSE)) {
    assign(handler@id, list(as.list(row)), envir=gtableAddRowsBuffers)
  } else {
    assign(handler@id,
           c(gtableAddRowsBuffers[[handler@id]], list(as.list(row))),
           envir=gtableAddRowsBuffers)
  }
  # For now, flushing at 100 rows; TODO: flushing at data size
  # thresholds.
  if (length(gtableAddRowsBuffers[[handler@id]]) > 10000) {
    flushRows(handler)
  }
})

##' Add Rows to a GTable
##'
##' Takes in a data frame and adds it as rows to the referenced
##' GTable.
##' 
##' @param handler A data object handler
##' @param rows A data frame containing the rows to add
##' @docType methods
##' @rdname addRows-methods
##' @export
##' @aliases addRows,DXGTable-method
##' @seealso \code{\link{addRow}} for adding single rows at a time
setMethod("addRows", "DXGTable", function(handler, rows) {
  # Add new rows to the buffer and flush as necessary

  # First, convert all factor columns using as.character
  classes <- as.character(sapply(rows, class))
  colClasses <- which(classes == "factor")
  rows[, colClasses] <- sapply(rows[, colClasses], as.character)

  result <- do.call(mapply,
                    c(function(...) { addRow(handler, list(...)) },
                      as.list(sapply(names(rows),
                                     function(name) { return (list(rows[[name]])) },
                                     USE.NAMES=FALSE))))
})

##' Flush Added Rows to a GTable
##'
##' Takes any buffered rows to be added to the GTable and adds them to
##' the GTable.
##' 
##' @param handler A GTable handler
##' @docType methods
##' @rdname flushRows-methods
##' @export
##' @aliases flushRows,DXGTable-method
##' @seealso \code{\link{closeObj}} which calls this function if
##' necessary
setMethod("flushRows", "DXGTable", function(handler) {
  if (exists(handler@id, envir=gtableAddRowsBuffers, inherits=FALSE)) {
    inputHash <- list(part=gtableNextPart(handler@id)[["part"]],
                      data=gtableAddRowsBuffers[[handler@id]])
    gtableAddRows(handler@id, inputHash)
    rm(list=handler@id, envir=gtableAddRowsBuffers)
  }
})

##' Close a GTable
##'
##' Flushes any queued rows (by calling \code{\link{flushRows}}) for the
##' GTable and closes it.
##' 
##' @param handler A GTable handler
##' @param block Whether to wait until the gtable has finished closing
##' @docType methods
##' @rdname closeObj-methods
##' @aliases closeObj,DXGTable-method
##' @seealso \code{\link{flushRows}}
##' @export
setMethod("closeObj", "DXGTable", function(handler, block=FALSE) {
  if (exists(handler@id, envir=gtableAddRowsBuffers, inherits=FALSE)) {
    flushRows(handler)
  }
  gtableClose(handler@id)
  if (block) {
    desc(handler) <- describe(handler)
    while (handler@desc$state != "closed") {
      Sys.sleep(2)
      desc(handler) <- describe(handler)
    }
  }
})

##' Construct a genomic range query for a GTable with a genomic range index
##'
##' Construct a genomic range query for a GTable for use with the
##' \code{\link{getRows}} method.  The GTable must have been
##' constructed with a genomic range index for the query to succeed.
##' See the API documentation for details on the two modes ("enclose"
##' and "overlap") of the query.
##'
##' @param chr Chromosome name
##' @param lo Integer low coordinate
##' @param hi Integer high coordinate
##' @param mode The type of query to perform: "overlap" (default) or
##' "enclose"
##' @param index The name of the genomic range index to use
##' @return query to use as an argument to \code{\link{getRows}}
##' @export
##' @seealso Can be used when calling \code{\link{getRows}}
##' @examples
##' genomicRangeQuery("chrI", 1000, 5000)
##' genomicRangeQuery("chrII", 1000, 10000, mode="enclose", index="othergri")
genomicRangeQuery <- function(chr, lo, hi, mode="overlap", index="gri") {
  return (list(index=index,
               parameters=list(mode=mode, coords=list(chr, lo, hi))))
}

# TODO: Make lexicographic query easier/more native to construct

##' Construct a lexicographic query for a GTable with a lexicographic index
##'
##' Construct a lexicographic query for a GTable for use with the
##' \code{\link{getRows}} method.  The GTable must have been
##' constructed with a lexicographic index for the query to succeed.
##' See the API documentation for full details on how to construct the
##' parameters for this query.
##'
##' @param query A MongoDB-style query
##' @param index The name of the lexicographic index to use
##' @return query to use as an argument to \code{\link{getRows}}
##' @export
##' @seealso Can be used when calling \code{\link{getRows}}
##' @examples
##' lexicographicQuery(list(quality=list("$gt"=22)), "qualityIndex")
lexicographicQuery <- function(query, index) {
  return (list(index=index,
               parameters=query))
}

##' Retrieve Rows from a GTable
##'
##' Returns a data frame containing rows from the referenced GTable.
##' Note that if \code{limit} rows are returned, there may be more
##' rows which satisfy your query.  To retrieve all rows, either set
##' the limit high enough or repeat this query by changing the
##' \code{starting} argument to be equal to one more than the last row
##' ID received until no more rows are received.
##' 
##' @param handler A data object handler
##' @param query An extra query with which to filter the results,
##' e.g. constructed with \code{\link{genomicRangeQuery}} or
##' \code{\link{lexicographicQuery}}
##' @param columns A list of column names to include in the results.
##' The row ID column "__id__" will be ignored.
##' @param starting The starting row ID from which to begin returning
##' results
##' @param limit The maximum number of rows to return
##' @return data frame of rows from the GTable with row names equal to
##' their row IDs
##' @docType methods
##' @rdname getRows-methods
##' @export
##' @aliases getRows,DXGTable-method
##' @seealso \code{\link{genomicRangeQuery}} and
##' \code{\link{lexicographicQuery}} for constructing the
##' \code{query} argument
setMethod("getRows", "DXGTable", function(handler,
                                          query=NA,
                                          columns=NA, starting=NA, limit=40000) {
  inputHash <- RJSONIO::emptyNamedList
  if (!is.na(query)) {
    inputHash$query <- query
  }
  if (length(columns) > 1 || !is.na(columns)) {
    if ("__id__" %in% columns) {
      warning("dxR::getRows: Requested \"__id__\" column is used for row names and is not provided as a separate named column",
              call.=FALSE)
      columns <- columns[columns!="__id__"]
    }
    # Always request the row ID first to be used for row names
    inputHash$columns <- c("__id__", columns)
  } else {
    columns <- names(handler)
  }
  if (!is.na(starting)) {
    inputHash$starting <- starting
  }
  if (!is.na(limit)) {
    inputHash$limit <- limit
  }

  resp <- gtableGet(handler@id, inputHash)
  # Preallocate memory as much as possible
  frame <- do.call(data.frame,
                   sapply(columns, function(name) { return (rep(NA, resp[["length"]])) },
                          simplify=FALSE,
                          USE.NAMES=TRUE))
  ids <- rep(NA, resp[["length"]])
  i <- 1
  for (row in resp[["data"]]) {
    ids[i] <- row[1]
    for (j in 1:length(columns)) {
      frame[i, columns[j]] <- row[j + 1]
    }
    i <- i + 1
  }
  rownames(frame) <- ids
  return (frame)
})

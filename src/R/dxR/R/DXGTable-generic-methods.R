# Copyright (C) 2013 DNAnexus, Inc.
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

# This file contains implementations of standard R methods for data
# frames and matrices, such as querying for the column names, number
# of rows, etc.

##' @import methods

library(methods)

##' Get Column Names of a GTable
##'
##' Returns a character vector of column names of the GTable.
##' 
##' @param x A GTable handler
##' @return vector of column names
##' @docType methods
##' @rdname names-methods
##' @export
##' @aliases names,DXGTable-method
setMethod("names", "DXGTable", function(x) {
  if (length(x@desc) == 0) {
    desc(x) <- describe(x)
  }

  if ("columns" %in% names(x@desc)) {
    return (sapply(x@desc$columns, function(coldesc) { return (coldesc[["name"]]) }))
  } else {
    stop("Did not find list of columns in the gtable description")
  }
})

##' Get the First Part of a GTable
##'
##' Returns the first part of the referenced GTable.  By default,
##' returns the first 6 rows.
##' 
##' @param x A GTable handler
##' @param n An integer: if positive, the max number of rows starting
##' from the beginning; if negative, all but the last "|n|" rows.
##' @return data frame of rows from the GTable
##' @docType methods
##' @rdname head-methods
##' @export
##' @aliases head,DXGTable-method
setMethod("head", "DXGTable", function(x, n=6L) {
  if (n >= 0) {
    return (getRows(x, limit=n))
  } else if (nrow(x) + n < 0) {
    return (getRows(x, limit=0))
  } else {
    return (getRows(x, limit=(nrow(x) + n)))
  }
})

##' Get the Last Part of a GTable
##'
##' Returns the last part of the referenced GTable.  By default,
##' returns the last 6 rows.
##' 
##' @param x A GTable handler
##' @param n An integer: if positive, the max number of rows to be
##' returned from the end of the GTable; if negative, all but the first
##' "|n|" rows.
##' @return data frame of rows from the GTable
##' @docType methods
##' @rdname tail-methods
##' @export
##' @aliases tail,DXGTable-method
setMethod("tail", "DXGTable", function(x, n=6L) {
  starting <- 0
  if (length(x@desc) == 0 ||
      !"length" %in% names(x@desc)) {
    # Cache it since describe may have to be called > 1 time
    # otherwise.
    desc(x) <- describe(x)
  }
  if (n >= 0) {
    if (nrow(x) < n) {
      return (getRows(x))
    } else {
      return (getRows(x, starting=(nrow(x)-n)))
    }
  } else {
    if (nrow(x) + n <= 0) {
      return (getRows(x, limit=0))
    } else {
      return (getRows(x, starting=(-n)))
    }
  }
})

##' The Number of Rows in the GTable
##'
##' Returns the number of rows in the GTable.
##' 
##' @param x A GTable handler
##' @return number of rows in the GTable
##' @docType methods
##' @rdname nrow-methods
##' @export
##' @aliases nrow,DXGTable-method
setMethod("nrow", "DXGTable", function(x) {
  if (length(x@desc) == 0 ||
      !"length" %in% names(x@desc)) {
    desc(x) <- describe(x)
  }

  if ("length" %in% names(x@desc)) {
    return (x@desc$length)
  } else {
    stop("Did not find the number of rows in the gtable description")
  }
})

##' The Number of Columns in the GTable
##'
##' Returns the number of columns (not including the row ID column) in
##' the GTable.
##' 
##' @param x A GTable handler
##' @return number of columns in the GTable
##' @docType methods
##' @rdname ncol-methods
##' @export
##' @aliases ncol,DXGTable-method
setMethod("ncol", "DXGTable", function(x) {
  if (length(x@desc) == 0) {
    desc(x) <- describe(x)
  }

  if ("columns" %in% names(x@desc)) {
    return (length(x@desc$columns))
  } else {
    stop("Did not find the number of columns in the gtable description")
  }
})

##' Dimensions of a GTable
##'
##' Returns a vector of the number of rows and columns (not including
##' the row ID column) of the referenced GTable, respectively.
##' 
##' @param x A GTable handler
##' @return number of rows and columns in the GTable
##' @docType methods
##' @rdname dim-methods
##' @export
##' @aliases dim,DXGTable-method
setMethod("dim", "DXGTable", function(x) {
  if (length(x@desc) == 0) {
    desc(x) <- describe(x)
  }

  if ("length" %in% names(x@desc) && "columns" %in% names(x@desc)) {
    return (c(x@desc$length, length(x@desc$columns)))
  } else {
    stop("Did not find the dimensions in the gtable description")
  }
})

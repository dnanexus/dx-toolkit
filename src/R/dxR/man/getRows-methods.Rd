\docType{methods}
\name{getRows}
\alias{getRows}
\alias{getRows,DXGTable-method}
\title{Retrieve Rows from a GTable}
\arguments{
  \item{handler}{A data object handler}

  \item{query}{An extra query with which to filter the
  results, e.g. constructed with
  \code{\link{genomicRangeQuery}} or
  \code{\link{lexicographicQuery}}}

  \item{columns}{A list of column names to include in the
  results. The row ID column "__id__" will be ignored.}

  \item{starting}{The starting row ID from which to begin
  returning results}

  \item{limit}{The maximum number of rows to return}
}
\value{
  data frame of rows from the GTable with row names equal
  to their row IDs
}
\description{
  Returns a data frame containing rows from the referenced
  GTable. Note that if \code{limit} rows are returned,
  there may be more rows which satisfy your query.  To
  retrieve all rows, either set the limit high enough or
  repeat this query by changing the \code{starting}
  argument to be equal to one more than the last row ID
  received until no more rows are received.
}
\seealso{
  \code{\link{genomicRangeQuery}} and
  \code{\link{lexicographicQuery}} for constructing the
  \code{query} argument
}


\docType{methods}
\name{tail}
\alias{tail}
\alias{tail,DXGTable-method}
\title{Get the Last Part of a GTable}
\arguments{
  \item{x}{A GTable handler}

  \item{n}{An integer: if positive, the max number of rows
  to be returned from the end of the GTable; if negative,
  all but the first "|n|" rows.}
}
\value{
  data frame of rows from the GTable
}
\description{
  Returns the last part of the referenced GTable.  By
  default, returns the last 6 rows.
}


\docType{methods}
\name{head}
\alias{head}
\alias{head,DXGTable-method}
\title{Get the First Part of a GTable}
\arguments{
  \item{x}{A GTable handler}

  \item{n}{An integer: if positive, the max number of rows
  starting from the beginning; if negative, all but the
  last "|n|" rows.}
}
\value{
  data frame of rows from the GTable
}
\description{
  Returns the first part of the referenced GTable.  By
  default, returns the first 6 rows.
}


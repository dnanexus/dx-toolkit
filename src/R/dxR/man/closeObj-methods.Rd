\docType{methods}
\name{closeObj}
\alias{closeObj}
\alias{closeObj,DXGTable-method}
\title{Close a GTable}
\arguments{
  \item{handler}{A GTable handler}

  \item{block}{Whether to wait until the gtable has
  finished closing}
}
\description{
  Flushes any queued rows (by calling
  \code{\link{flushRows}}) for the GTable and closes it.
}
\seealso{
  \code{\link{flushRows}}
}


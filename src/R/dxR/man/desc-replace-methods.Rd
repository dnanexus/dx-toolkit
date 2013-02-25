\docType{methods}
\name{desc<-}
\alias{desc<-}
\alias{desc<-,DXGTable-method}
\title{Set the cached description for the data object}
\arguments{
  \item{handler}{A data object handler}

  \item{value}{The value to save}
}
\value{
  the modified data object handler
}
\description{
  Sets the cached description for the data object with the
  given value.  Usually called with \code{\link{describe}}.
}
\examples{
# The following command refreshes the cached description a
# DXGTable object
\dontrun{desc(dxgtable) <- describe(dxgtable)}
}


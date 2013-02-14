\docType{methods}
\name{id}
\alias{id}
\alias{id,DXGTable-method}
\title{Get the ID from a DNAnexus handler}
\arguments{
  \item{handler}{A DNAnexus handler}
}
\value{
  string ID of the referenced object
}
\description{
  Returns the string ID of the referenced object..
}
\examples{
dxgtable <- DXGTable("gtable-123456789012345678901234", describe=FALSE)
id(dxgtable)
}


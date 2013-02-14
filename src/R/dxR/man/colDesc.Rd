\name{colDesc}
\alias{colDesc}
\title{Create a GTable column descriptor}
\usage{
  colDesc(name, type)
}
\arguments{
  \item{name}{Name of the column}

  \item{type}{Type of the column, e.g. "boolean", "int",
  "double", "string"}
}
\value{
  a list that can be used as a column descriptor in
}
\description{
  Creates a column descriptor for creating a new
  GenomicTable on the platform.  See the API documentation
  for a full list of available data types and restrictions
  on column names.
}
\examples{
colDesc("chr", "string")
colDesc("lo", "int64")
colDesc("a_boolean", "boolean")
}
\seealso{
  \code{\link{newDXGTable}}
}


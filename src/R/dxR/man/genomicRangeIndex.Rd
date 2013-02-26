\name{genomicRangeIndex}
\alias{genomicRangeIndex}
\title{Create a genomic range index descriptor}
\usage{
  genomicRangeIndex(chr, lo, hi, name = "gri")
}
\arguments{
  \item{chr}{Name of the string column of chromosome names
  to be used}

  \item{lo}{Name of the integer column of low coordinates
  to be used}

  \item{hi}{Name of the integer column of high coordinates
  to be used}

  \item{name}{Name to give the created genomic range index;
  used when querying the GTable after it has been closed.}
}
\value{
  a list that can be used as an index descriptor when
  calling \code{\link{newDXGTable}}
}
\description{
  Creates a genomic range index descriptor describing which
  columns are to be used to create the index.
}
\examples{
genomicRangeIndex("chr", "lo", "hi")
genomicRangeIndex("chr", "lo", "hi", name="othergri")
}
\seealso{
  \code{\link{newDXGTable}},
  \code{\link{genomicRangeQuery}}
}


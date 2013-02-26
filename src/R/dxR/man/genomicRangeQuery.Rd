\name{genomicRangeQuery}
\alias{genomicRangeQuery}
\title{Construct a genomic range query for a GTable with a genomic range index}
\usage{
  genomicRangeQuery(chr, lo, hi, mode = "overlap",
    index = "gri")
}
\arguments{
  \item{chr}{Chromosome name}

  \item{lo}{Integer low coordinate}

  \item{hi}{Integer high coordinate}

  \item{mode}{The type of query to perform: "overlap"
  (default) or "enclose"}

  \item{index}{The name of the genomic range index to use}
}
\value{
  query to use as an argument to \code{\link{getRows}}
}
\description{
  Construct a genomic range query for a GTable for use with
  the \code{\link{getRows}} method.  The GTable must have
  been constructed with a genomic range index for the query
  to succeed. See the API documentation for details on the
  two modes ("enclose" and "overlap") of the query.
}
\examples{
genomicRangeQuery("chrI", 1000, 5000)
genomicRangeQuery("chrII", 1000, 10000, mode="enclose", index="othergri")
}
\seealso{
  Can be used when calling \code{\link{getRows}}
}


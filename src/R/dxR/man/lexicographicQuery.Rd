\name{lexicographicQuery}
\alias{lexicographicQuery}
\title{Construct a lexicographic query for a GTable with a lexicographic index}
\usage{
  lexicographicQuery(query, index)
}
\arguments{
  \item{query}{A MongoDB-style query}

  \item{index}{The name of the lexicographic index to use}
}
\value{
  query to use as an argument to \code{\link{getRows}}
}
\description{
  Construct a lexicographic query for a GTable for use with
  the \code{\link{getRows}} method.  The GTable must have
  been constructed with a lexicographic index for the query
  to succeed. See the API documentation for full details on
  how to construct the parameters for this query.
}
\examples{
lexicographicQuery(list(quality=list("$gt"=22)), "qualityIndex")
}
\seealso{
  Can be used when calling \code{\link{getRows}}
}


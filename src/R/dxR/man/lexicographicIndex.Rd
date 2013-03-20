\name{lexicographicIndex}
\alias{lexicographicIndex}
\title{Create a lexicographic index descriptor}
\usage{
  lexicographicIndex(columns, name)
}
\arguments{
  \item{columns}{List of lexicographic column descriptors
  as created via \code{\link{lexicographicIndexColumn}}}

  \item{name}{Name to give the created lexicographic index;
  used when querying the GTable after it has been closed.}
}
\value{
  a list that can be used as an index descriptor when
  calling \code{\link{newDXGTable}}
}
\description{
  Creates a lexicographic index descriptor describing which
  column(s) are to be used to create the index.
}
\examples{
lexicographicIndex(list("quality"), "qualityIndex")
lexicographicIndex(list("quality", "othercol"), "multiColIndex")
}
\seealso{
  \code{\link{newDXGTable}},
  \code{\link{lexicographicQuery}},
  \code{\link{lexicographicIndexColumn}}
}


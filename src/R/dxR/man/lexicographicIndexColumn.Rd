\name{lexicographicIndexColumn}
\alias{lexicographicIndexColumn}
\title{Create a lexicographic index column descriptor}
\usage{
  lexicographicIndexColumn(name, ascending = TRUE,
    caseSensitive = NA)
}
\arguments{
  \item{name}{Name of the column to be indexed}

  \item{ascending}{Whether to order entries of this column
  in ascending order}

  \item{caseSensitive}{If \code{FALSE}, compare strings
  case-insensitively (default is TRUE for string columns;
  note that this option should only be used for string
  columns and will be an error if used otherwise)}
}
\value{
  a list that can be used to create a lexicographic index
  descriptor via \code{\link{lexicographicIndex}}
}
\description{
  Creates a lexicographic index column descriptor to be
  used for creating a lexicographic index descriptor using
  \code{\link{lexicographicIndex}}.
}
\examples{
lexicographicIndexColumn("gene", caseSensitive=FALSE)
}
\seealso{
  \code{\link{lexicographicIndex}}
}


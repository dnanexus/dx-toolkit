\name{newDXGTable}
\alias{newDXGTable}
\title{Create a new GenomicTable and return a handler}
\usage{
  newDXGTable(columns, indices = NA,
    project = dxEnv$DEFAULT_PROJECT)
}
\arguments{
  \item{columns}{list of column descriptors}

  \item{indices}{optional list of index descriptors; if not
  provided, no indices are created when the GTable is
  closed}

  \item{project}{String project or container ID}
}
\value{
  An R object of class DXGTable
}
\description{
  Construct a GenomicTable(GTable) handler after creating a
  new GTable on the platform.  If a project ID is not
  provided, the new GTable will be created in the default
  project (project or a job's temporary workspace) is used
  if available.
}
\seealso{
  \code{\link{DXGTable}} for making a handler for an
  existing GTable.  \code{\link{colDesc}} for creating
  column descriptors.  \code{\link{genomicRangeIndex}} and
  \code{\link{lexicographicIndex}} for creating index
  descriptors.
}


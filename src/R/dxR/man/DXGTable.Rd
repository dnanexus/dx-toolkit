\name{DXGTable}
\alias{DXGTable}
\title{GenomicTable handler constructor}
\usage{
  DXGTable(id, project = dxEnv$DEFAULT_PROJECT,
    describe = TRUE)
}
\arguments{
  \item{id}{String object ID}

  \item{project}{String project or container ID}

  \item{describe}{Whether to cache a description of the
  gtable}
}
\value{
  An R object of class DXGTable
}
\description{
  Construct a GenomicTable(GTable) handler using an object
  ID of the form "gtable-xxxx".  If a project ID is not
  provided, the default project (project or a job's
  temporary workspace) is used if available.
}
\examples{
DXGTable("gtable-123456789012345678901234", describe=FALSE)
DXGTable("gtable-123456789012345678901234", project="project-12345678901234567890abcd", describe=FALSE)
}
\seealso{
  \code{\link{newDXGTable}} for creating a new GenomicTable
}


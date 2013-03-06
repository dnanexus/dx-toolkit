\name{dxHTTPRequest}
\alias{dxHTTPRequest}
\title{Make HTTP Request to DNAnexus API Server}
\usage{
  dxHTTPRequest(resource, data, headers = list(),
    jsonifyData = TRUE, alwaysRetry = FALSE)
}
\arguments{
  \item{resource}{String URI of API method, e.g.
  "/file/new", or "/class-xxxx/describe", where
  "class-xxxx" is some entity ID on the DNAnexus platform.}

  \item{data}{R object to be converted into JSON, using
  \code{RJSONIO::toJSON}.  If jsonifyData is set to FALSE,
  it is treated as a string value instead and passed
  through directly.}

  \item{headers}{List of HTTP headers to use, keyed by the
  header names.}

  \item{jsonifyData}{Whether to call \code{RJSONIO::toJSON}
  on \code{data} to create the JSON string or pass through
  the value of \code{data} directly. (Default is
  \code{TRUE}.)}

  \item{alwaysRetry}{Whether to always retry the API call
  (assuming a non-error status code was received).}
}
\value{
  If the API call is successful, the parsed JSON of the API
  server response is returned (using
  \code{RJSONIO::fromJSON}).
}
\description{
  Makes a POST HTTP Request to the DNAnexus API Server
  using stored configuration values.
}
\examples{
# Basic API call; use RJSONIO::namedEmptyList for an empty hash
\dontrun{dxHTTPRequest("/gtable-xxxx/get", namedEmptyList)}

# API call with nonempty input hash
\dontrun{dxHTTPRequest("/record/new", list("project"="project-xxxx"))}
}
\seealso{
  \code{\link{printenv}}
}


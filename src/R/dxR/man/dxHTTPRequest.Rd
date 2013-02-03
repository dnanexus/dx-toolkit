\name{dxHTTPRequest}
\alias{dxHTTPRequest}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Makes HTTP Request to DNAnexus API Server
%%  ~~function to do ... ~~
}
\description{
%%  ~~ A concise (1-5 lines) description of what the function does. ~~
Makes a POST HTTP Request to the DNAnexus API Server using stored configuration values.
}
\usage{
dxHTTPRequest(resource, data, headers = list(), jsonifyData = TRUE, alwaysRetry = FALSE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{resource}{
%%     ~~Describe \code{resource} here~~
String URI of API method, e.g. "/file/new", or "/class-xxxx/describe", where "class-xxxx" is some entity ID on the DNAnexus platform.
}
  \item{data}{
%%     ~~Describe \code{data} here~~
R object to be converted into JSON, using \code{RJSONIO::toJSON}.  If jsonifyData is set to FALSE, it is treated as a string value instead and passed through directly.
}
  \item{headers}{
%%     ~~Describe \code{headers} here~~
List of HTTP headers to use, keyed by the header names.
}
  \item{jsonifyData}{
    Whether to call \code{RJSONIO::toJSON} on \code{data} to create the JSON string or pass through the value of \code{data} directly.  (Default is \code{TRUE}.)
}
  \item{alwaysRetry}{
%%     ~~Describe \code{alwaysRetry} here~~
Whether to always retry the API call (assuming a non-error status code was received).
}
}
\details{
%%  ~~ If necessary, more details than the description above ~~
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
If the API call is successful, the parsed JSON of the API server response is returned (using \code{RJSONIO::fromJSON}).
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

\seealso{
\code{\link{printenv}}
}
\examples{
\dontrun{
# Basic API call; use RJSONIO::namedEmptyList for an empty hash
dxHTTPRequest("/gtable-xxxx/get", namedEmptyList)

# API call with nonempty input hash
dxHTTPRequest("/record/new", list("project"="project-xxxx"))
}
}
\name{recordDescribe}
\alias{recordDescribe}
\title{recordDescribe API wrapper}
\description{
This function makes an API call to the \code{/record-xxxx/describe} API method; it is a simple wrapper around the \code{\link{dxHTTPRequest}} function which makes POST HTTP requests to the API server.
}
\usage{
recordDescribe(objectID, inputParams = RJSONIO::emptyNamedList, alwaysRetry = TRUE)
}
\arguments{
\item{objectID}{DNAnexus object ID}
\item{inputParams}{Either an R object that will be converted into JSON using \code{RJSONIO::toJSON} to be used as the input to the API call.  If providing the JSON string directly, you must set \code{jsonifyData} to \code{FALSE}.}
\item{alwaysRetry}{Whether to always retry even when no response is received from the API server}
}
\value{
If the API call is successful, the parsed JSON of the API server response is returned (using \code{RJSONIO::fromJSON}).
}
\seealso{
\code{\link{dxHTTPRequest}}
}

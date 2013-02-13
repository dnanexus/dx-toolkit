\name{appUpdate}
\alias{appUpdate}
\title{appUpdate API wrapper}
\usage{
  appUpdate(appNameOrID, alias = NULL,
    inputParams = emptyNamedList, jsonifyData = TRUE,
    alwaysRetry = TRUE)
}
\arguments{
  \item{appNameOrID}{An app identifier using either the
  name of an app ("app-name") or its full ID ("app-xxxx")}

  \item{alias}{If an app name is given for
  \code{appNameOrID}, this can be provided to specify a
  version or tag (if not provided, the "default" tag is
  used).}

  \item{inputParams}{Either an R object that will be
  converted into JSON using \code{RJSONIO::toJSON} to be
  used as the input to the API call.  If providing the JSON
  string directly, you must set \code{jsonifyData} to
  \code{FALSE}.}

  \item{jsonifyData}{Whether to call \code{RJSONIO::toJSON}
  on \code{inputParams} to create the JSON string or pass
  through the value of \code{inputParams} directly.
  (Default is \code{TRUE}.)}

  \item{alwaysRetry}{Whether to always retry even when no
  response is received from the API server}
}
\value{
  If the API call is successful, the parsed JSON of the API
  server response is returned (using
  \code{RJSONIO::fromJSON}).
}
\description{
  This function makes an API call to the
  \code{/app-xxxx/update} API method; it is a simple
  wrapper around the \code{\link{dxHTTPRequest}} function
  which makes POST HTTP requests to the API server.
}
\seealso{
  \code{\link{dxHTTPRequest}}
}


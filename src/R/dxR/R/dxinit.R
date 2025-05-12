# Copyright (C) 2013-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

##' @import RCurl RJSONIO

# dxEnv is an R environment storing config variables
dxEnv <- new.env()

kAPIVersion <- '1.0.0'
kNumMaxRetries <- 10

envVariables <- c('DX_APISERVER_HOST',
                  'DX_APISERVER_PORT',
                  'DX_APISERVER_PROTOCOL',
                  'DX_PROJECT_CONTEXT_ID',
                  'DX_WORKSPACE_ID',
                  'DX_JOB_ID',
                  'DX_CLI_WD',
                  'DX_USERNAME',
                  'DX_PROJECT_CONTEXT_NAME',
                  'DX_SECURITY_CONTEXT')

# Parse config options saved by dx, the command-line client
getFromJSONEnv <- function() {
  configjson <- tryCatch({
    RJSONIO::fromJSON("~/.dnanexus_config/environment.json")
  }, error=function(e) {
    return (emptyNamedList)
  })
  for (varname in names(configjson)) {
    if (dxEnv[[varname]] == '') {
      # Only use the value found if it was *not* found in the
      # environment variables
      assign(varname, configjson[[varname]], envir=dxEnv)
    }
  }
}

##' (Re)Load DNAnexus Configuration Values
##'
##' Load (or reload) state from:
##' 1) shell environment variables,
##' 2) configuration options saved by dx (the DNAnexus command-line client),
##' 3) hardcoded defaults
##'
##' @export
loadFromEnvironment <- function() {
  for (varname in envVariables) {
    assign(varname, Sys.getenv(varname), envir=dxEnv)
  }

  getFromJSONEnv()

  if (dxEnv$DX_APISERVER_HOST == '') {
    assign('DX_APISERVER_HOST', "api.dnanexus.com", envir=dxEnv)
  }
  if (dxEnv$DX_APISERVER_PORT == '') {
    assign('DX_APISERVER_PORT', "443", envir=dxEnv)
  }
  if (dxEnv$DX_APISERVER_PROTOCOL == '') {
    assign('DX_APISERVER_PROTOCOL', "https", envir=dxEnv)
  }

  if (dxEnv$DX_JOB_ID == '') {
    assign('DEFAULT_PROJECT', dxEnv$DX_PROJECT_CONTEXT_ID, envir=dxEnv)
  } else {
    assign('DEFAULT_PROJECT', dxEnv$DX_WORKSPACE_ID, envir=dxEnv)
  }
}



##' Print DNAnexus Configuration Values
##' 
##' Prints the current set of configuration values that are being used to
##' contact the DNAnexus platform.
##' 
##' @examples
##' printenv()
##' @export
printenv <- function() {
  cat("Currently loaded environment:\n")
  for (varname in envVariables) {
    if (dxEnv[[varname]] != '') {
      cat("  ", varname, ": ", dxEnv[[varname]], "\n", sep="")
    }
  }
}

.onLoad <- function(libname, pkgname){
  loadFromEnvironment()
}



##' Make HTTP Request to DNAnexus API Server
##' 
##' Makes a POST HTTP Request to the DNAnexus API Server using stored
##' configuration values.
##' 
##' @param resource String URI of API method, e.g. "/file/new", or
##' "/class-xxxx/describe", where "class-xxxx" is some entity ID on
##' the DNAnexus platform.
##' @param data R object to be converted into JSON, using
##' \code{RJSONIO::toJSON}.  If jsonifyData is set to FALSE, it is
##' treated as a string value instead and passed through directly.
##' @param headers List of HTTP headers to use, keyed by the header
##' names.
##' @param jsonifyData Whether to call \code{RJSONIO::toJSON} on \code{data} to
##' create the JSON string or pass through the value of \code{data} directly.
##' (Default is \code{TRUE}.)
##' @param alwaysRetry Whether is it safe to retry the API call, meaning request
##' is idempotent (assuming a non-error status code was received).
##' @return If the API call is successful, the parsed JSON of the API
##' server response is returned (using \code{RJSONIO::fromJSON}).
##' @seealso \code{\link{printenv}}
##' @examples
##' 
##' # Basic API call; use RJSONIO::namedEmptyList for an empty hash
##' \dontrun{dxHTTPRequest("/gtable-xxxx/get", namedEmptyList)}
##'
##' # API call with nonempty input hash
##' \dontrun{dxHTTPRequest("/record/new", list("project"="project-xxxx"))}
##' @export
dxHTTPRequest <- function(resource, data,
                          headers=list(),
                          jsonifyData=TRUE,
                          alwaysRetry=FALSE) {
  # option wasn't named correctly, so to not break existing clients rename it locally
  safeToRetry=alwaysRetry
  url <- paste(dxEnv$DX_APISERVER_PROTOCOL, "://",
               dxEnv$DX_APISERVER_HOST, ':', dxEnv$DX_APISERVER_PORT,
               resource,
               sep='')
  if (!"Content-Type" %in% colnames(headers)) {
    headers["Content-Type"] <- "application/json"
  }
  if (dxEnv$DX_SECURITY_CONTEXT != '') {
    secContext <- RJSONIO::fromJSON(dxEnv$DX_SECURITY_CONTEXT)
    headers["Authorization"] <- paste(secContext[["auth_token_type"]],
                                      secContext[["auth_token"]])
  }
  headers["DNAnexus-API"] <- kAPIVersion

  if (jsonifyData) {
    body <- RJSONIO::toJSON(data)
  } else {
    if (!is.character(data)) {
      stop("The given data was not a string of characters even though jsonifyData was set to FALSE.")
    }
    body <- data
  }

  # DEBUG:
  # print(body)

  h <- RCurl::basicTextGatherer()
  d <- RCurl::basicHeaderGatherer()
  exponentialSecondsToWait <- 2
  attempts <- 0
  attemptsWithThrottling <- 0
  while (TRUE) {
    secondsToWait <- exponentialSecondsToWait
    toRetry <- FALSE
    curlRetry <- FALSE
    isThrottlingError <- FALSE
    errorMsg <- ""

    curlResult <- tryCatch({
      RCurl::curlPerform(url=url,
                         httpheader=headers,
                         useragent=paste('dxR/', packageVersion("dxR"), sep=''),
                         postfields=body,
                         writefunction=h$update,
                         headerfunction=d$update,
                         cainfo=system.file("extdata", "ca-certificates.crt", package="dxR"))
    }, error=function(e) {
      return (e)
    })

    if ("GenericCurlError" %in% class(curlResult)) {
      if ("FAILED_INIT" %in% class(curlResult) ||
          "COULDNT_RESOLVE_PROXY" %in% class(curlResult) ||
          "COULDNT_RESOLVE_HOST" %in% class(curlResult) ||
          "COULDNT_CONNECT" %in% class(curlResult) ||
          "SSL_CONNECT_ERROR" %in% class(curlResult)) {
        # Some Curl errors are always safe to retry
        toRetry <- TRUE
        curlRetry <- TRUE
      } else {
        # Stop for all others
        stop(curlResult$message, call.=FALSE)
      }
    } else {
      statusCode <- as.numeric(d$value()['status'])
      if (statusCode == 200) {
        # DEBUG:
        # print(h$value())
        if ('Content-Length' %in% names(d$value())) {
          if (as.numeric(d$value()['Content-Length']) != nchar(h$value(), type="bytes")) {
            # Content-Length mismatch -> retry
            errorMsg <- "Content-Length mismatch"
            toRetry <- safeToRetry
          } else if (RJSONIO::isValidJSON(h$value(), TRUE)) {
            # Content-Length match && valid JSON
            return (RJSONIO::fromJSON(h$value()))
          } else {
            # Content-Length match && invalid JSON -> error
            stop('Invalid JSON received from server', call.=FALSE)
          }
        } else if (RJSONIO::isValidJSON(h$value(), TRUE)) {
          # No Content-Length header && valid JSON
          return (RJSONIO::fromJSON(h$value()))
        } else {
          errorMsg <- "No Content-Length header && invalid JSON"
          toRetry <- safeToRetry
        }
      } else if (statusCode == 429 || statusCode >= 500) {
        toRetry <- safeToRetry

        if ('retry-after' %in% names(d$value())) {
          toRetry <- TRUE
          suggestSecondsToWait <- as.numeric(d$value()['retry-after'])

          if (!is.na(suggestSecondsToWait) && suggestSecondsToWait < 120) {
            isThrottlingError <- TRUE

            # By default, apiserver doesn't track attempts and doesn't provide increased timeout over attempts.
            # So, increasing backoff for throttled requests up to x5 times from the original one.
            # The current implementation of apiserver returns a Retry-After header ranging from 20 to 30 seconds.
            # Thus, after the 20th attempt the delay will always be between 100 and 150 seconds.
            incrementedBackoff <- as.integer(0.25 * min(20, attemptsWithThrottling) * suggestSecondsToWait)
            secondsToWait <- suggestSecondsToWait + incrementedBackoff
          }
        }
      } else {
        if (RJSONIO::isValidJSON(h$value(), TRUE)) {
          errorHash <- RJSONIO::fromJSON(h$value())
          if ('error' %in% names(errorHash) &&
              'type' %in% names(errorHash$error) &&
              'message' %in% names(errorHash$error)) {
            stop(paste(errorHash$error['type'], ': ',
                       errorHash$error['message'], ', code ',
                       statusCode, sep=''),
                 call.=FALSE)
          } else {
            stop(paste(h$value(), ', code ', statusCode, sep=''))
          }
        } else {
          stop(paste(h$value(), ', code ', statusCode, sep=''))
        }
      }
    }

    errorMsg <- ifelse(
      curlRetry,
      paste("WARNING: POST", url, "had an error:", curlResult$message),
      paste("WARNING: POST", url, "returned with HTTP code", statusCode, errorMsg, "and body", h$value())
    )

    write(errorMsg, stderr())

    # Throttling errors can be retried indefinitely
    if (
      !toRetry || (attempts >= kNumMaxRetries && !isThrottlingError)
    ) {
      stop(paste("POST", url, "was unsuccessful; out of retries"), call.=FALSE)
    }

    throttlingErrorMsg <- ifelse(
      isThrottlingError,
      paste("Waiting", secondsToWait, "seconds before retry after", attemptsWithThrottling + 1, "attempts ..."),
      paste("Waiting", secondsToWait, "seconds before retry", attempts + 1, "of", kNumMaxRetries, "...")
    )

    write(throttlingErrorMsg, stderr())
    Sys.sleep(secondsToWait)

    attemptsWithThrottling <- attemptsWithThrottling + 1
    if (!isThrottlingError) {
      attempts <- attempts + 1

      if (attempts < 7) {
        # Max exponential backoff is 64 seconds
        exponentialSecondsToWait <- 2 * exponentialSecondsToWait
      }
    }

    h$reset()
    d$reset()
  }
}

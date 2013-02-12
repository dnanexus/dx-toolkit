# Copyright (C) 2013 DNAnexus, Inc.
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

# dxEnv is an R environment storing config variables
dxEnv <- new.env()

kAPIVersion <- '1.0.0'
kNumMaxRetries <- 5

envVariables <- c('DX_APISERVER_HOST',
                  'DX_APISERVER_PORT',
                  'DX_APISERVER_PROTOCOL',
                  'DX_PROJECT_CONTEXT_ID',
                  'DX_WORKSPACE_ID',
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
    assign(varname, configjson[[varname]], envir=dxEnv)
  }
}

# Load (or reload) state from
# 1) Shell environment variables
# 2) Config options saved by dx
# 3) hardcoded defaults
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
}

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

dxHTTPRequest <- function(resource, data,
                          headers=list(),
                          jsonifyData=TRUE,
                          alwaysRetry=FALSE) {
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

  h <- RCurl::basicTextGatherer()
  d <- RCurl::basicHeaderGatherer()
  secondsToWait <- 2
  for (i in 1:(kNumMaxRetries + 1)) {
    toRetry <- FALSE
    curlRetry <- FALSE

    curlResult <- tryCatch({
      RCurl::curlPerform(url=url,
                         httpheader=headers,
                         postfields=body,
                         writefunction=h$update,
                         headerfunction=d$update)
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
        if ('Content-Length' %in% d$value() &&
            as.numeric(d$value()['Content-Length']) != nchar(h$value(), type="bytes")) {
          toRetry <- TRUE
        } else {
          return (RJSONIO::fromJSON(h$value()))
        }
      } else if (statusCode >= 500 && statusCode <= 599) {
        toRetry <- TRUE
      } else {
        if (RJSONIO::isValidJSON(h$value(), TRUE)) {
          errorHash <- RJSONIO::fromJSON(h$value())
          print(names(errorHash))
          if ('error' %in% names(errorHash) &&
              'type' %in% names(errorHash$error) &&
              'messsage' %in% names(errorHash$error)) {
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

    if (toRetry && i < kNumMaxRetries) {
      if (curlRetry) {
        write(paste("WARNING: POST", url, "had an error:", curlResult$message),
              stderr())
      } else {
        write(paste("WARNING: POST", url, "returned with HTTP code",
                    statusCode, "and body", h$value()),
              stderr())
      }
      write(paste("Waiting", secondsToWait, "seconds before retry",
                  i, "of", kNumMaxRetries, "..."),
            stderr())
      Sys.sleep(secondsToWait)
    } else if (toRetry) {
      stop(paste("POST", url, "was unsuccessful; out of retries"),
           call.=FALSE)
    }

    secondsToWait <- 2 * secondsToWait
  }
}

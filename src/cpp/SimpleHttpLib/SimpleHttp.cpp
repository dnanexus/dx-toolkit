#include "SimpleHttp.h"

#ifndef WINDOWS_BUILD
  // We don't use openssl in windows, so do not use thread safety mechanism
  // in SSLThreads.h/.cpp
  #include "SSLThreads.h"

class SSLThreadsInitializer
{
public:
  SSLThreadsInitializer()
  {
//    std::cerr << "SimpleHttp: Initializing openssl for thread safety -> Calling SSLThreadsSetup()" << std::endl;
    SSLThreadsSetup();
  }

  ~SSLThreadsInitializer()
  {
//    std::cerr << "SimpleHttp: Cleaning up openssl thread safety mechanism -> Calling SSLThreadsCleanup()" << std::endl;
    SSLThreadsCleanup();
  }
};

// Our aim is to call this SSLThreadsSetup() once in beginning
// and SSLThreadsCleanup() at the end
// See: http://horstr.blogspot.com/2008/04/on-libcurl-openssl-and-thread-safety.html
SSLThreadsInitializer SSLThreads_initializer;
#endif

/*
 * This function serves as a callback for response headers read by libcurl
 *
 * The libcurl documentation at
 * http://curl.haxx.se/libcurl/c/curl_easy_setopt.html#CURLOPTHEADERFUNCTION
 * says that the callback is called once for each header and only complete
 * header line are passed. So we do not need to handle multi-line headers
 * ourselves.
 *
 * Please see: http://www.w3.org/Protocols/rfc2616/rfc2616-sec6.html
 *   The first response header returned will always be "Status-line",
 * Also see: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
 */
static size_t headers_callback(void *buffer, size_t size, size_t nmemb, void *userp) {
  char *buf = reinterpret_cast<char*>(buffer);
  HttpHeaders *pHeaders = reinterpret_cast<HttpHeaders*>(userp);
  size_t result = 0u;
  if (pHeaders != NULL) {
    /*
     * Note: std::string is capable of storing binary stream data and can
     * store "\0" as normal character (witout terminating string).
     *
     * - Until C++11, it is not guaranteed to be contiguously stored though.
     * - Use caution when using .c_str(), if string contains "\0", use .data() instead
     *
     * Since binary data can be stored in std::string, unicode characters
     * can be present But beware of using .length() or .size(), since they
     * will return number of bytes storage (i.e., char) needed to store the
     * string, and not actual number of characters.
     */
    std::string s = "";
    s.append(buf, size * nmemb);
    result = size * nmemb;

    // There can be 3 different cases, each should be handled differently:

    // Case 1: Check for last header line: CRLF,
    //         http://www.w3.org/Protocols/rfc2616/rfc2616-sec6.html
    if (s.size() == 2u && s[0] == '\r' && s[1] == '\n')
      return result;

    // Case 2: If it is the first header, the it must be a status Line.
    // For all other cases, statusLine must be populated with some non-zero length string
    if (pHeaders->getStatusLine().length() == 0u) {
      pHeaders->setStatusLine(HttpHelperUtils::stripWhitespaces(s));
      return result;
    }

    // Case 3: This is a usual message header, of form
    // message-header = field-name ":" [ field-value ]
    // http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
    pHeaders->appendHeaderString(s);
  }

  return result;
}

/* Callback for response data */
static size_t write_callback(void *buffer, size_t size, size_t nmemb, void *userdata) {
  char *buf = reinterpret_cast<char*>(buffer);
  std::string *response = reinterpret_cast<std::string*>(userdata);
  size_t result = 0u;
  if (userdata != NULL) {
    response->append(buf, size * nmemb);
    result = size * nmemb;
  }
  return result;
}

/* Callback for reading request data (for e.g., in PUT) */
static size_t read_callback(void *data, size_t size, size_t nmemb, void *userdata) {
  HttpRequest::reqData_struct *u = static_cast<HttpRequest::reqData_struct*>(userdata);

  // Set correct sizes
  size_t curl_size = size * nmemb; // This is the maximum size curl asked for
  size_t copy_size = (u->length < curl_size) ? u->length : curl_size;

  //Copy data to buffer
  memcpy(data, u->data, copy_size);

  // Decrement the length and increment the data pointer
  u->length -= copy_size;
  u->data += copy_size;

  // Return the copied size
  return copy_size;
}

// convert int to string
static std::string itos(int i)  {
  std::stringstream s;
  s << i;
  return s.str();
}

void HttpRequest::assertLibCurlFunctions(CURLcode retVal, const std::string &msg = "") {
  if (retVal != CURLE_OK) {
    // See http://curl.haxx.se/libcurl/c/libcurl-errors.html to interpret error code (if positive)
    // If error code is negative, then see SimpleHttp.h
    std::string exceptionStr = "\n*******";
    if (msg.size() > 0u)
      exceptionStr += "\n" + msg;
    exceptionStr += "\nError code (CURLcode) = " + itos(retVal) + "\nError Message: '";
    errorBuffer[CURL_ERROR_SIZE] = 0;
    exceptionStr += std::string(errorBuffer) + "'";
    exceptionStr += "\n********\n";
    // If a response code is available - get it's value 
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
    throw HttpRequestException(exceptionStr, retVal);
  }
}

//////////////////////////////////////////////////
/////////// Class method defintions //////////////
//////////////////////////////////////////////////

// This function makes the actual http request.
// Throws HttpRequestException in case of an error
// HttpRequestException::errorCode 
//    - is positive if error is due to a failed libcurl function (and is == returned curl_code by function)
//    - is never 0 (since 0 = CURLE_OK)
//    - is negative (one of static const value defined in HttpRequestException class)
//      if error is due to some other reason.
void HttpRequest::send() {
  // TODO: Not call curl_easy_cleanup() always at end of send()
  //       Instead allow to reuse the same curl handle for subsequent requests
  //       curl will reuse the existing connection this way - should be much faster

  // This function should never be called while "curl" member variable is in use
  if (curl != NULL)
    throw HttpRequestException("ERROR: curl member variable is already in use. Cannot be reused until previous operation is complete", HttpRequestException::ALREADY_IN_USE);

  curl = curl_easy_init();

  if (curl != NULL) {
    // Set errorBuffer to recieve human readable error messages from libcurl
    // http://curl.haxx.se/libcurl/c/curl_easy_setopt.html#CURLOPTERRORBUFFER
    assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, &errorBuffer));

    respData = "";
    // Set time out to infinite
    assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_TIMEOUT, 0l));

    /* Set the user agent - optional */
    assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_USERAGENT, "DNAnexus: libcurl-C++ wrapper"));

    /* Setting this option, since libcurl fails in multi-threaded environment otherwise */
    /* See: http://curl.haxx.se/libcurl/c/libcurl-tutorial.html#Multi-threading */
    assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1l));

    /* Set the header(s) */
    curl_slist *header = NULL;
    std::vector<std::string> header_vec;
    header_vec = reqHeader.getAllHeadersAsVector(); // inefficient quick hack, use iterator instead
    for (unsigned i = 0;i < header_vec.size(); i++) {
      header = curl_slist_append(header, header_vec[i].c_str());
    }

    if (header != NULL) {
      assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_HTTPHEADER, header));
    }
//    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);

    /*
     * Set the URL that is about to receive our POST. This URL can
     * just as well be a https:// URL if that is what should receive the
     * data.
     */
    assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_URL, url.c_str()));

    // Make a copy of reqData, because read_callback (see HTTP_PUT case below) will modify it
    reqData_struct reqData_temp;

    switch (method) {
      case HTTP_POST:
        assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_POST, 1L));
        // Always adding a body to POST request (might be empty)
        assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_POSTFIELDS, reqData.data));
        // To disallow strlen() on reqData, setting POSTFIELDSIZE explicitly
        // http://curl.haxx.se/libcurl/c/curl_easy_setopt.html#CURLOPTPOSTFIELDSIZE
        assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, reqData.length));
        break;
      case HTTP_PUT:
        reqData_temp = reqData; // Make a copy, since it will be modified
        // Set the request type to PUT
        // Using two methods to do it just to be safe
        // NOTE: CURLOPT_PUT will be deprecated in future libcurl)
        assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_PUT, 1L));
        assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L));

        if (reqData.length > 0u) {
         // Now set the read_call back function.
          assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback));
          /** set data object to pass to callback function */
          assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_READDATA, &reqData_temp));
        }
        break;
      case HTTP_GET:
        assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_HTTPGET, 1l));
        break;
      case HTTP_DELETE:
        assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE"));
        break;
      case HTTP_HEAD:
        assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_NOBODY, 1l));
        break;
      default:
        throw HttpRequestException("Unknown HttpMethod type", HttpRequestException::UNSUPPORTED_HTTP_METHOD);
    }

    // Set callback for receiving headers from the response
    assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headers_callback));
    // "respHeader" is a member variable referencing HttpHeaders
    assertLibCurlFunctions(curl_easy_setopt(curl, CURLOPT_WRITEHEADER, &respHeader));

    // Set callback for recieving the response data
    /** set callback function */
    assertLibCurlFunctions( curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback) );
    /** "respData" is a member variable of HttpRequest */
    assertLibCurlFunctions( curl_easy_setopt(curl, CURLOPT_WRITEDATA, &respData) );

    /* Perform the actual request */
    assertLibCurlFunctions( curl_easy_perform(curl), "Error in using curl_easy_perform.");

    assertLibCurlFunctions( curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode) );

    /* always cleanup */
    curl_easy_cleanup(curl);
    curl = NULL;
  } else {
    throw HttpRequestException("Error: Unable to initialize object of type CURL", HttpRequestException::INIT_FAILED);
  }
}

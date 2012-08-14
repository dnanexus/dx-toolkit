var DNAnexus = {};

(function() {

  var http = require('http');
  var process = require('process');
  var fs = require('fs');

  var security_context = system.env.SECURITY_CONTEXT;
  if (security_context === undefined) {
    throw new Error("DNAnexus.js: SECURITY_CONTEXT environment variable not set");
  }
  security_context = JSON.parse(security_context);

  var hasOwnProperty = Object.prototype.hasOwnProperty;

  var apiserverHost = (system.env.APISERVER_HOST === undefined) ? "127.0.0.1" : system.env.APISERVER_HOST;
  var apiserverPort = (system.env.APISERVER_PORT === undefined) ? "8124" : system.env.APISERVER_PORT;
  var apiserverPath = apiserverHost + ":" + apiserverPort;


  /** @brief Returns true if input is of type Object
   *  @internal
   *  @tparam Any_Type obj The variable to be tested
   *  @return Boolean : true or false
   */
  function isObject(obj) {
    return obj == Object(obj);
  }

  /** @brief Returns true if input if of type Object and empty, false otherwise.
   *  @internal
   *  @tparam Object obj The variable to be tested for being an empty object
   *  @return Boolean : true or false.
   */
  function isEmptyObject(obj) {
    var key;
    if (!isObject(obj)) {
      return false;
    }

    for (key in obj) {
      if (hasOwnProperty.call(obj, key)) {
        return false;
      }
    }
    return true;
  }

  /** @brief Makes an http request to apiserver using v8cgi's http module
   *  @internal
   *  This function provides a wrapper for making HTTP request via http module
   *  of v8cgi. The input request for this function should be built with buildReq()
   *  @tparam Hash inputReq A JSON hash describing HTTP Request parameters
   *  @markdown
   *  - _url_: (required) A string containing url for HTTP request
   *  - _method_: (required) A string containing HTTP request type (ex: "GET", "POST")
   *  - _header_: (optional) A hash/sring containing header to be sent with request.
   *  - _data_: (optional) A hash/string containing data to be  sent with request.
   *  @endmarkdown
   *  @return The full response of http request.
  */
  function httpReq(inputReq) {
    var request = new http.ClientRequest(apiserverPath + inputReq.url);
    request.method = inputReq.method.toUpperCase();

    if (inputReq.method.toUpperCase() in {'POST': '', 'PUT': '', 'DELETE': ''} && inputReq.data != undefined) {

      // v8cgi http seems to fail if object is empty (so explicitly convert it into empty object string)
      if (isObject(inputReq.data)) {
        inputReq.data = JSON.stringify(inputReq.data);
      } else { // For case of Arrays, etc
        inputReq.data = inputReq.data.toString("utf-8");
      }
      request.post = inputReq.data;
    }
    if (inputReq.header != undefined) {
      request.header(inputReq.header);
    }
    return request.send(false);
  }

  /** @brief Builds header to be sent with while making http request.
   *  @internal
   *  This function generates header to be sent with http request.
   *  User can specify wheter the request data is JSON, and/or, request is authenticated.
   *  @tparam Boolean boolIsJSON If true, content-type will be set to application/json in header.
   *  @tparam Boolean boolIsAuthenticated Is true, security context will be provided in header.
   *  @return Header(s) as a JSON object.
   */
  function buildHeader(boolIsJSON, boolIsAuthenticated) {
    // Default values for either of the arguments is false
    boolIsJSON = (typeof boolIsJSON) === "undefined" ? false : boolIsJSON;
    boolIsAuthenticated = (typeof boolIsAuthenticated) === "undefined" ? false : boolIsAuthenticated;
    var header = {};
    if (boolIsJSON) {
      header["Content-Type"] = "application/json";
    }
    if (boolIsAuthenticated) {
      header.Authorization = job.security_context.auth_token_type + " " + job.security_context.auth_token;
    }
    return header;
  }

  /** @brief This function is used for verifying status code of http req.
   *  @internal
   *  Matches returnedCode and expectedCode values, if they differ then
   *  an exception is thrown.
   *  @tparam Integer returnedCode Status code returned by server.
   *  @tparam Integer expectedCode Expected value for status code.
   *  @tparam String route (optional) The path for HTTP request, if provided will be added
   *   in exception thrown by the function.
   */
  function verifyStatusCode(returnedCode, expectedCode, route) {
    if (route === undefined) {
      route = "";
    }
    if (returnedCode != expectedCode) {
      throw new Error("Unexpected HTTP response code while accessing route \"" + route + "\". Expected: " + expectedCode + " Got: " + returnedCode);
    }
  }

  /** @brief Builds http request, to be used by httpReq()
   *  @internal
   *  This functions builds a request in format required by httpReq()
   *  @tparam String url URL for http request.
   *  @tparam String method HTTP method to be used for request.
   *  @tparam Hash header Headers to be sent with the request.
   *  @tparam Hash/String data Data to be sent as body of request.
   *  @return A hash, ready to be sent to httpReq()
   */
  function buildReq(url, method, header, data) {
    return {url: url, method: method, header: header, data: data};
  }

  /** @brief Converts server's response (utf-8 string) into valid JSON.
   *  @internal
   *  @tparam String data The "data" field of response of v8cgi http module.
   *  @return A JSON object parsed from input "data" (assuming utf-8 encoding).
   */
  function getResponseDataJSON(data) {
    return JSON.parse(data.toString('utf-8'));
  }

  /** @brief Sets global variable for API server path to given value
   *  @internal
   *  @tparam String path The ApiServer path
   */
  DNAnexus.setApiServerPath = function(path) {
    apiserverPath = path;
  };

  /** @brief Returns value of global variable apiServerPath
   *  @internal
   *  @return String Value of apiServerPath
   */
  DNAnexus.getApiServerPath = function() {
    return apiserverPath;
  };

  /** @brief Creates a new user with specified password
   *  @publish
   *  This function creates a new user, and set initial password to the given value.
   *  @tparam String password (required) The password to be associated with new user.
   *  @return ID_Type: ID of newly created user object.
   *  @route POST: /users
   */
  DNAnexus.createUser = function(password, username) {
    var resp = httpReq(buildReq("/users", "POST", buildHeader(true, false), {password: password, name: username}));
    verifyStatusCode(resp.status, 200, "POST /users");
    return getResponseDataJSON(resp.data).id;
  };

  /** @brief Return IDs and URLs of all objects user has access to.
   *  @publish
   *  This function returns all Object of type "object" which user has access to.
   *  @return An array of hashes of form {id: ..., url: ...}.
   *   Each hash in array represent a single object.
   *  @route GET: /objects
   */
  DNAnexus.listObjects = function() {
    var resp = httpReq(buildReq("/objects", "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /objects");
    return getResponseDataJSON(resp.data);
  };

  // This function is a loner in the sense: it's input does not directly map to API
  // but rather broken down for better readibility/accessibility to end-user
  // This loses on consistency with API call
  /** @brief Creates a new Object of type object
   *  @publish
   *  This function creates a new Object of type "object". Optionally user
   *  can specify properties and relationships for this new object.
   *  @tparam Hash propertiesHash (optional) A hash of key/value pairs, each will be added
   *  as property/value of new object.
   *  @tparam Array relationshipsArray (optional) An array containing following two fields
   *  @markdown
   *  - name : A string containing name of the relationship.
   *  - targets : An array of object URLs (each being an string).
   *  @endmarkdown
   *  @return ID_Type: ID of new object.
   *  @route POST: /objects
   */
  DNAnexus.createEmptyObject = function(propertiesHash, relationshipsArray) {
    var data = {};
    if (propertiesHash != null) {
      data.properties = propertiesHash;
    }
    if (relationshipsArray != null) {
      data.relationships = relationshipsArray;
    }
    var resp = httpReq(buildReq("/objects", "POST", buildHeader(true, true), data));
    verifyStatusCode(resp.status, 201, "POST /objects");
    return getResponseDataJSON(resp.data).id;
  };

  /** @brief: Return object's properties and relationships.
   *  @publish
   *  This function returns a hash containing all properties and relationships
   *  of an object.
   *  @tparam ID_Type id ID of the object
   *  @return Hash containing two fields:
   *  @markdown
   *  -_properties_: All properties of object(Hash of key/value pairs)
   *  -_relationships_: Array of hashes in the form {name: "relationship_name", targets:[obj_urls]}
   *  @endmarkdown
   *  @route GET: /objects/:id
   */
  DNAnexus.getObjectPropertiesAndRelationships = function(id) {
    var resp = httpReq(buildReq("/objects/" + id, "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /objects/:id");
    return getResponseDataJSON(resp.data);
  };

  /** @brief: Return object properties.
   *  @publish
   *  This function returns a hash of object properties as key/value pairs.
   *  @tparam ID_Type id ID of the object
   *  @return Hash containing key/value pairs - each representing a single property.
   *  @route GET: /object/:id/properties
   */
  DNAnexus.getObjectProperties = function(id) {
    var resp = httpReq(buildReq("/objects/" + id + "/properties", "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /objects/:id/properties");
    return getResponseDataJSON(resp.data);
  };

  /** @brief: Create/set multiple properties.
   *  @publish
   *  Update (or create if not present) properties.
   *  @tparam ID_Type id ID of the object
   *  @tparam Hash propertiesHash A hash containing key/value pairs -
   *   each representing a single property.
   *  @route POST: /object/:id/properties
   */
  DNAnexus.updateObjectProperties = function(id, propertiesHash) {
    var resp = httpReq(buildReq("/objects/" + id + "/properties", "POST", buildHeader(true, true), propertiesHash));
    verifyStatusCode(resp.status, 204, "POST /objects/:id/properties");
  };

  /** Table spec has recently changed to make closeTable(), createCoordsTable(), createTable()
   *  non-blocking requests, and expect user to poll for status of table set to "CLOSED"
   *  Adding code in each function to do such a polling. Will make the calls blocking for JS-binding
   *  case by polling until table status if set to "CLOSED"
   *
   *  TODO: Handle this case more efficiently (provide user a non-blocking JS binding request).
   *  Keyword: TABLE_POLLING
   */

  // ------------------------------------
  // TODO:
  // 1. Directly moving on to implementing /table routes. Due to uncertainity
  // around the conept of "Object", it might be a waste of effort to implement others
  // right now.
  // 2. Discuss about return values of some functions (like listObjects, listTables, etc).
  // -------------------------------------

  /** @brief Return IDs, URLs, and status of all tables user has access to
   *  @publish
   *  This function returns an array of hashes each of which represents a table
   *  user has access to.
   *  @return An Array of hashes of the form {id: ..., url: ... , status: "OPEN"|"CLOSED"}
   *  @route GET: /tables
   */
  DNAnexus.listTables = function() {
    var resp = httpReq(buildReq("/tables", "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /tables");
    return getResponseDataJSON(resp.data);
  };

  // TODO: How descriptive the comments should be ?? - should I replicate whole API wiki content ?
  /** @brief Creates a new table
   *  @publish
   *  This function creates a new table. Table can be created by specifying
   *  column descriptors, OR, by combining existing table(s).
   *  @tparam Array body An array of strings. All strings in the array should be in
   *  one of these fomats: "columnName:columnType", OR, "/tables/:id".
   *
   *  The exact behavior of function differs depending on the format of string contained in array.
   *  Please see API spec for details.
   *  @return Hash A hash containing following fields:
   *  @markdown
   *  - _id_: ID_Type - ID of table.
   *  - _status_: String - URL of table
   *  - _status_: String - Status of table ('OPEN' | 'CLOSED')
   *  - _numRows_: Integer - Number of rows in table
   *  - _columns_: Array - All column descriptors of table.
   *  @endmarkdown
   *  @route POST: /tables
   */
  DNAnexus.createTable = function(body) {
    var resp = httpReq(buildReq("/tables", "POST", buildHeader(true, true), body));
    verifyStatusCode(resp.status, 200, "POST /tables");

    resp = getResponseDataJSON(resp.data);

    // Poll for table being closed if it's a "/tables/:id" case)
    // Dirty hack for current purpose: See "TABLE_POLLING" comment above
    if (body[0][0] === "/") { // since columnName cannot have "/" in it, and url must start with it
      var tableStatus = "CLOSING";
      while (tableStatus === "CLOSING") {
        system.sleep(.2);
        tableStatus = DNAnexus.getTableInfo(resp.id).status;
      }
    }

    return resp;
  };

  /** @brief Get information about a single table
   *  @publish
   *  This function returns meta data information for a table specified by it's ID.
   *  @tparam ID_Type id ID of the table
   *  @return Hash A hash containing following fields:
   *  @markdown
   *  - _id_: ID_Type - ID of table.
   *  - _status_: String - URL of table
   *  - _status_: String - Status of table ('OPEN' | 'CLOSED')
   *  - _numRows_: Integer - Number of rows in table
   *  - _columns_: Array - All column descriptors of table.
   *  @endmarkdown
   *  @route GET: /tables/:id
   */
  DNAnexus.getTableInfo = function(id) {
    var resp = httpReq(buildReq("/tables/" + id, "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /table/:id");
    return getResponseDataJSON(resp.data);
  };

  /** @brief Closes a table.
   *  @publish
   *  This function closes a specified table and also triggers creation of any indices
   *  that has been added to the table.
   *  @tparam ID_Type id ID of the table
   *  @route PUT: /tables/:id
   */
  DNAnexus.closeTable = function(id) {
    var resp = httpReq(buildReq("/tables/" + id, "PUT", buildHeader(true, true), {status: "CLOSED"}));
    verifyStatusCode(resp.status, 200, "PUT /tables/:id");

    // Poll for table being closed if it's a "/tables/:id" case)
    // Dirty hack for current purpose: See "TABLE_POLLING" comment above
    var tableStatus = "CLOSING";
    while (tableStatus === "CLOSING") {
      system.sleep(.2);
      tableStatus = DNAnexus.getTableInfo(id).status;
    }
  };

  /** @brief Deletes a table.
   *  @publish
   *  This function deletes a specifed table.
   *  @tparam ID_Type id ID of the table
   *  @route DELETE: /tables/:id
   */
  DNAnexus.deleteTable = function(id) {
    var resp = httpReq(buildReq("/tables/" + id, "DELETE", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "DELETE /tables/:id");
  };

  /** @brief Get rows from a table
   *  @publish
   *  This function returns requested rows from a table.
   *  @tparam ID_Type id ID of the table
   *  @tparam Hash body A hash containing following optional fields
   *  @markdown
   *  - _columns_: (optional) URL encoded array of column names to be returned.
   *  Default is all columns.
   *  - _offset_: (optional) Starting row index. Default is 0.
   *  - _limit_: (optional) Maximum number of rows to return. Default is 1000.
   *  @endmarkdown
   *  @return Hash A hash containing following fields:
   *  @markdown
   *  - _offset_: Offset.
   *  - _limit_: Maximum number of rows
   *  - _rows_: Array of rows.
   *  @endmarkdown
   *  @route GET: /tables/:id/rows?columns=...&offset=...&limit=...
   */
  DNAnexus.getRowsFromTable = function(id, body) {
    var path = "/tables/" + id + "/rows?";
    var ampersandRequired = false;
    for (var key in body) {
      if (hasOwnProperty.call(body, key)) {
        path += (ampersandRequired ? "&" : "") + key + "=" + body[key];
        ampersandRequired = true;
      }
    }
    var resp = httpReq(buildReq(path, "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET " + path);
    return getResponseDataJSON(resp.data);
  };

  /** @brief Append rows to a table.
   *  @publish
   *  This functions append rows to a table. The table should be in an "OPEN" status
   *  before rows can be appended to it.
   *  @tparam ID_Type id ID of table
   *  @tparam Hash body A Hash containing following two fields:
   *  @markdown
   *  - _data_: An array of ordered arrays, where each sub-array contains one entry
      for each column
   *  - _nonce_: (optional) A token (a string) which allows the caller to append
      rows in an idempotent manner.
   *  @endmarkdown
   *  @return Hash A hash containing table state as returned by getTableInfo()
   *  @markdown
   *  - _id_: ID_Type - ID of table.
   *  - _status_: String - URL of table
   *  - _status_: String - Status of table ('OPEN' | 'CLOSED')
   *  - _numRows_: Integer - Number of rows in table
   *  - _columns_: Array - All column descriptors of table.
   *  @endmarkdown
   *  @route POST: /tables/:id/rows
   */
  DNAnexus.appendRowsToTable = function(id, body) {
    var resp = httpReq(buildReq("/tables/" + id + "/rows", "POST", buildHeader(true, true), body));
    verifyStatusCode(resp.status, 200, "POST /tables/:id/rows");
    return getResponseDataJSON(resp.data);
  };

  // TODO: Not implementing /tables indices routes yet.

  // current Priority: properties, runApp, runJob, coordsTable stuff

  /** @brief List all coords tables
   *  @publish
   *  This function returns all coords table user has access to along with thei status.
   *  @return Array of hashes of the form: {"id": ..., "url": ..., "status": ...}
   *  where each hash describes a single coords_table
   *  @route GET: /coords_tables
   */
  DNAnexus.listCoordsTable = function() {
    var resp = httpReq(buildReq("/coords_tables", "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /coords_tables");
    return getResponseDataJSON(resp.data);
  };

  /** @brief Create a coords table
   *  @publish
   *  This functions creates a new coords_table from a specifed table.
   *  @tparam String url URL of the source table
   *  @return Hash hash containing the following fields:
   *  @markdown
   *  - _id_: ID_Type: ID of the coords_table
   *  - _url_: String: URL of the coords_table
   *  - _status_: 'CLOSED'
   *  - _numRows_: Integer: Number of rows in table.
   *  - _columns_: Array of column descriptors
   *  @endmarkdown
   *  @route POST: /coords_tables
   */
  DNAnexus.createCoordsTable = function(url) {
    var resp = httpReq(buildReq("/coords_tables", "POST", buildHeader(true, true), {sourceTable: url}));
    verifyStatusCode(resp.status, 200, "POST /coords_tables");

    resp = getResponseDataJSON(resp.data);

    // Poll for table being closed if it's a "/tables/:id" case)
    // Dirty hack for current purpose: See "TABLE_POLLING" comment above
    var coordsTableStatus = "CLOSING";
    while (coordsTableStatus === "CLOSING") {
      system.sleep(.2);
      coordsTableStatus = DNAnexus.getCoordsTableInfo(resp.id).status;
    }

    return resp;
  }

  /** @brief Get information about a single coords table
   *  @publish
   *  This function returns meta data information for a coords table specified by it's ID.
   *  @tparam ID_Type id ID of the coords table
   *  @return Hash A hash containing following fields:
   *  @markdown
   *  - _id_: ID_Type - ID of coords table.
   *  - _url_: String - URL of coords table
   *  - _status_: String - Status of coords table ('CLOSED')
   *  - _numRows_: Integer - Number of rows in coords table
   *  - _columns_: Array - All column descriptors of coords table.
   *  @endmarkdown
   *  @route GET: /coords_tables/:id
   */
  DNAnexus.getCoordsTableInfo = function(id) {
    var resp = httpReq(buildReq("/coords_tables/" + id, "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /coords_tables/:id");
    return getResponseDataJSON(resp.data);
  };

  /** @brief Get rows from a coords table
   *  @publish
   *  This function returns requested rows from a coords table.
   *  @tparam ID_Type id ID of the coords table
   *  @tparam Hash body A hash containing following optional fields
   *  @markdown
   *  - _columns_: (optional) URL encoded array of column names to be returned.
   *  Default is all columns.
   *  - _offset_: (optional) Starting row index. Default is 0.
   *  - _limit_: (optional) Maximum number of rows to return. Default is 1000.
   *  - _chr_: (optional) The chromosome index.
   *  - _start_: (optional) The start location in the chromosome.
   *  - _stop_: (optional) The stop location in the chromosome.
   *  - _selection_: (optional) One of: {overlap, enclose}
   *
   *  NOTE: {chr, start, stop, selection} AND {offset, limit} are mutually exclusive groups,
   *  If any parameter from one group is present in request body, then it excludes possibility
   *  of parameter from other group.
   *  @endmarkdown
   *  @return Hash A hash containing following fields:
   *  @markdown
   *  - _offset_: Offset.
   *  - _limit_: Maximum number of rows
   *  - _rows_: Array of rows.
   *  @endmarkdown
   *  @route GET: /coords_tables/:id/rows?columns=...&offset=...&limit=...&...
   */
  DNAnexus.getRowsFromCoordsTable = function(id, body) {
    var path = "/coords_tables/" + id + "/rows?";
    var ampersandRequired = false;
    for (var key in body) {
      if (hasOwnProperty.call(body, key)) {
        path += (ampersandRequired ? "&" : "") + key + "=" + body[key];
        ampersandRequired = true;
      }
    }
    var resp = httpReq(buildReq(path, "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET " + path);
    return getResponseDataJSON(resp.data);
  };

  /** @brief Deletes a coords table.
   *  @publish
   *  This function deletes a specifed coords table.
   *  @tparam ID_Type id ID of the coords table
   *  @route DELETE: /coords_tables/:id
   */
  DNAnexus.deleteCoordsTable = function(id) {
    var resp = httpReq(buildReq("/coords_tables/" + id, "DELETE", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "DELETE /coords_tables/:id");
  };

  /** @brief Creates a new app
   *  @publish
   *  This functions creates a new app.
   *  @tparam Hash body A hash containing following fields (see App API for details)
   *  @markdown
   *  - _input\_spec_: The input spec of app.
   *  - _output\_spec_: The output spec of app.
   *  - _code_: A string representing Javascript code of the app.
   *  - _access\_spec_: (optional) A set of descriptors for the app requests
   *  - _version\_depends_: (optional) A set of other apps or data objects that this app
   *    will potentially access, and for which automatic versioning is required
   *  - _exec\_depends_: (optional) Execution environment dependencies; a string representing
   *    package names and versions that will be installed in the
   *    execution environment before the app is executed
   *  - _bundled\_depends_: (optional) Execution environment dependencies bundled with the app.
   *  - _sys\_reqs_: (optional) Execution environment resource requirements; a hash with keys
   *    corresponding to function names which are execution entry points (main and optionally
   *    others to be invoked by runJob), and values which are hashes enumerating system
   *    requirements of type ram, cpus or store.
   *  @endmarkdown
   *  @return ID_Type ID of the newly created app.
   *  @route POST: /apps
   */
  DNAnexus.createApp = function(body) {
    var resp = httpReq(buildReq("/apps", "POST", buildHeader(true, true), body));
    verifyStatusCode(resp.status, 201, "POST /apps");
    return getResponseDataJSON(resp.data).appID;
  }

  /** @brief Runs an app.
   *  @publish
   *  This functions runs an existing app.
   *  @tparam ID_Type id ID of the app to run
   *  @tparam Hash body A hash containing following fields (see App API for details)
   *  @markdown
   *  - _input_: A hash whose keys are names of inputs and whose values are immediate
   *  values or object references (or arrays of object references, for array types).
   *  This input hash needs to conform to the input spec of the app.
   *  - _version_: (optional) An identifier for app (see app details).
   *  - _billing\_id_: (optional) ID of the entity to be billed.
   *  - _default\_permissions: (optional) Default permissions for created objects.
   *  @endmarkdown
   *  @return ID_Type Returns Job object ID, which is responsible for this runApp() call.
   *  @route POST: /apps/:id/jobs
   */
  DNAnexus.runApp = function(id, body) {
    var resp = httpReq(buildReq("/apps/" + id + "/jobs", "POST", buildHeader(true, true), body));
    verifyStatusCode(resp.status, 201, "POST /apps/:id/jobs");
    return getResponseDataJSON(resp.data).jobID;
  }

  /** @brief Creates a job within a running job
   *  @publish
   *  This functions creates a new job within current job.
   *  @tparam Hash body A hash containing following fields (see App API for details)
   *  @markdown
   *  - _input_: A hash whose keys are names of inputs and whose values are immediate
   *  values or object references (or arrays of object references, for array types).
   *  This input hash needs to conform to the input spec of the app.
   *  - _func\_name_: (optional) A different entry point (instead of main) for app code.
   *  - _version_: (optional) An identifier for app (see app details).
   *  @endmarkdown
   *  @return ID_Type Returns Job object ID, which is responsible for this runJob() call.
   *  @route POST: /jobs/:id/jobs
   */
  DNAnexus.runJob = function(body) {
    // The current job's id is stored in globally available variable: job.id - See http://wiki.dnanexus.com/index.php/Execution_subsystem#Run_Job
    var resp = httpReq(buildReq("/jobs/" + job.id + "/jobs", "POST", buildHeader(true, true), body));
    verifyStatusCode(resp.status, 201, "POST /jobs/:id/jobs");
    return getResponseDataJSON(resp.data).jobID;
  }

  /** @brief Execute a given command in a separate process
   *  @publish
   *  This function is used for executing arbitrary commands in a different process
   *  @tparam String cmd The command to be executed
   *  @tparam Hash options A hash containing following boolean options 
   *  (Default is "false" for all options)
   *  @markdown
   *  - _capture\_stdout_: (optional) If set to true, then stdout's output is returned
   *  - _capture\_stderr_: (optional) If set to true, then stderr's ouptut is returned
   *  @endmarkdown
   *  @return A hash with following fields:
   *  @markdown
   *  - _exitCode_: Always present. The exit code returned by the process.
   *  - _stdout_: Present iff boolReturnOutput = true. Contains content of standard output.
   *  - _stderr_: Present iff boolReturnOutput = true. Contains content of standard error.
   *  @endmarkdown
   */
  DNAnexus.system = function(cmd, options) {
    var p = new process.Process();
    var exitCode;
    var tempFile_stdout = null, tempFile_stderr = null;
    var finalCmd = "(" + cmd + ")";
    var f;
    if (options === undefined) {
      options = {};
    }
    if (options["capture_stdout"] === true) {
      tempFile_stdout = p.exec("mktemp");
      if (tempFile_stdout == null) {
        throw new Error("Unable to execute command mktemp");
      }
      // Remove \n at end of string - since "mktemp" return with a "\n" at the end
      tempFile_stdout = tempFile_stdout.replace(/\n$/g, '');
      finalCmd += " > " + tempFile_stdout;
    }
    else {
      // If output is not required, redirect it to /dev/null
      finalCmd += " > /dev/null";
    }
    if (options["capture_stderr"]=== true) {
      tempFile_stderr = p.exec("mktemp");
      if (tempFile_stderr == null) {
        throw new Error("Unable to execute command mktemp");
      }
      tempFile_stderr = tempFile_stderr.replace(/\n$/g, '');
      finalCmd += " 2> " + tempFile_stderr;
    }
    else {
     // Disabling for debugging purposes - stderr output will go to log 
     // finalCmd += " 2> /dev/null";
    }

    // An internal function - just for use by DNAnexus.system()
    function clearResources() {
      // Remove temp files for stdout and stderr (if they are created in first place)
      if (tempFile_stdout !== null) {
        f = new fs.File(tempFile_stdout);
        f.remove();
        delete f;
      }
      if (tempFile_stderr !== null) {
        f= new fs.File(tempFile_stderr);
        f.remove();
        delete f;
      }
      delete p;
    }

    system.stderr("Running " + finalCmd + "\n");
    exitCode = p.system(finalCmd);

    if (exitCode !== 0) {
      clearResources();
      throw new Error("The command " + cmd + " exited with non-zero exit code");
    }
    var return_hash = {"exitCode": exitCode};
    if (options["capture_stdout"] === true) {
     f = new fs.File(tempFile_stdout);
     f.open("r");
     return_hash.stdout = f.read().toString("utf-8");
     f.close();
     delete f;
    }
    
    if (options["capture_stderr"] === true) {
      f = new fs.File(tempFile_stderr);
      f.open("r");
      return_hash.stderr = f.read().toString("utf-8");
      f.close();
      delete f;
    }
    clearResources();

    return return_hash;
  }

  /** @brief Consumes Search API
   *  @publish
   *  This function is used for searching objects in DNAnexus platform.
   *  @tparam Hash query The request body is a Hash with following optional fields:
   *  @markdown
   *  - _type_: An array of strings. For ex: type: ["app", "table"]
   *  - _properties_: A JSON obejct containing key/value pairs.
   *
   *  If neither of the field is present (query = {}), then all objects are returned by search.
   *  See search API for more details.
   *  @endmarkdown
   *  @return An array of complete JSON Hash of object(s) matching the search criteria.
   *  @route POST: /search
   */
  DNAnexus.search = function(query) {
    var resp = httpReq(buildReq("/search", "POST", buildHeader(true, true), query));
    verifyStatusCode(resp.status, 200, "POST /search");
    return getResponseDataJSON(resp.data)["search-output"];
  }

  /** @brief Get app specific meta data
   *  @publish
   *  This functions returns you meta data for an app as a Hash. The fields present
   *  are determined by permissions user have.
   *  @tparam ID_Type id Id of the app
   *  @return An hash containing information depending on your permission level.
   *  See API for details
   *  @route GET: /apps/:id
   */
  DNAnexus.getApp = function(id) {
    var resp = httpReq(buildReq("/apps/" + id, "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /apps/:id");
    return getResponseDataJSON(resp.data);
  }

  /** @brief Get job specific meta data
   *  @publish
   *  This functions returns you meta data for a job.
   *  @tparam ID_Type id Id of the job
   *  @return An hash containing following fields:
   *  @markdown
   *  - _input_: Input hash for the job (may be partially unresolved)
   *  - _output_: Output hash for the job (empty if job hasn't run or partially unresolved)
   *  - _state_: State of the job
   *  - _appID/jobID_: Corresponding appID or parent jobID
   *  - _func\_name_: (optional) If job has a func\_name specified, it will be returned.
   *  @endmarkdown
   *  @route GET: /jobs/:id
   */
  DNAnexus.getJob = function(id) {
    var resp = httpReq(buildReq("/jobs/" + id, "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /jobs/:id");
    return getResponseDataJSON(resp.data);
  }

  /** @brief Creates a new upload
   *  @publish
   *  This function creates a new upload.
   *  @tparam Integer num_parts (optional) The number of parts to split the upload into. default=1.
   *  @return Hash with following fields
   *  @markdown
   *  - _id_: ID of the upload
   *  - _parts_: Array of Urls's to put data to.
   *  @endmarkdown
   *  @route POST: /uploads
   */
  DNAnexus.createUpload = function(num_parts) {
    var path = "/uploads";
    path += (num_parts !== undefined) ? ("?num_parts=" + num_parts) : "";

    var resp = httpReq(buildReq(path , "POST", buildHeader(false, true)));
    verifyStatusCode(resp.status, 201, "POST /uploads");
    return getResponseDataJSON(resp.data);
  }

  /** @brief List all uploads
   *  @publish
   *  This function returns an array of all uploads (each being an hash with "id" and "num_parts")
   *  @return An array of hashes. Each hash is of form {id: ..., num_parts: ...} and describes
   *  a single upload.
   *  @route GET: /uploads
   */
  DNAnexus.listUploads = function() {
    var resp = httpReq(buildReq("/uploads" , "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /uploads");
    return getResponseDataJSON(resp.data);
  }

  /** @brief Get info about a particular upload
   *  @publish
   *  This function returns an array of parts for the specified upload, where each part
   *  is an hash containing "size", "url"
   *  @tparam ID_Type id ID of the upload
   *  @return An array of hashes of the form {"size": ...", "url" ...}. Each hash describe
   *  a single part of the upload.
   *  @route GET: /uploads/:id
   */
  DNAnexus.getUploadInfo = function(id) {
    var resp = httpReq(buildReq("/uploads/" + id , "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /uploads/:id");
    return getResponseDataJSON(resp.data);
  }
 
  /** @brief List all files
   *  @publish
   *  This function returns an array of all files (each being an hash with "id" and "url")
   *  @return An array of hashes. Each hash is of form {id: ..., url: ...} and describes
   *  a single file.
   *  @route GET: /file
   */
  DNAnexus.listFiles = function() {
    var resp = httpReq(buildReq("/files" , "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /files");
    return getResponseDataJSON(resp.data);
  }


  /** @brief Creates a new file
   *  @publish
   *  This function creates a new file from an upload
   *  @tparam Hash body A hash containing following possible fields:
   *  @markdown
   *  - _id_: ID of the upload object from which to obtain the file's contents.
   *  - _url_: URL of the upload object from which to obtain the file's contents.
   *  - _content\_type_: (optional) A string containing the MIME type to be associated with file.
   *  @endmarkdown
   *  @return A string containing the URL of the new file.
   *  @route POST: /files
   */
  DNAnexus.createFile = function(body) {
    var resp = httpReq(buildReq("/files" , "POST", buildHeader(true, true), body));
    verifyStatusCode(resp.status, 201, "POST /files");
    if (resp.header("Location") == undefined) {
      throw new Error("No location header present in response from POST /files");
    }
    return resp.header("Location");
  }


  /** @brief Get info about a particular file
   *  @publish
   *  This function returns meta data information about a specified file.
   *  @tparam ID_Type id ID of the file
   *  @return A JSON object containing "id", "url" and "size" fields of the file.
   *  @route GET: /files/:id/meta
   */
  DNAnexus.getFileInfo = function(id) {
    var resp = httpReq(buildReq("/files/" + id + "/meta", "GET", buildHeader(false, true)));
    verifyStatusCode(resp.status, 200, "GET /files/:id/meta");
    return getResponseDataJSON(resp.data);
  }
 
  
  exports.DNAnexus = DNAnexus;
}).call(this);

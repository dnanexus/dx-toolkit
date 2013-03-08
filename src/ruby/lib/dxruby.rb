require "net/http"
require "net/https"
require "json"
require "dxruby/version"
require "dxruby/api"

module DXRuby
  #TODO: Add a custom error class for APIErrors
  class DXHTTPRequestError < StandardError
    def initialize(msg = "An error occured in call to DXHTTPRequest")
      super(msg)
    end
  end

  @DEFAULT_RETRIES = 5

  @env_var_loaded = false # a hack, so that we load env variable only once. Better way ?

  ##
  # Loads environment variables (@apiserver_host, etc)
  def self.read_env_var()
    if @env_var_loaded
      return
    end
    user_env_file = File.expand_path("~/.dnanexus_config/environment.json")
    env_json = nil
    if File.exist?(user_env_file)
      begin
        env_json = JSON.parse(IO.read(user_env_file))
      rescue
        $stderr.puts("WARNING: The contents of file '" + user_env_file + "' cannot be parsed as a valid JSON .. Ignoring it")
        env_json = nil
      end
    end

    ##
    # Get value of env variable. Resolved in following order:
    #  1. Environment variable is present
    #  2. Value from config file
    #  3. default value (provided by caller of the function)
    # @param name string Name of the variable (as present in config file/env)
    # @param config_json JSON/nil parsed contents of the JSON file (or nil, if file is absent)
    # @param default_val String This value will be returned if value is not found in env variables/config file
    # @return String Value for given variable (see priority order above)
    def self.get_value(name, config_json, default_val="")
      if ENV[name] != nil
        return ENV[name]
      end

      if config_json != nil && config_json[name].is_a?(String)
        return config_json[name]
      end

      return default_val
    end

    @apiserver_host = get_value("DX_APISERVER_HOST", env_json, "api.dnanexus.com")
    @apiserver_port = get_value("DX_APISERVER_PORT", env_json, "443")
    @apiserver_protocol = get_value("DX_APISERVER_PROTOCOL", env_json, "https")
    @security_context = JSON.parse(get_value("DX_SECURITY_CONTEXT", env_json, "{}"))

    @env_var_loaded = true
  end

  ##
  # A wrapper to make HTTP requests to DNAnexus apiserver
  # @param resource string Path of the resource to be accessed. The path is assumed to be
  #                        relative to Apiserver, unless opts.prepend_srv == false
  # @param data String/JSON/nil If not nil, then represent data to be sent with the request
  # @param opts JSON A hash with additional options. Currentl supported keys are:
  #        - always_retry: boolean; indicates if it's safe to retry a route on failure.
  #                        (See "When a request is retried" document for details). Default = false
  #        - prepend_srv: If false, then Apiserver address is nor prepended to "resources" string
  #        - max_retries: Maximum number of retries
  #        - auth: If false, then authentication header is not added to the request
  #        - method: One of "GET"/"POST", specifies the HTTP method to be used for the request
  #                  Default is "POST"
  # @return String/JSON Data returned by the API request
  # Raises error if request was not completed (after retrying, if possible)
  def self.DXHTTPRequest(resource, data, opts = {})
    read_env_var()
    if opts["prepend_srv"] != false
      uri = URI.parse(@apiserver_protocol + "://" + @apiserver_host + ":" + @apiserver_port + resource)
    else
      uri = URI.parse(resource)
    end
    http = Net::HTTP.new(uri.host, uri.port)

    if opts.has_key?("max_retries")
      max_retries = opts['max_retries']
    else
      max_retries = @DEFAULT_RETRIES
    end

    if opts.has_key?("always_retry")
      always_retry = opts['always_retry']
    else
      always_retry = false
    end

    if uri.scheme == "https"
      http.use_ssl = true

      # http://notetoself.vrensk.com/2008/09/verified-https-in-ruby/
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER

      # Set certificate file from OpenSSL: http://stackoverflow.com/questions/9199660/why-is-ruby-unable-to-verify-an-ssl-certificate
      store = OpenSSL::X509::Store.new
      store.set_default_paths
      http.cert_store = store

      #request.ca_file =
    end

    to_send_data = (data != nil)
    if !data.is_a?(String)
      data = data.to_json
    end

    if opts.has_key?("method")
      if ("POST".casecmp(opts["method"]) == 0) || ("GET".casecmp(opts["method"]) == 0)
        method = opts["method"].upcase
      else
        raise DXHTTPRequestError, 'opts["method"] should be either GET or POST'
      end
    else
      method = "POST"
    end

    for num_try in 0..max_retries
      #TODO: Should we add uri.query & uri.fragments too ??
      if method == "POST"
        request = Net::HTTP::Post.new(uri.path)
      else
        request = Net::HTTP::Get.new(uri.path)
      end

      if to_send_data
        request.add_field("Content-Type", "application/json")
      end
      request.add_field("User-Agent", "dxruby/" + VERSION)

      if opts["auth"] != false
        if @security_context.empty? || (!@security_context["auth_token_type"].is_a?(String)) || (!@security_context["auth_token"].is_a?(String))
          raise DXHTTPRequestError, "DX_SECURITY_CONTEXT not found (or incorrect). Unable to set Authorization header"
        end
        request.add_field("Authorization", @security_context["auth_token_type"] + " " + @security_context["auth_token"])
      end
      if to_send_data
        request.body = data
      end

      err_msg = ""
      to_retry = false

      # Make the actual request
      begin
        response = http.request(request)
      rescue Timeout::Error => err
        err_msg = "Timeout::Error"
      rescue SystemCallError => err
        # http://stackoverflow.com/a/11458292
        raise err if !err.class.name.start_with?('Errno::')
        err_msg = err.class.name
      end

      if err_msg.size > 0
        # A response was not received from server
        to_retry = always_retry || (method == "GET")
      else
        # A response was received from server
        status_code = response.code.to_i

        # Check the status code of response
        if status_code >= 200 && status_code <= 299
          # Ok response case: Match content-length (if provided by server)
          if response['content-length'] && (response['content-length'].to_i != response.body.length)
            to_retry = always_retry || (method == "GET")
            err_msg = "Expected Content-Length from server: " + response['content-length'] + ", but recieved only " + response.body.length.to_s
          else
            # Everything is ok, just return the response (after parsing as JSON, if content-type is provided by server)
            if response['content-type'] && (/^\s*application\/json/i.match(response['content-type']) != nil)
              return JSON.parse(response.body)
            else
              return response.body
            end
          end
        else
          to_retry = (status_code >= 500 && status_code <= 599)
          err_msg = "HTTP Status code: " + status_code.to_s + ", Response Body = '" + response.body + "'"
        end
      end
      if to_retry && (num_try < max_retries)
        delay = 2 ** num_try
        $stderr.puts method + " " + uri.to_s +  ": '" + err_msg + "'. Waiting " + delay.to_s + " seconds before retry " + (num_try + 1).to_s + " of " + max_retries.to_s + "..."
        sleep(delay)
      else
        raise DXHTTPRequestError, "An error occured while making " + method + " request to '" + uri.to_s + "' (total tries = " + (num_try + 1).to_s + "). Error message = '" + err_msg + "'."
      end
    end
  end
end

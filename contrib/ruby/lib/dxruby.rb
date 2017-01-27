require "net/http"
require "net/https"
require "json"
require "dxruby/version"
require "dxruby/api"

module DX
  class HTTPRequestError < StandardError
    def initialize(msg = "An error occured in call to DX::http_request")
      super(msg)
    end
  end

  @DEFAULT_RETRIES = 5

  @env_var_loaded = false # a hack, so that we load env variable only once. Better way ?

  ##
  # Loads environment variables (@apiserver_host, etc)
  def self.read_env_var()
    return if @env_var_loaded
    user_env_file = File.expand_path("~/.dnanexus_config/environment.json")
    env_json = nil
    if File.exist?(user_env_file)
      begin
        env_json = JSON.parse(IO.read(user_env_file))
      rescue
        $stderr.puts("WARNING: The contents of file '#{user_env_file}' cannot be parsed as a valid JSON, ignoring")
        env_json = nil
      end
    end

    ##
    # Get the value of a configuration variable. Resolved in the following order:
    #  1. Environment variable, if set
    #  2. Value from config file
    #  3. Default value (provided by caller of the function)
    # @param name string Name of the variable (as present in config file/env)
    # @param config_json JSON/nil parsed contents of the JSON file (or nil, if file is absent)
    # @param default_val String This value will be returned if value is not found in env variables/config file
    # @return String Value for given variable (see priority order above)
    def self.get_config_var(name, config_json, default_val="")
      return ENV[name] if ENV[name]
      return config_json[name] if config_json && config_json[name].is_a?(String)
      return default_val
    end

    @apiserver_host = get_config_var("DX_APISERVER_HOST", env_json, "api.dnanexus.com")
    @apiserver_port = get_config_var("DX_APISERVER_PORT", env_json, "443")
    @apiserver_protocol = get_config_var("DX_APISERVER_PROTOCOL", env_json, "https")
    @security_context = JSON.parse(get_config_var("DX_SECURITY_CONTEXT", env_json, "{}"))

    @env_var_loaded = true
  end

  ##
  # A wrapper to make HTTP requests to DNAnexus apiserver
  # @param resource string Path of the resource to be accessed (excluding the server name, unless
  # opts.prepend_srv == false)
  # @param data String/JSON/nil If not nil, then represent data to be sent with the request
  # @param opts JSON A hash with additional options. Currently supported keys are:
  #        - always_retry: boolean; indicates if it's safe to retry a route on failure.
  #                        (See the "When a request is retried" document for details). Default = false
  #        - prepend_srv: If false, the API server address is not used, and the "resource" parameter is expected to
  # contain the protocol and hostname.
  #        - max_retries: Maximum number of retries
  #        - auth: If false, the authentication header is not added to the request
  #        - method: Specifies the HTTP method to be used for the request
  #                  Default is "POST"
  # @return String/JSON Data returned by the API request
  # Raises error if request was not completed (after retrying, if possible)
  def self.http_request(resource, data, opts = {})
    read_env_var()
    if opts["prepend_srv"] != false
      uri = URI.parse("#{@apiserver_protocol}://#{@apiserver_host}:#{@apiserver_port}#{resource}")
    else
      uri = URI.parse(resource)
    end
    http = Net::HTTP.new(uri.host, uri.port)

    max_retries = opts["max_retries"] || @DEFAULT_RETRIES
    always_retry = opts["always_retry"] || false

    if uri.scheme == "https"
      http.use_ssl = true

      # Ruby 1.8 configures OpenSSL to turn off hostname verification by default. Fix it here.
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER

      # Set certificate filename location from OpenSSL.
      # See http://stackoverflow.com/questions/9199660/why-is-ruby-unable-to-verify-an-ssl-certificate
      store = OpenSSL::X509::Store.new
      store.set_default_paths
      http.cert_store = store

      #request.ca_file =
    end

    send_data = !(data.nil?)

    # If data is not a string (and not nil), convert it to a string
    if send_data && !data.is_a?(String)
      data = data.to_json
    end

    method = opts["method"] || "POST"
    if method != 'GET' and method != 'POST'
       raise HTTPRequestError, 'opts["method"] should be either GET or POST'
    end

    for num_try in 0..max_retries
      # TODO: Should we add uri.query & uri.fragments too?
      if method == "POST"
        request = Net::HTTP::Post.new(uri.path)
      else
        request = Net::HTTP::Get.new(uri.path)
      end

      if send_data
        request.add_field("Content-Type", "application/json")
      end
      request.add_field("User-Agent", "dxruby/" + VERSION)

      if opts["auth"] != false
        if @security_context.empty? || (!@security_context["auth_token_type"].is_a?(String)) || (!@security_context["auth_token"].is_a?(String))
          raise HTTPRequestError, "DX_SECURITY_CONTEXT not found (or incorrect). Unable to set Authorization header"
        end
        request.add_field("Authorization", @security_context["auth_token_type"] + " " + @security_context["auth_token"])
      end
      if send_data
        request.body = data
      end

      err_msg = ""
      ok_to_retry = false

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
        # A response was not received from the server
        ok_to_retry = always_retry || (method == "GET")
      else
        # A response was received from the server
        status_code = response.code.to_i

        # Check the response status code
        if status_code >= 200 && status_code <= 299
          # Match content-length (if provided by the server)
          if response['content-length'] && (response['content-length'].to_i != response.body.length)
            ok_to_retry = always_retry || (method == "GET")
            err_msg = "Received response with content-length header set to " + response['content-length'] + " but content length is " + response.body.length.to_s
          else
            # Everything is OK, return the response (after parsing as JSON, if that content-type is provided by the server)
            if response['content-type'] && (/^\s*application\/json/i.match(response['content-type']) != nil)
              begin
                return JSON.parse(response.body)
              rescue JSON::ParserError
                # If response cannot be parsed as valid JSON,
                # we retry iff "content-length" header is absent (See PTFM-7182 for detail)
                if response['content-length']
                  raise
                else
                  ok_to_retry = true
                end
              end
            else
              return response.body
            end
          end
        else
          ok_to_retry = (status_code >= 500 && status_code <= 599)
          err_msg = "HTTP Status code: " + status_code.to_s + ", Response Body = '" + response.body + "'"
        end
      end
      if ok_to_retry && (num_try < max_retries)
        delay = 2 ** num_try
        $stderr.puts "#{method} #{uri.to_s}: #{err_msg or 'none'}. Waiting #{delay.to_s} seconds before retry #{(num_try + 1).to_s} of #{max_retries.to_s}..."
        sleep(delay)
      else
        raise HTTPRequestError, "#{method} #{uri.to_s}: #{err_msg or 'none'}"
      end
    end
  end
end

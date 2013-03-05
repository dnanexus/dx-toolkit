require "net/http"
require "net/https"
require "json/pure"
#require 'always_verify_ssl_certificates'

APISERVER_PROTOCOL = 'http'
APISERVER_HOST = 'localhost'
APISERVER_PORT = '8124'
DEFAULT_RETRIES = 5

# TODO: Get dx-toolkit version
TOOLKIT_VERSION = "blah"

#APISERVER_PROTOCOL = 'https'
#APISERVER_HOST = 'api.dnanexus.com'
#APISERVER_PORT = '443'

# TODOs:
# 1. Add support for authentication
# 2. See if Net:HTTP::Post.new(uri.path) is fine, or should we use uri.path + ur.fragments, etc

def DXHTTPRequest(resource, data, always_retry=false, opts = JSON.parse("{}"))
  # http://www.rubyinside.com/how-to-cure-nethttps-risky-default-https-behavior-4010.html
  #AlwaysVerifySeLCertificates.ca_file = "/path/path/path/cacert.pem"
  if ((!opts.has_key?("prepend_srv")) || (opts["prepend_srv"] == true))
    uri = URI.parse(APISERVE_PROTOCOL + "://" + APISERVER_HOST + ":" + APISERVER_PORT + resource)
  else
    uri = URI.parse(resource)
  end
  http = Net::HTTP.new(uri.host, uri.port)
  
  if opts.has_key?("max_retries")
    max_retries = opts['max_retries']
  else
    max_retries = DEFAULT_RETRIES
  end
  
  if uri.scheme == "https"
    http.use_ssl = true
    # TODO: DX_CA_CERT support ?? 
    http.verify_mode = OpenSSL::SSL::VERIFY_PEER
    # http://notetoself.vrensk.com/2008/09/verified-https-in-ruby/
    #request.ca_file = 
  end
  
  if (data.is_a?(Object))
    data = data.to_json
  end

  for num_try in 0..max_retries
    request = Net::HTTP::Post.new(uri.path)
    request.add_field("Content-Type", "application/json")
    request.add_field("User-Agent", "dxruby/" + TOOLKIT_VERSION)
    # TODO: Add headers from "opts" to request ?

    request.body = data
    errMsg = ""
    to_retry = false
    
    # Make the actual request
    begin
      response = http.request(request)
    rescue Timeout::Error => err
      errMsg = "Timeout::Error"
    rescue SystemCallError => err
      # http://stackoverflow.com/a/11458292
      raise err if(!err.class.name.start_with?('Errno::'))
      errMsg = err.class.name 
    end
     
    if (errMsg.length > 0)
      # A response was not recieved from server
      to_retry = always_retry
      if !to_retry
        raise "An error occured while trying to make the HTTP request. errMsg = '" + errMsg + "'"
      end
    else
      # A response was recieved from server
      status_code = Integer(response.code, 10);
      
      # Check the status code of response
      if (status_code >= 200 && status_code <= 299)
        # Ok response case: Match content-length (if provided by server)
        if response['content-length'] && (Integer(response['content-length'], 10) != response.body.length)
          to_retry = always_retry
          errMsg = "Expected Content-Length from server: " + response['content-length'] + ", but recieved only " + response.body.length.to_s
        else
          # Everything is ok, just return the response (after parsing as JSON, if content-type is provided by server)
          if (response['content-type'] && (/application\/json/i.match(response['content-type']) != nil))
            return JSON.parse(response.body)
          else
            return response.body
          end
        end
      else
        to_retry = (status_code >= 500 && status_code <= 599)
        errMsg = "HTTP Status code: " + status_code.to_s + ", body = '" + response.body + "'"
      end
    end
    if to_retry && (num_try < max_retries)
      delay = 2 ** num_try
      $stderr.puts uri.to_s + resource + ": '" + errMsg + "'. Waiting " + delay.to_s + " seconds before retry " + (num_try + 1).to_s + " of " + max_retries.to_s + "..."
      sleep(delay)
    else
      raise "An error occured while making POST request to " + uri.to_s + ". errMsg = '" + errMsg + "'."
    end
  end
end

resp = DXHTTPRequest("/system/findProjects", {}, true, JSON.parse('{"max_retries": 2}'))
puts resp.to_json

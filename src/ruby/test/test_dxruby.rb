require 'rubygems'
require "dxruby"
require "test/unit"
 
class TestDXHTTP < Test::Unit::TestCase
  def test_negative
    #printresp(DXRuby.DXHTTPRequest("/system/findProjects", {}, true, JSON.parse('{"max_retries": 2}')))
    assert_raise(RuntimeError) { DXRuby.DXHTTPRequest("/system/IDoNotExist", {}, false) }
    assert_raise(RuntimeError) { DXRuby.DXHTTPRequest("/system/greet", {}, true, JSON.parse('{"method": "PUT"}')) } # We don't support PUT method
    assert_raise(RuntimeError) { DXRuby.DXHTTPRequest("/system/findProjects", "{MalformedJson}", true) } # should fail because of malformed json
  end

  def test_get
    resp = DXRuby.DXHTTPRequest("http://www.google.com/", nil, true, JSON.parse('{"method": "GET", "prepend_srv": false, "auth": false}'))
    assert_match(/Search the world's information/i, resp, "Google's webpage should contain this string")
  end
  
  def test_apiserver_requests
    resp1 = DXRuby.DXHTTPRequest("/system/findProjects", {}, true)
    assert_instance_of(Hash, resp1)
    assert_instance_of(Array, resp1["results"])

    resp2 = DXRuby.DXHTTPRequest("/system/findProjects", "{}", true) # Passing {} or "{}" should both behave correctly
    assert_equal(resp1, resp2)
  end

end

##
# Useful for manual debugging
# Prints the response returned by DXRuby.DXHTTPRequest
def printresp(resp)
  if (!resp.is_a?(String))
    puts JSON.pretty_generate(resp)
  else
    puts resp
  end
end
#resp = DXRuby.DXHTTPRequest("/system/findProjects", {}, true)
#printresp(resp)

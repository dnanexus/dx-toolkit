require 'rubygems'
require "dxruby"
require "test/unit"

class TestDXHTTP < Test::Unit::TestCase
  def test_negative
    #printresp(DXRuby.DXHTTPRequest("/system/findProjects", {}, true, JSON.parse('{"max_retries": 2}')))
    assert_raise(DXRuby::DXHTTPRequestError) { DXRuby.DXHTTPRequest("/system/IDoNotExist", {}, {"always_retry"=> true, "max_retries"=> 2}) }
    assert_raise(DXRuby::DXHTTPRequestError) { DXRuby.DXHTTPRequest("/system/greet", {}, JSON.parse('{"method": "PUT"}')) } # We don't support methods other than POST/GET at this time
    assert_raise(DXRuby::DXHTTPRequestError) { DXRuby.DXHTTPRequest("/system/findProjects", "{MalformedJson}", {"always_retry"=> true}) } # should fail because of malformed json
  end

  def test_get
    resp = DXRuby.DXHTTPRequest("http://www.google.com/", nil, JSON.parse('{"method": "GET", "prepend_srv": false, "auth": false}'))
    assert_match(/Search the world's information/i, resp, "Google's webpage should contain this string")
  end

  def test_apiserver_requests
    resp1 = DXRuby.DXHTTPRequest("/system/findProjects", {}, {"max_retries"=> 2})
    assert_instance_of(Hash, resp1)
    assert_instance_of(Array, resp1["results"])

    resp2 = DXRuby.DXHTTPRequest("/system/findProjects", "{}") # Passing {} or "{}" should both behave correctly
    assert_equal(resp1, resp2)
  end
end

class TestWrappers < Test::Unit::TestCase
  def test_negative
    assert_raise(DXRuby::DXHTTPRequestError) { DXRuby::API.userDescribe("") }
  end

  def test_positive
    resp = DXRuby::API.systemFindProjects()
    assert_instance_of(Hash, resp)
    assert_instance_of(Array, resp["results"])
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

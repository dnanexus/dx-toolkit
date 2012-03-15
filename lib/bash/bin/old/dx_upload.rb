#!/usr/bin/env ruby

require 'optparse'
require 'net/http'
require 'json'

$user_id = nil
$server = nil
if (ENV['APISERVER_HOST'] != nil && ENV['APISERVER_PORT'] != nil)
  $server = ENV['APISERVER_HOST'] + ':' + ENV['APISERVER_PORT']
else
  $server = "http://localhost:8124"
end
$verbose = false
$token = nil
if(ENV['SECURITY_CONTEXT'] != nil)
  jsonToken = JSON.parse(ENV['SECURITY_CONTEXT'])
  $token = jsonToken["auth_token_type"] + " " + jsonToken["auth_token"]
end
$file = ""

def print_option(option, desc)
  printf("  %-15s %s\n", option, desc)
end

#
# options[:data] = Hash or String, where String is a file path
# options[:headers] = Hash of headers
# options[:method] = The HTTP method to use
#
def curl_command(path, options = {}, flags = nil)
  options[:headers] ||= {}

  flags = Array(flags)

  if options[:data]
    options[:method] ||= "POST"
  else
    options[:method] ||= "GET"
  end

  cmd = "curl -X #{options[:method]}"

  # Add Data
  if options[:data].is_a?(Hash)
    cmd += " -d '#{options[:data].to_json}'"
    options[:headers]["Content-Type"] = "application/json"
  elsif options[:data].is_a?(String)
    cmd += " -T #{options[:data]}"
    options[:headers]["Content-Type"] = "application/octet-stream"
  end

  # Add the headers
  options[:headers].each do |name, value|
    cmd += " -H '#{name}:#{value}'"
  end

  unless flags.empty?
    cmd += " #{flags.join(' ')}"
  end

  # Add the url
  cmd += " #{$server}#{path}"

  unless options[:keep_stderr]
    cmd += " 2> /dev/null"
  end

  return cmd
end

def print_usage
  puts <<-eos
  Usage: dx_upload [options] FILE

  dx_upload is a tool for uploading files to DNAnexus.
  Understand semantics of Environment variables:
  APISERVER_HOST, APISERVER_PORT, and SECURITY_CONTEXT

  Authentication
    If SECURITY_CONTEXT env variable is present, it is used for authentication.
    Else:
      -u flag uses an existing userId
      -c flag creates new user for this request (will be soon deprecated).
  Options:
  eos

  print_option("-c", "Create a new user for this request")
  print_option("-s SERVER", "The server to use, http://localhost:8124 by default")
  print_option("-u USER_ID", "User ID to use for the creation of this file")
  print_option("-v", "Enable verbose output")

  puts ""
end

def log(msg)
  puts("[LOG] #{msg}") if $verbose
end

def fail(msg)
  puts("Error #{msg}")
  exit -1
end

def create_token
  # Get the auth token
  #cmd = curl_command("/accounts/GetAccessToken?userId=#{$user_id}")
  #$token = `#{cmd}`
  #puts "Acquired access token #{$token}"

  #Anurag - Making changes to support new oauth2 protocol
  # Generate a random token and associate to the particular user
  $token = (0...6).map{ ('a'..'z').to_a[rand(26)] }.join
  #Set expiration date for a month right now
  expires = (Integer(`date +%s`.chop) + 24*30*60*60)*1000
  payload = {"userId" => Integer($user_id), "jobId" => "0", "token" => $token, "expires" => expires, "scope" => "none"}
  cmd = curl_command("/auth/token", {:data => payload, :header => {"Content-type" => "application/json"}});
  resp = `#{cmd}`
  $token = "Bearer " + $token;
  return $token
end

def create_new_user
  # Create the new user

  # create a new user name (random string for now)
  name = (0...10).map{ ('a'..'z').to_a[rand(26)] }.join
  cmd = curl_command("/users", {:data => {"password" => "password", "name" => name}})
  response = `#{cmd}`
  $user_id = JSON.parse(response)["id"]
  puts "Created user #{$user_id}"

  return create_token
end

def create_upload(file_path)
  #auth = {"userId" => $user_id, "token" => $token}.to_json
  # Create the upload
  cmd = curl_command("/uploads", {:method => "POST", :headers => {"Authorization" => $token}})
  response_hash = JSON.parse(`#{cmd}`)

  upload_id = response_hash["id"]
  log("Created upload #{upload_id}")
  part_url = response_hash["parts"].first

  # Perform the upload
  log("Uploading the data to part 1: #{$server}#{part_url}")

  cmd = curl_command(part_url,
                    {:method => "PUT", :data => file_path, :headers => {"Authorization" => $token}, :keep_stderr => true},
                    "-#")
  response = `#{cmd}`
  log("Upload response #{response}")

  # Create the file from the upload
  payload = {"id" => upload_id}
  cmd = curl_command("/files", {:data => payload, :headers => {"Authorization" => $token}}, "-i") + " | grep Location"
  location = `#{cmd}`

  # Extract the url from the headers
  url = location.split(':')[1].strip
  log("File created successfully, #{url}")

  # add the name property to the File object
  nm = File.basename(file_path)
  cmd = curl_command("#{url}/properties", {:method => "POST", :data => { :name => nm }, :headers => { "Authorization" => $token}})
  void = `#{cmd}`
  log("Set name to #{nm}")

  puts url
end

if ARGV.length < 1
  print_usage()
  exit -1
end

# Parse options
make_new_user = false

OptionParser.new do |opts|
  opts.banner = "Usage: dx_upload [options] FILE"

  opts.on("-c") do |c|
    make_new_user = true
  end

  opts.on("-s:", String) do |s|
    $server = s
  end

  opts.on("-v") do |v|
    $verbose = true
  end

  opts.on("-u:") do |u|
    $user_id = u.to_i
  end

end.parse!

fail("Please provide a file to upload") if ARGV.length == 0

if($token == nil)
  if make_new_user
    create_new_user
  elsif $user_id
    create_token
  else
    fail("SECURITY_CONTEXT env variable is absent.\nYou must use either the -c or -u flag")
  end
end

file = ARGV[0]

log "Using server #{$server}"
log "Using authentication token #{$token}"

if !File.exists?(file)
  puts "#{file} could not be found"
end

create_upload(file)

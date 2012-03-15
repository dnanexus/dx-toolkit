require 'rubygems'
require 'net/http'
require 'net/https'
require 'json'
require 'uri'

class Nucleus
  def initialize
    @headers = { 'Content-Type' => 'application/json; charset=utf-8' }
    @http = Net::HTTP.new("localhost", 8124)
    @url = "http://localhost:8124/"
  end

  def set_auth(auth)
    @headers["Authorization"] = auth
  end

  def get_object(url, error_msg)
    resp, data = @http.get(url, @headers)
    return data if (resp.class == Net::HTTPOK)
    raise error_msg
  end

  def create_object(url, objSpec, id_key, error_msg)
    resp, data = @http.post(url, objSpec, @headers)
    if (resp.class == Net::HTTPOK)
      id = JSON.parse(data)[id_key]
      return id if ! id.nil?
    end
    raise error_msg
  end

  def create_upload(num_parts)
    resp, data = @http.post("/uploads", {"num_parts" => num_parts}.to_json, @headers)
    return JSON.parse(data) if (resp.class == Net::HTTPCreated)
    raise "Error when create upload"
  end

  def put_upload(url, data)
    headers = { 'Content-Type' => 'charset=utf-8', "Authorization" => @headers["Authorization"] }
    resp, data = @http.put(url, data, headers);
    raise "Error when update data" if resp.class != Net::HTTPOK
  end

  def create_file(fileSpec)
    resp, data = @http.post("/files", fileSpec, @headers);
    if (resp.class == Net::HTTPCreated)
      return $1 if (resp["location"] =~ /files\/(\d+)/)
    end
    raise "Error when create file" 
  end

  def create_app(appSpec)
    return create_object("/apps", appSpec, "appID", "Error when creating App")
  end

  def run_app(app_id, jobSpec)
    return create_object("/apps/#{app_id}/jobs", jobSpec, "jobID", "Error when creating Job")
  end

  def run_job(job_id, jobSpec)
    return create_object("/jobs/#{job_id}/jobs", jobSpec, "jobID", "Error when creating Job")
  end

  def get_app(app_id)
    return get_object("/apps/#{app_id}", "Error when looking for App #{app_id}")
  end

  def get_job(job_id)
    return get_object("/jobs/#{job_id}", "Error when looking for Job #{job_id}")
  end

  def create_table(tableSpec)
    resp, data = @http.post("/tables", tableSpec, @headers);
    if (resp.class == Net::HTTPOK)
      return $1 if (resp["location"] =~ /tables\/(\d+)/)
    end
    raise "Error when create table"
  end
=begin
  def create_table(tableSpec)
    return create_object("/tables", tableSpec, "tableID", "Error when creating Table")
  end
=end
  def append_rows(table_id, nonce, rows)
    resp, data = @http.post("/tables/#{table_id}/rows", JSON.generate( { 'nonce'=>nonce, 'data'=>rows } ), @headers)
    return JSON.parse(data) if (resp.class == Net::HTTPOK)
    raise "Error when append rows"
  end

  def close_table(table_id)
    resp, data = @http.put("/tables/#{table_id}", JSON.generate( { 'status'=>'CLOSED' } ), @headers)
    return true if ((resp.class == Net::HTTPOK) && (JSON.parse(data)["status"] == "CLOSED"))
    return false
  end

  def get_table(table_id)
    return JSON.parse(get_object("/tables/#{table_id}", "Error when getting table"))
  end

  def get_rows(type, table_id, columns, offset, limit)
    columns_str = URI.escape(JSON.generate(columns), Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))
    data = get_object("/#{type}/#{table_id}/rows?columns=#{columns_str}&offset=#{offset}&limit=#{limit}", "Error when getting rows")
    return JSON.parse(data)["rows"]
  end

  def create_coordstable(table_id)
    resp, data = @http.post("/coords_tables", {"sourceTable" => @url + "tables/" + table_id}.to_json, @headers);
    if (resp.class == Net::HTTPOK)
      return $1 if (resp["location"] =~ /coords_tables\/(\d+)/)
    end
    raise "Error when create table"
  end

  def get_coordstable(table_id)
    return JSON.parse(get_object("/coords_tables/#{table_id}", "Error when getting coordstable"))
  end

  def set_property(type, object_id, name, value)
    resp = @http.send_request('PUT', "/#{type}/#{object_id}/properties/#{name}", value, @headers)
    return if (Net::HTTPSuccess === resp)
    raise "Error when set property"
  end

  def get_property(type, object_id, name)
    resp, data = @http.get("/#{type}/#{object_id}/properties/#{name}", @headers)
    return data if (Net::HTTPSuccess === resp)
    raise "Error when set property"
  end

  def update_job_output(table_id, output)
    resp, data = @http.post("/jobs/#{table_id}/output", output, @headers);
  end
end

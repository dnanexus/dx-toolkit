require 'rubygems'
require 'syslog'

class LogMessage
  @@processesPreTag = {
    "logServer" => "e129b093c53bc32f2907a15542b02708",
    "executionManager" => "da9c05f3a4fe651c90ea1373a4592277",
    "developer" => "5e8edd851d2fdfbd7415232c67367cc3",
    "app" => "d2a57dc1d883fd21fb9951699df71cc7",
    "webServer" => "2239c29d5987238724a34ea5ffcbd0f1",
    "system" => "54b53072540eeeb8f8e9343e71f28176",
    "apiServer" => "df31038e649c9e180f2b6ec54106aaaa",
    "test" => "098f6bcd4621d373cade4e832627b4f6",
    "jobManager" => "4d4f3fcbfe0468d32eda6621179aae5f"
  }

  @@processes = {
    "logServer" => "LOG",
    "executionManager" => "EM",
    "developer" => "DEV",
    "app" => "APP",
    "webServer" => "WEB",
    "system" => "SYS",
    "apiServer" => "API",
    "test" => "TST",
    "jobManager" => "JM",
    "cloudManager" => "CM"
  }

  @@head = "DNAnexus"

  def self.processes
    @@processes
  end

  def self.head
    @@head
  end

  def self.processIDs
    ret_val = {}
    @@processes.each{|key, val| ret_val[val] = key}
    return ret_val
  end

  def self.processPreTagIDs
    ret_val = {}
    @@processesPreTag.each{|key, val| ret_val[val] = key}
    return ret_val
  end

  def self.utcTimeString
    t = Time.now
    t -= t.utc_offset
    sprintf("%04d-%02d-%02d %02d:%02d:%02d", t.year, t.month, t.day, t.hour, t.min, t.sec)
  end

  def self.prepareMsg(hash)
    hash["facility"] = Syslog::LOG_USER if hash["facility"].nil?
    hash["level"] = Syslog::LOG_INFO if hash["level"].nil?
    hash["userID"] = hash["userID"].to_s
    hash["jobID"] = hash["jobID"].to_s
    hash["timestamp"] = utcTimeString
  end

  def self.msg(hash)
    return false if @@processes[hash["process"]].nil?
    prepareMsg(hash)
    tag = @@head + @@processes[hash["process"]]
    tag += "-" + hash["tag"] if ! hash["tag"].nil?
    tag += "_" if hash["dbStore"]
    msg = "[#{hash["timestamp"]}] #{hash["userID"]}-#{hash["jobID"]} [msg] #{hash["msg"]}"

    Syslog.open(tag, hash["facility"]) {|s| s.log(hash["level"], msg)}
  end

  def self.processList
    ret_val = []
    @@processes.each{|key, val| ret_val << key}
    return ret_val
  end
end

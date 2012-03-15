$:.unshift File.dirname(File.expand_path(__FILE__))
require 'rubygems'
require 'log_message'
require 'json'
require 'fileutils'

class LogParser
  @@processes = LogMessage.processes
  @@processIDs = LogMessage.processIDs
  @@processPreTagIDs = LogMessage.processPreTagIDs
  @@head = LogMessage.head

  def self.getTime(line)
    return [nil, line] if line[0, 5] == '[msg]'
    return [nil, line] if line[0].ord != 91
    return [nil, line] if (index = line.index(']')).nil?
    return [line[1, index - 1].strip, line[index + 2, line.size - index - 2]]
  end

  def self.getNextToken(line)
    return [nil, line] if (index = line.index(' ')).nil?
    return [nil, line] if index == 0
    return [line[0, index].strip, line[index + 1, line.size - index - 1]]
  end

  def self.normal(line)
    ret_val = {"userID" => "", "jobID" => ""}

    ret_val["receivedTime"], line = getTime(line.strip)

    process, line = getNextToken(line.strip)
    return nil if process.nil?

    process = process.sub(/^DNAnexus/, "").sub(/:$/, "")
    if process[process.length-1].ord == 95
      ret_val["dbStore"] = true
      info = process[0, process.length-1].split("-")
    else
      info = process.split("-")
    end
    if ! @@processIDs[info[0]].nil?
      ret_val["process"] = @@processIDs[info[0]]
      ret_val["processTag"] = process
    elsif ! @@processPreTagIDs[info[0]].nil?
      ret_val["process"] = @@processPreTagIDs[process]
      ret_val["processTag"] = @@processes[ret_val["process"]]
    end
    return nil if ret_val["process"].nil?

    ret_val["hostname"], line = getNextToken(line)
    ret_val["hostname"] = `hostname`.strip if ret_val["hostname"].nil?

    info, line = getNextToken(line.strip)
    return nil if info.nil?
    return nil if (property = info.split(".")).size != 2
    ret_val["facility"], ret_val["level"] = property[0], property[1]
    
    ret_val["timestamp"], line = getTime(line.strip)
    line.strip!
    if line[0, 5] == '[msg]'
      ret_val["msg"] = line[5, line.size-5].strip
    else
      info, line = getNextToken(line.strip)
      return nil if info.nil?

      ids = info.split("-")
      ret_val["userID"] = ids[0] if ids.size > 0
      ret_val["jobID"] = ids[1] if ids.size == 2

      line.strip!
      ret_val["msg"] = line[5, line.size-5].strip if line[0, 5] == '[msg]'
    end

    return nil if ret_val["msg"].nil?
    return ret_val
  end

  def self.compareMsgHash(m1, m2)
    return [0, {}] if m1.nil? && m2.nil?
    return [-1, m2] if m1.nil?
    return [1, m1] if m2.nil?
    ret_val = (m1.keys + m2.keys).uniq.inject({}) do |memo, key|
      memo[key] = [m1[key], m2[key]] unless m1[key] == m2[key]
      memo
    end
    return [0, ret_val]
  end

  def self.msgHash2Str(m)
    t = (m["receivedTime"].nil?) ? "" : "[" + m["receivedTime"] + "]"
    t2 = (m["timestamp"].nil?) ? "" : "[" + m["timestamp"] + "]"
    return [t, m["processTag"], m["hostname"], m["facility"] + "." + m["level"], t2, m["userID"] + "-" + m["jobID"], "[msg]", m["msg"]].join(" ").strip
  end
end

class LogFilterReader
  @@processList = LogMessage.processList
  @@facilityList = ["kern", "auth", "authpriv", "cron", "daemon", "ftp", "local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7", "lpr", "mail", "news", "syslog", "user", "uucp"]
  @@levelList = ["debug", "info", "notice", "warning", "err", "alert", "crit", "emerg"]

  def self.readFilterFromFile(fname)
    return nil if ! File.exist?(fname)
    f = File.open(fname, "r")
    body = f.read
    f.close
    return parse(body)
  end

  def self.parse(str)
    options_orig = JSON.parse(str.downcase)
    options = {}
    options_orig.each{|key, val|
      option = getOptions(val)
      optionList(@@processList, "process", key).each{|p| options[p] = option}
    }
    return options
  end

  def self.optionList(list, name, v)
    v = v.strip.gsub(" ", "")
    ret_val = (v == "all") ? list : v.split(",")
    ret_val.each{|value| raise "Invalid #{name} \"#{value}\", possible values are #{list.join(", ")}" if ! list.include?(value) }
  end

  def self.optionIntList(v)
    ret_val = []
    v.strip.split(",").each{|x| ret_val << x.to_i}
    return ret_val
  end

  def self.getOptions(hash)
    option = {"hostname" => [], "facility" => ["user"], "level" => ["info"], "userID" => [], "jobID" => [] }

    hash.each do |key, val|
      if key == "facility" 
        option["facility"] = optionList(@@facilityList, "facility", val)
      elsif key == "level"
        option["level"] = optionList(@@levelList, "level", val)
      elsif key == "hostname" 
        option["hostname"] = val.strip.gsub(" ", "").split(",")
      elsif key == "usrid"
        option["usrid"] = optionIntList(val)
      elsif key == "jobid"
        option["jobid"] = optionIntList(val)
      end   
    end
    return option
  end

  def self.addFilter(id)
    LogMessage.msg({"process" => "logServer", "msg" => "AddFilter #{id}"})
  end

  def self.rmFilter(id)
    LogMessage.msg({"process" => "logServer", "msg" => "RmFilter #{id}"})
  end
end

class LogFilter
  def initialize(options)
    @options = options
  end

  def verify(hash)   
    return false if @options[hash["process"].downcase].nil?
    return verifyOption(hash, @options[hash["process"].downcase])
  end

  def verifyOption(hash, option)
    return false if option["hostname"].size > 0 && (! option["hostname"].include?(hash["hostname"].downcase))
    return false if option["facility"].size > 0 && (! option["facility"].include?(hash["facility"].downcase))
    return false if option["level"].size > 0 && (! option["level"].include?(hash["level"].downcase))
    return false if option["userID"].size > 0 && (! option["userID"].include?(hash["userID"].to_i))
    # To do: implementing check job ID
    return true
  end
end

class LogFilterMgr
  def initialize(path = "/srv/nucleus/log/filters")
    @path = path
    loadFilters
  end

  def loadFilters
    `rm -rf #{@path}` if File.exists?(@path) && (! File.directory?(@path))
    if ! File.exists?(@path)
      FileUtils.mkdir_p(@path)
      return
    end

    @filters = {}
    Dir.entries(filter_path).each { |d| addFilter(d) }
  end

  def addFilter(filterID)
    path = "#{@path}/#{filterID}"
    return false if (options = LogFilterReader.readFilterFromFile(path + "/filter")).nil?
    @filters[filterID] = {"filter" => LogFilter.new(options), "output" => path + "/log"}
    return true
  end

  def removeFilter(filterID)
    filter = @filters.delete(filterID)
    `rm -rf #{filter["output"]}` if ! filter.nil?
  end

  def writeMsg(hash)
    msg = LogParser.msgHash2Str(hash)
    @filters.each{|key, f|
      next if ! f["filter"].verify(hash)
      file = File.open(f["output"], "a")
      file.puts msg
      file.close
    }
  end
end

$:.unshift File.dirname(File.expand_path(__FILE__))

require 'rubygems'
require 'log_parser'
require 'fileutils'

class JobLogger
  @@Root = "/var/log/dnanexus/job"

  def self.jobPath(jobID)
    str = "%016x" % jobID.to_i
    return [@@Root, str[0, 4], str[4, 4], str[8, 4]].join("/")
  end

  def self.writeHash(hash)
    path = self.jobPath(hash["jobID"])
    if ! File.exists?(path)
      FileUtils.mkdir_p(path)
    elsif ! File.directory?(path)
      `rm path`
      FileUtils.mkdir_p(path)
    end

    fname = path + "/" + hash["jobID"]
    f = File.open(fname, "a")
    f.puts LogParser.msgHash2Str(hash)
    f.flush
    f.close
  end
end

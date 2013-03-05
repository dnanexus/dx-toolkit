module DXRuby
  class DataObject
    def self.foo
      puts "hi"
    end
    private
    def self.bar
      puts "private"
    end
  end
end

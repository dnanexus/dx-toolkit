require File.expand_path('../lib/dxruby/version', __FILE__)

libdir = File.expand_path('../lib', __FILE__)

Gem::Specification.new do |gem|
  gem.name    = 'dxruby'
  gem.version = DXRuby::VERSION
  gem.date    = Date.today.to_s

  gem.summary = "DNAnexus Platform API bindings for Ruby"
  gem.description = "This gem provides native Ruby support for accessing the DNAnexus Platform API"

  gem.authors  = ['Anurag Biyani', 'Andrey Kislyuk', 'George Asimenos']
  gem.email    = 'expert-dev@dnanexus.com'
  gem.homepage = 'https://github.com/dnanexus/dx-toolkit'

#  gem.files = Dir[libdir+'/**/*']
  gem.files = Dir.glob('lib/**/*')
  gem.require_paths = %w['lib']
#  puts "foo"
#  puts Dir.pwd

  gem.add_development_dependency('rake')
  gem.add_development_dependency('rspec', ["~> 2.0"])
end

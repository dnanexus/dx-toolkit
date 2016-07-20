require File.expand_path('../lib/dxruby/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name    = 'dxruby'
  gem.version = DX::VERSION
  gem.date    = Time.now.strftime("%Y-%m-%d")

  gem.summary = "DNAnexus Platform API bindings for Ruby"
  gem.description = "This gem provides native Ruby support for accessing the DNAnexus Platform API"

  gem.authors  = ['Anurag Biyani', 'Andrey Kislyuk', 'George Asimenos']
  gem.email    = 'support@dnanexus.com'
  gem.homepage = 'https://github.com/dnanexus/dx-toolkit'

  gem.rubyforge_project = 'dxruby'

  gem.files = Dir.glob('lib/**/*')
  gem.require_paths = ["lib"]

  gem.add_dependency('json', '~> 1.8.3')

  gem.add_development_dependency('rake')
  gem.add_development_dependency('rspec', ["~> 2.0"])
  gem.add_development_dependency('yard')
end

dxruby: DNAnexus Ruby API
=========================

[API Documentation](http://autodoc.dnanexus.com/bindings/ruby/current/)

Build dependencies
------------------

* Ruby 1.8+
* rubygems
* git

### Ubuntu

Use `apt-get install rubygems git` to build with Ruby 1.8, or `apt-get install
ruby1.9.3 git make` to build with Ruby 1.9.

### OS X

On OS X, dependencies may fail to install using Apple Ruby and the XCode
toolchain. Instead, use `brew install ruby` to install the
[Homebrew](http://mxcl.github.com/homebrew/) Ruby 1.9.

Building
--------

    make ruby

Using the package
-----------------

The `environment` file will prepend to your `GEM_PATH` (see
[Installing the toolkit](#installing-the-toolkit)). To use `dxruby`, run:

```
require 'rubygems'
require 'dxruby'
```

In Ruby 1.9, `require 'rubygems'` is not necessary.

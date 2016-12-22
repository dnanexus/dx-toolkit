DNAnexus Ruby API
=================

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

    make
    # For API documentation...
    make doc

Using the package
-----------------

Set `GEM_PATH` appropriately:

    export GEM_PATH="${DNANEXUS_HOME}/lib/rubygems:$GEM_PATH"

Then, in your Ruby code:

    require 'rubygems'
    require 'dxruby'

In Ruby 1.9, `require 'rubygems'` is not necessary.

Tests / Sample Code
-------------------

    ruby test/test_dxruby.rb

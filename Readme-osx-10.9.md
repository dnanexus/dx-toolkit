Building on OS X 10.9
=====================

As of OS X 10.9, the steps to build the DNAnexus SDK and utilities from source are a bit different than the procedure for 10.8 and earlier.

On OS X 10.9 you can build the SDK and utils using either:

* GCC (provided via Homebrew, MacPorts, or Fink)
* Clang (provided by Apple, via the Command Line Tools for Xcode)

The environment setup steps for each option are shown below.

### Setup steps for GCC-based build
-----------------------------------

1. Install the [Command Line Tools for XCode](https://developer.apple.com/downloads/). (Free registration required with Apple)

1. Install the following packages from source or via [Homebrew](http://mxcl.github.com/homebrew/), [Fink](http://www.finkproject.org/), or [MacPorts](http://www.macports.org/):

  * [CMake](http://www.cmake.org/cmake/resources/software.html)
      * On Homebrew: ```brew install cmake```
      * On MacPorts: ```sudo port install cmake```
  * GCC >= 4.6
      * On MacPorts, install and select GCC with:

        ```
        sudo port install gcc47
        sudo port select --set gcc mp-gcc47
        ```

    * On Homebrew, install and select an up-to-date version of GCC with:

        ```
        brew tap homebrew/versions
        brew install gcc47
        export CC=gcc-4.7
        export CXX=g++-4.7
        ```
  * bison >= 2.7, autoconf, automake
    * On Homebrew: `brew install bison autoconf automake`
    * On MacPorts: `sudo port install bison autoconf automake`

  * Boost 1.55:
    * On OS X 10.9, boost must be compiled from source with GCC:

      ```brew install boost --build-from-source --cc=gcc-4.7```

      This is necessary to prevent dxcpp linker errors like the following:

      ```
      Undefined symbols for architecture x86_64:
        "boost::match_results<__gnu_cxx::__normal_iterator<char const*,  ...
      ```

  * Python 2.7
    * On Homebrew:

      ```
      brew install python
      brew link python
      sudo pip install virtualenv
      sudo pip install --upgrade setuptools
      ```
      
      Make sure to place /usr/local/bin ahead of /usr/bin in your PATH so the homebrew Python is used in later steps:
      ```
      export PATH=/usr/local/bin:$PATH
      ```

      Note: If you skip that step and use the Apple-provided Python, you'll see this error during installation of the Python psutil package:

      ```
      gcc-4.8: error: unrecognized command line option '-Wshorten64-to-32'
      ```

      That error occurs because the default Python install is configured to pass the -Wshorten64-to-32 option to the compiler (see 'python-config --cflags'), but GCC doesn't recognize it (since that option is specific to the Clang compiler). Installing Python via homebrew avoids that issue.

1. Build the DNAnexus SDK with GCC:

  ```
  export CC=gcc-4.7
  export CXX=g++-4.7

  cd dx-toolkit
  make
  ```


### Setup steps for Clang-based build
-------------------------------------

Using Clang to build the SDK and utils obviates the need to install GCC.

1. Install the [Command Line Tools for XCode](https://developer.apple.com/downloads/). (Free registration required with Apple)

1. Install `pip` and `virtualenv` for Python:

    ```
    easy_install-2.7 pip
    pip-2.7 install virtualenv
    ```

1. Install the following packages from source or via [Homebrew](http://mxcl.github.com/homebrew/), [Fink](http://www.finkproject.org/), or [MacPorts](http://www.macports.org/):

  * [CMake](http://www.cmake.org/cmake/resources/software.html) (```sudo port install cmake``` or ```brew install cmake```)
  * bison >= 2.7, autoconf, automake
    * On Homebrew:
      ```
      brew install bison autoconf automake
      ```
    * On MacPorts:
      ```
      sudo port install bison autoconf automake
      ```

1. Patch and build Boost 1.55 with Clang:
  * Boost must be compiled from source and patched to support Clang.

  This is fairly easy with homebrew; first, edit the boost formula:
    ```
    brew edit boost
    ```

    Then go to the opening ```stable do``` line in the formula, and just under it, insert this patch:
    ```
    # Patch boost:thread for Clang
    #
    # See: http://lists.boost.org/Archives/boost/2013/11/208241.php
    patch :p2 do
      url "https://github.com/boostorg/thread/commit/67528fc9.diff"
      sha1 "e3c6855e4074ea3923c844232156deaaa58fb558"
    end
    ```

    Finally, compile boost from source to ensure the Clang patch is applied:
    ```
    brew install boost --build-from-source
    ```

1. Build the SDK:
    ```
    cd dx-toolkit
    make
    ```

### Upload agent build setup steps for Clang
--------------------------------------

Building the upload agent with Clang requires a few changes to the dx-toolkit Makefiles:

1. In dx-toolkit/src/Makefile

```
diff --git a/src/Makefile b/src/Makefile
index 0cf3837..b67efc1 100644
--- a/src/Makefile
+++ b/src/Makefile
@@ -376,7 +376,15 @@ endif

 boost_build:
 	cd boost; ./bootstrap.sh --with-libraries=${BOOST_LIBS}
+# Clang patches for boost::thread and boost::atomic
+ifeq ($(UNAME), Darwin)
+	cd boost; curl https://github.com/boostorg/atomic/commit/6bb71fdd8f7cc346d90fb14beb38b7297fc1ffd9.diff | patch -p2
+	cd boost; curl https://github.com/boostorg/atomic/commit/e4bde20f2eec0a51be14533871d2123bd2ab9cf3.diff | patch -p2
+	cd boost; curl https://github.com/boostorg/thread/commit/67528fc9.diff | patch -p2
+	cd boost; ./b2 --toolset=darwin --layout=tagged -j8 stage
+else
 	cd boost; ./b2 --layout=tagged -j8 stage
+endif

 curl: c-ares/stage/lib/libcares.la curl/stage/lib/libcurl.la
```

1. src/ua/Makefile

```
diff --git a/src/ua/Makefile b/src/ua/Makefile
index d9bec45..c821d3c 100644
--- a/src/ua/Makefile
+++ b/src/ua/Makefile
@@ -99,8 +99,8 @@ else ifeq ($(UNAME), Linux)
 else ifeq ($(UNAME), Darwin)
 	cp -af {$(curl_dir)/lib,$(cares_dir)/lib,$(libmagic_dir)/lib,$(boost_dir)/stage/lib,$(openssl_dir)}/*.dylib dist/
 	mkdir -p dist/resources && install ca-certificates.crt dist/resources/
-	install $$(dyldinfo -dylibs ua|grep libstdc++) dist/
-	install $$(dyldinfo -dylibs ua|grep libgcc) dist/
+	install $$(dyldinfo -dylibs ua|grep libc++) dist/
 	install -s ua dist/
 	for bin in dist/ua dist/*.dylib; do \
 	    for lib in $$(dyldinfo -dylibs $$bin|egrep -v '(for arch|attributes|/usr/lib)'); do \
```


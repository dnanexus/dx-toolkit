##Installing Upload Agent on Windows

####1. Install MinGW & MSYS
Follow instructions here : http://mingw.org/

####2. Install packages for msys and msysgit
Once you have installed msys and MinGW, run following commands on mingw shell
```bash
$ mingw-get update
$ mingw-get install msys-wget msys-bzip2
```
Note: You may need these packages as well (not sure): ```libopenssl msys-openssl msys-libmagic msys-zlib```

We used ``msysgit`` for accessing git features in MinGW shell. Follow instructions here to install it: http://code.google.com/p/msysgit/

####3. Checkout the dx-toolkit repo
```
git checkout https://github.com/dnanexus/dx-toolkit.git
```

####4. Install all dependencies for compiling/linking UA
UA needs, libcurl, boost libraries, libmagic, and, zlib for compiling/linking. You can download/install all of them by running this shell script in MinGW shell
```bash
$ /path/to/dx-toolkit/src/ua/windows_build/install_all_deps.sh
```
Get some coffee, while it builds various libraries for you! :)
####5. Compile and make a distribution package for UA
Run following in MinGW shell
```bash
$ cd /path/to/dx-toolkit/src/ua
$ make dist
```
This will create a zip archive ("ua-VERSION.zip") in ./build/ directory.

Voila! You are all set to run UA on windows. Just unzip this file (which contains all the required DLLs, magic database, and the actual executable), and run ```ua``` from command line (the usual windows command prompt will work just fine).
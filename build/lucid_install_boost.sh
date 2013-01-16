#!/bin/bash -ex

# Installs boost 1.48 (required for dx C++ executables) into /usr/local.
#
# <Tested on Ubuntu 10.04>
#
# Relevant bits go into:
# /usr/local/lib/libboost_filesystem.so.1.48.0
# /usr/local/lib/libboost_program_options.so.1.48.0
# /usr/local/lib/libboost_regex.so.1.48.0
# /usr/local/lib/libboost_system.so.1.48.0
# /usr/local/lib/libboost_thread.so.1.48.0

# Short-circuit sudo when running as root. In a chrooted environment we are
# likely to be running as root already, and sudo may not be present on minimal
# installations.
if [ "$USER" == "root" ]; then
  MAYBE_SUDO=''
else
  MAYBE_SUDO='sudo'
fi

$MAYBE_SUDO apt-get install --yes g++ curl

TEMPDIR=$(mktemp -d)

pushd $TEMPDIR
curl -O http://superb-dca2.dl.sourceforge.net/project/boost/boost/1.48.0/boost_1_48_0.tar.bz2
tar -xjf boost_1_48_0.tar.bz2
cd boost_1_48_0
./bootstrap.sh --with-libraries=filesystem,program_options,regex,system,thread
# --layout=tagged installs libraries with the -mt prefix.
$MAYBE_SUDO ./b2 --layout=tagged install

popd
# rm -rf $TEMPDIR

#!/bin/bash -ex

# Installs Python 2.7 (required for running dx-toolkit) into /usr/local.
#
# <Tested on CentOS 6.2>

# Short-circuit sudo when running as root. In a chrooted environment we are
# likely to be running as root already, and sudo may not be present on minimal
# installations.
if [ "$USER" == "root" ]; then
  MAYBE_SUDO=''
else
  MAYBE_SUDO='sudo'
fi

$MAYBE_SUDO yum groupinstall -y "Development tools"
$MAYBE_SUDO yum install -y zlib-devel bzip2-devel openssl-devel ncurses-devel readline

# Install Python 2.7.3, distribute, and pip.

TEMPDIR=$(mktemp -d)

pushd $TEMPDIR
curl -O http://www.python.org/ftp/python/2.7.3/Python-2.7.3.tar.bz2
tar -xjf Python-2.7.3.tar.bz2
cd Python-2.7.3
./configure --prefix=/usr/local
make
$MAYBE_SUDO make altinstall

PYTHON=/usr/local/bin/python2.7

cd ..

curl -O http://pypi.python.org/packages/source/d/distribute/distribute-0.6.30.tar.gz
tar -xzf distribute-0.6.30.tar.gz
(cd distribute-0.6.30; $MAYBE_SUDO $PYTHON setup.py install)

curl -O http://pypi.python.org/packages/source/p/pip/pip-1.2.1.tar.gz
tar -xzf pip-1.2.1.tar.gz
(cd pip-1.2.1; $MAYBE_SUDO $PYTHON setup.py install)

popd
# rm -rf $TEMPDIR

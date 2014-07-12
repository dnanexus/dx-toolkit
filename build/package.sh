#!/bin/bash -e
#
# Copyright (C) 2013-2014 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

ostype=$(uname)

product_name=$1
if [[ $product_name == "" ]]; then
    product_name="unknown"
fi
echo "$product_name" > "$(dirname $0)"/info/target

# Hide any existing Python packages from the build process.
export PYTHONPATH=

source "$(dirname $0)/../environment"
cd "${DNANEXUS_HOME}"
make clean
make
rm Makefile
rm -rf debian src/{java,javascript,perl,R,ruby,ua,python/build,{dx-verify-file,dx-contigset-to-fasta,dx-wig-to-wiggle}/build} build/py27_env share/dnanexus/lib/javascript
mv build/Prebuilt-Readme.md Readme.md

"$(dirname $0)/fix_shebang_lines.sh" bin "/usr/bin/env python2.7"

if [[ "$ostype" == 'Linux' ]]; then
  osversion=$(lsb_release -c | sed s/Codename:.//)
  rhmajorversion=$(cat /etc/system-release | sed "s/^\(CentOS\) release \([0-9]\)\.[0-9]\+.*$/\2/")
  # TODO: detect versions that do and don't support mktemp --suffix more
  # reliably. What is known:
  #
  # Supports --suffix:
  #   Ubuntu 12.04
  #   CentOS 6
  #
  # Doesn't support --suffix:
  #   Ubuntu 10.04
  if [[ "$osversion" == 'precise' || "$rhmajorversion" == '6' ]]; then
    temp_archive=$(mktemp --suffix .tar.gz)
  else
    temp_archive=$(mktemp -t dx-toolkit.tar.gz.XXXXXXXXXX)
  fi
elif [[ "$ostype" == 'Darwin' ]]; then # Mac OS
  temp_archive=$(mktemp -t dx-toolkit.tar.gz)
else
  echo "Unsupported OS $ostype"
  exit 1
fi

# TODO: what if the checkout is not named dx-toolkit? The tar commands
# below will fail.
if [[ "$ostype" == 'Linux' ]]; then
  cd "${DNANEXUS_HOME}/.."
  tar --exclude-vcs -czf $temp_archive dx-toolkit
elif [[ "$ostype" == 'Darwin' ]]; then # Mac OS

  # BSD tar has no --exclude-vcs, so we do the same thing ourselves in a
  # temp dir.
  cd "${DNANEXUS_HOME}/.."
  tempdir=$(mktemp -d -t dx-packaging-workdir)
  cp -a dx-toolkit $tempdir

  # Make the packaged readline module OS-agnostic, so non
  # Apple-supplied Python installations can still find them.
  cd $tempdir/dx-toolkit/share/dnanexus/lib/python2.7/site-packages
  # e.g. readline-6.2.4.1-py2.7-macosx-10.7-intel.egg => readline-6.2.4.1-py2.7.egg
  for readline_egg in readline-*; do
    mv $readline_egg ${readline_egg/-macosx-10.*-intel/} || true
  done
  sed -i -e 's/-py2.7-macosx-10\.[0-9]+-intel.egg/-py2.7.egg/' easy-install.pth

  cd $tempdir
  rm -rf dx-toolkit/.git
  tar -czf $temp_archive dx-toolkit
fi

cd "${DNANEXUS_HOME}"

if [[ "$ostype" == 'Linux' ]]; then
  dest_tarball="${DNANEXUS_HOME}/dx-toolkit-$(git describe).tar.gz"
elif [[ "$ostype" == 'Darwin' ]]; then # Mac OS
  dest_tarball="${DNANEXUS_HOME}/dx-toolkit-$(git describe)-osx.tar.gz"
fi

mv $temp_archive $dest_tarball
if [[ "$ostype" == 'Darwin' ]]; then
  rm -rf $tempdir
fi

chmod 664 "${dest_tarball}"
echo "---"
echo "--- Package in ${dest_tarball}"
echo "---"

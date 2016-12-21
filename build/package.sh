#!/bin/bash -e
#
# Copyright (C) 2013-2016 DNAnexus, Inc.
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

#source "$(dirname $0)/../environment"

# Get home directory location
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
export DNANEXUS_HOME="$( cd -P "$( dirname "$SOURCE" )" && pwd )/.."

cd "${DNANEXUS_HOME}"
make clean
make
rm Makefile
rm -rf debian src/{java,javascript,perl,R,ruby,ua,python/build,{dx-verify-file,dx-contigset-to-fasta}/build} build/*_env share/dnanexus/lib/javascript
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

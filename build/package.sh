#!/bin/bash -e

ostype=$(uname)

# Hide any existing Python packages from the build process.
export PYTHONPATH=

source "$(dirname $0)/../environment"
cd "${DNANEXUS_HOME}"
make clean
make
rm Makefile
rm -r debian
mv build/Prebuilt-Readme.md Readme.md

if [[ "$ostype" == 'Linux' ]]; then
  temp_archive=$(mktemp --suffix .tar.gz)
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

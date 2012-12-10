#!/bin/bash -e

# Hide any existing Python packages from the build process.
export PYTHONPATH=

source "$(dirname $0)/../environment"
cd "${DNANEXUS_HOME}"
make clean
make
rm Makefile
rm -r debian
mv build/Prebuilt-Readme.md Readme.md
temp_archive=$(mktemp --suffix .tar.gz)
(cd "${DNANEXUS_HOME}/.."; tar --exclude-vcs -czf $temp_archive dx-toolkit)
mv $temp_archive "${DNANEXUS_HOME}/dx-toolkit-$(git describe).tar.gz"
chmod 664 "${DNANEXUS_HOME}/dx-toolkit-$(git describe).tar.gz"
echo "Package in {DNANEXUS_HOME}/dx-toolkit-$(git describe).tar.gz"

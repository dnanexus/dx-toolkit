#!/bin/bash -e

source "$(dirname $0)/../environment"
cd "${DNANEXUS_HOME}"
make clean
make
make install
temp_archive=$(mktemp --suffix .tar.gz)
(cd "${DNANEXUS_HOME}/.."; tar --exclude-vcs -czf $temp_archive dx-toolkit)
mv $temp_archive "${DNANEXUS_HOME}/dx-toolkit-$(git describe).tar.gz"
echo "Package in {DNANEXUS_HOME}/dx-toolkit-$(git describe).tar.gz"

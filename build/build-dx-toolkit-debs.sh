#!/bin/bash -ex

# Resolve symlinks so we can find the package root
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
root="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

echo $root

cd "$root/.."

# Build dx-toolkit (stable)
git reset --hard
git clean -dxf
debuild --no-tgz-check -us -uc

# Build dx-toolkit-beta
git reset --hard
git clean -dxf

sed s/^dx-toolkit/dx-toolkit-beta/ < debian/changelog > debian/changelog.tmp
mv debian/changelog.tmp debian/changelog

cat debian/control | \
  sed 's/^\(Source\|Package\): dx-toolkit/\1: dx-toolkit-beta/' | \
  sed 's/^Conflicts: \(.*\)dx-toolkit-beta\(.*\)/Conflicts: \1dx-toolkit\2/' > debian/control.tmp
mv debian/control.tmp debian/control

sed 's/debian\/dx-toolkit/debian\/dx-toolkit-beta/' < debian/rules > debian/rules.tmp
chmod +x debian/rules.tmp
mv debian/rules.tmp debian/rules

debuild --no-tgz-check -us -uc

# Build dx-toolkit-unstable
git reset --hard
git clean -dxf

sed s/^dx-toolkit/dx-toolkit-unstable/ < debian/changelog > debian/changelog.tmp
mv debian/changelog.tmp debian/changelog

cat debian/control | \
  sed 's/^\(Source\|Package\): dx-toolkit/\1: dx-toolkit-unstable/' | \
  sed 's/^Conflicts: \(.*\)dx-toolkit-unstable\(.*\)/Conflicts: \1dx-toolkit\2/' > debian/control.tmp
mv debian/control.tmp debian/control

sed 's/debian\/dx-toolkit/debian\/dx-toolkit-unstable/' < debian/rules > debian/rules.tmp
chmod +x debian/rules.tmp
mv debian/rules.tmp debian/rules

debuild --no-tgz-check -us -uc

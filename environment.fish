# -*- Mode: shell-script -*-
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
#
#
# Source this file in a fish shell to initialize DNAnexus environment
# variables:
# $ source environment.fish

if test ! -n "$version" -a -n "(echo $SHELL | grep -q fish)"
    echo "You are not using fish shell. Try 'environment' or 'environment.csh' instead."
    exit
end

set SOURCE (status --current-filename)

while test -L "$SOURCE"
  set SOURCE (readlink "$SOURCE")
end

switch (uname)
  case Darwin FreeBSD NetBSD DragonFly
    set -x DNANEXUS_HOME ( dirname (realpath "$SOURCE" ) )
  case Linux
    set -x DNANEXUS_HOME ( cd -P ( dirname "$SOURCE" ) ; pwd )
  case '*'
    echo "It doesn't look like you're on a linux system"
end

# Get RHEL version
if test -e /etc/redhat-release
  set RHEL_MAJOR_VERSION (grep -o "Red Hat Enterprise Linux .* release [0-9]\+" /etc/redhat-release | sed -e "s/Red Hat Enterprise Linux .* release //")
end

# Detect system installation of dx-toolkit
if [ "$DNANEXUS_HOME" = "/etc/profile.d" ]
  set -x DNANEXUS_HOME "/usr/share/dnanexus"
  # Private Python packages. We really ought not pollute PYTHONPATH with these though.
  set -xg PYTHONPATH /usr/share/dnanexus/lib/python2.7/site-packages $PYTHONPATH
  set -xg CLASSPATH /usr/share/java/dnanexus-api-0.1.0.jar $CLASSPATH
else
  set -x PATH $DNANEXUS_HOME/bin $PATH
  set -xg CLASSPATH $DNANEXUS_HOME/lib/java/*/ $CLASSPATH

  if [ "$RHEL_MAJOR_VERSION" = "7" ]
    set -xg PYTHONPATH $DNANEXUS_HOME/share/dnanexus/lib/python2.7/site-packages:$DNANEXUS_HOME/lib64/python2.7/site-packages $PYTHONPATH
  else
    set -xg PYTHONPATH $DNANEXUS_HOME/share/dnanexus/lib/python2.7/site-packages:$DNANEXUS_HOME/lib/python2.7 $PYTHONPATH
  end
end

# Note: The default I/O stream encoding in Python 2.7 (as configured on ubuntu) is ascii, not UTF-8 or the system locale
# encoding. We reset it here to avoid having to set it for every I/O operation explicitly.
set -x PYTHONIOENCODING UTF-8

# Clean up old session files
if [ $HOME != "" ]
    for session_dir in "$HOME/.dnanexus_config/sessions/"*
        if test ! 'ps -p (basename "$session_dir") &> /dev/null'
            rm -rf "$session_dir"
        end
    end
end

if test -z "$DX_SECURITY_CONTEXT" -a -n "$DX_AUTH_TOKEN"
    set -x DX_SECURITY_CONTEXT "{\"auth_token_type\":\"Bearer\",\"auth_token\":\"$DX_AUTH_TOKEN\"}"
end

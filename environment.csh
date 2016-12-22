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
# Source this file in a csh shell to initialize DNAnexus environment
# variables:
# $ source environment

set SOURCE=`echo $_ | cut -f 2 -d " "`
set SOURCE=`dirname "$SOURCE"`

setenv DNANEXUS_HOME "$SOURCE"

setenv PATH "$DNANEXUS_HOME/bin:$PATH"

if $?PYTHONPATH then
    setenv PYTHONPATH "$DNANEXUS_HOME/share/dnanexus/lib/python2.7/site-packages:$DNANEXUS_HOME/lib/python:$PYTHONPATH"
else
    setenv PYTHONPATH "$DNANEXUS_HOME/share/dnanexus/lib/python2.7/site-packages:$DNANEXUS_HOME/lib/python"
endif

if $?CLASSPATH then
    setenv CLASSPATH "$DNANEXUS_HOME/lib/java/*:$CLASSPATH"
else
    setenv CLASSPATH "$DNANEXUS_HOME/lib/java/*"
endif

setenv PYTHONIOENCODING UTF-8

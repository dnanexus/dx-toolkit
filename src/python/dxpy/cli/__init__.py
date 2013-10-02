# Copyright (C) 2013 DNAnexus, Inc.
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

'''
This submodule contains various parsers and other utilities used
almost exclusively by command-line tools such as dx.
'''

from ..exceptions import err_exit, default_expected_exceptions, DXError

def try_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except:
        err_exit(expected_exceptions=default_expected_exceptions + (DXError,))

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

'''
This submodule contains various parsers and other utilities used
almost exclusively by command-line tools such as dx.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import sys

INTERACTIVE_CLI = True if sys.stdin.isatty() and sys.stdout.isatty() else False

from ..exceptions import err_exit, default_expected_exceptions, DXError
from ..compat import input

def try_call_err_exit():
    err_exit(expected_exceptions=default_expected_exceptions + (DXError,))

def try_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except:
        try_call_err_exit()

def prompt_for_yn(prompt_str, default=None):
    if default == True:
        prompt = prompt_str + ' [Y/n]: '
    elif default == False:
        prompt = prompt_str + ' [y/N]: '
    else:
        prompt = prompt_str + ' [y/n]: '

    while True:
        try:
            value = input(prompt)
        except KeyboardInterrupt:
            print('')
            exit(1)
        except EOFError:
            print('')
            exit(1)
        if value != '':
            if value.lower()[0] == 'y':
                return True
            elif value.lower()[0] == 'n':
                return False
            else:
                print('Error: unrecognized response')
        elif default is not None:
            return default

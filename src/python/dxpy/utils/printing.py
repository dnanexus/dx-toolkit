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
This submodule gives basic utilities for printing to the terminal.
'''

import textwrap, subprocess, os, sys
import json
import platform
from ..compat import USING_PYTHON2, sys_encoding
from ..exceptions import DXCLIError

if sys.stdout.isatty():
    try:
        tty_rows, tty_cols = map(int, subprocess.check_output(['stty', 'size'], stderr=open(os.devnull, 'w')).split())
        std_width = min(tty_cols - 2, 100)
    except:
        tty_rows, tty_cols = 24, 80
        std_width = 78
    color_state = True
else:
    tty_rows, tty_cols = 24, 80
    std_width = 78
    color_state = False

delimiter = None

def CYAN(message=None):
    if message is None:
        return '\033[36m' if color_state else ''
    else:
        return CYAN() + message + ENDC()

def LIGHTBLUE(message=None):
    if message is None:
        return '\033[1;34m' if color_state else ''
    else:
        return LIGHTBLUE() + message + ENDC()

def BLUE(message=None):
    if message is None:
        return '\033[34m' if color_state else ''
    else:
        return BLUE() + message + ENDC()

def YELLOW(message=None):
    if message is None:
        return '\033[33m' if color_state else ''
    else:
        return YELLOW() + message + ENDC()

def GREEN(message=None):
    if message is None:
        return '\033[32m' if color_state else ''
    else:
        return GREEN() + message + ENDC()

def RED(message=None):
    if message is None:
        return '\033[31m' if color_state else ''
    else:
        return RED() + message + ENDC()

def WHITE(message=None):
    if message is None:
        return '\033[37m' if color_state else ''
    else:
        return WHITE() + message + ENDC()

def UNDERLINE(message=None):
    if message is None:
        return '\033[4m' if color_state else ''
    else:
        return UNDERLINE() + message + ENDC()

def BOLD(message=None):
    if message is None:
        return '\033[1m' if color_state else ''
    else:
        return BOLD() + message + ENDC()

def ENDC():
    return '\033[0m' if color_state else ''

def DNANEXUS_LOGO():
    return BOLD() + WHITE() + 'DNAne' + CYAN() + 'x' + WHITE() + 'us' + ENDC()

def DNANEXUS_X():
    return BOLD() + CYAN() + 'x' + WHITE() + ENDC()

def set_colors(state=True):
    global color_state
    color_state = state

def set_delimiter(delim=None):
    global delimiter
    delimiter = delim

def get_delimiter(delim=None):
    return delimiter

def DELIMITER(alt_delim):
    return alt_delim if delimiter is None else delimiter

def fill(string, width_adjustment=0, **kwargs):
    if "width" not in kwargs:
        kwargs['width'] = max(std_width + width_adjustment, 20)
    if "break_on_hyphens" not in kwargs:
        kwargs["break_on_hyphens"] = False
    return textwrap.fill(string, **kwargs)

def pager(content, pager=None, file=None):
    if file is None:
        file = sys.stdout

    pager_process = None
    try:
        if file != sys.stdout or not file.isatty():
            raise DXCLIError() # Just print the content, don't use a pager
        content_lines = content.splitlines()
        content_rows = len(content_lines)
        content_cols = max(len(i) for i in content_lines)

        if tty_rows > content_rows and tty_cols > content_cols:
            raise DXCLIError() # Just print the content, don't use a pager

        if pager is None:
            pager = os.environ.get('PAGER', 'less -RS')
        if platform.system() == 'Windows':
            # Verify if the pager is available on Windows
            try:
                subprocess.call(pager)
            except:
                raise DXCLIError()  # Just print the content, don't use a pager

        pager_process = subprocess.Popen(pager, shell=True, stdin=subprocess.PIPE, stdout=file)
        pager_process.stdin.write(content.encode(sys_encoding))
        pager_process.stdin.close()
        pager_process.wait()
        if pager_process.returncode != os.EX_OK:
            raise DXCLIError() # Pager had a problem, print the content without it
    except:
        file.write(content.encode(sys_encoding) if USING_PYTHON2 else content)
    finally:
        try:
            pager_process.terminate()
        except:
            pass

def refill_paragraphs(string, ignored_prefix='    '):
    """Refills the given text, where the text is composed of paragraphs
    separated by blank lines (i.e. '\n\n'). Lines that begin with
    ignored_prefix are not touched; this can be used to keep indented
    code snippets from being incorrectly reformatted.

    """
    paragraphs = string.split('\n\n')
    refilled_paragraphs = [fill(paragraph) if not paragraph.startswith(ignored_prefix) else paragraph for paragraph in paragraphs]
    return '\n\n'.join(refilled_paragraphs).strip('\n')


def _format_find_projects_results(results):
    for result in results:
        print(result["id"] + DELIMITER(" : ") + result['describe']['name'] +
              DELIMITER(' (') + result["level"] + DELIMITER(')'))


def _format_find_apps_results(results, verbose=False):
    def maybe_x(result):
        return DNANEXUS_X() if result['describe']['billTo'] == 'org-dnanexus' else ' '

    if not verbose:
        for result in results:
            print(maybe_x(result) + DELIMITER(" ") + result['describe'].get('title', result['describe']['name']) + DELIMITER(' (') + result["describe"]["name"] + DELIMITER("), v") + result["describe"]["version"])
    else:
        for result in results:
            print(maybe_x(result) + DELIMITER(" ") + result["id"] + DELIMITER(" ") + result['describe'].get('title', result['describe']['name']) + DELIMITER(' (') + result["describe"]["name"] + DELIMITER('), v') + result['describe']['version'] + DELIMITER(" (") + ("published" if result["describe"].get("published", 0) > 0 else "unpublished") + DELIMITER(")"))


def _format_find_org_members_results(results):
    for result in results:
        print(result["id"] + DELIMITER(" : ") + result['describe']['first'] + DELIMITER(' ') +
              result['describe']['last'] + DELIMITER(' ') + DELIMITER(' (') + result["level"] +
              DELIMITER(')'))


def format_find_results(args, results):
    """
    Formats the output of ``dx find ...`` commands for `--json` and `--brief` arguments; also formats if no formatting
    arguments are given.
    Currently used for ``dx find projects``, ``dx find org_projects``, ``dx find org_apps``,
    and ``dx find org_members``
    """
    if args.json:
        print(json.dumps(list(results), indent=4))
    elif args.brief:
        for result in results:
            print(result['id'])
    else:
        if args.func.__name__ in ("find_projects", "org_find_projects"):
            _format_find_projects_results(results)
        elif args.func.__name__ in ("org_find_members"):
            _format_find_org_members_results(results)
        elif args.func.__name__ in ("org_find_apps"):  # should have "find_apps" here one day
            _format_find_apps_results(results, verbose=args.verbose)

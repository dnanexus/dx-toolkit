#!/usr/bin/env python
# -*- coding: utf-8 -*-
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


from __future__ import print_function, unicode_literals, division, absolute_import

import re, collections
from .printing import (GREEN, BLUE, YELLOW, WHITE, BOLD, ENDC)
from ..compat import str, Mapping

REPLACEMENT_TABLE = (
    '\\x00',    #  0x00 -> NULL
    '\\x01',    #  0x01 -> START OF HEADING
    '\\x02',    #  0x02 -> START OF TEXT
    '\\x03',    #  0x03 -> END OF TEXT
    '\\x04',    #  0x04 -> END OF TRANSMISSION
    '\\x05',    #  0x05 -> ENQUIRY
    '\\x06',    #  0x06 -> ACKNOWLEDGE
    '\\x07',    #  0x07 -> BELL
    '\\x08',    #  0x08 -> BACKSPACE
    '\\t',      #  0x09 -> HORIZONTAL TABULATION
    '\\n',      #  0x0A -> LINE FEED
    '\\x0b',    #  0x0B -> VERTICAL TABULATION
    '\\x0c',    #  0x0C -> FORM FEED
    '\\r',      #  0x0D -> CARRIAGE RETURN
    '\\x0e',    #  0x0E -> SHIFT OUT
    '\\x0f',    #  0x0F -> SHIFT IN
    '\\x10',    #  0x10 -> DATA LINK ESCAPE
    '\\x11',    #  0x11 -> DEVICE CONTROL ONE
    '\\x12',    #  0x12 -> DEVICE CONTROL TWO
    '\\x13',    #  0x13 -> DEVICE CONTROL THREE
    '\\x14',    #  0x14 -> DEVICE CONTROL FOUR
    '\\x15',    #  0x15 -> NEGATIVE ACKNOWLEDGE
    '\\x16',    #  0x16 -> SYNCHRONOUS IDLE
    '\\x17',    #  0x17 -> END OF TRANSMISSION BLOCK
    '\\x18',    #  0x18 -> CANCEL
    '\\x19',    #  0x19 -> END OF MEDIUM
    '\\x1a',    #  0x1A -> SUBSTITUTE
    '\\x1b',    #  0x1B -> ESCAPE
    '\\x1c',    #  0x1C -> FILE SEPARATOR
    '\\x1d',    #  0x1D -> GROUP SEPARATOR
    '\\x1e',    #  0x1E -> RECORD SEPARATOR
    '\\x1f'     #  0x1F -> UNIT SEPARATOR
    )

def escape_unicode_string(u):
    """
    Escapes the nonprintable chars 0-31 and 127, and backslash;
    preferably with a friendly equivalent such as '\n' if available, but
    otherwise with a Python-style backslashed hex escape.
    """
    def replacer(matchobj):
        if ord(matchobj.group(1)) == 127:
            return "\\x7f"
        if ord(matchobj.group(1)) == 92: # backslash
            return "\\\\"
        return REPLACEMENT_TABLE[ord(matchobj.group(1))]
    return re.sub("([\\000-\\037\\134\\177])", replacer, u)

def format_tree(tree, root=None):
    ''' Tree pretty printer.
    Expects trees to be given as mappings (dictionaries). Keys will be printed; values will be traversed if they are
    mappings. To preserve order, use collections.OrderedDict.

    Example:

        print format_tree(collections.OrderedDict({'foo': 0, 'bar': {'xyz': 0}}))

    '''
    formatted_tree = [root] if root is not None else []
    def _format(tree, prefix='    '):
        nodes = list(tree.keys())
        for i in range(len(nodes)):
            node = nodes[i]
            if i == len(nodes)-1 and len(prefix) > 1:
                my_prefix = prefix[:-4] + '└── '
                my_multiline_prefix = prefix[:-4] + '    '
            else:
                my_prefix = prefix[:-4] + '├── '
                my_multiline_prefix = prefix[:-4] + '│   '
            n = 0
            for line in node.splitlines():
                if n == 0:
                    formatted_tree.append(my_prefix + line)
                else:
                    formatted_tree.append(my_multiline_prefix + line)
                n += 1

            if isinstance(tree[node], Mapping):
                subprefix = prefix
                if i < len(nodes)-1 and len(prefix) > 1 and prefix[-4:] == '    ':
                    subprefix = prefix[:-4] + '│   '
                _format(tree[node], subprefix + '    ')
    _format(tree)
    return '\n'.join(formatted_tree)

def format_table(table, column_names=None, column_specs=None, max_col_width=32,
                 report_dimensions=False):
    ''' Table pretty printer.
    Expects tables to be given as arrays of arrays.

    Example:

        print format_table([[1, "2"], [3, "456"]], column_names=['A', 'B'])

    '''
    if len(table) > 0:
        col_widths = [0] * len(list(table)[0])
    elif column_specs is not None:
        col_widths = [0] * (len(column_specs) + 1)
    elif column_names is not None:
        col_widths = [0] * len(column_names)
    my_column_names = []
    if column_specs is not None:
        column_names = ['Row']
        column_names.extend([col['name'] for col in column_specs])
        column_specs = [{'name': 'Row', 'type': 'float'}] + column_specs
    if column_names is not None:
        for i in range(len(column_names)):
            my_col = str(column_names[i])
            if len(my_col) > max_col_width:
                my_col = my_col[:max_col_width-1] + '…'
            my_column_names.append(my_col)
            col_widths[i] = max(col_widths[i], len(my_col))
    my_table = []
    for row in table:
        my_row = []
        for i in range(len(row)):
            my_item = escape_unicode_string(str(row[i]))
            if len(my_item) > max_col_width:
                my_item = my_item[:max_col_width-1] + '…'
            my_row.append(my_item)
            col_widths[i] = max(col_widths[i], len(my_item))
        my_table.append(my_row)

    def border(i):
        return WHITE() + i + ENDC()

    type_colormap = {'boolean': BLUE(),
                     'integer': YELLOW(),
                     'float': WHITE(),
                     'string': GREEN()}
    for i in 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64':
        type_colormap[i] = type_colormap['integer']
    type_colormap['double'] = type_colormap['float']

    def col_head(i):
        if column_specs is not None:
            return BOLD() + type_colormap[column_specs[i]['type']] + column_names[i] + ENDC()
        else:
            return BOLD() + WHITE() + column_names[i] + ENDC()

    formatted_table = [border('┌') + border('┬').join(border('─')*i for i in col_widths) + border('┐')]
    if len(my_column_names) > 0:
        padded_column_names = [col_head(i) + ' '*(col_widths[i]-len(my_column_names[i])) for i in range(len(my_column_names))]
        formatted_table.append(border('│') + border('│').join(padded_column_names) + border('│'))
        formatted_table.append(border('├') + border('┼').join(border('─')*i for i in col_widths) + border('┤'))

    for row in my_table:
        padded_row = [row[i] + ' '*(col_widths[i]-len(row[i])) for i in range(len(row))]
        formatted_table.append(border('│') + border('│').join(padded_row) + border('│'))
    formatted_table.append(border('└') + border('┴').join(border('─')*i for i in col_widths) + border('┘'))

    if report_dimensions:
        return '\n'.join(formatted_table), len(formatted_table), sum(col_widths) + len(col_widths) + 1
    else:
        return '\n'.join(formatted_table)

def flatten_json_array(json_string, array_name):
    """
    Flattens all arrays with the same name in the JSON string

    :param json_string: JSON string
    :type json_string: str
    :param array_name: Array name to flatten
    :type array_name: str
    """

    result = re.sub('"{}": \\[\r?\n\\s*'.format(array_name), '"{}": ['.format(array_name), json_string, flags=re.MULTILINE)
    flatten_regexp = re.compile('"{}": \\[(.*)(?<=,)\r?\n\\s*'.format(array_name), flags=re.MULTILINE)
    while flatten_regexp.search(result):
        result = flatten_regexp.sub('"{}": [\\1 '.format(array_name), result)
    result = re.sub('"{}": \\[(.*)\r?\n\\s*\\]'.format(array_name), '"{}": [\\1]'.format(array_name), result, flags=re.MULTILINE)
    return result

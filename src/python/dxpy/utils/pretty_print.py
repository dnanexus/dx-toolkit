#!/usr/bin/env python
# coding: utf-8

import re
import collections
from dxpy.utils.describe import *

def format_tree(tree):
    ''' Tree pretty printer.
    Expects trees to be given as mappings (dictionaries). Keys will be printed; values will be traversed if they are
    mappings. To preserve order, use collections.OrderedDict.
    
    Example:

        print format_tree(collections.OrderedDict({'foo': 0, 'bar': {'xyz': 0}}))

    '''
    formatted_tree = []
    def _format(tree, prefix=u'  '):
        nodes = tree.keys()
        for i in range(len(nodes)):
            node = nodes[i]
            if i == len(nodes)-1 and len(prefix) > 1:
                my_prefix = prefix[:-2] + u'└─'
                my_multiline_prefix = prefix[:-2] + u'  '
            else:
                my_prefix = prefix[:-2] + u'├─'
                my_multiline_prefix = prefix[:-2] + u'│ '
            n = 0
            for line in node.splitlines():
                if n == 0:
                    formatted_tree.append(my_prefix + line)
                else:
                    formatted_tree.append(my_multiline_prefix + line)
                n += 1

            if isinstance(tree[node], collections.Mapping):
                if i < len(nodes)-1 and len(prefix) > 1 and prefix[-2:] == u'  ':
                    prefix = prefix[:-2] + u'│ '
                _format(tree[node], prefix + u'  ')
    _format(tree)
    return '\n'.join(formatted_tree)

def format_table(table, column_names=None, column_specs=None, max_col_width=32, report_dimensions=False):
    ''' Table pretty printer.
    Expects tables to be given as arrays of arrays.

    Example:
        
        print format_table([[1, "2"], [3, "456"]], column_names=['A', 'B'])

    '''
    col_widths = [0] * len(table[0])
    my_column_names = []
    if column_specs is not None:
        column_names = ['Row']
        column_names.extend([col['name'] for col in column_specs])
        column_specs = [{'name': 'Row', 'type': 'float'}] + column_specs
    if column_names is not None:
        for i in range(len(column_names)):
            my_col = str(column_names[i])
            if len(my_col) > max_col_width:
                my_col = my_col[:max_col_width-1] + u'…'
            my_column_names.append(my_col)
            col_widths[i] = max(col_widths[i], len(my_col))
    my_table = []
    for row in table:
        my_row = []
        for i in range(len(row)):
            my_item = str(row[i])
            if len(my_item) > max_col_width:
                my_item = my_item[:max_col_width-1] + u'…'
            my_row.append(my_item)
            col_widths[i] = max(col_widths[i], len(my_item))
        my_table.append(my_row)
    
    def BLU(i):
        return WHITE + i + ENDC

    type_colormap = {'boolean': BLUE,
                     'integer': YELLOW,
                     'float': WHITE,
                     'string': GREEN}
    for i in 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64':
        type_colormap[i] = type_colormap['integer']
    type_colormap['double'] = type_colormap['float']

    def col_head(i):
        if column_specs is not None:
            return BOLD + type_colormap[column_specs[i]['type']] + column_names[i] + ENDC
        else:
            return BOLD + WHITE + column_names[i] + ENDC

    formatted_table = [BLU(u'┌') + BLU(u'┬').join(BLU(u'─')*i for i in col_widths) + BLU(u'┐')]
    if len(my_column_names) > 0:
        padded_column_names = [col_head(i) + ' '*(col_widths[i]-len(my_column_names[i])) for i in range(len(my_column_names))]
        formatted_table.append(BLU(u'│') + BLU(u'│').join(padded_column_names) + BLU(u'│'))
        formatted_table.append(BLU(u'├') + BLU(u'┼').join(BLU(u'─')*i for i in col_widths) + BLU(u'┤'))

    for row in my_table:
        padded_row = [row[i] + ' '*(col_widths[i]-len(row[i])) for i in range(len(row))]
        formatted_table.append(BLU(u'│') + BLU(u'│').join(padded_row) + BLU(u'│'))
    formatted_table.append(BLU(u'└') + BLU(u'┴').join(BLU(u'─')*i for i in col_widths) + BLU(u'┘'))

    if report_dimensions:
        return '\n'.join(formatted_table), len(formatted_table), len(formatted_table[0])
    else:
        return '\n'.join(formatted_table)

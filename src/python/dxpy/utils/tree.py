#!/usr/bin/env python
# coding: utf-8

import re
import collections

def print_tree(tree):
    ''' Tree pretty printer.
    Expects trees to be given as mappings (dictionaries). Keys will be printed; values will be traversed if they are
    mappings. To preserve order, use collections.OrderedDict.
    
    Example:

        print_tree(collections.OrderedDict({'foo': 0, 'bar': {'xyz': 0}}))

    '''
    _print_tree(tree)

def _print_tree(tree, prefix=u'  '):
    nodes = tree.keys()
    for i in range(len(nodes)):
        node = nodes[i]
        if i == len(nodes)-1 and len(prefix) > 1:
            my_prefix = prefix[:-2] + u'└─'
        elif len(prefix) > 1:
            my_prefix = prefix[:-2] + u'├─'
        else:
            my_prefix = prefix
        print my_prefix + node

        if isinstance(tree[node], collections.Mapping):
            if i < len(nodes)-1 and len(prefix) > 1 and prefix[-2:] == u'  ':
                prefix = prefix[:-2] + u'│ '
            _print_tree(tree[node], prefix + u'  ')

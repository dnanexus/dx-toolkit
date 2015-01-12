#!/usr/bin/env python
#
# Copyright (C) 2013-2014 DNAnexus, Inc.
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

from __future__ import print_function

import os, sys, json, argparse, csv
import dxpy
from dxpy.cli.parsers import *
from dxpy.utils.resolver import *
from dxpy.utils.describe import print_desc

parser = argparse.ArgumentParser(description='Import a local file as a GenomicTable.  The table will be closed after creation.  If no flags are given, the file given will be interpreted based on its contents.',
                                 parents=[stdout_args, json_arg, no_color_arg, parser_dataobject_args, parser_single_dataobject_output_args])
parser.add_argument('filename', help='local filename to import ("-" indicates stdin input)')
parser.add_argument('--gri', nargs=3, metavar=('CHR', 'LO', 'HI'), help='Specify column names to be used as chromosome, lo, and hi columns for a genomic range index (name will be set to "gri"); will also add the type "gri"')
parser.add_argument('--indices', help='JSON for specifying any other indices')
parser.add_argument('--wait', help='Wait until the GTable has finished closing', action='store_true')
parser.add_argument('--csv', help='Interpret the file as a comma-separated format instead of tsv', action='store_true')
parser.add_argument('--columns', help='Comma-separated list of column names to use, e.g. "col1,col2,col3"; non-string types can be specified using "name:type" syntax, e.g. "col1:int,col2:boolean".  If not given, the first line of the file will be used to infer column names.')

def parse_item(item, item_type):
    if item_type == 'string':
        return item
    elif item_type == 'int':
        return int(item)
    elif item_type == 'float':
        return float(item)
    elif item_type == 'boolean':
        if item == '0' or item.lower().startswith('f'):
            return False
        else:
            return True
    else:
        raise Exception('Unrecognized column type: ' + item_type + '\n')

def main(**kwargs):
    if len(kwargs) == 0:
        args = parser.parse_args(sys.argv[1:])
    else:
        args = parser.parse_args(kwargs)

    try:
        process_dataobject_args(args)
    except Exception as details:
        parser.exit(1, unicode(details) + '\n')

    try:
        process_single_dataobject_output_args(args)
    except Exception as details:
        parser.exit(1, unicode(details) + '\n')

    if args.output is None:
        project = dxpy.WORKSPACE_ID
        folder = dxpy.config.get('DX_CLI_WD', u'/')
        if args.filename != '-':
            name = os.path.basename(args.filename)
        else:
            name = None
    else:
        project, folder, name = resolve_path(args.output)
        if name is None and args.filename != '-':
            name = os.path.basename(args.filename)

    args.indices = [] if args.indices is None else json.loads(args.indices)
    if args.gri is not None:
        args.indices.append(dxpy.DXGTable.genomic_range_index(args.gri[0], args.gri[1], args.gri[2]))
        args.types = ['gri'] if args.types is None else args.types + ['gri']

    if args.filename == '-':
        fd = sys.stdin
    else:
        try:
            fd = open(args.filename, 'rb')
        except:
            parser.exit(1, fill(unicode('Could not open ' + args.filename + ' for reading')) + '\n')

    firstrow = fd.readline()

    if args.csv:
        delimiter = ','
        dialect = 'excel'
    else:
        delimiter = '\t'
        dialect = 'excel'
    # else:
    #     # Try to sniff the file format
    #     dialect = csv.Sniffer().sniff(firstrow)
    #     delimiter = dialect.delimiter
    firstrow_reader = csv.reader([firstrow], dialect=dialect,
                                 delimiter=delimiter)
    firstrow_data = firstrow_reader.next()
    reader = csv.reader(fd, dialect=dialect,
                        delimiter=delimiter)

    column_specs = []
    types = []
    if args.columns is not None:
        specs = split_unescaped(',', args.columns)
    else:
        specs = firstrow_data
    for spec in specs:
        if ':' in spec:
            col_type = spec[spec.find(':') + 1:]
            column_specs.append({'name': spec[:spec.find(':')],
                                 'type': col_type})
            if 'int' in col_type:
                types.append('int')
            elif col_type == 'boolean':
                types.append('boolean')
            elif col_type in ['float', 'double']:
                types.append('float')
            elif col_type == 'string':
                types.append('string')
            else:
                parser.exit(1, 'Unrecognized column type: ' + col_type + '\n')
        else:
            column_specs.append({'name': spec,
                                 'type': 'string'})
            types.append('string')
    try:
        dxgtable = dxpy.new_dxgtable(project=project, name=name,
                                     tags=args.tags, types=args.types, 
                                     hidden=args.hidden, properties=args.properties,
                                     details=args.details,
                                     folder=folder,
                                     parents=args.parents,
                                     columns=column_specs,
                                     indices=args.indices)
        if args.columns is not None:
            dxgtable.add_row([ parse_item(firstrow_data[i], types[i]) for i in range(len(types))])
        for row in reader:
            dxgtable.add_row([ parse_item(row[i], types[i]) for i in range(len(types))])
        dxgtable.close(block=args.wait)
        if args.brief:
            print(dxgtable.get_id())
        else:
            print_desc(dxgtable.describe(incl_properties=True, incl_details=True))
    except Exception as details:
        parser.exit(1, fill(unicode(details)) + '\n')

if __name__ == '__main__':
    main()

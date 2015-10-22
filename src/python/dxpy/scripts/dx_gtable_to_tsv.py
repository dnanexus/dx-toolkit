#!/usr/bin/env python
#
# Copyright (C) 2013-2015 DNAnexus, Inc.
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

import os, sys, argparse, csv
import dxpy
from dxpy.utils.resolver import ResolutionError, resolve_existing_path
from dxpy.utils.printing import fill

parser = argparse.ArgumentParser(description="Download a gtable into a tab-separated file.  Provide the --csv flag for commas instead of tabs.  Arbitrary gtable queries can also be provided to obtain a simple filtered subset of the gtable.")
parser.add_argument("path", help="Path to the GTable object")
parser.add_argument('-o', '--output', help='local filename to be saved ("-" indicates stdout output); if not supplied, the object\'s name on the platform will be used, along with any applicable extensions')
parser.add_argument('--no-ext', help='If -o is not provided, do not add an extension to the filename', action='store_true')
parser.add_argument('-f', '--overwrite', help='Overwrite the local file if necessary', action='store_true')
parser.add_argument('--no-header', help='Do not print a header in the CSV or TSV file', action='store_true')
parser.add_argument('--rowid', help='Include the row ID column', action='store_true')
parser.add_argument('--starting', type=int, help='Specify starting row ID', default=0)
parser.add_argument('--limit', type=int, help='Specify limit on # rows to return (by default, all results will be returned)')
#parser.add_argument('--columns', nargs='+', help='Specify a list of columns to display (default all columns)') TODO
parser.add_argument('--gri', nargs=3, metavar=('CHR', 'LO', 'HI'), help='Specify chromosome name, low coordinate, and high coordinate for Genomic Range Index')
parser.add_argument('--gri-mode', help='Specify the mode of the GRI query (\'overlap\' or \'enclose\'; default \'overlap\')', default="overlap")
parser.add_argument('--gri-name', help='Override the default name of the Genomic Range Index (default: "gri"))', default="gri")
parser.add_argument('--csv', help='Use commas instead of tabs', action='store_true')

def main(**kwargs):
    if len(kwargs) == 0:
        args = parser.parse_args(sys.argv[1:])
    else:
        args = parser.parse_args(kwargs)

    # Attempt to resolve name
    try:
        project, folderpath, entity_result = resolve_existing_path(args.path, expected='entity')
    except ResolutionError as details:
        parser.exit(1, fill(unicode(details)) + '\n')

    if entity_result is None:
        parser.exit(1, fill('Could not resolve ' + args.path + ' to a data object') + '\n')

    filename = args.output
    if filename is None:
        filename = entity_result['describe']['name'].replace('/', '%2F')

    dxtable = dxpy.get_handler(entity_result['id'])

    delimiter = ',' if args.csv else '\t'
    if args.output == '-':
        writer = csv.writer(sys.stdout, delimiter=delimiter)
    else:
        if args.output is None and not args.no_ext:
            filename += '.csv' if args.csv else '.tsv'
        if not args.overwrite and os.path.exists(filename):
            parser.exit(1, fill('Error: path \"' + filename + '\" already exists but -f/--overwrite was not set') + '\n')
        writer = csv.writer(open(filename, 'wb'),
                            delimiter=delimiter)
    if not args.no_header:
        writer.writerow((['__id__:int'] if args.rowid else []) + [(col['name'] + ':' + col['type']) for col in dxtable.describe()['columns']])

    # Query stuff
    if args.gri is not None:
        try:
            lo = int(args.gri[1])
            hi = int(args.gri[2])
        except:
            parser.exit(1, fill('Error: the LO and HI arguments to --gri must be integers') + '\n')
        gri_query = dxpy.DXGTable.genomic_range_query(args.gri[0],
                                                      lo,
                                                      hi,
                                                      args.gri_mode,
                                                      args.gri_name)
        iterator = dxtable.iterate_query_rows(query=gri_query, limit=args.limit)
    else:
        iterator = dxtable.iterate_rows(start=args.starting, end=(None if args.limit is None else args.starting + args.limit))
    for row in iterator:
        writer.writerow([unicode(item).encode('utf-8') for item in row[0 if args.rowid else 1:]])

if __name__ == '__main__':
    main()

'''
This file contains parsers with no added help that can be inherited by
other parsers, as well as utility functions for parsing the input to
those parsers.
'''

import argparse, json
from dxpy.utils.printing import *
from dxpy.utils.resolver import split_unescaped

class DXParserError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

no_color_arg = argparse.ArgumentParser(add_help=False)
no_color_arg.add_argument('--color',
                          help=fill('Set when color is used (auto=color is used when stdout is a TTY)', width_adjustment=-24),
                          choices=['off', 'on', 'auto'], default='auto')

delim_arg = argparse.ArgumentParser(add_help=False)
delim_arg.add_argument('--delimiter', '--delim',
                       dest='delimiter',
                       help=fill('Always use exactly one of DELIMITER to separate fields to be printed.  If no delimiter is provided with this flag, TAB will be used.', width_adjustment=-24),
                       nargs='?',
                       const="\t")

json_arg = argparse.ArgumentParser(add_help=False)
json_arg.add_argument('--json', help='Display return value in JSON', action='store_true')

stdout_args = argparse.ArgumentParser(add_help=False)
stdout_args_gp = stdout_args.add_mutually_exclusive_group()
stdout_args_gp.add_argument('--brief', help=fill('Display a brief version of the return value.  For most commands, prints a DNAnexus ID per line.', width_adjustment=-24), action='store_true')
stdout_args_gp.add_argument('--summary', help='Display summary output (default).', action='store_true')
stdout_args_gp.add_argument('--verbose', help='If available, displays extra verbose output',
                            action='store_true')

def get_output_flag(args):
    if not args.brief and not args.summary and not args.verbose:
        args.summary = True

parser_dataobject_args = argparse.ArgumentParser(add_help=False)
parser_dataobject_args_gp = parser_dataobject_args.add_argument_group('metadata arguments')
parser_dataobject_args_gp.add_argument('-o', '--output', help='DNAnexus path for the new object (default uses current project and folder if not provided)')
parser_dataobject_args_gp.add_argument('--visibility', choices=['hidden', 'visible'], dest='hidden', default='visible', help='Whether the object is hidden or not')
parser_dataobject_args_gp.add_argument('--properties', nargs='+', metavar='KEY=VALUE', help='Key-value pairs of properties, e.g. \'--properties property_key=property_value another_property_key=another_property_value\'')
parser_dataobject_args_gp.add_argument('--types', nargs='+', metavar='TYPE', help='Types of the data object')
parser_dataobject_args_gp.add_argument('--tags', nargs='+', metavar='TAG', help='Tags of the data object')
parser_dataobject_args_gp.add_argument('--details', help='JSON to store as details')
parser_dataobject_args_gp.add_argument('-p', '--parents', help='Create any parent folders necessary', action='store_true')

def process_properties_args(args):
    # Properties
    properties = None
    if args.properties is not None:
        properties = {}
        for keyeqval in args.properties:
            try:
                key, val = split_unescaped('=', keyeqval)
            except:
                raise DXParserError('Property key-value pair must be given using syntax "property_key=property_value"')
            properties[key] = val
    args.properties = properties

def process_dataobject_args(args):
    process_properties_args(args)

    # Visibility
    args.hidden = (args.hidden == 'hidden')

    # Details
    if args.details is not None:
        try:
            args.details = json.loads(args.details)
        except:
            raise DXParserError('Error: details could not be parsed as JSON')

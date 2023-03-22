#!/usr/bin/env python3

"""
parser.add_argument('--version', action=PrintDXVersion, nargs=0, help="show program's version number and exit")
"""

"""
class PrintDXVersion(argparse.Action):
    # Prints to stdout instead of the default stderr that argparse
    # uses (note: default changes to stdout in 3.4)
    def __call__(self, parser, namespace, values, option_string=None):
        print('dx v%s' % (dxpy.TOOLKIT_VERSION,))
        parser.exit(0)
"""

import dxpy

def project_available_instance_types():
    print(dxpy.TOOLKIT_VERSION)

if __name__ == "__main__":
    project_available_instance_types()

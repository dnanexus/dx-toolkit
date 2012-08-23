#!/usr/bin/env python

import sys
from dxpy.scripts import dx_gtable_to_tsv

def main():
    sys.argv = [sys.argv[0]] + ['--csv'] + sys.argv[1:]
    dx_gtable_to_tsv.main()

if __name__ == '__main__':
    main()

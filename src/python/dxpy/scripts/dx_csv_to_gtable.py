#!/usr/bin/env python

import sys
from dxpy.scripts import dx_tsv_to_gtable

def main():
    sys.argv = [sys.argv[0]] + ['--csv'] + sys.argv[1:]
    dx_tsv_to_gtable.main()

if __name__ == '__main__':
    main()

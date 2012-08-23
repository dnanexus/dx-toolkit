#!/usr/bin/env python

import sys
from dxpy.scripts import dx_build_app

def main():
    sys.argv = [sys.argv[0]] + ['--create-applet'] + sys.argv[1:]
    dx_build_app.main()

if __name__ == '__main__':
    main()

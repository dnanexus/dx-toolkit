#!/usr/bin/env python

import sys
from dxpy.scripts import dx_build_app

def main():
    sys.argv.append('--create-applet')
    dx_build_app.main()

if __name__ == '__main__':
    main()

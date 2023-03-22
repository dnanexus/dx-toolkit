#!/usr/bin/env python3

"""
utils __init__.py
from .toolkit_version import version as TOOLKIT_VERSION
__version__ = TOOLKIT_VERSION
"""

import sys

sys.path.insert(0, '../')
import src.python.dxpy.utils as utils

def project_available_instance_types():
    v = utils.toolkit_version.version
    print(v)

if __name__ == "__main__":
    project_available_instance_types()

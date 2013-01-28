#!/usr/bin/env python
#
# Copyright (C) 2013 DNAnexus, Inc.
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

import os, sys, glob
from setuptools import setup, find_packages

if sys.version_info < (2, 7):
    raise Exception("dxpy requires Python >= 2.7")

# Don't import, but use execfile.
# Importing would trigger interpretation of the dxpy entry point, which can fail if deps are not installed.
execfile(os.path.join(os.path.dirname(__file__), 'dxpy', 'toolkit_version.py'))

# Grab all the scripts from dxpy/scripts and install them without their .py extension.
# Replace underscores with dashes.
# See Readme.md for details.
scripts = []
for module in os.listdir(os.path.join(os.path.dirname(__file__), 'dxpy', 'scripts')):
    if module == '__init__.py' or module[-3:] != '.py':
        continue
    module = module[:-3]
    script = module.replace('_', '-')
    scripts.append("{s} = dxpy.scripts.{m}:main".format(s=script, m=module))

dependencies = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

# If on Windows, also depend on colorama, which translates ANSI terminal color control sequences into whatever cmd.exe uses.
if os.name == 'nt':
    dependencies.append("colorama==0.2.4")
else:
    # If this is an OS X system where GNU readline is imitated by libedit, add the readline module from pypi to dependencies.
    # See also http://stackoverflow.com/questions/7116038
    # Warning: This may not work as intended in cross-compilation scenarios
    try:
        import readline
        if 'libedit' in readline.__doc__:
            dependencies.append("readline==6.2.4.1")
    except ImportError:
        dependencies.append("readline==6.2.4.1")

setup(
    name='dxpy',
    version=version,
    description='DNAnexus Platform API bindings for Python',
    author='Katherine Lai, Phil Sung, Andrey Kislyuk',
    author_email='klai@dnanexus.com, psung@dnanexus.com, akislyuk@dnanexus.com',
    url='https://github.com/dnanexus/dx-toolkit',
    zip_safe=False,
    license='as-is',
    packages = find_packages(),
    scripts = glob.glob(os.path.join(os.path.dirname(__file__), 'scripts', 'dx*')),
    entry_points = {
        "console_scripts": scripts,
    },
    install_requires = dependencies,
)

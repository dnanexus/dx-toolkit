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

import os, sys, glob, platform
from setuptools import setup, find_packages

if sys.version_info < (2, 7):
    raise Exception("dxpy requires Python >= 2.7")

# Don't import, but use exec.
# Importing would trigger interpretation of the dxpy entry point, which can fail if deps are not installed.
with open(os.path.join(os.path.dirname(__file__), 'dxpy', 'toolkit_version.py')) as fh:
    exec(compile(fh.read(), 'toolkit_version.py', 'exec'))

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
test_dependencies = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements_test.txt"))]
dxfs_dependencies = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements_dxfs.txt"))]

# If on Windows, also depend on colorama, which translates ANSI terminal color control sequences into whatever cmd.exe uses.
if platform.system() == 'Windows':
    dependencies = [d for d in dependencies if not (d.startswith('distribute') or d.startswith('psutil'))]
    dependencies.append("colorama==0.2.4")

# If this is an OS X system where GNU readline is imitated by libedit, add the readline module from pypi to dependencies.
# See also http://stackoverflow.com/questions/7116038
# Warning: This may not work as intended in cross-compilation scenarios
if platform.system() == 'Darwin':
    try:
        import readline
        if 'libedit' in readline.__doc__:
            dependencies.append("readline==6.2.4.1")
    except ImportError:
        dependencies.append("readline==6.2.4.1")

# dxfs is not compatible with Windows, and is currently disabled on Python 3
if platform.system() != 'Windows' and sys.version_info[0] < 3:
    dependencies.extend(dxfs_dependencies)

setup(
    name='dxpy',
    version=version,
    description='DNAnexus Platform API bindings for Python',
    author='Katherine Lai, Phil Sung, Andrey Kislyuk, Anurag Biyani',
    author_email='expert-dev@dnanexus.com',
    url='https://github.com/dnanexus/dx-toolkit',
    zip_safe=False,
    license='Apache Software License',
    packages = find_packages(),
    package_data={'dxpy.packages.requests': ['*.pem']},
    scripts = glob.glob(os.path.join(os.path.dirname(__file__), 'scripts', 'dx*')),
    entry_points = {
        "console_scripts": scripts,
    },
    install_requires = dependencies,
    setup_requires = test_dependencies,
    tests_require = test_dependencies,
    test_suite = "test",
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Unix Shell',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

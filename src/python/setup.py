#!/usr/bin/env python
#
# Copyright (C) 2013-2016 DNAnexus, Inc.
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

import glob
import os
import platform
import re
from setuptools import setup, find_packages
import sys

if sys.version_info < (2, 7):
    raise Exception("dxpy requires Python >= 2.7")

# Pypi is the repository for python packages.
# It requires that version numbers look like this: X.Y.Z,
# where X, Y, and Z are numbers. It is more complicated than that, but that's
# the main idea.
#
# Clean up the version number. It starts like this:
#    '0.265.0-77-g059d243f'
# and we need 0.265.0
def make_valid_pypi_version(raw):
    m = re.match(r'(\d+)\.(\d+)\.(\d+)', raw)
    return m.group(0)


# Don't import, but use exec.
# Importing would trigger interpretation of the dxpy entry point, which can fail if deps are not installed.
#
# The result of this trickery is a variable called "version", initialized
# to the current version of dxpy.
with open(os.path.join(os.path.dirname(__file__), 'dxpy', 'toolkit_version.py')) as fh:
    exec(compile(fh.read(), 'toolkit_version.py', 'exec'))
version = make_valid_pypi_version(version)

# The readme file is used as the long-description of the package.
# It will show up in the pypi site.
with open(os.path.join(os.path.dirname(__file__), "Readme.md"), "r") as fh:
    readme_content = fh.read()

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
backports_dependencies = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements_backports.txt"))]

if sys.version_info[0] < 3:
    dependencies.extend(backports_dependencies)

if 'DNANEXUS_INSTALL_PYTHON_TEST_DEPS' in os.environ:
    dependencies.extend(test_dependencies)

template_files = []

for directory, subdirectories, files in os.walk("dxpy/templating/templates"):
    directory = directory[len("dxpy/templating/"):]
    template_files.extend([os.path.join(directory, _file) for _file in files])

setup(
    name='dxpy',
    version=version,
    description='DNAnexus Platform API bindings for Python',
    long_description=readme_content,
    long_description_content_type="text/markdown",
    author='Aleksandra Zalcman, Andrey Kislyuk, Anurag Biyani, Geet Duggal, Katherine Lai, Kurt Jensen, Ohad Rodeh, Phil Sung',
    author_email='support@dnanexus.com',
    url='https://github.com/dnanexus/dx-toolkit',
    zip_safe=False,
    license='Apache Software License',
    packages = find_packages(exclude=['test']),
    package_data={'dxpy.templating': template_files},
    scripts = glob.glob(os.path.join(os.path.dirname(__file__), 'scripts', 'dx*')),
    entry_points = {
        "console_scripts": scripts,
    },
    install_requires = dependencies,
    extras_require={
        'xattr': ["xattr==0.9.6; sys_platform == 'linux2' or sys_platform == 'linux'"]
    },
    tests_require = test_dependencies,
    test_suite = "test",
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Unix Shell',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

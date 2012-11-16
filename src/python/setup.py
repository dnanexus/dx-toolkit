#!/usr/bin/env python

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

# If this is an OS X system where GNU readline is imitated by libedit, add the readline module from pypi to dependencies.
# See also http://stackoverflow.com/questions/7116038
# Warning: This may not work as intended in cross-compilation scenarios
import readline
if 'libedit' in readline.__doc__:
    dependencies.append("readline==6.2.2")

# If on Windows, also depend on colorama, which translates ANSI terminal color control sequences into whatever cmd.exe uses.
if os.name == 'nt':
    dependencies.append("colorama==0.2.4")

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
    #scripts = glob.glob('scripts/*.py'),
    entry_points = {
        "console_scripts": scripts,
    },
    install_requires = dependencies,
)

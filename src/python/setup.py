#!/usr/bin/env python

import os, sys, glob
from setuptools import setup, find_packages

setup(
    name='dxpy',
    version='0.1',
    description='DNAnexus Platform API bindings for Python',
    author='Katherine Lai, Andrey Kislyuk',
    author_email='klai@dnanexus.com, akislyuk@dnanexus.com',
    url='https://github.com/dnanexus/nucleus_client',
    license='as-is',
    packages = find_packages(),
    scripts = glob.glob('scripts/*.py'),
    install_requires = ['requests',
                        'futures',
                        'ctypes-snappy'],
)

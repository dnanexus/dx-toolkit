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

SHELL=/bin/bash -e

ifndef MAKEOPTS
	MAKEOPTS=-j -l 2.0
endif
MAKE:=$(MAKE) $(MAKEOPTS)

UNAME := $(shell uname)

# Figure out which os we are on, and store that
# information in one succient variable called PLATFORM.
# The possible values are: {windows, osx, linux}.
ifeq ($(OS), Windows_NT)
	PLATFORM=windows
else ifeq ($(UNAME), Darwin)
	PLATFORM=osx
else
	PLATFORM=linux
endif

ifeq (${PLATFORM}, "linux")
	CENTOS_MAJOR_VERSION := $(shell test -e /etc/issue && (grep -o "CentOS release [0-9]\+" /etc/issue | sed -e "s/CentOS release //"))
	FEDORA_MAJOR_VERSION := $(shell test -e /etc/issue && (grep -o "Fedora release [0-9]\+" /etc/issue | sed -e "s/Fedora release //"))
	UBUNTU_VERSION := $(shell test -e /etc/issue && (grep -o "Ubuntu [0-9]\+\.[0-9]\+" /etc/issue | sed -e "s/Ubuntu //"))
	RHEL_MAJOR_VERSION := $(shell test -e /etc/redhat-release && (grep -o "Red Hat Enterprise Linux .* release [0-9]\+" /etc/redhat-release | sed -e	 "s/Red Hat Enterprise Linux .* release //"))
endif

# Extract the two most significant digits the python distribution
#
PYTHON_VERSION_NUMBER:=$(shell python3 -c 'import sys; print("{}.{}".format(sys.version_info[0], sys.version_info[1]))')
PYTHON_MAJOR_VERSION:=$(shell python3 -c 'import sys; print(sys.version_info[0])')

ifeq (${PYTHON_MAJOR_VERSION}, 2)
	PIP=pip
	VIRTUAL_ENV=virtualenv
else
	PIP=pip3
	VIRTUAL_ENV=python3 -m venv
endif

ifndef CCACHE_DISABLE
	export PATH := /usr/lib/ccache:$(PATH)
endif

# If installing into the system directories you probably want to set
#   DESTDIR=/ PREFIX=/usr
ifndef DESTDIR
	export DESTDIR=/opt/dnanexus
endif
ifndef PREFIX
	export PREFIX=/
endif

export DNANEXUS_HOME := $(CURDIR)/..
export PATH := $(DNANEXUS_HOME)/build/bin:$(PATH)
export DX_PY_ENV := $(DNANEXUS_HOME)/build/py_env
export DNANEXUS_LIBDIR := $(DNANEXUS_HOME)/share/dnanexus/lib

# Short-circuit sudo when running as root. In a chrooted environment we are
# likely to be running as root already, and sudo may not be present on minimal
# installations.
ifeq (${USER}, root)
	MAYBE_SUDO=
else
	MAYBE_SUDO='sudo'
endif


ifeq ($(PLATFORM), windows)
        ACTIVATE=Scripts/activate
	# On Windows using DNANEXUS_HOME as the pip install --prefix results in
	# an oddly nested dir, so make a new var to hold the staging dir name in
	# which we'll be installing packages:
	WHEEL_TEMPDIR=wheel_tempdir
else
        ACTIVATE=bin/activate
endif


# Docker2aci download URL
ifeq ($(PLATFORM), osx)
	DOCKER_ACI_URL=https://dl.dnanex.us/F/D/40XJf2gqYqYqzYbGzyqB4g9PxGX9ZY9Bqvgv0Pz5/docker2aci-osx
else ifeq ($(PLATFORM), windows)
# dx-docker not currently supported on Windows."
	DOCKER_ACI_URL=
else
	DOCKER_ACI_URL=https://dl.dnanex.us/F/D/B9ZFXqk6q9g0Z3FygYq61y82vpKQ2gj191vkP9jz/docker2aci
endif

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
UNAME := $(shell uname)

ifeq ($(OS), Windows_NT)
	include mk/Makefile.windows
else ifeq ($(UNAME), Darwin)
	include mk/Makefile.osx
else
	include mk/Makefile.linux
endif


# Typecheck python with pyre
# =====
py_typecheck:
	pyre \
		--typeshed $(HOME)/.local/lib/pyre_check/typeshed \
		--show-parse-errors check

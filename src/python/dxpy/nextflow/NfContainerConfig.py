#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

class NfContainerConfig(object):
    def __init__(self, file_url):
        """
        :param file_url: URL to the local(ized) file with docker references.
        :type file_url: String

        Abstract class to represent and store information about a single file with docker references in a given NF
        pipeline.
        """
        self.image_ref = None
        self.file_url = file_url

    @property
    def image_ref(self):
        return self.image_ref

    @image_ref.setter
    def image_ref(self, value):
        self._image_ref = value


class NfConfigFile(NfContainerConfig):
    def __init__(self, file_url):
        """
        Concrete implementation extracting docker references from the *.config files in a given pipeline
        """
        super().__init__(file_url)


    def _extract_docker_refs_from_src(self):
        self.image_ref = None


class NfSource(NfContainerConfig):
    def __init__(self, file_url):
        """
        Concrete implementation extracting docker references from the *.nf files in a given pipeline.
        NOT SUPPORTED
        """
        super().__init__(file_url)
        raise NotImplementedError("Parsing *.nf files is not supported")

    def _extract_docker_refs_from_src(self):
        self.image_ref = None





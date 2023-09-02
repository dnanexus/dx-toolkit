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

import dxpy

def collect_docker_images(resources_dir):
    """
    :param resources_dir: URL to the local(ized) NF pipeline in the app(let) resources.
    :type resources_dir: String
    :returns: an array of DockerImageRef objects.
    """
    container_configs = _collect_container_configs(resources_dir)
    return None


def _collect_container_configs(resources_dir):
    """
    :param resources_dir: URL to the local(ized) NF pipeline in the app(let) resources.
    :type resources_dir: String
    :returns: an array of objects of NfConfigFile and NfSource (subclasses of NfContainerConfig).
    """

    # unwrap from each NfContainerConfig
    return None
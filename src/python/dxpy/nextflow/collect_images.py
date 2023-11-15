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

import json
import subprocess
from dxpy.nextflow.ImageRefFactory import ImageRefFactory, ImageRefFactoryError

CONTAINERS_JSON = "containers.json"


def bundle_docker_images(image_refs):
    """
    :param image_refs: Image references extracted from run_nextaur_collect().
    :type image_refs: Dict
    :returns: Array of dicts for bundledDepends attribute of the applet resources. Also saves images on the platform
    if not done that before.
    """
    image_factories = [ImageRefFactory(x) for x in image_refs]
    images = [x.get_image() for x in image_factories]
    seen_images = set()
    bundled_depends = []
    for image in images:
        if image.identifier in seen_images:
            continue
        else:
            bundled_depends.append(image.bundled_depends.copy())
            seen_images.add(image.identifier)
    return bundled_depends


def run_nextaur_collect(resources_dir):
    """
        :param resources_dir: URL to the local(ized) NF pipeline in the app(let) resources.
        :type resources_dir: String
        :returns: Dict. Image references in the form of
            "process": String. Name of the process/task
            "repository": String. Repository (host) prefix
            "image_name": String. Image base name
            "tag": String. Version tag
            "digest": String. Image digest
            "file_id": String. File ID if found on the platform
            "engine": String. Container engine.
        Runs nextaur:collect
        """
    collect_cmd = "nextflow plugin nextaur:collect docker {}".format(resources_dir)
    _ = subprocess.check_output(collect_cmd, shell=True)
    with open(CONTAINERS_JSON, "r") as json_file:
        image_refs = json.load(json_file).get("processes", None)
        if not image_refs:
            raise ImageRefFactoryError("Could not extract processes from nextaur:collect")
    return image_refs


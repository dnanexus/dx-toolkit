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

import logging
logging.basicConfig(level=logging.DEBUG)

import argparse
import base64
import bs4
import dxpy
import imghdr
import os
import re
import urllib2

parser = argparse.ArgumentParser(description="Constructs and uploads an HTML report from HTML and attached images")
parser.add_argument("src", help="Source image or HTML file")
parser.add_argument("-p", "--project", help="Destination project ID. Can also be a full path in the form PROJECT:/PATH/TO/FOLDER")
parser.add_argument("-o", "--output", help="Local file to save baked HTML to", default=None)


class ImageBaker():
    """
    Takes an HTML and parses it for image links, then fetches those images and re-encodes the src
    attribute to include the image data encoded in Base64.
    """
    def __init__(self, src):
        src = os.path.realpath(src)
        self.path = os.path.dirname(src)
        self.file = os.path.basename(src)
        self.html = "".join(_load_file(src).readlines())
        if imghdr.what("", self.html):
            self.html = "<html><body><img src='{}'/></body></html>".format(self.file)

    def _image_to_data(self, img):
        """
        Does the work of encoding an image into Base64
        """
        # If the image is already encoded in Base64, we have nothing to do here
        if not img["src"] or re.match("data:", img["src"]):
            return
        if re.match("http[s]://", img["src"]):
            img_data = _load_url(img["src"]).read()
        else:
            img_data = _load_file(img["src"]).read()
        img_type = imghdr.what("", img_data)
        img_b64 = base64.b64encode(img_data)
        src_data = "data:image/{};base64,{}".format(img_type, img_b64)
        img["src"] = src_data

    def bake(self):
        """
        Runs the encoder on the file given when this class was instantiated
        """
        cwd = os.getcwd()
        os.chdir(self.path)
        bs_html = bs4.BeautifulSoup(self.html)
        images = bs_html.find_all("img")
        for image in images:
            self._image_to_data(image)
        os.chdir(cwd)
        return bs_html


def _load_file(path, mode="r"):
    """
    Loads a file from the local filesystem
    """
    if not os.path.exists(path):
        parser.error("{} was not found!".format(path))
    try:
        f = open(path, mode)
        return f
    except IOError:
        parser.error("{} could not be read due to an I/O error!".format(path))


def _load_url(url):
    """
    Loads a URL resource from a remote server
    """
    try:
        response = urllib2.urlopen(url)
        return response
    except urllib2.URLError:
        parser.error("{} could not be loaded remotely!".format(url))


def _upload_html(project, html, path="/"):
    """
    Uploads the HTML to a file on the server
    """
    try:
        dxfile = dxpy.bindings.dxfile_functions
        file_id = dxpy.api.file_new({"project": project, "folder": path, "hidden": True})["id"]
        html_file = dxfile.DXFile(file_id, project, "w")
        html_file.write(html)
        html_file.close()
        return file_id
    except:
        parser.error("Could not upload HTML report to DNAnexus server!")


def _create_record(project, file_ids, path="/"):
    """
    Creates a master record for the HTML report; this doesn't contain contain the actual HTML, but
    reports are required to be records rather than files
    """
    files = []
    for file_id in file_ids:
        files.append(dxpy.bindings.dxdataobject_functions.dxlink(file_id, project))
    args = {
        "project": project,
        "folder": path,
        "types": ["Report", "HTMLReport"],
        "details": {"files": files}
    }
    try:
        record_id = dxpy.api.record_new(args)["id"]
        dxpy.api.record_close(record_id)
        return record_id
    except:
        parser.error("Could not create an HTML report record on DNAnexus servers!")


def upload(project, html):
    """
    Uploads all HTML report components to DNAnexus and creates a record to encompass them all
    """
    if not re.match("project-", project):
        project = "project-" + project
    path = "/"
    if re.match("project-[\d\w]{24}:/", project):
        path = re.sub("^project-[\d\w]{24}:", "", project)
        project = re.sub("^(project-[\d\w]{24}):/.*", "\\1", project)
    file_id = _upload_html(project, html, path)
    record_id = _create_record(project, [file_id], path)
    return record_id


def save(filename, html):
    """
    Creates a baked HTML file on the local system
    """
    try:
        out_file = open(filename, "w")
        out_file.write(html)
        out_file.close()
    except IOError:
        parser.error("Could not write baked HTML to local file {}".format(filename))


def main(**kwargs):
    if len(kwargs) == 0:
        args = parser.parse_args()
    else:
        args = parser.parse_args(**kwargs)
    image_baker = ImageBaker(args.src)
    html = image_baker.bake()
    # Adjust all links in the HTML Report to address the top bar so they're still useful
    for link in html("a"):
        link["target"] = "_top"
    if args.project:
        print(upload(args.project, str(html)))
    if args.output:
        save(args.output, str(html))


if __name__ == "__main__":
    main()

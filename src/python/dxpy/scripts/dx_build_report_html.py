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
logging.basicConfig(level=logging.WARN)

import argparse
import base64
import bs4
import dxpy
import imghdr
import os
import re
import urllib2

parser = argparse.ArgumentParser(description="Constructs and uploads an HTML report from HTML and attached images")
parser.add_argument("src", help="Source image or HTML file", nargs="+")
parser.add_argument("-n", "--name", help="Name to give the report on the remote DNAnexus system")
parser.add_argument("-o", "--output", help="Local file to save baked HTML to", default=None)
parser.add_argument("-p", "--project", help="Destination project ID. Can also be a full path in the form PROJECT:/PATH/TO/FOLDER")


def _image_to_data(img):
    """
    Does the work of encoding an image into Base64
    """
    # If the image is already encoded in Base64, we have nothing to do here
    if not img["src"] or re.match("data:", img["src"]):
        return
    elif re.match("http[s]://", img["src"]):
        img_data = _load_url(img["src"]).read()
    else:
        img_data = _load_file(img["src"]).read()
    img_type = imghdr.what("", img_data)
    img_b64 = base64.b64encode(img_data)
    src_data = "data:image/{};base64,{}".format(img_type, img_b64)
    img["src"] = src_data


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


def bake(src):
    """
    Runs the encoder on the given source file
    """
    src = os.path.realpath(src)
    path = os.path.dirname(src)
    filename = os.path.basename(src)
    html = "".join(_load_file(src).readlines())
    if imghdr.what("", html):
        html = "<html><body><img src='{}'/></body></html>".format(filename)

    # Change to the file's directory so image files with relative paths can be loaded correctly
    cwd = os.getcwd()
    os.chdir(path)
    bs_html = bs4.BeautifulSoup(html)
    images = bs_html.find_all("img")
    for image in images:
        _image_to_data(image)
    os.chdir(cwd)
    return bs_html


def upload_html(project, html, path="/", name=""):
    """
    Uploads the HTML to a file on the server
    """
    try:
        dxfile = dxpy.bindings.dxfile_functions
        args = {
            "project": project,
            "folder": path,
            "hidden": True
        }
        if name and len(name) > 0:
            args["name"] = name
        file_id = dxpy.api.file_new(args)["id"]
        html_file = dxfile.DXFile(file_id, project, "w")
        html_file.write(html)
        html_file.close()
        return file_id
    except:
        parser.error("Could not upload HTML report to DNAnexus server!")


def create_record(project, file_ids, path="/", name=""):
    """
    Creates a master record for the HTML report; this doesn't contain contain the actual HTML, but reports
    are required to be records rather than files and we can link more than one HTML file to a report
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
    if name and len(name) > 0:
        args["name"] = name
    try:
        record_id = dxpy.api.record_new(args)["id"]
        dxpy.api.record_close(record_id)
        return record_id
    except:
        parser.error("Could not create an HTML report record on DNAnexus servers!")


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
    if args.project:
        if not re.match("project-", args.project):
            args.project = "project-" + args.project
        path = "/"
        if re.match("project-[\d\w]{24}:/", args.project):
            path = re.sub("^project-[\d\w]{24}:", "", args.project)
            args.project = re.sub("^(project-[\d\w]{24}):/.*", "\\1", args.project)
    remote_file_ids = []
    for i, source in enumerate(args.src):
        html = bake(source)
        # Adjust all links in the HTML Report to address the top bar so they're still useful
        for link in html("a"):
            link["target"] = "_top"

        # If we're supposed to upload the report to the server, upload the individual HTML file
        if args.project:
            remote_file_ids.append(upload_html(args.project, str(html), path, os.path.basename(source)))

        # If we're supposed to save locally, do that
        if args.output:
            filename = args.output
            # We may have to wrangle the filename a little if there are multiple output files
            if len(args.src) > 1:
                index_str = "." + str(i)
            else:
                index_str = ""
            if re.search("\.html?$", filename):
                filename = re.sub("(\.html?)$", index_str + "\\1", filename)
            else:
                filename += index_str + ".html"
            save(filename, str(html))
    if len(remote_file_ids) > 0:
        print(create_record(args.project, remote_file_ids, path, args.name or args.src[0]))


if __name__ == "__main__":
    main()

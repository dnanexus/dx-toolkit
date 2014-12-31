#!/usr/bin/env python
#
# Copyright (C) 2013-2014 DNAnexus, Inc.
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

from __future__ import print_function

import logging
logging.basicConfig(level=logging.WARN)

import argparse
import base64
import bs4
import cgi
import dxpy
from dxpy.compat import BytesIO
from .dx_build_app import parse_destination
import imghdr
import json
import os
import re
import requests

parser = argparse.ArgumentParser(description="Constructs and saves/uploads an HTML report from HTML and/or linked images")
parser.add_argument("src", help="Source image or HTML file(s)", nargs="+")
parser.add_argument("-r", "--remote", help="Destination route. Can be: (1) a project ID, (2) a path, with or without object name (e.g. /PATH/REPORT_NAME), (3) project ID + path (e.g. PROJECT:/PATH/REPORT_NAME)")
parser.add_argument("--local", help="Local file to save baked HTML to", default=None)
parser.add_argument("-w", "--width", help="Width of the final report, in pixels", default=None)
parser.add_argument("-g", "--height", help="Height of the final report, in pixels", default=None)


def _image_to_data(img):
    """
    Does the work of encoding an image into Base64
    """
    # If the image is already encoded in Base64, we have nothing to do here
    if not "src" in img.attrs or img["src"].startswith("data:"):
        return
    elif re.match("https?://", img["src"]):
        img_data = _load_url(img["src"]).read()
    else:
        img_data = _load_file(img["src"]).read()
    img_type = imghdr.what("", img_data)
    img_b64 = base64.b64encode(img_data)
    src_data = "data:image/none;base64,"
    if img_type:
        src_data = "data:image/{};base64,{}".format(img_type, img_b64)
    img["src"] = src_data


def _bake_css(link):
    """
    Takes a link element and turns it into an inline style link if applicable
    """
    if "href" in link.attrs and (re.search("\.css$", link["href"])) or ("rel" in link.attrs and link["rel"] is "stylesheet") or ("type" in link.attrs and link["type"] is "text/css"):
        if re.match("https?://", link["href"]):
            css_data = _load_url(link["href"]).read()
        else:
            css_data = _load_file(link["href"]).read()
        link.clear()
        link.string = css_data
        link.name = "style"
        del link["rel"]
        del link["href"]


def _bake_script(script):
    """
    Takes a script element and bakes it in only if it contains a remote resource
    """
    if "src" in script.attrs:
        if re.match("https?://", script["src"]):
            script_data = _load_url(script["src"]).read()
        else:
            script_data = _load_file(script["src"]).read()
        script.clear()
        script.string = "\n" + script_data + "\n"
        del script["src"]
        del script["type"]


def _topify_link(link):
    """
    Adds a target='_top' property to links so they can break out of iframes
    """
    if "target" not in link.attrs:
        link["target"] = "_top"


def _load_file(path, mode="r"):
    """
    Loads a file from the local filesystem
    """
    if not os.path.exists(path):
        parser.error("{} was not found!".format(path))
    try:
        f = open(path, mode)
        return f
    except IOError as ex:
        parser.error("{path} could not be read due to an I/O error! ({ex})".format(path=path, ex=ex))


def _load_url(url):
    """
    Loads a URL resource from a remote server
    """
    try:
        response = requests.get(url)
        return BytesIO(response.content)
    except IOError as ex:
        parser.error("{url} could not be loaded remotely! ({ex})".format(url=url, ex=ex))


def _get_bs4_string(soup):
    """
    Outputs a BeautifulSoup object as a string that should hopefully be minimally modified
    """
    if len(soup.find_all("script")) == 0:
        soup_str = soup.prettify(formatter=None).encode("utf-8").strip()
    else:
        soup_str = str(soup.html)
        soup_str = re.sub("&amp;", "&", soup_str)
        soup_str = re.sub("&lt;", "<", soup_str)
        soup_str = re.sub("&gt;", ">", soup_str)
    return soup_str


def bake(src):
    """
    Runs the encoder on the given source file
    """
    src = os.path.realpath(src)
    path = os.path.dirname(src)
    filename = os.path.basename(src)
    html = _load_file(src).read()
    if imghdr.what("", html):
        html = "<html><body><img src='{}'/></body></html>".format(cgi.escape(filename))

    # Change to the file's directory so image files with relative paths can be loaded correctly
    cwd = os.getcwd()
    os.chdir(path)
    bs_html = bs4.BeautifulSoup(html)
    images = bs_html.find_all("img")
    for image in images:
        _image_to_data(image)
    for link in bs_html.find_all("link"):
        _bake_css(link)
    for script in bs_html.find_all("script"):
        _bake_script(script)
    os.chdir(cwd)
    return bs_html


def upload_html(destination, html, name=None):
    """
    Uploads the HTML to a file on the server
    """
    [project, path, n] = parse_destination(destination)
    try:
        dxfile = dxpy.upload_string(html, media_type="text/html", project=project, folder=path, hidden=True, name=name or None)
        return dxfile.get_id()
    except dxpy.DXAPIError as ex:
        parser.error("Could not upload HTML report to DNAnexus server! ({ex})".format(ex=ex))


def create_record(destination, file_ids, width=None, height=None):
    """
    Creates a master record for the HTML report; this doesn't contain contain the actual HTML, but reports
    are required to be records rather than files and we can link more than one HTML file to a report
    """
    [project, path, name] = parse_destination(destination)
    files = [dxpy.dxlink(file_id) for file_id in file_ids]
    details = {"files": files}
    if width:
        details["width"] = width
    if height:
        details["height"] = height
    try:
        dxrecord = dxpy.new_dxrecord(project=project, folder=path, types=["Report", "HTMLReport"], details=details, name=name)
        dxrecord.close()
        return dxrecord.get_id()
    except dxpy.DXAPIError as ex:
        parser.error("Could not create an HTML report record on DNAnexus servers! ({ex})".format(ex=ex))


def save(filename, html):
    """
    Creates a baked HTML file on the local system
    """
    try:
        out_file = open(filename, "w")
        out_file.write(html)
        out_file.close()
    except IOError as ex:
        parser.error("Could not write baked HTML to local file {name}. ({ex})".format(name=filename, ex=ex))


def main(**kwargs):
    if len(kwargs) == 0:
        args = parser.parse_args()
    else:
        args = parser.parse_args(**kwargs)
    if not args.remote and not args.local:
        parser.error("Nothing to do! (At least one of --remote and --local must be specified.)")
    remote_file_ids = []
    for i, source in enumerate(args.src):
        html = bake(source)
        # Adjust all links in the HTML Report to address the top bar so they're still useful
        for link in html("a"):
            _topify_link(link)
        for link in html("area"):
            _topify_link(link)

        html_str = _get_bs4_string(html)
        # If we're supposed to upload the report to the server, upload the individual HTML file
        if args.remote:
            remote_file_ids.append(upload_html(args.remote, html_str, os.path.basename(source)))

        # If we're supposed to save locally, do that
        if args.local:
            filename = args.local
            # We may have to wrangle the filename a little if there are multiple output files
            if len(args.src) > 1:
                index_str = "." + str(i)
            else:
                index_str = ""
            if re.search("\.html?$", filename):
                filename = re.sub("(\.html?)$", index_str + "\\1", filename)
            else:
                filename += index_str + ".html"
            save(filename, html_str)
    if len(remote_file_ids) > 0:
        json_out = {"fileIds": remote_file_ids}
        json_out["recordId"] = create_record(args.remote, remote_file_ids, args.width, args.height)
        print(json.dumps(json_out))


if __name__ == "__main__":
    main()

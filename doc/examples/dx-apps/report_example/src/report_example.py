#!/usr/bin/env python
#
# Copyright (C) 2013 DNAnexus, Inc.
#
# This file is part of a tutorial on integrating HTML-based reports into apps
# to be run on the DNAnexus platform. Reproduction, modification, and distribution
# are all highly encouraged.
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
import json
import matplotlib
matplotlib.use("Agg")  # Used for headless environments like the DNAnexus workspace
import matplotlib.pyplot as pyplot
import numpy
import subprocess

use_html = False  # True to generate an HTML file containing the images and extra info
lines_filename = "lines.png"
bars_filename = "bars.png"
histogram_filename = "histogram.png"
html_filename = "tobebaked.html"


@dxpy.entry_point("main")
def main(**kwargs):
    """
    Draw a couple of simple graphs and optionally generate an HTML file to upload them
    """
    draw_lines()
    draw_histogram()
    draw_bar_chart()
    destination = "-r /report"
    if use_html:
        generate_html()
        command = "dx-build-report-html {h} {d}".format(h=html_filename, d=destination)
    else:
        command = "dx-build-report-html {l} {b} {h} {d}".format(l=lines_filename, b=bars_filename, h=histogram_filename, d=destination)
    sub_output = json.loads(subprocess.check_output(command, shell=True))
    output = {}
    output["report"] = dxpy.dxlink(sub_output["recordId"])
    return output


def draw_histogram():
    """
    Uses sample code from http://matplotlib.org/1.2.1/examples/api/histogram_demo.html
    """

    mu, sigma = 100, 15
    x = mu + sigma * numpy.random.randn(10000)
    fig = pyplot.figure()
    ax = fig.add_subplot(111)
    n, bins, patches = ax.hist(x, 50, normed=1, facecolor='green', alpha=0.75)

    ax.set_xlabel('Random number generated')
    ax.set_ylabel('Probability')
    ax.set_xlim(40, 160)
    ax.set_ylim(0, 0.03)
    ax.grid(True)

    pyplot.savefig(histogram_filename)


def draw_bar_chart():
    """
    Uses sample code from http://matplotlib.org/1.2.1/examples/api/barchart_demo.html
    """
    N = 5
    menMeans = (20, 35, 30, 35, 27)
    menStd = (2, 3, 4, 1, 2)

    ind = numpy.arange(N)
    width = 0.35

    fig = pyplot.figure()
    ax = fig.add_subplot(111)
    rects1 = ax.bar(ind, menMeans, width, color='r', yerr=menStd)

    womenMeans = (25, 32, 34, 20, 25)
    womenStd = (3, 5, 2, 3, 3)
    rects2 = ax.bar(ind+width, womenMeans, width, color='y', yerr=womenStd)

    ax.set_ylabel('Scores')
    ax.set_title('Scores by group and color')
    ax.set_xticks(ind+width)
    ax.set_xticklabels(('G1', 'G2', 'G3', 'G4', 'G5'))

    ax.legend((rects1[0], rects2[0]), ('Red', 'Yellow'))

    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d' % int(height),
                    ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)
    pyplot.savefig(bars_filename)


def draw_lines():
    """
    Draws a line between a set of random values
    """
    r = numpy.random.randn(200)
    fig = pyplot.figure()
    ax = fig.add_subplot(111)
    ax.plot(r)

    ax.grid(True)

    pyplot.savefig(lines_filename)


def generate_html():
    """
    Generate an HTML file incorporating the images produced by this script
    """
    html_file = open(html_filename, "w")
    html_file.write("<html><body>")
    html_file.write("<h1>Here are some graphs for you!</h1>")
    for image in [lines_filename, bars_filename, histogram_filename]:
        html_file.write("<div><h2>{0}</h2><img src='{0}' /></div>".format(image))
    html_file.write("</body></html>")
    html_file.close()


if __name__ == "__main__":
    main()
else:
    dxpy.run()

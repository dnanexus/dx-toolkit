#!/usr/bin/env python
# python_trimmer_example 0.0.1

import os
import dxpy
import subprocess

@dxpy.entry_point('main')
def main(input_file):

    # The following line(s) initialize your data object inputs on the platform
    # into dxpy.DXDataObject instances that you can start using immediately.

    input_file = dxpy.DXFile(input_file)

    # The following line(s) download your file inputs to the local file system
    # using variable names for the filenames.

    dxpy.download_dxfile(input_file.get_id(), "input_file")

    # Fill in your application code here.

    subprocess.check_call("fastq_quality_trimmer -t 20 -Q 33 -i input_file -o output_file", shell=True)

    # The following line(s) use the Python bindings to upload your file outputs
    # after you have created them on the local file system.  It assumes that you
    # have used the output field name for the filename for each output, but you
    # can change that behavior to suit your needs.

    output_file = dxpy.upload_local_file("output_file");

    # The following line fills in some basic dummy output and assumes
    # that you have created variables to represent your output with
    # the same name as your output fields.

    output = {}
    output["output_file"] = dxpy.dxlink(output_file)

    return output

dxpy.run()

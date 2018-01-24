# Copyright (C) 2013-2018 DNAnexus, Inc.
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

"""
Utilities used when running executables in a batch. For example, running an analysis on
a set of data samples.
"""

from __future__ import print_function, unicode_literals, division, absolute_import
import csv
import dxpy
import json

from ..compat import open
from ..exceptions import err_exit, DXError


# Informational columns in the CSV file, which we want to ignore
# TODO: are these the correct names?
INFO_COLUMNS = ["pattern", "error", "batch_id"]

# Figure out the type for each input, by describing the
# executable
def _get_type_for_columns(executable, input_keys):
    exec_desc = executable.describe()
    input_spec = []
    if 'inputs' in exec_desc:
        # workflow-level inputs were defined for the workflow
        input_spec = exec_desc['inputs']
    elif 'inputSpec' in exec_desc:
        input_spec = exec_desc['inputSpec']
    else:
        raise Exception("executable {} does not have an input specification".format(executable.get_id()))

    input_key_classes={}
    for arg_desc in input_spec:
        if arg_desc['name'] in input_keys:
            input_key_classes[arg_desc['name']] = arg_desc['class']

    return input_key_classes

# val: a value of type string, that needs to be converted
# col_class: the class of the executable argument
#
def _type_convert(val, klass):
    if klass == 'string':
        return val
    elif klass == 'int':
        return int(val)
    else:
        raise Exception("class {} not currently supported".format(klass))

# Parse the CSV file. Create a dictionary with the input arguments for
# each invocation. Return an array of dictionaries.
#
# For each line, except the header:
#   remove informational columns
#   create a dictionary of inputs we can pass to the executable
#
def batch_launch_args(executable, input_json, batch_csv_file):
    header_line = []
    lines = []
    with open(batch_csv_file, "rb") as f:
        reader = csv.reader(f, delimiter=str(u','))
        for i, line in enumerate(reader):
            if i == 0:
                for column_name in line:
                    header_line.append(column_name.strip())
            else:
                lines.append(line)
    # which columns to use
    column_names=[]
    column_nums=[]
    for i, key in enumerate(header_line):
        if key not in INFO_COLUMNS:
            column_nums.append(i)
            column_names.append(key)
    # Get the dx:type for each column
    column_classes = _get_type_for_columns(executable, column_names)
    print("column_classes={}".format(column_classes))

    # a dictionary with input arguments for each job invocation
    launch_args=[]
    for line in lines:
        d_args = {}
        for i, val in enumerate(line):
            if i in column_nums:
                col_name = header_line[i]
                col_class = column_classes[col_name]
                val = val.strip()
                d_args[col_name] = _type_convert(val, col_class)
        # Add all the common arguments
        for k,v in input_json.items():
            d_args[k] = v
        launch_args.append(d_args)
    # it would be nice to call validate at this point
    return launch_args

#
# executable: applet, app, or workflow
# launch_args: array of dictionaries, each of which contains all arguments needed to
#     invoke the executable.
#
# TODO: where do we call validate?
#       add property(s) to execution
#       make this a root execution
def batch_run(executable, launch_args, run_kwargs):
    exec_ids = []
    for input_json in launch_args:
        try:
            dxexecution = executable.run(input_json, **run_kwargs)
            exec_ids.append(dxexecution.get_id())
        except Exception:
            err_exit()

    return exec_ids
